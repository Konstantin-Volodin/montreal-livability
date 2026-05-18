"""S3-backed lakehouse resource for GeoDataFrames.
"""

import io
from typing import Optional

import boto3
import dagster as dg
import geopandas as gpd
from pydantic import PrivateAttr
from upath import UPath


def format_size(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f} MB"


class s3_datastore(dg.ConfigurableResource):
    """S3-backed data lakehouse for reading/writing GeoDataFrames as Parquet."""

    bucket_name: str
    region_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    _base_path: UPath = PrivateAttr()
    _s3: boto3.client = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        """Build the S3 client and base path for the current execution.

        Passing ``None`` for the keys lets boto3 fall back to its default
        credential chain (env vars, shared config, instance profile).
        """
        self._s3 = boto3.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id or None,
            aws_secret_access_key=self.aws_secret_access_key or None,
        )
        self._base_path = UPath(f"s3://{self.bucket_name}/")
        context.log.info(
            f"Initialized S3 client for bucket {self.bucket_name} "
            f"in region {self.region_name}"
        )

    def generate_s3_key(self, context) -> str:
        """Build the S3 key for an asset, including its partition when set."""
        asset_key = context.asset_key
        asset_name = asset_key.path[-1]
        metadata = context.assets_def.metadata_by_key[asset_key]
        layer = metadata.get("layer", "unknown_layer")

        if context.has_partition_key:
            return f"{layer}/{asset_name}.parquet/{context.partition_key}.parquet"
        else: 
            return f"{layer}/{asset_name}.parquet"

    def exists(self, context) -> bool:
        """Return True if this asset's object is already present in S3.

        Used by the raw assets to skip re-downloading source data on a server
        restart / re-materialization when the bucket already holds it.
        """
        key = self.generate_s3_key(context)
        try:
            self._s3.head_object(Bucket=self.bucket_name, Key=key)
            context.log.info(f"Found existing object at s3://{self.bucket_name}/{key}")
            return True
        except self._s3.exceptions.ClientError:
            return False

    def gpq_preview(self, gdf: gpd.GeoDataFrame, n: int = 5) -> str:
        """Markdown preview of the GeoDataFrame, dropping the geometry column."""
        preview_df = gdf.head(n).copy()
        if "geometry" in preview_df.columns:
            preview_df = preview_df.drop(columns=["geometry"])
        try:
            return preview_df.to_markdown()
        except ImportError:
            # `to_markdown` needs `tabulate`; fall back to a plain table.
            return f"```\n{preview_df.to_string()}\n```"

    def write_gpq(self, context, gdf: gpd.GeoDataFrame) -> None:
        """Write a GeoDataFrame to S3 in Parquet format."""
        if gdf is None or gdf.empty:
            context.log.info("No data for this partition. Skipping write.")
            return

        if context.has_partition_key:
            asset_key = context.asset_key
            metadata = context.assets_def.metadata_by_key[asset_key]
            partition_column = metadata.get("segmentation")

            if not partition_column or partition_column not in gdf.columns:
                raise ValueError(
                    "Partitioned assets must set metadata['segmentation'] to "
                    "the GeoDataFrame column used for partitioning."
                )

            gdf = gdf[gdf[partition_column] == context.partition_key]

        if gdf.empty:
            context.log.info("No data for this partition. Skipping write.")
            return

        try:
            s3_key = self.generate_s3_key(context)
            s3_path = self._base_path / s3_key
            context.log.info(f"Preparing to upload to {s3_path}")

            buffer = io.BytesIO()
            gdf.to_parquet(
                buffer, engine="pyarrow", index=False, compression="snappy"
            )
            buffer.seek(0)
            file_size = buffer.getbuffer().nbytes

            self._s3.upload_fileobj(buffer, self.bucket_name, s3_key)
            context.log.info(f"Uploaded file to {s3_path}")

            context.add_output_metadata(
                {
                    "s3_write_location": dg.MetadataValue.text(str(s3_path)),
                    "s3_key": dg.MetadataValue.text(s3_key),
                    "file_size": dg.MetadataValue.text(format_size(file_size)),
                    "num_records": dg.MetadataValue.int(len(gdf)),
                    "columns": dg.MetadataValue.json(list(gdf.columns)),
                    "preview": dg.MetadataValue.md(self.gpq_preview(gdf)),
                }
            )
        except Exception as e:
            context.log.error(f"Failed to upload file: {e}")
            raise

    def write_gpq_partitioned(self, context, gdf: gpd.GeoDataFrame, column: str) -> None:
        """Shard a GeoDataFrame into one Parquet object per distinct `column` value.

        Lets a non-partitioned asset pre-shard its output the same way an
        r7-partitioned downstream asset reads it
        (``{layer}/{asset}.parquet/{value}.parquet``), so each partition reads
        only its slice instead of the whole table.
        """
        if gdf is None or gdf.empty:
            context.log.info("No data. Skipping partitioned write.")
            return

        asset_key = context.asset_key
        asset_name = asset_key.path[-1]
        layer = context.assets_def.metadata_by_key[asset_key].get("layer", "unknown_layer")

        written = 0
        for value, group in gdf.groupby(column, sort=False):
            s3_key = f"{layer}/{asset_name}.parquet/{value}.parquet"
            buffer = io.BytesIO()
            group.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
            buffer.seek(0)
            self._s3.upload_fileobj(buffer, self.bucket_name, s3_key)
            written += 1

        context.log.info(f"write_gpq_partitioned: {written} objects under {layer}/{asset_name}.parquet/ keyed by '{column}'")
        context.add_output_metadata(
            {
                "partition_column": dg.MetadataValue.text(column),
                "num_partitions": dg.MetadataValue.int(written),
                "num_records": dg.MetadataValue.int(len(gdf)),
                "columns": dg.MetadataValue.json(list(gdf.columns)),
            }
        )

    def read_gpq(self, context, key: str) -> gpd.GeoDataFrame:
        """Read a GeoDataFrame back from S3 by key."""
        s3_path = self._base_path / key
        try:
            obj = self._s3.get_object(Bucket=self.bucket_name, Key=key)
            gdf = gpd.read_parquet(io.BytesIO(obj["Body"].read()))
            context.log.info(f"Successfully read data from {s3_path}")
            return gdf
        except Exception as e:
            context.log.error(f"Failed to read file from S3: {e}")
            raise


@dg.definitions
def resources() -> dg.Definitions:
    """Bind the s3_datastore resource into the autoloaded defs folder."""
    return dg.Definitions(
        resources={
            "s3_datastore": s3_datastore(
                bucket_name=dg.EnvVar("S3_BUCKET"),
                region_name=dg.EnvVar("S3_REGION"),
                aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
            ),
        }
    )
