"""S3-backed lakehouse resource: GeoDataFrames as timestamped Parquet snapshots."""

import io
from datetime import datetime, timezone
from typing import Optional

import boto3
import dagster as dg
import geopandas as gpd
import pandas as pd
from pydantic import PrivateAttr
from upath import UPath

# Sibling object in every snapshot directory; its body is the key of the
# newest ``*.parquet`` snapshot, so reads resolve "latest" with one GET.
_POINTER = "_latest"


def format_size(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f} MB"


def location_of(asset: dg.AssetsDefinition) -> str:
    """Lakehouse directory an asset writes to: ``{layer}/{asset_name}``.

    Lets a consumer address an upstream asset by the object itself (already
    imported for ``deps=``) instead of restating its S3 path as a string.
    """
    layer = asset.metadata_by_key[asset.key].get("layer", "unknown_layer")
    return f"{layer}/{asset.key.path[-1]}"


class s3_datastore(dg.ConfigurableResource):
    """S3-backed data lakehouse for reading/writing GeoDataFrames as Parquet."""

    bucket_name: str
    region_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    _base_path: UPath = PrivateAttr()
    _s3: boto3.client = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        """Build the S3 client and base path; blank keys fall back to boto3's default credential chain."""
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

    def asset_dir(self, context, shard: Optional[str] = None) -> str:
        """Directory holding an asset's snapshots: ``{layer}/{asset}[/{shard}]``."""
        base = location_of(context.assets_def)
        return f"{base}/{shard}" if shard is not None else base

    @staticmethod
    def _now_stamp() -> str:
        """Sortable UTC stamp used as both the snapshot filename and data version."""
        return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S_%f}Z"

    def _put_snapshot(self, directory: str, buffer: io.BytesIO, stamp: str) -> str:
        """Upload a snapshot under ``directory`` at ``stamp`` and repoint ``_latest``."""
        key = f"{directory}/{stamp}.parquet"
        self._s3.upload_fileobj(buffer, self.bucket_name, key)
        self._s3.put_object(
            Bucket=self.bucket_name,
            Key=f"{directory}/{_POINTER}",
            Body=key.encode("utf-8"),
        )
        return key

    def _resolve_latest(self, directory: str) -> str:
        """Return the snapshot key the directory's ``_latest`` pointer names."""
        obj = self._s3.get_object(Bucket=self.bucket_name, Key=f"{directory}/{_POINTER}")
        return obj["Body"].read().decode("utf-8")

    def latest_timestamp(self, directory: str) -> Optional[datetime]:
        """Parse the timestamp of the directory's latest snapshot, or None if absent."""
        try:
            key = self._resolve_latest(directory)
        except self._s3.exceptions.ClientError:
            return None
        stamp = key.rsplit("/", 1)[-1].removesuffix(".parquet")
        return datetime.strptime(stamp, "%Y%m%dT%H%M%S_%fZ").replace(tzinfo=timezone.utc)

    @staticmethod
    def _to_wgs84(gdf):
        """Reproject a GeoDataFrame to EPSG:4326; non-geo frames pass through.

        Normalizing on write means consumers can assume lat/lng and never
        reproject on read.
        """
        if not isinstance(gdf, gpd.GeoDataFrame):
            return gdf
        if gdf.crs is None:
            return gdf.set_crs(4326, allow_override=True)
        if gdf.crs.to_epsg() != 4326:
            return gdf.to_crs(4326)
        return gdf

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

    def _snapshot_metadata(self, s3_path, file_size: int, gdf) -> dict:
        """Rich materialization metadata for a snapshot, shared by fresh writes and cache hits."""
        return {
            "s3_location": dg.MetadataValue.text(str(s3_path)),
            "file_size": dg.MetadataValue.text(format_size(file_size)),
            "num_rows": dg.MetadataValue.int(len(gdf)),
            "schema": dg.MetadataValue.json({col: str(gdf[col].dtype) for col in gdf.columns}),
            "preview": dg.MetadataValue.md(self.gpq_preview(gdf)),
        }

    def describe_latest(self, context, directory: str) -> None:
        """Emit metadata for a directory's latest snapshot, reading it but never rewriting it (used on cache hits)."""
        key = self._resolve_latest(directory)
        raw = self._s3.get_object(Bucket=self.bucket_name, Key=key)["Body"].read()
        gdf = self._read_parquet_bytes(raw)
        context.add_output_metadata(self._snapshot_metadata(self._base_path / key, len(raw), gdf))

    def write_gpq(self, context, gdf: gpd.GeoDataFrame) -> Optional[str]:
        """Write a GeoDataFrame to S3 as a timestamped Parquet snapshot; return its stamp."""
        if context.has_partition_key and gdf is not None and not gdf.empty:
            metadata = context.assets_def.metadata_by_key[context.asset_key]
            partition_column = metadata.get("segmentation")
            if not partition_column or partition_column not in gdf.columns:
                raise ValueError(
                    "Partitioned assets must set metadata['segmentation'] to "
                    "the GeoDataFrame column used for partitioning."
                )
            gdf = gdf[gdf[partition_column] == context.partition_key]

        if gdf is None or gdf.empty:
            context.log.info("No data for this partition. Skipping write.")
            return None

        gdf = self._to_wgs84(gdf)
        shard = context.partition_key if context.has_partition_key else None

        buffer = io.BytesIO()
        gdf.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
        buffer.seek(0)
        file_size = buffer.getbuffer().nbytes

        stamp = self._now_stamp()
        s3_key = self._put_snapshot(self.asset_dir(context, shard), buffer, stamp)
        s3_path = self._base_path / s3_key
        context.log.info(f"Wrote snapshot {s3_path}")

        context.add_output_metadata(self._snapshot_metadata(s3_path, file_size, gdf))
        return stamp

    def write_gpq_partitioned(self, context, gdf: gpd.GeoDataFrame, column: str) -> Optional[str]:
        """Pre-shard a GeoDataFrame into one snapshot dir per distinct ``column`` value, so an r6-partitioned consumer reads only its slice. Returns the shared stamp."""
        if gdf is None or gdf.empty:
            context.log.info("No data. Skipping partitioned write.")
            return None

        gdf = self._to_wgs84(gdf)
        stamp = self._now_stamp()
        written = 0
        for value, group in gdf.groupby(column, sort=False):
            buffer = io.BytesIO()
            group.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
            buffer.seek(0)
            self._put_snapshot(self.asset_dir(context, str(value)), buffer, stamp)
            written += 1

        context.log.info(f"write_gpq_partitioned: {written} snapshots under {self.asset_dir(context)}/ keyed by '{column}'")
        context.add_output_metadata(
            {
                "partition_column": dg.MetadataValue.text(column),
                "num_partitions": dg.MetadataValue.int(written),
                "num_records": dg.MetadataValue.int(len(gdf)),
                "columns": dg.MetadataValue.json(list(gdf.columns)),
            }
        )
        return stamp

    @staticmethod
    def _read_parquet_bytes(raw: bytes):
        """Parse parquet bytes, falling back to plain pandas when there's no geometry column (gold tabular assets)."""
        buffer = io.BytesIO(raw)
        try:
            return gpd.read_parquet(buffer)
        except (ValueError, AttributeError):
            buffer.seek(0)
            return pd.read_parquet(buffer)

    def read_gpq(self, context, address: str):
        """Read the latest snapshot of an asset dir (``{layer}/{asset}[/{shard}]``)."""
        key = self._resolve_latest(address)
        obj = self._s3.get_object(Bucket=self.bucket_name, Key=key)
        df = self._read_parquet_bytes(obj["Body"].read())
        context.log.info(f"Read latest snapshot {self._base_path / key}")
        return df

    def read_gpq_prefix(self, context, prefix: str) -> gpd.GeoDataFrame:
        """Concat the latest snapshot of every per-partition subdir under ``prefix`` (the viz layer reading an r6-partitioned asset whole)."""
        paginator = self._s3.get_paginator("list_objects_v2")
        subdirs = [
            cp["Prefix"].rstrip("/")
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=f"{prefix}/", Delimiter="/"
            )
            for cp in page.get("CommonPrefixes", [])
        ]
        if not subdirs:
            raise FileNotFoundError(f"No partitions under s3://{self.bucket_name}/{prefix}/")

        frames = []
        for directory in subdirs:
            key = self._resolve_latest(directory)
            obj = self._s3.get_object(Bucket=self.bucket_name, Key=key)
            frames.append(self._read_parquet_bytes(obj["Body"].read()))

        combined = pd.concat(frames, ignore_index=True)
        if isinstance(frames[0], gpd.GeoDataFrame):
            gdf = gpd.GeoDataFrame(combined)
            if frames[0].crs is not None:
                gdf = gdf.set_crs(frames[0].crs, allow_override=True)
        else:
            gdf = combined
        context.log.info(
            f"read_gpq_prefix: {len(gdf)} rows from {len(subdirs)} partitions under {prefix}"
        )
        return gdf

    def write_html(self, context, html: str) -> str:
        """Upload an HTML document for this asset to ``{layer}/{asset}.html``; return its stamp."""
        asset_name = context.asset_key.path[-1]
        s3_key = f"{location_of(context.assets_def)}.html"
        s3_path = self._base_path / s3_key

        body = html.encode("utf-8")
        self._s3.put_object(
            Bucket=self.bucket_name,
            Key=s3_key,
            Body=body,
            ContentType="text/html",
        )
        context.log.info(f"Uploaded HTML to {s3_path}")
        context.add_output_metadata(
            {
                "s3_write_location": dg.MetadataValue.text(str(s3_path)),
                "s3_key": dg.MetadataValue.text(s3_key),
                "file_size": dg.MetadataValue.text(format_size(len(body))),
                "preview": dg.MetadataValue.md(
                    f"[{asset_name}.html](s3://{self.bucket_name}/{s3_key})"
                ),
            }
        )
        return self._now_stamp()


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
