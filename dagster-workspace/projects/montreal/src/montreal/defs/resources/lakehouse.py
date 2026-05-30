"""S3-backed lakehouse resource: GeoDataFrames as timestamped Parquet snapshots."""

import io
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

import boto3
import dagster as dg
import geopandas as gpd
import pandas as pd
from botocore.config import Config
from pydantic import PrivateAttr
from upath import UPath

# Threads used to read a sharded asset's per-shard snapshots in parallel; also the
# S3 client's connection-pool size, so concurrent GETs never queue on the pool.
_READ_WORKERS = 16

# Per-directory JSON, rewritten on each output: {key, code_version}. `key` names
# the newest snapshot (None at a sharded asset's base dir); `code_version` lets a
# logic change force a recompute. One GET, always consistent. See `should_skip`.
_MANIFEST = "_manifest"

# Subdir under an asset dir holding one JSON per check result. Written as each check
# runs so results outlive the throwaway instance; batch.py reads them into a run report.
_CHECKS_DIR = "_checks"


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
            config=Config(max_pool_connections=_READ_WORKERS),
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

    def _write_manifest(self, directory: str, key: Optional[str], code_version: Optional[str]) -> None:
        """Rewrite a directory's manifest: latest snapshot key + the code_version that produced it."""
        self._s3.put_object(
            Bucket=self.bucket_name,
            Key=f"{directory}/{_MANIFEST}",
            Body=json.dumps({"key": key, "code_version": code_version}).encode("utf-8"),
        )

    def _read_manifest(self, directory: str) -> Optional[dict]:
        """A directory's manifest dict, or None if it was never written / is unreadable."""
        try:
            obj = self._s3.get_object(Bucket=self.bucket_name, Key=f"{directory}/{_MANIFEST}")
        except self._s3.exceptions.ClientError:
            return None
        try:
            return json.loads(obj["Body"].read().decode("utf-8"))
        except ValueError:
            return None

    def _put_snapshot(self, directory: str, buffer: io.BytesIO, stamp: str, code_version: Optional[str] = None) -> str:
        """Upload a snapshot under ``directory`` at ``stamp`` and rewrite the directory manifest."""
        key = f"{directory}/{stamp}.parquet"
        self._s3.upload_fileobj(buffer, self.bucket_name, key)
        self._write_manifest(directory, key, code_version)
        return key

    def _resolve_latest(self, directory: str) -> str:
        """Snapshot key named by the directory's manifest (raises ClientError if the dir has none)."""
        obj = self._s3.get_object(Bucket=self.bucket_name, Key=f"{directory}/{_MANIFEST}")
        return json.loads(obj["Body"].read().decode("utf-8"))["key"]

    def latest_stamp(self, directory: str) -> Optional[str]:
        """Raw sortable stamp of the directory's latest snapshot, or None if absent.

        Stamps are fixed-width UTC strings (``%Y%m%dT%H%M%S_%fZ``), so they order
        lexicographically — comparing two stamps as plain strings is a valid
        "which is newer" test, which is what the change-detection skip relies on.
        """
        manifest = self._read_manifest(directory)
        key = manifest.get("key") if manifest else None
        return key.rsplit("/", 1)[-1].removesuffix(".parquet") if key else None

    def latest_timestamp(self, directory: str) -> Optional[datetime]:
        """Parse the timestamp of the directory's latest snapshot, or None if absent."""
        stamp = self.latest_stamp(directory)
        if stamp is None:
            return None
        return datetime.strptime(stamp, "%Y%m%dT%H%M%S_%fZ").replace(tzinfo=timezone.utc)

    def _shard_dirs(self, prefix: str) -> list[str]:
        """Immediate per-shard subdirectories under ``prefix`` (one S3 ``CommonPrefixes`` listing)."""
        paginator = self._s3.get_paginator("list_objects_v2")
        return [
            cp["Prefix"].rstrip("/")
            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=f"{prefix}/", Delimiter="/"
            )
            for cp in page.get("CommonPrefixes", [])
        ]

    def shard_keys(self, prefix: str) -> list[str]:
        """Names (last path segment) of the per-shard subdirs under ``prefix``.

        Used to re-register dynamic partitions from S3 when the Dagster instance is
        ephemeral and a sharded producer (e.g. ``h3_montreal_addresses``) skips its
        recompute but its r6 partitions still must exist for downstream consumers.
        """
        return [directory.rsplit("/", 1)[-1] for directory in self._shard_dirs(prefix)]

    def latest_stamp_under_prefix(self, prefix: str) -> Optional[str]:
        """Newest stamp across every per-shard subdir under ``prefix`` (None if no shards)."""
        stamps = [s for s in (self.latest_stamp(d) for d in self._shard_dirs(prefix)) if s]
        return max(stamps) if stamps else None

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

    # --- change-detection skip (serverless: all state lives in S3) -----------

    def _own_dir(self, context) -> str:
        """Directory holding this asset's output for the current run.

        The partition's shard dir for a partitioned run, else the base dir (which,
        for a sharded producer, is the parent of every shard subdir).
        """
        return self.asset_dir(context, context.partition_key if context.has_partition_key else None)

    def output_stamp(self, context) -> Optional[str]:
        """Stamp of this asset's own latest output, or None if it has never been written.

        Resolves a single snapshot, a partition's shard, or the newest stamp across
        all shards of a sharded asset, depending on how this asset writes.
        """
        if context.has_partition_key:
            return self.latest_stamp(self.asset_dir(context, context.partition_key))
        segmentation = context.assets_def.metadata_by_key[context.asset_key].get("segmentation")
        base = location_of(context.assets_def)
        if segmentation in (None, "snapshot"):
            return self.latest_stamp(base)
        return self.latest_stamp_under_prefix(base)

    def should_skip(self, context, upstreams, code_version: Optional[str] = None) -> bool:
        """Whether this asset can reuse its existing output instead of recomputing.

        True iff (a) it already has an output, (b) that output was produced by the
        same ``code_version``, and (c) every upstream's latest stamp is no newer
        than this asset's own output stamp — i.e. nothing upstream changed since.
        Each ``upstreams`` entry is a directory string, or a ``(directory, is_prefix)``
        tuple where ``is_prefix`` takes the newest stamp across the dir's shards.

        Because a bronze cache hit never rewrites its snapshot, an upstream stamp
        only advances on a real content change, so stamp dominance is a sound,
        DB-free "did my inputs change" test.
        """
        mine = self.output_stamp(context)
        if mine is None:
            return False

        provenance = self._read_manifest(self._own_dir(context)) or {}
        if provenance.get("code_version") != code_version:
            context.log.info(
                f"code_version changed ({provenance.get('code_version')!r} -> {code_version!r}); recomputing."
            )
            return False

        for entry in upstreams:
            directory, is_prefix = entry if isinstance(entry, tuple) else (entry, False)
            upstream = (
                self.latest_stamp_under_prefix(directory) if is_prefix else self.latest_stamp(directory)
            )
            if upstream is None or upstream > mine:
                context.log.info(
                    f"upstream {directory} changed (upstream={upstream}, mine={mine}); recomputing."
                )
                return False

        context.log.info(f"inputs unchanged since {mine}; reusing existing output.")
        return True

    def reemit_latest(self, context) -> dg.MaterializeResult:
        """Re-emit the existing output: stable DataVersion + cache-hit metadata, no S3 write.

        The manifest still names a valid snapshot, so downstream reads keep working;
        re-emitting the same DataVersion means no spurious invalidation.
        """
        stamp = self.output_stamp(context)
        try:
            if context.has_partition_key:
                self.describe_latest(context, self.asset_dir(context, context.partition_key))
            elif context.assets_def.metadata_by_key[context.asset_key].get("segmentation") in (None, "snapshot"):
                self.describe_latest(context, location_of(context.assets_def))
        except self._s3.exceptions.ClientError:
            pass  # sharded base dir has no snapshot of its own; the stable DataVersion is enough
        return dg.MaterializeResult(
            data_version=dg.DataVersion(stamp) if stamp else None,
            metadata={"skipped_unchanged": dg.MetadataValue.bool(True)},
        )

    def write_gpq(self, context, gdf: gpd.GeoDataFrame, code_version: Optional[str] = None) -> Optional[str]:
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
        directory = self.asset_dir(context, shard)
        s3_key = self._put_snapshot(directory, buffer, stamp, code_version)
        s3_path = self._base_path / s3_key
        context.log.info(f"Wrote snapshot {s3_path}")

        context.add_output_metadata(self._snapshot_metadata(s3_path, file_size, gdf))
        return stamp

    def write_gpq_partitioned(self, context, gdf: gpd.GeoDataFrame, column: str, code_version: Optional[str] = None) -> Optional[str]:
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
            self._put_snapshot(self.asset_dir(context, str(value)), buffer, stamp, code_version)
            written += 1

        # Base-dir manifest (no snapshot of its own) carries the code_version for the whole sharded asset.
        self._write_manifest(self.asset_dir(context), None, code_version)

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
        subdirs = self._shard_dirs(prefix)
        if not subdirs:
            raise FileNotFoundError(f"No partitions under s3://{self.bucket_name}/{prefix}/")

        def _read_shard(directory: str):
            key = self._resolve_latest(directory)
            obj = self._s3.get_object(Bucket=self.bucket_name, Key=key)
            return self._read_parquet_bytes(obj["Body"].read())

        # Shards are independent S3 reads, so fetch+parse them concurrently. `map`
        # preserves order, keeping the `frames[0]` CRS probe below deterministic.
        with ThreadPoolExecutor(max_workers=min(len(subdirs), _READ_WORKERS)) as pool:
            frames = list(pool.map(_read_shard, subdirs))

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

    def write_check_result(self, context, asset_location: str, check_name: str, result: dg.AssetCheckResult) -> None:
        """Persist a check result as ``{asset}/_checks/{check}.json``.

        The deployed batch runs on a throwaway instance, so check results would otherwise
        die with the task; this is their durable home, read back into one run report by batch.py.
        """
        payload = {
            "asset": asset_location,
            "check": check_name,
            "passed": bool(result.passed),
            "severity": result.severity.value if result.severity else None,
            "stamp": self._now_stamp(),
            "metadata": {k: getattr(v, "value", v) for k, v in (result.metadata or {}).items()},
        }
        self._s3.put_object(
            Bucket=self.bucket_name,
            Key=f"{asset_location}/{_CHECKS_DIR}/{check_name}.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        context.log.info(f"check {check_name} -> {'pass' if result.passed else 'FAIL'} ({asset_location})")


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
