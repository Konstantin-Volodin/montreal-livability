"""S3-backed lakehouse resource: GeoDataFrames as timestamped Parquet snapshots.

All S3 access goes through a single ``UPath`` (fsspec/s3fs); paths derive from it
with ``/``, so the resource never touches boto3 directly.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional

import dagster as dg
import geopandas as gpd
import pandas as pd
from pydantic import PrivateAttr
from upath import UPath

from . import frames
from .paths import format_size, location_of, now_stamp, parse_stamp, stamp_of

# Threads for reading a sharded asset's per-shard snapshots in parallel.
_READ_WORKERS = 16

# Per-directory JSON {key}: `key` names the newest snapshot (None at a sharded asset's
# base dir). One GET, always consistent -- the pointer to "latest" without a LIST.
_MANIFEST = "_manifest"

# Legacy per-asset check-result subdir (results now live in the Dagster event log);
# still skipped when reading sharded assets so stale dirs aren't mistaken for data shards.
_CHECKS = "_checks"


class s3_datastore(dg.ConfigurableResource):
    """S3-backed data lakehouse for reading/writing GeoDataFrames as Parquet."""

    bucket_name: str
    region_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    _base: UPath = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        """Open the S3 root; blank keys fall back to the default credential chain (task role)."""
        self._base = UPath(
            f"s3://{self.bucket_name}/",
            key=self.aws_access_key_id or None,
            secret=self.aws_secret_access_key or None,
            client_kwargs={"region_name": self.region_name},
        )
        context.log.info(f"Lakehouse root {self._base} ({self.region_name})")

    def asset_dir(self, context, shard: Optional[str] = None) -> str:
        """Directory holding an asset's snapshots: ``{layer}/{asset}[/{shard}]``."""
        base = location_of(context.assets_def)
        return f"{base}/{shard}" if shard is not None else base

    def _write_manifest(self, directory: str, key: Optional[str]) -> None:
        (self._base / directory / _MANIFEST).write_text(json.dumps({"key": key}))

    def _read_manifest(self, directory: str) -> Optional[dict]:
        """A directory's manifest dict, or None if it was never written / is unreadable."""
        try:
            return json.loads((self._base / directory / _MANIFEST).read_text())
        except (FileNotFoundError, ValueError):
            return None

    def _put_snapshot(self, directory: str, data: bytes, stamp: str) -> str:
        """Write a snapshot under ``directory`` at ``stamp`` and rewrite the directory manifest. Returns its key."""
        key = f"{directory}/{stamp}.parquet"
        (self._base / key).write_bytes(data)
        self._write_manifest(directory, key)
        return key

    def _resolve_latest(self, directory: str) -> UPath:
        """Path of the snapshot named by the directory's manifest (raises if the dir has none)."""
        manifest = self._read_manifest(directory)
        if not manifest or not manifest.get("key"):
            raise FileNotFoundError(f"No snapshot under {self._base / directory}")
        return self._base / manifest["key"]

    def latest_stamp(self, directory: str) -> Optional[str]:
        """Raw sortable stamp of the directory's latest snapshot, or None if absent."""
        manifest = self._read_manifest(directory)
        key = manifest.get("key") if manifest else None
        return stamp_of(key) if key else None

    def latest_timestamp(self, directory: str) -> Optional[datetime]:
        """Parsed timestamp of the directory's latest snapshot, or None if absent."""
        stamp = self.latest_stamp(directory)
        return parse_stamp(stamp) if stamp else None

    def _shard_dirs(self, prefix: str) -> list[str]:
        """Relative dirs of the immediate per-shard subdirectories under ``prefix``."""
        try:
            children = list((self._base / prefix).iterdir())
        except FileNotFoundError:
            return []
        # _CHECKS is a sibling meta dir (per-asset check results), not a data shard.
        return [f"{prefix}/{p.name}" for p in children if p.is_dir() and p.name != _CHECKS]

    def _snapshot_metadata(self, path: UPath, file_size: int, gdf) -> dict:
        """Rich materialization metadata for a freshly written snapshot."""
        return {
            "s3_location": dg.MetadataValue.text(str(path)),
            "file_size": dg.MetadataValue.text(format_size(file_size)),
            "num_rows": dg.MetadataValue.int(len(gdf)),
            "schema": dg.MetadataValue.json({col: str(gdf[col].dtype) for col in gdf.columns}),
            "preview": dg.MetadataValue.md(frames.preview(gdf)),
        }

    def write_gpq(self, context, gdf: gpd.GeoDataFrame) -> Optional[str]:
        """Write a GeoDataFrame to S3 as a timestamped Parquet snapshot; return its stamp."""
        if context.has_partition_key and gdf is not None and not gdf.empty:
            column = context.assets_def.metadata_by_key[context.asset_key].get("segmentation")
            if not column or column not in gdf.columns:
                raise ValueError(
                    "Partitioned assets must set metadata['segmentation'] to the "
                    "GeoDataFrame column used for partitioning."
                )
            gdf = gdf[gdf[column] == context.partition_key]

        if gdf is None or gdf.empty:
            context.log.info("No data for this partition. Skipping write.")
            return None

        gdf = frames.to_wgs84(gdf)
        shard = context.partition_key if context.has_partition_key else None
        data = frames.to_parquet_bytes(gdf)

        stamp = now_stamp()
        path = self._base / self._put_snapshot(self.asset_dir(context, shard), data, stamp)
        context.log.info(f"Wrote snapshot {path}")
        context.add_output_metadata(self._snapshot_metadata(path, len(data), gdf))
        return stamp

    def write_gpq_partitioned(self, context, gdf: gpd.GeoDataFrame, column: str) -> Optional[str]:
        """Pre-shard a GeoDataFrame into one snapshot dir per distinct ``column`` value, so an r6-partitioned consumer reads only its slice. Returns the shared stamp."""
        if gdf is None or gdf.empty:
            context.log.info("No data. Skipping partitioned write.")
            return None

        gdf = frames.to_wgs84(gdf)
        stamp = now_stamp()
        written_dirs = set()
        for value, group in gdf.groupby(column, sort=False):
            directory = self.asset_dir(context, str(value))
            self._put_snapshot(directory, frames.to_parquet_bytes(group), stamp)
            written_dirs.add(directory)
        written = len(written_dirs)

        # A shard whose value vanished would otherwise be read forever by
        # read_gpq_prefix (and the contract checks); the writer owns the prefix.
        for stale in sorted(set(self._shard_dirs(self.asset_dir(context))) - written_dirs):
            path = self._base / stale
            path.fs.rm(path.path, recursive=True)
            context.log.info(f"Removed stale shard {path}")

        # Base dir has no snapshot of its own; an empty manifest marks the sharded asset.
        self._write_manifest(self.asset_dir(context), None)
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

    def read_gpq(self, context, address: str):
        """Read the latest snapshot of an asset dir (``{layer}/{asset}[/{shard}]``)."""
        path = self._resolve_latest(address)
        df = frames.read_parquet_bytes(path.read_bytes())
        context.log.info(f"Read latest snapshot {path}")
        return df

    def read_gpq_prefix(self, context, prefix: str) -> gpd.GeoDataFrame:
        """Concat the latest snapshot of every per-partition subdir under ``prefix`` (the viz layer reading an r6-partitioned asset whole)."""
        subdirs = self._shard_dirs(prefix)
        if not subdirs:
            raise FileNotFoundError(f"No partitions under {self._base / prefix}/")

        def _read(directory: str):
            return frames.read_parquet_bytes(self._resolve_latest(directory).read_bytes())

        # Shards are independent reads, so fetch them concurrently. `map` preserves
        # order, keeping the `parts[0]` CRS probe below deterministic.
        with ThreadPoolExecutor(max_workers=min(len(subdirs), _READ_WORKERS)) as pool:
            parts = list(pool.map(_read, subdirs))

        combined = pd.concat(parts, ignore_index=True)
        if isinstance(parts[0], gpd.GeoDataFrame):
            combined = gpd.GeoDataFrame(combined)
            if parts[0].crs is not None:
                combined = combined.set_crs(parts[0].crs, allow_override=True)
        context.log.info(f"read_gpq_prefix: {len(combined)} rows from {len(subdirs)} partitions under {prefix}")
        return combined

    def write_html(self, context, html: str) -> str:
        """Upload an HTML document for this asset to ``{layer}/{asset}.html``; return a stamp."""
        name = context.asset_key.path[-1]
        key = f"{location_of(context.assets_def)}.html"
        path = self._base / key

        # Set content_type  so the object renders inline (S3 console / static hosting) rather than downloading.
        with path.fs.open(path.path, "wb", ContentType="text/html") as f:
            f.write(html.encode("utf-8"))

        context.log.info(f"Uploaded HTML to {path}")
        context.add_output_metadata(
            {
                "s3_write_location": dg.MetadataValue.text(str(path)),
                "s3_key": dg.MetadataValue.text(key),
                "file_size": dg.MetadataValue.text(format_size(len(html.encode("utf-8")))),
                "preview": dg.MetadataValue.md(f"[{name}.html]({path})"),
            }
        )
        return now_stamp()
