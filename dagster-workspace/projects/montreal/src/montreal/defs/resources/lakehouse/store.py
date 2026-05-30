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

# Per-directory JSON {key, code_version}: `key` names the newest snapshot (None at a
# sharded asset's base dir); `code_version` lets a logic change force a recompute.
# One GET, always consistent. See `should_skip`.
_MANIFEST = "_manifest"

# Subdir under an asset dir holding one JSON per check result; batch.py reads them into a run report.
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

    # --- directories & manifests ---------------------------------------------

    def asset_dir(self, context, shard: Optional[str] = None) -> str:
        """Directory holding an asset's snapshots: ``{layer}/{asset}[/{shard}]``."""
        base = location_of(context.assets_def)
        return f"{base}/{shard}" if shard is not None else base

    def _write_manifest(self, directory: str, key: Optional[str], code_version: Optional[str]) -> None:
        (self._base / directory / _MANIFEST).write_text(json.dumps({"key": key, "code_version": code_version}))

    def _read_manifest(self, directory: str) -> Optional[dict]:
        """A directory's manifest dict, or None if it was never written / is unreadable."""
        try:
            return json.loads((self._base / directory / _MANIFEST).read_text())
        except (FileNotFoundError, ValueError):
            return None

    def _put_snapshot(self, directory: str, data: bytes, stamp: str, code_version: Optional[str]) -> str:
        """Write a snapshot under ``directory`` at ``stamp`` and rewrite the directory manifest. Returns its key."""
        key = f"{directory}/{stamp}.parquet"
        (self._base / key).write_bytes(data)
        self._write_manifest(directory, key, code_version)
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
        return [f"{prefix}/{p.name}" for p in children if p.is_dir()]

    def shard_keys(self, prefix: str) -> list[str]:
        """Names (last segment) of the per-shard subdirs under ``prefix``.

        Used to re-register dynamic partitions from S3 when the Dagster instance is
        ephemeral and a sharded producer (e.g. ``h3_montreal_addresses``) skips its
        recompute but its r6 partitions still must exist for downstream consumers.
        """
        return [d.rsplit("/", 1)[-1] for d in self._shard_dirs(prefix)]

    def latest_stamp_under_prefix(self, prefix: str) -> Optional[str]:
        """Newest stamp across every per-shard subdir under ``prefix`` (None if no shards)."""
        stamps = [s for s in (self.latest_stamp(d) for d in self._shard_dirs(prefix)) if s]
        return max(stamps) if stamps else None

    # --- metadata ------------------------------------------------------------

    def _snapshot_metadata(self, path: UPath, file_size: int, gdf) -> dict:
        """Rich materialization metadata for a snapshot, shared by fresh writes and cache hits."""
        return {
            "s3_location": dg.MetadataValue.text(str(path)),
            "file_size": dg.MetadataValue.text(format_size(file_size)),
            "num_rows": dg.MetadataValue.int(len(gdf)),
            "schema": dg.MetadataValue.json({col: str(gdf[col].dtype) for col in gdf.columns}),
            "preview": dg.MetadataValue.md(frames.preview(gdf)),
        }

    def describe_latest(self, context, directory: str) -> None:
        """Emit metadata for a directory's latest snapshot, reading but never rewriting it (cache hits)."""
        path = self._resolve_latest(directory)
        raw = path.read_bytes()
        context.add_output_metadata(self._snapshot_metadata(path, len(raw), frames.read_parquet_bytes(raw)))

    # --- this asset's own output (queried by the change-detection skip) ------

    def _own_dir(self, context) -> str:
        """Directory holding this asset's output for the current run (partition shard, else base dir)."""
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

    # --- writes & reads ------------------------------------------------------

    def write_gpq(self, context, gdf: gpd.GeoDataFrame, code_version: Optional[str] = None) -> Optional[str]:
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
        path = self._base / self._put_snapshot(self.asset_dir(context, shard), data, stamp, code_version)
        context.log.info(f"Wrote snapshot {path}")
        context.add_output_metadata(self._snapshot_metadata(path, len(data), gdf))
        return stamp

    def write_gpq_partitioned(self, context, gdf: gpd.GeoDataFrame, column: str, code_version: Optional[str] = None) -> Optional[str]:
        """Pre-shard a GeoDataFrame into one snapshot dir per distinct ``column`` value, so an r6-partitioned consumer reads only its slice. Returns the shared stamp."""
        if gdf is None or gdf.empty:
            context.log.info("No data. Skipping partitioned write.")
            return None

        gdf = frames.to_wgs84(gdf)
        stamp = now_stamp()
        for value, group in gdf.groupby(column, sort=False):
            self._put_snapshot(self.asset_dir(context, str(value)), frames.to_parquet_bytes(group), stamp, code_version)
        written = int(gdf[column].nunique())

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
        # Set ContentType so the object renders inline (S3 console / static hosting) rather than downloading.
        with path.fs.open(path.path, "wb", s3_additional_kwargs={"ContentType": "text/html"}) as f:
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
            "stamp": now_stamp(),
            "metadata": {k: getattr(v, "value", v) for k, v in (result.metadata or {}).items()},
        }
        (self._base / asset_location / _CHECKS / f"{check_name}.json").write_text(json.dumps(payload, default=str))
        context.log.info(f"check {check_name} -> {'pass' if result.passed else 'FAIL'} ({asset_location})")
