"""Tests for the S3 lakehouse resource: path/format helpers, WGS84 normalization,
parquet read fallback, sharded-write reconciliation, and resource env binding."""

import io
import logging
import re
from pathlib import Path

import dagster as dg
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from upath import UPath

from montreal.defs.assets.bronze import montreal_pois
from montreal.defs.assets.silver.amenities import amenities
from montreal.defs.resources.lakehouse import format_size, location_of, s3_datastore
from montreal.defs.resources.lakehouse.frames import preview, read_parquet_bytes, to_wgs84
from montreal.defs.resources.lakehouse.paths import now_stamp




def test_location_of_is_layer_over_asset_name():
    assert location_of(montreal_pois) == "bronze/montreal_pois"
    assert location_of(amenities) == "silver/amenities"


def test_format_size_renders_megabytes():
    assert format_size(1024 * 1024) == "1.00 MB"


def test_now_stamp_is_a_sortable_fixed_width_utc_string():
    assert re.fullmatch(r"\d{8}T\d{6}_\d{6}Z", now_stamp())


def test_to_wgs84_normalizes_crs_and_passes_non_geo_through():
    no_crs = gpd.GeoDataFrame(geometry=[Point(-73.6, 45.5)])
    assert to_wgs84(no_crs).crs.to_epsg() == 4326

    projected = gpd.GeoDataFrame(geometry=[Point(-8_190_000, 5_690_000)], crs=3857)
    assert to_wgs84(projected).crs.to_epsg() == 4326

    plain = pd.DataFrame({"a": [1]})
    assert to_wgs84(plain) is plain


def test_read_parquet_bytes_falls_back_to_pandas_without_geometry():
    geo = gpd.GeoDataFrame({"a": [1]}, geometry=[Point(0, 0)], crs=4326)
    buf = io.BytesIO()
    geo.to_parquet(buf)
    assert isinstance(read_parquet_bytes(buf.getvalue()), gpd.GeoDataFrame)

    tabular = pd.DataFrame({"a": [1, 2]})
    buf = io.BytesIO()
    tabular.to_parquet(buf)
    out = read_parquet_bytes(buf.getvalue())
    assert isinstance(out, pd.DataFrame) and not isinstance(out, gpd.GeoDataFrame)


def test_preview_drops_geometry():
    geo = gpd.GeoDataFrame({"name": ["x"]}, geometry=[Point(0, 0)], crs=4326)
    md = preview(geo)
    assert "name" in md and "geometry" not in md




@dg.asset(name="sharded_fixture", metadata={"layer": "silver", "segmentation": "h3_r6"})
def sharded_fixture(): ...


class _Ctx:
    log = logging.getLogger("test_lakehouse")
    assets_def = sharded_fixture
    asset_key = sharded_fixture.key
    has_partition_key = False

    def add_output_metadata(self, metadata):
        pass


def _memory_store(root: str) -> s3_datastore:
    """Store over fsspec's memory filesystem -- S3-like (implicit dirs), no AWS."""
    store = s3_datastore(bucket_name="unused", region_name="unused")
    store._base = UPath(f"memory://{root}")
    return store


def _shard_frame(cells: list[str]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"h3_r6": cells}, geometry=[Point(-73.6, 45.5)] * len(cells), crs=4326
    )


def test_write_gpq_partitioned_removes_stale_shards():
    store, ctx = _memory_store("stale_shards"), _Ctx()
    base = location_of(sharded_fixture)
    (store._base / base / "_checks").mkdir(parents=True)  # meta dir, must survive

    store.write_gpq_partitioned(ctx, _shard_frame(["A", "A", "B"]), "h3_r6")
    assert set(store._shard_dirs(base)) == {f"{base}/A", f"{base}/B"}

    store.write_gpq_partitioned(ctx, _shard_frame(["A"]), "h3_r6")  # B vanished
    assert set(store._shard_dirs(base)) == {f"{base}/A"}
    assert (store._base / base / "_checks").exists()

    combined = store.read_gpq_prefix(ctx, base)
    assert list(combined["h3_r6"]) == ["A"]




def test_jobs_resolve_without_static_aws_keys(monkeypatch):
    """On Fargate the task role supplies credentials and no AWS_* env vars exist;
    run-config resolution must not demand them (EnvVar hard-fails when unset)."""
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("S3_BUCKET", "bucket")
    monkeypatch.setenv("S3_REGION", "ca-central-1")

    import montreal.definitions as md

    defs = dg.load_from_defs_folder(path_within_project=Path(md.__file__).parent)
    assert dg.validate_run_config(defs.get_job_def("pre_partition_job"))
