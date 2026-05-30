"""Tests for the S3 lakehouse resource: path/format helpers, WGS84 normalization,
parquet read fallback, and check-result persistence."""

import io
import json
import re

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


# --- pure helpers ---------------------------------------------------------


class _Ctx:
    class _Log:
        def info(self, *a, **k):
            pass

    log = _Log()


def test_location_of_is_layer_over_asset_name():
    assert location_of(montreal_pois) == "bronze/montreal_pois"
    assert location_of(amenities) == "silver/amenities"


def test_format_size_renders_megabytes():
    assert format_size(1024 * 1024) == "1.00 MB"


def test_now_stamp_is_a_sortable_fixed_width_utc_string():
    assert re.fullmatch(r"\d{8}T\d{6}_\d{6}Z", now_stamp())


def test_to_wgs84_normalizes_crs_and_passes_non_geo_through():
    # missing crs is assumed to be 4326.
    no_crs = gpd.GeoDataFrame(geometry=[Point(-73.6, 45.5)])
    assert to_wgs84(no_crs).crs.to_epsg() == 4326

    # a projected frame is reprojected.
    projected = gpd.GeoDataFrame(geometry=[Point(-8_190_000, 5_690_000)], crs=3857)
    assert to_wgs84(projected).crs.to_epsg() == 4326

    # a plain DataFrame is returned untouched.
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


def test_write_check_result_persists_normalized_json():
    store = s3_datastore(bucket_name="b", region_name="r")
    store._base = UPath("memory://lakehouse/")  # in-process fsspec backend, no S3 needed
    result = dg.AssetCheckResult(
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"duplicate_rows": 3, "subset": dg.MetadataValue.json(["ID_UEV"])},
    )
    store.write_check_result(_Ctx(), "silver/amenities", "row_uniqueness", result)

    written = (store._base / "silver/amenities/_checks/row_uniqueness.json").read_text()
    payload = json.loads(written)
    assert payload["passed"] is False and payload["severity"] == "ERROR"
    # MetadataValue and raw values both land as plain JSON.
    assert payload["metadata"]["duplicate_rows"] == 3
    assert payload["metadata"]["subset"] == ["ID_UEV"]
