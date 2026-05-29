"""Tests for the S3 lakehouse resource: path/format helpers, WGS84 normalization,
parquet read fallback, and the change-detection skip that the serverless caching
relies on (its only durable state is S3, so the skip logic is load-bearing)."""

import io
import re

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from montreal.defs.assets.bronze import montreal_pois
from montreal.defs.assets.silver.amenities import amenities
from montreal.defs.resources.lakehouse import format_size, location_of, s3_datastore

OLD = "20240101T000000_000000Z"
NEW = "20240601T000000_000000Z"


# --- pure helpers ---------------------------------------------------------


def test_location_of_is_layer_over_asset_name():
    assert location_of(montreal_pois) == "bronze/montreal_pois"
    assert location_of(amenities) == "silver/amenities"


def test_format_size_renders_megabytes():
    assert format_size(1024 * 1024) == "1.00 MB"


def test_now_stamp_is_a_sortable_fixed_width_utc_string():
    assert re.fullmatch(r"\d{8}T\d{6}_\d{6}Z", s3_datastore._now_stamp())


def test_to_wgs84_normalizes_crs_and_passes_non_geo_through():
    # missing crs is assumed to be 4326.
    no_crs = gpd.GeoDataFrame(geometry=[Point(-73.6, 45.5)])
    assert s3_datastore._to_wgs84(no_crs).crs.to_epsg() == 4326

    # a projected frame is reprojected.
    projected = gpd.GeoDataFrame(geometry=[Point(-8_190_000, 5_690_000)], crs=3857)
    assert s3_datastore._to_wgs84(projected).crs.to_epsg() == 4326

    # a plain DataFrame is returned untouched.
    plain = pd.DataFrame({"a": [1]})
    assert s3_datastore._to_wgs84(plain) is plain


def test_read_parquet_bytes_falls_back_to_pandas_without_geometry():
    geo = gpd.GeoDataFrame({"a": [1]}, geometry=[Point(0, 0)], crs=4326)
    buf = io.BytesIO()
    geo.to_parquet(buf)
    assert isinstance(s3_datastore._read_parquet_bytes(buf.getvalue()), gpd.GeoDataFrame)

    tabular = pd.DataFrame({"a": [1, 2]})
    buf = io.BytesIO()
    tabular.to_parquet(buf)
    out = s3_datastore._read_parquet_bytes(buf.getvalue())
    assert isinstance(out, pd.DataFrame) and not isinstance(out, gpd.GeoDataFrame)


def test_gpq_preview_drops_geometry():
    store = s3_datastore(bucket_name="b", region_name="r")
    geo = gpd.GeoDataFrame({"name": ["x"]}, geometry=[Point(0, 0)], crs=4326)
    preview = store.gpq_preview(geo)
    assert "name" in preview and "geometry" not in preview


# --- change-detection skip ------------------------------------------------


class _Ctx:
    class _Log:
        def info(self, *a, **k):
            pass

    log = _Log()


class SkipStore(s3_datastore):
    """Drives should_skip from test-set stamps instead of S3."""

    own_stamp: str | None = None
    provenance_version: str | None = None
    upstream_stamps: dict[str, str | None] = {}

    def setup_for_execution(self, context) -> None:
        pass

    def output_stamp(self, context):
        return self.own_stamp

    def _own_dir(self, context):
        return "self"

    def _read_manifest(self, directory):
        return {"code_version": self.provenance_version}

    def latest_stamp(self, directory):
        return self.upstream_stamps.get(directory)

    def latest_stamp_under_prefix(self, directory):
        return self.upstream_stamps.get(directory)


def _store(**kwargs) -> SkipStore:
    return SkipStore(bucket_name="b", region_name="r", **kwargs)


def test_skip_false_when_asset_has_no_output_yet():
    store = _store(own_stamp=None)
    assert store.should_skip(_Ctx(), ["bronze/x"], code_version="1") is False


def test_skip_false_when_code_version_changed():
    store = _store(own_stamp=NEW, provenance_version="0", upstream_stamps={"bronze/x": OLD})
    assert store.should_skip(_Ctx(), ["bronze/x"], code_version="1") is False


def test_skip_false_when_an_upstream_is_missing():
    store = _store(own_stamp=NEW, provenance_version="1", upstream_stamps={"bronze/x": None})
    assert store.should_skip(_Ctx(), ["bronze/x"], code_version="1") is False


def test_skip_false_when_an_upstream_is_newer():
    store = _store(own_stamp=OLD, provenance_version="1", upstream_stamps={"bronze/x": NEW})
    assert store.should_skip(_Ctx(), ["bronze/x"], code_version="1") is False


def test_skip_true_when_inputs_unchanged():
    store = _store(own_stamp=NEW, provenance_version="1", upstream_stamps={"bronze/x": OLD})
    assert store.should_skip(_Ctx(), ["bronze/x"], code_version="1") is True


def test_skip_handles_prefix_upstreams_via_shard_max():
    # a (directory, is_prefix=True) entry takes the newest stamp across shards.
    store = _store(own_stamp=NEW, provenance_version="1", upstream_stamps={"silver/sharded": OLD})
    assert store.should_skip(_Ctx(), [("silver/sharded", True)], code_version="1") is True
