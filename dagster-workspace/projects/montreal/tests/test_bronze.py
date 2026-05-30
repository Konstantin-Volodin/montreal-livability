"""Tests for the bronze layer: the raw_geo_asset factory's cache logic, the
contract->checks wiring, and per-module contract sanity."""

from datetime import datetime, timedelta, timezone

import dagster as dg
import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from montreal.defs.assets.bronze import (
    addresses,
    bike_paths,
    municipality_boundaries,
    parks,
    pois,
    transit_stops,
)
from montreal.defs.assets.bronze._config import (
    BronzeAssetDataContract,
    BronzeAssetMetadata,
    raw_geo_asset,
)
from montreal.defs.checks.factory import _dtype_matches, standard_checks
from montreal.defs.resources.lakehouse import s3_datastore

BRONZE_MODULES = [
    addresses,
    bike_paths,
    municipality_boundaries,
    parks,
    pois,
    transit_stops,
]

META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="example.test",
    description="fixture asset",
    url="https://example.test/data.geojson",
)
CONTRACT = BronzeAssetDataContract(
    schema={"osm_id": "numeric", "name": "str", "geometry": "geometry"},
    uniqueness=("osm_id",),
    completeness=("osm_id", "geometry"),
    freshness={"max_days": 25},
)

# Side channels the fake store writes to (module-level so they survive the copy
# Dagster makes of a resource for execution).
_CALLS: list[str] = []
_READ_FRAME: gpd.GeoDataFrame | None = None


class FakeStore(s3_datastore):
    """s3_datastore stand-in: no boto, with the snapshot age and read frame
    driven by the test instead of S3."""

    latest_age_days: int | None = None  # None => directory has no snapshot yet

    def setup_for_execution(self, context) -> None:  # skip the boto client
        pass

    def asset_dir(self, context, shard=None):
        return f"bronze/{context.asset_key.path[-1]}"

    def latest_timestamp(self, directory):
        if self.latest_age_days is None:
            return None
        return datetime.now(timezone.utc) - timedelta(days=self.latest_age_days)

    def latest_stamp(self, directory):
        # Only the fresh-snapshot re-emit reads this; use it as the cache-hit sentinel.
        _CALLS.append("reuse")
        return "STAMP"

    def write_gpq(self, context, gdf):
        _CALLS.append("write")
        return "STAMP"

    def read_gpq(self, context, address):
        return _READ_FRAME

    def write_check_result(self, context, asset_location, check_name, result):  # no S3 in tests
        pass


def _build(fetch, store):
    """Materialize a freshly-built raw_geo_asset against `store`, returning the result."""
    asset, _checks = raw_geo_asset("montreal_fixture", META, CONTRACT, fetch=fetch)
    return dg.materialize([asset], resources={"s3_datastore": store})


# --- factory wiring -------------------------------------------------------


def test_raw_geo_asset_wires_metadata_group_and_three_checks():
    asset, checks = raw_geo_asset("montreal_fixture", META, CONTRACT, fetch=lambda ctx: None)

    assert asset.key.path[-1] == "montreal_fixture"
    assert asset.group_names_by_key[asset.key] == "raw_data"
    metadata = asset.metadata_by_key[asset.key]
    assert metadata["layer"] == "bronze"
    assert metadata["source"] == META.source

    names = {key.name for c in checks for key in c.check_keys}
    assert names == {"schema_contract", "row_uniqueness", "field_completeness"}


def test_standard_checks_skips_value_range_without_bounds():
    # Bronze contracts carry no `bounds`, so value_range must not be added.
    checks = standard_checks(*_dummy_asset_and_contract())
    names = {key.name for c in checks for key in c.check_keys}
    assert names == {"schema_contract", "row_uniqueness", "field_completeness"}


def _dummy_asset_and_contract():
    asset, _ = raw_geo_asset("montreal_fixture", META, CONTRACT, fetch=lambda ctx: None)
    return asset, CONTRACT


# --- cache logic ----------------------------------------------------------


def test_fresh_snapshot_is_reused_without_fetching():
    _CALLS.clear()
    fetched = []
    result = _build(
        fetch=lambda ctx: fetched.append("called"),
        store=FakeStore(bucket_name="b", region_name="r", latest_age_days=1),
    )
    assert result.success
    assert _CALLS == ["reuse"]  # reused, never wrote
    assert fetched == []  # fetch was skipped


def test_stale_snapshot_triggers_fetch_and_write():
    _CALLS.clear()
    fetched = []

    def fetch(ctx):
        fetched.append("called")
        return gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=4326)

    # 400d old, contract freshness is 25d -> stale.
    result = _build(fetch, FakeStore(bucket_name="b", region_name="r", latest_age_days=400))
    assert result.success
    assert _CALLS == ["write"]
    assert fetched == ["called"]


def test_missing_snapshot_triggers_fetch_and_write():
    _CALLS.clear()
    fetched = []

    def fetch(ctx):
        fetched.append("called")
        return gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=4326)

    # latest_age_days=None -> latest_timestamp returns None -> never cached.
    result = _build(fetch, FakeStore(bucket_name="b", region_name="r", latest_age_days=None))
    assert result.success
    assert _CALLS == ["write"]
    assert fetched == ["called"]


# --- dtype matching (what schema_contract enforces) -----------------------


def test_dtype_matches_per_kind():
    assert _dtype_matches(pd.Series([1, 2, 3]), "numeric")
    assert _dtype_matches(pd.Series(["a", "b"]), "str")
    assert _dtype_matches(gpd.GeoSeries([Point(0, 0)]), "geometry")
    assert not _dtype_matches(pd.Series([1, 2]), "str")
    assert not _dtype_matches(pd.Series(["a"]), "numeric")
    with pytest.raises(ValueError):
        _dtype_matches(pd.Series([1]), "bogus")


# --- contract checks against real frames ----------------------------------


def _run_checks_over(frame: gpd.GeoDataFrame) -> dict[str, bool]:
    global _READ_FRAME
    _CALLS.clear()
    _READ_FRAME = frame
    asset, checks = raw_geo_asset("montreal_fixture", META, CONTRACT, fetch=lambda ctx: None)
    # Fresh snapshot so the asset reemits; the checks read `frame` via read_gpq.
    result = dg.materialize(
        [asset, *checks],
        resources={"s3_datastore": FakeStore(bucket_name="b", region_name="r", latest_age_days=1)},
    )
    assert result.success  # checks fail soft (return passed=False), they don't error
    return {ev.check_name: ev.passed for ev in result.get_asset_check_evaluations()}


def test_contract_checks_pass_on_clean_frame():
    clean = gpd.GeoDataFrame(
        {"osm_id": [1, 2], "name": ["a", "b"]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs=4326,
    )
    results = _run_checks_over(clean)
    assert results == {"schema_contract": True, "row_uniqueness": True, "field_completeness": True}


def test_contract_checks_catch_duplicates_and_nulls():
    dirty = gpd.GeoDataFrame(
        {"osm_id": [1, 1, None], "name": ["a", "b", "c"]},
        geometry=[Point(0, 0), Point(0, 0), Point(2, 2)],
        crs=4326,
    )
    results = _run_checks_over(dirty)
    assert results["schema_contract"] is True  # columns/types still fine
    assert results["row_uniqueness"] is False  # osm_id 1 repeated
    assert results["field_completeness"] is False  # osm_id has a null


# --- per-module contract sanity -------------------------------------------


def _asset_of(module):
    return next(v for v in vars(module).values() if isinstance(v, dg.AssetsDefinition))


@pytest.mark.parametrize("module", BRONZE_MODULES, ids=lambda m: m.__name__.split(".")[-1])
def test_each_bronze_module_has_a_coherent_contract(module):
    meta = module.ASSET_META
    contract = module.ASSET_DATA_CONTRACT
    asset = _asset_of(module)

    assert meta.layer == "bronze"
    assert meta.url  # something to fetch from

    schema_cols = set(contract.schema)
    assert contract.schema.get("geometry") == "geometry"  # every bronze asset is geospatial
    assert set(contract.uniqueness) <= schema_cols  # keys must be declared columns
    assert set(contract.completeness) <= schema_cols
    assert contract.freshness["max_days"] > 0

    # The module ships exactly the three standard checks, bound to this asset.
    names = {key.name for c in module.checks for key in c.check_keys}
    assert names == {"schema_contract", "row_uniqueness", "field_completeness"}
    assert all(key.asset_key == asset.key for c in module.checks for key in c.check_keys)


def test_missing_contract_field_fails_at_construction():
    # All four fields are required; omitting one must raise (the import-time guard).
    with pytest.raises(TypeError):
        BronzeAssetDataContract(
            schema={"geometry": "geometry"},
            uniqueness=("geometry",),
            completeness=("geometry",),
        )  # no freshness
