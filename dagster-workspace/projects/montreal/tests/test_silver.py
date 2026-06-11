"""Tests for the silver layer: the shared h3/geo helpers, the distance search,
the amenity reshape, and per-module contract sanity."""

import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import LineString, Point

from montreal.defs.assets.silver import amenities, distances, municipalities
from montreal.defs.assets.silver._config import (
    POI_CATEGORIES,
    h3_index,
    h3_linetrace,
    points_with_lat_lng,
    r6_partitions,
)
from montreal.defs.assets.silver.distances import distances_to_amenities, haversine, nearest
from montreal.defs.checks.factory import _read_checked
from montreal.defs.assets.silver.h3 import (
    addresses as h3_addresses,
    bike_paths as h3_bike_paths,
    osm_pois as h3_osm_pois,
    parks as h3_parks,
    transit_stops as h3_transit_stops,
)

SILVER_MODULES = [
    h3_addresses,
    h3_bike_paths,
    h3_osm_pois,
    h3_parks,
    h3_transit_stops,
    municipalities,
    amenities,
    distances,
]

MONTREAL = (45.5, -73.6)  # (lat, lng)




def test_h3_index_adds_r10_cell_for_each_point():
    lat, lng = MONTREAL
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[Point(lng, lat)], crs=4326)
    out = h3_index(gdf)
    assert out["h3_r10"].iloc[0] == h3.latlng_to_cell(lat, lng, 10)
    assert "id" in out.columns  # original columns preserved


def test_h3_linetrace_covers_a_line_with_r10_cells():
    line = LineString([(-73.60, 45.50), (-73.55, 45.52)])
    gdf = gpd.GeoDataFrame({"ID_CYCL": [7]}, geometry=[line], crs=4326)
    out = h3_linetrace(gdf)
    assert "h3_r10" in out.columns
    assert len(out) >= 1  # one row per covered cell (explode=True)
    assert out["h3_r10"].notna().all()
    assert (out["ID_CYCL"] == 7).all()  # parent id carried onto every cell row


def test_points_with_lat_lng_exposes_geometry_coordinates():
    lat, lng = MONTREAL
    gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[Point(lng, lat)], crs=4326)
    out = points_with_lat_lng(gdf)
    assert out["lat"].iloc[0] == pytest.approx(lat)
    assert out["lng"].iloc[0] == pytest.approx(lng)


def test_poi_categories_and_partition_name_are_stable():
    assert POI_CATEGORIES == ("grocery", "school", "health", "transit", "park", "bike")
    assert r6_partitions.name == "address_r6"




def test_haversine_zero_for_identical_points_and_known_arc():
    z = np.array([0.0])
    assert haversine(z, z, z, z)[0] == 0.0
    d = haversine(np.array([0.0]), np.array([0.0]), np.array([1.0]), np.array([0.0]))
    assert d[0] == pytest.approx(111_319, rel=1e-3)


def test_haversine_east_west_arc_shrinks_with_latitude():
    # 1° of longitude at 45.5°N spans cos(45.5°) of an equatorial degree. Swapping
    # the lat/lng argument roles would put the cos factor on the wrong coordinate.
    lat, lng = MONTREAL
    d = haversine(np.array([lng]), np.array([lat]), np.array([lng + 1.0]), np.array([lat]))
    assert d[0] == pytest.approx(111_319 * np.cos(np.radians(lat)), rel=1e-2)


def test_nearest_returns_true_meridian_distance():
    # Amenity 0.005° due north of the address: ~557 m at any longitude. Feeding
    # nearest()'s lat/lng columns to haversine in the wrong order would scale
    # this by cos(73.6°) and report ~157 m.
    lat, lng = MONTREAL
    addresses = pd.DataFrame(
        {"h3_r10": [h3.latlng_to_cell(lat, lng, 10)], "lat": [lat], "lng": [lng]}
    )
    amenity_lat = lat + 0.005
    amenity = pd.DataFrame(
        {
            "category": ["grocery"],
            "h3_r10": [h3.latlng_to_cell(amenity_lat, lng, 10)],
            "lat": [amenity_lat],
            "lng": [lng],
        }
    )

    out = nearest(addresses, amenity)

    assert out["dist_grocery"].iloc[0] == pytest.approx(556.6, rel=2e-2)


def test_nearest_finds_same_cell_amenity_and_leaves_empty_categories_nan():
    lat, lng = MONTREAL
    cell = h3.latlng_to_cell(lat, lng, 10)
    addresses = pd.DataFrame({"h3_r10": [cell], "lat": [lat], "lng": [lng]})
    amenity = pd.DataFrame(
        {"category": ["grocery"], "h3_r10": [cell], "lat": [lat + 1e-4], "lng": [lng + 1e-4]}
    )

    out = nearest(addresses, amenity)

    assert set(out.columns) == {f"dist_{c}" for c in POI_CATEGORIES}
    assert np.isfinite(out["dist_grocery"].iloc[0])
    assert out["dist_grocery"].iloc[0] < 200
    assert np.isnan(out["dist_school"].iloc[0])




def _candidate_frame() -> gpd.GeoDataFrame:
    lat, lng = MONTREAL
    return gpd.GeoDataFrame(
        {"category": ["grocery"], "h3_r10": [h3.latlng_to_cell(lat, lng, 10)]},
        geometry=[Point(lng, lat)],
        crs=4326,
    )


def test_amenity_frame_keeps_own_category_when_none_given():
    out = amenities._amenity_frame(_candidate_frame())
    assert list(out.columns) == ["category", "h3_r10", "lat", "lng", "geometry"]
    assert out["category"].iloc[0] == "grocery"


def test_amenity_frame_overrides_category_when_given():
    out = amenities._amenity_frame(_candidate_frame(), "transit")
    assert (out["category"] == "transit").all()




class _RecordingStore:
    def __init__(self):
        self.reads: list[tuple[str, str]] = []

    def read_gpq(self, context, address):
        self.reads.append(("read_gpq", address))

    def read_gpq_prefix(self, context, prefix):
        self.reads.append(("read_gpq_prefix", prefix))


class _Ctx:
    def __init__(self, partition_key=None):
        self.has_partition_key = partition_key is not None
        self.partition_key = partition_key


def test_partitioned_check_reads_only_its_partition_shard():
    store = _RecordingStore()
    _read_checked(_Ctx(partition_key="861f1d8c7"), store, distances_to_amenities)
    assert store.reads == [("read_gpq", "silver/distances_to_amenities/861f1d8c7")]


def test_unpartitioned_sharded_check_reads_all_shards():
    store = _RecordingStore()
    _read_checked(_Ctx(), store, distances_to_amenities)
    assert store.reads == [("read_gpq_prefix", "silver/distances_to_amenities")]




@pytest.mark.parametrize("module", SILVER_MODULES, ids=lambda m: m.__name__.split(".")[-1])
def test_each_silver_module_has_a_coherent_contract(module):
    meta = module.ASSET_META
    contract = module.ASSET_DATA_CONTRACT

    assert meta.layer == "silver"
    schema_cols = set(contract.schema)
    assert set(contract.uniqueness) <= schema_cols
    assert set(contract.completeness) <= schema_cols
    assert not hasattr(contract, "bounds")

    names = {key.name for c in module.checks for key in c.check_keys}
    assert names == {"schema_contract", "row_uniqueness", "field_completeness"}
    asset_keys = {key.asset_key for c in module.checks for key in c.check_keys}
    assert len(asset_keys) == 1
