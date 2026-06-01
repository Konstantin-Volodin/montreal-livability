"""Unit tests for the Overpass helpers behind the montreal_pois bronze asset."""

import geopandas as gpd
from shapely.geometry import Point

from montreal.defs.assets.bronze import pois
from montreal.defs.assets.silver.h3.osm_pois import _POI_CATEGORIES


def test_tags_by_key_merges_categories_sharing_a_key():
    assert set(pois._TAGS_BY_KEY) == {"shop", "amenity"}
    assert "supermarket" in pois._TAGS_BY_KEY["shop"]
    assert {"school", "hospital"} <= pois._TAGS_BY_KEY["amenity"]


def test_overpass_query_is_well_formed():
    query = pois._overpass_query(pois._MONTREAL_BBOX)
    minx, miny, maxx, maxy = pois._MONTREAL_BBOX

    assert query.startswith("[out:json][timeout:180];")
    assert query.endswith("out tags center;")
    assert query.count("nwr[") == len(pois._TAGS_BY_KEY)
    assert f"({miny},{minx},{maxy},{maxx});" in query


def test_fclass_returns_raw_value_for_matches_else_none():
    assert pois._fclass({"shop": "supermarket"}) == "supermarket"
    assert pois._fclass({"amenity": "hospital"}) == "hospital"
    assert pois._fclass({"amenity": "bench"}) is None  # not a livability value
    assert pois._fclass({}) is None


def test_lon_lat_reads_node_center_or_nothing():
    assert pois._lon_lat({"lon": -73.5, "lat": 45.5}) == (-73.5, 45.5)
    assert pois._lon_lat({"center": {"lon": -73.5, "lat": 45.5}}) == (-73.5, 45.5)
    assert pois._lon_lat({}) == (None, None)


def test_elements_to_points_filters_and_dedupes():
    elements = [
        {"type": "node", "id": 1, "lon": -73.5, "lat": 45.5, "tags": {"shop": "supermarket", "name": "A"}},
        {"type": "node", "id": 1, "lon": -73.5, "lat": 45.5, "tags": {"shop": "supermarket", "name": "A"}},
        {"type": "node", "id": 2, "lon": -73.6, "lat": 45.6, "tags": {"amenity": "bench"}},
        {"type": "way", "id": 3, "tags": {"amenity": "school"}},
        {"type": "way", "id": 4, "center": {"lon": -73.7, "lat": 45.7}, "tags": {"amenity": "school"}},
    ]

    gdf = pois._elements_to_points(elements)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs == "EPSG:4326"
    assert list(gdf.columns) == ["osm_type", "osm_id", "name", "fclass", "geometry"]
    assert sorted(gdf["osm_id"]) == [1, 4]
    assert gdf.loc[gdf["osm_id"] == 4, "geometry"].iloc[0] == Point(-73.7, 45.7)


def test_elements_to_points_handles_empty():
    gdf = pois._elements_to_points([])
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 0


def test_silver_categories_derive_from_bronze_tag_map():
    assert _POI_CATEGORIES == {
        category: set().union(*groups.values())
        for category, groups in pois.OSM_POI_TAGS.items()
    }
