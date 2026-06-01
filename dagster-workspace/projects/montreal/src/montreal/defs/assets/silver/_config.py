"""Silver layer contracts plus the constants and helpers its assets share."""

from dataclasses import dataclass
from typing import Dict

import dagster as dg
import geopandas as gpd
import h3pandas  # noqa: F401  -- registers the .h3 GeoDataFrame accessor


@dataclass(frozen=True)
class SilverAssetMetadata:
    layer: str
    data_category: str
    segmentation: str  # "snapshot" or shard column (e.g. "h3_r6")
    description: str


@dataclass(frozen=True)
class SilverAssetDataContract:
    """Shape validation; freshness handled by raw-asset sensor."""

    schema: Dict[str, str]
    uniqueness: tuple[str, ...]
    completeness: tuple[str, ...]


r6_partitions = dg.DynamicPartitionsDefinition(name="address_r6")

# Livability categories the distance + gold layers score against.
POI_CATEGORIES = ("grocery", "school", "health", "transit", "park", "bike")


def h3_index(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add h3_r10 column via representative point."""
    pts = gdf.copy()
    pts["geometry"] = gdf.geometry.representative_point()
    pts = pts.h3.geo_to_h3(resolution=10, set_index=False)
    return gdf.assign(h3_r10=pts["h3_10"].to_numpy())


def h3_linetrace(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """H3-cover line geometry; return h3_r10 column."""
    traced = gdf.h3.linetrace(resolution=10, explode=True)
    traced = traced[traced["h3_linetrace"].notna()].copy()
    return traced.rename(columns={"h3_linetrace": "h3_r10"})


def points_with_lat_lng(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reduce to representative points with lat/lng columns."""
    pts = gdf.geometry.representative_point()
    return gdf.assign(geometry=pts, lat=pts.y.to_numpy(dtype=float), lng=pts.x.to_numpy(dtype=float))
