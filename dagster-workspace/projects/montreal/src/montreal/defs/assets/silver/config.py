"""Silver layer contracts plus the constants and helpers its assets share."""

from dataclasses import dataclass
from typing import Dict, Sequence

import dagster as dg
import geopandas as gpd
import h3pandas


@dataclass(frozen=True)
class SilverAssetMetadata:
    layer: str
    data_category: str
    segmentation: str   # "snapshot" for a single snapshot, else the shard column (e.g. "h3_r6")
    description: str


@dataclass(frozen=True)
class SilverAssetDataContract:
    """Derived silver assets validate shape only (no freshness - that moves to a raw-asset sensor)."""

    schema: Dict[str, str]        # column -> expected kind ("numeric"|"str"|"geometry")
    uniqueness: Sequence[str]     # columns a row must be unique over
    completeness: Sequence[str]   # columns that must be non-null


# dynamic r6 partitions for the address layer
r6_partitions = dg.DynamicPartitionsDefinition(name="address_r6")

# Livability categories the distance + gold layers score against.
POI_CATEGORIES = ("grocery", "school", "health", "transit", "park", "bike")


def h3_index(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add the h3_r10 analysis column."""
    pts = gdf.copy()
    pts["geometry"] = gdf.geometry.representative_point()

    pts = pts.h3.geo_to_h3(resolution=10, set_index=False)  # -> column "h3_10"

    # to_numpy() makes the assignment position-based, immune to index reshuffling.
    return gdf.assign(h3_r10=pts["h3_10"].to_numpy())


def h3_linetrace(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """H3-cover line geometry (bike paths) and add the h3_r10 column."""
    traced = gdf.h3.linetrace(resolution=10, explode=True)  # adds "h3_linetrace"
    traced = traced[traced["h3_linetrace"].notna()].copy()
    return traced.rename(columns={"h3_linetrace": "h3_r10"})


def points_with_lat_lng(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Collapse geometry to a representative point and expose lat/lng columns."""
    points = gdf.geometry.representative_point()
    return gdf.assign(
        geometry=points,
        lat=points.y.to_numpy(dtype=float),
        lng=points.x.to_numpy(dtype=float),
    )
