"""Unify per-category amenity points into candidate points for nearest-distance search."""

from dataclasses import asdict

import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets.silver._config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    points_with_lat_lng,
)
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore
from montreal.defs.assets.silver.h3 import (
    h3_montreal_bike_paths,
    h3_montreal_osm_pois,
    h3_montreal_parks,
    h3_montreal_transit_stops,
)

ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="Amenity candidate points (grocery/school/health/transit/park/bike) for nearest-distance search",
)

ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"category": "str", "h3_r10": "str", "lat": "numeric", "lng": "numeric", "geometry": "geometry"},
    uniqueness=("category", "lat", "lng"),
    completeness=("category", "h3_r10", "lat", "lng", "geometry"),
)


def _amenity_frame(gdf: gpd.GeoDataFrame, category: str | None = None) -> gpd.GeoDataFrame:
    """Collapse to candidate points; optionally override category."""
    pts = points_with_lat_lng(gdf)
    if category is not None: pts["category"] = category
    return pts[["category", "h3_r10", "lat", "lng", "geometry"]]

@dg.asset(
    group_name="distance",
    metadata=asdict(ASSET_META),
    deps=[
        h3_montreal_osm_pois,
        h3_montreal_transit_stops,
        h3_montreal_parks,
        h3_montreal_bike_paths,
    ],
    code_version=CODE_VERSION,
)
def amenities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Amenity candidate points for nearest-distance search."""
    if cached := reuse_if_unchanged(context):
        return cached
    osm_pois = s3_datastore.read_gpq(context, location_of(h3_montreal_osm_pois))
    transit = s3_datastore.read_gpq(context, location_of(h3_montreal_transit_stops))
    parks = s3_datastore.read_gpq(context, location_of(h3_montreal_parks))
    bike_paths = s3_datastore.read_gpq(context, location_of(h3_montreal_bike_paths))

    bike = bike_paths[["h3_r10"]].drop_duplicates().copy()
    latlng = np.array(bike["h3_r10"].map(h3.cell_to_latlng).tolist())
    bike = bike.set_geometry(gpd.points_from_xy(latlng[:, 1], latlng[:, 0]), crs=4326)

    frames = [
        _amenity_frame(osm_pois),
        _amenity_frame(transit, "transit"),
        _amenity_frame(parks, "park"),
        _amenity_frame(bike, "bike"),
    ]
    amenities_gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=4326)
    amenities_gdf = amenities_gdf.dropna(subset=["category", "h3_r10", "lat", "lng"])
    context.log.info(f"amenity_points: {len(amenities_gdf)} rows")
    for cat in amenities_gdf["category"].unique():
        context.log.info(f"  {cat}: {(amenities_gdf['category'] == cat).sum()} rows")

    stamp = s3_datastore.write_gpq(context, amenities_gdf)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

checks = standard_checks(amenities, ASSET_DATA_CONTRACT)
