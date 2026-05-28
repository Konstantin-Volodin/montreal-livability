"""Unify per-category amenity points into candidate points for nearest-distance search."""

from dataclasses import asdict

import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd

from montreal.defs.assets.silver.config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    points_with_lat_lng,
)
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore
from montreal.defs.assets.silver.h3 import (
    h3_montreal_bike_paths,
    h3_montreal_osm_pois,
    h3_montreal_parks,
    h3_montreal_transit_stops,
)

# Bump to force a recompute when this asset's logic changes, even if inputs haven't.
CODE_VERSION = "1"

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="Amenity candidate points (grocery/school/health/transit/park/bike) for nearest-distance search",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"category": "str", "h3_r10": "str", "lat": "numeric", "lng": "numeric", "geometry": "geometry"},
    uniqueness=("category", "lat", "lng"),
    completeness=("category", "h3_r10", "lat", "lng", "geometry"),
)


def _amenity_frame(gdf: gpd.GeoDataFrame, category: str | None = None) -> gpd.GeoDataFrame:
    """Collapse to candidate points; set ``category`` when given, else keep the frame's own."""
    points = points_with_lat_lng(gdf)
    if category is not None: points["category"] = category
    return points[["category", "h3_r10", "lat", "lng", "geometry"]]

# asset
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
    """amenity candidate points for nearest-distance search."""
    upstreams = [
        location_of(h3_montreal_osm_pois),
        location_of(h3_montreal_transit_stops),
        location_of(h3_montreal_parks),
        location_of(h3_montreal_bike_paths),
    ]
    if s3_datastore.should_skip(context, upstreams, code_version=CODE_VERSION):
        return s3_datastore.reemit_latest(context)

    # read data
    osm_pois = s3_datastore.read_gpq(context, location_of(h3_montreal_osm_pois))
    transit = s3_datastore.read_gpq(context, location_of(h3_montreal_transit_stops))
    parks = s3_datastore.read_gpq(context, location_of(h3_montreal_parks))
    bike_paths = s3_datastore.read_gpq(context, location_of(h3_montreal_bike_paths))

    # for bike paths, we use the path-cell centroids
    bike = bike_paths[["h3_r10"]].drop_duplicates().copy()
    latlng = np.array(bike["h3_r10"].map(h3.cell_to_latlng).tolist())
    bike = bike.set_geometry(gpd.points_from_xy(latlng[:, 1], latlng[:, 0]), crs=4326)

    # osm_pois already carries its per-row category (grocery/school/health).
    frames = [
        _amenity_frame(osm_pois),
        _amenity_frame(transit, "transit"),
        _amenity_frame(parks, "park"),
        _amenity_frame(bike, "bike"),
    ]

    # results
    amenities = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), geometry="geometry", crs=4326,)
    amenities = amenities.dropna(subset=["category", "h3_r10", "lat", "lng"])
    context.log.info(f"amenity_points: {len(amenities)} rows")
    for category in amenities["category"].unique():
        count = (amenities["category"] == category).sum()
        context.log.info(f"  {category}: {count} rows")

    # export
    stamp = s3_datastore.write_gpq(context, amenities, code_version=CODE_VERSION)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
checks = standard_checks(amenities, ASSET_DATA_CONTRACT)
