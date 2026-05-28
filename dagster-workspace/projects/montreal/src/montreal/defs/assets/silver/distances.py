"""Nearest amenity distance per Montreal address and livability category, sharded by r6."""

from dataclasses import asdict

import dagster as dg
import h3
import numpy as np
import pandas as pd

from montreal.defs.assets.silver.h3 import h3_montreal_addresses
from montreal.defs.assets.silver.amenities import amenities
from montreal.defs.assets.silver.config import (
    POI_CATEGORIES,
    SilverAssetDataContract,
    SilverAssetMetadata,
    points_with_lat_lng,
    r6_partitions,
)
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# Bump to force a recompute when this asset's logic changes, even if inputs haven't.
CODE_VERSION = "1"

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="h3_r6",
    description="Nearest amenity distance per Montreal address and livability category, sharded by r6 cell",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={
        "ID_UEV": "str",
        "h3_r10": "str",
        "h3_r6": "str",
        "geometry": "geometry",
        **{f"dist_{category}": "numeric" for category in POI_CATEGORIES},
    },
    uniqueness=("ID_UEV",),
    completeness=("ID_UEV", "h3_r10", "geometry"),
)


def haversine(lon1, lat1, lon2, lat2):
    """Vectorized great-circle distance in metres. All args must be of equal length."""
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    m = 6378.137 * c * 1000
    return m


def nearest(addr_df, amenity_df, max_k=10, log=None) -> pd.DataFrame:
    """Nearest amenity distance per address and category using H3 ring search.

    algorithm:
      1. Drop address rows missing an H3 cell or coordinates.
      3. For each (category, address cell), step the H3 ring distance outward
         k = 0, 1, 2, ... and stop at the first k whose ring holds amenity
         points (giving up after max_k) -- those become the candidate set for
         every address in that cell.
      4. Haversine-distance every address to its candidates and keep the minimum.
    """
    def _info(msg):
        if log is not None: log.info(msg)

    dist_columns = [f"dist_{category}" for category in POI_CATEGORIES]
    distances = pd.DataFrame(index=addr_df.index, columns=dist_columns, dtype=float)

    # Step 1: only addresses with a cell + lat/lng can be matched.
    addr_work = addr_df.dropna(subset=["h3_r10", "lat", "lng"])
    _info(f"_nearest_distances: {len(addr_work)}/{len(addr_df)} addresses usable, {len(amenity_df)} amenity points")

    # Step 2: bucket each category's points by the H3 r10 cell they sit in.
    points_by_category = {}
    for category in POI_CATEGORIES:
        category_df = amenity_df[amenity_df["category"] == category]
        points_by_category[category] = {
            cell: group[["lat", "lng"]].to_numpy(dtype=float)
            for cell, group in category_df.groupby("h3_r10", sort=False)
        }
        _info(f"  category '{category}': {len(category_df)} points across {len(points_by_category[category])} H3 cells")

    cells = addr_work["h3_r10"].to_numpy()
    coords = addr_work[["lat", "lng"]].to_numpy(dtype=float)
    unique_cells = pd.unique(addr_work["h3_r10"])
    _info(f"  {len(unique_cells)} unique address cells to resolve per category")

    for category, points_by_cell in points_by_category.items():

        # Step 3: step the ring distance out one k at a time, stopping at the first ring that holds an amenity cell.
        candidate_cache = {}
        no_candidate_cells = 0
        for addr_cell in unique_cells:
            found = []

            # k ring search for the nearest amenity cell(s)
            for k in range(max_k + 1):
                grid_ring_cells = h3.grid_ring(addr_cell, k)
                found = [points_by_cell[cell] for cell in grid_ring_cells if cell in points_by_cell]
                if found: break

            if not found: no_candidate_cells += 1
            candidate_cache[addr_cell] = (np.vstack(found) if found else np.empty((0, 2), dtype=float))
        _info(f"  category '{category}': {no_candidate_cells} cells found no amenity within k={max_k}")

        # Step 4: vectorized haversine to candidates, keep the nearest.
        category_distances = np.full(len(addr_work), np.nan, dtype=float)
        for addr_cell, candidates in candidate_cache.items():
            if len(candidates) == 0: continue

            positions = np.flatnonzero(cells == addr_cell)
            addr_coords = coords[positions]
            candidate_distances = haversine(
                addr_coords[:, [0]],
                addr_coords[:, [1]],
                candidates[None, :, 0],
                candidates[None, :, 1],
            )
            category_distances[positions] = np.nanmin(candidate_distances, axis=1)
        resolved = int(np.count_nonzero(~np.isnan(category_distances)))

        _info(
            f"  category '{category}': {resolved}/{len(addr_work)} addresses got "
            f"a distance (median {np.nanmedian(category_distances):.0f} m)"
            if resolved else f"  category '{category}': 0 addresses got a distance"
        )
        distances.loc[addr_work.index, f"dist_{category}"] = category_distances

    return distances

# asset
@dg.asset(
    group_name="distance",
    metadata=asdict(ASSET_META),
    partitions_def=r6_partitions,
    deps=[h3_montreal_addresses, amenities],
    code_version=CODE_VERSION,
)
def distances_to_amenities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Nearest amenity distance per Montreal address and livability category."""
    upstreams = [
        f"{location_of(h3_montreal_addresses)}/{context.partition_key}",  # this partition's r6 shard
        location_of(amenities),
    ]
    if s3_datastore.should_skip(context, upstreams, code_version=CODE_VERSION):
        return s3_datastore.reemit_latest(context)

    addresses = s3_datastore.read_gpq(context, f"{location_of(h3_montreal_addresses)}/{context.partition_key}")
    amenities = s3_datastore.read_gpq(context, location_of(amenities))

    address_points = points_with_lat_lng(addresses)
    distance_df = nearest(address_points, amenities, log=context.log)

    out = addresses.copy()
    for column in distance_df.columns:
        out[column] = distance_df[column].to_numpy()

    context.log.info(f"distances_to_amenities: {len(out)} address rows with {len(distance_df.columns)} distance columns")
    stamp = s3_datastore.write_gpq(context, out, code_version=CODE_VERSION)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
checks = standard_checks(distances_to_amenities, ASSET_DATA_CONTRACT)
