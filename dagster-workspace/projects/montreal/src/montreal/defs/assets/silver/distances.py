"""Nearest amenity distance per Montreal address and livability category, sharded by r6."""

from dataclasses import asdict

import dagster as dg
import h3
import numpy as np
import pandas as pd

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.assets.silver.h3 import h3_montreal_addresses
from montreal.defs.assets.silver.amenities import amenities
from montreal.defs.assets.silver._config import (
    POI_CATEGORIES,
    SilverAssetDataContract,
    SilverAssetMetadata,
    points_with_lat_lng,
    r6_partitions,
)
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

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
    dist_columns = [f"dist_{category}" for category in POI_CATEGORIES]
    distances = pd.DataFrame(index=addr_df.index, columns=dist_columns, dtype=float)

    # Step 1: only addresses with a cell + lat/lng can be matched.
    addr_work = addr_df.dropna(subset=["h3_r10", "lat", "lng"])

    # Step 2: bucket each category's points by the H3 r10 cell they sit in.
    points_by_category = {
        category: {
            cell: group[["lat", "lng"]].to_numpy(dtype=float)
            for cell, group in amenity_df[amenity_df["category"] == category].groupby("h3_r10", sort=False)
        }
        for category in POI_CATEGORIES
    }

    cells = addr_work["h3_r10"].to_numpy()
    coords = addr_work[["lat", "lng"]].to_numpy(dtype=float)
    unique_cells = pd.unique(addr_work["h3_r10"])

    summary = []
    for category, points_by_cell in points_by_category.items():

        # Step 3: step the ring distance out one k at a time, stopping at the first ring that holds an amenity cell.
        candidate_cache = {}
        for addr_cell in unique_cells:
            found = []
            for k in range(max_k + 1):
                found = [points_by_cell[c] for c in h3.grid_ring(addr_cell, k) if c in points_by_cell]
                if found: break
            candidate_cache[addr_cell] = np.vstack(found) if found else np.empty((0, 2), dtype=float)

        # Step 4: vectorized haversine to candidates, keep the nearest.
        category_distances = np.full(len(addr_work), np.nan, dtype=float)
        for addr_cell, candidates in candidate_cache.items():
            if len(candidates) == 0: continue
            positions = np.flatnonzero(cells == addr_cell)
            addr_coords = coords[positions]
            category_distances[positions] = np.nanmin(haversine(
                addr_coords[:, [0]], addr_coords[:, [1]],
                candidates[None, :, 0], candidates[None, :, 1],
            ), axis=1)

        resolved = int(np.count_nonzero(~np.isnan(category_distances)))
        summary.append(f"{category} {resolved}/{len(addr_work)} ({np.nanmedian(category_distances):.0f}m)" if resolved else f"{category} 0/{len(addr_work)}")
        distances.loc[addr_work.index, f"dist_{category}"] = category_distances

    if log is not None:
        log.info(f"nearest: {len(addr_work)}/{len(addr_df)} addresses, {len(amenity_df)} amenities | " + "  ".join(summary))
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
    # Per-partition gate: if neither upstream (both unpartitioned) changed and the
    # code version held, this r6 shard's prior snapshot is reused -- skipping the
    # ring search for every address in the cell.
    if cached := reuse_if_unchanged(context):
        return cached
    addresses = s3_datastore.read_gpq(context, f"{location_of(h3_montreal_addresses)}/{context.partition_key}")
    amenity_points = s3_datastore.read_gpq(context, location_of(amenities))

    address_points = points_with_lat_lng(addresses)
    distance_df = nearest(address_points, amenity_points, log=context.log)

    out = addresses.copy()
    for column in distance_df.columns:
        out[column] = distance_df[column].to_numpy()

    context.log.info(f"distances_to_amenities: {len(out)} address rows with {len(distance_df.columns)} distance columns")
    stamp = s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

# asset checks
checks = standard_checks(distances_to_amenities, ASSET_DATA_CONTRACT)
