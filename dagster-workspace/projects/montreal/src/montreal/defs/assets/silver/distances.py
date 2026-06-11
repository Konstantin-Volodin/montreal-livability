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

ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="h3_r6",
    description="Nearest amenity distance per Montreal address and livability category, sharded by r6 cell",
)

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
    """Vectorized great-circle distance in metres."""
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    a = np.sin((lat2-lat1)/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin((lon2-lon1)/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6378.137 * c * 1000


def nearest(addr_df, amenity_df, max_k=10, log=None) -> pd.DataFrame:
    """Nearest amenity distance per address via H3 ring search and haversine."""
    dist_cols = [f"dist_{cat}" for cat in POI_CATEGORIES]
    distances = pd.DataFrame(index=addr_df.index, columns=dist_cols, dtype=float)

    addr_work = addr_df.dropna(subset=["h3_r10", "lat", "lng"])
    points_by_cat = {
        cat: {
            cell: grp[["lat", "lng"]].to_numpy(dtype=float)
            for cell, grp in amenity_df[amenity_df["category"] == cat].groupby("h3_r10", sort=False)
        }
        for cat in POI_CATEGORIES
    }

    cells = addr_work["h3_r10"].to_numpy()
    coords = addr_work[["lat", "lng"]].to_numpy(dtype=float)
    unique_cells = pd.unique(addr_work["h3_r10"])

    summary = []
    for cat, pts_by_cell in points_by_cat.items():
        cand_cache = {}
        for addr_cell in unique_cells:
            found = []
            for k in range(max_k + 1):
                found = [pts_by_cell[c] for c in h3.grid_ring(addr_cell, k) if c in pts_by_cell]
                if found: break
            cand_cache[addr_cell] = np.vstack(found) if found else np.empty((0, 2), dtype=float)

        cat_dists = np.full(len(addr_work), np.nan, dtype=float)
        for addr_cell, cands in cand_cache.items():
            if len(cands) == 0: continue
            pos = np.flatnonzero(cells == addr_cell)
            cat_dists[pos] = np.nanmin(haversine(
                coords[pos, 1][:, None], coords[pos, 0][:, None],
                cands[None, :, 1], cands[None, :, 0],
            ), axis=1)

        resolved = int(np.count_nonzero(~np.isnan(cat_dists)))
        med = np.nanmedian(cat_dists)
        summary.append(f"{cat} {resolved}/{len(addr_work)} ({med:.0f}m)" if resolved else f"{cat} 0/{len(addr_work)}")
        distances.loc[addr_work.index, f"dist_{cat}"] = cat_dists

    if log is not None:
        log.info(f"nearest: {len(addr_work)}/{len(addr_df)} addresses, {len(amenity_df)} amenities | {' '.join(summary)}")
    return distances

@dg.asset(
    group_name="distance",
    metadata=asdict(ASSET_META),
    partitions_def=r6_partitions,
    deps=[h3_montreal_addresses, amenities],
    code_version=CODE_VERSION,
)
def distances_to_amenities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Nearest amenity distance per Montreal address and livability category."""
    if cached := reuse_if_unchanged(context):
        return cached
    addresses = s3_datastore.read_gpq(context, f"{location_of(h3_montreal_addresses)}/{context.partition_key}")
    amenity_points = s3_datastore.read_gpq(context, location_of(amenities))

    addr_pts = points_with_lat_lng(addresses)
    dist_df = nearest(addr_pts, amenity_points, log=context.log)

    out = addresses.copy()
    for col in dist_df.columns:
        out[col] = dist_df[col].to_numpy()

    context.log.info(f"distances_to_amenities: {len(out)} addresses, {len(dist_df.columns)} distance columns")
    stamp = s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

checks = standard_checks(distances_to_amenities, ASSET_DATA_CONTRACT)
