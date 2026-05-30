"""H3-cover the Montreal bike paths (linetrace); one row per (path, r10 cell)."""

from dataclasses import asdict

import dagster as dg

from montreal.defs.assets.bronze import montreal_bike_paths
from montreal.defs.assets.silver._config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_linetrace,
)
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# Bump to force a recompute when this asset's logic changes, even if inputs haven't.
CODE_VERSION = "1"

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="Montreal bike paths H3-traced to r10 cells; one row per (path, covered cell)",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"ID_CYCL": "numeric", "h3_r10": "str"},
    uniqueness=("ID_CYCL", "h3_r10"),
    completeness=("ID_CYCL", "h3_r10"),
)

# asset
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_bike_paths], code_version=CODE_VERSION)
def h3_montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """h3-cover the montreal_bike_paths lines (linetrace); add h3_r10, one row per covered r10 cell"""
    if s3_datastore.should_skip(context, [location_of(montreal_bike_paths)], code_version=CODE_VERSION):
        return s3_datastore.reemit_latest(context)

    gdf = s3_datastore.read_gpq(context, location_of(montreal_bike_paths))
    gdf = h3_linetrace(gdf)
    context.log.info(f"montreal_bike_paths: {len(gdf)} (path, r10 cell) rows H3-traced")
    stamp = s3_datastore.write_gpq(context, gdf, code_version=CODE_VERSION)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
checks = standard_checks(h3_montreal_bike_paths, ASSET_DATA_CONTRACT)
