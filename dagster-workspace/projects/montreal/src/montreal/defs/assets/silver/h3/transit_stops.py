"""H3-index the Montreal transit stops."""

from dataclasses import asdict

import dagster as dg

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets.bronze import montreal_transit_stops
from montreal.defs.assets.silver._config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_index,
)
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="STM transit stops with the h3_r10 analysis column",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"stop_id": "str", "h3_r10": "str", "geometry": "geometry"},
    uniqueness=("stop_id",),
    completeness=("stop_id", "h3_r10", "geometry"),
)

# asset
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_transit_stops], code_version=CODE_VERSION)
def h3_montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_transit_stops asset; add the h3_r10 analysis column"""
    if cached := reuse_if_unchanged(context):
        return cached
    gdf = s3_datastore.read_gpq(context, location_of(montreal_transit_stops))
    gdf = h3_index(gdf)
    context.log.info(f"montreal_transit_stops: {len(gdf)} rows H3-indexed (r10)")
    stamp = s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

# asset checks
checks = standard_checks(h3_montreal_transit_stops, ASSET_DATA_CONTRACT)
