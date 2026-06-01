"""H3-index the Montreal park polygons."""

from dataclasses import asdict

import dagster as dg

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets.bronze import montreal_parks
from montreal.defs.assets.silver._config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_index,
)
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="Montreal parks with the h3_r10 analysis column",
)

ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"geometry": "geometry", "h3_r10": "str"},
    uniqueness=("geometry",),
    completeness=("geometry", "h3_r10"),
)

@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_parks], code_version=CODE_VERSION)
def h3_montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """H3-index parks; add h3_r10 column."""
    if cached := reuse_if_unchanged(context):
        return cached
    gdf = s3_datastore.read_gpq(context, location_of(montreal_parks))
    gdf = h3_index(gdf)
    context.log.info(f"montreal_parks: {len(gdf)} rows H3-indexed (r10)")
    stamp = s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

checks = standard_checks(h3_montreal_parks, ASSET_DATA_CONTRACT)
