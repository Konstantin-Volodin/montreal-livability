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
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore, skip

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="Montreal parks with the h3_r10 analysis column",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"geometry": "geometry", "h3_r10": "str"},
    uniqueness=("geometry",),
    completeness=("geometry", "h3_r10"),
)

# asset
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_parks], code_version=CODE_VERSION)
def h3_montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_parks asset; add the h3_r10 analysis column"""
    if skip.should_skip(s3_datastore, context, [location_of(montreal_parks)], code_version=CODE_VERSION):
        return skip.reemit_latest(s3_datastore, context)

    gdf = s3_datastore.read_gpq(context, location_of(montreal_parks))
    gdf = h3_index(gdf)
    context.log.info(f"montreal_parks: {len(gdf)} rows H3-indexed (r10)")
    stamp = s3_datastore.write_gpq(context, gdf, code_version=CODE_VERSION)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
checks = standard_checks(h3_montreal_parks, ASSET_DATA_CONTRACT)
