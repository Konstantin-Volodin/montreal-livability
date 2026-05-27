"""H3-index the Montreal park polygons."""

from dataclasses import asdict

import dagster as dg

from montreal.defs.assets.bronze import montreal_parks
from montreal.defs.assets.silver.config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_index,
)
from montreal.defs.checks.factory import (
    field_completeness_factory,
    row_uniqueness_factory,
    schema_contract_factory,
)
from montreal.defs.resources.lakehouse import location_of, s3_datastore

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
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_parks])
def h3_montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_parks asset; add the h3_r10 analysis column"""
    gdf = s3_datastore.read_gpq(context, location_of(montreal_parks))
    gdf = h3_index(gdf)
    context.log.info(f"montreal_parks: {len(gdf)} rows H3-indexed (r10)")
    stamp = s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
parks_schema = schema_contract_factory(h3_montreal_parks, ASSET_DATA_CONTRACT.schema)
parks_uniqueness = row_uniqueness_factory(h3_montreal_parks, ASSET_DATA_CONTRACT.uniqueness)
parks_completeness = field_completeness_factory(h3_montreal_parks, ASSET_DATA_CONTRACT.completeness)
