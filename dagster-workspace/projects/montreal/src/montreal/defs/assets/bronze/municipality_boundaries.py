"""
Montreal agglomeration boundaries: fetch from donnees.montreal.ca, cache on S3, validate.
"""

import dagster as dg
import geopandas as gpd
import datetime
from dataclasses import asdict

from montreal.defs.resources.lakehouse import s3_datastore
from montreal.defs.assets.bronze.config import (
    BronzeAssetDataContract, 
    BronzeAssetMetadata,
)
from montreal.defs.checks.factory import (
    field_completeness_factory,
    row_uniqueness_factory,
    schema_contract_factory,
)

# metadata
ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="donnees.montreal.ca",
    description="Montreal agglomeration administrative boundary polygons",
    url="https://donnees.montreal.ca/dataset/9797a946-9da8-41ec-8815-f6b276dec7e9/resource/e18bfd07-edc8-4ce8-8a5a-3b617662a794/download/limites-administratives-agglomeration.geojson",
)

# data contract
ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry","NOM":"str", "TYPE": "str"},
    uniqueness=("geometry",),
    completeness=("geometry","TYPE","NOM",),
    freshness={"max_days": 360},
)

# asset
@dg.asset(group_name="raw_data", metadata=asdict(ASSET_META))
def montreal_municipality_boundaries(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetch the agglomeration boundaries, reusing the S3 snapshot while it is within the freshness window."""
    directory = s3_datastore.asset_dir(context)
    last = s3_datastore.latest_timestamp(directory)
    age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

    if age is not None and age <= datetime.timedelta(days=ASSET_DATA_CONTRACT.freshness["max_days"]):
        context.log.info(f"Using snapshot for {directory} ({age.days}d old).")
        return s3_datastore.reemit_latest(context)

    context.log.info("Downloading latest dataset")
    data = gpd.read_file(ASSET_META.url)
    stamp = s3_datastore.write_gpq(context, data)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={"s3_cache_hit": False},
    )

# asset checks
municipality_boundaries_schema = schema_contract_factory(montreal_municipality_boundaries, ASSET_DATA_CONTRACT.schema)
municipality_boundaries_uniqueness = row_uniqueness_factory(montreal_municipality_boundaries, ASSET_DATA_CONTRACT.uniqueness)
municipality_boundaries_completeness = field_completeness_factory(montreal_municipality_boundaries, ASSET_DATA_CONTRACT.completeness)
