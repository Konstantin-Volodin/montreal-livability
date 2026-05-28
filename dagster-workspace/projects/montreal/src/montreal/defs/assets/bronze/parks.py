"""
Montreal parks (espaces verts): fetch from donnees.montreal.ca, cache on S3, validate.
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
    description="Montreal parks (espaces verts) dataset",
    url="https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json",
)

# data contract
ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry"},
    uniqueness=("geometry",),
    completeness=("geometry",),
    freshness={"max_days": 25},
)


@dg.asset(group_name="raw_data", metadata=asdict(ASSET_META))
def montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetch Montreal parks, reusing the S3 snapshot while it is within the freshness window."""
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
parks_schema = schema_contract_factory(montreal_parks, ASSET_DATA_CONTRACT.schema)
parks_uniqueness = row_uniqueness_factory(montreal_parks, ASSET_DATA_CONTRACT.uniqueness)
parks_completeness = field_completeness_factory(montreal_parks, ASSET_DATA_CONTRACT.completeness)
