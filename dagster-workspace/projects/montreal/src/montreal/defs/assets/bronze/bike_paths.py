"""
Montreal bike paths (réseau cyclable): fetch from donnees.montreal.ca, cache on S3, validate.
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
    snapshot_freshness_factory,
)

# metadata
ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="donnees.montreal.ca",
    description="Montreal bike paths (réseau cyclable) dataset",
    url="https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson",
)

# data contract
ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry"},
    uniqueness=("geometry",),
    completeness=("geometry",),
    freshness={"max_days": 28},
)

# asset
@dg.asset(group_name="raw_data", metadata=asdict(ASSET_META))
def montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetch Montreal bike paths, reusing the S3 snapshot while it is within the freshness window."""
    directory = s3_datastore.asset_dir(context)
    last = s3_datastore.latest_timestamp(directory)
    age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

    if age is not None and age <= datetime.timedelta(days=ASSET_DATA_CONTRACT.freshness["max_days"]):
        context.log.info(f"Using snapshot for {directory} ({age.days}d old).")
        return dg.MaterializeResult(
            data_version=dg.DataVersion(f"{last:%Y%m%dT%H%M%S_%f}Z"),
            metadata={"s3_cache_hit": True, "snapshot_age_days": age.days},
        )

    context.log.info("Downloading latest dataset")
    data = gpd.read_file(ASSET_META.url)
    stamp = s3_datastore.write_gpq(context, data)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={"s3_cache_hit": False},
    )

# asset checks
bike_paths_freshness = snapshot_freshness_factory(montreal_bike_paths, ASSET_DATA_CONTRACT.freshness)
bike_paths_schema = schema_contract_factory(montreal_bike_paths, ASSET_DATA_CONTRACT.schema)
bike_paths_uniqueness = row_uniqueness_factory(montreal_bike_paths, ASSET_DATA_CONTRACT.uniqueness)
bike_paths_completeness = field_completeness_factory(montreal_bike_paths, ASSET_DATA_CONTRACT.completeness)
