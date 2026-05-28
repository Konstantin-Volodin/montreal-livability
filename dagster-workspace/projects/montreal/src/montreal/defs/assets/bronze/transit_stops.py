"""
Montreal transit stops: fetch from the STM GTFS feed, cache on S3, validate.
"""
import dagster as dg
import geopandas as gpd
import pandas as pd
import datetime
import io
import urllib.request
import zipfile
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
    source="stm.info",
    description="Montreal transit stops from the STM GTFS feed (stops.txt)",
    url="https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip",
)

# data contract
ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry", "stop_id": "str"},
    uniqueness=("stop_id",),
    completeness=("stop_id", "geometry"),
    freshness={"max_days": 25},
)


def _read_stm_stops() -> gpd.GeoDataFrame:
    with urllib.request.urlopen(ASSET_META.url, timeout=240) as resp:
        with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
            with zf.open("stops.txt") as f:
                df = pd.read_csv(f)
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["stop_lon"], df["stop_lat"]),
        crs=4326,
    )


@dg.asset(group_name="raw_data", metadata=asdict(ASSET_META))
def montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetch STM transit stops, reusing the S3 snapshot while it is within the freshness window."""
    directory = s3_datastore.asset_dir(context)
    last = s3_datastore.latest_timestamp(directory)
    age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

    if age is not None and age <= datetime.timedelta(days=ASSET_DATA_CONTRACT.freshness["max_days"]):
        context.log.info(f"Using snapshot for {directory} ({age.days}d old).")
        return s3_datastore.reemit_latest(context)

    context.log.info("Downloading latest dataset")
    data = _read_stm_stops()
    stamp = s3_datastore.write_gpq(context, data)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={"s3_cache_hit": False},
    )

# asset checks
transit_stops_schema = schema_contract_factory(montreal_transit_stops, ASSET_DATA_CONTRACT.schema)
transit_stops_uniqueness = row_uniqueness_factory(montreal_transit_stops, ASSET_DATA_CONTRACT.uniqueness)
transit_stops_completeness = field_completeness_factory(montreal_transit_stops, ASSET_DATA_CONTRACT.completeness)
