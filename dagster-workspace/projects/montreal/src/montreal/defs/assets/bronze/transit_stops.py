"""
Montreal transit stops: fetch from the STM GTFS feed, cache on S3, validate.
"""

import io
import urllib.request
import zipfile

import geopandas as gpd
import pandas as pd

from montreal.defs.assets.bronze._config import (
    BronzeAssetDataContract,
    BronzeAssetMetadata,
    raw_geo_asset,
)

ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="stm.info",
    description="Montreal transit stops from the STM GTFS feed (stops.txt)",
    url="https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip",
)

ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry", "stop_id": "str"},
    uniqueness=("stop_id",),
    completeness=("stop_id", "geometry"),
    freshness={"max_days": 25},
)


def _read_stm_stops(context) -> gpd.GeoDataFrame:
    with urllib.request.urlopen(ASSET_META.url, timeout=240) as resp:
        with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
            with zf.open("stops.txt") as f:
                df = pd.read_csv(f)
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["stop_lon"], df["stop_lat"]),
        crs=4326,
    )


montreal_transit_stops, checks = raw_geo_asset(
    "montreal_transit_stops",
    ASSET_META,
    ASSET_DATA_CONTRACT,
    fetch=_read_stm_stops,
)
