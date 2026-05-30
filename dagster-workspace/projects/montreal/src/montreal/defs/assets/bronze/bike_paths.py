"""
Montreal bike paths (réseau cyclable): fetch from donnees.montreal.ca, cache on S3, validate.
"""

import geopandas as gpd

from montreal.defs.assets.bronze._config import (
    BronzeAssetDataContract,
    BronzeAssetMetadata,
    raw_geo_asset,
)

ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="donnees.montreal.ca",
    description="Montreal bike paths (réseau cyclable) dataset",
    url="https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson",
)

ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"ID_CYCL": "numeric", "geometry": "geometry"},
    uniqueness=("ID_CYCL",),
    completeness=("ID_CYCL", "geometry"),
    freshness={"max_days": 360},
)

montreal_bike_paths, checks = raw_geo_asset(
    "montreal_bike_paths",
    ASSET_META,
    ASSET_DATA_CONTRACT,
    fetch=lambda context: gpd.read_file(ASSET_META.url),
)
