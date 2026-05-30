"""
Montreal parks (espaces verts): fetch from donnees.montreal.ca, cache on S3, validate.
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
    description="Montreal parks (espaces verts) dataset",
    url="https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json",
)

ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry"},
    uniqueness=("geometry",),
    completeness=("geometry",),
    freshness={"max_days": 25},
)

montreal_parks, checks = raw_geo_asset(
    "montreal_parks",
    ASSET_META,
    ASSET_DATA_CONTRACT,
    fetch=lambda context: gpd.read_file(ASSET_META.url),
)
