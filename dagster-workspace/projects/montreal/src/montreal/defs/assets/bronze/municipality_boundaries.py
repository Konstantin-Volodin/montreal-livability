"""
Montreal agglomeration boundaries: fetch from donnees.montreal.ca, cache on S3, validate.
"""

import geopandas as gpd

from montreal.defs.assets.bronze.config import (
    BronzeAssetDataContract,
    BronzeAssetMetadata,
    raw_geo_asset,
)

ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="donnees.montreal.ca",
    description="Montreal agglomeration administrative boundary polygons",
    url="https://donnees.montreal.ca/dataset/9797a946-9da8-41ec-8815-f6b276dec7e9/resource/e18bfd07-edc8-4ce8-8a5a-3b617662a794/download/limites-administratives-agglomeration.geojson",
)

ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={"geometry": "geometry", "NOM": "str", "TYPE": "str"},
    uniqueness=("geometry",),
    completeness=("geometry", "TYPE", "NOM"),
    freshness={"max_days": 360},
)

montreal_municipality_boundaries, checks = raw_geo_asset(
    "montreal_municipality_boundaries",
    ASSET_META,
    ASSET_DATA_CONTRACT,
    fetch=lambda context: gpd.read_file(ASSET_META.url),
)
