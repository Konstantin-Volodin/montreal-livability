"""
Montreal address points: fetch from donnees.montreal.ca, cache on S3, validate.

Addresses move only a few times a year, so the freshness window is generous.
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
    description="Montreal address points dataset",
    url="https://donnees.montreal.ca/dataset/4ad6baea-4d2c-460f-a8bf-5d000db498f7/resource/866a3dbc-8b59-48ff-866d-f2f9d3bbee9d/download/uniteevaluationfonciere.geojson.zip",
)

ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={
        "ID_UEV": "str",
        "CIVIQUE_DEBUT": "str",
        "CIVIQUE_FIN": "str",
        "NOM_RUE": "str",
        "SUITE_DEBUT": "str",
        "ETAGE_HORS_SOL": "numeric",
        "NOMBRE_LOGEMENT": "numeric",
        "ANNEE_CONSTRUCTION": "numeric",
        "CODE_UTILISATION": "str",
        "LETTRE_DEBUT": "str",
        "LETTRE_FIN": "str",
        "LIBELLE_UTILISATION": "str",
        "CATEGORIE_UEF": "str",
        "MATRICULE83": "str",
        "SUPERFICIE_TERRAIN": "numeric",
        "SUPERFICIE_BATIMENT": "numeric",
        "NO_ARROND_ILE_CUM": "str",
        "MUNICIPALITE": "str",
        "geometry": "geometry",
    },
    uniqueness=("ID_UEV",),
    completeness=("ID_UEV", "geometry"),
    freshness={"max_days": 360},
)

montreal_addresses, checks = raw_geo_asset(
    "montreal_addresses",
    ASSET_META,
    ASSET_DATA_CONTRACT,
    fetch=lambda context: gpd.read_file(ASSET_META.url, compression="zip"),
)
