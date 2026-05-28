"""
Montreal address points: fetch from donnees.montreal.ca, cache on S3, validate.

Addresses move only a few times a year, so the freshness window is generous.
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
    description="Montreal address points dataset",
    url="https://donnees.montreal.ca/dataset/4ad6baea-4d2c-460f-a8bf-5d000db498f7/resource/866a3dbc-8b59-48ff-866d-f2f9d3bbee9d/download/uniteevaluationfonciere.geojson.zip",
)

# data contract
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

# asset
@dg.asset(
    group_name="raw_data",
    metadata=asdict(ASSET_META)
)
def montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetch Montreal addresses, reusing the S3 snapshot while it is within the freshness window."""
    directory = s3_datastore.asset_dir(context)
    last = s3_datastore.latest_timestamp(directory)
    age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

    if age is not None and age <= datetime.timedelta(days=ASSET_DATA_CONTRACT.freshness["max_days"]):
        context.log.info(f"Using snapshot for {directory} ({age.days}d old).")
        return s3_datastore.reemit_latest(context)

    context.log.info("Downloading latest dataset")
    data = gpd.read_file(ASSET_META.url, compression="zip")
    stamp = s3_datastore.write_gpq(context, data)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={"s3_cache_hit": False},
    )

# asset checks
addresses_schema = schema_contract_factory(montreal_addresses, ASSET_DATA_CONTRACT.schema)
addresses_uniqueness = row_uniqueness_factory(montreal_addresses, ASSET_DATA_CONTRACT.uniqueness)
addresses_completeness = field_completeness_factory(montreal_addresses, ASSET_DATA_CONTRACT.completeness)
