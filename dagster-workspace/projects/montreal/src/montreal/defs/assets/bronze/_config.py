import datetime
from dataclasses import asdict, dataclass
from typing import Callable, Dict, Sequence

import dagster as dg
import geopandas as gpd

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import s3_datastore


@dataclass(frozen=True)
class BronzeAssetMetadata:
    layer: str
    data_category: str
    source: str
    description: str
    url: str


@dataclass(frozen=True)
class BronzeAssetDataContract:
    schema: Dict[str, str]
    uniqueness: Sequence[str]
    completeness: Sequence[str]
    freshness: Dict[str, int]


Fetch = Callable[[dg.AssetExecutionContext], gpd.GeoDataFrame]
def raw_geo_asset(
    name: str,
    meta: BronzeAssetMetadata,
    contract: BronzeAssetDataContract,
    fetch: Fetch,
):
    """Bronze asset factory. Reuses the S3 snapshot while it's younger than max_days
    AND the code version is unchanged; else fetches fresh."""

    @dg.asset(name=name, group_name="raw_data", metadata=asdict(meta), code_version=CODE_VERSION)
    def _asset(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
        directory = s3_datastore.asset_dir(context)
        last = s3_datastore.latest_timestamp(directory)
        age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

        fresh = age is not None and age <= datetime.timedelta(days=contract.freshness["max_days"])
        if fresh and (cached := reuse_if_unchanged(context)):
            return cached

        context.log.info("Downloading latest dataset")
        stamp = s3_datastore.write_gpq(context, fetch(context))
        return dg.MaterializeResult(
            data_version=dg.DataVersion(stamp),
            metadata={"s3_cache_hit": False},
        )

    return _asset, standard_checks(_asset, contract)
