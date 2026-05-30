import datetime
from dataclasses import asdict, dataclass
from typing import Callable, Dict, Sequence

import dagster as dg
import geopandas as gpd

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
    """Every bronze asset must set all four fields (no defaults - a missing one fails at import)."""

    schema: Dict[str, str]        # column -> expected kind ("numeric"|"str"|"geometry")
    uniqueness: Sequence[str]     # columns a row must be unique over
    completeness: Sequence[str]   # columns that must be non-null
    freshness: Dict[str, int]     # e.g. {"max_days": 365}


Fetch = Callable[[dg.AssetExecutionContext], gpd.GeoDataFrame]
def raw_geo_asset(
    name: str,
    meta: BronzeAssetMetadata,
    contract: BronzeAssetDataContract,
    fetch: Fetch,
):
    """A bronze asset plus its standard checks.

    The asset reuses the latest S3 snapshot while it is younger than
    ``contract.freshness["max_days"]``; otherwise it calls ``fetch(context)``,
    writes a new timestamped snapshot, and emits its stamp. Returns
    ``(asset, checks)``.
    """

    @dg.asset(name=name, group_name="raw_data", metadata=asdict(meta))
    def _asset(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
        directory = s3_datastore.asset_dir(context)
        last = s3_datastore.latest_timestamp(directory)
        age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

        if age is not None and age <= datetime.timedelta(days=contract.freshness["max_days"]):
            context.log.info(f"Using snapshot for {directory} ({age.days}d old).")
            # Re-emit the existing stamp unchanged: downstream staleness stays FRESH, no rewrite.
            return dg.MaterializeResult(
                data_version=dg.DataVersion(s3_datastore.latest_stamp(directory)),
                metadata={"s3_cache_hit": True},
            )

        context.log.info("Downloading latest dataset")
        stamp = s3_datastore.write_gpq(context, fetch(context))
        return dg.MaterializeResult(
            data_version=dg.DataVersion(stamp),
            metadata={"s3_cache_hit": False},
        )

    return _asset, standard_checks(_asset, contract)
