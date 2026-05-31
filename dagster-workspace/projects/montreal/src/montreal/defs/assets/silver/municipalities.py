"""Normalize the official Montreal boundary polygons to a reference dimension."""

from dataclasses import asdict

import dagster as dg

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.assets.bronze import montreal_municipality_boundaries
from montreal.defs.assets.silver._config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
)
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="Official Montreal boundary polygons normalized to [municipality, type, geometry] (WGS84)",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"municipality": "str", "type": "str", "geometry": "geometry"},
    uniqueness=("municipality",),
    completeness=("municipality", "type", "geometry"),
)

# asset
@dg.asset(
    group_name="reference",
    metadata=asdict(ASSET_META),
    deps=[montreal_municipality_boundaries],
    code_version=CODE_VERSION,
)
def montreal_municipalities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Normalize the official boundary polygons to ``[municipality, type, geometry]`` (WGS84)."""
    if cached := reuse_if_unchanged(context):
        return cached
    gdf = s3_datastore.read_gpq(context, location_of(montreal_municipality_boundaries))
    for col in ("NOM", "TYPE"):
        if col not in gdf.columns:
            raise ValueError(
                f"Expected column {col!r} on the boundary file. "
                f"Available columns: {list(gdf.columns)}"
            )
    out = gdf.rename(columns={"NOM": "municipality", "TYPE": "type"})[["municipality", "type", "geometry"]]
    context.log.info(f"montreal_municipalities: {len(out)} boundaries ({out['type'].value_counts().to_dict()})")
    stamp = s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

# asset checks
checks = standard_checks(montreal_municipalities, ASSET_DATA_CONTRACT)
