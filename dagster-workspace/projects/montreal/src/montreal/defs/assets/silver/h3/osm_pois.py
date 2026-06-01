"""Filter OSM POIs to livability categories and H3-index them."""

from dataclasses import asdict

import dagster as dg

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets.bronze import montreal_pois
from montreal.defs.assets.bronze.pois import OSM_POI_TAGS
from montreal.defs.assets.silver._config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_index,
)
from montreal.defs.assets._cache import reuse_if_unchanged
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="OSM POIs filtered to livability categories (grocery/school/health) with h3_r10",
)

ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"geometry": "geometry", "h3_r10": "str", "name": "str", "category": "str"},
    uniqueness=("geometry", "category"),
    completeness=("geometry", "h3_r10", "category"),
)

_POI_CATEGORIES = {
    category: set().union(*tag_groups.values())
    for category, tag_groups in OSM_POI_TAGS.items()
}

@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_pois], code_version=CODE_VERSION)
def h3_montreal_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Filter OSM POIs to livability categories; H3-index."""
    if cached := reuse_if_unchanged(context):
        return cached
    gdf = s3_datastore.read_gpq(context, location_of(montreal_pois))
    if "fclass" not in gdf.columns:
        raise ValueError(f"Expected 'fclass' column. Available: {list(gdf.columns)}")

    fclass_to_cat = {
        fclass: cat
        for cat, fclasses in _POI_CATEGORIES.items()
        for fclass in fclasses
    }
    gdf = gdf.copy()
    gdf["category"] = gdf["fclass"].map(fclass_to_cat)
    gdf = gdf[gdf["category"].notna()].copy()
    if "name" not in gdf.columns: gdf["name"] = None

    h3_indexed = h3_index(gdf)
    final = h3_indexed[["geometry", "h3_r10", "name", "category"]]
    context.log.info(f"h3_montreal_osm_pois: {len(final)} rows across {final['category'].nunique()} categories")

    stamp = s3_datastore.write_gpq(context, final)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": False},
    )

checks = standard_checks(h3_montreal_osm_pois, ASSET_DATA_CONTRACT)
