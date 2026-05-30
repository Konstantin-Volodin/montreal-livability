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
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore, skip

# metadata
ASSET_META = SilverAssetMetadata(
    layer="silver",
    data_category="geospatial",
    segmentation="snapshot",
    description="OSM POIs filtered to livability categories (grocery/school/health) with h3_r10",
)

# data contract
ASSET_DATA_CONTRACT = SilverAssetDataContract(
    schema={"geometry": "geometry", "h3_r10": "str", "name": "str", "category": "str"},
    uniqueness=("geometry", "category"),
    completeness=("geometry", "h3_r10", "category"),
)

# fclass values per category, flattened from the bronze Overpass tag map.
_POI_CATEGORIES = {
    category: set().union(*tag_groups.values())
    for category, tag_groups in OSM_POI_TAGS.items()
}

# asset
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_pois], code_version=CODE_VERSION)
def h3_montreal_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Filter OSM POIs to livability categories used by the distance layer."""
    if skip.should_skip(s3_datastore, context, [location_of(montreal_pois)], code_version=CODE_VERSION):
        return skip.reemit_latest(s3_datastore, context)

    # read data
    gdf = s3_datastore.read_gpq(context, location_of(montreal_pois))
    if "fclass" not in gdf.columns:
        raise ValueError(
            "Expected Geofabrik POI class column 'fclass'. "
            f"Available columns: {list(gdf.columns)}"
        )

    # categorize
    fclass_to_category = {
        fclass: category
        for category, fclasses in _POI_CATEGORIES.items()
        for fclass in fclasses
    }
    categorized = gdf.copy()
    categorized["category"] = categorized["fclass"].map(fclass_to_category)
    categorized = categorized[categorized["category"].notna()].copy()
    context.log.info(fclass_to_category)

    if "name" not in categorized.columns:
        categorized["name"] = None

    # h3 indexing
    h3_indexed = h3_index(categorized)
    context.log.info("h3_montreal_osm_pois: added the h3_r10 analysis column")

    final = h3_indexed[["geometry", "h3_r10", "name", "category"]]
    context.log.info(
        "h3_montreal_osm_pois: "
        f"{len(final)} rows across "
        f"{final['category'].nunique()} categories"
    )

    stamp = s3_datastore.write_gpq(context, final, code_version=CODE_VERSION)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)

# asset checks
checks = standard_checks(h3_montreal_osm_pois, ASSET_DATA_CONTRACT)
