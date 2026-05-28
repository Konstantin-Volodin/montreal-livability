"""Filter OSM POIs to livability categories and H3-index them."""

from dataclasses import asdict

import dagster as dg

from montreal.defs.assets.bronze import montreal_pois
from montreal.defs.assets.silver.config import (
    SilverAssetDataContract,
    SilverAssetMetadata,
    h3_index,
)
from montreal.defs.checks.factory import (
    field_completeness_factory,
    row_uniqueness_factory,
    schema_contract_factory,
)
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# Bump to force a recompute when this asset's logic changes, even if inputs haven't.
CODE_VERSION = "1"

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

_POI_CATEGORIES = {
    "grocery": {"supermarket", "convenience", "greengrocer", "bakery", "butcher"},
    "school": {"school", "college", "university", "kindergarten"},
    "health": {"clinic", "hospital", "pharmacy", "doctors", "dentist"},
}

# asset
@dg.asset(group_name="H3_indexed", metadata=asdict(ASSET_META), deps=[montreal_pois], code_version=CODE_VERSION)
def h3_montreal_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Filter OSM POIs to livability categories used by the distance layer."""
    if s3_datastore.should_skip(context, [location_of(montreal_pois)], code_version=CODE_VERSION):
        return s3_datastore.reemit_latest(context)

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
osm_pois_schema = schema_contract_factory(h3_montreal_osm_pois, ASSET_DATA_CONTRACT.schema)
osm_pois_uniqueness = row_uniqueness_factory(h3_montreal_osm_pois, ASSET_DATA_CONTRACT.uniqueness)
osm_pois_completeness = field_completeness_factory(h3_montreal_osm_pois, ASSET_DATA_CONTRACT.completeness)
