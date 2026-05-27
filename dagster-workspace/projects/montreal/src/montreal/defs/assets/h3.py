import dagster as dg
import geopandas as gpd
import h3
import h3pandas

from montreal.defs.assets.raw import (
    montreal_addresses,
    montreal_bike_paths,
    montreal_municipality_boundaries,
    montreal_parks,
    montreal_transit_stops,
    montreal_pois,
)
from montreal.defs.resources.lakehouse import location_of, s3_datastore

# dynamic r6 partitions for the address layer
r6_partitions = dg.DynamicPartitionsDefinition(name="address_r6")


def _h3_index(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add the h3_r10 analysis column."""
    pts = gdf.copy()
    pts["geometry"] = gdf.geometry.representative_point()

    pts = pts.h3.geo_to_h3(resolution=10, set_index=False)  # -> column "h3_10"

    # to_numpy() makes the assignment position-based, immune to index reshuffling.
    return gdf.assign(h3_r10=pts["h3_10"].to_numpy())

def _h3_linetrace(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """H3-cover line geometry (bike paths) and add the h3_r10 column."""
    traced = gdf.h3.linetrace(resolution=10, explode=True)  # adds "h3_linetrace"
    traced = traced[traced["h3_linetrace"].notna()].copy()
    return traced.rename(columns={"h3_linetrace": "h3_r10"})

_SILVER_META = {
    "layer": "silver",
    "data_category": "geospacial",
    "segmentation": "snapshot",
}

# Re-derive whenever an upstream asset produces a new snapshot (data version).
_EAGER = dg.AutomationCondition.eager()

_POI_CATEGORIES = {
    "grocery": {"supermarket", "convenience", "greengrocer", "bakery", "butcher"},
    "school": {"school", "college", "university", "kindergarten"},
    "health": {"clinic", "hospital", "pharmacy", "doctors", "dentist"},
}


@dg.asset(group_name="H3_indexed", metadata=_SILVER_META, deps=[montreal_addresses], automation_condition=_EAGER)
def h3_montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """H3-index addresses, shard the output by r6, and reconcile r6 partitions."""
    gdf = s3_datastore.read_gpq(context, location_of(montreal_addresses))
    gdf = _h3_index(gdf)
    gdf["h3_r6"] = gdf["h3_r10"].map(lambda cell: str(h3.cell_to_parent(cell, 6)))
    gdf = gdf[gdf["h3_r6"].notna()]
    context.log.info(f"montreal_addresses: {len(gdf)} rows H3-indexed (r10 + r6)")

    desired = set(gdf["h3_r6"].unique())
    existing = set(context.instance.get_dynamic_partitions(r6_partitions.name))
    context.instance.add_dynamic_partitions(r6_partitions.name, sorted(desired - existing))
    for stale in sorted(existing - desired):
        context.instance.delete_dynamic_partition(r6_partitions.name, stale)
    context.log.info(
        f"r6 partitions: {len(desired)} cells "
        f"(+{len(desired - existing)} / -{len(existing - desired)})"
    )

    stamp = s3_datastore.write_gpq_partitioned(context, gdf, "h3_r6")
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)


@dg.asset(group_name="reference", metadata=_SILVER_META, deps=[montreal_municipality_boundaries], automation_condition=_EAGER)
def montreal_municipalities(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Normalize the official boundary polygons to ``[municipality, type, geometry]`` (WGS84)."""
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
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)


@dg.asset(group_name="H3_indexed", metadata=_SILVER_META, deps=[montreal_parks], automation_condition=_EAGER)
def h3_montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_parks asset; add the h3_r10 analysis column"""
    gdf = s3_datastore.read_gpq(context, location_of(montreal_parks))
    gdf = _h3_index(gdf)
    context.log.info(f"montreal_parks: {len(gdf)} rows H3-indexed (r10)")
    stamp = s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)


@dg.asset(group_name="H3_indexed", metadata=_SILVER_META, deps=[montreal_transit_stops], automation_condition=_EAGER)
def h3_montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_transit_stops asset; add the h3_r10 analysis column"""
    gdf = s3_datastore.read_gpq(context, location_of(montreal_transit_stops))
    gdf = _h3_index(gdf)
    context.log.info(f"montreal_transit_stops: {len(gdf)} rows H3-indexed (r10)")
    stamp = s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)


@dg.asset(group_name="H3_indexed", metadata=_SILVER_META, deps=[montreal_bike_paths], automation_condition=_EAGER)
def h3_montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """h3-cover the montreal_bike_paths lines (linetrace); add h3_r10, one row per covered r10 cell"""
    gdf = s3_datastore.read_gpq(context, location_of(montreal_bike_paths))
    gdf = _h3_linetrace(gdf)
    context.log.info(f"montreal_bike_paths: {len(gdf)} (path, r10 cell) rows H3-traced")
    stamp = s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)


@dg.asset(group_name="H3_indexed", metadata=_SILVER_META, deps=[montreal_pois], automation_condition=_EAGER)
def h3_montreal_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Filter OSM POIs to livability categories used by the distance layer."""

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
    h3_indexed = _h3_index(categorized)
    context.log.info("h3_montreal_osm_pois: added the h3_r10 analysis column")

    final = h3_indexed[["geometry", "h3_r10", "name", "category"]]
    context.log.info(
        "h3_montreal_osm_pois: "
        f"{len(final)} rows across "
        f"{final['category'].nunique()} categories"
    )

    stamp = s3_datastore.write_gpq(context, final)
    return dg.MaterializeResult(data_version=dg.DataVersion(stamp) if stamp else None)
