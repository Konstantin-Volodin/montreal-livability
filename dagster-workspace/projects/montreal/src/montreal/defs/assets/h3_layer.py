import dagster as dg
import geopandas as gpd
import h3
import h3pandas

from montreal.defs.assets.raw import (
    montreal_addresses,
    montreal_bike_paths,
    montreal_parks,
    montreal_transit_stops,
    quebec_osm_pois,
)
from montreal.defs.resources.lakehouse import s3_datastore


def _to_wgs84(gdf):
    """h3 needs lat/lng; reproject (or assume 4326 when the CRS is missing)."""
    if gdf.crs is None: return gdf.set_crs(4326, allow_override=True)
    if gdf.crs.to_epsg() != 4326: return gdf.to_crs(4326)
    return gdf

def _h3_index(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add h3_r7 (partition key) and h3_r10 (analysis) columns."""
    pts = gdf.copy()
    pts["geometry"] = gdf.geometry.representative_point()

    pts = pts.h3.geo_to_h3(resolution=7, set_index=False)  # -> column "h3_07"
    pts = pts.h3.geo_to_h3(resolution=10, set_index=False)  # -> column "h3_10"

    # to_numpy() makes the assignment position-based, immune to index reshuffling.
    return gdf.assign(
        h3_r7=pts["h3_07"].to_numpy(),
        h3_r10=pts["h3_10"].to_numpy(),
    )

def _h3_linetrace(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """H3-cover line geometry (bike paths) and add h3_r7 / h3_r10 columns."""
    traced = gdf.h3.linetrace(resolution=10, explode=True)  # adds "h3_linetrace"
    traced = traced[traced["h3_linetrace"].notna()].copy()
    traced = traced.rename(columns={"h3_linetrace": "h3_r10"})
    traced["h3_r7"] = traced["h3_r10"].map(lambda cell: h3.cell_to_parent(cell, 7))
    return traced

_SILVER_META = {
    "layer": "silver",
    "data_category": "geospacial",
    "segmentation": "snapshot",
}

_POI_CATEGORIES = {
    "grocery": {"supermarket", "convenience", "deli"},
    "school": {"school", "college", "university", "kindergarten"},
    "health": {"clinic", "hospital", "pharmacy"},
}


@dg.asset(group_name="h3_indexed_data", metadata=_SILVER_META, deps=[montreal_addresses])
def h3_montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_addresses asset; add h3_r7 and h3_r10 columns for analysis"""
    gdf = _to_wgs84(s3_datastore.read_gpq(context, "bronze/montreal_addresses.parquet"))
    gdf = _h3_index(gdf)
    context.log.info(f"montreal_addresses: {len(gdf)} rows H3-indexed (r7/r10)")
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="h3_indexed_data", metadata=_SILVER_META, deps=[montreal_parks])
def h3_montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_parks asset; add h3_r7 and h3_r10 columns for analysis"""
    gdf = _to_wgs84(s3_datastore.read_gpq(context, "bronze/montreal_parks.parquet"))
    gdf = _h3_index(gdf)
    context.log.info(f"montreal_parks: {len(gdf)} rows H3-indexed (r7/r10)")
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="h3_indexed_data", metadata=_SILVER_META, deps=[quebec_osm_pois])
def h3_montreal_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the quebec_osm_pois asset; add h3_r7 and h3_r10 columns for analysis"""
    gdf = _to_wgs84(s3_datastore.read_gpq(context, "bronze/quebec_osm_pois.parquet"))
    gdf = _h3_index(gdf)
    context.log.info(f"quebec_osm_pois: {len(gdf)} rows H3-indexed (r7/r10)")
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="h3_indexed_data", metadata=_SILVER_META, deps=[montreal_transit_stops])
def h3_montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """apply h3 indexing to the montreal_transit_stops asset; add h3_r7 and h3_r10 columns for analysis"""
    gdf = _to_wgs84(s3_datastore.read_gpq(context, "bronze/montreal_transit_stops.parquet"))
    gdf = _h3_index(gdf)
    context.log.info(f"montreal_transit_stops: {len(gdf)} rows H3-indexed (r7/r10)")
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="h3_indexed_data", metadata=_SILVER_META, deps=[montreal_bike_paths])
def h3_montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """h3-cover the montreal_bike_paths lines (linetrace); add h3_r7 and h3_r10 columns, one row per covered r10 cell"""
    gdf = _to_wgs84(s3_datastore.read_gpq(context, "bronze/montreal_bike_paths.parquet"))
    gdf = _h3_linetrace(gdf)
    context.log.info(f"montreal_bike_paths: {len(gdf)} (path, r10 cell) rows H3-traced")
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="categorized_data", metadata=_SILVER_META, deps=[h3_montreal_osm_pois])
def h3_montreal_osm_pois_categorized(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Filter OSM POIs to livability categories used by the distance layer."""
    gdf = s3_datastore.read_gpq(context, "silver/h3_montreal_osm_pois.parquet")
    if "fclass" not in gdf.columns:
        raise ValueError(
            "Expected Geofabrik POI class column 'fclass'. "
            f"Available columns: {list(gdf.columns)}"
        )

    fclass_to_category = {
        fclass: category
        for category, fclasses in _POI_CATEGORIES.items()
        for fclass in fclasses
    }
    categorized = gdf.copy()
    categorized["category"] = categorized["fclass"].map(fclass_to_category)
    categorized = categorized[categorized["category"].notna()].copy()

    if "name" not in categorized.columns:
        categorized["name"] = None

    categorized = categorized[["geometry", "h3_r7", "h3_r10", "name", "category"]]
    context.log.info(
        "h3_montreal_osm_pois_categorized: "
        f"{len(categorized)} rows across "
        f"{categorized['category'].nunique()} categories"
    )
    s3_datastore.write_gpq(context, categorized)
    return dg.MaterializeResult()
