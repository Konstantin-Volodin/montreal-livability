import dagster as dg
import geopandas as gpd

from montreal.defs.resources.lakehouse import s3_datastore


@dg.asset(group_name="raw_data", metadata={"layer": "bronze", "source": "donnees.montreal.ca", "data_category": "geospacial", "segmentation": "snapshot", })
def montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal address points dataset from data.montreal.ca and writes it to S3."""    
    url = "https://donnees.montreal.ca/dataset/4ad6baea-4d2c-460f-a8bf-5d000db498f7/resource/866a3dbc-8b59-48ff-866d-f2f9d3bbee9d/download/uniteevaluationfonciere.geojson.zip"
    gdf = gpd.read_file(url, compression='zip')

    # write to s3 (metadata is attached via context.add_output_metadata)
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="raw_data", metadata={"layer": "bronze", "source": "donnees.montreal.ca", "data_category": "geospacial", "segmentation": "snapshot", })
def montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal parks dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json"
    gdf = gpd.read_file(url)

    # write to s3 (metadata is attached via context.add_output_metadata)
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


# Geofabrik publishes Quebec OSM as a shapefile bundle. We read the POI layer
# straight out of the remote zip via GDAL's /vsizip//vsicurl/ virtual FS so we
# never have to download or unzip by hand. `gis_osm_pois_free_1` is the point
# POI layer; each row carries an `fclass` column (restaurant, cafe, bank, ...).
QUEBEC_POIS_VSI = (
    "/vsizip//vsicurl/"
    "https://download.geofabrik.de/north-america/canada/"
    "quebec-latest-free.shp.zip/gis_osm_pois_free_1.shp"
)


@dg.asset(group_name="raw_data", metadata={"layer": "bronze", "source": "geofabrik.de", "data_category": "geospacial", "segmentation": "snapshot", })
def quebec_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Downloads the full Quebec OSM point-of-interest layer from Geofabrik once and writes it to S3.

    Downstream partitioned assets filter this table by POI category instead of
    re-streaming the ~hundreds-of-MB zip per partition.
    """
    gdf = gpd.read_file(QUEBEC_POIS_VSI)
    context.log.info(
        f"Read {len(gdf)} POIs; distinct fclass values: "
        f"{sorted(gdf['fclass'].dropna().unique())}"
    )

    # write to s3 (metadata is attached via context.add_output_metadata)
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


# TODO(user): fill in the curated category -> fclass-values mapping you said
# you'd provide. Keys become the static partition names; each value is the set
# of OSM `fclass` strings that roll up into that category.
POI_CATEGORIES: dict[str, set[str]] = {
    # "food": {"restaurant", "cafe", "fast_food", "bar", "pub"},
    # "retail": {"supermarket", "convenience", "mall"},
    # ...
}

poi_category_partitions = dg.StaticPartitionsDefinition(
    list(POI_CATEGORIES.keys()) or ["__placeholder__"]
)


@dg.asset(
    group_name="raw_data",
    partitions_def=poi_category_partitions,
    deps=[quebec_osm_pois],
    metadata={"layer": "bronze", "source": "geofabrik.de", "data_category": "geospacial", "segmentation": "by_poi_category", },
)
def quebec_pois_by_category(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
) -> dg.MaterializeResult:
    """Filters the full Quebec POI table down to one curated category per partition."""
    category = context.partition_key
    fclasses = POI_CATEGORIES[category]

    gdf = s3_datastore.read_gpq(context, "bronze/quebec_osm_pois.parquet")
    filtered = gdf[gdf["fclass"].isin(fclasses)].copy()
    context.log.info(
        f"Partition '{category}': {len(filtered)} / {len(gdf)} POIs "
        f"matched fclass in {sorted(fclasses)}"
    )

    s3_datastore.write_gpq(context, filtered)
    return dg.MaterializeResult()