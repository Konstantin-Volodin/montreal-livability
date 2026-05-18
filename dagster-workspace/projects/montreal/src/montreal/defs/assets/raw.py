import io
import urllib.request
import zipfile

import dagster as dg
import geopandas as gpd
import pandas as pd

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


@dg.asset(group_name="raw_data", metadata={"layer": "bronze", "source": "geofabrik.de", "data_category": "geospacial", "segmentation": "snapshot", })
def quebec_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Downloads the full Quebec OSM point-of-interest layers from Geofabrik once and writes them to S3.

    Geofabrik splits POIs across two layers: ``gis_osm_pois_free`` (point
    features) and ``gis_osm_pois_a_free`` (areas mapped as polygons, e.g. a
    school or hospital footprint)
    """
    url = "https://download.geofabrik.de/north-america/canada/quebec-latest-free.gpkg.zip"
    layers = ["gis_osm_pois_free", "gis_osm_pois_a_free"]
    parts = []
    for layer in layers:
        part = gpd.read_file(url, compression="zip", layer=layer)
        context.log.info(f"Read {len(part)} features from layer {layer}")
        parts.append(part)
    gdf = pd.concat(parts, ignore_index=True)
    context.log.info(f"Combined {len(gdf)} POIs from Geofabrik Quebec extract.")

    # write to s3 (metadata is attached via context.add_output_metadata)
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="raw_data", metadata={"layer": "bronze", "source": "donnees.montreal.ca", "data_category": "geospacial", "segmentation": "snapshot", })
def montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal bike paths dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson"
    gdf = gpd.read_file(url)

    # write to s3 (metadata is attached via context.add_output_metadata)
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()


@dg.asset(group_name="raw_data", metadata={"layer": "bronze", "source": "stm.info", "data_category": "geospacial", "segmentation": "snapshot", })
def montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal transit stops from the STM GTFS feed and writes it to S3."""
    url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"
    with urllib.request.urlopen(url) as resp:
        with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
            with zf.open("stops.txt") as f:
                df = pd.read_csv(f)
    gdf = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(df["stop_lon"], df["stop_lat"]),crs=4326,)

    # write to s3 (metadata is attached via context.add_ output_metadata)
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult()