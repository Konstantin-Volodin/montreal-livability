import io
import urllib.request
import zipfile
from typing import Callable

import dagster as dg
import geopandas as gpd
import pandas as pd

from montreal.defs.resources.lakehouse import s3_datastore

# PARAMS
_MONTREAL_BBOX = (-74.05, 45.35, -73.40, 45.75)
_BRONZE_META = {
    "layer": "bronze",
    "data_category": "geospacial",
}


def _materialize_raw(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
    fetch: Callable[[], gpd.GeoDataFrame],
) -> dg.MaterializeResult:
    """Write fetched data to S3, skipping the download when it already exists.

    On a server restart / re-materialization, if the object is already in the
    bucket we return a cache-hit result without re-downloading the source.
    """
    if s3_datastore.exists(context):
        s3_key = s3_datastore.generate_s3_key(context)
        context.log.info(f"Cache hit for {s3_key}; skipping source download.")
        return dg.MaterializeResult(
            metadata={
                "s3_cache_hit": dg.MetadataValue.bool(True),
                "s3_key": dg.MetadataValue.text(s3_key),
            }
        )

    gdf = fetch()
    s3_datastore.write_gpq(context, gdf)
    return dg.MaterializeResult(metadata={"s3_cache_hit": dg.MetadataValue.bool(False)})


_BRONZE_META.update({"source": "donnees.montreal.ca"})
@dg.asset(group_name="raw_data", metadata=_BRONZE_META)
def montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal address points dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/4ad6baea-4d2c-460f-a8bf-5d000db498f7/resource/866a3dbc-8b59-48ff-866d-f2f9d3bbee9d/download/uniteevaluationfonciere.geojson.zip"
    return _materialize_raw(
        context, s3_datastore, lambda: gpd.read_file(url, compression="zip")
    )


@dg.asset(group_name="raw_data", metadata=_BRONZE_META)
def montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal parks dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json"
    return _materialize_raw(context, s3_datastore, lambda: gpd.read_file(url))


@dg.asset(group_name="raw_data", metadata=_BRONZE_META)
def montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal bike paths dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson"
    return _materialize_raw(context, s3_datastore, lambda: gpd.read_file(url))


@dg.asset(group_name="raw_data", metadata=_BRONZE_META)
def montreal_municipality_boundaries(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the official agglomeration boundary polygons from data.montreal.ca and writes them to S3."""
    url = "https://donnees.montreal.ca/dataset/9797a946-9da8-41ec-8815-f6b276dec7e9/resource/e18bfd07-edc8-4ce8-8a5a-3b617662a794/download/limites-administratives-agglomeration.geojson"
    return _materialize_raw(context, s3_datastore, lambda: gpd.read_file(url))


_BRONZE_META.update({"source": "geofabrik.de"})
@dg.asset(group_name="raw_data", metadata=_BRONZE_META)
def quebec_osm_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Downloads the full Quebec OSM point-of-interest layers from Geofabrik once and writes them to S3."""
    url = "https://download.geofabrik.de/north-america/canada/quebec-latest-free.gpkg.zip"
    layers = ["gis_osm_pois_free", "gis_osm_pois_a_free"]
    minx, miny, maxx, maxy = _MONTREAL_BBOX

    def fetch() -> gpd.GeoDataFrame:
        parts = []
        for layer in layers:
            part = gpd.read_file(url, compression="zip", layer=layer)
            if part.crs is not None and part.crs.to_epsg() != 4326:
                part = part.to_crs(4326)
            clipped = part.cx[minx:maxx, miny:maxy]
            context.log.info(
                f"Layer {layer}: {len(part)} Quebec features -> {len(clipped)} in Montreal bbox"
            )
            parts.append(clipped)
        gdf = pd.concat(parts, ignore_index=True)
        context.log.info(
            f"Combined {len(gdf)} Montreal POIs from Geofabrik Quebec extract."
        )
        return gdf

    return _materialize_raw(context, s3_datastore, fetch)


_BRONZE_META.update({"source": "stm.info"})
@dg.asset(group_name="raw_data", metadata=_BRONZE_META)
def montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Fetches the Montreal transit stops from the STM GTFS feed and writes it to S3."""
    url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"

    def fetch() -> gpd.GeoDataFrame:
        with urllib.request.urlopen(url) as resp:
            with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
                with zf.open("stops.txt") as f:
                    df = pd.read_csv(f)
        return gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["stop_lon"], df["stop_lat"]),
            crs=4326,
        )

    return _materialize_raw(context, s3_datastore, fetch)