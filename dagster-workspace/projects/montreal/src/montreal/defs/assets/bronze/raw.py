import io
import json
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Callable

import dagster as dg
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from montreal.defs.resources.lakehouse import s3_datastore

# Bronze sources refresh monthly: on_cron nudges each root on the 1st, the
# age check below makes that tick actually re-download, and the freshness
# policy surfaces a FAIL pill if a deadline is missed.
_MONTHLY_CRON = "0 0 1 * *"
_MAX_RAW_AGE = timedelta(days=28)
_RAW_AUTOMATION = dg.AutomationCondition.on_cron(_MONTHLY_CRON)
_RAW_FRESHNESS = dg.FreshnessPolicy.cron(
    deadline_cron=_MONTHLY_CRON, lower_bound_delta=timedelta(days=2)
)

# PARAMS
_MONTREAL_BBOX = (-74.05, 45.35, -73.40, 45.75)
_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
_OSM_POI_TAGS = {
    "grocery": {
        "shop": {"supermarket", "convenience", "greengrocer", "bakery", "butcher"},
    },
    "school": {
        "amenity": {"school", "college", "university", "kindergarten"},
    },
    "health": {
        "amenity": {"clinic", "hospital", "pharmacy", "doctors", "dentist"},
    },
}
_BRONZE_META = {
    "layer": "bronze",
    "data_category": "geospacial",
}


class RawFetchConfig(dg.Config):
    force_refresh: bool = False


def _materialize_raw(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
    fetch: Callable[[], gpd.GeoDataFrame],
    *,
    force_refresh: bool = False,
) -> dg.MaterializeResult:
    """Write fetched data to S3, re-downloading only when the snapshot is stale.

    The data version is the snapshot timestamp: a cache hit re-emits the existing
    version (so downstream does not cascade), a fresh download emits a new one.
    """
    directory = s3_datastore.asset_dir(context)
    last = s3_datastore.latest_timestamp(directory)
    age = None if last is None else datetime.now(timezone.utc) - last

    if not force_refresh and age is not None and age <= _MAX_RAW_AGE:
        context.log.info(f"Fresh snapshot for {directory} ({age.days}d old); skipping download.")
        return dg.MaterializeResult(
            data_version=dg.DataVersion(f"{last:%Y%m%dT%H%M%S_%f}Z"),
            metadata={
                "s3_cache_hit": dg.MetadataValue.bool(True),
                "s3_location": dg.MetadataValue.text(directory),
                "snapshot_age_days": dg.MetadataValue.int(age.days),
            },
        )

    stamp = s3_datastore.write_gpq(context, fetch())
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={"s3_cache_hit": dg.MetadataValue.bool(False)},
    )


_BRONZE_META.update({"source": "donnees.montreal.ca"})
@dg.asset(group_name="raw_data", metadata=_BRONZE_META,
          automation_condition=_RAW_AUTOMATION, freshness_policy=_RAW_FRESHNESS)
def montreal_addresses(context: dg.AssetExecutionContext, s3_datastore: s3_datastore, config: RawFetchConfig) -> dg.MaterializeResult:
    """Fetches the Montreal address points dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/4ad6baea-4d2c-460f-a8bf-5d000db498f7/resource/866a3dbc-8b59-48ff-866d-f2f9d3bbee9d/download/uniteevaluationfonciere.geojson.zip"
    return _materialize_raw(
        context, s3_datastore, lambda: gpd.read_file(url, compression="zip"),
        force_refresh=config.force_refresh,
    )


@dg.asset(group_name="raw_data", metadata=_BRONZE_META,
          automation_condition=_RAW_AUTOMATION, freshness_policy=_RAW_FRESHNESS)
def montreal_parks(context: dg.AssetExecutionContext, s3_datastore: s3_datastore, config: RawFetchConfig) -> dg.MaterializeResult:
    """Fetches the Montreal parks dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/2e9e4d2f-173a-4c3d-a5e3-565d79baa27d/resource/35796624-15df-4503-a569-797665f8768e/download/espace_vert.json"
    return _materialize_raw(context, s3_datastore, lambda: gpd.read_file(url), force_refresh=config.force_refresh)


@dg.asset(group_name="raw_data", metadata=_BRONZE_META,
          automation_condition=_RAW_AUTOMATION, freshness_policy=_RAW_FRESHNESS)
def montreal_bike_paths(context: dg.AssetExecutionContext, s3_datastore: s3_datastore, config: RawFetchConfig) -> dg.MaterializeResult:
    """Fetches the Montreal bike paths dataset from data.montreal.ca and writes it to S3."""
    url = "https://donnees.montreal.ca/dataset/5ea29f40-1b5b-4f34-85b3-7c67088ff536/resource/0dc6612a-be66-406b-b2d9-59c9e1c65ebf/download/reseau_cyclable.geojson"
    return _materialize_raw(context, s3_datastore, lambda: gpd.read_file(url), force_refresh=config.force_refresh)


@dg.asset(group_name="raw_data", metadata=_BRONZE_META,
          automation_condition=_RAW_AUTOMATION, freshness_policy=_RAW_FRESHNESS)
def montreal_municipality_boundaries(context: dg.AssetExecutionContext, s3_datastore: s3_datastore, config: RawFetchConfig) -> dg.MaterializeResult:
    """Fetches the official agglomeration boundary polygons from data.montreal.ca and writes them to S3."""
    url = "https://donnees.montreal.ca/dataset/9797a946-9da8-41ec-8815-f6b276dec7e9/resource/e18bfd07-edc8-4ce8-8a5a-3b617662a794/download/limites-administratives-agglomeration.geojson"
    return _materialize_raw(context, s3_datastore, lambda: gpd.read_file(url), force_refresh=config.force_refresh)


def _overpass_query_for_bbox(bbox: tuple[float, float, float, float]) -> str:
    minx, miny, maxx, maxy = bbox
    overpass_bbox = f"{miny},{minx},{maxy},{maxx}"
    selectors = []
    for tag_groups in _OSM_POI_TAGS.values():
        for key, values in tag_groups.items():
            pattern = "|".join(sorted(values))
            selectors.append(f'nwr["{key}"~"^({pattern})$"]({overpass_bbox});')

    return "\n".join(
        [
            "[out:json][timeout:180];",
            "(",
            *selectors,
            ");",
            "out tags center;",
        ]
    )


def _osm_fclass(tags: dict) -> str | None:
    for tag_groups in _OSM_POI_TAGS.values():
        for key, values in tag_groups.items():
            value = tags.get(key)
            if value in values:
                return value
    return None


def _osm_lon_lat(element: dict) -> tuple[float | None, float | None]:
    if "lon" in element and "lat" in element:
        return element["lon"], element["lat"]
    center = element.get("center") or {}
    return center.get("lon"), center.get("lat")


def _read_osm_pois_from_overpass(
    context: dg.AssetExecutionContext,
    bbox: tuple[float, float, float, float] = _MONTREAL_BBOX,
) -> gpd.GeoDataFrame:
    query = _overpass_query_for_bbox(bbox)
    payload = urllib.parse.urlencode({"data": query}).encode("utf-8")
    req = urllib.request.Request(
        _OVERPASS_URL,
        data=payload,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": "montreal-livability/1.0",
        },
        method="POST",
    )
    context.log.info("Querying Overpass for Montréal OSM POIs.")
    with urllib.request.urlopen(req, timeout=240) as resp:
        body = resp.read()

    elements = json.loads(body).get("elements", [])
    rows = []
    seen = set()
    for element in elements:
        tags = element.get("tags") or {}
        fclass = _osm_fclass(tags)
        lon, lat = _osm_lon_lat(element)
        if fclass is None or lon is None or lat is None:
            continue

        osm_type = element.get("type")
        osm_id = element.get("id")
        dedupe_key = (osm_type, osm_id)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        rows.append(
            {
                "osm_type": osm_type,
                "osm_id": osm_id,
                "name": tags.get("name"),
                "fclass": fclass,
                "geometry": Point(lon, lat),
            }
        )

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=4326)
    context.log.info(
        "Overpass Montréal POIs: "
        f"{len(gdf)} rows across {gdf['fclass'].nunique() if not gdf.empty else 0} classes"
    )
    return gdf


_BRONZE_META.update({"source": "overpass-api.de"})
@dg.asset(group_name="raw_data", metadata=_BRONZE_META,
          automation_condition=_RAW_AUTOMATION, freshness_policy=_RAW_FRESHNESS)
def montreal_pois(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
    config: RawFetchConfig,
) -> dg.MaterializeResult:
    """Query OSM POIs for the Montréal bbox and write them to S3."""
    return _materialize_raw(
        context,
        s3_datastore,
        lambda: _read_osm_pois_from_overpass(context),
        force_refresh=config.force_refresh,
    )


_BRONZE_META.update({"source": "stm.info"})
@dg.asset(group_name="raw_data", metadata=_BRONZE_META,
          automation_condition=_RAW_AUTOMATION, freshness_policy=_RAW_FRESHNESS)
def montreal_transit_stops(context: dg.AssetExecutionContext, s3_datastore: s3_datastore, config: RawFetchConfig) -> dg.MaterializeResult:
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

    return _materialize_raw(context, s3_datastore, fetch, force_refresh=config.force_refresh)
