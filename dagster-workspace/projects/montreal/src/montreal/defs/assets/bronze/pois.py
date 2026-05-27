"""
Montreal points of interest: query OpenStreetMap via Overpass, cache on S3, validate.
"""

import dagster as dg
import geopandas as gpd
import datetime
import json
import urllib.parse
import urllib.request
from dataclasses import asdict
from shapely.geometry import Point

from montreal.defs.resources.lakehouse import s3_datastore
from montreal.defs.assets.bronze.config import (
    BronzeAssetDataContract, 
    BronzeAssetMetadata,
)
from montreal.defs.checks.factory import (
    field_completeness_factory,
    row_uniqueness_factory,
    schema_contract_factory,
    snapshot_freshness_factory,
)

# metadata
ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="overpass-api.de",
    description="OpenStreetMap points of interest (grocery, school, health) for the Montréal bbox",
    url="https://overpass-api.de/api/interpreter",
)

# data contract
ASSET_DATA_CONTRACT = BronzeAssetDataContract(
    schema={
        "osm_type": "str",
        "osm_id": "numeric",
        "name": "str",
        "fclass": "str",
        "geometry": "geometry",
    },
    uniqueness=("osm_id",),
    completeness=("fclass", "geometry"),
    freshness={"max_days": 28},
)

_MONTREAL_BBOX = (-74.05, 45.35, -73.40, 45.75)
_OSM_POI_TAGS = {
    "grocery": {"shop": {"supermarket", "convenience", "greengrocer", "bakery", "butcher"}},
    "school": {"amenity": {"school", "college", "university", "kindergarten"}},
    "health": {"amenity": {"clinic", "hospital", "pharmacy", "doctors", "dentist"}},
}

def _overpass_query_for_bbox(bbox: tuple[float, float, float, float]) -> str:
    minx, miny, maxx, maxy = bbox
    overpass_bbox = f"{miny},{minx},{maxy},{maxx}"
    selectors = []
    for tag_groups in _OSM_POI_TAGS.values():
        for key, values in tag_groups.items():
            pattern = "|".join(sorted(values))
            selectors.append(f'nwr["{key}"~"^({pattern})$"]({overpass_bbox});')

    return "\n".join(["[out:json][timeout:180];", "(", *selectors, ");", "out tags center;"])


def _osm_fclass(tags: dict) -> str | None:
    for tag_groups in _OSM_POI_TAGS.values():
        for key, values in tag_groups.items():
            if tags.get(key) in values:
                return tags[key]
    return None


def _osm_lon_lat(element: dict) -> tuple[float | None, float | None]:
    if "lon" in element and "lat" in element:
        return element["lon"], element["lat"]
    center = element.get("center") or {}
    return center.get("lon"), center.get("lat")


def _read_osm_pois_from_overpass(context: dg.AssetExecutionContext) -> gpd.GeoDataFrame:
    query = _overpass_query_for_bbox(_MONTREAL_BBOX)
    payload = urllib.parse.urlencode({"data": query}).encode("utf-8")
    req = urllib.request.Request(
        ASSET_META.url,
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

    rows = []
    seen = set()
    for element in json.loads(body).get("elements", []):
        tags = element.get("tags") or {}
        fclass = _osm_fclass(tags)
        lon, lat = _osm_lon_lat(element)
        if fclass is None or lon is None or lat is None:
            continue

        dedupe_key = (element.get("type"), element.get("id"))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        rows.append(
            {
                "osm_type": element.get("type"),
                "osm_id": element.get("id"),
                "name": tags.get("name"),
                "fclass": fclass,
                "geometry": Point(lon, lat),
            }
        )

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=4326)
    context.log.info(
        f"Overpass Montréal POIs: {len(gdf)} rows across "
        f"{gdf['fclass'].nunique() if not gdf.empty else 0} classes"
    )
    return gdf


@dg.asset(group_name="raw_data", metadata=asdict(ASSET_META))
def montreal_pois(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """Query OSM POIs for the Montréal bbox, reusing the S3 snapshot while it is within the freshness window."""
    directory = s3_datastore.asset_dir(context)
    last = s3_datastore.latest_timestamp(directory)
    age = None if last is None else datetime.datetime.now(datetime.timezone.utc) - last

    if age is not None and age <= datetime.timedelta(days=ASSET_DATA_CONTRACT.freshness["max_days"]):
        context.log.info(f"Using snapshot for {directory} ({age.days}d old).")
        s3_datastore.describe_latest(context, directory)
        return dg.MaterializeResult(
            data_version=dg.DataVersion(f"{last:%Y%m%dT%H%M%S_%f}Z"),
            metadata={"s3_cache_hit": True, "snapshot_age_days": age.days},
        )

    context.log.info("Downloading latest dataset")
    data = _read_osm_pois_from_overpass(context)
    stamp = s3_datastore.write_gpq(context, data)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={"s3_cache_hit": False},
    )

# asset checks
pois_freshness = snapshot_freshness_factory(montreal_pois, ASSET_DATA_CONTRACT.freshness)
pois_schema = schema_contract_factory(montreal_pois, ASSET_DATA_CONTRACT.schema)
pois_uniqueness = row_uniqueness_factory(montreal_pois, ASSET_DATA_CONTRACT.uniqueness)
pois_completeness = field_completeness_factory(montreal_pois, ASSET_DATA_CONTRACT.completeness)
