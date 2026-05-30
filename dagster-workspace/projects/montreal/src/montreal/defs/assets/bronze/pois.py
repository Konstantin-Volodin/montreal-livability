"""
Montreal points of interest: query OpenStreetMap via Overpass, cache on S3, validate.
"""

import json
import urllib.parse
import urllib.request

import dagster as dg
import geopandas as gpd
from shapely.geometry import Point

from montreal.defs.assets.bronze._config import (
    BronzeAssetDataContract,
    BronzeAssetMetadata,
    raw_geo_asset,
)

ASSET_META = BronzeAssetMetadata(
    layer="bronze",
    data_category="geospatial",
    source="overpass-api.de",
    description="OpenStreetMap points of interest (grocery, school, health) for the Montréal bbox",
    url="https://overpass-api.de/api/interpreter",
)

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
    freshness={"max_days": 25},
)

_MONTREAL_BBOX = (-74.05, 45.35, -73.40, 45.75)

# livability category -> {OSM tag key -> accepted values}. 
OSM_POI_TAGS = {
    "grocery": {"shop": {"supermarket", "convenience", "greengrocer", "bakery", "butcher"}},
    "school": {"amenity": {"school", "college", "university", "kindergarten"}},
    "health": {"amenity": {"clinic", "hospital", "pharmacy", "doctors", "dentist"}},
}

# update OSM tag into the form Overpass query can use
_TAGS_BY_KEY: dict[str, set[str]] = {}
for _groups in OSM_POI_TAGS.values():
    for _key, _vals in _groups.items():
        _TAGS_BY_KEY.setdefault(_key, set()).update(_vals)


def _overpass_query(bbox: tuple[float, float, float, float]) -> str:
    """Overpass QL selecting every node/way/relation in `bbox` matching _TAGS_BY_KEY."""
    minx, miny, maxx, maxy = bbox
    area = f"{miny},{minx},{maxy},{maxx}"
    selectors = [
        f'nwr["{key}"~"^({"|".join(sorted(values))})$"]({area});'
        for key, values in _TAGS_BY_KEY.items()
    ]
    return "\n".join(["[out:json][timeout:180];", "(", *selectors, ");", "out tags center;"])


def _fclass(tags: dict) -> str | None:
    """The accepted OSM value present on `tags` (kept as the row's fclass), or None."""
    for key, values in _TAGS_BY_KEY.items():
        if tags.get(key) in values:
            return tags[key]
    return None


def _lon_lat(element: dict) -> tuple[float | None, float | None]:
    """An element's coordinates, from its own lon/lat or its `center` (ways/relations)."""
    if "lon" in element and "lat" in element:
        return element["lon"], element["lat"]
    center = element.get("center") or {}
    return center.get("lon"), center.get("lat")


def _post_overpass(context: dg.AssetExecutionContext, query: str) -> list[dict]:
    """POST `query` to Overpass and return its raw element list."""
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
        return json.loads(resp.read()).get("elements", [])


def _elements_to_points(elements: list[dict]) -> gpd.GeoDataFrame:
    """WGS84 point frame from Overpass elements, dropping unclassifiable, locationless, or duplicate rows."""
    rows, seen = [], set()
    for element in elements:
        tags = element.get("tags") or {}
        fclass = _fclass(tags)
        lon, lat = _lon_lat(element)
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

    # overpass can return nothing, or every element can be filtered out.
    columns = ["osm_type", "osm_id", "name", "fclass", "geometry"]
    return gpd.GeoDataFrame(rows, columns=columns, geometry="geometry", crs=4326)


def _read_pois(context: dg.AssetExecutionContext) -> gpd.GeoDataFrame:
    elements = _post_overpass(context, _overpass_query(_MONTREAL_BBOX))
    gdf = _elements_to_points(elements)
    context.log.info(
        f"Overpass Montréal POIs: {len(gdf)} rows across "
        f"{gdf['fclass'].nunique() if not gdf.empty else 0} classes"
    )
    return gdf


montreal_pois, checks = raw_geo_asset(
    "montreal_pois",
    ASSET_META,
    ASSET_DATA_CONTRACT,
    fetch=_read_pois,
)
