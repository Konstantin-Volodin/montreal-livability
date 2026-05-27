"""HTML rendering for the livability report."""

import unicodedata
from pathlib import Path

import branca.colormap as cm
import folium
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from montreal.defs.assets.distance import POI_CATEGORIES

POI_LABELS = {
    "grocery": "Grocery stores",
    "school": "Schools",
    "health": "Health",
    "park": "Parks",
    "transit": "Transit",
    "bike": "Bike paths",
}
TABLE_LABELS = {
    "grocery": "Gro.",
    "school": "Sch.",
    "health": "Health",
    "transit": "Transit",
    "park": "Park",
    "bike": "Bike",
}
SCORE_WEIGHTS = {
    "score_grocery": 0.20,
    "score_transit": 0.20,
    "score_park": 0.20,
    "score_bike": 0.15,
    "score_school": 0.15,
    "score_health": 0.10,
}
ENV = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)
COLORMAP = cm.LinearColormap(
    ["#d7191c", "#fdae61", "#ffffbf", "#a6d96a", "#1a9641"],
    vmin=0.0,
    vmax=100.0,
    caption="Score (0-100)",
)


def _key(value: object) -> str:
    return unicodedata.normalize("NFKC", str(value)).casefold().strip()


def _round_coords(coords):
    if isinstance(coords, (float, int)):
        return round(coords, 6)
    return [_round_coords(c) for c in coords]


def _feature(row, properties: dict) -> dict:
    geometry = row.geometry.__geo_interface__
    return {
        "type": "Feature",
        "geometry": {
            "type": geometry["type"],
            "coordinates": _round_coords(geometry["coordinates"]),
        },
        "properties": properties,
    }


def _hex_feature_collection(hexes: pd.DataFrame) -> dict:
    features = []
    for row in hexes.itertuples(index=False):
        properties = {
            "municipality": str(row.municipality),
            "addresses": int(row.addresses),
            "livability": round(float(row.livability), 1),
        }
        for category in POI_CATEGORIES:
            properties[f"score_{category}"] = round(
                float(getattr(row, f"score_{category}")), 1
            )
        features.append(_feature(row, properties))
    return {"type": "FeatureCollection", "features": features}


def _boundary_feature_collection(boundaries: pd.DataFrame) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            _feature(row, {"municipality": str(row.municipality)})
            for row in boundaries.itertuples(index=False)
        ],
    }


def build_map_html(hexes: pd.DataFrame, boundaries: pd.DataFrame) -> str:
    centroids = hexes["geometry"].map(lambda poly: poly.centroid)
    fmap = folium.Map(
        location=[
            centroids.map(lambda p: p.y).mean(),
            centroids.map(lambda p: p.x).mean(),
        ],
        zoom_start=11,
        tiles="cartodbpositron",
    )
    COLORMAP.add_to(fmap)

    metrics = [("livability", "Overall livability")]
    metrics += [(f"score_{c}", c.capitalize()) for c in POI_CATEGORIES]
    hex_layer = folium.GeoJson(
        _hex_feature_collection(hexes),
        name="Livability cells",
        style_function=lambda feature: {
            "fillColor": COLORMAP(feature["properties"]["livability"]),
            "color": COLORMAP(feature["properties"]["livability"]),
            "weight": 0,
            "fillOpacity": 0.6,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["municipality", "addresses", *[key for key, _label in metrics]],
            aliases=[
                "Municipality",
                "Addresses",
                *[label for _key, label in metrics],
            ],
            localize=True,
            sticky=False,
        ),
    )
    hex_layer.add_to(fmap)

    boundary_collection = _boundary_feature_collection(boundaries)
    bounds = folium.FeatureGroup(name="Municipality boundaries", show=True)
    folium.GeoJson(
        boundary_collection,
        style_function=lambda _f: {
            "fill": False,
            "color": "#39495c",
            "weight": 0.8,
            "opacity": 0.48,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["municipality"],
            aliases=["Municipality"],
        ),
    ).add_to(bounds)
    bounds.add_to(fmap)

    map_name = fmap.get_name()
    hex_layer_name = hex_layer.get_name()
    boundary_group_name = bounds.get_name()
    map_html = fmap.get_root().render()
    boundary_features = {
        _key(feature["properties"]["municipality"]): feature
        for feature in boundary_collection["features"]
    }
    map_script = ENV.get_template("map_interactions.html").render(
        map_name=map_name,
        hex_layer_name=hex_layer_name,
        boundary_group_name=boundary_group_name,
        boundary_features=boundary_features,
        metric_weights=SCORE_WEIGHTS,
    )
    return map_html.replace(
        "</html>",
        f"{map_script}\n</html>",
        1,
    )


def render_report(*, stats: dict, table: pd.DataFrame, map_html: str) -> str:
    summary_stats = [
        ("Addresses scored", f"{stats['addresses']:,}"),
        ("Amenities indexed", f"{stats['amenities']:,}"),
        ("Municipalities", str(stats["municipalities"])),
        ("Mean livability", f"{stats['mean_livability']:.1f}"),
    ]
    category_total = max(sum(stats["by_category"].get(c, 0) for c in POI_CATEGORIES), 1)
    category_stats = sorted(
        [
            {
                "label": POI_LABELS.get(c, c.capitalize()),
                "value": f"{stats['by_category'].get(c, 0):,}",
                "count": stats["by_category"].get(c, 0),
                "share": round(stats["by_category"].get(c, 0) / category_total * 100, 1),
            }
            for c in POI_CATEGORIES
        ],
        key=lambda item: item["count"],
        reverse=True,
    )

    columns = ["#", "Municipality", "Addresses", "Livability"]
    columns += [TABLE_LABELS.get(c, c.capitalize()) for c in POI_CATEGORIES]
    rows = [
        {
            "rank": rank,
            "municipality": str(r.municipality),
            "municipality_key": _key(r.municipality),
            "addresses": f"{int(r.addresses):,}",
            "livability": f"{r.livability:.1f}",
            "livability_bg": COLORMAP(r.livability),
            "scores": [f"{getattr(r, f'score_{c}'):.0f}" for c in POI_CATEGORIES],
        }
        for rank, r in enumerate(table.itertuples(index=False), 1)
    ]

    return ENV.get_template("report.html").render(
        summary_stats=summary_stats,
        category_stats=category_stats,
        columns=columns,
        rows=rows,
        municipality_count=len(rows),
        map_metrics=[
            {
                "key": f"score_{category}",
                "label": POI_LABELS.get(category, category.capitalize()),
            }
            for category in POI_CATEGORIES
        ],
        map_html=map_html,
    )
