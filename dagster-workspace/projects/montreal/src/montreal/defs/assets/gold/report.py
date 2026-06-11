"""HTML rendering for the livability report."""

import unicodedata
from pathlib import Path

import branca.colormap as cm
import folium
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from montreal.defs.assets.gold._config import DEFAULT_WEIGHTS
from montreal.defs.assets.silver._config import POI_CATEGORIES

POI_LABELS = {
    "grocery": "Grocery stores", "school": "Schools", "health": "Health",
    "park": "Parks", "transit": "Transit", "bike": "Bike paths",
}
TABLE_LABELS = {
    "grocery": "Grocery", "school": "Schools", "health": "Health",
    "transit": "Transit", "park": "Parks", "bike": "Bike paths",
}
SCORE_WEIGHTS = {f"score_{c}": w for c, w in DEFAULT_WEIGHTS.items()}
TEMPLATES = Path(__file__).parent / "templates"
ENV = Environment(
    loader=FileSystemLoader(TEMPLATES), autoescape=select_autoescape(["html"]),
    trim_blocks=True, lstrip_blocks=True,
)
REPORT_CSS = (TEMPLATES / "report.css").read_text(encoding="utf-8")
SCORE_COLORS = ["#d7191c", "#fdae61", "#ffffbf", "#a6d96a", "#1a9641"]
COLOR_STOPS = [  # [score, [r, g, b]] pairs for the in-map JS colormap; mirrors COLORMAP
    [i * 100 // (len(SCORE_COLORS) - 1), [int(c[j:j + 2], 16) for j in (1, 3, 5)]]
    for i, c in enumerate(SCORE_COLORS)
]
COLORMAP = cm.LinearColormap(SCORE_COLORS, vmin=0.0, vmax=100.0, caption="Score (0-100)")


def _key(value: object) -> str:
    return unicodedata.normalize("NFKC", str(value)).casefold().strip()


def _round_coords(coords):
    if isinstance(coords, (float, int)):
        return round(coords, 6)
    return [_round_coords(c) for c in coords]


def _feature(row, properties: dict) -> dict:
    geo = row.geometry.__geo_interface__
    return {
        "type": "Feature",
        "geometry": {"type": geo["type"], "coordinates": _round_coords(geo["coordinates"])},
        "properties": properties,
    }


def _hex_feature_collection(hexes: pd.DataFrame) -> dict:
    features = [
        _feature(
            row,
            {
                "municipality": str(row.municipality), "addresses": int(row.addresses),
                "livability": round(float(row.livability), 1),
                **{f"score_{c}": round(float(getattr(row, f"score_{c}")), 1) for c in POI_CATEGORIES},
            },
        )
        for row in hexes.itertuples(index=False)
    ]
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
    cents = hexes["geometry"].map(lambda p: p.centroid)
    fmap = folium.Map(
        location=[cents.map(lambda p: p.y).mean(), cents.map(lambda p: p.x).mean()],
        zoom_start=11, tiles="cartodbpositron",
    )
    COLORMAP.add_to(fmap)

    metrics = [("livability", "Overall livability")] + [(f"score_{c}", c.capitalize()) for c in POI_CATEGORIES]
    hex_layer = folium.GeoJson(
        _hex_feature_collection(hexes), name="Livability cells",
        style_function=lambda f: {
            "fillColor": COLORMAP(f["properties"]["livability"]),
            "color": COLORMAP(f["properties"]["livability"]),
            "weight": 0, "fillOpacity": 0.6,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["municipality", "addresses", *[k for k, _ in metrics]],
            aliases=["Municipality", "Addresses", *[l for _, l in metrics]],
            localize=True, sticky=False,
        ),
    )
    hex_layer.add_to(fmap)

    bounds = folium.FeatureGroup(name="Municipality boundaries", show=True)
    folium.GeoJson(
        _boundary_feature_collection(boundaries),
        style_function=lambda _: {"fill": False, "color": "#39495c", "weight": 0.8, "opacity": 0.48},
        tooltip=folium.GeoJsonTooltip(fields=["municipality"], aliases=["Municipality"]),
    ).add_to(bounds)
    bounds.add_to(fmap)

    map_html = fmap.get_root().render()
    map_script = ENV.get_template("map.html").render(
        map_name=fmap.get_name(), hex_layer_name=hex_layer.get_name(),
        boundary_group_name=bounds.get_name(),
        color_stops=COLOR_STOPS, metric_weights=SCORE_WEIGHTS,
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
    cat_total = max(sum(stats["by_category"].get(c, 0) for c in POI_CATEGORIES), 1)
    category_stats = sorted(
        [
            {
                "label": POI_LABELS.get(c, c.capitalize()),
                "value": f"{stats['by_category'].get(c, 0):,}",
                "count": stats["by_category"].get(c, 0),
                "share": round(100 * stats["by_category"].get(c, 0) / cat_total, 1),
            }
            for c in POI_CATEGORIES
        ],
        key=lambda x: x["count"], reverse=True,
    )

    columns = ["#", "Municipality", "Addresses", "Livability"] + [TABLE_LABELS.get(c, c.capitalize()) for c in POI_CATEGORIES]
    rows = [
        {
            "rank": rank, "municipality": str(r.municipality), "municipality_key": _key(r.municipality),
            "addresses": f"{int(r.addresses):,}", "livability": f"{r.livability:.1f}",
            "livability_bg": COLORMAP(r.livability),
            "scores": [
                {"value": f"{s:.0f}", "color": COLORMAP(s)}
                for s in (getattr(r, f"score_{c}") for c in POI_CATEGORIES)
            ],
        }
        for rank, r in enumerate(table.itertuples(index=False), 1)
    ]

    return ENV.get_template("report.html").render(
        styles=REPORT_CSS, summary_stats=summary_stats, category_stats=category_stats,
        columns=columns, rows=rows, municipality_count=len(rows), updated_on=stats.get("updated_on", ""),
        map_metrics=[{"key": f"score_{c}", "label": POI_LABELS.get(c, c.capitalize())} for c in POI_CATEGORIES],
        map_html=map_html,
    )
