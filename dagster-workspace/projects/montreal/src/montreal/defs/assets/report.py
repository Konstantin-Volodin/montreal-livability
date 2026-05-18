"""HTML presentation for the livability report.

Pure rendering: summary pills, the municipality ranking table, and the
selectable Folium map. `analytics.py` owns the data; this owns how it looks.
"""

from pathlib import Path

import branca.colormap as cm
import folium
import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from montreal.defs.assets.distance_layer import _AMENITY_CATEGORIES

# Human-readable pill labels for the Amenities section; falls back to a
# capitalised category name for anything not listed here.
_AMENITY_LABELS = {
    "grocery": "Grocery stores",
    "school": "Schools",
    "health": "Health",
    "park": "Parks",
    "transit": "Transit",
    "bike": "Bike paths",
}

# Markup lives in templates/report.html; this module only prepares the data
# that fills it. Autoescaping handles all the escaping the raw f-strings used
# to do by hand (municipality names, pill labels, the map's srcdoc attribute).
_ENV = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)

# Shared 0-100 colour scale for both the map fill and the table's livability
# cell. Captioned once; never mutated, so it is safe to reuse per run.
_COLORMAP = cm.LinearColormap(
    ["#d7191c", "#fdae61", "#ffffbf", "#a6d96a", "#1a9641"],
    vmin=0.0,
    vmax=100.0,
    caption="Score (0–100)",
)

def build_map_html(hexes: pd.DataFrame, boundaries: pd.DataFrame) -> str:
    """Folium map with one selectable choropleth layer per metric, plus a
    municipality-boundary overlay drawn on top."""
    centroids = hexes["geometry"].map(lambda poly: poly.centroid)
    fmap = folium.Map(
        location=[centroids.map(lambda p: p.y).mean(),
                  centroids.map(lambda p: p.x).mean()],
        zoom_start=11,
        tiles="cartodbpositron",
    )
    _COLORMAP.add_to(fmap)

    layers = [("livability", "Overall livability")]
    layers += [(f"score_{c}", c.capitalize()) for c in _AMENITY_CATEGORIES]
    for column, label in layers:
        group = folium.FeatureGroup(name=label, show=(column == "livability"))
        for row in hexes.itertuples(index=False):
            value = getattr(row, column)
            breakdown = "".join(
                f"<br>{c}: {getattr(row, f'score_{c}'):.0f}" for c in _AMENITY_CATEGORIES
            )
            folium.GeoJson(
                row.geometry.__geo_interface__,
                style_function=lambda _f, v=value: {
                    "fillColor": _COLORMAP(v), "color": _COLORMAP(v),
                    "weight": 0, "fillOpacity": 0.6,
                },
                tooltip=(f"<b>{label}: {value:.0f}</b>"
                         f"<br>{row.municipality}"
                         f"<br>{row.addresses} addresses"
                         f"<br>Livability: {row.livability:.0f}{breakdown}"),
            ).add_to(group)
        group.add_to(fmap)

    # Added last so the outlines sit above whichever choropleth is active.
    bounds = folium.FeatureGroup(name="Municipality boundaries", show=True)
    for row in boundaries.itertuples(index=False):
        folium.GeoJson(
            row.geometry.__geo_interface__,
            style_function=lambda _f: {
                "fill": False, "color": "#1f2933", "weight": 1.5,
            },
            tooltip=str(row.municipality),
        ).add_to(bounds)
    bounds.add_to(fmap)

    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap.get_root().render()


def render_report(*, stats: dict, table: pd.DataFrame, map_html: str) -> str:
    """Assemble the self-contained livability report page from raw numbers."""
    summary_pills = [
        ("Addresses scored", f"{stats['addresses']:,}"),
        ("Municipalities", str(stats["municipalities"])),
        ("Mean livability", f"{stats['mean_livability']:.1f}"),
    ]
    amenity_pills = [
        (_AMENITY_LABELS.get(c, c.capitalize()), f"{stats['by_category'].get(c, 0):,}")
        for c in _AMENITY_CATEGORIES
    ]

    columns = ["#", "Municipality", "Addresses", "Livability"]
    columns += [c.capitalize() for c in _AMENITY_CATEGORIES]
    rows = [
        {
            "rank": rank,
            "municipality": str(r.municipality),
            "addresses": f"{int(r.addresses):,}",
            "livability": f"{r.livability:.1f}",
            "livability_bg": _COLORMAP(r.livability),
            "scores": [f"{getattr(r, f'score_{c}'):.0f}" for c in _AMENITY_CATEGORIES],
        }
        for rank, r in enumerate(table.itertuples(index=False), 1)
    ]

    return _ENV.get_template("report.html").render(
        summary_pills=summary_pills,
        amenity_pills=amenity_pills,
        columns=columns,
        rows=rows,
        municipality_count=len(rows),
        map_html=map_html,
    )
