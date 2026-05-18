"""HTML presentation for the livability report.

Pure rendering: summary pills, the municipality ranking table, and the
selectable Folium map. `analytics.py` owns the data; this owns how it looks.
"""

import json
import unicodedata
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


def _key(value: object) -> str:
    """Stable browser/Python key for joining table rows to map boundaries."""
    return unicodedata.normalize("NFKC", str(value)).casefold().strip()


def _hex_feature_collection(hexes: pd.DataFrame) -> dict:
    """Serialize r9 cells once, with all switchable metric values as properties."""
    def _round_coords(coords):
        if isinstance(coords, (float, int)):
            return round(coords, 6)
        return [_round_coords(c) for c in coords]

    features = []
    for row in hexes.itertuples(index=False):
        geometry = row.geometry.__geo_interface__
        properties = {
            "municipality": str(row.municipality),
            "addresses": int(row.addresses),
            "livability": round(float(row.livability), 1),
        }
        for category in _AMENITY_CATEGORIES:
            properties[f"score_{category}"] = round(
                float(getattr(row, f"score_{category}")), 1
            )
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": geometry["type"],
                    "coordinates": _round_coords(geometry["coordinates"]),
                },
                "properties": properties,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _map_interaction_assets(
    *,
    map_name: str,
    hex_layer_name: str,
    boundary_group_name: str,
    boundary_features: dict[str, dict],
) -> str:
    """Bridge the outer report controls into the Folium iframe."""
    boundary_json = json.dumps(boundary_features, separators=(",", ":"))
    return f"""
<script>
(function () {{
  var map = {map_name};
  var hexLayer = {hex_layer_name};
  var boundaryGroup = {boundary_group_name};
  var boundaryFeatures = {boundary_json};
  var highlightStyle = {{
    fill: true,
    fillColor: "#0f766e",
    fillOpacity: 0.12,
    color: "#0f766e",
    weight: 2.6,
    opacity: 0.95
  }};
  var highlightLayer = L.geoJSON(null, {{
    style: highlightStyle,
    interactive: false
  }}).addTo(map);
  var activeMetrics = ["livability"];

  function colorForScore(value) {{
    var stops = [
      [0, [215, 25, 28]],
      [25, [253, 174, 97]],
      [50, [255, 255, 191]],
      [75, [166, 217, 106]],
      [100, [26, 150, 65]]
    ];
    var score = Math.max(0, Math.min(100, Number(value) || 0));
    for (var i = 1; i < stops.length; i++) {{
      if (score <= stops[i][0]) {{
        var left = stops[i - 1];
        var right = stops[i];
        var ratio = (score - left[0]) / (right[0] - left[0]);
        var rgb = left[1].map(function (channel, index) {{
          return Math.round(channel + ratio * (right[1][index] - channel));
        }});
        return "rgb(" + rgb.join(",") + ")";
      }}
    }}
    return "rgb(26,150,65)";
  }}

  function hexStyle(feature) {{
    var color = colorForScore(scoreForFeature(feature));
    return {{
      fillColor: color,
      color: color,
      weight: 0,
      fillOpacity: 0.6,
      opacity: 0
    }};
  }}

  function scoreForFeature(feature) {{
    var total = 0;
    var count = 0;
    activeMetrics.forEach(function (metric) {{
      var score = Number(feature.properties[metric]);
      if (!Number.isNaN(score)) {{
        total += score;
        count++;
      }}
    }});
    return count ? total / count : feature.properties.livability;
  }}

  function setMetrics(metrics) {{
    activeMetrics = Array.isArray(metrics) && metrics.length ? metrics : ["livability"];
    hexLayer.setStyle(hexStyle);
  }}

  function clearHighlight() {{
    highlightLayer.clearLayers();
  }}

  function setBoundariesVisible(visible) {{
    if (visible && !map.hasLayer(boundaryGroup)) boundaryGroup.addTo(map);
    if (!visible && map.hasLayer(boundaryGroup)) map.removeLayer(boundaryGroup);
  }}

  function highlightMunicipality(municipalityKey, options) {{
    clearHighlight();
    options = options || {{}};
    var feature = boundaryFeatures[municipalityKey];
    if (!feature) return;
    var added = highlightLayer.addData(feature);
    if (highlightLayer.bringToFront) highlightLayer.bringToFront();
    if (options.fit && added.getBounds) {{
      map.fitBounds(added.getBounds(), {{ padding: [24, 24], maxZoom: 12 }});
    }}
  }}

  window.addEventListener("message", function (event) {{
    var data = event.data || {{}};
    if (data.type === "set-map-metrics") {{
      setMetrics(data.metrics);
    }}
    if (data.type === "set-map-metric") {{
      setMetrics([data.metric]);
    }}
    if (data.type === "set-boundary-visibility") {{
      setBoundariesVisible(Boolean(data.visible));
    }}
    if (data.type === "highlight-municipality") {{
      highlightMunicipality(data.municipalityKey, {{ fit: Boolean(data.fit) }});
    }}
    if (data.type === "clear-municipality-highlight") {{
      clearHighlight();
    }}
  }});
  window.highlightMunicipality = highlightMunicipality;
  window.clearMunicipalityHighlight = clearHighlight;
  window.setMapMetric = function (metric) {{ setMetrics([metric]); }};
  window.setMapMetrics = setMetrics;
  window.setMunicipalityBoundariesVisible = setBoundariesVisible;
}}());
</script>
"""


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

    metrics = [("livability", "Overall livability")]
    metrics += [(f"score_{c}", c.capitalize()) for c in _AMENITY_CATEGORIES]
    hex_layer = folium.GeoJson(
        _hex_feature_collection(hexes),
        name="Livability cells",
        style_function=lambda feature: {
            "fillColor": _COLORMAP(feature["properties"]["livability"]),
            "color": _COLORMAP(feature["properties"]["livability"]),
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

    # Added last so the outlines sit above whichever choropleth is active.
    bounds = folium.FeatureGroup(name="Municipality boundaries", show=True)
    boundary_features = {}
    for row in boundaries.itertuples(index=False):
        boundary_feature = {
            "type": "Feature",
            "geometry": row.geometry.__geo_interface__,
            "properties": {"municipality": str(row.municipality)},
        }
        boundary = folium.GeoJson(
            boundary_feature,
            style_function=lambda _f: {
                "fill": False,
                "color": "#39495c",
                "weight": 0.8,
                "opacity": 0.48,
            },
            tooltip=str(row.municipality),
        )
        boundary.add_to(bounds)
        boundary_features[_key(row.municipality)] = boundary_feature
    bounds.add_to(fmap)

    map_name = fmap.get_name()
    hex_layer_name = hex_layer.get_name()
    boundary_group_name = bounds.get_name()
    map_html = fmap.get_root().render()
    return map_html.replace(
        "</html>",
        f"{_map_interaction_assets(
            map_name=map_name,
            hex_layer_name=hex_layer_name,
            boundary_group_name=boundary_group_name,
            boundary_features=boundary_features,
        )}\n</html>",
        1,
    )


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
            "municipality_key": _key(r.municipality),
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
        map_metrics=[
            {
                "key": f"score_{category}",
                "label": _AMENITY_LABELS.get(category, category.capitalize()),
            }
            for category in _AMENITY_CATEGORIES
        ],
        map_html=map_html,
    )
