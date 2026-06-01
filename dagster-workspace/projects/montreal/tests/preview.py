"""Render a standalone HTML preview of the livability report with synthetic data.

Eyeball template/CSS changes without a pipeline run. Builds a real folium map, so
the layer toggles and row -> map highlighting work exactly as in production:

    uv run python tests/preview.py        # writes preview_report.html and opens it

``build_report_html`` is shared with test_gold.py so the preview path stays exercised.
"""

import math
import random
import webbrowser
from pathlib import Path

import pandas as pd
from shapely.geometry import Polygon
from shapely.ops import unary_union

from montreal.defs.assets.gold import report
from montreal.defs.assets.gold._config import SCORE_COLUMNS
from montreal.defs.assets.gold.livability_map import _address_weighted
from montreal.defs.assets.silver._config import POI_CATEGORIES

import h3

PREVIEW_PATH = Path(__file__).resolve().parents[1] / "preview_report.html"
CENTER = (45.52, -73.60)  # (lat, lng)
MUNIS = [
    "Le Plateau-Mont-Royal", "Ville-Marie", "Rosemont-La Petite-Patrie",
    "Verdun", "Outremont", "Côte-des-Neiges-Notre-Dame-de-Grâce",
]


def _hex_polygon(cell: str) -> Polygon:
    return Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)])


def _sample_hexes(rng: random.Random) -> pd.DataFrame:
    """A patch of r9 cells around downtown, each tagged to a municipality by sector."""
    cells = h3.grid_disk(h3.latlng_to_cell(*CENTER, 9), 6)
    rows = []
    for cell in cells:
        lat, lng = h3.cell_to_latlng(cell)
        sector = (math.atan2(lat - CENTER[0], lng - CENTER[1]) + math.pi) / (2 * math.pi)
        base = rng.uniform(35, 92)
        row = {
            "municipality": MUNIS[int(sector * len(MUNIS)) % len(MUNIS)],
            "addresses": rng.randint(200, 4000),
            "livability": round(base, 1),
            "geometry": _hex_polygon(cell),
        }
        for category in POI_CATEGORIES:
            row[f"score_{category}"] = round(min(100.0, max(0.0, base + rng.uniform(-25, 25))), 1)
        rows.append(row)
    return pd.DataFrame(rows)


def _sample_boundaries(hexes: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {"municipality": muni, "geometry": unary_union(list(group["geometry"]))}
        for muni, group in hexes.groupby("municipality")
    )


def build_report_html(seed: int = 7) -> str:
    """Full report HTML (real embedded map) from deterministic synthetic data."""
    rng = random.Random(seed)
    hexes = _sample_hexes(rng)
    boundaries = _sample_boundaries(hexes)
    table = (
        _address_weighted(hexes.rename(columns={"addresses": "n_addresses"}), "municipality")
        .sort_values("livability", ascending=False)
        .reset_index(drop=True)
    )
    stats = {
        "addresses": int(hexes["addresses"].sum()),
        "amenities": int(sum(rng.randint(1500, 8000) for _ in POI_CATEGORIES)),
        "by_category": {c: rng.randint(1500, 8000) for c in POI_CATEGORIES},
        "mean_livability": float(hexes["livability"].mean()),
        "municipalities": int(hexes["municipality"].nunique()),
        "updated_on": "2026-05-31 (preview)",
    }
    return report.render_report(
        stats=stats,
        table=table,
        map_html=report.build_map_html(hexes, boundaries),
    )


def write_preview(path: Path = PREVIEW_PATH, *, open_browser: bool = False) -> Path:
    path.write_text(build_report_html(), encoding="utf-8")
    if open_browser:
        webbrowser.open(path.as_uri())
    return path


if __name__ == "__main__":
    out = write_preview(open_browser=True)
    print(f"wrote {out}")
