"""Gold layer + visualization.

`amenity_scores` turns each address's nearest-amenity distances into per-category
0-100 scores; `livability_score` collapses those into one weighted livability
number (weights are a Dagster `Config`, which is what the personalized-recommender
extension reuses); `livability_map` aggregates that to H3 r9 cells and renders a
Folium choropleth. All three are unpartitioned gold tables.
"""

import branca.colormap as cm
import dagster as dg
import folium
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

from montreal.defs.assets.distance_layer import (
    _AMENITY_CATEGORIES,
    distances_to_amenities,
)
from montreal.defs.resources.lakehouse import s3_datastore

_GOLD_META = {"layer": "gold", "data_category": "geospacial"}

# (distance_m, score) knots; linearly interpolated between, 0 past the last.
_SCORE_CURVE = ((100.0, 100.0), (500.0, 50.0), (1000.0, 20.0))


def _distance_score(distances) -> np.ndarray:
    """Piecewise-linear distance (m) -> 0-100 livability score.

    100 within 100 m, decaying linearly through the _SCORE_CURVE knots,
    then 0 beyond 1000 m or where the distance is missing.
    """
    knots_m, knot_scores = zip(*_SCORE_CURVE)
    d = np.asarray(distances, dtype=float)
    score = np.interp(d, knots_m, knot_scores)
    score[(d > knots_m[-1]) | np.isnan(d)] = 0.0
    return score


@dg.asset(
    group_name="analytics",
    metadata=_GOLD_META,
    deps=[distances_to_amenities],
)
def amenity_scores(
    context: dg.AssetExecutionContext, s3_datastore: s3_datastore
) -> dg.MaterializeResult:
    """Per-category 0-100 score per address, all r7 partitions in one gold table."""
    distances = s3_datastore.read_gpq_prefix(
        context, "silver/distances_to_amenities.parquet/"
    )

    # Keep only what the gold layer needs: the analysis cell + the scores.
    out = pd.DataFrame({"h3_r10": distances["h3_r10"].to_numpy()})
    for category in _AMENITY_CATEGORIES:
        scores = _distance_score(distances[f"dist_{category}"].to_numpy())
        out[f"score_{category}"] = scores
        resolved = int(np.count_nonzero(scores > 0))
        context.log.info(
            f"  category '{category}': {resolved}/{len(out)} scored "
            f"(mean {np.nanmean(scores):.1f})"
            if resolved else f"  category '{category}': 0 addresses scored"
        )

    context.log.info(
        f"amenity_scores: {len(out)} addresses, {len(_AMENITY_CATEGORIES)} score columns"
    )
    s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult()


class LivabilityWeights(dg.Config):
    """Per-category weights for the blended livability score.

    Defaults follow the README. Weights are normalised by their sum, so
    zeroing a category (or re-weighting for the personalized recommender)
    never deflates the 0-100 range.
    """

    grocery: float = 0.20
    transit: float = 0.20
    park: float = 0.20
    bike: float = 0.15
    school: float = 0.15
    health: float = 0.10


@dg.asset(
    group_name="analytics",
    metadata=_GOLD_META,
    deps=[amenity_scores],
)
def livability_score(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
    config: LivabilityWeights,
) -> dg.MaterializeResult:
    """Weighted 0-100 livability score per address (gold)."""
    scores = s3_datastore.read_gpq(context, "gold/amenity_scores.parquet")

    weights = {category: getattr(config, category) for category in _AMENITY_CATEGORIES}
    total = sum(weights.values())
    if total <= 0: raise ValueError(f"Livability weights must sum to > 0, got {weights}")

    blended = np.zeros(len(scores), dtype=float)
    for category, weight in weights.items():
        blended += weight * scores[f"score_{category}"].to_numpy(dtype=float)
    blended /= total

    out = scores.copy()
    out["livability"] = blended
    context.log.info(
        f"livability_score: {len(out)} addresses, mean {np.nanmean(blended):.1f} (weights {weights}, sum {total:.2f})"
    )

    s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult(
        metadata={
            "weights": dg.MetadataValue.json(weights),
            "mean_livability": dg.MetadataValue.float(round(float(np.nanmean(blended)), 2)),
        }
    )


def _r9_hexes(scores: pd.DataFrame, resolution: int) -> pd.DataFrame:
    """Mean livability + category breakdown per aggregated H3 cell, with geometry."""
    df = scores.dropna(subset=["h3_r10"]).copy()
    df["h3_agg"] = df["h3_r10"].map(lambda cell: h3.cell_to_parent(cell, resolution))

    value_cols = ["livability"] + [f"score_{c}" for c in _AMENITY_CATEGORIES]
    agg = df.groupby("h3_agg").agg(
        addresses=("h3_agg", "size"),
        **{col: (col, "mean") for col in value_cols},
    ).reset_index()

    # h3.cell_to_boundary -> ((lat, lng), ...); shapely wants (lng, lat).
    agg["geometry"] = agg["h3_agg"].map(
        lambda cell: Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)])
    )
    return agg


@dg.asset(
    group_name="analytics",
    metadata=_GOLD_META,
    deps=[livability_score],
)
def livability_map(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
) -> dg.MaterializeResult:
    """Interactive Folium choropleth of mean livability over H3 cells (gold)."""
    scores = s3_datastore.read_gpq(context, "gold/livability_score.parquet")
    hexes = _r9_hexes(scores, 9)
    context.log.info(f"livability_map: {len(hexes)} r9 cells from {len(scores)} addresses")

    centroids = hexes["geometry"].map(lambda poly: poly.centroid)
    centre = [
        centroids.map(lambda p: p.y).mean(),
        centroids.map(lambda p: p.x).mean(),
    ]
    fmap = folium.Map(location=centre, zoom_start=12, tiles="cartodbpositron")

    colormap = cm.LinearColormap(
        ["#d7191c", "#fdae61", "#ffffbf", "#a6d96a", "#1a9641"],
        vmin=0.0,
        vmax=100.0,
        caption="Mean livability score",
    )
    colormap.add_to(fmap)

    for row in hexes.itertuples(index=False):
        livability = getattr(row, "livability")
        parts = "".join(f"<br>{c}: {getattr(row, f'score_{c}'):.0f}" for c in _AMENITY_CATEGORIES)
        folium.GeoJson(
            row.geometry.__geo_interface__,
            style_function=lambda _f, v=livability: {
                "fillColor": colormap(v),
                "color": colormap(v),
                "weight": 0,
                "fillOpacity": 0.6,
            },
            tooltip=(
                f"<b>Livability: {livability:.0f}</b>"
                f"<br>{row.addresses} addresses{parts}"
            ),
        ).add_to(fmap)

    s3_datastore.write_html(context, fmap.get_root().render())
    return dg.MaterializeResult(
        metadata={
            "num_cells": dg.MetadataValue.int(len(hexes)),
            "num_addresses": dg.MetadataValue.int(int(hexes["addresses"].sum())),
            "mean_livability": dg.MetadataValue.float(
                round(float(hexes["livability"].mean()), 2)
            ),
        }
    )
