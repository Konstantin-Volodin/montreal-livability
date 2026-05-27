"""Gold livability scores and HTML report."""

from datetime import timedelta

import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Point, Polygon

from montreal.defs.assets.gold import report
from montreal.defs.assets.silver.distance import (
    POI_CATEGORIES,
    amenity_points,
    distances_to_amenities,
)
from montreal.defs.assets.silver.h3 import montreal_municipalities
from montreal.defs.resources.lakehouse import location_of, s3_datastore

_UNKNOWN_MUNICIPALITY = "Inconnu"
_GOLD_META = {"layer": "gold", "data_category": "geospacial"}

# Re-derive whenever an upstream snapshot changes; the report carries the
# end-to-end monthly freshness SLA for the whole pipeline.
_EAGER = dg.AutomationCondition.eager()
_GOLD_FRESHNESS = dg.FreshnessPolicy.cron(
    deadline_cron="0 0 1 * *", lower_bound_delta=timedelta(days=2)
)
_SCORE_CURVE = ((100.0, 100.0), (500.0, 50.0), (1000.0, 20.0))
_SCORE_COLUMNS = [f"score_{c}" for c in POI_CATEGORIES]


def _tag_municipalities(cells: pd.Series, boundaries: gpd.GeoDataFrame) -> pd.Series:
    unique = pd.Index(cells.dropna().unique(), name="h3_r10")
    latlng = np.array([h3.cell_to_latlng(c) for c in unique])
    cell_points = gpd.GeoDataFrame(
        {"h3_r10": unique},
        geometry=[Point(lng, lat) for lat, lng in latlng],
        crs=4326,
    )
    joined = cell_points.sjoin(
        boundaries[["municipality", "geometry"]], how="left", predicate="within"
    ).drop_duplicates("h3_r10")
    mapping = joined.set_index("h3_r10")["municipality"].fillna(_UNKNOWN_MUNICIPALITY)
    return cells.map(mapping).fillna(_UNKNOWN_MUNICIPALITY)


def _distance_score(distances) -> np.ndarray:
    knots_m, knot_scores = zip(*_SCORE_CURVE)
    d = np.asarray(distances, dtype=float)
    score = np.interp(d, knots_m, knot_scores)
    score[(d > knots_m[-1]) | np.isnan(d)] = 0.0
    return score


class LivabilityWeights(dg.Config):
    grocery: float = 0.20
    transit: float = 0.20
    park: float = 0.20
    bike: float = 0.15
    school: float = 0.15
    health: float = 0.10


@dg.asset(
    group_name="analytics",
    metadata=_GOLD_META,
    deps=[distances_to_amenities, montreal_municipalities],
    automation_condition=_EAGER,
)
def livability_score(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
    config: LivabilityWeights,
) -> dg.MaterializeResult:
    """Per-r10-cell amenity scores + the weighted livability blend (gold)."""
    distances = s3_datastore.read_gpq_prefix(context, location_of(distances_to_amenities))
    boundaries = s3_datastore.read_gpq(context, location_of(montreal_municipalities))

    per_address = pd.DataFrame(
        {
            "h3_r10": distances["h3_r10"].to_numpy(),
            **{
                f"score_{c}": _distance_score(distances[f"dist_{c}"].to_numpy())
                for c in POI_CATEGORIES
            },
        }
    )
    out = (
        per_address.groupby("h3_r10")
        .agg(n_addresses=("h3_r10", "size"), **{c: (c, "mean") for c in _SCORE_COLUMNS})
        .reset_index()
    )
    out["municipality"] = _tag_municipalities(out["h3_r10"], boundaries).to_numpy()

    for category in POI_CATEGORIES:
        column = f"score_{category}"
        resolved = int(np.count_nonzero(out[column].to_numpy() > 0))
        context.log.info(
            f"  category '{category}': {resolved}/{len(out)} cells scored "
            f"(mean {out[column].mean():.1f})"
            if resolved else f"  category '{category}': 0 cells scored"
        )

    weights = {category: getattr(config, category) for category in POI_CATEGORIES}
    total = sum(weights.values())
    if total <= 0:
        raise ValueError(f"Livability weights must sum to > 0, got {weights}")

    score_weights = pd.Series({f"score_{c}": w for c, w in weights.items()})
    out["livability"] = out[_SCORE_COLUMNS].mul(score_weights).sum(axis=1) / total

    context.log.info(
        f"livability_score: {len(out)} r10 cells from {len(per_address)} addresses, "
        f"{out['municipality'].nunique()} municipalities, "
        f"mean livability {np.nanmean(out['livability']):.1f} "
        f"(weights {weights}, sum {total:.2f})"
    )

    stamp = s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp) if stamp else None,
        metadata={
            "num_cells": dg.MetadataValue.int(len(out)),
            "weights": dg.MetadataValue.json(weights),
            "mean_livability": dg.MetadataValue.float(
                round(float(np.nanmean(out["livability"])), 2)
            ),
        }
    )


def _address_weighted(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    value_cols = ["livability", *_SCORE_COLUMNS]
    weighted = df[[group_col, "n_addresses", *value_cols]].copy()
    weighted[value_cols] = weighted[value_cols].mul(weighted["n_addresses"], axis=0)

    out = weighted.groupby(group_col, as_index=False).sum(numeric_only=True)
    out[value_cols] = out[value_cols].div(out["n_addresses"], axis=0)
    return out.rename(columns={"n_addresses": "addresses"})


def _dominant_municipality(s: pd.Series) -> str:
    m = s.mode(dropna=True)
    return m.iloc[0] if not m.empty else _UNKNOWN_MUNICIPALITY


def _agg_hexes(scores: pd.DataFrame, resolution: int) -> pd.DataFrame:
    df = scores.dropna(subset=["h3_r10"]).copy()
    df["h3_agg"] = df["h3_r10"].map(lambda cell: h3.cell_to_parent(cell, resolution))
    agg = _address_weighted(df, "h3_agg")
    agg["municipality"] = agg["h3_agg"].map(
        df.groupby("h3_agg")["municipality"].agg(_dominant_municipality)
    )
    agg["geometry"] = agg["h3_agg"].map(
        lambda cell: Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)])
    )
    return agg


def _municipality_table(scores: pd.DataFrame) -> pd.DataFrame:
    df = scores.dropna(subset=["municipality"])
    df = df[df["municipality"] != _UNKNOWN_MUNICIPALITY]
    return (
        _address_weighted(df, "municipality")
        .sort_values("livability", ascending=False)
        .reset_index(drop=True)
    )


@dg.asset(
    group_name="analytics",
    metadata=_GOLD_META,
    deps=[livability_score, amenity_points, montreal_municipalities],
    automation_condition=_EAGER,
    freshness_policy=_GOLD_FRESHNESS,
)
def livability_map(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
) -> dg.MaterializeResult:
    """HTML livability report: summary pills, municipality ranking, embedded map (gold)."""
    scores = s3_datastore.read_gpq(context, location_of(livability_score))
    amenities = s3_datastore.read_gpq(context, location_of(amenity_points))
    boundaries = s3_datastore.read_gpq(context, location_of(montreal_municipalities))

    hexes = _agg_hexes(scores, 9)
    table = _municipality_table(scores)

    stats = {
        "addresses": int(scores["n_addresses"].sum()),
        "amenities": int(len(amenities)),
        "by_category": amenities["category"].value_counts().to_dict(),
        "mean_livability": float(scores["livability"].mean()),
        "municipalities": int(table["municipality"].nunique()),
    }

    context.log.info(
        f"livability_map: {len(hexes)} r9 cells, {stats['municipalities']} municipalities, "
        f"{stats['addresses']} addresses, {stats['amenities']} amenities, "
        f"mean livability {stats['mean_livability']:.1f}"
    )

    stamp = s3_datastore.write_html(
        context,
        report.render_report(
            stats=stats,
            table=table,
            map_html=report.build_map_html(hexes, boundaries),
        ),
    )
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={
            "num_addresses": dg.MetadataValue.int(stats["addresses"]),
            "num_amenities": dg.MetadataValue.int(stats["amenities"]),
            "num_municipalities": dg.MetadataValue.int(stats["municipalities"]),
            "mean_livability": dg.MetadataValue.float(round(stats["mean_livability"], 2)),
        }
    )
