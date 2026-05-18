"""Gold layer + visualization.

`amenity_scores` turns each address's nearest-amenity distances into per-category
0-100 scores (tagged with the dominant municipality); `livability_score`
collapses those into one weighted livability number (weights are a Dagster
`Config`, which is what the personalized-recommender extension reuses);
`livability_map` builds an HTML report — summary pills, a municipality ranking
table, and an embedded H3 r8 Folium map with selectable amenity layers. The
first two are unpartitioned gold tables, the last a gold HTML artifact.
"""

import dagster as dg
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

from montreal.defs.assets import report
from montreal.defs.assets.distance_layer import (
    _AMENITY_CATEGORIES,
    distances_to_amenities,
)
from montreal.defs.resources.lakehouse import s3_datastore

_GOLD_META = {"layer": "gold", "data_category": "geospacial"}

# donnees.montreal.ca `uniteevaluationfonciere` MUNICIPALITE code -> name.
# Code 50 is all of Montréal proper; the rest are demerged on-island suburbs.
_MUNICIPALITY_NAMES = {
    "02": "Baie-D'Urfé",
    "03": "Beaconsfield",
    "04": "Côte-Saint-Luc",
    "05": "Dollard-Des Ormeaux",
    "06": "Dorval",
    "07": "Hampstead",
    "09": "L'Île-Dorval",
    "10": "Kirkland",
    "13": "Mont-Royal",
    "14": "Montréal-Est",
    "15": "Montréal-Ouest",
    "20": "Pointe-Claire",
    "22": "Senneville",
    "23": "Sainte-Anne-de-Bellevue",
    "29": "Westmount",
    "50": "Montréal",
}
_UNKNOWN_MUNICIPALITY = "Inconnu"


def _municipality_column(df: pd.DataFrame) -> str:
    """Resolve the MUNICIPALITE column case-insensitively, or fail loudly."""
    for col in df.columns:
        if col.strip().lower() == "municipalite":
            return col
    raise ValueError(
        "Expected a 'MUNICIPALITE' column on the address/distance frame to "
        f"derive municipalities. Available columns: {list(df.columns)}"
    )


def _municipality_name(codes: pd.Series) -> pd.Series:
    """Map raw MUNICIPALITE codes (e.g. 50, '2') to readable names."""
    normalized = (
        codes.astype("string")
        .str.strip()
        .str.split(".").str[0]  # tolerate floats like '50.0'
        .str.zfill(2)
    )
    return normalized.map(_MUNICIPALITY_NAMES).fillna(_UNKNOWN_MUNICIPALITY)

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
    """Per-category 0-100 score aggregated to H3 r10 cells, all r7 partitions in one gold table."""
    distances = s3_datastore.read_gpq_prefix(context, "silver/distances_to_amenities.parquet/")

    # Score each address, then collapse to one row per r10 cell
    municipality_col = _municipality_column(distances)
    per_address = pd.DataFrame({"h3_r10": distances["h3_r10"].to_numpy()})
    per_address["municipality"] = _municipality_name(distances[municipality_col]).to_numpy()
    score_columns = []
    for category in _AMENITY_CATEGORIES:
        column = f"score_{category}"
        per_address[column] = _distance_score(distances[f"dist_{category}"].to_numpy())
        score_columns.append(column)

    grouped = per_address.groupby("h3_r10")
    out = (
        grouped.agg(n_addresses=("h3_r10", "size"), **{c: (c, "mean") for c in score_columns})
        .reset_index()
    )

    # One r10 cell can straddle a municipal boundary; tag it with the
    # dominant (modal) municipality of the addresses it contains.
    def _dominant(s: pd.Series) -> str:
        m = s.mode(dropna=True)
        return m.iloc[0] if not m.empty else _UNKNOWN_MUNICIPALITY

    municipality_by_cell = grouped["municipality"].agg(_dominant)
    out["municipality"] = out["h3_r10"].map(municipality_by_cell).to_numpy()

    for category in _AMENITY_CATEGORIES:
        column = f"score_{category}"
        resolved = int(np.count_nonzero(out[column].to_numpy() > 0))
        context.log.info(
            f"  category '{category}': {resolved}/{len(out)} cells scored "
            f"(mean {out[column].mean():.1f})"
            if resolved else f"  category '{category}': 0 cells scored"
        )

    context.log.info(
        f"amenity_scores: {len(out)} r10 cells from {len(per_address)} addresses, "
        f"{len(_AMENITY_CATEGORIES)} score columns, "
        f"{out['municipality'].nunique()} municipalities"
    )
    s3_datastore.write_gpq(context, out)
    return dg.MaterializeResult()


class LivabilityWeights(dg.Config):
    """Per-category weights for the blended livability score."""
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


def _address_weighted(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """Group `df`, averaging livability + per-category scores weighted by the
    address count of each r10 row (so a sparse cell can't drag a dense
    neighbour's mean around). Returns one row per group + an `addresses` sum.
    """
    value_cols = ["livability"] + [f"score_{c}" for c in _AMENITY_CATEGORIES]

    def _agg(group: pd.DataFrame) -> pd.Series:
        w = group["n_addresses"].to_numpy(dtype=float)
        return pd.Series({
            "addresses": int(w.sum()),
            **{c: np.average(group[c].to_numpy(dtype=float), weights=w) for c in value_cols},
        })

    return df.groupby(group_col).apply(_agg, include_groups=False).reset_index()


def _agg_hexes(scores: pd.DataFrame, resolution: int) -> pd.DataFrame:
    """Address-weighted livability per aggregated H3 cell, with cell polygons."""
    df = scores.dropna(subset=["h3_r10"]).copy()
    df["h3_agg"] = df["h3_r10"].map(lambda cell: h3.cell_to_parent(cell, resolution))
    agg = _address_weighted(df, "h3_agg")
    # h3.cell_to_boundary -> ((lat, lng), ...); shapely wants (lng, lat).
    agg["geometry"] = agg["h3_agg"].map(
        lambda cell: Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)])
    )
    return agg


def _municipality_table(scores: pd.DataFrame) -> pd.DataFrame:
    """Address-weighted livability per municipality, best first."""
    df = scores.dropna(subset=["municipality"])
    return (
        _address_weighted(df, "municipality")
        .sort_values("livability", ascending=False)
        .reset_index(drop=True)
    )


@dg.asset(
    group_name="analytics",
    metadata=_GOLD_META,
    deps=[livability_score],
)
def livability_map(
    context: dg.AssetExecutionContext,
    s3_datastore: s3_datastore,
) -> dg.MaterializeResult:
    """HTML livability report: summary pills, municipality ranking, embedded map (gold)."""
    scores = s3_datastore.read_gpq(context, "gold/livability_score.parquet")
    amenities = s3_datastore.read_gpq(context, "silver/amenity_points.parquet")

    hexes = _agg_hexes(scores, 8)
    table = _municipality_table(scores)

    weights = scores["n_addresses"].to_numpy(dtype=float)
    stats = {
        "addresses": int(weights.sum()),
        "amenities": int(len(amenities)),
        "by_category": amenities["category"].value_counts().to_dict(),
        "mean_livability": float(
            np.average(scores["livability"].to_numpy(dtype=float), weights=weights)
        ),
        "municipalities": int(table["municipality"].nunique()),
    }

    context.log.info(
        f"livability_map: {len(hexes)} r8 cells, {stats['municipalities']} municipalities, "
        f"{stats['addresses']} addresses, {stats['amenities']} amenities, "
        f"mean livability {stats['mean_livability']:.1f}"
    )

    s3_datastore.write_html(context, report.renderreport(
        stats=stats,
        table=table,
        map_html=report.build_map_html(hexes),
    ))
    return dg.MaterializeResult(
        metadata={
            "num_addresses": dg.MetadataValue.int(stats["addresses"]),
            "num_amenities": dg.MetadataValue.int(stats["amenities"]),
            "num_municipalities": dg.MetadataValue.int(stats["municipalities"]),
            "mean_livability": dg.MetadataValue.float(round(stats["mean_livability"], 2)),
        }
    )
