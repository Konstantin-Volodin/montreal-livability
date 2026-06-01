"""Gold: per-r10-cell amenity scores and the weighted livability blend."""

from dataclasses import asdict

import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Point

from montreal import __version__ as CODE_VERSION
from montreal.defs.assets.gold._config import (
    DEFAULT_WEIGHTS, SCORE_COLUMNS, UNKNOWN_MUNICIPALITY,
    GoldAssetDataContract, GoldAssetMetadata,
)
from montreal.defs.assets.silver._config import POI_CATEGORIES
from montreal.defs.assets.silver.municipalities import montreal_municipalities
from montreal.defs.assets.silver.distances import distances_to_amenities
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import location_of, s3_datastore

_SCORE_CURVE = ((100.0, 100.0), (500.0, 50.0), (1000.0, 20.0))


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
    mapping = joined.set_index("h3_r10")["municipality"].fillna(UNKNOWN_MUNICIPALITY)
    return cells.map(mapping).fillna(UNKNOWN_MUNICIPALITY)


def _distance_score(distances) -> np.ndarray:
    knots_m, knot_scores = zip(*_SCORE_CURVE)
    d = np.asarray(distances, dtype=float)
    score = np.interp(d, knots_m, knot_scores)
    score[(d > knots_m[-1]) | np.isnan(d)] = 0.0
    return score


class LivabilityWeights(dg.Config):
    grocery: float = DEFAULT_WEIGHTS["grocery"]
    transit: float = DEFAULT_WEIGHTS["transit"]
    park: float = DEFAULT_WEIGHTS["park"]
    bike: float = DEFAULT_WEIGHTS["bike"]
    school: float = DEFAULT_WEIGHTS["school"]
    health: float = DEFAULT_WEIGHTS["health"]


ASSET_META = GoldAssetMetadata(
    layer="gold",
    data_category="geospatial",
    segmentation="snapshot",
    description="Per-r10-cell amenity scores and the weighted livability blend",
)
ASSET_DATA_CONTRACT = GoldAssetDataContract(
    schema={
        "h3_r10": "str",
        "n_addresses": "numeric",
        **{column: "numeric" for column in SCORE_COLUMNS},
        "livability": "numeric",
        "municipality": "str",
    },
    uniqueness=("h3_r10",),
    completeness=("h3_r10", "n_addresses", "livability", "municipality", *SCORE_COLUMNS),
    bounds={"livability": (0.0, 100.0), **{column: (0.0, 100.0) for column in SCORE_COLUMNS}},
)


@dg.asset(
    group_name="analytics",
    metadata=asdict(ASSET_META),
    deps=[distances_to_amenities, montreal_municipalities],
    code_version=CODE_VERSION,
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
        .agg(n_addresses=("h3_r10", "size"), **{c: (c, "mean") for c in SCORE_COLUMNS})
        .reset_index()
    )
    out["municipality"] = _tag_municipalities(out["h3_r10"], boundaries).to_numpy()

    for category in POI_CATEGORIES:
        col = f"score_{category}"
        resolved = int(np.count_nonzero(out[col].to_numpy() > 0))
        context.log.info(
            f"  {category}: {resolved}/{len(out)} cells (mean {out[col].mean():.1f})"
            if resolved else f"  {category}: 0 cells"
        )

    weights = {c: getattr(config, c) for c in POI_CATEGORIES}
    total = sum(weights.values())
    if total <= 0:
        raise ValueError(f"weights must sum > 0, got {weights}")

    out["livability"] = (
        out[SCORE_COLUMNS].mul(pd.Series({f"score_{c}": w for c, w in weights.items()}))
        .sum(axis=1) / total
    )

    mean_liv = np.nanmean(out['livability'])
    context.log.info(
        f"livability_score: {len(out)} r10 cells, {out['municipality'].nunique()} "
        f"municipalities, mean {mean_liv:.1f} (sum {total:.2f})"
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


# asset checks
checks = standard_checks(livability_score, ASSET_DATA_CONTRACT)
