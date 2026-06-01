"""Gold: HTML livability report with summary, municipality ranking, embedded map."""

from dataclasses import asdict
from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg
import h3
import pandas as pd
from shapely.geometry import Polygon

from montreal.defs.assets.gold import report
from montreal.defs.assets.gold._config import SCORE_COLUMNS, UNKNOWN_MUNICIPALITY, GoldAssetMetadata
from montreal.defs.assets.gold.livability_score import livability_score
from montreal.defs.assets.silver.amenities import amenities
from montreal.defs.assets.silver.municipalities import montreal_municipalities
from montreal.defs.resources.lakehouse import location_of, s3_datastore


def _address_weighted(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    value_cols = ["livability", *SCORE_COLUMNS]
    weighted = df[[group_col, "n_addresses", *value_cols]].copy()
    weighted[value_cols] = weighted[value_cols].mul(weighted["n_addresses"], axis=0)

    out = weighted.groupby(group_col, as_index=False).sum(numeric_only=True)
    out[value_cols] = out[value_cols].div(out["n_addresses"], axis=0)
    return out.rename(columns={"n_addresses": "addresses"})


def _dominant_municipality(s: pd.Series) -> str:
    m = s.mode(dropna=True)
    return m.iloc[0] if not m.empty else UNKNOWN_MUNICIPALITY


def _agg_hexes(scores: pd.DataFrame, resolution: int) -> pd.DataFrame:
    df = scores.dropna(subset=["h3_r10"]).copy()
    df["h3_agg"] = df["h3_r10"].map(lambda c: h3.cell_to_parent(c, resolution))
    agg = _address_weighted(df, "h3_agg")
    agg["municipality"] = agg["h3_agg"].map(
        df.groupby("h3_agg")["municipality"].agg(_dominant_municipality)
    )
    agg["geometry"] = agg["h3_agg"].map(
        lambda c: Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(c)])
    )
    return agg


def _municipality_table(scores: pd.DataFrame) -> pd.DataFrame:
    df = scores.dropna(subset=["municipality"])
    df = df[df["municipality"] != UNKNOWN_MUNICIPALITY]
    return (
        _address_weighted(df, "municipality")
        .sort_values("livability", ascending=False)
        .reset_index(drop=True)
    )


ASSET_META = GoldAssetMetadata(
    layer="gold", data_category="report", segmentation="snapshot",
    description="HTML livability report: summary pills, municipality ranking, embedded map",
)


@dg.asset(
    group_name="analytics",
    metadata=asdict(ASSET_META),
    deps=[livability_score, amenities, montreal_municipalities],
)
def livability_map(context: dg.AssetExecutionContext, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    """HTML livability report with summary pills, municipality ranking, embedded map."""
    scores = s3_datastore.read_gpq(context, location_of(livability_score))
    hexes = _agg_hexes(scores, 9)
    table = _municipality_table(scores)

    amenities_gdf = s3_datastore.read_gpq(context, location_of(amenities))
    mean_liv = float(scores["livability"].mean())
    stats = {
        "addresses": int(scores["n_addresses"].sum()),
        "amenities": int(len(amenities_gdf)),
        "by_category": amenities_gdf["category"].value_counts().to_dict(),
        "mean_livability": mean_liv,
        "municipalities": int(table["municipality"].nunique()),
        "updated_on": datetime.now(ZoneInfo("America/Toronto")).strftime("%Y-%m-%d"),
    }

    context.log.info(
        f"livability_map: {len(hexes)} r9 cells, {stats['municipalities']} municipalities, "
        f"{stats['addresses']} addresses, {stats['amenities']} amenities, mean {mean_liv:.1f}"
    )

    boundaries = s3_datastore.read_gpq(context, location_of(montreal_municipalities))
    stamp = s3_datastore.write_html(
        context, report.render_report(
            stats=stats, table=table,
            map_html=report.build_map_html(hexes, boundaries),
        ),
    )
    return dg.MaterializeResult(
        data_version=dg.DataVersion(stamp),
        metadata={
            "num_addresses": dg.MetadataValue.int(stats["addresses"]),
            "num_amenities": dg.MetadataValue.int(stats["amenities"]),
            "num_municipalities": dg.MetadataValue.int(stats["municipalities"]),
            "mean_livability": dg.MetadataValue.float(round(mean_liv, 2)),
            "updated_on": dg.MetadataValue.text(stats["updated_on"]),
        }
    )
