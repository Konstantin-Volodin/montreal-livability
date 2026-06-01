"""Tests for the gold layer: the score curve, municipality tagging, the
address-weighted report aggregations, and the value_range check that gold adds
on top of the silver shape checks."""

import dagster as dg
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point, Polygon, box

from montreal.defs.assets.gold import livability_map, livability_score, report
from montreal.defs.assets.gold._config import (
    DEFAULT_WEIGHTS,
    SCORE_COLUMNS,
    UNKNOWN_MUNICIPALITY,
    GoldAssetDataContract,
)
from montreal.defs.assets.gold.livability_map import (
    _address_weighted,
    _agg_hexes,
    _dominant_municipality,
    _municipality_table,
)
from montreal.defs.assets.gold.livability_score import _distance_score, _tag_municipalities
from montreal.defs.assets.silver._config import POI_CATEGORIES
from montreal.defs.checks.factory import standard_checks
from montreal.defs.resources.lakehouse import s3_datastore

MONTREAL = (45.5, -73.6)  # (lat, lng)
_VALUE_COLS = ["livability", *SCORE_COLUMNS]


def _scored(municipality: list[str], n_addresses: list[int], livability: list[float], cells=None):
    """A minimal per-cell score frame carrying every value column the report reads."""
    frame = {
        "municipality": municipality,
        "n_addresses": n_addresses,
        **{c: list(livability) for c in _VALUE_COLS},
    }
    if cells is not None:
        frame["h3_r10"] = cells
    return pd.DataFrame(frame)




def test_distance_score_is_piecewise_with_zero_past_the_last_knot():
    got = _distance_score(np.array([0, 100, 500, 750, 1000, 1500, np.nan]))
    np.testing.assert_allclose(got, [100, 100, 50, 35, 20, 0, 0])




def test_tag_municipalities_labels_inside_cells_and_marks_outsiders_unknown():
    boundaries = gpd.GeoDataFrame(
        {"municipality": ["CityA"], "type": ["arr"]},
        geometry=[box(-73.7, 45.4, -73.5, 45.6)],
        crs=4326,
    )
    inside = h3.latlng_to_cell(45.5, -73.6, 10)
    outside = h3.latlng_to_cell(40.0, -100.0, 10)

    tagged = _tag_municipalities(pd.Series([inside, outside]), boundaries)

    assert tagged.iloc[0] == "CityA"
    assert tagged.iloc[1] == UNKNOWN_MUNICIPALITY




def test_dominant_municipality_picks_the_mode_or_falls_back():
    assert _dominant_municipality(pd.Series(["A", "A", "B"])) == "A"
    assert _dominant_municipality(pd.Series([], dtype=object)) == UNKNOWN_MUNICIPALITY


def test_address_weighted_mean_weights_by_address_count():
    df = _scored(["A", "A"], [1, 3], [100.0, 0.0])
    out = _address_weighted(df, "municipality")
    assert out.loc[0, "addresses"] == 4  # n_addresses summed and renamed
    assert out.loc[0, "livability"] == pytest.approx(25.0)  # (100*1 + 0*3) / 4


def test_municipality_table_drops_unknown_and_sorts_by_livability():
    df = _scored(["A", "B", UNKNOWN_MUNICIPALITY], [1, 1, 1], [80.0, 90.0, 100.0])
    # _scored sets every value col to the same list; override livability per row.
    df["livability"] = [80.0, 90.0, 100.0]
    out = _municipality_table(df)
    assert list(out["municipality"]) == ["B", "A"]  # desc, Inconnu removed


def test_agg_hexes_rolls_r10_children_up_to_one_parent_polygon():
    parent = h3.latlng_to_cell(*MONTREAL, 9)
    children = list(h3.cell_to_children(parent, 10))[:2]
    scores = _scored(["A", "A"], [1, 1], [50.0, 50.0], cells=children)

    agg = _agg_hexes(scores, 9)

    assert len(agg) == 1
    assert agg.iloc[0]["municipality"] == "A"
    assert isinstance(agg.iloc[0]["geometry"], Polygon)




def test_default_weights_cover_every_category_and_sum_to_one():
    assert set(DEFAULT_WEIGHTS) == set(POI_CATEGORIES)
    assert sum(DEFAULT_WEIGHTS.values()) == pytest.approx(1.0)
    assert SCORE_COLUMNS == [f"score_{c}" for c in POI_CATEGORIES]


_FRAME: pd.DataFrame | None = None


class FakeStore(s3_datastore):
    def setup_for_execution(self, context) -> None:
        pass

    def read_gpq(self, context, address):
        return _FRAME

    def read_gpq_prefix(self, context, prefix):
        return _FRAME


@dg.asset(name="gold_fixture", metadata={"layer": "gold", "segmentation": "snapshot"})
def gold_fixture(context, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    return dg.MaterializeResult()


GOLD_CONTRACT = GoldAssetDataContract(
    schema={"h3_r10": "str", "livability": "numeric"},
    uniqueness=("h3_r10",),
    completeness=("h3_r10", "livability"),
    bounds={"livability": (0.0, 100.0)},
)


def _run_gold_checks(frame: pd.DataFrame) -> dict[str, bool]:
    global _FRAME
    _FRAME = frame
    checks = standard_checks(gold_fixture, GOLD_CONTRACT)
    result = dg.materialize(
        [gold_fixture, *checks],
        resources={"s3_datastore": FakeStore(bucket_name="b", region_name="r")},
    )
    assert result.success
    return {ev.check_name: ev.passed for ev in result.get_asset_check_evaluations()}


def test_standard_checks_adds_value_range_when_contract_has_bounds():
    checks = standard_checks(gold_fixture, GOLD_CONTRACT)
    names = {key.name for c in checks for key in c.check_keys}
    assert names == {"schema_contract", "row_uniqueness", "field_completeness", "value_range"}


def test_value_range_passes_in_bounds_and_fails_out_of_bounds():
    in_bounds = _run_gold_checks(pd.DataFrame({"h3_r10": ["a", "b"], "livability": [10.0, 90.0]}))
    assert all(in_bounds.values())

    out_of_bounds = _run_gold_checks(pd.DataFrame({"h3_r10": ["a", "b"], "livability": [10.0, 150.0]}))
    assert out_of_bounds["value_range"] is False
    assert out_of_bounds["schema_contract"] is True




def test_livability_score_contract_carries_bounds_and_four_checks():
    assert livability_score.ASSET_META.layer == "gold"
    contract = livability_score.ASSET_DATA_CONTRACT
    assert set(contract.bounds) <= set(contract.schema)  # bounded cols are declared
    names = {key.name for c in livability_score.checks for key in c.check_keys}
    assert names == {"schema_contract", "row_uniqueness", "field_completeness", "value_range"}


def test_livability_map_is_a_terminal_artifact_without_contract_or_checks():
    assert livability_map.ASSET_META.layer == "gold"
    assert livability_map.ASSET_META.data_category == "report"
    # HTML report: no geoparquet to validate, so no data contract / checks.
    assert not hasattr(livability_map, "ASSET_DATA_CONTRACT")
    assert not hasattr(livability_map, "checks")




def _report_table() -> pd.DataFrame:
    scores = {f"score_{c}": float(i * 10) for i, c in enumerate(POI_CATEGORIES, 1)}
    return pd.DataFrame([
        {"municipality": "Le Plateau-Mont-Royal", "addresses": 42000, "livability": 88.0, **scores},
        {"municipality": "Verdun", "addresses": 30000, "livability": 71.5, **scores},
    ])


def _report_stats() -> dict:
    return {
        "addresses": 512288,
        "amenities": 25039,
        "municipalities": 34,
        "mean_livability": 72.7,
        "by_category": {c: 1000 for c in POI_CATEGORIES},
        "updated_on": "2026-05-31 12:00 UTC",
    }


def _render() -> str:
    return report.render_report(stats=_report_stats(), table=_report_table(), map_html="MAPMARKER")


def test_render_report_inlines_the_stylesheet_unescaped():
    html = _render()
    assert "<style>" in html and "box-sizing:border-box" in html
    assert 'content:""' in html


def test_render_report_carries_bilingual_labels_and_hex_motif():
    html = _render()
    for label in ("Données ouvertes", "Commodités", "Carte", "Classement"):
        assert label in html
    assert "clip-path:polygon" in html
    assert "image/svg+xml" in html


def test_render_report_emits_one_ranking_row_per_municipality():
    html = _render()
    assert html.count('data-municipality-key="') == len(_report_table())
    assert "Le Plateau-Mont-Royal" in html and "Verdun" in html
    assert "MAPMARKER" in html


def test_render_report_stacks_the_ranking_table_below_the_map():
    html = _render()
    assert 'class="map-frame"' in html and 'class="table-frame"' in html
    assert html.index('class="map-frame"') < html.index('class="table-frame"')
    assert "scrollIntoView" in html


def test_preview_builds_a_full_report_with_a_real_map():
    from tests import preview

    out = preview.write_preview()
    html = out.read_text(encoding="utf-8")
    assert "leaflet" in html.lower()
    assert html.count('data-municipality-key="') == len(preview.MUNIS)
    assert "Carte" in html and "Classement" in html
