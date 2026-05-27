from datetime import datetime, timedelta, timezone

import dagster as dg
import pandas as pd

from montreal.defs.assets.silver.distance import POI_CATEGORIES
from montreal.defs.checks import checks
from montreal.defs.quality import (
    category_coverage_result,
    not_null_result,
    numeric_bounds_result,
    required_columns_result,
    row_count_result,
    snapshot_freshness_result,
    unique_rows_result,
)


def test_snapshot_freshness_uses_snapshot_timestamp():
    now = datetime(2026, 5, 27, tzinfo=timezone.utc)

    assert snapshot_freshness_result(now - timedelta(days=34), now).passed
    assert not snapshot_freshness_result(now - timedelta(days=36), now).passed
    assert not snapshot_freshness_result(None, now).passed
    assert snapshot_freshness_result(None, now).severity == dg.AssetCheckSeverity.WARN


def test_required_columns_rejects_empty_or_missing_data():
    assert required_columns_result(pd.DataFrame({"a": [1]}), ["a"]).passed
    assert not required_columns_result(pd.DataFrame({"a": [1]}), ["a", "b"]).passed


def test_row_count_enforces_minimum_size():
    assert row_count_result(pd.DataFrame({"a": [1, 2]}), min_rows=2).passed
    assert not row_count_result(pd.DataFrame({"a": [1]}), min_rows=2).passed


def test_not_null_flags_missing_values():
    valid = pd.DataFrame({"h3_r10": ["a"], "livability": [10.0]})
    invalid = pd.DataFrame({"h3_r10": ["a", None], "livability": [10.0, None]})

    assert not_null_result(valid, ["h3_r10", "livability"]).passed
    assert not not_null_result(invalid, ["h3_r10", "livability"]).passed


def test_unique_rows_flags_duplicate_keys():
    valid = pd.DataFrame({"h3_r10": ["a", "b"], "livability": [10.0, 20.0]})
    invalid = pd.DataFrame({"h3_r10": ["a", "a"], "livability": [10.0, 20.0]})

    assert unique_rows_result(valid, ["h3_r10"]).passed
    assert not unique_rows_result(invalid, ["h3_r10"]).passed


def test_category_coverage_requires_all_expected_categories():
    valid = pd.DataFrame({"category": list(POI_CATEGORIES)})
    invalid = pd.DataFrame({"category": ["grocery", "school", "not-a-category"]})

    assert category_coverage_result(valid, POI_CATEGORIES).passed
    assert not category_coverage_result(invalid, POI_CATEGORIES).passed


def test_numeric_bounds_flags_out_of_range_and_null_values():
    valid = pd.DataFrame({"score": [0, 50, 100]})
    invalid = pd.DataFrame({"score": [0, 101, None]})

    assert numeric_bounds_result(
        valid,
        ["score"],
        lower=0,
        upper=100,
        allow_nulls=False,
    ).passed
    assert not numeric_bounds_result(
        invalid,
        ["score"],
        lower=0,
        upper=100,
        allow_nulls=False,
    ).passed


def test_definitions_register_asset_checks():
    assert len(checks().asset_checks or []) == 31
