"""Small data-quality result builders used by asset checks and unit tests."""

from datetime import datetime, timedelta, timezone
from typing import Iterable

import dagster as dg
import pandas as pd

MAX_RAW_SNAPSHOT_AGE = timedelta(days=35)


def _metadata_text_list(values: Iterable[str]) -> dg.MetadataValue:
    return dg.MetadataValue.json(sorted(str(value) for value in values))


def snapshot_freshness_result(
    ts: datetime | None,
    now: datetime | None = None,
) -> dg.AssetCheckResult:
    now = now or datetime.now(timezone.utc)
    age = None if ts is None else now - ts
    return dg.AssetCheckResult(
        passed=age is not None and age <= MAX_RAW_SNAPSHOT_AGE,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "snapshot": dg.MetadataValue.text(str(ts)),
            "age_days": dg.MetadataValue.int(age.days if age else -1),
            "max_age_days": dg.MetadataValue.int(MAX_RAW_SNAPSHOT_AGE.days),
        },
    )


def required_columns_result(
    df: pd.DataFrame,
    required_columns: Iterable[str],
) -> dg.AssetCheckResult:
    required = set(required_columns)
    missing = required - set(df.columns)
    return dg.AssetCheckResult(
        passed=not missing,
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "required_columns": _metadata_text_list(required),
            "missing_columns": _metadata_text_list(missing),
        },
    )


def row_count_result(df: pd.DataFrame, min_rows: int) -> dg.AssetCheckResult:
    return dg.AssetCheckResult(
        passed=len(df) >= min_rows,
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "min_rows": dg.MetadataValue.int(min_rows),
        },
    )


def category_coverage_result(
    df: pd.DataFrame,
    expected_categories: Iterable[str],
    *,
    column: str = "category",
) -> dg.AssetCheckResult:
    expected = set(expected_categories)
    actual = set(df[column].dropna().astype(str)) if column in df.columns else set()
    missing = expected - actual
    unexpected = actual - expected
    return dg.AssetCheckResult(
        passed=not missing and not unexpected,
        metadata={
            "expected_categories": _metadata_text_list(expected),
            "actual_categories": _metadata_text_list(actual),
            "missing_categories": _metadata_text_list(missing),
            "unexpected_categories": _metadata_text_list(unexpected),
        },
    )


def not_null_result(df: pd.DataFrame, columns: Iterable[str]) -> dg.AssetCheckResult:
    checked = [column for column in columns if column in df.columns]
    missing = set(columns) - set(checked)
    null_counts = {column: int(df[column].isna().sum()) for column in checked}
    return dg.AssetCheckResult(
        passed=not missing and all(count == 0 for count in null_counts.values()),
        metadata={
            "checked_columns": _metadata_text_list(checked),
            "missing_columns": _metadata_text_list(missing),
            "null_counts": dg.MetadataValue.json(null_counts),
        },
    )


def unique_rows_result(df: pd.DataFrame, columns: Iterable[str]) -> dg.AssetCheckResult:
    subset = [column for column in columns if column in df.columns]
    missing = set(columns) - set(subset)
    duplicate_count = 0 if missing else int(df.duplicated(subset=subset).sum())
    return dg.AssetCheckResult(
        passed=not missing and duplicate_count == 0,
        metadata={
            "unique_by": _metadata_text_list(subset),
            "missing_columns": _metadata_text_list(missing),
            "duplicate_rows": dg.MetadataValue.int(duplicate_count),
        },
    )


def numeric_bounds_result(
    df: pd.DataFrame,
    columns: Iterable[str],
    *,
    lower: float | None = None,
    upper: float | None = None,
    allow_nulls: bool = True,
) -> dg.AssetCheckResult:
    checked = [column for column in columns if column in df.columns]
    missing = set(columns) - set(checked)
    invalid_counts = {}
    null_counts = {}

    for column in checked:
        values = pd.to_numeric(df[column], errors="coerce")
        invalid = pd.Series(False, index=df.index)
        if lower is not None:
            invalid |= values < lower
        if upper is not None:
            invalid |= values > upper
        if not allow_nulls:
            invalid |= values.isna()
        invalid_counts[column] = int(invalid.sum())
        null_counts[column] = int(values.isna().sum())

    return dg.AssetCheckResult(
        passed=not missing and all(count == 0 for count in invalid_counts.values()),
        metadata={
            "checked_columns": _metadata_text_list(checked),
            "missing_columns": _metadata_text_list(missing),
            "invalid_counts": dg.MetadataValue.json(invalid_counts),
            "null_counts": dg.MetadataValue.json(null_counts),
            "lower_bound": (
                dg.MetadataValue.float(float(lower))
                if lower is not None
                else dg.MetadataValue.text("none")
            ),
            "upper_bound": (
                dg.MetadataValue.float(float(upper))
                if upper is not None
                else dg.MetadataValue.text("none")
            ),
        },
    )
