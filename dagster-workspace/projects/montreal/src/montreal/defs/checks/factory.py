"""
Allows reusing common asset checks for different data assets.

common checks:
- snapshot_freshness
- schema_contract
- row_uniqueness
- field_completeness
"""

import datetime

import dagster as dg
import geopandas as gpd
import pandas as pd

from montreal.defs.resources.lakehouse import location_of, s3_datastore

CHECK_TYPES = [
    "snapshot_freshness",
    "schema_contract",
    "row_uniqueness",
    "field_completeness",
]


def _dtype_matches(series: pd.Series, kind: str) -> bool:
    """Whether a column's dtype matches a contract kind."""
    if kind == "numeric":
        return pd.api.types.is_numeric_dtype(series)
    if kind == "str":
        return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)
    if kind == "geometry":
        return str(series.dtype) == "geometry"
    raise ValueError(f"Unknown schema kind {kind!r}")


def snapshot_freshness_factory(asset: dg.AssetsDefinition, freshness: dict[str, int]):
    """Latest snapshot is no older than ``freshness['max_days']``."""
    max_days = freshness["max_days"]

    @dg.asset_check(asset=asset, name=CHECK_TYPES[0])
    def _check(s3_datastore: s3_datastore):
        ts = s3_datastore.latest_timestamp(location_of(asset))
        now = datetime.datetime.now(datetime.timezone.utc)
        age = None if ts is None else now - ts

        return dg.AssetCheckResult(
            passed=age is not None and age <= datetime.timedelta(days=max_days),
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "snapshot": ts.isoformat() if ts else None,
                "age_days": age.days if age else -1,
                "max_days": max_days,
            },
        )

    return _check

def schema_contract_factory(asset: dg.AssetsDefinition, schema: dict[str, str]):
    """Each contract column exists and has the expected dtype kind."""
    @dg.asset_check(asset=asset, name=CHECK_TYPES[1])
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):
        df = s3_datastore.read_gpq(context, location_of(asset))

        present = set(df.columns)
        missing = sorted(col for col in schema if col not in present)
        wrong_type = {
            col: str(df[col].dtype)
            for col, kind in schema.items()
            if col in present and not _dtype_matches(df[col], kind)
        }

        return dg.AssetCheckResult(
            passed=not missing and not wrong_type,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "missing_columns": dg.MetadataValue.json(missing),
                "wrong_type": dg.MetadataValue.json(wrong_type),
                "expected": dg.MetadataValue.json(dict(schema)),
                "actual_schema": dg.MetadataValue.json({col: str(df[col].dtype) for col in df.columns}),
            },
        )

    return _check

def row_uniqueness_factory(asset: dg.AssetsDefinition, subset: tuple[str, ...]):
    """Rows are unique over ``subset`` (geometry columns compared by WKB)."""
    @dg.asset_check(asset=asset, name=CHECK_TYPES[2])
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):

        df = s3_datastore.read_gpq(context, location_of(asset))
        keys = df[list(subset)].copy()
        for col in subset:
            if str(df[col].dtype) == "geometry":
                keys[col] = gpd.GeoSeries(df[col]).to_wkb()
        duplicates = int(keys.duplicated().sum())

        return dg.AssetCheckResult(
            passed=duplicates == 0,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "duplicate_rows": duplicates,
                "subset": list(subset),
                "total_rows": len(df),
            },
        )

    return _check

def field_completeness_factory(
    asset: dg.AssetsDefinition,
    required_columns: tuple[str, ...],
    max_null_ratio: float = 0.0,
):
    @dg.asset_check(asset=asset, name=CHECK_TYPES[3])
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):

        df = s3_datastore.read_gpq(context, location_of(asset))

        null_ratios = {}
        failing_columns = []

        for col in required_columns:
            null_count = df[col].isna().sum()
            ratio = null_count / len(df) if len(df) > 0 else 1.0

            null_ratios[col] = ratio

            if ratio > max_null_ratio:
                failing_columns.append(col)

        return dg.AssetCheckResult(
            passed=len(failing_columns) == 0,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "null_ratios": null_ratios,
                "max_null_ratio": max_null_ratio,
                "failing_columns": failing_columns,
            },
        )

    return _check
