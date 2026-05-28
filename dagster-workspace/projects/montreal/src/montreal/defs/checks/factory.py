"""
Allows reusing common asset checks for different data assets.

common checks:
- schema_contract
- row_uniqueness
- field_completeness
- value_range
"""

import dagster as dg
import geopandas as gpd
import pandas as pd

from montreal.defs.resources.lakehouse import location_of, s3_datastore


def _dtype_matches(series: pd.Series, kind: str) -> bool:
    """Whether a column's dtype matches a contract kind."""
    if kind == "numeric":
        return pd.api.types.is_numeric_dtype(series)
    if kind == "str":
        return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)
    if kind == "geometry":
        return str(series.dtype) == "geometry"
    raise ValueError(f"Unknown schema kind {kind!r}")


def _read_whole(context, s3_datastore, asset: dg.AssetsDefinition):
    """Read an asset's full dataset, concatenating shards for sharded (partitioned) assets.

    Sharded assets (metadata ``segmentation`` set to a column, e.g. ``h3_r6``) have no
    single ``_latest`` at their base dir, so they are read across all per-shard subdirs.
    """
    location = location_of(asset)
    segmentation = asset.metadata_by_key[asset.key].get("segmentation")
    if segmentation in (None, "snapshot"):
        return s3_datastore.read_gpq(context, location)
    return s3_datastore.read_gpq_prefix(context, location)


def schema_contract_factory(asset: dg.AssetsDefinition, schema: dict[str, str]):
    """Each contract column exists and has the expected dtype kind."""
    @dg.asset_check(asset=asset, name="schema_contract")
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):
        df = _read_whole(context, s3_datastore, asset)

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
    @dg.asset_check(asset=asset, name="row_uniqueness")
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):

        df = _read_whole(context, s3_datastore, asset)
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
    @dg.asset_check(asset=asset, name="field_completeness")
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):

        df = _read_whole(context, s3_datastore, asset)

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


def standard_checks(asset: dg.AssetsDefinition, contract) -> list:
    """Every check a data contract implies: schema, uniqueness, completeness, and
    value_range when the contract carries ``bounds`` (gold). Returned as a list so
    the defs autoloader unpacks it into individual checks."""
    checks = [
        schema_contract_factory(asset, contract.schema),
        row_uniqueness_factory(asset, contract.uniqueness),
        field_completeness_factory(asset, contract.completeness),
    ]
    if getattr(contract, "bounds", None):
        checks.append(value_range_factory(asset, contract.bounds))
    return checks


def value_range_factory(asset: dg.AssetsDefinition, bounds: dict[str, tuple[float, float]]):
    """Each column stays within its inclusive ``(low, high)`` range (NaN ignored)."""

    @dg.asset_check(asset=asset, name="value_range")
    def _check(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):
        df = _read_whole(context, s3_datastore, asset)

        violations = {}
        for col, (lo, hi) in bounds.items():
            series = pd.to_numeric(df[col], errors="coerce")
            below = int((series < lo).sum())
            above = int((series > hi).sum())
            if below or above:
                violations[col] = {
                    "below": below,
                    "above": above,
                    "min": float(series.min()),
                    "max": float(series.max()),
                }

        return dg.AssetCheckResult(
            passed=not violations,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={
                "violations": dg.MetadataValue.json(violations),
                "bounds": dg.MetadataValue.json({c: list(b) for c, b in bounds.items()}),
            },
        )

    return _check
