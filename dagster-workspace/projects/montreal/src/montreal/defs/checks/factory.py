"""
Reusable data-contract checks, bundled per asset into a single multi-check.

All of an asset's contract checks (schema, uniqueness, completeness, and
value_range when the contract carries bounds) run in ONE step that reads the
asset's data once. For a sharded asset that's a single pass over all shards
instead of a full 27-partition read per check.

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
    if kind == "numeric": return pd.api.types.is_numeric_dtype(series)
    if kind == "str": return pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)
    if kind == "geometry": return str(series.dtype) == "geometry"
    raise ValueError(f"Unknown schema kind {kind!r}")


def _read_checked(context, s3_datastore, asset: dg.AssetsDefinition):
    """Read the data this check run covers.

    A Dagster-partitioned asset runs its checks per-partition, so read only that
    partition's shard. A sharded-but-unpartitioned asset (``segmentation`` set to a
    column, e.g. ``h3_r6``) has no single snapshot, so concat its per-shard subdirs.
    Everything else is one snapshot at the base dir.
    """
    location = location_of(asset)
    if context.has_partition_key: 
        return s3_datastore.read_gpq(context, f"{location}/{context.partition_key}")
    segmentation = asset.metadata_by_key[asset.key].get("segmentation")
    if segmentation in (None, "snapshot"):
        return s3_datastore.read_gpq(context, location)
    return s3_datastore.read_gpq_prefix(context, location)


# --- individual contract assertions: pure ``df -> AssetCheckResult`` -------

def _schema_contract_result(df, schema: dict[str, str]) -> dg.AssetCheckResult:
    """Each contract column exists and has the expected dtype kind."""
    present = set(df.columns)
    missing = sorted(col for col in schema if col not in present)
    wrong_type = {
        col: str(df[col].dtype)
        for col, kind in schema.items()
        if col in present and not _dtype_matches(df[col], kind)
    }
    return dg.AssetCheckResult(
        check_name="schema_contract",
        passed=not missing and not wrong_type,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "missing_columns": dg.MetadataValue.json(missing),
            "wrong_type": dg.MetadataValue.json(wrong_type),
            "expected": dg.MetadataValue.json(dict(schema)),
            "actual_schema": dg.MetadataValue.json({col: str(df[col].dtype) for col in df.columns}),
        },
    )


def _row_uniqueness_result(df, subset: tuple[str, ...]) -> dg.AssetCheckResult:
    """Rows are unique over ``subset`` (geometry columns compared by WKB)."""
    keys = df[list(subset)].copy()
    for col in subset:
        if str(df[col].dtype) == "geometry":
            keys[col] = gpd.GeoSeries(df[col]).to_wkb()
    duplicates = int(keys.duplicated().sum())
    return dg.AssetCheckResult(
        check_name="row_uniqueness",
        passed=duplicates == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "duplicate_rows": duplicates,
            "subset": list(subset),
            "total_rows": len(df),
        },
    )


def _field_completeness_result(
    df, required_columns: tuple[str, ...], max_null_ratio: float = 0.0
) -> dg.AssetCheckResult:
    """No required column exceeds ``max_null_ratio`` nulls."""
    null_ratios = {}
    failing_columns = []
    for col in required_columns:
        ratio = df[col].isna().sum() / len(df) if len(df) > 0 else 1.0
        null_ratios[col] = ratio
        if ratio > max_null_ratio:
            failing_columns.append(col)
    return dg.AssetCheckResult(
        check_name="field_completeness",
        passed=len(failing_columns) == 0,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "null_ratios": null_ratios,
            "max_null_ratio": max_null_ratio,
            "failing_columns": failing_columns,
        },
    )


def _value_range_result(df, bounds: dict[str, tuple[float, float]]) -> dg.AssetCheckResult:
    """Each column stays within its inclusive ``(low, high)`` range (NaN ignored)."""
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
        check_name="value_range",
        passed=not violations,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "violations": dg.MetadataValue.json(violations),
            "bounds": dg.MetadataValue.json({c: list(b) for c, b in bounds.items()}),
        },
    )


def standard_checks(asset: dg.AssetsDefinition, contract) -> list:
    """Every check a data contract implies - schema, uniqueness, completeness, and
    value_range when the contract carries ``bounds`` (gold) - as a single multi-check
    that reads the asset once. Returned as a one-element list so the defs autoloader
    unpacks it, keeping every ``standard_checks(...)`` call site unchanged."""
    bounds = getattr(contract, "bounds", None)

    specs = [
        dg.AssetCheckSpec("schema_contract", asset=asset),
        dg.AssetCheckSpec("row_uniqueness", asset=asset),
        dg.AssetCheckSpec("field_completeness", asset=asset),
    ]
    if bounds:
        specs.append(dg.AssetCheckSpec("value_range", asset=asset))

    @dg.multi_asset_check(specs=specs, name=f"{asset.key.path[-1]}_contract_checks")
    def _checks(context: dg.AssetCheckExecutionContext, s3_datastore: s3_datastore):
        df = _read_checked(context, s3_datastore, asset)
        location = location_of(asset)
        results = [
            _schema_contract_result(df, contract.schema),
            _row_uniqueness_result(df, contract.uniqueness),
            _field_completeness_result(df, contract.completeness),
        ]
        if bounds:
            results.append(_value_range_result(df, bounds))
        for result in results:
            s3_datastore.write_check_result(context, location, result.check_name, result)
            yield result

    return [_checks]
