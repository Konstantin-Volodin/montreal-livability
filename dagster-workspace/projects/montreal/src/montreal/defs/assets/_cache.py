"""Native data-version cache gate for derived (silver/gold) assets.

The bronze layer already serves a cached S3 snapshot while it is fresh and
re-emits the *same* ``DataVersion`` when it does (see ``raw_geo_asset``). This
module carries that skip one layer further: a derived asset can short-circuit
its own recompute when nothing it depends on has actually changed.

Why this exists instead of Dagster's declarative automation: the pipeline runs
as a single ephemeral job once a month with no daemon, so ``AutomationCondition``
never fires. The skip therefore has to happen *inside* the run. The durable
(EFS-backed) event log lets us do it natively: Dagster records, on every
materialization, the data versions of the upstreams the asset consumed
(``DataProvenance.input_data_versions``) plus its ``code_version``. Comparing
those against the upstreams' current data versions answers "did anything I
depend on change?" without any hand-rolled bookkeeping.

Scope: the gate keys off each *unpartitioned* upstream's current data version.
It is partition-aware on the *downstream* side -- a partitioned asset compares
its own per-partition provenance, so each r6 shard skips independently. But it
cannot key off a *partitioned upstream*: Dagster records that input as a sha256
aggregate over all partition versions (see ``DataVersionCache``), which a single
latest-record read does not reconstruct. Such an upstream simply never matches,
so the gate falls through to recompute -- safe, never a false hit. That is why
``livability_score`` (depends on the partitioned ``distances_to_amenities``) is
left always-recompute, while ``distances_to_amenities`` itself (partitioned, but
with unpartitioned upstreams) is gated per partition.
"""

import dagster as dg
from dagster._core.definitions.data_version import (
    extract_data_provenance_from_entry,
    extract_data_version_from_entry,
)


def reuse_if_unchanged(context: dg.AssetExecutionContext) -> dg.MaterializeResult | None:
    """Return a cache-hit ``MaterializeResult`` when this asset can skip recompute.

    A hit means: the asset has a prior materialization, its ``code_version`` is
    unchanged, and every upstream's current data version equals the version this
    asset consumed at that prior materialization. In that case the prior
    ``DataVersion`` is re-emitted unchanged (so downstream staleness stays put)
    with ``s3_cache_hit=True`` -- which the contract checks read to re-emit their
    prior verdicts rather than re-reading S3 (see ``checks/factory.py``).

    Returns ``None`` when anything changed or no prior run exists, meaning the
    caller must recompute as usual.
    """
    instance = context.instance
    asset_key = context.asset_key

    # For a partitioned asset each r6 shard runs as its own step with its own
    # provenance, so the cache key must be scoped to this partition -- otherwise
    # we'd compare against whichever partition happened to materialize last.
    partition_key = context.partition_key if context.has_partition_key else None

    # This asset's last materialization (this partition's, if partitioned): its
    # provenance carries both halves of the cache key (the input versions it
    # consumed + the code version it ran).
    own_record = instance.get_latest_data_version_record(asset_key, partition_key=partition_key)
    if own_record is None:
        return None
    provenance = extract_data_provenance_from_entry(own_record.event_log_entry)
    if provenance is None:
        return None

    # A code change must force a recompute even if the inputs are identical.
    current_code_version = context.assets_def.code_versions_by_key.get(asset_key)
    if provenance.code_version != current_code_version:
        return None

    # Every upstream's current data version must match what we consumed last time.
    for upstream_key in context.assets_def.dependency_keys:
        upstream_record = instance.get_latest_data_version_record(upstream_key)
        if upstream_record is None:
            return None  # upstream never materialized -> can't claim a hit
        current_version = extract_data_version_from_entry(upstream_record.event_log_entry)
        if provenance.input_data_versions.get(upstream_key) != current_version:
            return None

    prior_version = extract_data_version_from_entry(own_record.event_log_entry)
    if prior_version is None:
        return None

    context.log.info(
        f"{asset_key.to_user_string()}: inputs unchanged, code version held -- "
        f"reusing prior snapshot (data_version {prior_version.value})"
    )
    return dg.MaterializeResult(
        data_version=prior_version,
        metadata={"s3_cache_hit": True},
    )
