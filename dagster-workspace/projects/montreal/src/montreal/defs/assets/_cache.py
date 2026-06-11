"""In-run data-version cache gate (silver/gold; bronze composes it with its freshness window).

The pipeline runs as one ephemeral job/month with no daemon, so
``AutomationCondition`` never fires -- the skip has to happen inside the run.
Dagster records each materialization's input data versions + ``code_version`` in
the durable (EFS) event log; comparing those against the upstreams' current
versions tells us whether anything changed, with no extra bookkeeping.

Keys off each *unpartitioned* upstream's version. A partitioned upstream is
recorded as a sha256 aggregate over all partition versions that a single
latest-record read can't reconstruct, so it never matches and the gate just
recomputes (safe) -- hence ``livability_score`` is left always-recompute.
"""

import dagster as dg
from dagster._core.definitions.data_version import (
    extract_data_provenance_from_entry,
    extract_data_version_from_entry,
)


def reuse_if_unchanged(context: dg.AssetExecutionContext) -> dg.MaterializeResult | None:
    """A cache-hit ``MaterializeResult`` when this asset can skip recompute, else ``None``.

    Hit = a prior materialization exists, its ``code_version`` is unchanged, and
    every upstream's current data version matches what was consumed then. The
    prior ``DataVersion`` is re-emitted with ``s3_cache_hit=True`` (which the
    contract checks read to skip re-evaluating S3; see ``checks/factory.py``).
    """
    instance = context.instance
    asset_key = context.asset_key
    name = asset_key.to_user_string()

    def miss(reason: str) -> None:
        """Log why the gate fell through to recompute (one line, INFO)."""
        context.log.info(f"{name}: cache miss -> recompute ({reason})")

    # Scope to this partition: each shard has its own provenance, else we'd
    # compare against whichever partition materialized last.
    partition_key = context.partition_key if context.has_partition_key else None

    # Own last materialization -> the input versions + code version it ran with.
    own_record = instance.get_latest_data_version_record(asset_key, partition_key=partition_key)
    if own_record is None:
        miss("no prior materialization on this instance")
        return None
    provenance = extract_data_provenance_from_entry(own_record.event_log_entry)
    if provenance is None:
        miss("prior materialization carries no data provenance")
        return None

    # A code change must force a recompute even if the inputs are identical.
    current_code_version = context.assets_def.code_versions_by_key.get(asset_key)
    if provenance.code_version != current_code_version:
        miss(f"code_version changed {provenance.code_version!r} -> {current_code_version!r}")
        return None

    # Every upstream's current data version must match what we consumed last time.
    for upstream_key in context.assets_def.dependency_keys:
        upstream_record = instance.get_latest_data_version_record(upstream_key)
        if upstream_record is None:
            miss(f"upstream {upstream_key.to_user_string()} never materialized")
            return None  # can't claim a hit
        current_version = extract_data_version_from_entry(upstream_record.event_log_entry)
        prior = provenance.input_data_versions.get(upstream_key)
        if prior != current_version:
            prior_v = prior.value if prior else None
            current_v = current_version.value if current_version else None
            miss(f"upstream {upstream_key.to_user_string()} version {prior_v} -> {current_v}")
            return None

    prior_version = extract_data_version_from_entry(own_record.event_log_entry)
    if prior_version is None:
        miss("prior materialization carries no data version")
        return None

    context.log.info(
        f"{asset_key.to_user_string()}: inputs unchanged, code version held -- "
        f"reusing prior snapshot (data_version {prior_version.value})"
    )
    return dg.MaterializeResult(
        data_version=prior_version,
        metadata={"s3_cache_hit": True},
    )
