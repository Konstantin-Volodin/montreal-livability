# Issue Description: Implement Downstream Asset Caching & Conditional Execution in Dagster

## Context
Currently, our Bronze layer assets utilize Dagster's caching mechanism correctly. 
As long as the freshness data contract is satisfied, Bronze assets return cached data instead of re-fetching or re-computing.

## The Problem
Downstream Silver and Gold assets (including asset checks) are not respecting this cache. 
Even when the upstream Bronze data remains unchanged and is successfully served from the cache, the Silver assets trigger a full recalculation.

**Incorrect Behavior**: Silver assets re-compute using the cached Bronze data as input, which generates identical data but applies a brand-new timestamp.

**Expected Behavior**: Downstream computations (Silver/Gold) should only execute if the upstream Bronze data has actually been updated or if their own freshness policies have expired. Given our current data, Silver recomputations are unnecessary for the next month.

## Goal / Requirements
**Downstream Cache Hits**: Enable a mechanism where Silver and Gold layers perform a cache hit check. They should skip execution if their upstream dependencies haven't changed.

**Conditional Execution**: Silver/Gold assets should only recalculate when a Bronze freshness policy fails (forcing a Bronze redownload) or when a month has passed.

**Decoupled Architecture**: This logic needs to be handled cleanly—ideally natively within Dagster—without hardcoding brittle, asset-specific conditional checks inside @job definitions.

## Possible solutions:
- The Native Job-Level Solution: SourceAsset or DataVersions with a Selection
- The Code-Level Solution: DataVersion / Memoization

```
from dagster import define_asset_job

# This job will automatically skip assets if their inputs/code haven't changed
monthly_job = define_asset_job(
    name="monthly_pipeline",
    selection="*",
    config={"execution": {"config": {"memoizable": True}}} 
)
```

---

## Resolution

### Why the obvious paths don't apply here
- **`config={"execution": {"config": {"memoizable": True}}}`** is the legacy
  versioned/memoized executor. It was removed from Dagster; it does not exist in
  1.13.5. Dead end.
- **`AutomationCondition` / Declarative Automation** is the modern native skip,
  but it is evaluated by the **automation daemon**. This pipeline is serverless:
  one ephemeral job runs ~30 min once a month, no daemon the other 29 days. The
  daemon never ticks, so automation conditions never fire.
- A plain `define_asset_job` materializes its **entire selection unconditionally**.
  Dagster computes staleness/data-versions and shows them in the UI, but a manual
  job run never consults them. That is exactly why silver/gold recomputed even
  though bronze served a cached snapshot with an unchanged `DataVersion`.

The skip therefore has to happen **inside the run**, and the only durable
cross-run state is the lakehouse (S3) plus the **EFS-backed Dagster event log**.

### What was implemented
A native data-version cache gate: `defs/assets/_cache.py::reuse_if_unchanged`.
Dagster records, on every materialization, the data versions an asset consumed
(`DataProvenance.input_data_versions`) and its `code_version`; the EFS event log
persists this between the monthly runs. At the top of each derived asset the gate
compares the upstreams' *current* data versions against what the asset consumed
last time:
- all upstream versions unchanged **and** `code_version` unchanged -> re-emit the
  prior `DataVersion` with `s3_cache_hit=True`, skipping the heavy compute.
- otherwise -> recompute and emit a new `DataVersion`.

The contract checks already short-circuit on `s3_cache_hit` (see
`checks/factory.py::_reused_snapshot`), so a skipped asset also skips its checks,
re-emitting the prior verdicts instead of re-reading S3.

"A month has passed" needs no separate trigger: bronze freshness expiry forces a
redownload, which writes a new stamp -> new `DataVersion` -> the dependent
silver/gold input-version hash changes -> they recompute. Per-asset granularity
falls out for free (fresh-bronze-fed assets skip; changed-bronze-fed assets run).

### Scope / what is intentionally NOT gated
The gate reconstructs each upstream's current version from its latest record,
which is **not** the aggregate version Dagster records for a *partitioned*
upstream. So the gate is applied only to **unpartitioned assets with unpartitioned
upstreams** (the six h3/silver assets + `amenities` + `montreal_municipalities`).
Left always-recompute for now:
- `distances_to_amenities` — partitioned by r6.
- `livability_score` — unpartitioned but depends on the partitioned distances.
- `livability_map` — deliberately re-renders every run for a fresh "updated on"
  timestamp (no checks, no data contract).

Extending the gate to the partitioned cases is the natural follow-up (it needs the
partition-aware aggregate input version, verified against Dagster's provenance).

### Tests
- `tests/test_provenance_probe.py` — pins the load-bearing assumption that a
  `deps=[...]` edge is recorded in downstream `DataProvenance`.
- `tests/test_cache_gate.py` — end-to-end against a durable temp-dir instance:
  first run computes, unchanged second run hits cache (compute skipped), an
  upstream version change forces recompute.