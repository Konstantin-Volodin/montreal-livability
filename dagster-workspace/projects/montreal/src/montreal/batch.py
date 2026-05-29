#!/usr/bin/env python
"""One-shot orchestration entrypoint for the Montreal livability pipeline.

Materializes the whole asset graph in dependency-correct stages against a
local (in-container) Dagster instance, then exits. This is the container
ENTRYPOINT of the Fargate task that EventBridge Scheduler launches monthly
(run as ``python -m montreal.batch``).

Why staged, instead of a single ``dagster asset materialize '*'``:

* ``h3_montreal_addresses`` registers the dynamic ``address_r6`` partitions at
  runtime, and ``distances_to_amenities`` (plus the gold layer downstream) are
  partitioned by those cells. The partitions do not exist until that asset
  runs, yet a single ``materialize`` command resolves its partition set up
  front -- so the partitioned assets must run in a *later* command.

Parallelism: the unpartitioned stages run their independent steps in parallel
via the ``multiprocess_executor`` on the Definitions (see ``montreal.definitions``).
The partitioned distance stage is parallelized differently -- Dagster runs one
partition per run, so partition parallelism means launching concurrent runs,
which is what ``materialize_partitions`` does (bounded by ``PARTITION_CONCURRENCY``).

Durable output is the S3 lakehouse; the SQLite instance under ``$DAGSTER_HOME``
is throwaway and dies with the task.
"""

from __future__ import annotations

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from dagster import DagsterInstance

MODULE = "montreal.definitions"
R6_PARTITIONS = "address_r6"

# Concurrent r6 partition runs. Kept modest by default (vCPU count) because the
# concurrent runs share one throwaway SQLite instance; pushing this high invites
# SQLite write contention for little gain on a monthly batch. Tune via env.
PARTITION_CONCURRENCY = int(os.environ.get("PARTITION_CONCURRENCY", os.cpu_count() or 2))

# Unpartitioned stages, in dependency order. Each value is a Dagster asset
# selection query ("+x" = x and everything upstream of it).
UPSTREAM_STAGES: list[tuple[str, str]] = [
    ("addresses", "montreal_addresses"),
    ("h3 index + register r6 partitions", "h3_montreal_addresses"),
    ("amenity candidate points", "+amenities"),
    ("municipality reference", "+montreal_municipalities"),
]

# Gold aggregation, run after every r6 partition exists.
GOLD_STAGES: list[tuple[str, str]] = [
    ("livability score", "livability_score"),
    ("livability map + HTML report", "livability_map"),
]


def materialize(selection: str, *, partition: str | None = None) -> None:
    """Run one ``dagster asset materialize`` against the local instance."""
    cmd = ["dagster", "asset", "materialize", "-m", MODULE, "--select", selection]
    if partition is not None:
        cmd += ["--partition", partition]
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def materialize_partitions(selection: str, partitions: list[str]) -> list[str]:
    """Materialize ``selection`` once per partition, up to PARTITION_CONCURRENCY at
    a time. Returns the partitions whose run failed (empty == all succeeded)."""
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=PARTITION_CONCURRENCY) as pool:
        futures = {
            pool.submit(materialize, selection, partition=p): p for p in partitions
        }
        for future in as_completed(futures):
            partition = futures[future]
            try:
                future.result()
            except subprocess.CalledProcessError:
                failed.append(partition)
    return failed


def main() -> int:
    # Stage 1 - unpartitioned upstream (bronze, h3 index, amenities, reference).
    for label, selection in UPSTREAM_STAGES:
        print(f"\n=== {label} ===", flush=True)
        materialize(selection)

    # Stage 2 - the r6-partitioned distance asset, one run per cell (concurrent).
    with DagsterInstance.get() as instance:
        partitions = sorted(instance.get_dynamic_partitions(R6_PARTITIONS))
    if not partitions:
        print(
            "No 'address_r6' partitions were registered by h3_montreal_addresses; "
            "aborting before the distance/gold stages.",
            file=sys.stderr,
        )
        return 1

    print(
        f"\n=== distances_to_amenities: {len(partitions)} r6 partitions "
        f"({PARTITION_CONCURRENCY}-way concurrent) ===",
        flush=True,
    )
    failed = materialize_partitions("distances_to_amenities", partitions)
    if failed:
        print(
            f"{len(failed)}/{len(partitions)} partition(s) failed: {sorted(failed)}",
            file=sys.stderr,
        )
        return 1

    # Stage 3 - gold aggregation + report.
    for label, selection in GOLD_STAGES:
        print(f"\n=== {label} ===", flush=True)
        materialize(selection)

    print("\nPipeline complete.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
