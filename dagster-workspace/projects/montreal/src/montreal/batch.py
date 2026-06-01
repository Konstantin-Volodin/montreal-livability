#!/usr/bin/env python
"""One-shot container entrypoint: materialize the asset graph in phases, then exit.

Phased because h3_montreal_addresses registers the dynamic `address_r6` partitions at
runtime, so the r6-partitioned distances can't resolve its partition set up front:
bronze and unpartitioned silver each run as one command (Dagster pipelines them by real
dependency edges), distances once per partition, gold last. The $DAGSTER_HOME SQLite
instance is synced to EFS around the run (restore/persist_state) so run history + dynamic
partitions survive between monthly tasks; data output is the S3 lakehouse.

Each check writes its verdict to the lakehouse; afterwards we read them into one
`quality/{run}.json` report and SNS-email the ERROR failures (recorded, not gated, so a
bad month doesn't wedge the batch).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
from dagster import DagsterInstance
from dagster._core.storage.asset_check_execution_record import AssetCheckExecutionRecordStatus

from montreal.defs.resources.state import efs_state

# A check's completed runs (excludes still-planned events that carry no verdict).
_DONE = {AssetCheckExecutionRecordStatus.SUCCEEDED, AssetCheckExecutionRecordStatus.FAILED}

MODULE = "montreal.definitions"
R6_PARTITIONS = "address_r6"

# Concurrent r6 partition runs. Each is its own `dagster asset materialize` process doing
# memory-heavy geo joins; pinned to match the task's 2 vCPU / 8 GB (memory is the ceiling,
# not cores). The processes share one local-disk SQLite instance, which takes concurrent
# writers fine. Override via env.
PARTITION_CONCURRENCY = int(os.environ.get("PARTITION_CONCURRENCY", "2"))

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_REGION = os.environ.get("S3_REGION", "ca-central-1")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN")


def summarize(results: list[dict]) -> tuple[str, list[dict]]:
    """(tally line, ERROR-severity failures). WARN failures count but don't alert."""
    ok = warn = 0
    errors = []
    for r in results:
        if r["passed"]: ok += 1
        elif r["severity"] == "WARN": warn += 1
        else: errors.append(r)
    return f"{ok} ok | {warn} warn | {len(errors)} fail", errors


def _check_results(instance, graph) -> list[dict]:
    """Every check's latest completed verdict, per partition -- so a failed shard isn't
    masked by a later passing one (a partitioned check runs once per r6 partition).

    Check evaluations don't store the partition, so resolve it from the run's
    ``dagster/partition`` tag (cached per run_id)."""
    els = instance.event_log_storage
    partition_of: dict[str, str | None] = {}

    def run_partition(run_id: str) -> str | None:
        if run_id not in partition_of:
            run = instance.get_run_by_id(run_id)
            partition_of[run_id] = run.tags.get("dagster/partition") if run else None
        return partition_of[run_id]

    results = []
    for key in graph.asset_check_keys:
        seen = set()
        for rec in els.get_asset_check_execution_history(key, limit=1000, status=_DONE):
            partition = run_partition(rec.run_id)
            if partition in seen:  # history is newest-first, so the first per partition is its latest
                continue
            seen.add(partition)
            e = rec.evaluation
            results.append({
                "asset": e.asset_key.to_user_string(), "check": e.check_name, "passed": e.passed,
                "severity": e.severity.value if e.severity else None,
                "metadata": {k: getattr(v, "value", v) for k, v in (e.metadata or {}).items()},
            })
    return results


def report_quality(run_stamp: str) -> None:
    """Gather every check verdict from the event log, save a report, log the tally, SNS-alert ERRORs."""
    from pathlib import Path

    import dagster as dg
    import montreal.definitions as md

    graph = dg.load_from_defs_folder(path_within_project=Path(md.__file__).parent).resolve_asset_graph()
    with DagsterInstance.get() as instance:
        results = _check_results(instance, graph)
    if not results:
        print("no check results found", flush=True)
        return

    summary, errors = summarize(results)
    print(f"\n=== quality: {summary} ===", flush=True)
    boto3.client("s3", region_name=S3_REGION).put_object(
        Bucket=S3_BUCKET, Key=f"quality/{run_stamp}.json",
        Body=json.dumps({"run": run_stamp, "results": results}, default=str).encode(),
        ContentType="application/json",
    )
    if errors and ALERT_TOPIC_ARN:
        boto3.client("sns", region_name=S3_REGION).publish(
            TopicArn=ALERT_TOPIC_ARN,
            Subject=f"[livability] {len(errors)} data-quality check(s) failed",
            Message="\n".join(f"{e['asset']} / {e['check']}: {json.dumps(e.get('metadata', {}))}" for e in errors),
        )
        print(f"alerted on {len(errors)} ERROR failure(s)", flush=True)


def materialize_partitions(partitions: list[str]) -> None:
    """Materialize distances once per r6 partition, PARTITION_CONCURRENCY at a time.

    Concurrent runs share one stdout, so each partition's output is captured and printed
    as a contiguous block (a one-line status on success, the full log on failure). Raises
    if any partition fails, so the batch surfaces it instead of silently moving to gold."""
    cmd = lambda p: [
        "dagster", "asset", "materialize", "-m", MODULE,
        "--select", "distances_to_amenities", "--partition", p
    ]
    failed, done = [], 0
    with ThreadPoolExecutor(max_workers=PARTITION_CONCURRENCY) as pool:
        futures = {pool.submit(subprocess.run, cmd(p), capture_output=True, text=True): p for p in partitions}
        for future in as_completed(futures):
            partition, result, done = futures[future], future.result(), done + 1
            ok = result.returncode == 0
            print(f"  [{done}/{len(partitions)}] {partition} {'ok' if ok else 'FAILED'}", flush=True)
            if not ok:
                failed.append(partition)
                print(result.stdout, result.stderr, sep="\n", file=sys.stderr, flush=True)
    if failed:
        raise RuntimeError(f"{len(failed)}/{len(partitions)} distance partition(s) failed: {sorted(failed)}")


def main() -> int:
    with efs_state():
        # pre partitioned section
        subprocess.run(["dagster", "job", "execute", "-m", MODULE, "-j", "pre_partition_job"], check=True)

        # partitioned section: distances, PARTITION_CONCURRENCY partitions at a time
        with DagsterInstance.get() as instance: partitions = sorted(instance.get_dynamic_partitions(R6_PARTITIONS))
        materialize_partitions(partitions)

        # gold layer
        subprocess.run(["dagster", "job", "execute", "-m", MODULE, "-j", "gold_job"], check=True)

        report_quality(f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
