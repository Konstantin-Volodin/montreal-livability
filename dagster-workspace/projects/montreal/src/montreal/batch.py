#!/usr/bin/env python
"""One-shot container entrypoint: materialize the asset graph in three runs, then exit.

Why not one run: h3_montreal_addresses registers the dynamic `address_r6` partitions
at runtime, so distances (r6-partitioned) and gold can't share a command that resolves
its partition set up front. That's the *only* barrier -- everything upstream of it goes
in a single run so Dagster's executor pipelines by real dependency edges (h3_parks
starts when its bronze lands, not after the whole bronze "stage" finishes). Output is
the S3 lakehouse; the $DAGSTER_HOME SQLite instance is throwaway and dies with the task.

Each check writes its result to the lakehouse (see lakehouse.write_check_result); after
the pipeline we read them into one `quality/{run}.json` report, print a summary, and email
the ERROR-severity failures via SNS. Check failures are recorded, not gated -- the email
is the alert, so a bad month doesn't wedge the monthly batch.
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

MODULE = "montreal.definitions"
R6_PARTITIONS = "address_r6"

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_REGION = os.environ.get("S3_REGION", "ca-central-1")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN")  # SNS topic for ERROR-check failures; unset -> no email

# Concurrent r6 partition runs. Pinned, not os.cpu_count() (host cores on Fargate);
# the runs share one throwaway SQLite instance. Override via env.
PARTITION_CONCURRENCY = int(os.environ.get("PARTITION_CONCURRENCY", "2"))

# Every non-partitioned asset, comma-unioned into one selection (CLI splits on ",").
# Explicit (no "+"): each token names exactly what it covers. Assets self-gate on
# freshness/upstream change, so re-runs are cheap; only livability_map always re-renders.
UPSTREAM_SELECTION = ",".join([
    "group:raw_data",           # bronze: addresses, bike paths, municipality boundaries, parks, pois, transit stops
    "group:H3_indexed",         # silver h3 layers; h3_montreal_addresses registers the r6 partitions
    "amenities",                # silver amenity points
    "montreal_municipalities",  # silver municipalities
])

# Gold, after every r6 distance partition exists: livability_score then livability_map.
GOLD_SELECTION = "group:analytics"


def _cmd(selection: str, partition: str | None = None) -> list[str]:
    cmd = ["dagster", "asset", "materialize", "-m", MODULE, "--select", selection]
    return cmd + ["--partition", partition] if partition else cmd


def materialize(selection: str) -> None:
    """Run one ``dagster asset materialize``, streaming its output live."""
    subprocess.run(_cmd(selection), check=True)


def materialize_partitions(selection: str, partitions: list[str]) -> list[str]:
    """Run `selection` once per partition, PARTITION_CONCURRENCY at a time.

    Concurrent runs share one stdout, so output is captured and printed as a
    contiguous block per partition (live streaming would interleave). On success
    only a one-line status; the full log is dumped on failure. Returns the failures.
    """
    failed: list[str] = []
    done = 0
    with ThreadPoolExecutor(max_workers=PARTITION_CONCURRENCY) as pool:
        futures = {pool.submit(subprocess.run, _cmd(selection, p), capture_output=True, text=True): p for p in partitions}
        for future in as_completed(futures):
            partition, result, done = futures[future], future.result(), done + 1
            ok = result.returncode == 0
            print(f"  [{done}/{len(partitions)}] {partition} {'ok' if ok else 'FAILED'}", flush=True)
            if not ok:
                failed.append(partition)
                print(result.stdout, result.stderr, sep="\n", file=sys.stderr, flush=True)
    return failed


def collect_check_results(s3) -> list[dict]:
    """Every check result the run wrote, read from ``{asset}/_checks/*.json`` across the bucket."""
    results = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            if "/_checks/" in obj["Key"] and obj["Key"].endswith(".json"):
                results.append(json.loads(s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()))
    return results


def summarize(results: list[dict]) -> tuple[str, list[dict]]:
    """A status line per check plus a tally; returns (printable summary, failed ERROR results)."""
    ok = warn = 0
    errors: list[dict] = []
    lines = []
    for r in sorted(results, key=lambda r: (r["asset"], r["check"])):
        if r["passed"]:
            status, ok = "ok", ok + 1
        elif r.get("severity") == "WARN":
            status, warn = "WARN", warn + 1
        else:
            status = "FAIL"
            errors.append(r)
        lines.append(f"  {r['asset']:<34} {r['check']:<18} {status}")
    lines.append(f"  {ok} ok | {warn} warn | {len(errors)} fail")
    return "\n".join(lines), errors


def email_errors(errors: list[dict], run_stamp: str) -> None:
    """SNS-publish the ERROR-severity failures (no-op if ALERT_TOPIC_ARN is unset)."""
    if not ALERT_TOPIC_ARN:
        print(f"{len(errors)} ERROR check(s) failed but ALERT_TOPIC_ARN is unset; not emailing.", file=sys.stderr, flush=True)
        return
    body = f"{len(errors)} data-quality ERROR check(s) failed in run {run_stamp}:\n\n" + "\n".join(
        f"- {e['asset']} / {e['check']}: {json.dumps(e.get('metadata', {}))}" for e in errors
    )
    boto3.client("sns", region_name=S3_REGION).publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject=f"[livability] {len(errors)} data-quality check(s) failed",
        Message=body,
    )
    print(f"emailed {len(errors)} failure(s) via SNS", flush=True)


def report_quality(run_stamp: str) -> None:
    """Read check results, print a summary, persist ``quality/{run}.json``, email ERROR failures."""
    s3 = boto3.client("s3", region_name=S3_REGION)
    results = collect_check_results(s3)
    if not results:
        print("no check results found", flush=True)
        return
    summary, errors = summarize(results)
    print("\n=== quality summary ===\n" + summary, flush=True)
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"quality/{run_stamp}.json",
        Body=json.dumps({"run": run_stamp, "results": results}, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    if errors:
        email_errors(errors, run_stamp)


def main() -> int:
    # Run 1: all non-partitioned assets; the executor pipelines bronze -> silver.
    print("\n=== upstream: bronze + silver (pipelined) ===", flush=True)
    materialize(UPSTREAM_SELECTION)

    # Run 2: one materialize per r6 cell, so the partitions must already be registered.
    with DagsterInstance.get() as instance: partitions = sorted(instance.get_dynamic_partitions(R6_PARTITIONS))
    print(f"\n=== distances_to_amenities: {len(partitions)} r6 partitions ===", flush=True)
    failed = materialize_partitions("distances_to_amenities", partitions)
    if failed:
        print(f"{len(failed)}/{len(partitions)} partition(s) failed: {sorted(failed)}", file=sys.stderr)
        return 1

    # Run 3: gold.
    print("\n=== gold: score + report ===", flush=True)
    materialize(GOLD_SELECTION)

    # Durable check results -> one run report + email on ERROR failures (recorded, not gated).
    report_quality(f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}")

    print("\nPipeline complete.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
