#!/usr/bin/env python
"""One-shot container entrypoint: materialize the asset graph in three runs, then exit.

Why not one run: h3_montreal_addresses registers the dynamic `address_r6` partitions
at runtime, so distances (r6-partitioned) and gold can't share a command that resolves
its partition set up front. That's the *only* barrier -- everything upstream of it goes
in a single run so Dagster's executor pipelines by real dependency edges (h3_parks
starts when its bronze lands, not after the whole bronze "stage" finishes). Output is
the S3 lakehouse; the $DAGSTER_HOME SQLite instance lives on local disk and is synced
to EFS around the run (restore_state/persist_state) so run history + dynamic partitions
survive between monthly tasks.

Each check writes its result to the lakehouse (see lakehouse.write_check_result); after
the pipeline we read them into one `quality/{run}.json` report, print a summary, and email
the ERROR-severity failures via SNS. Check failures are recorded, not gated -- the email
is the alert, so a bad month doesn't wedge the monthly batch.
"""

from __future__ import annotations

import json
import os
import shutil
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

DAGSTER_HOME = os.environ.get("DAGSTER_HOME", "")
DAGSTER_STATE_DIR = os.environ.get("DAGSTER_STATE_DIR")  # EFS mount; unset (local dev/tests) -> no persistence

# Concurrent r6 partition runs. Pinned, not os.cpu_count() (host cores on Fargate);
# the runs share one local-disk SQLite instance (synced to EFS around the batch),
# which handles concurrent writers fine. Override via env.
PARTITION_CONCURRENCY = int(os.environ.get("PARTITION_CONCURRENCY", "2"))

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


def restore_state() -> None:
    """Copy the durable instance from EFS into the local $DAGSTER_HOME (cold run: no-op)."""
    if DAGSTER_STATE_DIR and os.path.isdir(DAGSTER_STATE_DIR) and os.listdir(DAGSTER_STATE_DIR):
        shutil.copytree(DAGSTER_STATE_DIR, DAGSTER_HOME, dirs_exist_ok=True)
        print(f"restored Dagster instance from {DAGSTER_STATE_DIR}", flush=True)


def persist_state() -> None:
    """Copy the local $DAGSTER_HOME back to EFS so the next run inherits it."""
    if DAGSTER_STATE_DIR:
        shutil.copytree(DAGSTER_HOME, DAGSTER_STATE_DIR, dirs_exist_ok=True)
        print(f"persisted Dagster instance to {DAGSTER_STATE_DIR}", flush=True)


def _asset_graph():
    """The project's resolved asset graph (keys, groups, partition flags)."""
    from pathlib import Path

    import dagster as dg

    import montreal.definitions as md

    return dg.load_from_defs_folder(path_within_project=Path(md.__file__).parent).resolve_asset_graph()


def _stale(asset_graph, partition_of: str | None = None) -> set[str] | None:
    """Asset names (or partition keys of ``partition_of``) Dagster reports STALE/MISSING.

    Opens the EFS-restored instance fresh each call, so the verdict reflects every prior
    phase's writes. Returns None on any failure -- the caller then materializes the full
    selection, so a broken resolver over-materializes rather than skipping needed work.
    Uses internal `_core` APIs, pinned against dagster==1.13.5.
    """
    try:
        from dagster._core.asset_graph_view.asset_graph_view import AssetGraphView, TemporalContext
        from dagster._core.definitions.data_version import CachingStaleStatusResolver, StaleStatus
        from dagster._time import get_current_datetime

        recompute = {StaleStatus.STALE, StaleStatus.MISSING}
        with DagsterInstance.get() as instance:
            view = AssetGraphView(
                temporal_context=TemporalContext(effective_dt=get_current_datetime(), last_event_id=None),
                instance=instance, asset_graph=asset_graph,
            )
            resolver = CachingStaleStatusResolver(instance=instance, asset_graph=asset_graph, loading_context=view)
            if partition_of is not None:
                key = next(k for k in asset_graph.materializable_asset_keys if k.to_user_string() == partition_of)
                return {p for p in instance.get_dynamic_partitions(R6_PARTITIONS) if resolver.get_status(key, p) in recompute}
            return {
                k.to_user_string()
                for k in asset_graph.materializable_asset_keys
                if not asset_graph.get(k).is_partitioned and resolver.get_status(k) in recompute
            }
    except Exception as e:
        print(f"stale-status query failed ({e!r}); materializing the full selection", file=sys.stderr, flush=True)
        return None


def _run() -> int:
    ag = _asset_graph()
    # Unpartitioned silver: H3_indexed + amenities + municipalities (everything that is
    # neither bronze `raw_data` nor gold `analytics`, and not the partitioned distances).
    silver = {
        k.to_user_string() for k in ag.materializable_asset_keys
        if not ag.get(k).is_partitioned and ag.get(k).group_name not in ("raw_data", "analytics")
    }

    # Phase 1 -- bronze, always. Each raw asset self-gates on external (time) freshness:
    # young snapshot -> re-emit its stamp unchanged (cheap, keeps downstream FRESH); old
    # -> re-download with a new stamp. Dagster staleness can't see external age, so this
    # one gate stays.
    print("\n=== bronze: raw_data (always) ===", flush=True)
    materialize("group:raw_data")

    # Phase 2 -- silver: only what Dagster now reports stale. Queried *after* bronze ran,
    # so a bronze re-download has already advanced its DataVersion and the dependent
    # silver shows STALE. Sequential (not pipelined) so the verdict is current.
    stale = _stale(ag)
    targets = sorted(silver if stale is None else silver & stale)
    print(f"\n=== silver: {len(targets)}/{len(silver)} stale ===", flush=True)
    if targets:
        materialize(",".join(targets))

    # Phase 3 -- distances: one run per stale r6 partition. Partitions are registered by
    # h3_montreal_addresses (phase 2) or inherited from the EFS instance.
    with DagsterInstance.get() as instance:
        partitions = set(instance.get_dynamic_partitions(R6_PARTITIONS))
    stale_parts = _stale(ag, partition_of="distances_to_amenities")
    targets = sorted(partitions if stale_parts is None else partitions & stale_parts)
    print(f"\n=== distances_to_amenities: {len(targets)}/{len(partitions)} stale r6 partitions ===", flush=True)
    if targets:
        failed = materialize_partitions("distances_to_amenities", targets)
        if failed:
            print(f"{len(failed)}/{len(targets)} partition(s) failed: {sorted(failed)}", file=sys.stderr)
            return 1

    # Phase 4 -- gold: the map re-renders every run; the score only when stale.
    stale = _stale(ag)
    gold = (["livability_score"] if stale is None or "livability_score" in stale else []) + ["livability_map"]
    print(f"\n=== gold: {', '.join(gold)} ===", flush=True)
    materialize(",".join(gold))

    # Durable check results -> one run report + email on ERROR failures (recorded, not gated).
    report_quality(f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}")

    print("\nPipeline complete.", flush=True)
    return 0


def main() -> int:
    # Restore first, then run; the finally saves the instance even on the partition-failure
    # path so partial history persists. persist_state runs only once all materialize
    # subprocesses have joined - the DB is quiesced, so copytree grabs a consistent dir.
    restore_state()
    try:
        return _run()
    finally:
        persist_state()


if __name__ == "__main__":
    raise SystemExit(main())
