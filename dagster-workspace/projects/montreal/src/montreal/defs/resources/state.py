"""Durable Dagster instance storage on EFS for the serverless batch.

The batch runs as ephemeral tasks with no daemon, so the Dagster instance (run
history + dynamic partitions) would otherwise die with each task. EFS is durable
but slow under SQLite's locking, so the instance lives on local disk ($DAGSTER_HOME)
during the run and is copied to/from EFS around it.
"""

import os
import shutil
from contextlib import contextmanager

DAGSTER_HOME = os.environ.get("DAGSTER_HOME", "")
DAGSTER_STATE_DIR = os.environ.get("DAGSTER_STATE_DIR")  # EFS mount; unset (local dev/tests) -> no persistence


@contextmanager
def efs_state():
    """Restore the instance from EFS, run the batch, persist it back.

    No-op without DAGSTER_STATE_DIR (local dev/tests). Persist runs even on
    failure so partial history survives.
    """
    if DAGSTER_STATE_DIR and os.path.isdir(DAGSTER_STATE_DIR) and os.listdir(DAGSTER_STATE_DIR):
        shutil.copytree(DAGSTER_STATE_DIR, DAGSTER_HOME, dirs_exist_ok=True)
        print(f"restored Dagster instance from {DAGSTER_STATE_DIR}", flush=True)
    try:
        yield
    finally:
        if DAGSTER_STATE_DIR:
            shutil.copytree(DAGSTER_HOME, DAGSTER_STATE_DIR, dirs_exist_ok=True)
            print(f"persisted Dagster instance to {DAGSTER_STATE_DIR}", flush=True)
