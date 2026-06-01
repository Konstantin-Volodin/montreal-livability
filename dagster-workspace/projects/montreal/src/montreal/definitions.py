import os
from pathlib import Path

import dagster as dg

# Pinned, not os.cpu_count() (the default) -- on Fargate that reports host cores,
# not the task vCPU, and OOM-kills the geo workers. Override via env.
MAX_CONCURRENT = int(os.environ.get("DAGSTER_MAX_CONCURRENT", "2"))


@dg.definitions
def defs():
    loaded = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    executor = dg.multiprocess_executor.configured({"max_concurrent": MAX_CONCURRENT})
    return dg.Definitions.merge(loaded, dg.Definitions(executor=executor))
