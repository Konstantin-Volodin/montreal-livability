from pathlib import Path

import dagster as dg


@dg.definitions
def defs():
    loaded = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    # The multiprocess executor parallelizes independent steps within a run -- the
    # H3-index + amenity assets in the unpartitioned upstream stage. It does NOT
    # parallelize partitions (those are separate runs; see montreal/batch.py).
    return dg.Definitions.merge(loaded, dg.Definitions(executor=dg.multiprocess_executor))
