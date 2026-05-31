"""Probe: does Dagster record upstream input data versions in an asset's
DataProvenance when the dependency is declared via `deps=[...]` (a graph dep,
NOT an IO-managed function arg)?

This is the load-bearing assumption behind the proposed silver/gold
DataVersion-gated memoization: the gate reads each upstream's current data
version and compares it to the input versions recorded in the downstream's own
last materialization. If `deps`-style edges are absent from provenance, the
whole approach is unworkable and we fall back to S3-stamp bookkeeping.
"""

import dagster as dg
from dagster import DagsterInstance
from dagster._core.definitions.data_version import (
    extract_data_provenance_from_entry,
    extract_data_version_from_entry,
)


@dg.asset(code_version="u1")
def _probe_upstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion("UP_V1"))


# downstream depends via deps=[...] -- a graph edge, not a function input.
@dg.asset(deps=[_probe_upstream], code_version="d1")
def _probe_downstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion("DOWN_V1"))


def test_deps_edge_is_recorded_in_downstream_provenance():
    instance = DagsterInstance.ephemeral()

    result = dg.materialize([_probe_upstream, _probe_downstream], instance=instance)
    assert result.success

    up_key = dg.AssetKey("_probe_upstream")
    down_key = dg.AssetKey("_probe_downstream")

    # The current data version of the upstream (what the gate would read live).
    up_rec = instance.get_latest_data_version_record(up_key)
    current_up_version = extract_data_version_from_entry(up_rec.event_log_entry)
    assert current_up_version == dg.DataVersion("UP_V1")

    # The downstream's provenance: did Dagster capture the deps-edge input version?
    down_rec = instance.get_latest_data_version_record(down_key)
    prov = extract_data_provenance_from_entry(down_rec.event_log_entry)
    assert prov is not None, "downstream has no DataProvenance at all"

    # The crux: the upstream key, reached only via deps=[...], must appear with
    # the version the downstream consumed.
    assert up_key in prov.input_data_versions, (
        f"deps-edge upstream missing from provenance; "
        f"keys present: {set(prov.input_data_versions)}"
    )
    assert prov.input_data_versions[up_key] == dg.DataVersion("UP_V1")

    # And code_version is tracked too (the other half of the gate's key).
    assert prov.code_version == "d1"
