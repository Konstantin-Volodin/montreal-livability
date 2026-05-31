"""Probe: how does Dagster record DataProvenance for the two PARTITIONED shapes
the montreal pipeline actually has, so the cache gate can be extended to them?

Case A -- partitioned downstream, unpartitioned upstream (``distances_to_amenities``):
  Each partition materializes separately. Question: does a per-partition
  materialization's provenance record the *unpartitioned* upstream's data
  version (so a single-partition gate works like the unpartitioned gate, just
  keyed by partition)?

Case B -- unpartitioned downstream, partitioned upstream (``livability_score``):
  Question: what does ``input_data_versions`` hold for a *partitioned* upstream
  key, and can we reconstruct that value live from the event log to compare?
"""

import dagster as dg
from dagster import DagsterInstance
from dagster._core.definitions.data_version import (
    extract_data_provenance_from_entry,
    extract_data_version_from_entry,
)

PARTS = dg.StaticPartitionsDefinition(["a", "b", "c"])


# --- Case A fixtures: unpartitioned upstream -> partitioned downstream ---
@dg.asset(code_version="u1")
def _pa_upstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion("UP_V1"))


@dg.asset(deps=[_pa_upstream], partitions_def=PARTS, code_version="d1")
def _pa_part_downstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion(f"DOWN_{context.partition_key}"))


def test_case_a_partition_provenance_records_unpartitioned_upstream():
    instance = DagsterInstance.ephemeral()

    assert dg.materialize([_pa_upstream], instance=instance).success
    for pk in ("a", "b", "c"):
        assert dg.materialize(
            [_pa_upstream, _pa_part_downstream], instance=instance,
            partition_key=pk, selection=[_pa_part_downstream],
        ).success

    up_key = dg.AssetKey("_pa_upstream")
    down_key = dg.AssetKey("_pa_part_downstream")

    # Per-partition own record carries its own data version.
    rec_a = instance.get_latest_data_version_record(down_key, partition_key="a")
    rec_b = instance.get_latest_data_version_record(down_key, partition_key="b")
    print("CASE A own version a:", extract_data_version_from_entry(rec_a.event_log_entry))
    print("CASE A own version b:", extract_data_version_from_entry(rec_b.event_log_entry))

    prov_a = extract_data_provenance_from_entry(rec_a.event_log_entry)
    print("CASE A partition a provenance input_data_versions:", prov_a.input_data_versions)
    print("CASE A partition a code_version:", prov_a.code_version)

    # The unpartitioned upstream should appear with its (single) live version.
    assert up_key in prov_a.input_data_versions, set(prov_a.input_data_versions)
    assert prov_a.input_data_versions[up_key] == dg.DataVersion("UP_V1")
    # And per-partition own data versions are distinct & retrievable.
    assert extract_data_version_from_entry(rec_a.event_log_entry) == dg.DataVersion("DOWN_a")
    assert extract_data_version_from_entry(rec_b.event_log_entry) == dg.DataVersion("DOWN_b")


# --- Case B fixtures: partitioned upstream -> unpartitioned downstream ---
@dg.asset(partitions_def=PARTS, code_version="u1")
def _pb_part_upstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion(f"UP_{context.partition_key}"))


@dg.asset(deps=[_pb_part_upstream], code_version="d1")
def _pb_downstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion("DOWN_V1"))


def test_case_b_what_is_recorded_for_partitioned_upstream():
    instance = DagsterInstance.ephemeral()

    for pk in ("a", "b", "c"):
        assert dg.materialize(
            [_pb_part_upstream], instance=instance, partition_key=pk,
        ).success
    assert dg.materialize(
        [_pb_part_upstream, _pb_downstream], instance=instance, selection=[_pb_downstream],
    ).success

    up_key = dg.AssetKey("_pb_part_upstream")
    down_key = dg.AssetKey("_pb_downstream")

    down_rec = instance.get_latest_data_version_record(down_key)
    prov = extract_data_provenance_from_entry(down_rec.event_log_entry)
    print("CASE B downstream provenance input_data_versions:", prov.input_data_versions)
    print("CASE B keys present:", set(prov.input_data_versions))

    # What does a bare get_latest_data_version_record return for a partitioned key?
    bare = instance.get_latest_data_version_record(up_key)
    print("CASE B bare latest record partition:", None if bare is None else bare.partition_key)
    print("CASE B bare latest version:",
          None if bare is None else extract_data_version_from_entry(bare.event_log_entry))

    # Per-partition versions of the upstream.
    for pk in ("a", "b", "c"):
        r = instance.get_latest_data_version_record(up_key, partition_key=pk)
        print(f"CASE B upstream[{pk}] version:",
              None if r is None else extract_data_version_from_entry(r.event_log_entry))

    recorded = prov.input_data_versions.get(up_key)
    print("CASE B recorded input version for partitioned upstream:", recorded)

    # Re-materialize downstream WITHOUT touching upstream: is recorded version stable?
    assert dg.materialize(
        [_pb_part_upstream, _pb_downstream], instance=instance, selection=[_pb_downstream],
    ).success
    down_rec2 = instance.get_latest_data_version_record(down_key)
    prov2 = extract_data_provenance_from_entry(down_rec2.event_log_entry)
    recorded2 = prov2.input_data_versions.get(up_key)
    print("CASE B recorded input version after unchanged re-run:", recorded2)
    assert recorded == recorded2, "partitioned-upstream input version not stable across unchanged runs"

    # The recorded value is a sha256 aggregate over the partition versions, NOT any
    # single partition's version -- so a bare latest-record read cannot reconstruct
    # it. (Confirmed above: bare read returns partition 'c' = UP_c, != the hash.)
    # That is exactly why the gate cannot key off a partitioned upstream and
    # livability_score is left always-recompute.
    assert recorded != extract_data_version_from_entry(bare.event_log_entry)
