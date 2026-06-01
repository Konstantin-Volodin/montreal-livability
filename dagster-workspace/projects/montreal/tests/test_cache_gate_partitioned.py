"""The reuse_if_unchanged() gate on a PARTITIONED downstream with unpartitioned
upstreams -- the ``distances_to_amenities`` shape (per-r6 shard, fed by the
unpartitioned ``h3_montreal_addresses`` + ``amenities``).

Each partition must skip independently off its OWN per-partition provenance, so
the gate has to scope its lookup to ``context.partition_key`` -- otherwise a
partition would compare against whichever shard materialized last.

Mirrors test_cache_gate.py against a durable temp-dir instance:
  * first run of a partition -> recompute
  * unchanged second run of that partition -> cache hit (compute skipped)
  * one partition recomputes while another still hits, after upstream changes
"""

import dagster as dg
import pytest
from dagster import DagsterInstance

from montreal.defs.assets._cache import reuse_if_unchanged

PARTS = dg.StaticPartitionsDefinition(["a", "b", "c"])

_UPSTREAM_VERSION = {"value": "UP_V1"}
_COMPUTE_CALLS: list[tuple[str, str]] = []


@dg.asset(code_version="c1")
def gate_upstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion(_UPSTREAM_VERSION["value"]))


@dg.asset(deps=[gate_upstream], partitions_def=PARTS, code_version="c1")
def gate_part_downstream(context) -> dg.MaterializeResult:
    if cached := reuse_if_unchanged(context):
        return cached
    _COMPUTE_CALLS.append((context.partition_key, _UPSTREAM_VERSION["value"]))
    return dg.MaterializeResult(
        data_version=dg.DataVersion(f"DOWN_{context.partition_key}_{_UPSTREAM_VERSION['value']}"),
        metadata={"s3_cache_hit": False},
    )


def _materialize(instance, partition_key):
    result = dg.materialize(
        [gate_upstream, gate_part_downstream],
        instance=instance,
        partition_key=partition_key,
        selection=[gate_part_downstream],
    )
    assert result.success
    return result


def _seed_upstream(instance):
    assert dg.materialize([gate_upstream], instance=instance).success


def _down_cache_hit(result) -> bool:
    for event in result.get_asset_materialization_events():
        mat = event.event_specific_data.materialization
        if mat.asset_key == dg.AssetKey("gate_part_downstream"):
            flag = mat.metadata.get("s3_cache_hit")
            return bool(getattr(flag, "value", flag))
    raise AssertionError("no gate_part_downstream materialization found")


@pytest.fixture
def instance(tmp_path):
    _COMPUTE_CALLS.clear()
    _UPSTREAM_VERSION["value"] = "UP_V1"
    with DagsterInstance.local_temp(str(tmp_path)) as inst:
        yield inst


def test_first_run_of_partition_computes(instance):
    _seed_upstream(instance)
    result = _materialize(instance, "a")
    assert _COMPUTE_CALLS == [("a", "UP_V1")]
    assert _down_cache_hit(result) is False


def test_unchanged_partition_hits_cache(instance):
    _seed_upstream(instance)
    _materialize(instance, "a")  # seed partition a
    result = _materialize(instance, "a")  # upstream unchanged

    assert _COMPUTE_CALLS == [("a", "UP_V1")]  # compute did NOT run again
    assert _down_cache_hit(result) is True


def test_partitions_skip_independently(instance):
    """A partition seeded before an upstream change still hits; one seeded after
    the change recomputes -- proving the gate keys off per-partition provenance,
    not a single shared marker."""
    _seed_upstream(instance)
    _materialize(instance, "a")  # partition a seeded at UP_V1

    # Upstream changes; re-seed the unpartitioned upstream so its live version moves.
    _UPSTREAM_VERSION["value"] = "UP_V2"
    _seed_upstream(instance)

    res_b = _materialize(instance, "b")  # never seen -> computes at UP_V2
    res_a = _materialize(instance, "a")  # its input moved UP_V1 -> UP_V2 -> recomputes

    assert ("b", "UP_V2") in _COMPUTE_CALLS
    assert ("a", "UP_V2") in _COMPUTE_CALLS  # a recomputes because its upstream changed
    assert _down_cache_hit(res_b) is False
    assert _down_cache_hit(res_a) is False


def test_unchanged_partition_hits_after_other_partition_runs(instance):
    """Partition a stays a cache hit even after partition b materializes in
    between -- the gate must not be fooled by b being the latest materialization."""
    _seed_upstream(instance)
    _materialize(instance, "a")  # seed a
    _materialize(instance, "b")  # b is now the latest materialization of the asset
    result = _materialize(instance, "a")  # a's own inputs unchanged

    assert _down_cache_hit(result) is True
    assert _COMPUTE_CALLS == [("a", "UP_V1"), ("b", "UP_V1")]  # a did not recompute
