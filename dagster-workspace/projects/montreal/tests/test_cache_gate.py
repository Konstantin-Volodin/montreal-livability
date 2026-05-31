"""The reuse_if_unchanged() cache gate, exercised end-to-end against a durable
(temp-dir-backed) Dagster instance -- the EFS event log stands in here as a
local temp dir so DataProvenance survives between the separate materialize calls,
exactly as it must between the monthly serverless runs.

Covers the three behaviours the gate promises:
  * first run -> recompute (no prior provenance)
  * second run, upstream unchanged -> cache hit (compute skipped, version held)
  * upstream data version changes -> recompute
"""

import dagster as dg
import pytest
from dagster import DagsterInstance

from montreal.defs.assets._cache import reuse_if_unchanged

# Module-level side channels: the controllable upstream version and a record of
# every time the downstream actually ran its compute body.
_UPSTREAM_VERSION = {"value": "UP_V1"}
_COMPUTE_CALLS: list[str] = []


@dg.asset(code_version="c1")
def gate_upstream(context) -> dg.MaterializeResult:
    return dg.MaterializeResult(data_version=dg.DataVersion(_UPSTREAM_VERSION["value"]))


@dg.asset(deps=[gate_upstream], code_version="c1")
def gate_downstream(context) -> dg.MaterializeResult:
    if cached := reuse_if_unchanged(context):
        return cached  # inputs unchanged -> skip the (here, trivial) compute
    _COMPUTE_CALLS.append(_UPSTREAM_VERSION["value"])
    return dg.MaterializeResult(
        data_version=dg.DataVersion(f"DOWN_{len(_COMPUTE_CALLS)}"),
        metadata={"s3_cache_hit": False},
    )


def _materialize(instance):
    result = dg.materialize([gate_upstream, gate_downstream], instance=instance)
    assert result.success
    return result


def _down_cache_hit(result) -> bool:
    """Read the s3_cache_hit flag off the gate_downstream materialization."""
    for event in result.get_asset_materialization_events():
        mat = event.event_specific_data.materialization
        if mat.asset_key == dg.AssetKey("gate_downstream"):
            flag = mat.metadata.get("s3_cache_hit")
            return bool(getattr(flag, "value", flag))
    raise AssertionError("no gate_downstream materialization found")


@pytest.fixture
def instance(tmp_path):
    _COMPUTE_CALLS.clear()
    _UPSTREAM_VERSION["value"] = "UP_V1"
    with DagsterInstance.local_temp(str(tmp_path)) as inst:
        yield inst


def test_first_run_computes(instance):
    result = _materialize(instance)
    assert _COMPUTE_CALLS == ["UP_V1"]  # nothing cached yet
    assert _down_cache_hit(result) is False


def test_second_run_unchanged_hits_cache(instance):
    _materialize(instance)  # seed
    result = _materialize(instance)  # upstream version identical

    assert _COMPUTE_CALLS == ["UP_V1"]  # compute did NOT run a second time
    assert _down_cache_hit(result) is True


def test_upstream_version_change_forces_recompute(instance):
    _materialize(instance)  # seed at UP_V1
    _UPSTREAM_VERSION["value"] = "UP_V2"  # upstream produces new data
    result = _materialize(instance)

    assert _COMPUTE_CALLS == ["UP_V1", "UP_V2"]  # recomputed on the change
    assert _down_cache_hit(result) is False
