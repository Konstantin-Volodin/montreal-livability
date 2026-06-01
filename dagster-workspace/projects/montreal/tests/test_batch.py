"""Tests for the batch entrypoint's quality reporting: the pure summary/tally and
ERROR-failure extraction, plus per-partition gathering from the event log."""

import dagster as dg
from dagster import DagsterInstance

from montreal.batch import _check_results, summarize


def _r(asset, check, passed, severity="ERROR"):
    return {"asset": asset, "check": check, "passed": passed, "severity": severity}


def test_summarize_tallies_and_extracts_only_error_failures():
    results = [
        _r("silver/amenities", "schema_contract", True),
        _r("bronze/montreal_pois", "field_completeness", False, "WARN"),  # warn, not an error
        _r("bronze/montreal_pois", "row_uniqueness", False, "ERROR"),     # the one true failure
    ]
    summary, errors = summarize(results)
    assert "1 ok | 1 warn | 1 fail" in summary
    assert [e["check"] for e in errors] == ["row_uniqueness"]


def test_summarize_handles_all_passing():
    summary, errors = summarize([_r("silver/amenities", "schema_contract", True)])
    assert errors == []
    assert "1 ok | 0 warn | 0 fail" in summary



_PARTS = dg.StaticPartitionsDefinition(["a", "b"])
_FAILING = {"a"}  # which partition's check currently fails


@dg.asset(partitions_def=_PARTS)
def _p_asset(context):
    return dg.MaterializeResult()


@dg.asset_check(asset=_p_asset)
def _p_check(context):
    return dg.AssetCheckResult(passed=context.partition_key not in _FAILING)


def _materialize(instance, pk):
    assert dg.materialize([_p_asset, _p_check], instance=instance, partition_key=pk).success


def test_check_results_surfaces_each_partition_and_keeps_latest(tmp_path):
    _FAILING.clear()
    _FAILING.add("a")
    graph = dg.Definitions(assets=[_p_asset], asset_checks=[_p_check]).resolve_asset_graph()
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        _materialize(instance, "a")  # check fails for partition a
        _materialize(instance, "b")  # passes for partition b

        # both partitions' verdicts surface -- a's failure is NOT masked by b's pass
        verdicts = {(r["check"], r["passed"]) for r in _check_results(instance, graph)}
        assert ("_p_check", False) in verdicts
        assert ("_p_check", True) in verdicts

        _FAILING.discard("a")        # a is fixed
        _materialize(instance, "a")  # re-run a -> now passes
        latest = {r["passed"] for r in _check_results(instance, graph) if r["check"] == "_p_check"}
        assert latest == {True}      # stale failure gone; latest-per-partition wins
