"""When an asset re-emits a cached snapshot (bronze freshness hit OR a silver/gold
reuse_if_unchanged data-version hit, both flagged ``s3_cache_hit``), its checks
re-emit the prior verdict from the event log instead of re-reading S3.

A reused FAIL must keep its diagnostic metadata (duplicate_rows, subset, ...) so the
run report / SNS alert stays useful -- only a ``reused_snapshot`` flag is added.
"""

import dagster as dg
import pandas as pd
from dagster import DagsterInstance

from montreal.defs.assets.gold._config import GoldAssetDataContract
from montreal.defs.checks.factory import _reused_snapshot, standard_checks
from montreal.defs.resources.lakehouse import s3_datastore

# A frame that fails row_uniqueness on h3_r10 (two identical keys).
_FRAME = pd.DataFrame({"h3_r10": ["a", "a"], "livability": [10.0, 10.0]})
_CACHE_HIT = {"value": False}

CONTRACT = GoldAssetDataContract(
    schema={"h3_r10": "str", "livability": "numeric"},
    uniqueness=("h3_r10",),
    completeness=("h3_r10", "livability"),
    bounds={"livability": (0.0, 100.0)},
)


class FakeStore(s3_datastore):
    def setup_for_execution(self, context) -> None:
        pass

    def read_gpq(self, context, address):
        return _FRAME

    def read_gpq_prefix(self, context, prefix):
        return _FRAME


@dg.asset(name="reuse_fixture", metadata={"layer": "gold", "segmentation": "snapshot"})
def reuse_fixture(context, s3_datastore: s3_datastore) -> dg.MaterializeResult:
    # Stable data version; the s3_cache_hit flag is what the check-reuse path keys off.
    return dg.MaterializeResult(
        data_version=dg.DataVersion("v1"),
        metadata={"s3_cache_hit": _CACHE_HIT["value"]},
    )


def _run(instance) -> dict[str, dg.AssetCheckEvaluation]:
    checks = standard_checks(reuse_fixture, CONTRACT)
    result = dg.materialize(
        [reuse_fixture, *checks],
        instance=instance,
        resources={"s3_datastore": FakeStore(bucket_name="b", region_name="r")},
    )
    assert result.success
    return {ev.check_name: ev for ev in result.get_asset_check_evaluations()}


def _meta(ev) -> dict:
    return {k: getattr(v, "value", v) for k, v in (ev.metadata or {}).items()}


def test_partitioned_check_never_reuses():
    # Verdict history isn't partition-scoped, so a partitioned cache hit must
    # re-evaluate rather than risk re-emitting another partition's verdict. The
    # bare context would raise on any instance access past the partition gate.
    class Ctx:
        has_partition_key = True

    assert _reused_snapshot(Ctx(), reuse_fixture) is False


def test_reused_failing_check_keeps_its_diagnostic_metadata(tmp_path):
    _CACHE_HIT["value"] = False
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        first = _run(instance)
        assert first["row_uniqueness"].passed is False
        assert _meta(first["row_uniqueness"])["duplicate_rows"] == 1

        _CACHE_HIT["value"] = True
        reused = _run(instance)

        ev = reused["row_uniqueness"]
        assert ev.passed is False
        meta = _meta(ev)
        assert meta["reused_snapshot"] is True
        assert meta["duplicate_rows"] == 1
        assert meta["subset"] == ["h3_r10"]
