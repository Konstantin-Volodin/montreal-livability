"""Tests for the batch entrypoint's quality reporting: the pure summary/tally and
ERROR-failure extraction that drives the SNS email."""

from montreal.batch import summarize


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
