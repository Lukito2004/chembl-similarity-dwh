import pytest

from chembl_sim.quality import runner
from chembl_sim.quality.checks import BLOCK, WARN, Check, at_least, build_checks, equals, is_zero
from chembl_sim.quality.runner import (
    QualityGateError,
    Result,
    describe,
    run_checks,
    select_checks,
)
from chembl_sim.settings import get_settings

PASSING = Check(name="passing", layer="test", severity=BLOCK, expected="0", query="SELECT 0")
BLOCKING = Check(name="blocking", layer="test", severity=BLOCK, expected="0", query="SELECT 1")
WARNING = Check(name="warning", layer="test", severity=WARN, expected="0", query="SELECT 1")


def test_check_names_are_unique():
    names = [check.name for check in build_checks()]
    assert len(names) == len(set(names))


def test_the_gate_covers_every_layer_that_writes_data():
    assert {check.layer for check in build_checks()} == {"silver", "source", "gold"}


def test_both_severities_are_in_use():
    assert {check.severity for check in build_checks()} == {BLOCK, WARN}


def test_every_query_asks_for_something():
    for check in build_checks():
        assert "SELECT" in check.query


def test_an_unknown_severity_is_rejected():
    with pytest.raises(ValueError, match="unknown severity"):
        Check(name="x", layer="test", severity="fatal", expected="0", query="SELECT 0")


def test_the_top_n_comes_from_settings(monkeypatch):
    monkeypatch.setenv("CHEMBL_SIMILARITY_TOP_N", "5")
    get_settings.cache_clear()
    check = next(c for c in build_checks() if c.name == "fact_holds_the_top_n_for_every_source")
    assert "count(*) <> 5" in check.query
    assert check.expected == "0 sources away from 5 matches"


def test_the_source_size_comes_from_settings(monkeypatch):
    monkeypatch.setenv("CHEMBL_SOURCE_TARGET_SIZE", "42")
    get_settings.cache_clear()
    check = next(c for c in build_checks() if c.name == "source_set_reaches_the_configured_size")
    assert check.holds(42)
    assert not check.holds(41)


def test_the_predicates():
    assert is_zero(0)
    assert not is_zero(1)
    assert equals(3)(3)
    assert not equals(3)(4)
    assert at_least(0.9)(0.9)
    assert not at_least(0.9)(0.89)


def test_select_checks_narrows_to_the_layer():
    narrowed = select_checks(layers=["silver"])
    assert narrowed
    assert {check.layer for check in narrowed} == {"silver"}


def test_select_checks_keeps_everything_when_no_layer_is_given():
    assert len(select_checks()) == len(build_checks())


def test_select_checks_rejects_a_layer_with_no_checks():
    with pytest.raises(ValueError, match="no quality checks"):
        select_checks(layers=["bronze"])


def test_describe_names_the_check_and_both_numbers():
    assert describe([Result(check=BLOCKING, observed=3.0)]) == "blocking observed 3, expected 0"


def test_every_result_is_recorded(fake_warehouse):
    connection = fake_warehouse(runner, [(0,)])
    report = run_checks("run-1", checks=[PASSING])
    cursor = connection.cursor_object
    assert cursor.statements[0] == "SELECT 0"
    assert "meta.quality_check" in cursor.statements[1]
    assert cursor.params[1] == ("run-1", "passing", "test", "block", 0.0, "0", True)
    assert report.summary() == {"checks": 1, "passed": 1, "warnings": 0, "blocking": 0}


def test_a_blocking_failure_stops_the_run(fake_warehouse):
    fake_warehouse(runner, [(1,)])
    with pytest.raises(QualityGateError, match="blocking observed 1, expected 0"):
        run_checks("run-2", checks=[BLOCKING])


def test_results_are_recorded_before_the_gate_raises(fake_warehouse):
    connection = fake_warehouse(runner, [(0,), (1,)])
    with pytest.raises(QualityGateError):
        run_checks("run-3", checks=[PASSING, BLOCKING])
    recorded = [s for s in connection.cursor_object.statements if "meta.quality_check" in s]
    assert len(recorded) == 2


def test_a_warning_is_reported_without_stopping_the_run(fake_warehouse):
    fake_warehouse(runner, [(1,)])
    report = run_checks("run-4", checks=[WARNING])
    assert report.summary() == {"checks": 1, "passed": 0, "warnings": 1, "blocking": 0}
    assert report.warnings[0].check.name == "warning"
    assert not report.blocking


def test_a_mixed_run_separates_the_two_severities(fake_warehouse):
    fake_warehouse(runner, [(0,), (1,)])
    report = run_checks("run-5", checks=[PASSING, WARNING])
    assert report.summary() == {"checks": 2, "passed": 1, "warnings": 1, "blocking": 0}
    assert [result.check.name for result in report.failures] == ["warning"]
