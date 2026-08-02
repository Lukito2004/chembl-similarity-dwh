"""Evaluates the quality checks and records what every run observed."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from chembl_sim.logging_setup import get_logger
from chembl_sim.quality.checks import BLOCK, Check, build_checks
from chembl_sim.storage.db import warehouse_connection

log = get_logger(__name__)

RECORD_RESULT = """
    INSERT INTO meta.quality_check
        (run_id, check_name, layer, severity, observed, expected, passed)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, check_name) DO UPDATE SET
        observed = EXCLUDED.observed,
        passed = EXCLUDED.passed,
        checked_at = now()
"""

SUMMARY_LIMIT = 400


class QualityGateError(RuntimeError):
    """A blocking check failed, so whatever the run produced must not be trusted."""


@dataclass(frozen=True)
class Result:
    check: Check
    observed: float

    @property
    def passed(self) -> bool:
        return self.check.holds(self.observed)


@dataclass(frozen=True)
class Report:
    run_id: str
    results: tuple[Result, ...]

    @property
    def failures(self) -> tuple[Result, ...]:
        return tuple(result for result in self.results if not result.passed)

    @property
    def blocking(self) -> tuple[Result, ...]:
        return tuple(r for r in self.failures if r.check.severity == BLOCK)

    @property
    def warnings(self) -> tuple[Result, ...]:
        return tuple(r for r in self.failures if r.check.severity != BLOCK)

    def summary(self) -> dict[str, int]:
        return {
            "checks": len(self.results),
            "passed": len(self.results) - len(self.failures),
            "warnings": len(self.warnings),
            "blocking": len(self.blocking),
        }


def describe(results: Sequence[Result]) -> str:
    """One clause per failure, short enough to survive the truncation in a Teams card."""
    return "; ".join(
        f"{r.check.name} observed {r.observed:g}, expected {r.check.expected}" for r in results
    )


def select_checks(
    layers: Sequence[str] | None = None,
    checks: Sequence[Check] | None = None,
) -> tuple[Check, ...]:
    """Narrow the checks to the layers a DAG is responsible for."""
    chosen = tuple(checks) if checks is not None else build_checks()
    if layers is None:
        return chosen
    wanted = set(layers)
    narrowed = tuple(check for check in chosen if check.layer in wanted)
    if not narrowed:
        raise ValueError(f"no quality checks are defined for layers {sorted(wanted)}")
    return narrowed


def run_checks(
    run_id: str,
    layers: Sequence[str] | None = None,
    checks: Sequence[Check] | None = None,
    dsn: str | None = None,
) -> Report:
    """Evaluate every selected check, record the results then raise if any blocker failed.

    The raise happens after the connection closes so the results are committed first. A
    failing gate that erased its own evidence would be worse than no gate at all.
    """
    selected = select_checks(layers, checks)
    results: list[Result] = []

    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        for check in selected:
            cursor.execute(check.query)
            observed = float(cursor.fetchone()[0])
            result = Result(check=check, observed=observed)
            results.append(result)
            cursor.execute(
                RECORD_RESULT,
                (
                    run_id,
                    check.name,
                    check.layer,
                    check.severity,
                    observed,
                    check.expected,
                    result.passed,
                ),
            )

    report = Report(run_id=run_id, results=tuple(results))
    for result in report.warnings:
        log.warning("Quality warning: %s observed %g", result.check.name, result.observed)
    for result in report.blocking:
        log.error("Quality failure: %s observed %g", result.check.name, result.observed)
    log.info("Quality gate for %s: %s", run_id, report.summary())

    if report.blocking:
        raise QualityGateError(describe(report.blocking)[:SUMMARY_LIMIT])
    return report
