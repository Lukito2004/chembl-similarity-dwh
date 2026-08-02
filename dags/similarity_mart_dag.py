"""Scores the source molecules against ChEMBL and rebuilds the data mart."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.dag import DAG
from airflow.operators.python import get_current_context

from chembl_datasets import FINGERPRINTS, SOURCE_MOLECULE
from chembl_sim.alerting import notify_failure, notify_success
from chembl_sim.chem.search import run_similarity_search
from chembl_sim.logging_setup import get_logger
from chembl_sim.quality.runner import QualityGateError, run_checks
from chembl_sim.storage.db import apply_sql_directory, warehouse_connection
from chembl_sim.transform.mart import load_mart
from chembl_sim.transform.views import create_pivot_view

log = get_logger(__name__)

SQL_DIR = Path("/opt/airflow/sql")

with DAG(
    dag_id="similarity_mart",
    description="Tanimoto search over the source molecules, then the gold data mart",
    schedule=[FINGERPRINTS, SOURCE_MOLECULE],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["chembl", "gold"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_failure,
    },
) as dag:

    @task
    def apply_ddl() -> list[str]:
        return [path.name for path in apply_sql_directory(SQL_DIR)]

    @task
    def compute_similarity() -> dict:
        """Score every source, publish its full table to S3, stage the top matches."""
        result = run_similarity_search()
        return {
            "sources": result.sources,
            "missing": result.missing,
            "top_rows": result.top_rows,
            "flagged_rows": result.flagged_rows,
        }

    @task
    def build_mart() -> dict:
        """Rebuild the dimension and fact tables from the staged matches."""
        load = load_mart()
        return {"dimension_rows": load.dimension_rows, "fact_rows": load.fact_rows}

    @task
    def run_quality_checks() -> dict[str, int]:
        """Runs between the mart and the pivot, so a bad mart never reaches the views."""
        try:
            return run_checks(get_current_context()["run_id"], layers=("gold",)).summary()
        except QualityGateError as exc:
            # A failed check is deterministic, so retrying it only delays the alert.
            raise AirflowFailException(str(exc)) from exc

    @task
    def build_pivot_view() -> list[str]:
        """The pivot's columns are the chosen source molecules, so it is generated."""
        return create_pivot_view()

    @task(on_success_callback=notify_success)
    def summarise(search: dict, mart: dict) -> dict:
        totals = {**search, **mart}
        log.info("Similarity mart rebuilt: %s", totals)
        with warehouse_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT count(DISTINCT source_chembl_id) FROM gold.fact_molecule_similarity"
            )
            totals["distinct_sources_in_fact"] = cursor.fetchone()[0]
        return totals

    ddl = apply_ddl()
    search = compute_similarity()
    mart = build_mart()
    quality = run_quality_checks()
    pivot = build_pivot_view()
    report = summarise(search, mart)

    ddl >> search >> mart >> quality >> pivot >> report
