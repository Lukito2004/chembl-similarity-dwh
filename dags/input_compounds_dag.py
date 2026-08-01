"""Loads the personal input files and selects the source molecule set."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

from chembl_sim.inputs.batch_csv import BRONZE_COLUMNS, parse_batch_csv
from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import get_settings
from chembl_sim.storage.db import apply_sql_directory, insert_rows, warehouse_connection
from chembl_sim.storage.s3 import list_keys, read_text
from chembl_sim.transform.source_molecules import build_source_molecules

log = get_logger(__name__)

SQL_DIR = Path("/opt/airflow/sql")

with DAG(
    dag_id="input_compounds",
    description="Lands the personal input files and selects the source molecule set",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["chembl", "source"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
) as dag:

    @task
    def apply_ddl() -> list[str]:
        """Keeps this DAG runnable on its own, not only after the ingest."""
        return [path.name for path in apply_sql_directory(SQL_DIR)]

    @task
    def load_input_files() -> int:
        """Land every batch CSV verbatim, whatever columns it happens to carry."""
        settings = get_settings().source
        keys = [k for k in list_keys(settings.input_prefix) if k.lower().endswith(".csv")]
        log.info("Found %s input files under %s", len(keys), settings.input_prefix)

        loaded = 0
        with warehouse_connection() as conn, conn.cursor() as cursor:
            cursor.execute("TRUNCATE bronze.input_compound")
            for key in keys:
                parsed = parse_batch_csv(read_text(key), Path(key).name)
                loaded += insert_rows(
                    cursor,
                    "bronze",
                    "input_compound",
                    BRONZE_COLUMNS,
                    parsed.rows,
                )
        return loaded

    @task
    def select_source_molecules() -> dict:
        """Resolve input names to ChEMBL, then top up to the configured size."""
        selection = build_source_molecules()
        return {
            "total": selection.total,
            "from_input": selection.from_input,
            "topped_up": selection.topped_up,
            "rejected": selection.rejected,
        }

    apply_ddl() >> load_input_files() >> select_source_molecules()
