"""Ingests ChEMBL into the bronze layer from the release dump or the REST API."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from chembl_datasets import SILVER_MOLECULE
from chembl_sim.chembl.client import ChemblClient
from chembl_sim.chembl.dump import ingest_from_dump
from chembl_sim.chembl.loader import ingest_resource
from chembl_sim.chembl.records import CHEMBL_ID_LOOKUP, MOLECULE
from chembl_sim.logging_setup import get_logger
from chembl_sim.storage.db import apply_sql_directory, warehouse_connection
from chembl_sim.transform.silver import build_silver_molecule

log = get_logger(__name__)

SQL_DIR = Path("/opt/airflow/sql")

BRONZE_TABLES = (
    "molecule_dictionary",
    "compound_properties",
    "compound_structures",
    "chembl_id_lookup",
)

# Maps the ingest_path parameter onto the task that begins that branch.
INGEST_PATH_TASKS = {
    "dump": "load_from_dump",
    "api": "fetch_api_release",
}

with DAG(
    dag_id="chembl_ingest",
    description="Loads ChEMBL into bronze from the release dump or the REST API",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["chembl", "bronze"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=12),
    },
    params={
        "ingest_path": Param(
            "dump",
            type="string",
            enum=["dump", "api"],
            description="The dump moves six times fewer bytes and survives API outages",
        ),
        "force_reingest": Param(
            False,
            type="boolean",
            description="Reload even when this release is already complete",
        ),
    },
) as dag:

    @task
    def apply_ddl() -> list[str]:
        """Create any missing schemas and tables before anything writes to them."""
        return [path.name for path in apply_sql_directory(SQL_DIR)]

    @task.branch
    def choose_ingest_path() -> str:
        """Decide from the parameter alone, so an unreachable API cannot block the dump."""
        return INGEST_PATH_TASKS[get_current_context()["params"]["ingest_path"]]

    @task
    def load_from_dump() -> dict:
        """Download the release dump and restore its four tables into bronze."""
        force = get_current_context()["params"]["force_reingest"]
        result = ingest_from_dump(force=force)
        return {"release": result.release, "rows": result.rows}

    @task
    def fetch_api_release() -> str:
        """The release the API serves, which decides whether a reload is needed."""
        return ChemblClient().chembl_release()

    @task
    def ingest_molecules(release: str) -> int:
        """Runs first because silver, gold and the fingerprints all depend on it."""
        force = get_current_context()["params"]["force_reingest"]
        return ingest_resource(MOLECULE, release, force=force)

    @task
    def ingest_chembl_id_lookup(release: str) -> int:
        """Required by the specification but read by nothing downstream, so it runs last."""
        force = get_current_context()["params"]["force_reingest"]
        return ingest_resource(CHEMBL_ID_LOOKUP, release, force=force)

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, outlets=[SILVER_MOLECULE])
    def build_silver() -> int:
        """Conform whichever branch just landed into the typed silver table."""
        return build_silver_molecule()

    @task
    def summarise() -> dict[str, int]:
        """Report what the warehouse holds, whichever branch produced it."""
        counts: dict[str, int] = {}
        with warehouse_connection() as conn, conn.cursor() as cursor:
            for table in BRONZE_TABLES:
                cursor.execute(f"SELECT count(*) FROM bronze.{table}")
                counts[f"bronze.{table}"] = cursor.fetchone()[0]
            cursor.execute("SELECT count(*) FROM silver.molecule")
            counts["silver.molecule"] = cursor.fetchone()[0]
            cursor.execute(
                "SELECT resource, chembl_release, is_complete FROM meta.ingest_watermark"
            )
            watermarks = cursor.fetchall()
        for table, count in counts.items():
            log.info("%s holds %s rows", table, count)
        for resource, release, complete in watermarks:
            log.info("watermark %s at %s, complete=%s", resource, release, complete)
        return counts

    ddl = apply_ddl()
    branch = choose_ingest_path()
    dump_load = load_from_dump()
    api_release = fetch_api_release()
    molecules = ingest_molecules(api_release)
    lookup = ingest_chembl_id_lookup(api_release)
    silver = build_silver()
    report = summarise()

    ddl >> branch
    branch >> dump_load >> silver
    branch >> api_release >> molecules >> lookup >> silver
    silver >> report
