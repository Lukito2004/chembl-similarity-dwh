"""Computes Morgan fingerprints for every silver molecule and publishes them to S3."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

from chembl_datasets import FINGERPRINTS, SILVER_MOLECULE
from chembl_sim.chem.fingerprints import fingerprint_rows
from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import get_settings
from chembl_sim.storage.db import warehouse_connection
from chembl_sim.storage.s3 import (
    delete_prefix,
    fingerprint_shard_key,
    fingerprint_table,
    fingerprints_prefix,
    write_parquet,
)

log = get_logger(__name__)

# One pass over the primary key index gives the first identifier of each shard.
SHARD_BOUNDARIES = """
    SELECT chembl_id FROM (
        SELECT chembl_id, row_number() OVER (ORDER BY chembl_id) AS rn
        FROM silver.molecule
    ) numbered
    WHERE (rn - 1) %% %s = 0
    ORDER BY chembl_id
"""

SHARD_RANGE = """
    SELECT chembl_id, canonical_smiles FROM silver.molecule
    WHERE chembl_id >= %s AND chembl_id < %s ORDER BY chembl_id
"""

SHARD_TAIL = """
    SELECT chembl_id, canonical_smiles FROM silver.molecule
    WHERE chembl_id >= %s ORDER BY chembl_id
"""

with DAG(
    dag_id="fingerprint_build",
    description="Morgan fingerprints for every silver molecule, published to S3 as parquet",
    schedule=[SILVER_MOLECULE],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["chembl", "fingerprints"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=2),
    },
) as dag:

    @task
    def plan_shards() -> list[dict]:
        """Split the molecules into contiguous identifier ranges and clear the old set."""
        shard_size = get_settings().fingerprint.shard_size
        with warehouse_connection() as conn, conn.cursor() as cursor:
            cursor.execute(SHARD_BOUNDARIES, (shard_size,))
            boundaries = [row[0] for row in cursor.fetchall()]

        delete_prefix(fingerprints_prefix())

        shards = [
            {
                "index": index,
                "first_chembl_id": first,
                "last_chembl_id": boundaries[index + 1] if index + 1 < len(boundaries) else None,
            }
            for index, first in enumerate(boundaries)
        ]
        log.info("Planned %s shards of up to %s molecules", len(shards), shard_size)
        return shards

    @task
    def build_shard(shard: dict) -> dict:
        """Fingerprint one identifier range and write it as a single parquet object."""
        settings = get_settings().fingerprint
        first, last = shard["first_chembl_id"], shard["last_chembl_id"]

        with warehouse_connection() as conn, conn.cursor() as cursor:
            if last is None:
                cursor.execute(SHARD_TAIL, (first,))
            else:
                cursor.execute(SHARD_RANGE, (first, last))
            rows = cursor.fetchall()

        batch = fingerprint_rows(rows, settings.radius, settings.n_bits)
        key = fingerprint_shard_key(shard["index"])
        size = write_parquet(fingerprint_table(batch.chembl_ids, batch.fingerprints), key)
        log.info(
            "Shard %s: %s fingerprints, %s rejected",
            shard["index"],
            len(batch.chembl_ids),
            batch.rejected,
        )
        return {
            "index": shard["index"],
            "written": len(batch.chembl_ids),
            "rejected": batch.rejected,
            "bytes": size,
        }

    @task(outlets=[FINGERPRINTS])
    def summarise(results: list[dict]) -> dict:
        """Total what was published, so a partial run is obvious in the logs."""
        totals = {
            "shards": len(results),
            "fingerprints": sum(r["written"] for r in results),
            "rejected": sum(r["rejected"] for r in results),
            "megabytes": round(sum(r["bytes"] for r in results) / 1e6, 1),
        }
        log.info("Published %s", totals)
        return totals

    summarise(build_shard.expand(shard=plan_shards()))
