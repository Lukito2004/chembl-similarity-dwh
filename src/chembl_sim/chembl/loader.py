"""Loads ChEMBL API records into bronze, recording resumable progress."""

from __future__ import annotations

from dataclasses import dataclass

from chembl_sim.chembl import records as record_map
from chembl_sim.chembl.client import ChemblClient, ChemblResource
from chembl_sim.logging_setup import get_logger
from chembl_sim.storage.db import upsert_rows, warehouse_connection

log = get_logger(__name__)


@dataclass(frozen=True)
class Watermark:
    """How far a resource got, and which ChEMBL release that progress belongs to."""

    resource: str
    chembl_release: str
    next_offset: int
    total_count: int | None
    is_complete: bool


def read_watermark(cursor, resource: str) -> Watermark | None:
    cursor.execute(
        "SELECT resource, chembl_release, next_offset, total_count, is_complete "
        "FROM meta.ingest_watermark WHERE resource = %s",
        (resource,),
    )
    row = cursor.fetchone()
    return Watermark(*row) if row else None


def write_watermark(cursor, watermark: Watermark) -> None:
    cursor.execute(
        "INSERT INTO meta.ingest_watermark "
        "(resource, chembl_release, next_offset, total_count, is_complete, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (resource) DO UPDATE SET "
        "chembl_release = EXCLUDED.chembl_release, "
        "next_offset = EXCLUDED.next_offset, "
        "total_count = EXCLUDED.total_count, "
        "is_complete = EXCLUDED.is_complete, "
        "updated_at = now()",
        (
            watermark.resource,
            watermark.chembl_release,
            watermark.next_offset,
            watermark.total_count,
            watermark.is_complete,
        ),
    )


def _load_lookup(cursor, records) -> int:
    return upsert_rows(
        cursor,
        "bronze",
        "chembl_id_lookup",
        record_map.CHEMBL_ID_LOOKUP_COLUMNS,
        record_map.chembl_id_lookup_rows(records),
    )


def _load_molecules(cursor, records) -> int:
    rows = record_map.molecule_rows(records)
    loaded = upsert_rows(
        cursor,
        "bronze",
        "molecule_dictionary",
        record_map.MOLECULE_DICTIONARY_COLUMNS,
        rows.dictionary,
    )
    upsert_rows(
        cursor,
        "bronze",
        "compound_properties",
        record_map.COMPOUND_PROPERTIES_COLUMNS,
        rows.properties,
    )
    upsert_rows(
        cursor,
        "bronze",
        "compound_structures",
        record_map.COMPOUND_STRUCTURES_COLUMNS,
        rows.structures,
    )
    return loaded


LOADERS = {
    "chembl_id_lookup": _load_lookup,
    "molecule": _load_molecules,
}


def ingest_resource(
    resource: ChemblResource,
    chembl_release: str,
    client: ChemblClient | None = None,
    dsn: str | None = None,
    force: bool = False,
) -> int:
    """Page a resource into bronze, committing progress after every window."""
    client = client or ChemblClient()
    load = LOADERS[resource.name]
    loaded_total = 0

    with warehouse_connection(dsn) as conn:
        with conn.cursor() as cursor:
            watermark = read_watermark(cursor, resource.name)
            if watermark and watermark.chembl_release != chembl_release:
                log.info("Release is now %s, restarting %s", chembl_release, resource.name)
                watermark = None
            if watermark and watermark.is_complete:
                log.info("%s already complete for %s", resource.name, chembl_release)
                return 0
            start_offset = watermark.next_offset if watermark else 0
            total = client.total_count(resource)
        conn.commit()

        log.info("Ingesting %s from offset %s of %s", resource.name, start_offset, total)
        for batch in client.iter_batches(resource, start_offset=start_offset, total_count=total):
            with conn.cursor() as cursor:
                loaded_total += load(cursor, batch.records)
                write_watermark(
                    cursor,
                    Watermark(
                        resource=resource.name,
                        chembl_release=chembl_release,
                        next_offset=batch.next_offset,
                        total_count=total,
                        is_complete=batch.next_offset >= total,
                    ),
                )
            conn.commit()
            log.info("%s reached offset %s of %s", resource.name, batch.next_offset, total)

    return loaded_total
