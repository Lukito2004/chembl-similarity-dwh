"""Loads ChEMBL into bronze from the official release dump.

The REST API sends roughly six times more bytes for the same data and proved
unreliable during development, so this is the default ingest path.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path

import requests

from chembl_sim.chembl.loader import Watermark, write_watermark
from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import DumpSettings, get_settings
from chembl_sim.storage.db import warehouse_connection

log = get_logger(__name__)

ARCHIVE_PATTERN = re.compile(r"^chembl_(\d+)_postgresql\.tar\.gz$")
WATERMARK_RESOURCES = ("molecule", "chembl_id_lookup")
STAGING_SCHEMA = "chembl_raw"


class DumpError(RuntimeError):
    """The release dump could not be located, verified or restored."""


@dataclass(frozen=True)
class DumpArtifact:
    """The release archive to fetch, with the checksum published beside it."""

    filename: str
    url: str
    sha256: str
    release: str


@dataclass(frozen=True)
class DumpResult:
    """Release loaded and the row count landed in each bronze table."""

    release: str
    rows: dict[str, int]


# The dump keys compounds by molregno, so the identifier map is built from
# molecule_dictionary before that table is dropped.
BUILD_MOLREGNO_MAP = f"""
    DROP TABLE IF EXISTS {STAGING_SCHEMA}.molregno_map;
    CREATE TABLE {STAGING_SCHEMA}.molregno_map AS
        SELECT molregno, chembl_id FROM public.molecule_dictionary;
    ALTER TABLE {STAGING_SCHEMA}.molregno_map ADD PRIMARY KEY (molregno);
"""

# The dump stores flags as 0/1 numbers where the API sends real booleans, and
# helm_notation belongs to the biotherapeutics table rather than this one.
DRAIN_MOLECULE_DICTIONARY = """
    TRUNCATE bronze.molecule_dictionary;
    INSERT INTO bronze.molecule_dictionary (
        chembl_id, pref_name, max_phase, molecule_type, structure_type, first_approval,
        availability_type, usan_year, usan_stem, usan_substem, usan_stem_definition,
        helm_notation, black_box_warning, chemical_probe, chirality, first_in_class,
        inorganic_flag, natural_product, orphan, polymer_flag, prodrug, veterinary,
        dosed_ingredient, oral, parenteral, topical, therapeutic_flag, withdrawn_flag
    )
    SELECT
        d.chembl_id, d.pref_name, d.max_phase::text, d.molecule_type, d.structure_type,
        d.first_approval::int, d.availability_type::int, d.usan_year::int, d.usan_stem,
        d.usan_substem, d.usan_stem_definition,
        NULL::text,
        d.black_box_warning::int, d.chemical_probe::int, d.chirality::int,
        d.first_in_class::int, d.inorganic_flag::int, d.natural_product::int,
        d.orphan::int, d.polymer_flag::int, d.prodrug::int, d.veterinary::int,
        d.dosed_ingredient = 1, d.oral = 1, d.parenteral = 1, d.topical = 1,
        d.therapeutic_flag = 1, d.withdrawn_flag = 1
    FROM public.molecule_dictionary d;
"""

DRAIN_COMPOUND_PROPERTIES = f"""
    TRUNCATE bronze.compound_properties;
    INSERT INTO bronze.compound_properties (
        chembl_id, alogp, psa, mw_freebase, full_mwt, full_molformula, qed_weighted,
        np_likeness_score, ro3_pass, aromatic_rings, heavy_atoms, hba, hbd, rtb,
        num_ro5_violations
    )
    SELECT
        m.chembl_id, p.alogp::text, p.psa::text, p.mw_freebase::text, p.full_mwt::text,
        p.full_molformula, p.qed_weighted::text, p.np_likeness_score::text, p.ro3_pass,
        p.aromatic_rings::int, p.heavy_atoms::int, p.hba::int, p.hbd::int, p.rtb::int,
        p.num_ro5_violations::int
    FROM public.compound_properties p
    JOIN {STAGING_SCHEMA}.molregno_map m ON m.molregno = p.molregno;
"""

DRAIN_COMPOUND_STRUCTURES = f"""
    TRUNCATE bronze.compound_structures;
    INSERT INTO bronze.compound_structures (
        chembl_id, canonical_smiles, standard_inchi, standard_inchi_key, molfile
    )
    SELECT m.chembl_id, s.canonical_smiles, s.standard_inchi, s.standard_inchi_key, s.molfile
    FROM public.compound_structures s
    JOIN {STAGING_SCHEMA}.molregno_map m ON m.molregno = s.molregno;
"""

# resource_url exists only in the API representation, so it stays null here.
DRAIN_CHEMBL_ID_LOOKUP = """
    TRUNCATE bronze.chembl_id_lookup;
    INSERT INTO bronze.chembl_id_lookup (
        chembl_id, entity_type, entity_id, last_active, resource_url, status
    )
    SELECT l.chembl_id, l.entity_type, l.entity_id::bigint, l.last_active::int,
           NULL::text, l.status
    FROM public.chembl_id_lookup l;
"""


@dataclass(frozen=True)
class TableLoad:
    """A dump table, the SQL that drains it into bronze, and anything that runs first."""

    table: str
    drain_sql: str
    prepare_sql: str | None = None


# molecule_dictionary must come first because the other two are keyed by molregno
# and need the identifier map it produces.
TABLE_LOADS = (
    TableLoad("molecule_dictionary", DRAIN_MOLECULE_DICTIONARY, BUILD_MOLREGNO_MAP),
    TableLoad("compound_properties", DRAIN_COMPOUND_PROPERTIES),
    TableLoad("compound_structures", DRAIN_COMPOUND_STRUCTURES),
    TableLoad("chembl_id_lookup", DRAIN_CHEMBL_ID_LOOKUP),
)


def discover_artifact(session: requests.Session, settings: DumpSettings) -> DumpArtifact:
    """Read the release checksum file, which names the archive and its version."""
    response = session.get(f"{settings.base_url}/checksums.txt", timeout=settings.timeout_seconds)
    response.raise_for_status()
    for line in response.text.splitlines():
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        checksum, filename = parts[0].strip(), parts[1].strip()
        match = ARCHIVE_PATTERN.match(filename)
        if match:
            return DumpArtifact(
                filename=filename,
                url=f"{settings.base_url}/{filename}",
                sha256=checksum,
                release=f"ChEMBL_{match.group(1)}",
            )
    raise DumpError(f"no postgresql archive listed at {settings.base_url}/checksums.txt")


def download_archive(
    session: requests.Session,
    artifact: DumpArtifact,
    settings: DumpSettings,
) -> Path:
    """Fetch the archive, resuming a partial file and skipping one already complete."""
    settings.download_dir.mkdir(parents=True, exist_ok=True)
    target = settings.download_dir / artifact.filename

    head = session.head(artifact.url, timeout=settings.timeout_seconds)
    head.raise_for_status()
    total = int(head.headers["Content-Length"])
    have = target.stat().st_size if target.exists() else 0

    if have == total:
        log.info("Archive already complete at %s bytes", have)
        return target
    if have > total:
        log.warning("Local archive exceeds the published size, starting again")
        target.unlink()
        have = 0

    headers = {"Range": f"bytes={have}-"} if have else {}
    with session.get(
        artifact.url, headers=headers, stream=True, timeout=settings.timeout_seconds
    ) as response:
        response.raise_for_status()
        if have and response.status_code != 206:
            raise DumpError("server ignored the range request, refusing to append")
        with target.open("ab" if have else "wb") as handle:
            for chunk in response.iter_content(chunk_size=settings.chunk_size):
                handle.write(chunk)

    log.info("Downloaded %s at %.2f GB", artifact.filename, target.stat().st_size / 1e9)
    return target


def verify_checksum(path: Path, expected: str, chunk_size: int) -> None:
    """Catch a truncated or corrupt download before spending an hour on the restore."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise DumpError(f"checksum mismatch for {path.name}: {actual} != {expected}")


def extract_dump(archive: Path, destination: Path) -> Path:
    """Pull the single pg_dump member out of the archive."""
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar:
            if not member.name.endswith(".dmp"):
                continue
            source = tar.extractfile(member)
            if source is None:
                raise DumpError(f"{member.name} is not a regular file")
            target = destination / Path(member.name).name
            if target.exists() and target.stat().st_size == member.size:
                log.info("%s is already extracted", target.name)
                return target
            source = tar.extractfile(member)
            if source is None:
                raise DumpError(f"{member.name} is not a regular file")
            with source, target.open("wb") as handle:
                shutil.copyfileobj(source, handle)
            log.info("Extracted %s at %.2f GB", target.name, target.stat().st_size / 1e9)
            return target
    raise DumpError(f"no .dmp member inside {archive.name}")


def restore_table(dump_file: Path, table: str, dsn: str) -> None:
    """Restore one table's definition and rows, skipping indexes and constraints."""
    command = [
        "pg_restore",
        "--dbname",
        dsn,
        "--schema",
        "public",
        "--table",
        table,
        "--section",
        "pre-data",
        "--section",
        "data",
        "--no-owner",
        "--no-privileges",
        "--exit-on-error",
        str(dump_file),
    ]
    log.info("Restoring %s", table)
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise DumpError(f"pg_restore failed for {table}: {result.stderr.strip()[:500]}")


def already_loaded(cursor, release: str) -> bool:
    """True when every resource watermark reports this release as complete."""
    cursor.execute(
        "SELECT count(*) FROM meta.ingest_watermark WHERE chembl_release = %s AND is_complete",
        (release,),
    )
    return cursor.fetchone()[0] >= len(WATERMARK_RESOURCES)


def ingest_from_dump(
    dsn: str | None = None,
    settings: DumpSettings | None = None,
    session: requests.Session | None = None,
    force: bool = False,
) -> DumpResult:
    """Download the release dump and load its four tables into bronze.

    Each table is restored, drained and dropped in turn so that at most one copy of
    the largest table exists at a time.
    """
    settings = settings or get_settings().dump
    session = session or requests.Session()
    resolved_dsn = dsn or get_settings().dwh.dsn

    artifact = discover_artifact(session, settings)
    log.info("Release %s from %s", artifact.release, artifact.url)
    if not force:
        with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
            if already_loaded(cursor, artifact.release):
                log.info("%s is already loaded, skipping the download", artifact.release)
                return DumpResult(release=artifact.release, rows={})

    archive = download_archive(session, artifact, settings)
    verify_checksum(archive, artifact.sha256, settings.chunk_size)
    dump_file = extract_dump(archive, settings.download_dir)

    counts: dict[str, int] = {}
    with warehouse_connection(dsn) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
        conn.commit()

        for load in TABLE_LOADS:
            # Committed before pg_restore so its own connection meets no locks.
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS public.{load.table}")
            conn.commit()

            restore_table(dump_file, load.table, resolved_dsn)

            with conn.cursor() as cursor:
                if load.prepare_sql:
                    cursor.execute(load.prepare_sql)
                cursor.execute(load.drain_sql)
                cursor.execute(f"SELECT count(*) FROM bronze.{load.table}")
                counts[load.table] = cursor.fetchone()[0]
                cursor.execute(f"DROP TABLE public.{load.table}")
            conn.commit()
            log.info("Loaded %s rows into bronze.%s", counts[load.table], load.table)

        with conn.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {STAGING_SCHEMA} CASCADE")
            for resource in WATERMARK_RESOURCES:
                write_watermark(
                    cursor,
                    Watermark(
                        resource=resource,
                        chembl_release=artifact.release,
                        next_offset=0,
                        total_count=None,
                        is_complete=True,
                    ),
                )
        conn.commit()

    dump_file.unlink()
    log.info("Kept %s for cheap re-runs, delete it to reclaim the space", archive)
    return DumpResult(release=artifact.release, rows=counts)
