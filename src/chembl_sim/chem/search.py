"""Runs the similarity search and stages the top matches for the mart."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from chembl_sim.chem.tanimoto import FingerprintLibrary, tanimoto_scores, top_matches
from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import Settings, get_settings
from chembl_sim.storage.db import insert_rows, warehouse_connection
from chembl_sim.storage.s3 import (
    delete_keys,
    fingerprints_prefix,
    list_keys,
    read_parquet,
    similarity_key,
    similarity_prefix,
    similarity_table,
    write_parquet,
)

log = get_logger(__name__)

SIMILARITY_TOP_COLUMNS = (
    "source_chembl_id",
    "target_chembl_id",
    "tanimoto_score",
    "has_duplicates_of_last_largest_score",
    "match_rank",
)


class IncompleteLibraryError(RuntimeError):
    """S3 holds fewer fingerprint shards than a complete library needs."""


class MissingFingerprintError(RuntimeError):
    """A source molecule cannot be scored, because nothing fingerprinted it."""


@dataclass(frozen=True)
class SearchResult:
    """What one full search produced."""

    sources: int
    top_rows: int
    flagged_rows: int


def load_source_molecules(dsn: str | None = None) -> list[str]:
    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT chembl_id FROM silver.source_molecule ORDER BY chembl_id")
        return [row[0] for row in cursor.fetchall()]


def expected_shard_count(settings: Settings, dsn: str | None = None) -> int:
    """Shards a complete library holds, from the same split the fingerprint build uses."""
    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM silver.molecule")
        molecules = cursor.fetchone()[0]
    shard_size = settings.fingerprint.shard_size
    return (molecules + shard_size - 1) // shard_size


def run_similarity_search(
    settings: Settings | None = None,
    dsn: str | None = None,
) -> SearchResult:
    """Score every source against the library, publish each full table, stage the top matches.

    The library costs far more to load than to query, so every source is scored in one
    pass rather than spread across tasks.
    """
    settings = settings or get_settings()
    sources = load_source_molecules(dsn)
    keys = list_keys(fingerprints_prefix(settings.s3))

    expected = expected_shard_count(settings, dsn)
    if len(keys) != expected:
        raise IncompleteLibraryError(
            f"the fingerprint library holds {len(keys)} shards, expected {expected}"
        )

    table = pa.concat_tables([read_parquet(key) for key in keys]).combine_chunks()
    library = FingerprintLibrary.from_table(
        table, n_bytes=settings.fingerprint.n_bytes, block_size=settings.similarity.block_size
    )
    target_ids = table.column("chembl_id").chunk(0)

    # Both checks run before anything is deleted, so a run that cannot finish leaves the
    # last good results on S3 instead of replacing them with a shorter set.
    indexes = {chembl_id: library.index_of(chembl_id) for chembl_id in sources}
    unscoreable = sorted(name for name, index in indexes.items() if index is None)
    if unscoreable:
        raise MissingFingerprintError(
            f"{len(unscoreable)} source molecules have no fingerprint: {', '.join(unscoreable[:5])}"
        )

    log.info("Scoring %s sources against %s molecules", len(sources), len(library))

    staged: list[tuple] = []
    for chembl_id in sources:
        index = indexes[chembl_id]
        scores = tanimoto_scores(
            library,
            library.fingerprints[index],
            int(library.popcounts[index]),
            settings.similarity.block_size,
        )
        write_parquet(
            similarity_table(chembl_id, target_ids, scores),
            similarity_key(chembl_id, settings.s3),
            settings.s3,
        )
        for rank, match in enumerate(
            top_matches(library, scores, settings.similarity.top_n, exclude_index=index), start=1
        ):
            staged.append(
                (
                    chembl_id,
                    match.target_chembl_id,
                    match.tanimoto_score,
                    match.has_duplicates_of_last_largest_score,
                    rank,
                )
            )

    # Pruned only once every source has been written, so a run that cannot finish leaves
    # the previous score tables in place instead of removing them up front.
    written = {similarity_key(chembl_id, settings.s3) for chembl_id in sources}
    stale = [
        key for key in list_keys(similarity_prefix(settings.s3), settings.s3) if key not in written
    ]
    if stale:
        delete_keys(stale, settings.s3)

    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("TRUNCATE silver.similarity_top")
        insert_rows(cursor, "silver", "similarity_top", SIMILARITY_TOP_COLUMNS, staged)

    result = SearchResult(
        sources=len(sources),
        top_rows=len(staged),
        flagged_rows=sum(1 for row in staged if row[3]),
    )
    log.info("Search complete: %s", result)
    return result
