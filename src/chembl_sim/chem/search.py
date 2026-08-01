"""Runs the similarity search and stages the top matches for the mart."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from chembl_sim.chem.tanimoto import FingerprintLibrary, tanimoto_scores, top_matches
from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import Settings, get_settings
from chembl_sim.storage.db import insert_rows, warehouse_connection
from chembl_sim.storage.s3 import (
    delete_prefix,
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


@dataclass(frozen=True)
class SearchResult:
    """What one full search produced."""

    sources: int
    missing: int
    top_rows: int
    flagged_rows: int


def load_source_molecules(dsn: str | None = None) -> list[str]:
    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT chembl_id FROM silver.source_molecule ORDER BY chembl_id")
        return [row[0] for row in cursor.fetchall()]


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

    table = pa.concat_tables(
        [read_parquet(key) for key in list_keys(fingerprints_prefix(settings.s3))]
    ).combine_chunks()
    library = FingerprintLibrary.from_table(
        table, n_bytes=settings.fingerprint.n_bytes, block_size=settings.similarity.block_size
    )
    target_ids = table.column("chembl_id").chunk(0)
    log.info("Scoring %s sources against %s molecules", len(sources), len(library))

    delete_prefix(similarity_prefix(settings.s3), settings.s3)

    staged: list[tuple] = []
    missing = 0
    for chembl_id in sources:
        index = library.index_of(chembl_id)
        if index is None:
            log.warning("%s has no fingerprint, skipping", chembl_id)
            missing += 1
            continue

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

    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("TRUNCATE silver.similarity_top")
        insert_rows(cursor, "silver", "similarity_top", SIMILARITY_TOP_COLUMNS, staged)

    result = SearchResult(
        sources=len(sources) - missing,
        missing=missing,
        top_rows=len(staged),
        flagged_rows=sum(1 for row in staged if row[3]),
    )
    log.info("Search complete: %s", result)
    return result
