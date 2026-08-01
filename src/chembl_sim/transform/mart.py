"""Loads the gold star schema from the staged silver tables."""

from __future__ import annotations

from dataclasses import dataclass

from chembl_sim.logging_setup import get_logger
from chembl_sim.storage.db import warehouse_connection

log = get_logger(__name__)

# Both tables are emptied in one statement because the fact's foreign keys forbid
# truncating the dimension on its own. The dimension is then filled first.
LOAD_MART = """
    TRUNCATE gold.fact_molecule_similarity, gold.dim_molecule;

    INSERT INTO gold.dim_molecule (
        chembl_id, molecule_type, mw_freebase, alogp, psa, cx_logp,
        molecular_species, full_mwt, aromatic_rings, heavy_atoms
    )
    SELECT m.chembl_id, m.molecule_type, m.mw_freebase, m.alogp, m.psa, m.cx_logp,
           m.molecular_species, m.full_mwt, m.aromatic_rings, m.heavy_atoms
    FROM silver.molecule m
    WHERE m.chembl_id IN (
        SELECT source_chembl_id FROM silver.similarity_top
        UNION
        SELECT target_chembl_id FROM silver.similarity_top
    );

    INSERT INTO gold.fact_molecule_similarity (
        source_chembl_id, target_chembl_id, tanimoto_score,
        has_duplicates_of_last_largest_score
    )
    SELECT source_chembl_id, target_chembl_id, tanimoto_score,
           has_duplicates_of_last_largest_score
    FROM silver.similarity_top;

    ANALYZE gold.dim_molecule;
    ANALYZE gold.fact_molecule_similarity;
"""


@dataclass(frozen=True)
class MartLoad:
    """Row counts after a mart rebuild."""

    dimension_rows: int
    fact_rows: int


def load_mart(dsn: str | None = None) -> MartLoad:
    """Rebuild the dimension and fact tables from the staged top matches."""
    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(LOAD_MART)
        cursor.execute("SELECT count(*) FROM gold.dim_molecule")
        dimension_rows = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM gold.fact_molecule_similarity")
        fact_rows = cursor.fetchone()[0]

    load = MartLoad(dimension_rows=dimension_rows, fact_rows=fact_rows)
    log.info("Mart loaded: %s dimension rows, %s fact rows", load.dimension_rows, load.fact_rows)
    return load
