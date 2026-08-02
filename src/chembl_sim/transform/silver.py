"""Builds the silver molecule table from the bronze landing tables."""

from __future__ import annotations

from chembl_sim.logging_setup import get_logger
from chembl_sim.storage.db import warehouse_connection

log = get_logger(__name__)

# Driven from compound_structures because a molecule without a structure cannot be
# fingerprinted or scored. Properties are optional for roughly one percent of rows.
BUILD_SILVER_MOLECULE = """
    SET LOCAL work_mem = '256MB';

    TRUNCATE silver.molecule;

    INSERT INTO silver.molecule (
        chembl_id, molecule_type, pref_name, max_phase, structure_type,
        canonical_smiles, standard_inchi_key,
        mw_freebase, full_mwt, alogp, psa, qed_weighted, np_likeness_score,
        aromatic_rings, heavy_atoms, hba, hbd, rtb, num_ro5_violations,
        ro3_pass, cx_logp, molecular_species
    )
    SELECT
        d.chembl_id,
        d.molecule_type,
        d.pref_name,
        silver.safe_numeric(d.max_phase),
        d.structure_type,
        s.canonical_smiles,
        s.standard_inchi_key,
        silver.safe_numeric(p.mw_freebase),
        silver.safe_numeric(p.full_mwt),
        silver.safe_numeric(p.alogp),
        silver.safe_numeric(p.psa),
        silver.safe_numeric(p.qed_weighted),
        silver.safe_numeric(p.np_likeness_score),
        p.aromatic_rings,
        p.heavy_atoms,
        p.hba,
        p.hbd,
        p.rtb,
        p.num_ro5_violations,
        CASE upper(p.ro3_pass) WHEN 'Y' THEN true WHEN 'N' THEN false END,
        NULL::numeric,
        NULL::text
    FROM bronze.compound_structures s
    JOIN bronze.molecule_dictionary d ON d.chembl_id = s.chembl_id
    LEFT JOIN bronze.compound_properties p ON p.chembl_id = s.chembl_id
    WHERE s.canonical_smiles IS NOT NULL;

    ANALYZE silver.molecule;
"""


def build_silver_molecule(dsn: str | None = None) -> int:
    """Rebuild silver.molecule from bronze and return the row count."""
    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(BUILD_SILVER_MOLECULE)
        cursor.execute("SELECT count(*) FROM silver.molecule")
        count = cursor.fetchone()[0]
    log.info("silver.molecule rebuilt with %s rows", count)
    return count
