"""Selects the source molecules the similarity search runs over."""

from __future__ import annotations

from dataclasses import dataclass

from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import get_settings
from chembl_sim.storage.db import warehouse_connection

log = get_logger(__name__)

# DISTINCT ON guards against two input rows resolving to the same molecule.
RESOLVE_INPUT_COMPOUNDS = """
    TRUNCATE silver.source_molecule;
    TRUNCATE silver.source_molecule_rejected;

    INSERT INTO silver.source_molecule (
        chembl_id, canonical_smiles, origin, input_compound_id, input_compound_name
    )
    SELECT DISTINCT ON (m.chembl_id)
        m.chembl_id, m.canonical_smiles, 'input_file', i.compound_id, i.compound_name
    FROM bronze.input_compound i
    JOIN silver.molecule m ON upper(m.pref_name) = upper(btrim(i.compound_name))
    WHERE btrim(coalesce(i.compound_name, '')) <> ''
    ORDER BY m.chembl_id, i.source_file, i.source_row;

    INSERT INTO silver.source_molecule_rejected (
        source_file, source_row, compound_id, compound_name, reason
    )
    SELECT i.source_file, i.source_row, i.compound_id, i.compound_name,
           CASE
               WHEN btrim(coalesce(i.compound_name, '')) = '' THEN 'compound name is blank'
               ELSE 'name matches no ChEMBL preferred name'
           END
    FROM bronze.input_compound i
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.molecule m
        WHERE upper(m.pref_name) = upper(btrim(i.compound_name))
    );
"""

# md5 ordering is deterministic and spreads the choice across the whole library,
# unlike ordering by identifier which would cluster on the earliest ChEMBL entries.
TOP_UP_SOURCE_MOLECULES = """
    INSERT INTO silver.source_molecule (chembl_id, canonical_smiles, origin)
    SELECT m.chembl_id, m.canonical_smiles, 'top_up'
    FROM silver.molecule m
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.source_molecule s WHERE s.chembl_id = m.chembl_id
    )
    ORDER BY md5(m.chembl_id)
    LIMIT %s;
"""


@dataclass(frozen=True)
class SourceSelection:
    """How the source set was made up."""

    from_input: int
    topped_up: int
    rejected: int

    @property
    def total(self) -> int:
        return self.from_input + self.topped_up


def build_source_molecules(
    dsn: str | None = None, target_size: int | None = None
) -> SourceSelection:
    """Resolve the input compounds, then top the set up to the target size."""
    target_size = target_size if target_size is not None else get_settings().source.target_size

    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(RESOLVE_INPUT_COMPOUNDS)

        cursor.execute("SELECT count(*) FROM silver.source_molecule")
        from_input = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM silver.source_molecule_rejected")
        rejected = cursor.fetchone()[0]

        deficit = max(target_size - from_input, 0)
        if deficit:
            cursor.execute(TOP_UP_SOURCE_MOLECULES, (deficit,))
        cursor.execute("SELECT count(*) FROM silver.source_molecule")
        total = cursor.fetchone()[0]

    selection = SourceSelection(
        from_input=from_input, topped_up=total - from_input, rejected=rejected
    )
    log.info(
        "Source set of %s: %s from input files, %s topped up, %s rejected",
        selection.total,
        selection.from_input,
        selection.topped_up,
        selection.rejected,
    )
    return selection
