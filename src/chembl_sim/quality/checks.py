"""The invariants the warehouse must hold, one SQL query each.

Constraints the database already enforces are deliberately absent. The score range, the
primary keys and the fact's foreign keys cannot be violated, so asserting them proves nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from chembl_sim.settings import Settings, get_settings

BLOCK = "block"
WARN = "warn"
SEVERITIES = (BLOCK, WARN)


def is_zero(observed: float) -> bool:
    """The usual shape. The query counts offending rows, so anything above zero fails."""
    return observed == 0


def equals(target: float) -> Callable[[float], bool]:
    return lambda observed: observed == target


def at_least(threshold: float) -> Callable[[float], bool]:
    return lambda observed: observed >= threshold


@dataclass(frozen=True)
class Check:
    """One invariant. The query must return a single row holding a single number."""

    name: str
    layer: str
    severity: str
    expected: str
    query: str
    holds: Callable[[float], bool] = is_zero

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"{self.name} has an unknown severity: {self.severity}")


def build_checks(settings: Settings | None = None) -> tuple[Check, ...]:
    """Assemble the checks. Two of them read sizes from configuration rather than hard coding.

    The interpolated values come from the integer readers in settings, so they cannot
    carry anything but a number into the statement.
    """
    settings = settings or get_settings()
    top_n = settings.similarity.top_n
    target_size = settings.source.target_size

    return (
        Check(
            name="silver_molecule_is_populated",
            layer="silver",
            severity=BLOCK,
            expected="at least one row",
            holds=at_least(1),
            query="SELECT count(*) FROM silver.molecule",
        ),
        Check(
            name="silver_conversion_keeps_every_structure",
            layer="silver",
            severity=BLOCK,
            expected="0 structures left behind",
            query="""
                SELECT count(*)
                FROM bronze.compound_structures s
                JOIN bronze.molecule_dictionary d ON d.chembl_id = s.chembl_id
                LEFT JOIN silver.molecule m ON m.chembl_id = s.chembl_id
                WHERE s.canonical_smiles IS NOT NULL AND m.chembl_id IS NULL
            """,
        ),
        Check(
            name="silver_smiles_is_never_blank",
            layer="silver",
            severity=BLOCK,
            expected="0 blank strings",
            query="SELECT count(*) FROM silver.molecule WHERE btrim(canonical_smiles) = ''",
        ),
        Check(
            # safe_numeric turns anything unparseable into null, which is silent by design.
            # This is what makes that silence visible.
            name="silver_numeric_cast_drops_nothing",
            layer="silver",
            severity=BLOCK,
            expected="0 values lost to the cast",
            query="""
                SELECT count(*)
                FROM bronze.compound_properties p
                JOIN silver.molecule m ON m.chembl_id = p.chembl_id
                WHERE p.alogp IS NOT NULL AND btrim(p.alogp) <> '' AND m.alogp IS NULL
            """,
        ),
        Check(
            # Counting rows would not survive two input names resolving to one molecule,
            # which DISTINCT ON collapses. This asks after each row individually instead.
            name="every_input_row_is_accounted_for",
            layer="source",
            severity=BLOCK,
            expected="0 rows unaccounted for",
            query="""
                SELECT count(*)
                FROM bronze.input_compound i
                WHERE NOT EXISTS (
                    SELECT 1 FROM silver.source_molecule_rejected r
                    WHERE r.source_file = i.source_file AND r.source_row = i.source_row
                )
                AND NOT EXISTS (
                    SELECT 1 FROM silver.source_molecule s
                    JOIN silver.molecule m ON m.chembl_id = s.chembl_id
                    WHERE s.origin = 'input_file'
                      AND upper(m.pref_name) = upper(btrim(i.compound_name))
                )
            """,
        ),
        Check(
            name="source_set_reaches_the_configured_size",
            layer="source",
            severity=WARN,
            expected=f"exactly {target_size} molecules",
            holds=equals(target_size),
            query="SELECT count(*) FROM silver.source_molecule",
        ),
        Check(
            name="fact_excludes_self_matches",
            layer="gold",
            severity=BLOCK,
            expected="0 self matches",
            query="""
                SELECT count(*) FROM gold.fact_molecule_similarity
                WHERE source_chembl_id = target_chembl_id
            """,
        ),
        Check(
            name="fact_holds_the_top_n_for_every_source",
            layer="gold",
            severity=BLOCK,
            expected=f"0 sources away from {top_n} matches",
            query=f"""
                SELECT count(*) FROM (
                    SELECT source_chembl_id
                    FROM gold.fact_molecule_similarity
                    GROUP BY source_chembl_id
                    HAVING count(*) <> {top_n}
                ) AS wrong_sized
            """,
        ),
        Check(
            name="fact_sources_come_from_the_source_set",
            layer="gold",
            severity=BLOCK,
            expected="0 sources missing from silver",
            query="""
                SELECT count(*) FROM (
                    SELECT DISTINCT source_chembl_id FROM gold.fact_molecule_similarity
                ) AS scored
                WHERE NOT EXISTS (
                    SELECT 1 FROM silver.source_molecule s
                    WHERE s.chembl_id = scored.source_chembl_id
                )
            """,
        ),
        Check(
            # The foreign keys stop the fact pointing at a missing molecule. Nothing stops
            # the dimension carrying molecules the fact never mentions.
            name="dim_holds_no_unreferenced_molecules",
            layer="gold",
            severity=BLOCK,
            expected="0 unreferenced rows",
            query="""
                SELECT count(*) FROM gold.dim_molecule d
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.fact_molecule_similarity f
                    WHERE f.source_chembl_id = d.chembl_id OR f.target_chembl_id = d.chembl_id
                )
            """,
        ),
        Check(
            name="tie_flag_sits_only_on_the_boundary_score",
            layer="gold",
            severity=BLOCK,
            expected="0 flags above the boundary",
            query="""
                SELECT count(*) FROM (
                    SELECT
                        tanimoto_score,
                        has_duplicates_of_last_largest_score AS flagged,
                        min(tanimoto_score) OVER (PARTITION BY source_chembl_id) AS boundary
                    FROM gold.fact_molecule_similarity
                ) AS ranked
                WHERE flagged AND tanimoto_score <> boundary
            """,
        ),
        Check(
            name="tie_flag_agrees_across_the_boundary_score",
            layer="gold",
            severity=BLOCK,
            expected="0 sources flagged inconsistently",
            query="""
                SELECT count(*) FROM (
                    SELECT source_chembl_id
                    FROM gold.fact_molecule_similarity f
                    WHERE tanimoto_score = (
                        SELECT min(tanimoto_score) FROM gold.fact_molecule_similarity g
                        WHERE g.source_chembl_id = f.source_chembl_id
                    )
                    GROUP BY source_chembl_id
                    HAVING count(DISTINCT has_duplicates_of_last_largest_score) > 1
                ) AS inconsistent
            """,
        ),
        Check(
            name="dim_alogp_coverage_stays_high",
            layer="gold",
            severity=WARN,
            expected="at least 0.90 of rows populated",
            holds=at_least(0.90),
            query="""
                SELECT coalesce(avg((alogp IS NOT NULL)::int), 0)
                FROM gold.dim_molecule
            """,
        ),
        Check(
            # Inverted on purpose. This fires when the data improves, telling us a later
            # release started serving the two columns the README documents as empty.
            name="release_gap_columns_are_still_empty",
            layer="gold",
            severity=WARN,
            expected="0 populated rows while the release omits them",
            query="""
                SELECT count(*) FROM gold.dim_molecule
                WHERE cx_logp IS NOT NULL OR molecular_species IS NOT NULL
            """,
        ),
        Check(
            name="source_set_includes_the_input_molecules",
            layer="source",
            severity=BLOCK,
            expected="at least one molecule from the input files",
            holds=at_least(1),
            query="SELECT count(*) FROM silver.source_molecule WHERE origin = 'input_file'",
        ),
        Check(
            # A skipped source leaves no group for the top-N check to count, so absence
            # needs its own check rather than another count of violations.
            name="every_source_molecule_has_matches",
            layer="gold",
            severity=BLOCK,
            expected="0 sources with no matches",
            query="""
                SELECT count(*) FROM silver.source_molecule s
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.fact_molecule_similarity f
                    WHERE f.source_chembl_id = s.chembl_id
                )
            """,
        ),
    )
