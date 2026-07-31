"""Maps ChEMBL API records onto the bronze table row shapes."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from chembl_sim.chembl.client import ChemblResource

CHEMBL_ID_LOOKUP_COLUMNS = (
    "chembl_id",
    "entity_type",
    "entity_id",
    "last_active",
    "resource_url",
    "status",
)

MOLECULE_DICTIONARY_COLUMNS = (
    "chembl_id",
    "pref_name",
    "max_phase",
    "molecule_type",
    "structure_type",
    "first_approval",
    "availability_type",
    "usan_year",
    "usan_stem",
    "usan_substem",
    "usan_stem_definition",
    "helm_notation",
    "black_box_warning",
    "chemical_probe",
    "chirality",
    "first_in_class",
    "inorganic_flag",
    "natural_product",
    "orphan",
    "polymer_flag",
    "prodrug",
    "veterinary",
    "dosed_ingredient",
    "oral",
    "parenteral",
    "topical",
    "therapeutic_flag",
    "withdrawn_flag",
)

COMPOUND_PROPERTIES_COLUMNS = (
    "chembl_id",
    "alogp",
    "psa",
    "mw_freebase",
    "full_mwt",
    "full_molformula",
    "qed_weighted",
    "np_likeness_score",
    "ro3_pass",
    "aromatic_rings",
    "heavy_atoms",
    "hba",
    "hbd",
    "rtb",
    "num_ro5_violations",
)

COMPOUND_STRUCTURES_COLUMNS = (
    "chembl_id",
    "canonical_smiles",
    "standard_inchi",
    "standard_inchi_key",
    "molfile",
)

# Every dictionary column past the first matches its API field name exactly; only the
# identifier is renamed, from molecule_chembl_id to chembl_id.
MOLECULE_API_FIELDS = (
    "molecule_chembl_id",
    *MOLECULE_DICTIONARY_COLUMNS[1:],
    "molecule_properties",
    "molecule_structures",
)

MOLECULE = ChemblResource(
    name="molecule",
    collection="molecules",
    fields=MOLECULE_API_FIELDS,
)

CHEMBL_ID_LOOKUP = ChemblResource(
    name="chembl_id_lookup",
    collection="chembl_id_lookups",
)

RESOURCES = (MOLECULE, CHEMBL_ID_LOOKUP)


@dataclass(frozen=True)
class MoleculeRows:
    """Rows destined for the three bronze tables the molecule endpoint feeds."""

    dictionary: list[tuple]
    properties: list[tuple]
    structures: list[tuple]


def chembl_id_lookup_rows(records: Iterable[dict]) -> list[tuple]:
    """Rows for bronze.chembl_id_lookup, skipping any record without an identifier."""
    rows = []
    for record in records:
        if not record.get("chembl_id"):
            continue
        rows.append(tuple(record.get(column) for column in CHEMBL_ID_LOOKUP_COLUMNS))
    return rows


def molecule_rows(records: Iterable[dict]) -> MoleculeRows:
    """Split molecule records across the three bronze tables they populate.

    Roughly one percent of molecules carry no properties or structures, so both
    sub-documents are optional.
    """
    dictionary: list[tuple] = []
    properties: list[tuple] = []
    structures: list[tuple] = []

    for record in records:
        chembl_id = record.get("molecule_chembl_id")
        if not chembl_id:
            continue
        dictionary.append(
            (chembl_id, *(record.get(column) for column in MOLECULE_DICTIONARY_COLUMNS[1:]))
        )
        molecule_properties = record.get("molecule_properties")
        if molecule_properties:
            properties.append(
                (
                    chembl_id,
                    *(molecule_properties.get(c) for c in COMPOUND_PROPERTIES_COLUMNS[1:]),
                )
            )
        molecule_structures = record.get("molecule_structures")
        if molecule_structures:
            structures.append(
                (
                    chembl_id,
                    *(molecule_structures.get(c) for c in COMPOUND_STRUCTURES_COLUMNS[1:]),
                )
            )

    return MoleculeRows(dictionary=dictionary, properties=properties, structures=structures)
