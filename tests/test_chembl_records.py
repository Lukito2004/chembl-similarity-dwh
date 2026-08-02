from chembl_sim.chembl import records

API_MOLECULE = {
    "molecule_chembl_id": "CHEMBL25",
    "pref_name": "ASPIRIN",
    "max_phase": "4.0",
    "molecule_type": "Small molecule",
    "structure_type": "MOL",
    "veterinary": 0,
    "oral": True,
    "molecule_properties": {"alogp": "1.31", "aromatic_rings": 1, "heavy_atoms": 13},
    "molecule_structures": {
        "canonical_smiles": "CC(=O)Oc1ccccc1C(=O)O",
        "standard_inchi_key": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
        "molfile": "line one\nline two",
    },
}


def test_lookup_rows_follow_the_column_order():
    record = {
        "chembl_id": "CHEMBL1",
        "entity_type": "COMPOUND",
        "last_active": 28,
        "resource_url": "/chembl/api/data/molecule/CHEMBL1",
        "status": "ACTIVE",
    }
    assert records.chembl_id_lookup_rows([record]) == [
        ("CHEMBL1", "COMPOUND", None, 28, "/chembl/api/data/molecule/CHEMBL1", "ACTIVE")
    ]


def test_lookup_records_without_an_identifier_are_skipped():
    assert records.chembl_id_lookup_rows([{"entity_type": "COMPOUND"}]) == []


def test_molecule_rows_split_across_three_tables():
    rows = records.molecule_rows([API_MOLECULE])
    assert len(rows.dictionary) == 1
    assert len(rows.properties) == 1
    assert len(rows.structures) == 1


def test_every_row_matches_its_column_count():
    rows = records.molecule_rows([API_MOLECULE])
    assert len(rows.dictionary[0]) == len(records.MOLECULE_DICTIONARY_COLUMNS)
    assert len(rows.properties[0]) == len(records.COMPOUND_PROPERTIES_COLUMNS)
    assert len(rows.structures[0]) == len(records.COMPOUND_STRUCTURES_COLUMNS)


def test_identifier_is_renamed_and_leads_every_row():
    rows = records.molecule_rows([API_MOLECULE])
    assert rows.dictionary[0][0] == "CHEMBL25"
    assert rows.properties[0][0] == "CHEMBL25"
    assert rows.structures[0][0] == "CHEMBL25"


def test_absent_fields_become_none():
    rows = records.molecule_rows([{"molecule_chembl_id": "CHEMBL1"}])
    assert set(rows.dictionary[0][1:]) == {None}


def test_missing_sub_documents_produce_no_rows():
    record = {"molecule_chembl_id": "CHEMBL1", "molecule_properties": None}
    rows = records.molecule_rows([record])
    assert rows.dictionary and not rows.properties and not rows.structures


def test_molecules_without_an_identifier_are_skipped():
    rows = records.molecule_rows([{"pref_name": "orphan record"}])
    assert not rows.dictionary


def test_molfile_newlines_survive_mapping():
    rows = records.molecule_rows([API_MOLECULE])
    assert "\n" in rows.structures[0][-1]


def test_requested_api_fields_cover_every_mapped_column():
    expected = set(records.MOLECULE_DICTIONARY_COLUMNS[1:])
    assert expected <= set(records.MOLECULE_API_FIELDS)
    assert "molecule_chembl_id" in records.MOLECULE_API_FIELDS


def test_lookup_requests_no_field_filter():
    assert records.CHEMBL_ID_LOOKUP.fields == ()


def test_molecule_requests_a_field_filter():
    assert records.MOLECULE.fields == records.MOLECULE_API_FIELDS
    assert "molecule_properties" in records.MOLECULE.fields
