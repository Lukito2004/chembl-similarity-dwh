from chembl_sim.transform import mart


def test_both_tables_are_emptied_in_one_statement():
    # The fact's foreign keys forbid truncating the dimension alone.
    assert "TRUNCATE gold.fact_molecule_similarity, gold.dim_molecule;" in mart.LOAD_MART


def test_the_dimension_is_filled_before_the_fact():
    sql = mart.LOAD_MART
    assert sql.index("INSERT INTO gold.dim_molecule") < sql.index(
        "INSERT INTO gold.fact_molecule_similarity"
    )


def test_the_dimension_holds_only_referenced_molecules():
    sql = mart.LOAD_MART
    assert "SELECT source_chembl_id FROM silver.similarity_top" in sql
    assert "SELECT target_chembl_id FROM silver.similarity_top" in sql
    assert "UNION" in sql


def test_the_specified_dimension_columns_are_all_present():
    for column in (
        "chembl_id",
        "molecule_type",
        "mw_freebase",
        "alogp",
        "psa",
        "cx_logp",
        "molecular_species",
        "full_mwt",
        "aromatic_rings",
        "heavy_atoms",
    ):
        assert column in mart.LOAD_MART


def test_statistics_are_refreshed():
    assert "ANALYZE gold.dim_molecule" in mart.LOAD_MART
    assert "ANALYZE gold.fact_molecule_similarity" in mart.LOAD_MART
