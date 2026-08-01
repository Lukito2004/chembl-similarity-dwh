from chembl_sim.chem import search


def test_staged_columns_match_the_silver_table():
    assert search.SIMILARITY_TOP_COLUMNS == (
        "source_chembl_id",
        "target_chembl_id",
        "tanimoto_score",
        "has_duplicates_of_last_largest_score",
        "match_rank",
    )


def test_the_result_counts_are_reported():
    result = search.SearchResult(sources=100, missing=0, top_rows=1000, flagged_rows=40)
    assert result.sources == 100 and result.top_rows == 1000
