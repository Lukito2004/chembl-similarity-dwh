import numpy as np
import pyarrow as pa
import pytest

from chembl_sim.chem import search
from chembl_sim.chem.tanimoto import FingerprintLibrary


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


@pytest.fixture
def tiny_library(monkeypatch):
    library = FingerprintLibrary.from_arrays(
        np.array(["CHEMBL1", "CHEMBL2"], dtype="S16"),
        np.array([[0b11110000], [0b00111100]], dtype=np.uint8),
    )
    monkeypatch.setattr(search, "list_keys", lambda prefix, settings=None: ["shard"])
    monkeypatch.setattr(
        search,
        "read_parquet",
        lambda key: pa.table(
            {"chembl_id": ["CHEMBL1", "CHEMBL2"], "fingerprint": [b"\xf0", b"\x3c"]}
        ),
    )
    monkeypatch.setattr(
        FingerprintLibrary,
        "from_table",
        classmethod(lambda cls, table, n_bytes, block_size=250_000: library),
    )
    monkeypatch.setattr(search, "delete_prefix", lambda prefix, settings=None: 0)
    monkeypatch.setattr(search, "write_parquet", lambda table, key, settings=None: 1)
    return library


def test_sources_without_a_fingerprint_are_counted_not_dropped_silently(
    tiny_library, fake_warehouse, monkeypatch
):
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL1", "CHEMBL404"])
    monkeypatch.setattr(search, "insert_rows", lambda *args, **kwargs: 0)
    fake_warehouse(search)
    result = search.run_similarity_search()
    assert result.missing == 1
    assert result.sources == 1


def test_staged_rows_carry_a_rank(tiny_library, fake_warehouse, monkeypatch):
    staged = {}
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL1"])
    monkeypatch.setattr(
        search, "insert_rows", lambda cursor, schema, table, columns, rows: staged.update(rows=rows)
    )
    fake_warehouse(search)
    search.run_similarity_search()
    assert staged["rows"]
    assert staged["rows"][0][0] == "CHEMBL1"
    assert staged["rows"][0][4] == 1
