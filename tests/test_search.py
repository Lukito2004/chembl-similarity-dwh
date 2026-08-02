import numpy as np
import pyarrow as pa
import pytest

from chembl_sim.chem import search
from chembl_sim.chem.tanimoto import FingerprintLibrary
from chembl_sim.settings import get_settings


def test_staged_columns_match_the_silver_table():
    assert search.SIMILARITY_TOP_COLUMNS == (
        "source_chembl_id",
        "target_chembl_id",
        "tanimoto_score",
        "has_duplicates_of_last_largest_score",
        "match_rank",
    )


def test_the_result_counts_are_reported():
    result = search.SearchResult(sources=100, top_rows=1000, flagged_rows=40)
    assert result.sources == 100 and result.top_rows == 1000


def test_the_expected_shard_count_rounds_up(fake_warehouse):
    fake_warehouse(search, [(250_001,)])
    assert search.expected_shard_count(get_settings()) == 2


@pytest.fixture
def tiny_library(monkeypatch):
    library = FingerprintLibrary.from_arrays(
        np.array(["CHEMBL1", "CHEMBL2"], dtype="S16"),
        np.array([[0b11110000], [0b00111100]], dtype=np.uint8),
    )
    monkeypatch.setattr(
        search,
        "list_keys",
        lambda prefix, settings=None: ["shard"] if "fingerprints" in prefix else [],
    )
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
    monkeypatch.setattr(search, "delete_keys", lambda keys, settings=None: len(keys))
    monkeypatch.setattr(search, "write_parquet", lambda table, key, settings=None: 1)
    return library


def test_a_source_without_a_fingerprint_stops_the_search(tiny_library, fake_warehouse, monkeypatch):
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL1", "CHEMBL404"])
    fake_warehouse(search, [(2,)])
    with pytest.raises(search.MissingFingerprintError, match="CHEMBL404"):
        search.run_similarity_search()


def test_the_previous_results_survive_a_source_that_cannot_be_scored(
    tiny_library, fake_warehouse, monkeypatch
):
    deleted = []
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL404"])
    monkeypatch.setattr(search, "delete_keys", lambda keys, settings=None: deleted.extend(keys))
    fake_warehouse(search, [(2,)])
    with pytest.raises(search.MissingFingerprintError):
        search.run_similarity_search()
    assert deleted == []


def test_stale_score_tables_are_pruned_only_after_a_successful_run(
    tiny_library, fake_warehouse, monkeypatch
):
    deleted = []
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL1"])
    monkeypatch.setattr(search, "insert_rows", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        search,
        "list_keys",
        lambda prefix, settings=None: ["shard"] if "fingerprints" in prefix else ["gone.parquet"],
    )
    monkeypatch.setattr(search, "delete_keys", lambda keys, settings=None: deleted.extend(keys))
    fake_warehouse(search, [(2,)])
    search.run_similarity_search()
    assert deleted == ["gone.parquet"]


def test_a_short_fingerprint_library_stops_the_search(tiny_library, fake_warehouse, monkeypatch):
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL1"])
    fake_warehouse(search, [(500_000,)])
    with pytest.raises(search.IncompleteLibraryError, match="1 shards, expected 2"):
        search.run_similarity_search()


def test_staged_rows_carry_a_rank(tiny_library, fake_warehouse, monkeypatch):
    staged = {}
    monkeypatch.setattr(search, "load_source_molecules", lambda dsn=None: ["CHEMBL1"])
    monkeypatch.setattr(
        search, "insert_rows", lambda cursor, schema, table, columns, rows: staged.update(rows=rows)
    )
    fake_warehouse(search, [(2,)])
    search.run_similarity_search()
    assert staged["rows"]
    assert staged["rows"][0][0] == "CHEMBL1"
    assert staged["rows"][0][4] == 1
