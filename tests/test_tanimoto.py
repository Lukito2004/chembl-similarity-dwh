import numpy as np
import pyarrow as pa
import pytest

from chembl_sim.chem import tanimoto


def library(bit_patterns, ids=None):
    packed = np.array([[p] for p in bit_patterns], dtype=np.uint8)
    names = ids or [f"CHEMBL{i}" for i in range(len(bit_patterns))]
    return tanimoto.FingerprintLibrary.from_arrays(np.array(names, dtype=tanimoto.ID_DTYPE), packed)


def test_popcount_matches_unpackbits():
    values = np.arange(256, dtype=np.uint8)
    assert np.array_equal(tanimoto.popcount(values), np.unpackbits(values[:, None], axis=1).sum(1))


def test_a_hand_computed_score():
    # 0b11110000 and 0b00111100 share two bits of the six in their union.
    lib = library([0b00111100])
    scores = tanimoto.tanimoto_scores(lib, np.array([0b11110000], dtype=np.uint8), 4)
    assert scores[0] == pytest.approx(2 / 6, abs=1e-6)


def test_a_molecule_is_identical_to_itself():
    lib = library([0b10110110])
    scores = tanimoto.tanimoto_scores(lib, lib.fingerprints[0], int(lib.popcounts[0]))
    assert scores[0] == pytest.approx(1.0)


def test_disjoint_fingerprints_score_zero():
    lib = library([0b00001111])
    scores = tanimoto.tanimoto_scores(lib, np.array([0b11110000], dtype=np.uint8), 4)
    assert scores[0] == 0.0


def test_two_empty_fingerprints_score_zero_not_one():
    lib = library([0b00000000])
    scores = tanimoto.tanimoto_scores(lib, np.array([0b00000000], dtype=np.uint8), 0)
    assert scores[0] == 0.0


def test_blocking_does_not_change_the_result():
    rng = np.random.default_rng(7)
    packed = rng.integers(0, 256, size=(500, 4), dtype=np.uint8)
    lib = tanimoto.FingerprintLibrary.from_arrays(
        np.array([f"CHEMBL{i}" for i in range(500)], dtype=tanimoto.ID_DTYPE), packed
    )
    query, qpop = packed[3], int(lib.popcounts[3])
    whole = tanimoto.tanimoto_scores(lib, query, qpop, block_size=10_000)
    blocked = tanimoto.tanimoto_scores(lib, query, qpop, block_size=7)
    assert np.array_equal(whole, blocked)


def test_matches_come_back_in_descending_score_order():
    lib = library([0b00000001, 0b00000011, 0b00000111])
    scores = np.array([0.1, 0.9, 0.5], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=3)
    assert [m.tanimoto_score for m in matches] == pytest.approx([0.9, 0.5, 0.1], abs=1e-6)


def test_only_top_n_are_returned():
    lib = library([1, 2, 3, 4, 5])
    scores = np.array([0.5, 0.4, 0.3, 0.2, 0.1], dtype=np.float32)
    assert len(tanimoto.top_matches(lib, scores, top_n=2)) == 2


def test_the_source_molecule_is_excluded():
    lib = library([1, 2, 3])
    scores = np.array([1.0, 0.4, 0.3], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=2, exclude_index=0)
    assert [m.target_chembl_id for m in matches] == ["CHEMBL1", "CHEMBL2"]


def test_a_short_library_returns_what_it_has():
    lib = library([1, 2])
    scores = np.array([0.5, 0.4], dtype=np.float32)
    assert len(tanimoto.top_matches(lib, scores, top_n=10)) == 2


def test_ties_beyond_the_cut_flag_the_boundary_rows():
    lib = library([1, 2, 3, 4])
    scores = np.array([0.9, 0.5, 0.5, 0.5], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=3)
    assert [m.tanimoto_score for m in matches] == pytest.approx([0.9, 0.5, 0.5], abs=1e-6)
    assert [m.has_duplicates_of_last_largest_score for m in matches] == [False, True, True]


def test_ties_that_fit_exactly_are_not_flagged():
    lib = library([1, 2, 3])
    scores = np.array([0.9, 0.5, 0.5], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=3)
    assert not any(m.has_duplicates_of_last_largest_score for m in matches)


def test_no_ties_means_no_flag():
    lib = library([1, 2, 3])
    scores = np.array([0.9, 0.5, 0.1], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=2)
    assert not any(m.has_duplicates_of_last_largest_score for m in matches)


def test_only_the_boundary_score_is_flagged_not_the_whole_top_n():
    lib = library([1, 2, 3, 4, 5])
    scores = np.array([0.9, 0.9, 0.5, 0.5, 0.5], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=4)
    assert [m.has_duplicates_of_last_largest_score for m in matches] == [False, False, True, True]


def test_ties_break_deterministically_on_the_identifier():
    ids = ["CHEMBL30", "CHEMBL10", "CHEMBL20"]
    lib = library([1, 2, 3], ids=ids)
    scores = np.array([0.5, 0.5, 0.5], dtype=np.float32)
    matches = tanimoto.top_matches(lib, scores, top_n=3)
    assert [m.target_chembl_id for m in matches] == ["CHEMBL10", "CHEMBL20", "CHEMBL30"]


def test_index_of_finds_and_misses():
    lib = library([1, 2, 3])
    assert lib.index_of("CHEMBL1") == 1
    assert lib.index_of("CHEMBL404") is None


def test_a_library_loads_from_a_parquet_table_without_copying():
    ids = ["CHEMBL1", "CHEMBL2"]
    packed = [bytes([1, 2, 3, 4]), bytes([5, 6, 7, 8])]
    table = pa.table({"chembl_id": pa.array(ids), "fingerprint": pa.array(packed, pa.binary())})
    lib = tanimoto.FingerprintLibrary.from_table(table, n_bytes=4)
    assert len(lib) == 2
    assert lib.fingerprints[1].tolist() == [5, 6, 7, 8]
    assert lib.index_of("CHEMBL2") == 1


def test_a_wrong_fingerprint_width_is_rejected():
    table = pa.table(
        {"chembl_id": pa.array(["CHEMBL1"]), "fingerprint": pa.array([b"\x01\x02"], pa.binary())}
    )
    with pytest.raises(ValueError, match="fingerprint bytes"):
        tanimoto.FingerprintLibrary.from_table(table, n_bytes=256)
