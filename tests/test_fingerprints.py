import numpy as np
import pytest

from chembl_sim.chem import fingerprints

ASPIRIN = "CC(=O)Oc1ccccc1C(=O)O"


@pytest.fixture
def generator():
    return fingerprints.build_generator(radius=2, n_bits=2048)


def test_a_fingerprint_packs_to_the_expected_width(generator):
    assert len(fingerprints.packed_fingerprint(generator, ASPIRIN)) == 256


def test_the_same_molecule_always_gives_the_same_bytes(generator):
    assert fingerprints.packed_fingerprint(generator, ASPIRIN) == fingerprints.packed_fingerprint(
        generator, ASPIRIN
    )


def test_different_molecules_differ(generator):
    assert fingerprints.packed_fingerprint(generator, ASPIRIN) != fingerprints.packed_fingerprint(
        generator, "CCO"
    )


def test_packed_bytes_unpack_to_the_original_bits(generator):
    packed = fingerprints.packed_fingerprint(generator, ASPIRIN)
    bits = np.unpackbits(np.frombuffer(packed, dtype=np.uint8))
    assert bits.size == 2048
    assert bits.sum() > 0


def test_unparseable_smiles_returns_none(generator):
    assert fingerprints.packed_fingerprint(generator, "not-a-molecule") is None


def test_a_batch_drops_and_counts_rejects():
    batch = fingerprints.fingerprint_rows(
        [("CHEMBL25", ASPIRIN), ("BAD", "not-a-molecule"), ("CHEMBL545", "CCO")], 2, 2048
    )
    assert batch.chembl_ids == ["CHEMBL25", "CHEMBL545"]
    assert batch.rejected == 1
    assert all(len(fp) == 256 for fp in batch.fingerprints)


def test_a_null_smiles_is_rejected_rather_than_raising():
    batch = fingerprints.fingerprint_rows([("CHEMBL1", None)], 2, 2048)
    assert batch.chembl_ids == [] and batch.rejected == 1


def test_bit_count_changes_the_packed_width():
    narrow = fingerprints.build_generator(radius=2, n_bits=512)
    assert len(fingerprints.packed_fingerprint(narrow, ASPIRIN)) == 64
