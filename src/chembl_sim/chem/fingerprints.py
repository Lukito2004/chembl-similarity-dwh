"""Morgan fingerprints packed into fixed-width bytes."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
from rdkit import Chem, RDLogger
from rdkit.Chem import rdFingerprintGenerator

from chembl_sim.logging_setup import get_logger

log = get_logger(__name__)

# RDKit logs a warning per unparseable SMILES, which is unusable across millions of rows.
RDLogger.DisableLog("rdApp.*")


@dataclass(frozen=True)
class FingerprintBatch:
    """Identifiers and packed fingerprints, plus the molecules RDKit could not parse."""

    chembl_ids: list[str]
    fingerprints: list[bytes]
    rejected: int


def build_generator(radius: int, n_bits: int):
    """One generator is reused across a whole shard; building per molecule is far slower."""
    return rdFingerprintGenerator.GetMorganGenerator(radius=radius, fpSize=n_bits)


def packed_fingerprint(generator, smiles: str) -> bytes | None:
    """Pack a fingerprint into n_bits/8 bytes, or None when the SMILES will not parse."""
    molecule = Chem.MolFromSmiles(smiles)
    if molecule is None:
        return None
    return np.packbits(generator.GetFingerprintAsNumPy(molecule)).tobytes()


def fingerprint_rows(
    rows: Iterable[tuple[str, str]],
    radius: int,
    n_bits: int,
) -> FingerprintBatch:
    """Fingerprint (chembl_id, smiles) pairs, dropping any RDKit cannot parse."""
    generator = build_generator(radius, n_bits)
    chembl_ids: list[str] = []
    fingerprints: list[bytes] = []
    rejected = 0

    for chembl_id, smiles in rows:
        packed = packed_fingerprint(generator, smiles) if smiles else None
        if packed is None:
            rejected += 1
            continue
        chembl_ids.append(chembl_id)
        fingerprints.append(packed)

    return FingerprintBatch(chembl_ids=chembl_ids, fingerprints=fingerprints, rejected=rejected)
