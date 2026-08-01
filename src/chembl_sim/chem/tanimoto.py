"""Tanimoto similarity over packed fingerprints."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from chembl_sim.logging_setup import get_logger

log = get_logger(__name__)

# numpy gained a native popcount in 2.0, but Airflow 2.10 pins numpy to 1.26,
# so a 256 entry lookup table stands in for it.
_POPCOUNT = np.unpackbits(np.arange(256, dtype=np.uint8)[:, None], axis=1).sum(1).astype(np.uint8)

ID_DTYPE = "S16"


def popcount(values: np.ndarray) -> np.ndarray:
    """Bits set per byte, preferring the native operation when numpy provides one."""
    if hasattr(np, "bitwise_count"):
        return np.bitwise_count(values)
    return _POPCOUNT[values]


@dataclass(frozen=True)
class TopMatch:
    """One row of a source molecule's top-N table."""

    target_chembl_id: str
    tanimoto_score: float
    has_duplicates_of_last_largest_score: bool


@dataclass(frozen=True)
class FingerprintLibrary:
    """Every molecule's packed fingerprint, held in memory for scoring."""

    chembl_ids: np.ndarray
    fingerprints: np.ndarray
    popcounts: np.ndarray

    def __len__(self) -> int:
        return int(self.fingerprints.shape[0])

    def index_of(self, chembl_id: str) -> int | None:
        """Position of a molecule, or None when the library does not hold it."""
        hits = np.flatnonzero(self.chembl_ids == chembl_id.encode())
        return int(hits[0]) if hits.size else None

    @classmethod
    def from_arrays(
        cls,
        chembl_ids: np.ndarray,
        fingerprints: np.ndarray,
        block_size: int = 250_000,
    ) -> FingerprintLibrary:
        rows = fingerprints.shape[0]
        counts = np.empty(rows, dtype=np.int32)
        for start in range(0, rows, block_size):
            stop = min(start + block_size, rows)
            counts[start:stop] = popcount(fingerprints[start:stop]).sum(axis=1, dtype=np.int32)
        return cls(chembl_ids=chembl_ids, fingerprints=fingerprints, popcounts=counts)

    @classmethod
    def from_table(
        cls,
        table: pa.Table,
        n_bytes: int,
        block_size: int = 250_000,
    ) -> FingerprintLibrary:
        """Build from the fingerprint parquet, reshaping the byte buffer without copying."""
        combined = table.combine_chunks()
        buffer = combined.column("fingerprint").chunk(0).buffers()[2]
        rows = combined.num_rows
        if len(buffer) != rows * n_bytes:
            raise ValueError(f"expected {rows * n_bytes} fingerprint bytes, found {len(buffer)}")
        packed = np.frombuffer(buffer, dtype=np.uint8).reshape(rows, n_bytes)
        ids = np.array(combined.column("chembl_id").to_pylist(), dtype=ID_DTYPE)
        return cls.from_arrays(ids, packed, block_size=block_size)


def tanimoto_scores(
    library: FingerprintLibrary,
    query: np.ndarray,
    query_popcount: int,
    block_size: int = 250_000,
) -> np.ndarray:
    """Score one fingerprint against the whole library.

    Blocked so the intermediate AND never materialises for every molecule at once.
    Scores are double precision so exact ratios such as 7/10 survive rounding into the
    mart's eight decimal places; float32 renders that as 0.69999999.
    """
    total = len(library)
    scores = np.zeros(total, dtype=np.float64)
    for start in range(0, total, block_size):
        stop = min(start + block_size, total)
        intersection = popcount(np.bitwise_and(library.fingerprints[start:stop], query)).sum(
            axis=1, dtype=np.int32
        )
        union = library.popcounts[start:stop] + query_popcount - intersection
        # Two empty fingerprints have an empty union; that scores zero rather than one.
        np.divide(intersection, union, out=scores[start:stop], where=union > 0)
    return scores


def top_matches(
    library: FingerprintLibrary,
    scores: np.ndarray,
    top_n: int,
    exclude_index: int | None = None,
) -> list[TopMatch]:
    """The highest scoring molecules, with boundary ties flagged.

    When more molecules share the last included score than there is room for, every
    included row holding that score is flagged.
    """
    working = scores
    if exclude_index is not None:
        working = scores.copy()
        working[exclude_index] = -1.0

    available = working.size - (1 if exclude_index is not None else 0)
    if available <= 0:
        return []
    limit = min(top_n, available)

    partition = np.argpartition(working, -limit)[-limit:]
    threshold = float(working[partition].min())

    candidates = np.flatnonzero(working >= threshold)
    if exclude_index is not None:
        candidates = candidates[candidates != exclude_index]

    # lexsort takes its last key as primary, so this is score descending then id ascending.
    order = np.lexsort((library.chembl_ids[candidates], -working[candidates]))
    chosen = candidates[order[:limit]]
    overflow = candidates.size > limit

    return [
        TopMatch(
            target_chembl_id=library.chembl_ids[index].decode(),
            tanimoto_score=float(working[index]),
            has_duplicates_of_last_largest_score=bool(
                overflow and float(working[index]) == threshold
            ),
        )
        for index in chosen
    ]
