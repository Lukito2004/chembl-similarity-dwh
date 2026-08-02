"""Parses the personal input batch CSVs into a single bronze row shape."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field

from chembl_sim.logging_setup import get_logger

log = get_logger(__name__)

# The union of every column seen across the batch files, in bronze column order.
INPUT_COLUMNS = (
    "compound_id",
    "compound_name",
    "molecular_weight",
    "logp",
    "ic50_nm",
    "assay_date",
    "collection_date",
    "lab_id",
)

BRONZE_COLUMNS = ("source_file", "source_row", *INPUT_COLUMNS)

# Names are the only way a batch row reaches ChEMBL, so a file without them is unusable.
NAME_COLUMN = "compound_name"


class MissingNameColumnError(ValueError):
    """A batch file carries no compound_name column, so none of its rows can be matched."""


@dataclass
class ParsedBatch:
    """Rows ready for bronze, plus any header the loader did not recognise."""

    rows: list[tuple] = field(default_factory=list)
    unknown_headers: set[str] = field(default_factory=set)


def normalise_header(name: str) -> str:
    """Headers drift in case between files, so IC50_nM and ic50_nm are the same column.

    A spreadsheet export puts a byte order mark on the first header. Python does not count
    it as whitespace, so it has to be stripped by hand or the first column goes unrecognised.
    """
    return name.lstrip("\ufeff").strip().lower()


def parse_batch_csv(text: str, source_file: str) -> ParsedBatch:
    """Read one batch file, filling absent columns with None."""
    reader = csv.reader(io.StringIO(text))
    try:
        header = [normalise_header(name) for name in next(reader)]
    except StopIteration:
        log.warning("%s is empty", source_file)
        return ParsedBatch()

    positions = {name: index for index, name in enumerate(header)}
    if NAME_COLUMN not in positions:
        raise MissingNameColumnError(
            f"{source_file} has no {NAME_COLUMN} column, found {sorted(positions)}"
        )
    parsed = ParsedBatch(unknown_headers={h for h in header if h not in INPUT_COLUMNS})

    for row_number, values in enumerate(reader, start=2):
        if not any(value.strip() for value in values):
            continue
        cells = []
        for column in INPUT_COLUMNS:
            index = positions.get(column)
            value = values[index].strip() if index is not None and index < len(values) else ""
            cells.append(value or None)
        parsed.rows.append((source_file, row_number, *cells))

    if parsed.unknown_headers:
        log.warning("%s has unrecognised headers: %s", source_file, sorted(parsed.unknown_headers))
    return parsed
