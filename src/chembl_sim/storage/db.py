"""Warehouse connections and SQL file execution."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import get_settings

log = get_logger(__name__)


class SqlDirectoryError(RuntimeError):
    """The SQL directory is missing or holds nothing to apply."""


@contextmanager
def warehouse_connection(dsn: str | None = None) -> Iterator[psycopg2.extensions.connection]:
    """Open a warehouse connection, committing on success and rolling back on failure."""
    conn = psycopg2.connect(dsn or get_settings().dwh.dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def discover_sql_files(directory: Path) -> list[Path]:
    """Return the .sql files ordered by filename, which zero-padded prefixes make correct."""
    if not directory.is_dir():
        raise SqlDirectoryError(f"SQL directory does not exist: {directory}")
    files = sorted(directory.glob("*.sql"))
    if not files:
        raise SqlDirectoryError(f"no .sql files found in {directory}")
    return files


def apply_sql_directory(directory: Path, dsn: str | None = None) -> list[Path]:
    """Apply every .sql file in order inside one transaction, so a failure applies nothing."""
    files = discover_sql_files(directory)
    with warehouse_connection(dsn) as conn, conn.cursor() as cur:
        for path in files:
            log.info("Applying %s", path.name)
            cur.execute(path.read_text(encoding="utf-8"))
    return files


def upsert_rows(
    cursor,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple],
    conflict_column: str = "chembl_id",
    page_size: int = 1000,
) -> int:
    """Insert rows, refreshing any that already exist so a resumed ingest never duplicates."""
    if not rows:
        return 0
    assignments = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(column))
        for column in columns
        if column != conflict_column
    )
    statement = sql.SQL(
        "INSERT INTO {target} ({columns}) VALUES %s "
        "ON CONFLICT ({conflict}) DO UPDATE SET {assignments}, loaded_at = now()"
    ).format(
        target=sql.Identifier(schema, table),
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        conflict=sql.Identifier(conflict_column),
        assignments=assignments,
    )
    execute_values(cursor, statement, rows, page_size=page_size)
    return len(rows)


def insert_rows(
    cursor,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Sequence[tuple],
    page_size: int = 1000,
) -> int:
    """Bulk insert with no conflict handling, for a table the caller has just emptied."""
    if not rows:
        return 0
    statement = sql.SQL("INSERT INTO {target} ({columns}) VALUES %s").format(
        target=sql.Identifier(schema, table),
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )
    execute_values(cursor, statement, rows, page_size=page_size)
    return len(rows)
