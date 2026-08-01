"""Builds the pivot view, whose columns depend on which molecules are in the mart."""

from __future__ import annotations

from psycopg2 import sql

from chembl_sim.logging_setup import get_logger
from chembl_sim.storage.db import warehouse_connection

log = get_logger(__name__)

# The task asks for ten source molecules. Taking the lowest ten identifiers keeps the
# same columns on every rebuild instead of shuffling them.
PIVOT_SOURCE_COUNT = 10

SELECT_PIVOT_SOURCES = """
    SELECT DISTINCT source_chembl_id
    FROM gold.fact_molecule_similarity
    ORDER BY source_chembl_id
    LIMIT %s
"""


def render_pivot_view(sources: list[str]) -> sql.Composed:
    """One real column per source molecule, so the result is a grid rather than JSON."""
    columns = [
        sql.SQL("max(tanimoto_score) FILTER (WHERE source_chembl_id = {value}) AS {name}").format(
            value=sql.Literal(source), name=sql.Identifier(source)
        )
        for source in sources
    ]
    return sql.SQL(
        "CREATE VIEW gold.v_similarity_pivot AS "
        "SELECT target_chembl_id, {columns} "
        "FROM gold.fact_molecule_similarity "
        "WHERE source_chembl_id IN ({sources}) "
        "GROUP BY target_chembl_id"
    ).format(
        columns=sql.SQL(", ").join(columns),
        sources=sql.SQL(", ").join(sql.Literal(source) for source in sources),
    )


def create_pivot_view(dsn: str | None = None) -> list[str]:
    """Rebuild the pivot. It is dropped first because a replace cannot rename columns."""
    with warehouse_connection(dsn) as conn, conn.cursor() as cursor:
        cursor.execute(SELECT_PIVOT_SOURCES, (PIVOT_SOURCE_COUNT,))
        sources = [row[0] for row in cursor.fetchall()]
        if not sources:
            raise RuntimeError("the fact table holds no source molecules")
        cursor.execute("DROP VIEW IF EXISTS gold.v_similarity_pivot")
        cursor.execute(render_pivot_view(sources))
    log.info("Pivot view rebuilt over %s source molecules", len(sources))
    return sources
