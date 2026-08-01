from pathlib import Path

from psycopg2 import sql

from chembl_sim.transform import views

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def read_sql(name):
    return (SQL_DIR / name).read_text(encoding="utf-8")


def sql_body(name):
    """Statements only. Line comments are stripped so prose cannot trip the assertions.

    Adequate here because none of these files put a double dash inside a string literal.
    """
    return "\n".join(line.split("--")[0] for line in read_sql(name).splitlines())


def render(composed):
    """Render psycopg2 sql objects offline, since quote_ident needs a live connection."""
    if isinstance(composed, sql.Composed):
        return "".join(render(part) for part in composed.seq)
    if isinstance(composed, sql.Identifier):
        return ".".join('"' + s.replace('"', '""') + '"' for s in composed.strings)
    if isinstance(composed, sql.Literal):
        return "'" + str(composed.wrapped).replace("'", "''") + "'"
    if isinstance(composed, sql.SQL):
        return composed.string
    return str(composed)


def test_the_pivot_has_one_column_per_source():
    statement = render(views.render_pivot_view(["CHEMBL1", "CHEMBL2", "CHEMBL3"]))
    assert statement.count("FILTER (WHERE source_chembl_id =") == 3
    assert '"CHEMBL1"' in statement and '"CHEMBL3"' in statement


def test_the_pivot_keys_rows_by_target():
    statement = render(views.render_pivot_view(["CHEMBL1"]))
    assert "SELECT target_chembl_id" in statement
    assert "GROUP BY target_chembl_id" in statement


def test_the_pivot_restricts_to_the_chosen_sources():
    statement = render(views.render_pivot_view(["CHEMBL1", "CHEMBL2"]))
    assert "WHERE source_chembl_id IN (" in statement


def test_a_hostile_identifier_is_escaped_not_interpolated():
    hostile = 'a"; DROP TABLE gold.dim_molecule; --'
    statement = render(views.render_pivot_view([hostile]))
    assert '"a""; DROP TABLE gold.dim_molecule; --"' in statement
    assert '"a"; ' not in statement


def test_the_selection_is_ordered_and_capped():
    assert "ORDER BY source_chembl_id" in views.SELECT_PIVOT_SOURCES
    assert "LIMIT %s" in views.SELECT_PIVOT_SOURCES
    assert views.PIVOT_SOURCE_COUNT == 10


def test_the_rollup_uses_grouping_sets_and_no_union():
    statement = sql_body("13_view_similarity_rollup.sql").upper()
    assert "GROUPING SETS" in statement
    assert "UNION" not in statement


def test_the_rollup_labels_only_aggregated_nulls():
    statement = sql_body("13_view_similarity_rollup.sql")
    assert statement.count("grouping(") == 3
    assert statement.count("'TOTAL'") == 3


def test_the_rollup_covers_all_four_groupings():
    statement = read_sql("13_view_similarity_rollup.sql")
    assert "(f.source_chembl_id)" in statement
    assert "(d.aromatic_rings, d.heavy_atoms)" in statement
    assert "(d.heavy_atoms)" in statement
    assert "()" in statement


def test_the_rollup_groups_by_the_source_molecule_properties():
    assert "ON d.chembl_id = f.source_chembl_id" in read_sql("13_view_similarity_rollup.sql")


def test_the_neighbour_view_frames_the_whole_partition():
    statement = read_sql("12_view_similarity_neighbours.sql")
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in statement
    assert "nth_value(target_chembl_id, 2)" in statement
    assert "lead(target_chembl_id)" in statement


def test_the_neighbour_view_breaks_ties_deterministically():
    statement = read_sql("12_view_similarity_neighbours.sql")
    assert "ORDER BY tanimoto_score DESC, target_chembl_id" in statement


def test_the_deviation_view_is_a_mean_absolute_deviation():
    statement = read_sql("11_view_alogp_deviation.sql")
    assert "avg(abs(t.alogp - s.alogp))" in statement


def test_every_view_targets_the_gold_schema():
    for name in (
        "10_view_avg_similarity.sql",
        "11_view_alogp_deviation.sql",
        "12_view_similarity_neighbours.sql",
        "13_view_similarity_rollup.sql",
    ):
        assert "CREATE OR REPLACE VIEW gold." in read_sql(name)
