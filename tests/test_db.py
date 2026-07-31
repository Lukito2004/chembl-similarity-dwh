import pytest
from psycopg2 import sql as pgsql

from chembl_sim.storage import db


def render_sql(composed):
    """Render psycopg2 sql objects offline, since quote_ident needs a live connection."""
    if isinstance(composed, pgsql.Composed):
        return "".join(render_sql(part) for part in composed.seq)
    if isinstance(composed, pgsql.Identifier):
        return ".".join(f'"{s}"' for s in composed.strings)
    if isinstance(composed, pgsql.SQL):
        return composed.string
    return str(composed)


class FakeCursor:
    def __init__(self, executed):
        self.executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, statement):
        self.executed.append(statement)


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self.executed)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


@pytest.fixture
def fake_connect(monkeypatch):
    connection = FakeConnection()

    def connect(dsn):
        connection.dsn = dsn
        return connection

    monkeypatch.setattr(db.psycopg2, "connect", connect)
    return connection


def write_sql_files(directory, names):
    for name in names:
        (directory / name).write_text(f"select '{name}';", encoding="utf-8")


def test_files_are_ordered_by_numeric_prefix(tmp_path):
    write_sql_files(tmp_path, ["04_gold.sql", "01_schemas.sql", "10_view.sql", "02_bronze.sql"])
    assert [p.name for p in db.discover_sql_files(tmp_path)] == [
        "01_schemas.sql",
        "02_bronze.sql",
        "04_gold.sql",
        "10_view.sql",
    ]


def test_non_sql_files_are_ignored(tmp_path):
    write_sql_files(tmp_path, ["01_schemas.sql"])
    (tmp_path / "notes.md").write_text("ignore me", encoding="utf-8")
    (tmp_path / "02_view.sql.j2").write_text("{{ template }}", encoding="utf-8")
    assert [p.name for p in db.discover_sql_files(tmp_path)] == ["01_schemas.sql"]


def test_missing_directory_is_reported(tmp_path):
    with pytest.raises(db.SqlDirectoryError, match="does not exist"):
        db.discover_sql_files(tmp_path / "absent")


def test_empty_directory_is_reported(tmp_path):
    with pytest.raises(db.SqlDirectoryError, match="no .sql files"):
        db.discover_sql_files(tmp_path)


def test_every_file_is_applied_in_order(tmp_path, fake_connect):
    write_sql_files(tmp_path, ["02_bronze.sql", "01_schemas.sql"])
    applied = db.apply_sql_directory(tmp_path)
    assert [p.name for p in applied] == ["01_schemas.sql", "02_bronze.sql"]
    assert fake_connect.executed == ["select '01_schemas.sql';", "select '02_bronze.sql';"]


def test_success_commits_and_closes(tmp_path, fake_connect):
    write_sql_files(tmp_path, ["01_schemas.sql"])
    db.apply_sql_directory(tmp_path)
    assert fake_connect.committed
    assert fake_connect.closed
    assert not fake_connect.rolled_back


def test_failure_rolls_back_and_reraises(tmp_path, fake_connect, monkeypatch):
    write_sql_files(tmp_path, ["01_schemas.sql"])

    def explode(self, statement):
        raise RuntimeError("bad ddl")

    monkeypatch.setattr(FakeCursor, "execute", explode)
    with pytest.raises(RuntimeError, match="bad ddl"):
        db.apply_sql_directory(tmp_path)
    assert fake_connect.rolled_back
    assert fake_connect.closed
    assert not fake_connect.committed


def test_dsn_falls_back_to_settings(tmp_path, fake_connect, monkeypatch):
    monkeypatch.setenv("CHEMBL_DWH_DSN", "postgresql://from:settings@host:5432/db")
    write_sql_files(tmp_path, ["01_schemas.sql"])
    db.apply_sql_directory(tmp_path)
    assert fake_connect.dsn == "postgresql://from:settings@host:5432/db"


def test_explicit_dsn_overrides_settings(tmp_path, fake_connect):
    write_sql_files(tmp_path, ["01_schemas.sql"])
    db.apply_sql_directory(tmp_path, dsn="postgresql://explicit@host:5432/db")
    assert fake_connect.dsn == "postgresql://explicit@host:5432/db"


class RecordingCursor:
    def __init__(self):
        self.statement = None
        self.rows = None

    def execute(self, statement, params=None):
        self.statement = statement


def test_upsert_builds_an_on_conflict_statement(monkeypatch):
    captured = {}

    def fake_execute_values(cursor, statement, rows, page_size):
        captured["statement"] = render_sql(statement)
        captured["rows"] = rows

    monkeypatch.setattr(db, "execute_values", fake_execute_values)
    count = db.upsert_rows(
        RecordingCursor(), "bronze", "widget", ("chembl_id", "name"), [("CHEMBL1", "x")]
    )
    assert count == 1
    assert '"bronze"."widget"' in captured["statement"]
    assert 'ON CONFLICT ("chembl_id") DO UPDATE' in captured["statement"]
    assert '"name" = EXCLUDED."name"' in captured["statement"]
    assert "loaded_at = now()" in captured["statement"]


def test_upsert_of_no_rows_touches_nothing(monkeypatch):
    monkeypatch.setattr(db, "execute_values", lambda *a, **k: pytest.fail("should not run"))
    assert db.upsert_rows(RecordingCursor(), "bronze", "widget", ("chembl_id",), []) == 0
