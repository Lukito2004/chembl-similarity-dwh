from chembl_sim.transform import silver


class RecordingCursor:
    def __init__(self, count):
        self.count = count
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append(statement)

    def fetchone(self):
        return (self.count,)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        pass


def test_the_table_is_rebuilt_rather_than_appended():
    assert "TRUNCATE silver.molecule" in silver.BUILD_SILVER_MOLECULE


def test_only_molecules_with_a_structure_are_conformed():
    sql = silver.BUILD_SILVER_MOLECULE
    assert "FROM bronze.compound_structures s" in sql
    assert "WHERE s.canonical_smiles IS NOT NULL" in sql


def test_properties_are_optional():
    assert "LEFT JOIN bronze.compound_properties" in silver.BUILD_SILVER_MOLECULE


def test_every_text_decimal_goes_through_the_total_cast():
    sql = silver.BUILD_SILVER_MOLECULE
    for column in (
        "max_phase",
        "mw_freebase",
        "full_mwt",
        "alogp",
        "psa",
        "qed_weighted",
        "np_likeness_score",
    ):
        assert (
            f"silver.safe_numeric(d.{column})" in sql or f"silver.safe_numeric(p.{column})" in sql
        )


def test_the_release_37_gap_is_explicit():
    assert "NULL::numeric" in silver.BUILD_SILVER_MOLECULE
    assert "NULL::text" in silver.BUILD_SILVER_MOLECULE


def test_statistics_are_refreshed_for_the_planner():
    assert "ANALYZE silver.molecule" in silver.BUILD_SILVER_MOLECULE


def test_build_returns_the_row_count(monkeypatch):
    cursor = RecordingCursor(count=2890000)
    monkeypatch.setattr(
        silver,
        "warehouse_connection",
        lambda dsn=None: _as_context(FakeConnection(cursor)),
    )
    assert silver.build_silver_molecule() == 2890000
    assert silver.BUILD_SILVER_MOLECULE in cursor.statements


def _as_context(connection):
    from contextlib import contextmanager

    @contextmanager
    def ctx():
        yield connection

    return ctx()
