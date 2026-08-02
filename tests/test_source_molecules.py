from contextlib import contextmanager

import pytest

from chembl_sim.transform import source_molecules


class SequenceCursor:
    def __init__(self, counts):
        self.counts = list(counts)
        self.statements = []
        self.params = []

    def execute(self, statement, params=None):
        self.statements.append(statement)
        self.params.append(params)

    def fetchone(self):
        return (self.counts.pop(0),)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


@pytest.fixture
def patched(monkeypatch):
    def install(counts):
        cursor = SequenceCursor(counts)

        @contextmanager
        def fake_connection(dsn=None):
            yield FakeConnection(cursor)

        monkeypatch.setattr(source_molecules, "warehouse_connection", fake_connection)
        return cursor

    return install


def test_both_tables_are_rebuilt():
    sql = source_molecules.RESOLVE_INPUT_COMPOUNDS
    assert "TRUNCATE silver.source_molecule;" in sql
    assert "TRUNCATE silver.source_molecule_rejected;" in sql


def test_resolution_matches_on_the_preferred_name():
    assert "upper(m.pref_name) = upper(btrim(i.compound_name))" in (
        source_molecules.RESOLVE_INPUT_COMPOUNDS
    )


def test_one_input_row_wins_per_molecule():
    assert "DISTINCT ON (m.chembl_id)" in source_molecules.RESOLVE_INPUT_COMPOUNDS


def test_both_rejection_reasons_are_recorded():
    sql = source_molecules.RESOLVE_INPUT_COMPOUNDS
    assert "compound name is blank" in sql
    assert "name matches no ChEMBL preferred name" in sql


def test_the_top_up_is_deterministic():
    assert "ORDER BY md5(m.chembl_id)" in source_molecules.TOP_UP_SOURCE_MOLECULES


def test_the_top_up_never_repeats_an_existing_source():
    assert "NOT EXISTS" in source_molecules.TOP_UP_SOURCE_MOLECULES


def test_the_deficit_is_topped_up(patched):
    cursor = patched([56, 2, 100])
    selection = source_molecules.build_source_molecules(target_size=100)
    assert selection.from_input == 56
    assert selection.topped_up == 44
    assert selection.rejected == 2
    assert selection.total == 100
    top_up_params = [
        params
        for statement, params in zip(cursor.statements, cursor.params, strict=False)
        if statement == source_molecules.TOP_UP_SOURCE_MOLECULES
    ]
    assert top_up_params == [(44,)]


def test_a_full_set_is_not_topped_up(patched):
    cursor = patched([100, 0, 100])
    selection = source_molecules.build_source_molecules(target_size=100)
    assert selection.topped_up == 0
    assert source_molecules.TOP_UP_SOURCE_MOLECULES not in cursor.statements


def test_an_oversized_input_set_is_left_alone(patched):
    patched([120, 0, 120])
    selection = source_molecules.build_source_molecules(target_size=100)
    assert selection.total == 120 and selection.topped_up == 0
