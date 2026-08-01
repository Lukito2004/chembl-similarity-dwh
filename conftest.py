"""Shared pytest fixtures."""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager

import pytest

# Airflow reads these at import time; a throwaway home keeps DAG tests off the real database.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-test-"))
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from chembl_sim.settings import get_settings

BASE_ENV = {
    "CHEMBL_DWH_DSN": "postgresql://test:test@localhost:5432/test",
    "CHEMBL_S3_BUCKET": "test-bucket",
    "CHEMBL_S3_ROOT_PREFIX": "final_task/test_user",
    "CHEMBL_INPUT_PREFIX": "input/test-user",
}


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    """Tests never inherit CHEMBL_ variables from the host machine."""
    for key in list(os.environ):
        if key.startswith("CHEMBL_"):
            monkeypatch.delenv(key, raising=False)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class FakeCursor:
    """Records every statement and returns queued results in order."""

    def __init__(self, results=None):
        self.statements = []
        self.params = []
        self._results = list(results or [])

    def execute(self, statement, params=None):
        self.statements.append(str(statement))
        self.params.append(params)

    def fetchone(self):
        return self._results.pop(0) if self._results else None

    def fetchall(self):
        return self._results.pop(0) if self._results else []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_object = cursor
        self.commits = 0

    def cursor(self):
        return self.cursor_object

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


@pytest.fixture
def fake_warehouse(monkeypatch):
    """Replace warehouse_connection inside a module and hand back the fake connection."""

    def install(module, results=None):
        connection = FakeConnection(FakeCursor(results))

        @contextmanager
        def fake_connection(dsn=None):
            yield connection

        monkeypatch.setattr(module, "warehouse_connection", fake_connection)
        return connection

    return install
