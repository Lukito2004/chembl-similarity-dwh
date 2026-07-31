"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest

from chembl_sim.settings import get_settings

BASE_ENV = {
    "CHEMBL_DWH_DSN": "postgresql://test:test@localhost:5432/test",
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
