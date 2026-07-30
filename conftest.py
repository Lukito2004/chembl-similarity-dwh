"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    """Tests never inherit CHEMBL_ variables from the host machine."""
    for key in list(os.environ):
        if key.startswith("CHEMBL_"):
            monkeypatch.delenv(key, raising=False)
