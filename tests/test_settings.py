import pytest

from chembl_sim.settings import (
    MissingSettingError,
    Settings,
    env_decimal,
    env_integer,
    env_required,
    env_text,
    get_settings,
)


def test_required_returns_the_value(monkeypatch):
    monkeypatch.setenv("CHEMBL_TEST", "value")
    assert env_required("CHEMBL_TEST") == "value"


def test_required_names_the_missing_variable():
    with pytest.raises(MissingSettingError, match="CHEMBL_TEST"):
        env_required("CHEMBL_TEST")


def test_required_treats_whitespace_as_missing(monkeypatch):
    monkeypatch.setenv("CHEMBL_TEST", "   ")
    with pytest.raises(MissingSettingError):
        env_required("CHEMBL_TEST")


def test_text_falls_back_to_the_default():
    assert env_text("CHEMBL_TEST", "fallback") == "fallback"


def test_text_strips_surrounding_whitespace(monkeypatch):
    monkeypatch.setenv("CHEMBL_TEST", "  value  ")
    assert env_text("CHEMBL_TEST", "fallback") == "value"


def test_integer_parses_a_value_and_falls_back(monkeypatch):
    assert env_integer("CHEMBL_TEST", 7) == 7
    monkeypatch.setenv("CHEMBL_TEST", "42")
    assert env_integer("CHEMBL_TEST", 7) == 42


def test_integer_rejects_a_non_numeric_value(monkeypatch):
    monkeypatch.setenv("CHEMBL_TEST", "abc")
    with pytest.raises(ValueError):
        env_integer("CHEMBL_TEST", 7)


def test_decimal_parses_a_value_and_falls_back(monkeypatch):
    assert env_decimal("CHEMBL_TEST", 1.5) == 1.5
    monkeypatch.setenv("CHEMBL_TEST", "6")
    assert env_decimal("CHEMBL_TEST", 1.5) == 6.0


def test_warehouse_dsn_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("CHEMBL_DWH_DSN", "postgresql://u:p@h:5432/d")
    assert Settings.from_env().dwh.dsn == "postgresql://u:p@h:5432/d"


def test_missing_warehouse_dsn_is_reported(monkeypatch):
    monkeypatch.delenv("CHEMBL_DWH_DSN")
    with pytest.raises(MissingSettingError, match="CHEMBL_DWH_DSN"):
        Settings.from_env()


def test_get_settings_returns_a_cached_instance():
    assert get_settings() is get_settings()
