import pytest

from chembl_sim.settings import (
    MissingSettingError,
    env_decimal,
    env_integer,
    env_required,
    env_text,
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
