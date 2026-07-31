"""Typed readers for environment configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


class MissingSettingError(RuntimeError):
    """A setting has no value and no safe default."""


def env_required(name: str) -> str:
    """Read a setting that has no sensible default. Whitespace counts as missing."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise MissingSettingError(f"environment variable {name} is not set")
    return value


def env_text(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def env_integer(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


def env_decimal(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


@dataclass(frozen=True)
class DwhSettings:
    """Warehouse connection. Schema names are fixed in the DDL, not configurable."""

    dsn: str

    @classmethod
    def from_env(cls) -> DwhSettings:
        return cls(dsn=env_required("CHEMBL_DWH_DSN"))


@dataclass(frozen=True)
class Settings:
    """Everything the pipeline reads from the environment."""

    dwh: DwhSettings

    @classmethod
    def from_env(cls) -> Settings:
        return cls(dwh=DwhSettings.from_env())


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings. Call get_settings.cache_clear() after changing the environment."""
    return Settings.from_env()
