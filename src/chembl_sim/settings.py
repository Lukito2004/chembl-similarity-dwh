"""Typed readers for environment configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

DEFAULT_API_BASE_URL = "https://www.ebi.ac.uk/chembl/api/data"
DEFAULT_DUMP_BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest"
API_MAX_PAGE_SIZE = 1000


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
    api: ApiSettings
    dump: DumpSettings

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            dwh=DwhSettings.from_env(),
            api=ApiSettings.from_env(),
            dump=DumpSettings.from_env(),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings. Call get_settings.cache_clear() after changing the environment."""
    return Settings.from_env()


@dataclass(frozen=True)
class ApiSettings:
    """ChEMBL REST client tuning. Endpoint latency varies by an order of magnitude."""

    base_url: str
    page_size: int
    timeout_seconds: int
    max_retries: int
    requests_per_second: float
    max_workers: int

    @classmethod
    def from_env(cls) -> ApiSettings:
        return cls(
            base_url=env_text("CHEMBL_API_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/"),
            page_size=min(
                env_integer("CHEMBL_API_PAGE_SIZE", API_MAX_PAGE_SIZE), API_MAX_PAGE_SIZE
            ),
            timeout_seconds=env_integer("CHEMBL_API_TIMEOUT_SECONDS", 180),
            max_retries=env_integer("CHEMBL_API_MAX_RETRIES", 5),
            requests_per_second=env_decimal("CHEMBL_API_REQUESTS_PER_SECOND", 6.0),
            max_workers=env_integer("CHEMBL_API_MAX_WORKERS", 6),
        )


@dataclass(frozen=True)
class DumpSettings:
    """Release dump download and restore."""

    base_url: str
    download_dir: Path
    chunk_size: int
    timeout_seconds: int

    @classmethod
    def from_env(cls) -> DumpSettings:
        return cls(
            base_url=env_text("CHEMBL_DUMP_BASE_URL", DEFAULT_DUMP_BASE_URL).rstrip("/"),
            download_dir=Path(env_text("CHEMBL_DUMP_DIR", "/opt/airflow/data")),
            chunk_size=env_integer("CHEMBL_DUMP_CHUNK_SIZE", 8 * 1024 * 1024),
            timeout_seconds=env_integer("CHEMBL_DUMP_TIMEOUT_SECONDS", 300),
        )
