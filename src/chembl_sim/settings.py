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
    s3: S3Settings
    fingerprint: FingerprintSettings
    source: SourceSettings
    similarity: SimilaritySettings

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            dwh=DwhSettings.from_env(),
            api=ApiSettings.from_env(),
            dump=DumpSettings.from_env(),
            s3=S3Settings.from_env(),
            fingerprint=FingerprintSettings.from_env(),
            source=SourceSettings.from_env(),
            similarity=SimilaritySettings.from_env(),
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


@dataclass(frozen=True)
class S3Settings:
    """Bucket and prefixes. Prefixes are stored without surrounding slashes."""

    bucket: str
    root_prefix: str

    @classmethod
    def from_env(cls) -> S3Settings:
        return cls(
            bucket=env_required("CHEMBL_S3_BUCKET"),
            root_prefix=env_required("CHEMBL_S3_ROOT_PREFIX").strip("/"),
        )


@dataclass(frozen=True)
class FingerprintSettings:
    """Morgan parameters. radius and n_bits are fixed by the task specification."""

    radius: int
    n_bits: int
    shard_size: int

    def __post_init__(self) -> None:
        if self.n_bits % 8:
            raise ValueError(f"CHEMBL_FP_N_BITS must be a multiple of 8, got {self.n_bits}")

    @property
    def n_bytes(self) -> int:
        """Packed width of one fingerprint. 2048 bits fit into 256 bytes."""
        return self.n_bits // 8

    @classmethod
    def from_env(cls) -> FingerprintSettings:
        return cls(
            radius=env_integer("CHEMBL_FP_RADIUS", 2),
            n_bits=env_integer("CHEMBL_FP_N_BITS", 2048),
            shard_size=env_integer("CHEMBL_FP_SHARD_SIZE", 250_000),
        )


@dataclass(frozen=True)
class SourceSettings:
    """Where the personal input files live and how large the source set should be."""

    input_prefix: str
    target_size: int

    @classmethod
    def from_env(cls) -> SourceSettings:
        return cls(
            input_prefix=env_required("CHEMBL_INPUT_PREFIX").strip("/"),
            target_size=env_integer("CHEMBL_SOURCE_TARGET_SIZE", 100),
        )


@dataclass(frozen=True)
class SimilaritySettings:
    """Top-N size and the row block that bounds peak memory during scoring."""

    top_n: int
    block_size: int

    @classmethod
    def from_env(cls) -> SimilaritySettings:
        return cls(
            top_n=env_integer("CHEMBL_SIMILARITY_TOP_N", 10),
            block_size=env_integer("CHEMBL_SIMILARITY_BLOCK_SIZE", 250_000),
        )
