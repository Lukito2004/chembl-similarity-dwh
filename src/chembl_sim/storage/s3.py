"""S3 object storage: key layout and parquet transfer."""

from __future__ import annotations

import io
from collections.abc import Sequence

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import S3Settings, get_settings

log = get_logger(__name__)

FINGERPRINTS_FOLDER = "fingerprints"


def s3_client():
    """A client using the standard credential chain, which resolves the SSO profile."""
    return boto3.Session().client("s3")


def fingerprints_prefix(settings: S3Settings | None = None) -> str:
    settings = settings or get_settings().s3
    return f"{settings.root_prefix}/{FINGERPRINTS_FOLDER}"


def fingerprint_shard_key(index: int, settings: S3Settings | None = None) -> str:
    """Deterministic key, so a retried shard overwrites rather than duplicates."""
    return f"{fingerprints_prefix(settings)}/part-{index:05d}.parquet"


def fingerprint_table(chembl_ids: Sequence[str], fingerprints: Sequence[bytes]) -> pa.Table:
    """Two columns: the identifier and the packed fingerprint as raw bytes."""
    return pa.table(
        {
            "chembl_id": pa.array(chembl_ids, pa.string()),
            "fingerprint": pa.array(fingerprints, pa.binary()),
        }
    )


def write_parquet(table: pa.Table, key: str, settings: S3Settings | None = None) -> int:
    """Serialise in memory and upload. Returns the object size in bytes."""
    settings = settings or get_settings().s3
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    size = buffer.tell()
    buffer.seek(0)
    s3_client().upload_fileobj(buffer, settings.bucket, key)
    log.info("Wrote s3://%s/%s at %.1f MB", settings.bucket, key, size / 1e6)
    return size


def read_parquet(key: str, settings: S3Settings | None = None) -> pa.Table:
    settings = settings or get_settings().s3
    buffer = io.BytesIO()
    s3_client().download_fileobj(settings.bucket, key, buffer)
    buffer.seek(0)
    return pq.read_table(buffer)


def list_keys(prefix: str, settings: S3Settings | None = None) -> list[str]:
    settings = settings or get_settings().s3
    paginator = s3_client().get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=settings.bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return sorted(keys)


def delete_prefix(prefix: str, settings: S3Settings | None = None) -> int:
    """Clear a prefix so a rebuild cannot leave stale shards behind."""
    settings = settings or get_settings().s3
    keys = list_keys(prefix, settings)
    client = s3_client()
    for start in range(0, len(keys), 1000):
        batch = keys[start : start + 1000]
        client.delete_objects(
            Bucket=settings.bucket, Delete={"Objects": [{"Key": k} for k in batch]}
        )
    log.info("Deleted %s objects under %s", len(keys), prefix)
    return len(keys)


def read_text(key: str, settings: S3Settings | None = None) -> str:
    """Fetch a small text object, tolerating the odd non-UTF-8 byte."""
    settings = settings or get_settings().s3
    body = s3_client().get_object(Bucket=settings.bucket, Key=key)["Body"].read()
    return body.decode("utf-8", errors="replace")
