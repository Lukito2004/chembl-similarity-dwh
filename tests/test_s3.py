import io

import pyarrow.parquet as pq
import pytest

from chembl_sim.settings import S3Settings
from chembl_sim.storage import s3


@pytest.fixture
def settings():
    return S3Settings(bucket="test-bucket", root_prefix="final_task/test_user")


def test_shard_keys_are_zero_padded_and_deterministic(settings):
    assert (
        s3.fingerprint_shard_key(7, settings)
        == "final_task/test_user/fingerprints/part-00007.parquet"
    )
    assert s3.fingerprint_shard_key(7, settings) == s3.fingerprint_shard_key(7, settings)


def test_everything_lives_under_the_root_prefix(settings):
    assert s3.fingerprints_prefix(settings).startswith("final_task/test_user/")


def test_the_table_carries_identifiers_and_raw_bytes():
    table = s3.fingerprint_table(["CHEMBL25", "CHEMBL545"], [b"\x01" * 256, b"\x02" * 256])
    assert table.column_names == ["chembl_id", "fingerprint"]
    assert table.num_rows == 2
    assert table.column("fingerprint")[0].as_py() == b"\x01" * 256


def test_fingerprints_survive_a_parquet_round_trip():
    original = [bytes([i % 256]) * 256 for i in range(50)]
    table = s3.fingerprint_table([f"CHEMBL{i}" for i in range(50)], original)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    buffer.seek(0)
    restored = pq.read_table(buffer)
    assert restored.column("fingerprint").to_pylist() == original
