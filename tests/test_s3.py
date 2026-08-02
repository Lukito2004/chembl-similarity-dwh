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


def test_similarity_objects_are_named_after_their_source(settings):
    assert (
        s3.similarity_key("CHEMBL25", settings)
        == "final_task/test_user/similarity_scores/CHEMBL25.parquet"
    )


def test_the_similarity_table_is_self_contained():
    import numpy as np
    import pyarrow as pa

    scores = np.array([0.5, 0.25], dtype=np.float32)
    table = s3.similarity_table("CHEMBL25", pa.array(["CHEMBL1", "CHEMBL2"]), scores)
    assert table.column_names == ["source_chembl_id", "target_chembl_id", "tanimoto_score"]
    assert table.column("source_chembl_id").to_pylist() == ["CHEMBL25", "CHEMBL25"]


def test_the_archive_stores_scores_as_float32():
    import numpy as np
    import pyarrow as pa

    scores = np.array([0.7, 0.25], dtype=np.float64)
    table = s3.similarity_table("CHEMBL25", pa.array(["CHEMBL1", "CHEMBL2"]), scores)
    assert table.schema.field("tanimoto_score").type == pa.float32()


def test_stale_keys_are_deleted_in_batches(monkeypatch, settings):
    batches = []

    class StubClient:
        def delete_objects(self, **kwargs):
            batches.append([item["Key"] for item in kwargs["Delete"]["Objects"]])

    monkeypatch.setattr(s3, "s3_client", StubClient)
    keys = [f"prefix/part-{index:05d}.parquet" for index in range(1500)]
    assert s3.delete_keys(keys, settings) == 1500
    assert [len(batch) for batch in batches] == [1000, 500]
    assert batches[0][0] == "prefix/part-00000.parquet"


def test_deleting_nothing_makes_no_request(monkeypatch, settings):
    class StubClient:
        def delete_objects(self, **kwargs):
            raise AssertionError("no request may be made for an empty key list")

    monkeypatch.setattr(s3, "s3_client", StubClient)
    assert s3.delete_keys([], settings) == 0


def test_a_table_survives_the_upload_and_download(monkeypatch, settings):
    import io

    store = {}

    class StubClient:
        def upload_fileobj(self, buffer, bucket, key):
            store[(bucket, key)] = buffer.read()

        def download_fileobj(self, bucket, key, buffer):
            buffer.write(store[(bucket, key)])

    monkeypatch.setattr(s3, "s3_client", StubClient)
    table = s3.fingerprint_table(["CHEMBL1"], [b"\x01" * 256])
    size = s3.write_parquet(table, "some/key.parquet", settings)
    assert size > 0
    restored = s3.read_parquet("some/key.parquet", settings)
    assert restored.column("fingerprint")[0].as_py() == b"\x01" * 256
    assert isinstance(io.BytesIO(), io.BytesIO)
