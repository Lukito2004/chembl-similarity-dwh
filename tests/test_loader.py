import pytest

from chembl_sim.chembl import loader
from chembl_sim.chembl.records import MOLECULE


class FakeClient:
    def __init__(self, total=0, batches=()):
        self.total = total
        self.batches = list(batches)
        self.total_count_calls = 0

    def total_count(self, resource):
        self.total_count_calls += 1
        return self.total

    def iter_batches(self, resource, start_offset=0, total_count=None):
        self.start_offset = start_offset
        yield from self.batches


class Batch:
    def __init__(self, records, next_offset):
        self.records = records
        self.next_offset = next_offset


@pytest.fixture
def no_real_upserts(monkeypatch):
    calls = []
    monkeypatch.setattr(loader, "upsert_rows", lambda *args, **kwargs: calls.append(args) or 1)
    return calls


def watermark_row(release="ChEMBL_37", offset=0, complete=False):
    return ("molecule", release, offset, 100, complete)


def test_a_complete_release_is_not_ingested_again(fake_warehouse, no_real_upserts):
    fake_warehouse(loader, results=[watermark_row(complete=True)])
    client = FakeClient()
    assert loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client) == 0
    assert client.total_count_calls == 0


def test_force_ingests_a_complete_release(fake_warehouse, no_real_upserts):
    fake_warehouse(loader, results=[watermark_row(complete=True)])
    client = FakeClient(total=0)
    loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client, force=True)
    assert client.total_count_calls == 1


def test_a_new_release_restarts_from_zero(fake_warehouse, no_real_upserts):
    fake_warehouse(loader, results=[watermark_row(release="ChEMBL_36", offset=900)])
    client = FakeClient(total=0)
    loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client)
    assert client.start_offset == 0


def test_the_same_release_resumes_from_the_watermark(fake_warehouse, no_real_upserts):
    fake_warehouse(loader, results=[watermark_row(offset=900)])
    client = FakeClient(total=1000)
    loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client)
    assert client.start_offset == 900


def test_an_absent_watermark_starts_from_zero(fake_warehouse, no_real_upserts):
    fake_warehouse(loader, results=[None])
    client = FakeClient(total=0)
    loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client)
    assert client.start_offset == 0


def test_every_batch_is_loaded_and_committed(fake_warehouse, no_real_upserts):
    batches = [
        Batch([{"molecule_chembl_id": "CHEMBL1"}], 1),
        Batch([{"molecule_chembl_id": "CHEMBL2"}], 2),
    ]
    connection = fake_warehouse(loader, results=[None])
    client = FakeClient(total=2, batches=batches)
    loaded = loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client)
    assert loaded == 2
    # One commit for the watermark read, then one per batch, so progress survives a crash.
    assert connection.commits >= len(batches)


def test_the_watermark_is_written_after_each_batch(fake_warehouse, no_real_upserts):
    connection = fake_warehouse(loader, results=[None])
    client = FakeClient(total=1, batches=[Batch([{"molecule_chembl_id": "CHEMBL1"}], 1)])
    loader.ingest_resource(MOLECULE, "ChEMBL_37", client=client)
    written = [s for s in connection.cursor_object.statements if "meta.ingest_watermark" in s]
    assert any("INSERT INTO" in s for s in written)
