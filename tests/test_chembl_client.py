import pytest
import requests
import requests_mock

from chembl_sim.chembl.client import (
    ChemblApiError,
    ChemblClient,
    ChemblResource,
    RateLimiter,
    TransientApiError,
)
from chembl_sim.settings import ApiSettings

RESOURCE = ChemblResource(name="widget", collection="widgets", fields=("a", "b"))
URL = "https://api.test/data/widget.json"


@pytest.fixture
def settings():
    # requests_per_second of zero disables sleeping so the tests stay fast.
    return ApiSettings(
        base_url="https://api.test/data",
        page_size=2,
        timeout_seconds=5,
        max_retries=3,
        requests_per_second=0,
        max_workers=2,
    )


@pytest.fixture
def client(settings):
    session = requests.Session()
    with requests_mock.Mocker(session=session) as mocker:
        api = ChemblClient(settings=settings, session=session)
        api.mocker = mocker
        yield api


def page(*ids):
    return {"widgets": [{"id": i} for i in ids], "page_meta": {"total_count": 5}}


def test_page_request_carries_limit_offset_and_only(client):
    client.mocker.get(URL, json=page(1, 2))
    client.fetch_page(RESOURCE, offset=4)
    query = client.mocker.last_request.qs
    assert query["limit"] == ["2"]
    assert query["offset"] == ["4"]
    assert query["only"] == ["a,b"]


def test_records_are_read_from_the_collection_key(client):
    client.mocker.get(URL, json=page(7, 8))
    assert client.fetch_page(RESOURCE, 0) == [{"id": 7}, {"id": 8}]


def test_server_error_is_retried_then_succeeds(client):
    client.mocker.get(URL, [{"status_code": 500}, {"json": page(1), "status_code": 200}])
    assert client.fetch_page(RESOURCE, 0) == [{"id": 1}]
    assert client.mocker.call_count == 2


def test_rate_limit_is_retried(client):
    client.mocker.get(URL, [{"status_code": 429}, {"json": page(1), "status_code": 200}])
    assert client.fetch_page(RESOURCE, 0) == [{"id": 1}]


def test_exhausted_retries_raise(client):
    client.mocker.get(URL, status_code=503)
    with pytest.raises(TransientApiError):
        client.fetch_page(RESOURCE, 0)
    assert client.mocker.call_count == 3


def test_client_error_is_not_retried(client):
    client.mocker.get(URL, status_code=404)
    with pytest.raises(ChemblApiError):
        client.fetch_page(RESOURCE, 0)
    assert client.mocker.call_count == 1


def test_timeout_is_treated_as_transient(client):
    client.mocker.get(URL, exc=requests.exceptions.ConnectTimeout)
    with pytest.raises(TransientApiError):
        client.fetch_page(RESOURCE, 0)


def test_total_count_comes_from_page_meta(client):
    client.mocker.get(URL, json=page(1))
    assert client.total_count(RESOURCE) == 5


def test_release_is_read_from_the_status_document(client):
    client.mocker.get("https://api.test/data/status.json", json={"chembl_db_version": "ChEMBL_37"})
    assert client.chembl_release() == "ChEMBL_37"


def test_batches_cover_every_offset_and_stop_at_the_total(client):
    client.mocker.get(URL, json=page(1, 2))
    batches = list(client.iter_batches(RESOURCE, start_offset=0, total_count=5))
    assert [b.next_offset for b in batches] == [4, 5]
    assert len(batches[0].records) == 4


def test_batches_resume_from_the_given_offset(client):
    client.mocker.get(URL, json=page(1, 2))
    batches = list(client.iter_batches(RESOURCE, start_offset=4, total_count=5))
    assert [b.next_offset for b in batches] == [5]


def test_rate_limiter_spaces_calls(monkeypatch):
    clock = {"now": 0.0}
    slept = []
    monkeypatch.setattr("chembl_sim.chembl.client.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("chembl_sim.chembl.client.time.sleep", slept.append)
    limiter = RateLimiter(calls_per_second=2)
    limiter.acquire()
    limiter.acquire()
    assert slept == [0.5]


def test_rate_limiter_is_disabled_at_zero(monkeypatch):
    slept = []
    monkeypatch.setattr("chembl_sim.chembl.client.time.sleep", slept.append)
    RateLimiter(calls_per_second=0).acquire()
    assert slept == []
