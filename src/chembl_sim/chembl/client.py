"""ChEMBL REST client with rate limiting, retries and concurrent paging."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from chembl_sim.logging_setup import get_logger
from chembl_sim.settings import ApiSettings, get_settings

log = get_logger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class ChemblApiError(RuntimeError):
    """The API returned a response the client cannot use."""


class TransientApiError(ChemblApiError):
    """A rate limit, server error or timeout that is worth retrying."""


@dataclass(frozen=True)
class ChemblResource:
    """An endpoint, the key its records live under, and the fields to request."""

    name: str
    collection: str
    fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecordBatch:
    """Records from one window of pages, and the offset a resumed run starts at."""

    records: list[dict]
    next_offset: int


STATUS_RESOURCE = ChemblResource(name="status", collection="")


class RateLimiter:
    """Spaces calls across threads so the client stays under a fixed request rate."""

    def __init__(self, calls_per_second: float):
        self._min_interval = 1.0 / calls_per_second if calls_per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        """Reserve the next slot under the lock, then sleep outside it."""
        if not self._min_interval:
            return
        with self._lock:
            now = time.monotonic()
            wait_for = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self._min_interval
        if wait_for:
            time.sleep(wait_for)


class ChemblClient:
    """Pages a ChEMBL endpoint, fetching a window of pages concurrently."""

    def __init__(
        self,
        settings: ApiSettings | None = None,
        session: requests.Session | None = None,
    ):
        self.settings = settings or get_settings().api
        self.session = session or requests.Session()
        self.rate_limiter = RateLimiter(self.settings.requests_per_second)

    def _retrying(self) -> Retrying:
        return Retrying(
            retry=retry_if_exception_type(TransientApiError),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            stop=stop_after_attempt(self.settings.max_retries),
            reraise=True,
        )

    def _get(self, resource: ChemblResource, params: dict) -> dict:
        url = f"{self.settings.base_url}/{resource.name}.json"
        self.rate_limiter.acquire()
        try:
            response = self.session.get(url, params=params, timeout=self.settings.timeout_seconds)
        except requests.RequestException as exc:
            raise TransientApiError(f"{resource.name} request failed: {exc}") from exc
        if response.status_code in RETRYABLE_STATUS:
            raise TransientApiError(f"{resource.name} returned HTTP {response.status_code}")
        if response.status_code >= 400:
            raise ChemblApiError(f"{resource.name} returned HTTP {response.status_code}")
        return response.json()

    def chembl_release(self) -> str:
        """The release the API currently serves, such as ChEMBL_37."""
        payload = self._retrying()(self._get, STATUS_RESOURCE, {})
        return str(payload["chembl_db_version"])

    def total_count(self, resource: ChemblResource) -> int:
        """Record count the endpoint reports, used to size the paging plan."""
        payload = self._retrying()(self._get, resource, {"limit": 1})
        return int(payload["page_meta"]["total_count"])

    def fetch_page(self, resource: ChemblResource, offset: int) -> list[dict]:
        """One page of records, retrying transient failures with exponential backoff."""
        params: dict = {"limit": self.settings.page_size, "offset": offset}
        if resource.fields:
            params["only"] = ",".join(resource.fields)
        payload = self._retrying()(self._get, resource, params)
        return payload.get(resource.collection, [])

    def iter_batches(
        self,
        resource: ChemblResource,
        start_offset: int = 0,
        total_count: int | None = None,
    ) -> Iterator[RecordBatch]:
        """Yield one window of pages at a time, fetched concurrently.

        Progress advances a whole window at once, so an interrupted run redoes at most one.
        """
        total = total_count if total_count is not None else self.total_count(resource)
        page_size = self.settings.page_size
        window = max(1, self.settings.max_workers)
        offset = start_offset
        fetch = partial(self.fetch_page, resource)

        with ThreadPoolExecutor(max_workers=window) as pool:
            while offset < total:
                stop = min(offset + window * page_size, total)
                offsets = list(range(offset, stop, page_size))
                pages = list(pool.map(fetch, offsets))
                records = [record for page in pages for record in page]
                offset = min(offsets[-1] + page_size, total)
                yield RecordBatch(records=records, next_offset=offset)
