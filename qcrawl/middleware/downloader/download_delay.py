import asyncio
import logging
import time
from typing import TYPE_CHECKING

from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult
from qcrawl.utils.url import get_domain

if TYPE_CHECKING:
    from qcrawl.core.request import Request
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class DownloadDelayMiddleware(DownloaderMiddleware):
    """Per-domain minimum inter-request delay middleware.

    Arguments:
        delay_per_domain: minimum seconds between requests to same domain

    Notes:
      - Honors per-request `request.meta['retry_delay']` when present.
      - Records last-download timestamp on response/exception.
    """

    def __init__(self, delay_per_domain: float = 0.25) -> None:
        try:
            d = float(delay_per_domain)
        except Exception:
            raise TypeError("delay_per_domain must be a number") from None
        if d < 0:
            raise ValueError("delay_per_domain must be >= 0")
        self._delay = d
        self._last: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _domain_key(self, url: str) -> str:
        try:
            d = get_domain(url)
            return d or "default"
        except Exception:
            return "default"

    def _get_lock(self, domain_key: str) -> asyncio.Lock:
        """Get or create a lock for the given domain."""
        if domain_key not in self._locks:
            self._locks[domain_key] = asyncio.Lock()
        return self._locks[domain_key]

    async def process_request(self, request: "Request", spider: "Spider") -> MiddlewareResult:
        # Determine effective delay (global vs per-request retry_delay)
        request_delay = 0.0
        try:
            meta = getattr(request, "meta", None)
            if isinstance(meta, dict):
                rd = meta.get("retry_delay")
                if isinstance(rd, (int, float)) and not isinstance(rd, bool):
                    request_delay = float(rd)
        except Exception:
            request_delay = 0.0

        effective = max(self._delay, request_delay or 0.0)
        if effective > 0:
            key = self._domain_key(request.url)
            lock = self._get_lock(key)

            # Use lock to serialize delay checking per domain (prevents race conditions)
            async with lock:
                last = self._last.get(key, 0.0)
                elapsed = time.monotonic() - last
                if elapsed < effective:
                    await asyncio.sleep(effective - elapsed)

        # mark which domain we used so response/exception can update timestamp
        if getattr(request, "meta", None) is None:
            request.meta = {}
        request.meta["_domain_delay_key"] = self._domain_key(request.url)
        return MiddlewareResult.continue_()

    async def process_response(
        self, request: "Request", response, spider: "Spider"
    ) -> MiddlewareResult:
        meta = getattr(request, "meta", None)
        slot_key = None
        if isinstance(meta, dict):
            slot_key = meta.pop("_domain_delay_key", None)

        if isinstance(slot_key, str):
            try:
                self._last[slot_key] = time.monotonic()
            except Exception:
                logger.exception("Failed to update last-download time for %s", slot_key)
        return MiddlewareResult.keep(response)

    async def process_exception(
        self, request: "Request", exception: BaseException, spider: "Spider"
    ) -> MiddlewareResult:
        meta = getattr(request, "meta", None)
        slot_key = None
        if isinstance(meta, dict):
            slot_key = meta.pop("_domain_delay_key", None)

        if isinstance(slot_key, str):
            try:
                self._last[slot_key] = time.monotonic()
            except Exception:
                logger.exception("Failed to update last-download time %s on exception", slot_key)
        return MiddlewareResult.continue_()

    async def open_spider(self, spider: "Spider") -> None:
        self._last.clear()
        self._locks.clear()
        logger.info("delay_per_domain: %.3f seconds", self._delay)
