import asyncio
import logging
import random
from collections.abc import Iterable
from contextlib import suppress
from typing import TYPE_CHECKING

import aiohttp

from qcrawl.core.request import Request
from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult

if TYPE_CHECKING:
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class RetryMiddleware(DownloaderMiddleware):
    """Downloader middleware that retries transient failures with exponential backoff.

    Behavior
        - On network exceptions (aiohttp.ClientError, asyncio.TimeoutError) the middleware
          may return `MiddlewareResult.retry(new_request)` to reschedule the request.
        - On responses whose status code is in `retry_http_codes` the middleware will
          attempt a retry unless `retry_count` meets or exceeds `max_retries`.
        - Retry requests are clones of the original request with `retry_count` incremented,
          `retry_delay` set and `priority` adjusted by `priority_adjust`.
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_http_codes: Iterable[int] | None = None,
        priority_adjust: int = -1,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
        backoff_jitter: float = 0.3,
        enabled: bool = True,
    ) -> None:
        """Initialize RetryMiddleware.

        Args:
            max_retries: maximum retries before giving up (non-negative int).
            retry_http_codes: iterable of HTTP status codes that should be retried.
            priority_adjust: integer to lower priority of retried requests (negative by default).
            backoff_base: base multiplier for exponential backoff in seconds.
            backoff_max: maximum backoff delay in seconds.
            backoff_jitter: jitter fraction applied to computed backoff (0.0 = no jitter).
            enabled: when False, responses/exceptions pass through without retrying.
        """
        self._enabled = bool(enabled)
        self.max_retries = int(max_retries)
        default_codes = {429, 500, 502, 503, 504}
        self.retry_http_codes = (
            set(retry_http_codes) if retry_http_codes is not None else set(default_codes)
        )
        self.priority_adjust = int(priority_adjust)
        self.backoff_base = float(backoff_base)
        self.backoff_max = float(backoff_max)
        self.backoff_jitter = float(backoff_jitter)

    @classmethod
    def from_crawler(cls, crawler):
        """Create from crawler, reading the `MAX_RETRIES`, `RETRY_HTTP_CODES`,
        `RETRY_PRIORITY_ADJUST`, `RETRY_BACKOFF_*` and `RETRY_ENABLED` settings.

        `Settings` validates these (types and ranges), so the values are trusted
        here; the constructor still coerces them for direct (non-settings) use.
        """
        from qcrawl.settings import Settings

        settings: Settings = getattr(crawler, "runtime_settings", None) or Settings()
        return cls(
            max_retries=settings.MAX_RETRIES,
            retry_http_codes=settings.RETRY_HTTP_CODES,
            priority_adjust=settings.RETRY_PRIORITY_ADJUST,
            backoff_base=settings.RETRY_BACKOFF_BASE,
            backoff_max=settings.RETRY_BACKOFF_MAX,
            backoff_jitter=settings.RETRY_BACKOFF_JITTER,
            enabled=settings.RETRY_ENABLED,
        )

    def _dont_retry(self, request: Request) -> bool:
        """True when the request opts out of retries via `meta['dont_retry']`."""
        meta = getattr(request, "meta", None)
        return bool(meta.get("dont_retry")) if isinstance(meta, dict) else False

    def _max_retries_for(self, request: Request) -> int:
        """Per-request retry cap: `meta['max_retry_times']` overrides `max_retries`."""
        meta = getattr(request, "meta", None)
        if isinstance(meta, dict):
            val = meta.get("max_retry_times")
            if isinstance(val, int) and not isinstance(val, bool) and val >= 0:
                return val
        return self.max_retries

    def _compute_delay(self, retry_count: int, header_retry_after: int | None = None) -> float:
        """Compute delay for next retry.

        Uses `Retry-After` header value when provided; otherwise exponential backoff:
            delay = min(backoff_base * (2 ** retry_count), backoff_max)

        Jitter:
            when `backoff_jitter > 0` the returned delay is randomized within
            +/- backoff_jitter fraction of the base delay.

        Args:
            retry_count: number of completed retry attempts for this request.
            header_retry_after: optional Retry-After value from response headers (seconds).

        Returns:
            float seconds to wait before next retry.
        """
        if header_retry_after is not None:
            base_delay = max(0.0, float(header_retry_after))
        else:
            base_delay = min(self.backoff_base * (2**retry_count), self.backoff_max)

        if self.backoff_jitter > 0:
            jitter = self.backoff_jitter * base_delay
            min_delay = max(0.0, base_delay - jitter)
            max_delay = base_delay + jitter
            base_delay = random.uniform(min_delay, max_delay)

        return base_delay

    def _make_retry_request(self, request: Request, retry_delay: float) -> Request:
        new_req: Request = request.copy()

        if new_req.meta is None:
            new_req.meta = {}
        new_req.meta["retry_count"] = self._get_retry_count(request) + 1
        new_req.meta["retry_delay"] = retry_delay

        new_req.priority = request.priority + self.priority_adjust

        return new_req

    def _get_retry_count(self, request: Request) -> int:
        meta = getattr(request, "meta", None)
        if not meta or not isinstance(meta, dict):
            return 0

        if "retry_count" not in meta:
            return 0

        val = meta["retry_count"]
        if not isinstance(val, int) or isinstance(val, bool):
            raise TypeError(f"retry_count must be int, got {type(val).__name__}")
        return val

    async def process_exception(
        self, request: Request, exception: BaseException, spider: "Spider"
    ) -> MiddlewareResult:
        """Handle network exceptions from the downloader.

        - If the exception is transient (aiohttp.ClientError or asyncio.TimeoutError),
          and retry_count < max_retries, schedule a retry with exponential backoff.
        - Otherwise continue or drop as appropriate.

        Returns:
            MiddlewareResult.retry(new_request) on retry,
            MiddlewareResult.drop() when max retries reached,
            MiddlewareResult.continue_() for non-transient exceptions.
        """
        transient = isinstance(exception, (aiohttp.ClientError, asyncio.TimeoutError))
        if not self._enabled or not transient or self._dont_retry(request):
            return MiddlewareResult.continue_()

        max_retries = self._max_retries_for(request)
        retry_count = self._get_retry_count(request)
        if retry_count >= max_retries:
            spider.crawler.stats.inc("downloader/retry/max_reached")
            spider.crawler.stats.inc("downloader/retry/network_error")
            return MiddlewareResult.drop()

        delay = self._compute_delay(retry_count, None)
        spider.crawler.stats.inc("downloader/retry/total")
        spider.crawler.stats.inc("downloader/retry/network_error")
        spider.crawler.stats.inc(f"downloader/retry/network_error/{type(exception).__name__}")
        spider.crawler.stats.inc(f"downloader/retry/attempt/{retry_count + 1}")

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Retrying %s (network error: %s, attempt %d/%d)",
                request.url,
                type(exception).__name__,
                retry_count + 1,
                max_retries,
            )
        return MiddlewareResult.retry(self._make_retry_request(request, delay))

    async def process_response(
        self, request: Request, response: "Page", spider: "Spider"
    ) -> MiddlewareResult:
        """Handle responses that should be retried based on status code.

        - If response.status_code is in `self.retry_http_codes` and retry_count < max_retries:
            compute delay (honouring Retry-After header if present) and return RETRY.
        - If max retries reached: return KEEP to let the response pass through.
        - Otherwise return KEEP.

        Returns:
            MiddlewareResult.retry(new_request) to reschedule,
            MiddlewareResult.keep(response) to pass the response onward.
        """
        status = getattr(response, "status_code", None)
        if not self._enabled or status not in self.retry_http_codes or self._dont_retry(request):
            return MiddlewareResult.keep(response)

        max_retries = self._max_retries_for(request)
        retry_count = self._get_retry_count(request)
        if retry_count >= max_retries:
            spider.crawler.stats.inc("downloader/retry/max_reached")
            spider.crawler.stats.inc("downloader/retry/http_error")
            spider.crawler.stats.inc(f"downloader/retry/http_status/{status}")
            return MiddlewareResult.keep(response)

        retry_after = None
        if hasattr(response, "headers"):
            ra = response.headers.get("Retry-After")
            if ra:
                with suppress(Exception):
                    retry_after = int(ra)

        delay = self._compute_delay(retry_count, retry_after)
        spider.crawler.stats.inc("downloader/retry/total")
        spider.crawler.stats.inc("downloader/retry/http_error")
        spider.crawler.stats.inc(f"downloader/retry/http_status/{status}")
        spider.crawler.stats.inc(f"downloader/retry/attempt/{retry_count + 1}")

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Retrying %s (status %d, attempt %d/%d)",
                request.url,
                status,
                retry_count + 1,
                max_retries,
            )
        return MiddlewareResult.retry(self._make_retry_request(request, delay))

    async def process_request(self, request: Request, spider: "Spider") -> MiddlewareResult:
        """Pre-download hook. RetryMiddleware performs no pre-download actions and returns CONTINUE.

        Kept for middleware contract completeness.
        """
        return MiddlewareResult.continue_()

    async def open_spider(self, spider: "Spider") -> None:
        """Lifecycle hook when spider is opened. Log configured retry parameters."""
        try:
            codes = None
            if self.retry_http_codes is not None:
                try:
                    codes = sorted(self.retry_http_codes)
                except Exception:
                    codes = list(self.retry_http_codes)
        except Exception:
            codes = list(self.retry_http_codes)

        logger.info(
            "max_retries=%d, retry_http_codes=%r, priority_adjust=%d, backoff_base=%.3f, backoff_max=%.3f, backoff_jitter=%.3f",
            self.max_retries,
            codes,
            self.priority_adjust,
            self.backoff_base,
            self.backoff_max,
            self.backoff_jitter,
        )
