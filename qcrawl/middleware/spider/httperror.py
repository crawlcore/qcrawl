import logging
from typing import TYPE_CHECKING

from qcrawl.middleware.base import SpiderMiddleware

if TYPE_CHECKING:
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class IgnoreResponse(Exception):
    """Raised/returned by spider middlewares to indicate a response should be ignored."""

    pass


class HttpErrorMiddleware(SpiderMiddleware):
    """Filter responses with HTTP error status codes.

    Features:
        - Configurable allowed status codes
        - Per-spider configuration via `HTTPERROR_ALLOW_ALL` or `HTTPERROR_ALLOWED_CODES`
        - Stats collection for filtered responses (emits `request_dropped` signal)
        - Clear logging of configured ranges
    """

    DEFAULT_ALLOWED_CODES = list(range(200, 400))

    def __init__(self, allowed_codes: list[int] | None = None) -> None:
        self.default_allowed_codes = allowed_codes or self.DEFAULT_ALLOWED_CODES
        self._filtered_count = 0

    def _get_allowed_codes(self, spider: "Spider") -> set[int] | None:
        if getattr(spider, "HTTPERROR_ALLOW_ALL", False):
            return None

        allowed = getattr(spider, "HTTPERROR_ALLOWED_CODES", None)
        if allowed is not None:
            if isinstance(allowed, (list, tuple, set)):
                return set(allowed)
            return {allowed}

        return set(self.default_allowed_codes)

    def _should_filter(self, response: "Page", allowed_codes: set[int] | None) -> bool:
        if allowed_codes is None:
            return False
        status = getattr(response, "status_code", None)
        if status is None:
            return False
        return status not in allowed_codes

    async def process_spider_input(self, response: "Page", spider: "Spider") -> Exception | None:
        allowed_codes = self._get_allowed_codes(spider)

        if self._should_filter(response, allowed_codes):
            status = getattr(response, "status_code", None)
            self._filtered_count += 1

            logger.debug("Ignoring response %s from %s", status, getattr(response, "url", None))

            try:
                dispatcher = getattr(spider, "signals", None)
                if dispatcher is not None:
                    # dispatcher is already bound to `spider` as the sender;
                    # do not pass `spider` as a positional arg (or duplicate via kw).
                    await dispatcher.send_async(
                        "request_dropped",
                        request=getattr(response, "request", None),
                        exception=None,
                    )
            except Exception:
                logger.exception(
                    "Error sending request_dropped signal for %s", getattr(response, "url", None)
                )

            return IgnoreResponse(f"Ignored response {status} by HttpErrorMiddleware")

        return None

    async def open_spider(self, spider: "Spider") -> None:
        """Log configuration when spider opens (lifecycle hook)."""
        allowed_codes = self._get_allowed_codes(spider)
        if allowed_codes is None:
            logger.info("all status codes allowed")
        elif not allowed_codes:
            logger.info("no status codes allowed (will drop all responses)")
        else:
            # join integers safely
            logger.info(
                "allowed status codes: %s", ", ".join(str(c) for c in sorted(allowed_codes))
            )

        if logger.isEnabledFor(logging.DEBUG) and allowed_codes:
            sorted_codes = sorted(allowed_codes)
            ranges: list[str] = []
            start = sorted_codes[0]
            prev = start

            for code in sorted_codes[1:] + [None]:
                if code is None or code != prev + 1:
                    if start == prev:
                        ranges.append(str(start))
                    else:
                        ranges.append(f"{start}-{prev}")
                    if code is not None:
                        start = code
                if code is not None:
                    prev = code

            logger.debug("HttpErrorMiddleware: allowed_codes=[%s]", ", ".join(ranges))

    async def close_spider(self, spider: "Spider") -> None:
        """Log statistics when spider closes (lifecycle hook)."""
        if self._filtered_count > 0:
            spider.crawler.stats.set("httperror/filtered", self._filtered_count)
