import logging
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from qcrawl.middleware.base import SpiderMiddleware

if TYPE_CHECKING:
    from qcrawl.core.item import Item
    from qcrawl.core.request import Request
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class OffsiteMiddleware(SpiderMiddleware):
    """Filter requests to URLs outside allowed domains.

    Features:
        - Configurable allowed domains per spider (`allowed_domains` or auto-extract from `start_urls`)
        - Automatic domain extraction and normalization (strip ports, lowercase)
        - Subdomain support (example.com allows api.example.com)
        - Accepts `Request` objects and `str` URLs from spider output (converts `str` to `Request`)
        - Emits `request_dropped` signal for stats when a request is filtered
    """

    def __init__(self) -> None:
        self._dropped_count = 0

    def _normalize_domain(self, netloc: str) -> str:
        """Lowercase and remove port from netloc."""
        if not netloc:
            return ""
        host = netloc.split(":", 1)[0] if ":" in netloc else netloc
        return host.lower()

    def _get_allowed_domains(self, spider: "Spider") -> set[str] | None:
        """Return a set of allowed domains (normalized), or None to allow all.
        Reads the spider's `allowed_domains` attribute (str/list/tuple/set). If
        not set, extracts domains from `start_urls`.
        """
        allowed = getattr(spider, "allowed_domains", None)
        if allowed is not None:
            if not isinstance(allowed, (str, list, tuple, set)):
                raise TypeError("allowed_domains must be a str or a collection of str")
            if isinstance(allowed, (list, tuple, set)):
                # Empty collection -> None (allow all), same as "" and unset.
                return {self._normalize_domain(d) for d in allowed if d} or None
            return {self._normalize_domain(allowed)} if allowed else None

        start_urls = getattr(spider, "start_urls", [])
        if not start_urls:
            return None

        domains: set[str] = set()
        for url in start_urls:
            parsed = urlparse(url)
            if parsed.netloc:
                domains.add(self._normalize_domain(parsed.netloc))

        return domains if domains else None

    def _extract_domain(self, url: str) -> str | None:
        """Extract normalized domain from URL, or None for invalid URLs."""
        try:
            parsed = urlparse(url)
            if not parsed.netloc:
                return None
            return self._normalize_domain(parsed.netloc)
        except Exception:
            return None

    def _is_offsite(self, url: str, allowed_domains: set[str]) -> bool:
        """Return True if URL is offsite (not allowed).

        A URL is on-site when its host exactly matches an allowed domain or is a
        subdomain of one. Subdomain matching only applies to multi-label allowed
        domains (those containing a dot), so a single-label entry such as
        ``"com"`` matches by exact host only and cannot act as a public suffix
        that admits every host beneath it.

        Restricting to a subdomain (e.g. ``"api.example.com"``) does NOT admit
        the parent ``example.com`` or sibling subdomains.
        """
        domain = self._extract_domain(url)
        if not domain:
            return True

        if domain in allowed_domains:
            return False

        for allowed in allowed_domains:
            # Only treat `allowed` as a parent for subdomain matching when it has
            # at least two labels; this prevents matching arbitrary hosts under a
            # public suffix (e.g. allowed="com" must not admit "evil.com").
            if "." in allowed and domain.endswith(f".{allowed}"):
                return False

        return True

    def _is_request(self, item: object) -> bool:
        """Heuristic check for Request instances without importing at module level."""
        return hasattr(item, "url") and hasattr(item, "meta")

    async def _emit_offsite_drop(
        self,
        url: str,
        request: "Request | None",
        spider: "Spider",
        allowed_domains: set[str],
    ) -> None:
        """Record and signal that an offsite URL was filtered out.

        Shared by the Request and str branches of `process_spider_output`; pass
        the dropped `Request` (or `None` for a bare URL string).
        """
        self._dropped_count += 1

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Filtered offsite request to %s: %s (allowed: %s)",
                self._extract_domain(url),
                url,
                ", ".join(sorted(allowed_domains)),
            )

        dispatcher = getattr(spider, "signals", None)
        try:
            if dispatcher is not None:
                await dispatcher.send_async("request_dropped", request=request, exception=None)
        except Exception:
            logger.exception("Error sending request_dropped signal for %s", url)

    async def process_spider_output(
        self,
        response: "Page",
        result: "AsyncGenerator[Item | Request | str, None]",
        spider: "Spider",
    ) -> AsyncGenerator["Item | Request | str", None]:
        """Filter offsite requests from spider output.

        Yields items and onsite requests. Converts `str` results to `Request`
        and preserves depth information from `response.request.meta`.
        """

        allowed_domains = self._get_allowed_domains(spider)
        if allowed_domains is None:
            # No filtering
            async for item in result:
                yield item
            return

        # Determine current depth if available
        current_depth = 0
        if getattr(response, "request", None) is not None and hasattr(response.request, "meta"):
            current_depth = int(response.request.meta.get("depth", 0))

        # Local imports to avoid circular dependencies at module import time
        from qcrawl.core.item import Item
        from qcrawl.core.request import Request as _Req

        async for item in result:
            # Pass Items unchanged
            if isinstance(item, Item):
                yield item
                continue

            # Handle Request objects
            if isinstance(item, _Req):
                if self._is_offsite(item.url, allowed_domains):
                    await self._emit_offsite_drop(item.url, item, spider, allowed_domains)
                    continue
                yield item
                continue

            # Handle string URLs: convert to Request
            if isinstance(item, str):
                if self._is_offsite(item, allowed_domains):
                    await self._emit_offsite_drop(item, None, spider, allowed_domains)
                    continue

                new_req = _Req(url=item, priority=0, meta={"depth": current_depth + 1})
                yield new_req
                continue

            # Unknown type: pass through unchanged
            yield item

    async def open_spider(self, spider: "Spider") -> None:
        """Log configured allowed domains when spider opens."""

        allowed_domains = self._get_allowed_domains(spider)
        if allowed_domains is None:
            logger.info("all domains allowed")
        elif allowed_domains:
            logger.info("allowed_domains=%s", ", ".join(sorted(allowed_domains)))

    async def close_spider(self, spider: "Spider") -> None:
        """Log offsite statistics when spider closes."""
        if self._dropped_count > 0:
            spider.crawler.stats.set("offsite/filtered", self._dropped_count)
