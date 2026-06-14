import logging
from collections import defaultdict
from http.cookies import SimpleCookie
from typing import TYPE_CHECKING

from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult
from qcrawl.utils.url import get_domain

if TYPE_CHECKING:
    from qcrawl.core.request import Request
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class CookiesMiddleware(DownloaderMiddleware):
    """Handle cookies for requests/responses.

    - Stores cookies per spider and per-domain.
    - Resets per-spider cookie jars on `open_spider` and clears them on `close_spider`
      so runs are deterministic and do not leak state across spiders.
    """

    def __init__(self, enabled: bool = True) -> None:
        self._enabled = bool(enabled)
        # cookies[spider_id][domain] = SimpleCookie
        self._cookies: dict[object, dict[str, SimpleCookie]] = defaultdict(
            lambda: defaultdict(SimpleCookie)
        )

    @classmethod
    def from_crawler(cls, crawler):
        """Create from crawler, reading the `COOKIES_ENABLED` setting."""
        from qcrawl.settings import Settings

        settings: Settings = getattr(crawler, "runtime_settings", None) or Settings()
        return cls(enabled=settings.COOKIES_ENABLED)

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL using canonical helper."""
        return get_domain(url)

    def _get_spider_id(self, spider: "Spider") -> object:
        """Get unique spider identifier."""
        return getattr(spider, "name", id(spider))

    async def process_request(self, request: "Request", spider: "Spider") -> MiddlewareResult:
        """Add cookies to outgoing request."""
        if not self._enabled:
            return MiddlewareResult.continue_()

        spider_id = self._get_spider_id(spider)
        domain = self._get_domain(request.url)

        # Get cookies for this domain
        cookie_jar = self._cookies.get(spider_id, {}).get(domain)
        if not cookie_jar:
            return MiddlewareResult.continue_()

        # Build Cookie header
        cookie_header = "; ".join(f"{name}={morsel.value}" for name, morsel in cookie_jar.items())

        if cookie_header:
            # Make a copy before mutating headers
            request.headers = dict(request.headers) if request.headers is not None else {}
            request.headers["Cookie"] = cookie_header

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Added cookies to %s: %s", request.url, cookie_header)

        return MiddlewareResult.continue_()

    async def process_response(
        self, request: "Request", response: "Page", spider: "Spider"
    ) -> MiddlewareResult:
        """Extract Set-Cookie from response."""
        if not self._enabled:
            return MiddlewareResult.keep(response)

        spider_id = self._get_spider_id(spider)
        domain = self._get_domain(request.url)

        # Case-insensitive collection of Set-Cookie headers
        set_cookie_headers = [v for k, v in response.headers.items() if k.lower() == "set-cookie"]

        if not set_cookie_headers:
            return MiddlewareResult.keep(response)

        # Parse and store cookies
        cookie_jar = self._cookies[spider_id][domain]
        for header in set_cookie_headers:
            cookie = SimpleCookie()
            try:
                cookie.load(header)
                cookie_jar.update(cookie)

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Stored cookie from %s: %s", domain, header)

            except Exception as exc:
                spider.crawler.stats.inc("cookies/errors")
                logger.warning(
                    "Failed to parse Set-Cookie header %r for %s: %s", header, request.url, exc
                )

        return MiddlewareResult.keep(response)

    async def process_exception(
        self, request: "Request", exception: BaseException, spider: "Spider"
    ) -> MiddlewareResult:
        """No special exception handling for cookies."""
        return MiddlewareResult.continue_()

    async def open_spider(self, spider: "Spider") -> None:
        """Async lifecycle hook: reset per-spider cookie jars on spider open."""
        try:
            spider_id = self._get_spider_id(spider)
            # Replace existing entry with a fresh per-domain mapping
            self._cookies[spider_id] = defaultdict(SimpleCookie)

            logger.info("started")

        except Exception:
            logger.exception("Error in CookiesMiddleware.open_spider")

    async def close_spider(self, spider: "Spider") -> None:
        """Async lifecycle hook: clear per-spider cookie jars on spider close."""
        spider_id = self._get_spider_id(spider)
        self._cookies.pop(spider_id, None)

    def clear_cookies(self, spider: "Spider | None" = None, domain: str | None = None) -> None:
        """Clear cookies for spider/domain (sync helper)."""
        if spider is None:
            self._cookies.clear()
            return

        spider_id = self._get_spider_id(spider)
        if domain is None:
            self._cookies[spider_id].clear()
        else:
            self._cookies[spider_id].pop(domain, None)
