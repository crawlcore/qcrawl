import asyncio
import logging
import time
from typing import TYPE_CHECKING
from urllib import robotparser

import aiohttp

from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult
from qcrawl.utils.url import get_domain_base

if TYPE_CHECKING:
    from qcrawl.core.request import Request
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class RobotsTxtMiddleware(DownloaderMiddleware):
    """Downloader middleware enforcing robots.txt rules per-host.

    Configuration is provided via constructor parameters or spider attributes:
      - obey_robots_txt: bool (default True)
      - user_agent: str | None (default None -> spider.runtime_settings.USER_AGENT or default)
      - cache_ttl: float seconds to keep parsed robots for a domain (default 3600.0)
    """

    def __init__(
        self, obey_robots_txt: bool = True, user_agent: str | None = None, cache_ttl: float = 3600.0
    ) -> None:
        self.obey = bool(obey_robots_txt)
        self.user_agent = user_agent
        self.cache_ttl = float(cache_ttl)
        # cache: domain_base -> (fetched_at_ts, RobotFileParser | None)
        self._cache: dict[str, tuple[float, robotparser.RobotFileParser | None]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def _fetch_robots(self, domain_base: str) -> robotparser.RobotFileParser | None:
        """Fetch and parse robots.txt for domain_base (e.g., 'https://example.com')."""
        url = domain_base.rstrip("/") + "/robots.txt"
        try:
            async with aiohttp.ClientSession() as sess:
                async with asyncio.timeout(10):
                    async with sess.get(url) as resp:
                        if resp.status != 200:
                            return None
                        text = await resp.text()
        except Exception:
            logger.debug("Failed to fetch robots.txt for %s", domain_base, exc_info=True)
            return None

        rp = robotparser.RobotFileParser()
        try:
            rp.parse(text.splitlines())
        except Exception:
            logger.debug("Failed to parse robots.txt for %s", domain_base, exc_info=True)
            return None
        return rp

    async def _ensure_parser(self, domain_base: str) -> robotparser.RobotFileParser | None:
        now = time.time()
        entry = self._cache.get(domain_base)
        if entry:
            fetched_at, parser_obj = entry
            if (now - fetched_at) < self.cache_ttl:
                return parser_obj

        # Acquire per-domain lock to avoid duplicate concurrent fetches
        lock = self._locks.get(domain_base)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[domain_base] = lock

        async with lock:
            # Re-check cache after acquiring lock
            entry = self._cache.get(domain_base)
            if entry:
                fetched_at, parser_obj = entry
                if (time.time() - fetched_at) < self.cache_ttl:
                    return parser_obj
            parser_obj = await self._fetch_robots(domain_base)
            self._cache[domain_base] = (time.time(), parser_obj)
            return parser_obj

    def _resolve_user_agent(self, spider: "Spider") -> str:
        # priority: explicit middleware.user_agent -> spider.runtime_settings.USER_AGENT -> fallback
        if self.user_agent:
            return str(self.user_agent)
        try:
            rs = getattr(spider, "runtime_settings", None)
            if rs is not None:
                ua = getattr(rs, "USER_AGENT", None) or getattr(rs, "user_agent", None)
                if ua:
                    return str(ua)
        except Exception:
            pass
        return "qCrawl/1.0"

    async def process_request(self, request: "Request", spider: "Spider") -> MiddlewareResult:
        """Enforce robots.txt before download.

        If robots are unavailable or fetching/parsing fails, the request is allowed.
        """
        if not self.obey:
            return MiddlewareResult.continue_()

        domain_base = get_domain_base(request.url)
        if not domain_base:
            # Unable to resolve domain; be permissive
            return MiddlewareResult.continue_()

        parser = await self._ensure_parser(domain_base)
        if parser is None:
            # No rules -> allow
            return MiddlewareResult.continue_()

        ua = self._resolve_user_agent(spider)
        try:
            allowed = parser.can_fetch(ua, request.url)
        except Exception:
            logger.debug("robots parser error for %s", request.url, exc_info=True)
            allowed = True

        if not allowed:
            spider.crawler.stats.inc("robotstxt/blocked")

            # Emit request_dropped via spider-bound dispatcher when available
            try:
                dispatcher = getattr(spider, "signals", None)
                if dispatcher is not None:
                    await dispatcher.send_async("request_dropped", request=request, exception=None)
            except Exception:
                logger.exception("Error sending request_dropped signal for %s", request.url)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Blocked by robots.txt: %s", request.url)

            return MiddlewareResult.drop()

        return MiddlewareResult.continue_()

    async def open_spider(self, spider: "Spider") -> None:
        ua: str = self._resolve_user_agent(spider)
        logger.info(
            "obey=%s user_agent=%s cache_ttl=%s",
            self.obey,
            ua or "<default>",
            self.cache_ttl,
        )

    async def close_spider(self, spider: "Spider") -> None:
        # Clear caches to free memory
        self._cache.clear()
        self._locks.clear()
