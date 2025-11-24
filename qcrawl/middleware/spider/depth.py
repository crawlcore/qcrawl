import logging
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

from qcrawl.middleware.base import SpiderMiddleware
from qcrawl.utils.url import normalize_url

if TYPE_CHECKING:
    from qcrawl.core.item import Item
    from qcrawl.core.request import Request
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class DepthMiddleware(SpiderMiddleware):
    """Limit crawl depth to prevent infinite recursion.

    Features:
    - Configurable max depth per spider (uses `spider.max_depth`)
    - Optional depth priority adjustment via `spider.depth_priority` or `spider.DEPTH_PRIORITY`
    - Automatic depth tracking in request.meta
    - Clones and adjusts Request objects safely via Request.copy()
    - Converts `str` yields to Request and applies depth logic
    - Tracks depth distribution statistics
    """

    def __init__(self, default_max_depth: int = 0, default_priority: int = 1):
        self.default_max_depth = default_max_depth
        self.default_priority = default_priority
        self._depth_stats: dict[int, int] = {}

    def _get_max_depth(self, spider: "Spider") -> int:
        """Return configured max depth for the spider (0 = unlimited)."""
        return getattr(spider, "max_depth", self.default_max_depth)

    def _get_depth_priority(self, spider: "Spider") -> int:
        """Return priority adjustment per depth from spider settings."""
        return getattr(
            spider, "depth_priority", getattr(spider, "DEPTH_PRIORITY", self.default_priority)
        )

    async def process_spider_output(
        self,
        response: "Page",
        result: AsyncGenerator["Item | Request | str", None],
        spider: "Spider",
    ) -> AsyncGenerator["Item | Request | str", None]:
        """Process spider output and filter/adjust requests by depth.

        Yields Items unchanged. For Request objects or URL strings, enforces
        max depth, sets `meta['depth']`, and adjusts `priority`.

        This implementation honors an explicit `Request.meta['depth']` when the
        spider yields a `Request` with that key; otherwise it uses parent's depth + 1.
        """
        max_depth = self._get_max_depth(spider)
        depth_priority = self._get_depth_priority(spider)

        # Determine current depth from response.request.meta if available
        current_depth = 0
        if getattr(response, "request", None) is not None and hasattr(response.request, "meta"):
            try:
                current_depth = int(response.request.meta.get("depth", 0))
            except Exception:
                current_depth = 0

        async for item in result:
            # Items pass through unchanged
            from qcrawl.core.item import Item  # local import for TYPE_CHECKING friendliness
            from qcrawl.core.request import (
                Request,
            )  # local import to avoid circulars at module import time

            if isinstance(item, Item):
                yield item
                continue

            # Handle Request objects
            if isinstance(item, Request):
                # If the yielded Request explicitly sets depth, respect and validate it.
                if getattr(item, "meta", None) and "depth" in item.meta:
                    val = item.meta["depth"]
                    if not isinstance(val, int) or isinstance(val, bool):
                        raise TypeError("Request.meta['depth'] must be an int when present")
                    next_depth = int(val)
                else:
                    next_depth = current_depth + 1

                if max_depth > 0 and next_depth > max_depth:
                    logger.debug("Ignoring link (depth > %d): %s", max_depth, item.url)
                    continue

                new_req = item.copy()
                new_req.meta = dict(new_req.meta or {})
                new_req.meta["depth"] = next_depth

                if depth_priority != 0:
                    new_req.priority = getattr(new_req, "priority", 0) - (
                        next_depth * depth_priority
                    )

                self._depth_stats[next_depth] = self._depth_stats.get(next_depth, 0) + 1

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Enqueued request at depth %d: %s (priority: %d)",
                        next_depth,
                        new_req.url,
                        getattr(new_req, "priority", 0),
                    )
                yield new_req
                continue

            # Handle string URLs: convert to Request using parent_depth + 1
            if isinstance(item, str):
                next_depth = current_depth + 1
                if max_depth > 0 and next_depth > max_depth:
                    logger.debug("Ignoring link (depth > %d): %s", max_depth, item)
                    continue

                from qcrawl.core.request import Request as _Req

                priority = 0
                if depth_priority != 0:
                    priority = 0 - (next_depth * depth_priority)

                normalized_url = normalize_url(item)
                new_req = _Req(url=normalized_url, priority=priority, meta={"depth": next_depth})
                self._depth_stats[next_depth] = self._depth_stats.get(next_depth, 0) + 1

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Enqueued request (from str) at depth %d: %s (priority: %d)",
                        next_depth,
                        item,
                        priority,
                    )
                yield new_req
                continue

            # Unknown type: pass through unchanged
            yield item

    async def open_spider(self, spider: "Spider") -> None:
        """Log configured depth when spider opens."""

        if logger.isEnabledFor(logging.DEBUG):
            max_depth = self._get_max_depth(spider)

            if max_depth > 0:
                logger.info("DepthMiddleware: max_depth=%d", max_depth)
            else:
                logger.info("DepthMiddleware: unlimited depth")

    async def close_spider(self, spider) -> None:
        """Emit depth statistics."""
        if not self._depth_stats:
            return

        max_depth_reached = max(self._depth_stats.keys()) if self._depth_stats else 0
        spider.crawler.stats.set_counter("depth/max", max_depth_reached)
        spider.crawler.stats.set_meta("depth/distribution", str(dict(self._depth_stats)))
