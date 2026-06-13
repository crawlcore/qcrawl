import inspect
from collections.abc import AsyncGenerator, AsyncIterable
from typing import TYPE_CHECKING

from qcrawl.middleware.base import DownloaderMiddleware, SpiderMiddleware

if TYPE_CHECKING:
    from qcrawl.core.item import Item
    from qcrawl.core.request import Request
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider


class MiddlewareManager:
    """Coordinate the spider middleware chains.

    Runs the spider-facing phases (start_requests, input, output, exception);
    each spider middleware method must return an *async iterable* when it
    returns a non-None value, and the manager validates that contract.

    The `downloader` middleware list is held for reference (and reported by
    `__repr__`), but the downloader request/response/exception chain is executed
    by the engine (`CrawlEngine._run_middleware_chain`), the single owner of that
    logic.
    """

    def __init__(
        self,
        downloader: list[DownloaderMiddleware] | None = None,
        spider: list[SpiderMiddleware] | None = None,
    ) -> None:
        self.downloader: list[DownloaderMiddleware] = downloader or []
        self.spider: list[SpiderMiddleware] = spider or []

    def process_start_requests(
        self, start_requests: AsyncIterable["Request"], spider: "Spider"
    ) -> AsyncGenerator["Request", None]:
        async def _gen() -> AsyncGenerator["Request", None]:
            ag: AsyncIterable[Request] = start_requests
            for mw in self.spider:
                proc = getattr(mw, "process_start_requests", None)
                if proc is None:
                    continue
                res = proc(ag, spider)
                # Support coroutine-returning implementations by awaiting them
                if inspect.isawaitable(res):
                    res = await res
                if res is None:
                    continue
                if not hasattr(res, "__aiter__"):
                    raise TypeError(f"{mw!r}.process_start_requests must return an async iterable")
                ag = res
            async for request in ag:
                yield request

        return _gen()

    async def process_spider_input(self, response: "Page", spider: "Spider") -> Exception | None:
        """Run spider `process_spider_input` hooks.

        Called before passing a `Page` to the spider parse coroutine. If a middleware
        returns a non-None `Exception`, parsing is short-circuited and the exception is
        propagated to the engine.
        """
        for mw in self.spider:
            result = await mw.process_spider_input(response, spider)
            if result is not None:
                return result
        return None

    def process_spider_output(
        self,
        response: "Page",
        result: AsyncGenerator["Item | Request | str", None],  # ← ONLY AsyncGenerator
        spider: "Spider",
    ) -> AsyncGenerator["Item | Request | str", None]:
        """Apply spider output middlewares to the spider's parse output stream.

        Contract
        - `result` must be an async iterable (an async-generator) yielding `Item | Request | str`.
        - Each spider middleware's `process_spider_output(response, ag, spider)` may:
            - return `None` to indicate "no change" (passthrough), or
            - return an async iterable (async-generator) which replaces/wraps the incoming stream.
        """

        async def _gen() -> AsyncGenerator["Item | Request | str", None]:
            ag: AsyncGenerator[Item | Request | str, None] = result
            for mw in self.spider:
                proc = getattr(mw, "process_spider_output", None)
                if proc is None:
                    continue
                res = proc(response, ag, spider)
                if res is None:
                    continue
                if not hasattr(res, "__aiter__"):
                    raise TypeError(
                        f"{mw!r}.process_spider_output must return async generator or None"
                    )
                ag = res
            async for item in ag:
                yield item

        return _gen()

    async def process_spider_exception(
        self, response: "Page", exception: BaseException, spider: "Spider"
    ) -> AsyncGenerator["Item | Request | str", None] | None:
        """Run spider `process_spider_exception` hooks.

        Behavior:
          - Walk spider middleware in registration order and call `process_spider_exception`.
          - If a middleware returns a non-None value it must be an async iterable; that
            async iterable is returned immediately and will be consumed by the caller.
          - If no middleware handles the exception, returns `None`.
        """
        for mw in self.spider:
            proc = getattr(mw, "process_spider_exception", None)
            if proc is None:
                continue
            res = proc(response, exception, spider)
            # Support coroutine-returning implementations by awaiting them
            if inspect.isawaitable(res):
                res = await res
            if res is None:
                continue
            if not hasattr(res, "__aiter__"):
                raise TypeError(
                    f"{mw!r}.process_spider_exception must return None or an async iterable"
                )

            async def _wrap(
                ag: AsyncIterable["Item | Request | str"],
            ) -> AsyncGenerator["Item | Request | str", None]:
                async for r in ag:
                    yield r

            return _wrap(res)
        return None

    def __repr__(self) -> str:
        return f"MiddlewareManager(downloader={len(self.downloader)}, spider={len(self.spider)})"
