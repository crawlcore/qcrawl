from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcrawl.core.item import Item
    from qcrawl.core.request import Request
    from qcrawl.core.response import Page
    from qcrawl.core.spider import Spider


class Action(Enum):
    """Explicit action returned by downloader middleware.

    Values:
        CONTINUE: Do nothing; let engine proceed (e.g. process_request -> download).
        KEEP: Replace or accept the response (payload is a Page).
        RETRY: Schedule a retry (payload is a Request).
        DROP: Drop the current response/request (no payload).
    """

    CONTINUE = auto()  # no change, let engine proceed (e.g. process_request -> perform download)
    KEEP = auto()  # keep / replace the response (payload: Page)
    RETRY = auto()  # schedule a retry (payload: Request)
    DROP = auto()  # drop current response/request (no payload)


@dataclass(frozen=True)
class MiddlewareResult:
    """Typed result wrapper for downloader middleware.

    Use the constructor helpers to express intent:

        - MiddlewareResult.continue_() -> continue processing
        - MiddlewareResult.keep(page) -> use provided Page
        - MiddlewareResult.retry(request) -> schedule Request for retry
        - MiddlewareResult.drop() -> drop the response/request

    Attributes:
        action: Action enum describing the requested engine behaviour.
        payload: Optional associated object (Page or Request) depending on action.
    """

    action: Action
    payload: object | None = None

    @classmethod
    def continue_(cls) -> MiddlewareResult:
        """Return a result indicating no action; engine should continue."""
        return cls(Action.CONTINUE, None)

    @classmethod
    def keep(cls, page: Page) -> MiddlewareResult:
        """Return a result that accepts/replaces the response with `page`."""
        return cls(Action.KEEP, page)

    @classmethod
    def retry(cls, request: Request) -> MiddlewareResult:
        """Return a result requesting that `request` be scheduled for retry."""
        return cls(Action.RETRY, request)

    @classmethod
    def drop(cls) -> MiddlewareResult:
        """Return a result indicating the response/request should be dropped."""
        return cls(Action.DROP, None)


class DownloaderMiddleware:
    """Base class for downloader middleware.

    Downloader middleware participates in the request/response/exception phases
    around the actual network download.

    Contract:
      - All middleware hooks must be defined as `async def`.
      - Hooks that participate in the request/response/exception phases must return
        a `MiddlewareResult` instance expressing the desired engine action.
      - Implementations must avoid blocking I/O; use async APIs.
    """

    async def process_request(self, request: Request, spider: Spider) -> MiddlewareResult:
        """Process request before download."""
        return MiddlewareResult.continue_()

    async def process_response(
        self, request: Request, response: Page, spider: Spider
    ) -> MiddlewareResult:
        """Process response after download."""
        return MiddlewareResult.keep(response)

    async def process_exception(
        self, request: Request, exception: BaseException, spider: Spider
    ) -> MiddlewareResult:
        """Handle download exceptions.md."""
        return MiddlewareResult.continue_()

    async def open_spider(self, spider: Spider) -> None:
        """Optional async hook called when a spider is opened."""
        return None

    async def close_spider(self, spider: Spider) -> None:
        """Optional async hook called when a spider is closed."""
        return None


class SpiderMiddleware:
    """Base class for spider middleware.

    Spider middleware must return *async* iterables (async-generators).

    Implementations are expected to be async-generators, i.e. call sites will
    iterate them with `async for`. Returning sync iterables or coroutines is
    considered incorrect and will be rejected by the manager.
    """

    async def process_start_requests(
        self, start_requests: AsyncGenerator[Request, None], spider: Spider
    ) -> AsyncGenerator[Request, None]:
        """Process the spider's initial start_requests stream.

        This hook receives the async-generator produced by `Spider.start_requests()`
        and may return:
          - An async iterable (async-generator) that will replace the incoming stream,
          - `None` to indicate no changes (manager treats None as passthrough).

        Typical uses: filter or transform start requests, inject additional requests,
        or apply per-request metadata before scheduling.

        Args:
            start_requests: AsyncGenerator yielding `Request` or URL `str`.
            spider: The spider instance.

        Returns:
            An async-generator yielding `Request` objects (or the original stream).
        """
        async for r in start_requests:
            yield r

    async def process_spider_input(self, response: Page, spider: Spider) -> Exception | None:
        """Inspect a `Page` before it is handed to the spider parser.

        Called immediately prior to invoking the spider's `parse()` coroutine for
        a given `response`. Middleware may perform validation or short-circuit
        parsing by returning an `Exception` instance which the engine will treat
        as a parsing error and propagate to the exception handling path.

        Returning `None` indicates parsing should proceed as normal.

        Args:
            response: The `Page` object to be parsed.
            spider: The spider instance.

        Returns:
            An `Exception` to abort parsing, or `None` to continue.
        """
        return None

    async def process_spider_output(
        self,
        response: Page,
        result: AsyncGenerator[Item | Request | str, None],
        spider: Spider,
    ) -> AsyncGenerator[Item | Request | str, None]:
        async for r in result:
            yield r

    async def process_spider_exception(
        self, response: Page, exception: BaseException, spider: Spider
    ) -> AsyncGenerator[Item | Request | str, None] | None:
        """Handle exceptions raised during spider parsing.

        This hook is invoked when the spider's `parse()` generator raises an exception.
        Middleware may choose to handle the error and provide a recovery stream by
        returning an async iterable yielding `Item`, `Request`, or `str` values. If a
        non-None value is returned it must be an async iterable; the engine will consume
        its items as if produced by the spider.

        Returning `None` indicates the middleware does not handle the exception and
        the engine should continue normal exception handling.

        Args:
            response: The `Page` being parsed when the exception occurred.
            exception: The exception instance raised by the spider.
            spider: The spider instance.

        Returns:
            An async iterable of `Item | Request | str` to recover from the error,
            or `None` to indicate the exception was not handled.
        """
        return None

    async def open_spider(self, spider: Spider) -> None:
        """Optional async hook called when a spider is opened."""
        return None

    async def close_spider(self, spider: Spider) -> None:
        """Optional async hook called when a spider is closed."""
        return None
