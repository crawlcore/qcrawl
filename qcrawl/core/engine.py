from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import aiohttp

from qcrawl import signals
from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.core.scheduler import Scheduler
from qcrawl.core.spider import Spider
from qcrawl.downloaders.handler_manager import DownloadHandlerManager
from qcrawl.middleware import DownloaderMiddleware
from qcrawl.middleware.base import Action, MiddlewareResult
from qcrawl.middleware.manager import MiddlewareManager

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from qcrawl.core.crawler import Crawler

logger = logging.getLogger(__name__)


class CrawlEngine:
    """Core engine orchestrating scheduler, handler manager, spider, and middleware.

    Responsibilities
        - Wire scheduler, handler manager and spider together for the crawl lifecycle.
        - Execute downloader middleware chains (request/response/exception).
        - Use MiddlewareManager for spider middleware phases (start/input/output/exception).
        - Manage worker tasks and ensure graceful shutdown.
        - Emit signals for observability and stats.
    """

    __slots__ = (
        "scheduler",
        "handler_manager",
        "spider",
        "signals",
        "middlewares",
        "_reversed_mws",
        "_running",
        "crawler",
        "_mw_manager",
        "_item_processor",
    )

    def __init__(
        self,
        scheduler: Scheduler,
        handler_manager: DownloadHandlerManager,
        spider: Spider,
    ) -> None:
        """Initialize engine with required components.

        Args:
            scheduler: Request scheduler used to obtain new work.
            handler_manager: DownloadHandlerManager responsible for routing requests to appropriate handlers.
            spider: Spider instance to which responses are dispatched.
        """
        self.scheduler = scheduler
        self.handler_manager = handler_manager
        self.spider = spider
        self.signals = signals.signals_registry.for_sender(self)

        self.middlewares: list[DownloaderMiddleware] = []
        self._reversed_mws: list[DownloaderMiddleware] = []
        self._mw_manager = MiddlewareManager(downloader=self.middlewares, spider=[])

        self._running = False
        self.crawler: Crawler | None = None
        # Optional direct-path item processor (set by the Crawler to
        # PipelineManager.process_item). Returns the processed item, or None if a
        # pipeline dropped it. Pipelines run here, on the item's data path — not
        # via a signal handler — so item_scraped/item_dropped stay observation-only.
        self._item_processor: Callable[[Item, Spider], Awaitable[Item | None]] | None = None

    def add_middleware(self, mw: DownloaderMiddleware) -> None:
        """Register a downloader middleware before crawl starts.

        Raises:
            RuntimeError: if called after `crawl()` has started.
        """
        if self._running:
            raise RuntimeError("Cannot add middleware after crawl() has started")
        self.middlewares.append(mw)
        self._reversed_mws = list(reversed(self.middlewares))
        # keep MiddlewareManager in sync (only update downloader chain)
        self._mw_manager.downloader = self.middlewares

    async def crawl(self) -> None:
        """Start the crawl lifecycle.

        Workflow:
          1. Schedule `start_requests()` via spider middleware.
          2. Spawn worker tasks equal to `spider.concurrency`.
          3. Await `scheduler.join()` until all work finishes.
          4. On unhandled exception: emit `spider_error` and re-raise.
          5. Always ensure scheduler is closed and workers cancelled.
        """
        self._running = True
        reason = "finished"
        workers: list[asyncio.Task[None]] = []

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Crawl started for spider=%s", getattr(self.spider, "name", None))

        try:
            await self._schedule_start_requests()

            concurrency = getattr(self.spider, "concurrency", None)
            if concurrency is None:
                rs = getattr(self.spider, "runtime_settings", None)
                if rs is not None:
                    raw = getattr(rs, "concurrency", None)
                    # Accept only true ints (reject bool). Do NOT coerce from float/str.
                    if isinstance(raw, int) and not isinstance(raw, bool):
                        concurrency = raw
                    else:
                        concurrency = None

            # Ensure we end up with a plain int; otherwise use default 10
            if not isinstance(concurrency, int) or isinstance(concurrency, bool):
                concurrency = 10

            # Validate range; if invalid, log and use default
            if concurrency < 1 or concurrency > 10000:
                logger.warning(
                    "Invalid spider concurrency %r for %s; using default 10",
                    getattr(self.spider, "concurrency", None),
                    getattr(self.spider, "name", "<unknown>"),
                )
                concurrency = 10

            workers = [
                asyncio.create_task(self._worker(i), name=f"worker-{i}") for i in range(concurrency)
            ]

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Workers spawned: %d for spider=%s",
                    len(workers),
                    getattr(self.spider, "name", None),
                )

            await self.scheduler.join()
            await self._drain_idle()

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Crawl finished for spider=%s", getattr(self.spider, "name", None))

        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            logger.exception("Crawl failed: %s", reason)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "signal.emit spider_error -- spider=%s payload=%s",
                    getattr(self.spider, "name", None),
                    str(exc),
                )

            await self.signals.send_async("spider_error", exc=exc)
            raise

        finally:
            self._running = False
            await self.scheduler.close()

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Scheduler closed for spider=%s", getattr(self.spider, "name", None))

            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Workers stopped for spider=%s", getattr(self.spider, "name", None))

    async def _schedule_start_requests(self) -> None:
        """Enqueue initial requests from spider.start_requests() using spider middleware."""
        async for req in self._mw_manager.process_start_requests(
            self.spider.start_requests(), self.spider
        ):
            request = (
                Request(url=req, priority=0, meta={"depth": 0}) if isinstance(req, str) else req
            )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Enqueuing start URL: %s", getattr(request, "url", None))

            await self.scheduler.add(request)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Scheduling request <%s %s>",
                    getattr(request, "method", None),
                    getattr(request, "url", None),
                )

    async def _worker(self, worker_id: int) -> None:
        """Worker loop: fetch requests from scheduler and process them."""

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Worker %s started", worker_id)

        while True:
            try:
                request = await self.scheduler.get()
            except asyncio.CancelledError:
                break

            try:
                response = await self._process_request(request)
                if response:
                    await self._process_parse_results(request, response)
            except asyncio.CancelledError as exc:
                # Cancellation during request processing must still notify downloader
                # middlewares so they can release resources (e.g. concurrency semaphores).
                try:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "Worker %s cancelled while processing %s; invoking process_exception chain",
                            worker_id,
                            getattr(request, "url", None),
                        )
                    # Run exception chain in reverse middleware order with the CancelledError payload
                    with contextlib.suppress(Exception):
                        await self._run_middleware_chain(
                            "process_exception", request, self._reversed_mws, exc
                        )
                except Exception:
                    logger.exception(
                        "Error running middleware process_exception chain on CancelledError for %s",
                        getattr(request, "url", None),
                    )
                break
            except Exception as exc:
                await self._handle_exception(request, exc)
            finally:
                try:
                    self.scheduler.task_done()
                except Exception:
                    logger.exception("Error calling scheduler.task_done() in worker %s", worker_id)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Worker %s stopped", worker_id)

    async def _process_request(self, request: Request) -> Page | None:
        """Process a single Request through downloader middleware and perform fetch."""
        # Request phase
        result = await self._run_middleware_chain("process_request", request, self.middlewares)
        if result.action is Action.KEEP:
            return result.payload  # type: ignore[return-value]
        if result.action in (Action.RETRY, Action.DROP):
            await self._handle_retry_or_drop(result, request)
            return None

        # Download
        await self.signals.send_async("request_reached_downloader", request=request)

        # Delegate to handler manager; it routes to appropriate handler and passes spider
        response = await self.handler_manager.fetch(request, spider=self.spider)

        # Response phase
        result = await self._run_middleware_chain(
            "process_response", request, self._reversed_mws, response
        )
        if result.action is Action.KEEP:
            return result.payload  # type: ignore[return-value]
        if result.action in (Action.RETRY, Action.DROP):
            await self._handle_retry_or_drop(result, request)
            return None

        return response

    async def _run_middleware_chain(
        self,
        method_name: str,
        request: Request,
        chain: list[DownloaderMiddleware],
        initial_payload: object | None = None,
    ) -> MiddlewareResult:
        """Run a downloader middleware chain.

        Process each middleware in sequence. For response chains, both KEEP and
        CONTINUE allow processing to continue. Only RETRY and DROP short-circuit.
        """
        current_payload = initial_payload

        for mw in chain:
            method = getattr(mw, method_name)

            # Call middleware method with or without payload
            if current_payload is not None:
                result = await method(request, current_payload, self.spider)
            else:
                result = await method(request, self.spider)

            if not isinstance(result, MiddlewareResult):
                raise TypeError(
                    f"{mw.__class__.__name__}.{method_name} must return MiddlewareResult"
                )

            # Short-circuit on RETRY or DROP
            if result.action in (Action.RETRY, Action.DROP):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "middleware %s.%s -> %s (mw=%s)",
                        method_name,
                        getattr(request, "url", None),
                        result.action.name,
                        mw.__class__.__name__,
                    )
                return result

            # Log and continue for KEEP or CONTINUE
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "middleware %s %s (mw=%s url=%s)",
                    method_name,
                    result.action.name,
                    mw.__class__.__name__,
                    getattr(request, "url", None),
                )

            # Update payload for next middleware
            if result.payload is not None:
                current_payload = result.payload

        # All middlewares processed - return final result
        if isinstance(current_payload, Page):
            return MiddlewareResult.keep(current_payload)
        return MiddlewareResult.continue_()

    async def _handle_retry_or_drop(
        self, result: MiddlewareResult, original_request: Request
    ) -> None:
        """Handle RETRY or DROP results from downloader middleware."""
        if result.action is Action.RETRY:
            if not isinstance(result.payload, Request):
                raise TypeError("Retry payload must be Request")
            if logger.isEnabledFor(logging.DEBUG):
                try:
                    preview = getattr(result.payload, "to_dict", lambda: repr(result.payload))()
                except Exception:
                    preview = "<unreprable>"
                logger.debug("middleware.retry scheduling new request %s", preview)
            await self.scheduler.add(result.payload)
        elif result.action is Action.DROP:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("middleware.drop url=%s", getattr(original_request, "url", None))
            await self.signals.send_async(
                "request_dropped",
                request=original_request,
                exception=None,
            )

    async def _process_parse_results(self, request: Request, response: Page) -> None:
        """Run spider parse coroutine via spider middleware and handle yielded values."""

        # run spider input hooks (may return an Exception to abort)
        exc = await self._mw_manager.process_spider_input(response, self.spider)
        if exc is not None:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "spider_input.exception url=%s err=%s", getattr(request, "url", None), str(exc)
                )
            await self._handle_exception(request, exc)  # delegate to existing logic
            return

        # Resolve the per-request callback (by name) on the spider, defaulting to
        # `parse`. The callback was stored as a method name so the request stayed
        # serializable across queue backends.
        callback_name = request.callback if isinstance(request.callback, str) else "parse"
        callback = getattr(self.spider, callback_name, None)
        if not callable(callback):
            callback = self.spider.parse
        cb_kwargs = request.cb_kwargs or {}

        # Obtain spider parse async-iterator (support coroutine-returning implementations)
        parse_result = callback(response, **cb_kwargs)

        # If parse() returns a coroutine, await it to get the async generator
        parsed_ag: AsyncGenerator[Item | Request | str, None]
        if inspect.isawaitable(parse_result):
            parsed_ag = await parse_result
        else:
            parsed_ag = parse_result

        if not hasattr(parsed_ag, "__aiter__"):
            raise TypeError("Spider.parse must return an async iterable (async-generator)")

        # Apply spider middlewares (they are responsible for depth & normalization)
        wrapped_ag = self._mw_manager.process_spider_output(response, parsed_ag, self.spider)

        # Drive the spider output stream. An exception raised while producing
        # output (the spider's parse() body or a spider middleware) is offered to
        # process_spider_exception; if no middleware returns recovery output it
        # re-raises, so the worker's existing handling and stats still see it.
        # Errors while emitting a result (scheduler/signal) are not caught here.
        ait = wrapped_ag.__aiter__()
        while True:
            try:
                result = await ait.__anext__()
            except StopAsyncIteration:
                break
            except Exception as spider_exc:
                recovery = await self._mw_manager.process_spider_exception(
                    response, spider_exc, self.spider
                )
                if recovery is None:
                    raise
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "spider_exception handled by middleware for url=%s err=%s",
                        getattr(request, "url", None),
                        str(spider_exc),
                    )
                async for recovered in recovery:
                    await self._emit_parse_result(recovered)
                break
            await self._emit_parse_result(result)

    async def _emit_parse_result(self, result: object) -> None:
        """Dispatch one value yielded by spider parse (or exception recovery).

        Items are emitted as item_scraped; Requests (and bare URL strings) are
        scheduled; anything else is logged and ignored.
        """
        if isinstance(result, (Item, dict)):
            item = result if isinstance(result, Item) else Item(data=result)
            # Run the item through the pipeline on its data path. item_scraped is
            # emitted only for items that survive; a pipeline drop (None) emits
            # item_dropped instead. Both are observation-only signals.
            processed: Item | None = item
            if self._item_processor is not None:
                try:
                    processed = await self._item_processor(item, self.spider)
                except Exception as item_exc:
                    # A pipeline raised (a deliberate DropItem returns None instead).
                    # Emit item_error — distinct from a deliberate item_dropped — and
                    # continue; one item's pipeline bug must not abort the parse output.
                    logger.exception(
                        "Pipeline error processing item from %s",
                        getattr(self.spider, "name", None),
                    )
                    await self.signals.send_async(
                        "item_error", item=item, error=item_exc, spider=self.spider
                    )
                    return
            if processed is None:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "item_dropped %s from %s",
                        getattr(item, "data", None),
                        getattr(self.spider, "name", None),
                    )
                await self.signals.send_async("item_dropped", item=item, spider=self.spider)
                return
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "item_scraped %s from %s",
                    getattr(processed, "data", None),
                    getattr(self.spider, "name", None),
                )
            await self.signals.send_async("item_scraped", item=processed, spider=self.spider)

        elif isinstance(result, Request):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "scheduling request %s (priority=%s) from %s",
                    getattr(result, "url", None),
                    getattr(result, "priority", None),
                    getattr(self.spider, "name", None),
                )
            await self.scheduler.add(result)

        elif isinstance(result, str):
            # Convert stray strings to Request and schedule (no depth enforcement here).
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "scheduling URL string %s from %s",
                    result,
                    getattr(self.spider, "name", None),
                )
            await self.scheduler.add(Request(url=result))

        else:
            # Unknown yielded type: log and ignore
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Ignoring unexpected spider parse result type %s from %s",
                    type(result),
                    getattr(self.spider, "name", None),
                )

    async def _drain_idle(self) -> None:
        """Fire `spider_idle` and re-drain until a round enqueues no new work.

        Lets idle handlers feed the crawl in repeated waves: after each emit, if
        the handler added requests, drain them and fire again; stop once a round
        leaves the scheduler empty.
        """
        while True:
            await self.signals.send_async("spider_idle", spider=self.spider)
            if (await self.scheduler.qsize()) == 0:
                break
            await self.scheduler.join()

    async def _handle_exception(self, request: Request, exc: Exception) -> None:
        """Handle exceptions raised while processing a request.

        Emits `request_failed` once, when the request is abandoned because of an
        error (non-retryable, retries exhausted, or unhandled) — NOT per attempt.
        `request_dropped` is reserved for deliberate filtering / middleware DROP.
        """
        is_network = isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientError))

        if not is_network:
            exc_info = (type(exc), exc, getattr(exc, "__traceback__", None))
            logger.exception("Unhandled error for %s", request.url, exc_info=exc_info)
            await self.signals.send_async("request_failed", request=request, error=exc)
            return

        try:
            result = await self._run_middleware_chain(
                "process_exception", request, self._reversed_mws, exc
            )
        except Exception:
            exc_info = (type(exc), exc, getattr(exc, "__traceback__", None))
            logger.exception(
                "Middleware chain raised while processing exception for %s",
                request.url,
                exc_info=exc_info,
            )
            await self.signals.send_async("request_failed", request=request, error=exc)
            return

        if result.action is Action.RETRY:
            # The request is rescheduled; it has not failed yet.
            await self._handle_retry_or_drop(result, request)
        else:
            # DROP (retries exhausted) or unhandled: the errored request is
            # abandoned -> request_failed (request_dropped is for filtering).
            logger.error("Network error for %s: %s", request.url, exc)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Network exception traceback for %s",
                    request.url,
                    exc_info=(type(exc), exc, getattr(exc, "__traceback__", None)),
                )
            await self.signals.send_async("request_failed", request=request, error=exc)
