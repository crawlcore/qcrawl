"""Tests for qcrawl.core.engine.CrawlEngine"""

from unittest.mock import AsyncMock, Mock

import pytest

from qcrawl import signals
from qcrawl.core.engine import CrawlEngine
from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.core.scheduler import Scheduler
from qcrawl.core.spider import Spider
from qcrawl.downloaders import DownloadHandlerManager
from qcrawl.middleware import DownloaderMiddleware
from qcrawl.middleware.base import Action, MiddlewareResult, SpiderMiddleware


@pytest.fixture
def mock_scheduler():
    """Provide a mock scheduler."""
    return Mock(spec=Scheduler)


@pytest.fixture
def mock_handler_manager():
    """Provide a mock download handler manager."""
    return Mock(spec=DownloadHandlerManager)


@pytest.fixture
def engine(mock_scheduler, mock_handler_manager, spider):
    """Provide a CrawlEngine instance with mocked dependencies."""
    return CrawlEngine(mock_scheduler, mock_handler_manager, spider)


# Initialization Tests


def test_engine_initializes_correctly(engine, mock_scheduler, mock_handler_manager, spider):
    """Engine initializes with all required components."""
    assert engine.scheduler is mock_scheduler
    assert engine.handler_manager is mock_handler_manager
    assert engine.spider is spider
    assert engine.signals is not None
    assert engine._running is False
    assert isinstance(engine.middlewares, list)
    assert len(engine.middlewares) == 0


# Middleware Registration Tests


def test_add_single_middleware(engine, downloader_middleware):
    """Engine accepts and stores middleware."""
    engine.add_middleware(downloader_middleware)

    assert downloader_middleware in engine.middlewares
    assert downloader_middleware in engine._reversed_mws


def test_add_multiple_middlewares_preserves_order(engine):
    """Engine maintains middleware order and reverses for response chain."""
    from tests.core.conftest import DummyDownloaderMiddleware

    mw1 = DummyDownloaderMiddleware()
    mw2 = DummyDownloaderMiddleware()
    mw3 = DummyDownloaderMiddleware()

    engine.add_middleware(mw1)
    engine.add_middleware(mw2)
    engine.add_middleware(mw3)

    # Request chain: mw1 -> mw2 -> mw3
    assert engine.middlewares == [mw1, mw2, mw3]
    # Response chain: mw3 -> mw2 -> mw1 (reversed)
    assert engine._reversed_mws == [mw3, mw2, mw1]


def test_add_middleware_after_start_raises(engine):
    """Cannot add middleware after engine has started."""
    engine._running = True

    from tests.core.conftest import DummyDownloaderMiddleware

    with pytest.raises(RuntimeError, match="Cannot add middleware"):
        engine.add_middleware(DummyDownloaderMiddleware())


# State Management


def test_engine_initial_state(engine):
    """Engine starts in non-running state."""
    assert engine._running is False


def test_middleware_manager_initialized(engine):
    """Engine has middleware manager with correct setup."""
    assert engine._mw_manager is not None
    assert engine._mw_manager.downloader == engine.middlewares


# Downloader Chain Execution Tests
# The engine — not MiddlewareManager — owns downloader-chain execution via
# _run_middleware_chain. These cover the ordering / short-circuit / payload
# semantics that previously were only tested against the unused manager methods.


@pytest.mark.asyncio
async def test_run_middleware_chain_request_runs_in_order(engine):
    """process_request runs downloader middleware in registration order."""
    order: list[str] = []

    class OrderMW(DownloaderMiddleware):
        def __init__(self, tag):
            self.tag = tag

        async def process_request(self, request, spider):
            order.append(self.tag)
            return MiddlewareResult.continue_()

    engine.add_middleware(OrderMW("a"))
    engine.add_middleware(OrderMW("b"))
    request = Request(url="https://example.com")

    result = await engine._run_middleware_chain("process_request", request, engine.middlewares)

    assert order == ["a", "b"]
    assert result.action is Action.CONTINUE


@pytest.mark.asyncio
async def test_run_middleware_chain_response_runs_in_reverse(engine):
    """process_response runs downloader middleware in reverse registration order."""
    order: list[str] = []

    class OrderMW(DownloaderMiddleware):
        def __init__(self, tag):
            self.tag = tag

        async def process_response(self, request, response, spider):
            order.append(self.tag)
            return MiddlewareResult.continue_()

    engine.add_middleware(OrderMW("a"))
    engine.add_middleware(OrderMW("b"))
    request = Request(url="https://example.com")
    response = Page(
        url="https://example.com", content=b"", status_code=200, headers={}, request=request
    )

    result = await engine._run_middleware_chain(
        "process_response", request, engine._reversed_mws, response
    )

    assert order == ["b", "a"]
    assert result.action is Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_run_middleware_chain_short_circuits_on_retry(engine):
    """A RETRY result stops the chain; later middleware does not run."""
    order: list[str] = []

    class RetryMW(DownloaderMiddleware):
        async def process_request(self, request, spider):
            order.append("retry")
            return MiddlewareResult.retry(request)

    class NeverMW(DownloaderMiddleware):
        async def process_request(self, request, spider):
            order.append("never")
            return MiddlewareResult.continue_()

    engine.add_middleware(RetryMW())
    engine.add_middleware(NeverMW())
    request = Request(url="https://example.com")

    result = await engine._run_middleware_chain("process_request", request, engine.middlewares)

    assert result.action is Action.RETRY
    assert order == ["retry"]


@pytest.mark.asyncio
async def test_run_middleware_chain_short_circuits_on_drop(engine):
    """A DROP result stops the chain; later middleware does not run."""
    order: list[str] = []

    class DropMW(DownloaderMiddleware):
        async def process_request(self, request, spider):
            order.append("drop")
            return MiddlewareResult.drop()

    class NeverMW(DownloaderMiddleware):
        async def process_request(self, request, spider):
            order.append("never")
            return MiddlewareResult.continue_()

    engine.add_middleware(DropMW())
    engine.add_middleware(NeverMW())
    request = Request(url="https://example.com")

    result = await engine._run_middleware_chain("process_request", request, engine.middlewares)

    assert result.action is Action.DROP
    assert order == ["drop"]


@pytest.mark.asyncio
async def test_run_middleware_chain_response_keep_replaces_payload(engine):
    """A KEEP result replaces the response payload carried down the chain."""
    request = Request(url="https://example.com")
    original = Page(
        url="https://example.com",
        content=b"original",
        status_code=200,
        headers={},
        request=request,
    )
    replacement = Page(
        url="https://example.com",
        content=b"replaced",
        status_code=200,
        headers={},
        request=request,
    )

    class ReplaceMW(DownloaderMiddleware):
        async def process_response(self, request, response, spider):
            return MiddlewareResult.keep(replacement)

    engine.add_middleware(ReplaceMW())

    result = await engine._run_middleware_chain(
        "process_response", request, engine._reversed_mws, original
    )

    assert result.action is Action.KEEP
    assert result.payload is replacement


@pytest.mark.asyncio
async def test_run_middleware_chain_raises_on_invalid_return(engine):
    """The chain raises TypeError when a middleware returns a non-MiddlewareResult."""

    class BadMW(DownloaderMiddleware):
        async def process_request(self, request, spider):
            return "not a MiddlewareResult"

    engine.add_middleware(BadMW())
    request = Request(url="https://example.com")

    with pytest.raises(TypeError, match="must return MiddlewareResult"):
        await engine._run_middleware_chain("process_request", request, engine.middlewares)


# Spider Exception Handling Tests
# A parse-time exception is offered to spider middleware via
# process_spider_exception; recovery output is emitted, otherwise it propagates.


@pytest.mark.asyncio
async def test_process_spider_exception_recovery_emits_items(mock_handler_manager):
    """A parse exception handled by a spider middleware emits its recovery items."""

    class FailingSpider(Spider):
        name = "failing"
        start_urls = ["https://example.com"]

        async def parse(self, response):
            raise ValueError("boom")
            yield  # makes parse an async generator

    class RecoverMW(SpiderMiddleware):
        async def process_spider_exception(self, response, exception, spider):
            async def recovery():
                yield Item(data={"recovered": True})

            return recovery()

    engine = CrawlEngine(Mock(spec=Scheduler), mock_handler_manager, FailingSpider())
    engine._mw_manager.spider.append(RecoverMW())

    scraped: list[Item] = []

    async def on_item(sender, item, spider=None, **kwargs):
        scraped.append(item)

    signals.signals_registry.connect("item_scraped", on_item, sender=engine, weak=False)
    request = Request(url="https://example.com")
    response = Page(
        url="https://example.com", content=b"", status_code=200, headers={}, request=request
    )
    try:
        await engine._process_parse_results(request, response)
    finally:
        signals.signals_registry.disconnect("item_scraped", on_item, sender=engine)

    assert len(scraped) == 1
    assert scraped[0].data["recovered"] is True


@pytest.mark.asyncio
async def test_process_spider_exception_unhandled_propagates(mock_handler_manager):
    """An unhandled parse exception propagates when no spider middleware recovers."""

    class FailingSpider(Spider):
        name = "failing"
        start_urls = ["https://example.com"]

        async def parse(self, response):
            raise ValueError("boom")
            yield  # makes parse an async generator

    engine = CrawlEngine(Mock(spec=Scheduler), mock_handler_manager, FailingSpider())
    request = Request(url="https://example.com")
    response = Page(
        url="https://example.com", content=b"", status_code=200, headers={}, request=request
    )

    with pytest.raises(ValueError, match="boom"):
        await engine._process_parse_results(request, response)


# Pipeline (direct-path) Item Tests
# Items run through the injected _item_processor (the Crawler's PipelineManager)
# before item_scraped/item_dropped are emitted as observation signals.


@pytest.mark.asyncio
async def test_emit_parse_result_runs_processor_then_emits_item_scraped(engine):
    """A surviving item is run through the processor, then emitted as item_scraped."""

    async def processor(item, spider):
        item.data["processed"] = True
        return item

    engine._item_processor = processor

    scraped: list[Item] = []

    async def on_scraped(sender, item, spider=None, **kwargs):
        scraped.append(item)

    signals.signals_registry.connect("item_scraped", on_scraped, sender=engine, weak=False)
    try:
        await engine._emit_parse_result(Item(data={"x": 1}))
    finally:
        signals.signals_registry.disconnect("item_scraped", on_scraped, sender=engine)

    assert len(scraped) == 1
    assert scraped[0].data["processed"] is True


@pytest.mark.asyncio
async def test_emit_parse_result_dropped_item_emits_item_dropped(engine):
    """A pipeline drop (processor returns None) emits item_dropped, not item_scraped."""

    async def drop_processor(item, spider):
        return None

    engine._item_processor = drop_processor

    scraped: list[Item] = []
    dropped: list[Item] = []

    async def on_scraped(sender, item, spider=None, **kwargs):
        scraped.append(item)

    async def on_dropped(sender, item, spider=None, **kwargs):
        dropped.append(item)

    signals.signals_registry.connect("item_scraped", on_scraped, sender=engine, weak=False)
    signals.signals_registry.connect("item_dropped", on_dropped, sender=engine, weak=False)
    try:
        await engine._emit_parse_result(Item(data={"x": 1}))
    finally:
        signals.signals_registry.disconnect("item_scraped", on_scraped, sender=engine)
        signals.signals_registry.disconnect("item_dropped", on_dropped, sender=engine)

    assert len(scraped) == 0
    assert len(dropped) == 1


@pytest.mark.asyncio
async def test_item_processor_error_emits_item_error_and_continues(mock_handler_manager):
    """A processor error emits item_error for that item; the rest of the parse continues."""

    async def buggy_processor(item, spider):
        if item.data.get("id") == 2:
            raise ValueError("processor bug")
        return item

    class ThreeItemSpider(Spider):
        name = "three"
        start_urls = ["https://example.com"]

        async def parse(self, response):
            yield {"id": 1}
            yield {"id": 2}
            yield {"id": 3}

    engine = CrawlEngine(Mock(spec=Scheduler), mock_handler_manager, ThreeItemSpider())
    engine._item_processor = buggy_processor

    scraped: list[int] = []
    errored: list[int] = []

    async def on_scraped(sender, item, spider=None, **kwargs):
        scraped.append(item.data["id"])

    async def on_error(sender, item=None, error=None, spider=None, **kwargs):
        errored.append(item.data["id"])

    signals.signals_registry.connect("item_scraped", on_scraped, sender=engine, weak=False)
    signals.signals_registry.connect("item_error", on_error, sender=engine, weak=False)
    request = Request(url="https://example.com")
    response = Page(
        url="https://example.com", content=b"", status_code=200, headers={}, request=request
    )
    try:
        await engine._process_parse_results(request, response)
    finally:
        signals.signals_registry.disconnect("item_scraped", on_scraped, sender=engine)
        signals.signals_registry.disconnect("item_error", on_error, sender=engine)

    assert scraped == [1, 3]
    assert errored == [2]


@pytest.mark.asyncio
async def test_handle_exception_emits_request_failed(engine):
    """A request failure emits request_failed with the request and error."""
    import aiohttp

    failed: list[tuple[object, object]] = []

    async def on_failed(sender, request=None, error=None, spider=None, **kwargs):
        failed.append((request, error))

    signals.signals_registry.connect("request_failed", on_failed, sender=engine, weak=False)
    request = Request(url="https://example.com")
    exc = aiohttp.ClientError("boom")
    try:
        await engine._handle_exception(request, exc)
    finally:
        signals.signals_registry.disconnect("request_failed", on_failed, sender=engine)

    assert len(failed) == 1
    assert failed[0][0] is request
    assert failed[0][1] is exc


@pytest.mark.asyncio
async def test_request_failed_not_emitted_when_retried(engine, mock_scheduler):
    """A retryable exception reschedules the request and does NOT emit request_failed."""
    import aiohttp

    fired: list[int] = []

    async def on_failed(sender, request=None, error=None, spider=None, **kwargs):
        fired.append(1)

    class RetryMW(DownloaderMiddleware):
        async def process_request(self, request, spider):
            return MiddlewareResult.continue_()

        async def process_response(self, request, response, spider):
            return MiddlewareResult.keep(response)

        async def process_exception(self, request, exception, spider):
            return MiddlewareResult.retry(request)

    engine.add_middleware(RetryMW())
    mock_scheduler.add = AsyncMock()
    signals.signals_registry.connect("request_failed", on_failed, sender=engine, weak=False)
    request = Request(url="https://example.com")
    try:
        await engine._handle_exception(request, aiohttp.ClientError("boom"))
    finally:
        signals.signals_registry.disconnect("request_failed", on_failed, sender=engine)

    assert fired == []  # rescheduled, not failed
    mock_scheduler.add.assert_awaited_once_with(request)


@pytest.mark.asyncio
async def test_parse_results_dispatches_to_named_callback(engine, spider):
    """A request whose callback names a spider method routes the response there with cb_kwargs."""
    calls: list[object] = []

    async def parse_detail(response, page=None):
        calls.append(page)
        return
        yield  # noqa: makes this an async generator

    spider.parse_detail = parse_detail

    request = Request(url="https://example.com/x", callback="parse_detail", cb_kwargs={"page": 7})
    response = Page(
        url="https://example.com/x", content=b"", status_code=200, headers={}, request=request
    )

    await engine._process_parse_results(request, response)

    assert calls == [7]


@pytest.mark.asyncio
async def test_drain_idle_loops_until_no_new_work(engine, mock_scheduler):
    """_drain_idle re-fires spider_idle while a handler enqueues work, then stops."""
    fires: list[int] = []

    async def on_idle(sender, spider=None, **kwargs):
        fires.append(1)

    # Round 1: handler added work (qsize 2) -> drain and fire again.
    # Round 2: nothing added (qsize 0) -> stop.  qsize() is async on Scheduler.
    mock_scheduler.qsize = AsyncMock(side_effect=[2, 0])
    mock_scheduler.join = AsyncMock()
    signals.signals_registry.connect("spider_idle", on_idle, sender=engine, weak=False)
    try:
        await engine._drain_idle()
    finally:
        signals.signals_registry.disconnect("spider_idle", on_idle, sender=engine)

    assert len(fires) == 2
