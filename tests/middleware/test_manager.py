"""Tests for qcrawl.middleware.manager - MiddlewareManager behavior

Tests focus on the spider middleware chains (start_requests, input, output,
exception): chaining, passthrough, and return-type validation. The downloader
request/response/exception chain is owned and tested at the engine
(see tests/core/test_engine.py).
"""

import pytest

from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.core.spider import Spider
from qcrawl.middleware.base import DownloaderMiddleware, SpiderMiddleware
from qcrawl.middleware.manager import MiddlewareManager

# Test Helper Classes


class DummySpider(Spider):
    """Minimal spider for testing."""

    name = "dummy"
    start_urls = ["http://example.com"]

    async def parse(self, response):
        yield {"data": "test"}


# Spider Middleware Tests - process_start_requests


@pytest.mark.asyncio
async def test_process_start_requests_chains_middleware():
    """process_start_requests chains spider middleware generators."""

    class AddPrefixMiddleware(SpiderMiddleware):
        async def process_start_requests(self, start_requests, spider):
            async for request in start_requests:
                request.meta["prefix"] = "added"
                yield request

    class AddSuffixMiddleware(SpiderMiddleware):
        async def process_start_requests(self, start_requests, spider):
            async for request in start_requests:
                request.meta["suffix"] = "added"
                yield request

    async def initial_requests():
        yield Request("http://example.com/1")
        yield Request("http://example.com/2")

    manager = MiddlewareManager(spider=[AddPrefixMiddleware(), AddSuffixMiddleware()])
    spider = DummySpider()

    result = manager.process_start_requests(initial_requests(), spider)

    requests = []
    async for request in result:
        requests.append(request)

    assert len(requests) == 2
    assert all(req.meta["prefix"] == "added" for req in requests)
    assert all(req.meta["suffix"] == "added" for req in requests)


@pytest.mark.asyncio
async def test_process_start_requests_raises_on_invalid_return_type():
    """process_start_requests raises TypeError when middleware returns wrong type."""

    class BadMiddleware(SpiderMiddleware):
        async def process_start_requests(self, start_requests, spider):
            return ["not", "async", "iterable"]  # Should return async generator

    async def initial_requests():
        yield Request("http://example.com")

    manager = MiddlewareManager(spider=[BadMiddleware()])
    spider = DummySpider()

    result = manager.process_start_requests(initial_requests(), spider)

    with pytest.raises(TypeError, match="must return an async iterable"):
        async for _ in result:
            pass


# Spider Middleware Tests - process_spider_input


@pytest.mark.asyncio
async def test_process_spider_input_returns_first_exception():
    """process_spider_input returns first non-None exception from middleware."""

    class MW1(SpiderMiddleware):
        async def process_spider_input(self, response, spider):
            return None  # Continue

    class MW2(SpiderMiddleware):
        async def process_spider_input(self, response, spider):
            return ValueError("validation error")  # First exception

    class MW3(SpiderMiddleware):
        async def process_spider_input(self, response, spider):
            return RuntimeError("should not be called")

    manager = MiddlewareManager(spider=[MW1(), MW2(), MW3()])
    response = Page(url="http://example.com", content=b"test", status_code=200, headers={})
    spider = DummySpider()

    result = await manager.process_spider_input(response, spider)

    assert isinstance(result, ValueError)
    assert str(result) == "validation error"


@pytest.mark.asyncio
async def test_process_spider_input_returns_none_when_all_pass():
    """process_spider_input returns None when all middleware return None."""

    class PassthroughMiddleware(SpiderMiddleware):
        async def process_spider_input(self, response, spider):
            return None

    manager = MiddlewareManager(spider=[PassthroughMiddleware(), PassthroughMiddleware()])
    response = Page(url="http://example.com", content=b"test", status_code=200, headers={})
    spider = DummySpider()

    result = await manager.process_spider_input(response, spider)

    assert result is None


# Spider Middleware Tests - process_spider_output


@pytest.mark.asyncio
async def test_process_spider_output_chains_middleware():
    """process_spider_output chains spider middleware generators."""

    class FilterItemsMiddleware(SpiderMiddleware):
        async def process_spider_output(self, response, result, spider):
            async for item in result:
                if isinstance(item, Item) and item.data.get("keep"):
                    yield item

    class AddMetadataMiddleware(SpiderMiddleware):
        async def process_spider_output(self, response, result, spider):
            async for item in result:
                if isinstance(item, Item):
                    item.metadata["processed"] = True
                yield item

    async def spider_output():
        yield Item(data={"id": 1, "keep": True})
        yield Item(data={"id": 2, "keep": False})
        yield Item(data={"id": 3, "keep": True})

    manager = MiddlewareManager(spider=[FilterItemsMiddleware(), AddMetadataMiddleware()])
    response = Page(url="http://example.com", content=b"test", status_code=200, headers={})
    spider = DummySpider()

    result = manager.process_spider_output(response, spider_output(), spider)

    items: list[Item] = []
    async for item in result:
        if isinstance(item, Item):
            items.append(item)

    assert len(items) == 2, "Should filter out items without keep=True"
    assert all(item.metadata.get("processed") for item in items)


@pytest.mark.asyncio
async def test_process_spider_output_raises_on_invalid_return_type():
    """process_spider_output raises TypeError when middleware returns wrong type."""

    class BadMiddleware(SpiderMiddleware):
        def process_spider_output(self, response, result, spider):
            return ["not", "async", "iterable"]  # Should return async generator or None

    async def spider_output():
        yield Item(data={"test": "value"})

    manager = MiddlewareManager(spider=[BadMiddleware()])
    response = Page(url="http://example.com", content=b"test", status_code=200, headers={})
    spider = DummySpider()

    result = manager.process_spider_output(response, spider_output(), spider)

    with pytest.raises(TypeError, match="must return async generator or None"):
        async for _ in result:
            pass


# Spider Middleware Tests - process_spider_exception


@pytest.mark.asyncio
async def test_process_spider_exception_returns_first_handler():
    """process_spider_exception returns first non-None async iterable from middleware."""

    class MW1(SpiderMiddleware):
        async def process_spider_exception(self, response, exception, spider):
            return None  # Don't handle

    class MW2(SpiderMiddleware):
        async def process_spider_exception(self, response, exception, spider):
            async def recovery():
                yield Item(data={"recovered": True})

            return recovery()

    class MW3(SpiderMiddleware):
        async def process_spider_exception(self, response, exception, spider):
            async def should_not_be_called():
                yield Item(data={"should_not_appear": True})

            return should_not_be_called()

    manager = MiddlewareManager(spider=[MW1(), MW2(), MW3()])
    response = Page(url="http://example.com", content=b"test", status_code=200, headers={})
    exception = Exception("parse error")
    spider = DummySpider()

    result = await manager.process_spider_exception(response, exception, spider)

    assert result is not None, "Should return recovery async iterable"

    items: list[Item] = []
    async for item in result:
        if isinstance(item, Item):
            items.append(item)

    assert len(items) == 1
    assert items[0].data["recovered"] is True


@pytest.mark.asyncio
async def test_process_spider_exception_returns_none_when_no_handler():
    """process_spider_exception returns None when no middleware handles exception."""

    class PassthroughMiddleware(SpiderMiddleware):
        async def process_spider_exception(self, response, exception, spider):
            return None

    manager = MiddlewareManager(spider=[PassthroughMiddleware(), PassthroughMiddleware()])
    response = Page(url="http://example.com", content=b"test", status_code=200, headers={})
    exception = Exception("parse error")
    spider = DummySpider()

    result = await manager.process_spider_exception(response, exception, spider)

    assert result is None


# Edge Cases


@pytest.mark.asyncio
async def test_empty_spider_middleware_list():
    """MiddlewareManager works with empty spider middleware list."""
    manager = MiddlewareManager(spider=[])

    async def start_requests():
        yield Request("http://example.com")

    spider = DummySpider()

    # Should pass through unchanged
    result = manager.process_start_requests(start_requests(), spider)

    requests = []
    async for request in result:
        requests.append(request)

    assert len(requests) == 1


@pytest.mark.asyncio
async def test_manager_repr():
    """MiddlewareManager __repr__ shows middleware counts."""
    manager = MiddlewareManager(
        downloader=[DownloaderMiddleware(), DownloaderMiddleware()],
        spider=[SpiderMiddleware()],
    )

    repr_str = repr(manager)

    assert "MiddlewareManager" in repr_str
    assert "downloader=2" in repr_str
    assert "spider=1" in repr_str
