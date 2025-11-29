"""Tests for DepthMiddleware."""

import pytest

from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.spider.depth import DepthMiddleware

# Initialization Tests


def test_middleware_init_default():
    """DepthMiddleware initializes with defaults."""
    middleware = DepthMiddleware()

    assert middleware.default_max_depth == 0
    assert middleware.default_priority == 1


def test_middleware_init_custom():
    """DepthMiddleware initializes with custom parameters."""
    middleware = DepthMiddleware(default_max_depth=5, default_priority=2)

    assert middleware.default_max_depth == 5
    assert middleware.default_priority == 2


# Helper Method Tests


def test_get_max_depth_from_spider(spider):
    """_get_max_depth uses spider's max_depth."""
    middleware = DepthMiddleware(default_max_depth=10)
    spider.max_depth = 5

    max_depth = middleware._get_max_depth(spider)

    assert max_depth == 5


def test_get_max_depth_default(spider):
    """_get_max_depth falls back to default."""
    middleware = DepthMiddleware(default_max_depth=10)

    max_depth = middleware._get_max_depth(spider)

    assert max_depth == 10


def test_get_depth_priority_from_spider(spider):
    """_get_depth_priority uses spider's depth_priority."""
    middleware = DepthMiddleware(default_priority=1)
    spider.depth_priority = 3

    priority = middleware._get_depth_priority(spider)

    assert priority == 3


def test_get_depth_priority_default(spider):
    """_get_depth_priority falls back to default."""
    middleware = DepthMiddleware(default_priority=2)

    priority = middleware._get_depth_priority(spider)

    assert priority == 2


# process_spider_output Tests - Items


@pytest.mark.asyncio
async def test_process_spider_output_passes_items(spider, http_response):
    """process_spider_output passes items unchanged."""
    middleware = DepthMiddleware()

    async def spider_output():
        yield Item(data={"title": "Test"}, metadata={})

    results = []
    async for item in middleware.process_spider_output(http_response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1
    assert isinstance(results[0], Item)
    assert results[0].data["title"] == "Test"


# process_spider_output Tests - Request Objects


@pytest.mark.asyncio
async def test_process_spider_output_sets_depth_on_request(spider):
    """process_spider_output sets depth meta on requests."""
    middleware = DepthMiddleware()
    parent_request = Request(url="https://example.com/parent", meta={"depth": 1})
    parent_response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/child")

    results = []
    async for item in middleware.process_spider_output(parent_response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1
    child_request = results[0]
    assert isinstance(child_request, Request)
    assert child_request.meta["depth"] == 2


@pytest.mark.asyncio
async def test_process_spider_output_honors_max_depth(spider):
    """process_spider_output filters requests beyond max_depth."""
    middleware = DepthMiddleware(default_max_depth=2)
    parent_request = Request(url="https://example.com/parent", meta={"depth": 2})
    parent_response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/child")

    results = []
    async for item in middleware.process_spider_output(parent_response, spider_output(), spider):
        results.append(item)

    # Should be filtered (would be depth 3, max is 2)
    assert len(results) == 0


@pytest.mark.asyncio
async def test_process_spider_output_adjusts_priority(spider):
    """process_spider_output adjusts priority based on depth."""
    middleware = DepthMiddleware(default_priority=10)
    parent_request = Request(url="https://example.com/parent", meta={"depth": 0})
    parent_response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/child", priority=100)

    results = []
    async for item in middleware.process_spider_output(parent_response, spider_output(), spider):
        results.append(item)

    child_request = results[0]
    assert isinstance(child_request, Request)
    # depth 1, priority_adjust 10 -> 100 - (1 * 10) = 90
    assert child_request.priority == 90


@pytest.mark.asyncio
async def test_process_spider_output_respects_explicit_depth(spider):
    """process_spider_output respects explicitly set depth."""
    middleware = DepthMiddleware()
    parent_request = Request(url="https://example.com/parent", meta={"depth": 1})
    parent_response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        # Explicitly set depth to 5
        yield Request(url="https://example.com/child", meta={"depth": 5})

    results = []
    async for item in middleware.process_spider_output(parent_response, spider_output(), spider):
        results.append(item)

    child_request = results[0]
    assert isinstance(child_request, Request)
    assert child_request.meta["depth"] == 5


# process_spider_output Tests - String URLs


@pytest.mark.asyncio
async def test_process_spider_output_converts_strings_to_requests(spider, http_response):
    """process_spider_output converts string URLs to Request objects."""
    middleware = DepthMiddleware()

    async def spider_output():
        yield "https://example.com/page"

    results = []
    async for item in middleware.process_spider_output(http_response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1
    assert isinstance(results[0], Request)
    assert results[0].url == "https://example.com/page"
    assert results[0].meta["depth"] == 1


@pytest.mark.asyncio
async def test_process_spider_output_filters_strings_by_depth(spider):
    """process_spider_output filters string URLs beyond max_depth."""
    middleware = DepthMiddleware(default_max_depth=1)
    parent_request = Request(url="https://example.com/parent", meta={"depth": 1})
    parent_response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield "https://example.com/child"

    results = []
    async for item in middleware.process_spider_output(parent_response, spider_output(), spider):
        results.append(item)

    # Should be filtered (would be depth 2, max is 1)
    assert len(results) == 0


@pytest.mark.asyncio
async def test_process_spider_output_string_priority_adjustment(spider, http_response):
    """process_spider_output applies priority adjustment to string URLs."""
    middleware = DepthMiddleware(default_priority=5)

    async def spider_output():
        yield "https://example.com/page"

    results = []
    async for item in middleware.process_spider_output(http_response, spider_output(), spider):
        results.append(item)

    request = results[0]
    assert isinstance(request, Request)
    # depth 1, priority_adjust 5 -> 0 - (1 * 5) = -5
    assert request.priority == -5


# process_spider_output Tests - Unlimited Depth


@pytest.mark.asyncio
async def test_process_spider_output_unlimited_depth(spider):
    """process_spider_output allows unlimited depth when max_depth=0."""
    middleware = DepthMiddleware(default_max_depth=0)
    parent_request = Request(url="https://example.com/parent", meta={"depth": 100})
    parent_response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/child")

    results = []
    async for item in middleware.process_spider_output(parent_response, spider_output(), spider):
        results.append(item)

    # Should not be filtered even at depth 101
    assert len(results) == 1
    assert isinstance(results[0], Request)
    assert results[0].meta["depth"] == 101


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_max_depth(spider, caplog):
    """open_spider logs max_depth configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = DepthMiddleware(default_max_depth=5)

    await middleware.open_spider(spider)

    assert "max_depth=5" in caplog.text


@pytest.mark.asyncio
async def test_open_spider_logs_unlimited(spider, caplog):
    """open_spider logs unlimited depth."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = DepthMiddleware(default_max_depth=0)

    await middleware.open_spider(spider)

    assert "unlimited depth" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_depth_tracking_through_multiple_levels(spider):
    """Depth is correctly tracked through multiple levels."""
    middleware = DepthMiddleware(default_max_depth=3)

    # Level 0 (seed)
    request0 = Request(url="https://example.com/", meta={"depth": 0})
    response0 = Page(
        url="https://example.com/",
        content=b"",
        status_code=200,
        headers={},
        request=request0,
    )

    async def spider_output0():
        yield Request(url="https://example.com/page1")

    results0 = []
    async for item in middleware.process_spider_output(response0, spider_output0(), spider):
        results0.append(item)

    assert isinstance(results0[0], Request)
    assert results0[0].meta["depth"] == 1

    # Level 1
    request1 = results0[0]
    response1 = Page(
        url="https://example.com/page1",
        content=b"",
        status_code=200,
        headers={},
        request=request1,
    )

    async def spider_output1():
        yield Request(url="https://example.com/page2")

    results1 = []
    async for item in middleware.process_spider_output(response1, spider_output1(), spider):
        results1.append(item)

    assert isinstance(results1[0], Request)
    assert results1[0].meta["depth"] == 2


@pytest.mark.asyncio
async def test_mixed_output_types(spider, http_response):
    """Handles mixed output types (Items, Requests, strings)."""
    middleware = DepthMiddleware(default_max_depth=5)

    async def spider_output():
        yield Item(data={"type": "item"}, metadata={})
        yield Request(url="https://example.com/request")
        yield "https://example.com/string"

    results = []
    async for item in middleware.process_spider_output(http_response, spider_output(), spider):
        results.append(item)

    assert len(results) == 3
    assert isinstance(results[0], Item)
    assert isinstance(results[1], Request)
    assert isinstance(results[2], Request)
