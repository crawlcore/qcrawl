"""Tests for qcrawl.core.spider.Spider and ResponseView"""

import pytest

from qcrawl.core.response import Page
from qcrawl.core.spider import ResponseView


@pytest.fixture
def mock_engine():
    """Provide a mock engine for spider lifecycle tests."""

    class MockEngine:
        crawler = None

    return MockEngine()


# Initialization Tests


def test_spider_init_valid(dummy_spider):
    """Spider initializes with valid name and start_urls."""
    assert dummy_spider.name == "dummy"
    assert dummy_spider.start_urls == ["https://example.com"]
    assert dummy_spider.engine is None
    assert dummy_spider.crawler is None


def test_spider_init_missing_name():
    """Spider raises TypeError if name is missing."""
    from qcrawl.core.spider import Spider

    class NoNameSpider(Spider):
        start_urls = ["https://example.com"]

        async def parse(self, response):
            yield {}

    with pytest.raises(TypeError, match="must define a non-empty `name: str`"):
        NoNameSpider()


def test_spider_init_missing_start_urls():
    """Spider raises TypeError if start_urls is missing."""
    from qcrawl.core.spider import Spider

    class NoUrlsSpider(Spider):
        name = "test"

        async def parse(self, response):
            yield {}

    with pytest.raises(TypeError, match="must define a non-empty `start_urls: list"):
        NoUrlsSpider()


# Start Requests Tests


@pytest.mark.asyncio
async def test_start_requests(dummy_spider):
    """start_requests yields Requests from start_urls."""
    requests = []

    async for req in dummy_spider.start_requests():
        requests.append(req)

    assert len(requests) == 1
    assert requests[0].url == "https://example.com/"  # Normalized
    assert requests[0].priority == 0
    assert requests[0].meta == {"depth": 0}


# Lifecycle Tests


@pytest.mark.asyncio
async def test_open_spider(dummy_spider, mock_engine):
    """open_spider attaches engine and crawler."""
    await dummy_spider.open_spider(mock_engine)

    assert dummy_spider.engine is mock_engine
    assert dummy_spider.crawler is None


@pytest.mark.asyncio
async def test_close_spider(dummy_spider, mock_engine):
    """close_spider completes without error."""
    # Should not raise
    await dummy_spider.close_spider(mock_engine)


# ResponseView Tests


def test_response_view_creation(dummy_spider):
    """ResponseView stores response and spider correctly."""
    page = Page(
        url="https://example.com",
        content=b"<html><body>Test</body></html>",
        status_code=200,
        headers={},
    )

    view = ResponseView(page, dummy_spider)

    assert view.response is page
    assert view.spider is dummy_spider


def test_response_view_doc(dummy_spider):
    """ResponseView.doc returns lxml document tree."""
    page = Page(
        url="https://example.com",
        content=b"<html><body><h1>Title</h1></body></html>",
        status_code=200,
        headers={},
    )

    view = ResponseView(page, dummy_spider)
    doc = view.doc

    # Verify doc is an lxml element
    assert doc is not None
    # Should be able to use XPath on it
    results = doc.xpath("//h1")
    assert len(results) > 0


def test_response_view_follow(dummy_spider):
    """ResponseView.follow() creates Request from relative URL."""
    page = Page(
        url="https://example.com/page1",
        content=b"<html><body><a href='/page2'>Link</a></body></html>",
        status_code=200,
        headers={},
    )

    view = ResponseView(page, dummy_spider)
    request = view.follow("/page2", priority=5)

    assert request.url == "https://example.com/page2"
    assert request.priority == 5


def test_response_view_urljoin(dummy_spider):
    """ResponseView.urljoin() resolves relative URL."""
    page = Page(
        url="https://example.com/page1",
        content=b"<html><body>Test</body></html>",
        status_code=200,
        headers={},
    )

    view = ResponseView(page, dummy_spider)
    abs_url = view.urljoin("/page2")

    assert abs_url == "https://example.com/page2"
