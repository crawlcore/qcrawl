"""Pytest fixtures for middleware tests."""

import pytest

from qcrawl.core.crawler import Crawler
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.core.spider import Spider
from qcrawl.settings import Settings


class DummySpider(Spider):
    """Reusable dummy spider for middleware tests."""

    name = "test"
    start_urls = ["https://example.com"]

    async def parse(self, response):
        yield {"url": response.url}


@pytest.fixture
def spider():
    """Provide a DummySpider instance with mock crawler."""
    from unittest.mock import Mock

    spider_instance = DummySpider()
    # Add mock crawler with stats for middleware tests
    mock_crawler = Mock()
    mock_crawler.stats = Mock()
    spider_instance.crawler = mock_crawler  # type: ignore[assignment]
    return spider_instance


@pytest.fixture
def settings():
    """Provide a Settings instance."""
    return Settings()


@pytest.fixture
def crawler(spider, settings):
    """Provide a Crawler instance with spider and settings."""
    return Crawler(spider, settings)


@pytest.fixture
def http_request():
    """Provide an HTTP Request instance for testing."""
    return Request(url="https://example.com/page")


@pytest.fixture
def http_response():
    """Provide an HTTP Response (Page) instance for testing."""
    return Page(
        url="https://example.com/page",
        content=b"<html><body>Test</body></html>",
        status_code=200,
        headers={"Content-Type": "text/html"},
        request=Request(url="https://example.com/page"),
    )
