"""Pytest fixtures for core component tests."""

import pytest

from qcrawl.core.crawler import Crawler
from qcrawl.core.spider import Spider
from qcrawl.middleware import DownloaderMiddleware
from qcrawl.middleware.base import SpiderMiddleware
from qcrawl.settings import Settings


class DummySpider(Spider):
    """Reusable dummy spider for tests."""

    name = "test"
    start_urls = ["https://example.com"]

    async def parse(self, response):
        yield {"url": response.url}


class DummyDownloaderMiddleware(DownloaderMiddleware):
    """Reusable dummy downloader middleware for tests."""

    async def process_request(self, request, spider):
        pass


class DummySpiderMiddleware(SpiderMiddleware):
    """Reusable dummy spider middleware for tests."""

    async def process_spider_input(self, response, spider):
        pass


@pytest.fixture
def spider():
    """Provide a DummySpider instance."""
    return DummySpider()


@pytest.fixture
def settings():
    """Provide a Settings instance."""
    return Settings()


@pytest.fixture
def crawler(spider, settings):
    """Provide a Crawler instance with spider and settings."""
    return Crawler(spider, settings)


@pytest.fixture
def downloader_middleware():
    """Provide a DummyDownloaderMiddleware instance."""
    return DummyDownloaderMiddleware()


@pytest.fixture
def spider_middleware():
    """Provide a DummySpiderMiddleware instance."""
    return DummySpiderMiddleware()
