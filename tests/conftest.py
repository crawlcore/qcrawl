"""Shared pytest fixtures for all tests."""

import asyncio

import pytest

from qcrawl.core.spider import Spider


class DummySpider(Spider):
    """Shared test spider used across all test files."""

    name = "dummy"
    start_urls = ["https://example.com"]

    async def parse(self, response):
        yield {"url": response.url}


@pytest.fixture
def dummy_spider():
    """Provide a DummySpider instance."""
    return DummySpider()


@pytest.fixture
def run_coro_sync():
    """Provide helper to run coroutine synchronously in tests."""

    def _run(coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    return _run
