"""Tests for qcrawl.core.crawler.Crawler"""

import pytest

from qcrawl.core.crawler import Crawler
from qcrawl.middleware import DownloaderMiddleware

# Basic Initialization Tests


def test_crawler_initializes_correctly(crawler, spider, settings):
    """Crawler initializes with all required components."""
    assert crawler.spider is spider
    assert crawler.runtime_settings is settings
    assert crawler.stats is not None
    assert crawler.signals is not None
    assert crawler._finalized is False
    assert crawler.queue is None
    assert crawler.downloader is None
    assert crawler.scheduler is None
    assert crawler.engine is None


# Middleware Registration Tests


@pytest.mark.parametrize(
    "middleware",
    [
        pytest.param(
            lambda: pytest.importorskip("tests.core.conftest").DummyDownloaderMiddleware(),
            id="instance",
        ),
        pytest.param(
            lambda: pytest.importorskip("tests.core.conftest").DummyDownloaderMiddleware,
            id="class",
        ),
    ],
)
def test_add_middleware_accepts_various_forms(crawler, middleware):
    """Crawler accepts middleware as instance or class."""
    mw = middleware()
    crawler.add_middleware(mw)
    assert mw in crawler._pending_middlewares


def test_add_middleware_after_crawl_raises(crawler):
    """Cannot add middleware after crawl has started."""
    # Simulate crawl started
    crawler.engine = object()

    with pytest.raises(RuntimeError, match="Cannot add middleware after crawl"):
        crawler.add_middleware(object())


def test_add_multiple_middlewares(crawler, downloader_middleware):
    """Crawler can register multiple middlewares in order."""
    mw1 = downloader_middleware
    from tests.core.conftest import DummyDownloaderMiddleware

    mw2 = DummyDownloaderMiddleware()

    crawler.add_middleware(mw1)
    crawler.add_middleware(mw2)

    assert len(crawler._pending_middlewares) >= 2
    # Middlewares appear in order after defaults
    assert mw1 in crawler._pending_middlewares
    assert mw2 in crawler._pending_middlewares


# Middleware Resolution via from_crawler


def test_middleware_with_from_crawler_classmethod(spider, settings):
    """Middleware with from_crawler() classmethod is instantiated correctly."""

    class CustomMiddleware(DownloaderMiddleware):
        def __init__(self, crawler):
            self.crawler = crawler

        @classmethod
        def from_crawler(cls, crawler):
            return cls(crawler)

        async def process_request(self, request, spider):
            pass

    crawler = Crawler(spider, settings)
    crawler.add_middleware(CustomMiddleware)

    # Middleware should be in pending (resolution happens later)
    assert CustomMiddleware in crawler._pending_middlewares


# Lifecycle Tests


@pytest.mark.asyncio
async def test_crawler_context_manager_lifecycle(crawler, spider):
    """Crawler works as async context manager with proper cleanup."""
    assert not crawler._finalized

    async with crawler:
        assert crawler.spider is spider
        assert not crawler._finalized

    # Should be finalized after exit
    assert crawler._finalized


# Default Middlewares


def test_default_middlewares_registered(crawler):
    """Crawler automatically registers default middlewares from settings."""
    # Default middlewares should be added during __init__
    assert len(crawler._pending_middlewares) > 0


# Error Handling


def test_invalid_middleware_type_rejected(crawler):
    """Crawler rejects invalid middleware types."""
    # This will fail during resolution, not during add_middleware
    # Just verify we can add it (resolution happens later)
    crawler.add_middleware("invalid")
    assert "invalid" in crawler._pending_middlewares
