"""Tests for qcrawl.core.engine.CrawlEngine"""

from unittest.mock import Mock

import pytest

from qcrawl.core.downloader import Downloader
from qcrawl.core.engine import CrawlEngine
from qcrawl.core.scheduler import Scheduler


@pytest.fixture
def mock_scheduler():
    """Provide a mock scheduler."""
    return Mock(spec=Scheduler)


@pytest.fixture
def mock_downloader():
    """Provide a mock downloader."""
    return Mock(spec=Downloader)


@pytest.fixture
def engine(mock_scheduler, mock_downloader, spider):
    """Provide a CrawlEngine instance with mocked dependencies."""
    return CrawlEngine(mock_scheduler, mock_downloader, spider)


# Initialization Tests


def test_engine_initializes_correctly(engine, mock_scheduler, mock_downloader, spider):
    """Engine initializes with all required components."""
    assert engine.scheduler is mock_scheduler
    assert engine.downloader is mock_downloader
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
