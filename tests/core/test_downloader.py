"""Production-quality tests for qcrawl.core.downloader.Downloader"""

from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from qcrawl.core.downloader import Downloader


@pytest.fixture
def mock_session():
    """Provide a mock aiohttp ClientSession."""
    session = Mock(spec=aiohttp.ClientSession)
    session.close = AsyncMock()
    session.closed = False
    return session


# Initialization Tests


def test_downloader_init_with_owned_session(mock_session):
    """Downloader initializes correctly when it owns the session."""
    downloader = Downloader(mock_session, own_session=True)

    assert downloader._session is mock_session
    assert downloader._own_session is True
    assert downloader._closed is False
    assert downloader.signals is not None


def test_downloader_init_with_external_session(mock_session):
    """Downloader initializes correctly with external session."""
    downloader = Downloader(mock_session, own_session=False)

    assert downloader._session is mock_session
    assert downloader._own_session is False
    assert downloader._closed is False


# Factory Method Test


@pytest.mark.asyncio
async def test_downloader_create_factory():
    """Downloader.create() produces a working instance."""
    downloader = await Downloader.create()

    try:
        assert downloader._session is not None
        assert isinstance(downloader._session, aiohttp.ClientSession)
        assert downloader._own_session is True
        assert not downloader._closed
    finally:
        await downloader.close()


# Session Ownership and Cleanup Tests


@pytest.mark.asyncio
@pytest.mark.parametrize("own_session", [True, False])
async def test_close_behavior_based_on_ownership(mock_session, own_session):
    """Downloader only closes session it owns."""
    downloader = Downloader(mock_session, own_session=own_session)

    await downloader.close()

    if own_session:
        mock_session.close.assert_called_once()
    else:
        mock_session.close.assert_not_called()

    assert downloader._closed is True


@pytest.mark.asyncio
async def test_multiple_close_calls_are_safe(mock_session):
    """Downloader handles multiple close() calls gracefully."""
    downloader = Downloader(mock_session, own_session=True)

    await downloader.close()
    await downloader.close()  # Should not error or double-close

    # Session close should only be called once
    assert mock_session.close.call_count == 1
    assert downloader._closed is True


# Signal Registry


def test_downloader_has_signals(mock_session):
    """Downloader has access to signal registry."""
    downloader = Downloader(mock_session)

    assert downloader.signals is not None
    # Signal dispatcher is available
    assert hasattr(downloader, "signals")
