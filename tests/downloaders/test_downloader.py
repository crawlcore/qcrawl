"""Production-quality tests for qcrawl.core.downloader.HTTPDownloader"""

from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from qcrawl.downloaders import HTTPDownloader


@pytest.fixture
def mock_session():
    """Provide a mock aiohttp ClientSession."""
    session = Mock(spec=aiohttp.ClientSession)
    session.close = AsyncMock()
    session.closed = False
    return session


# Initialization Tests


def test_downloader_init_with_owned_session(mock_session):
    """HTTPDownloader initializes correctly when it owns the session."""
    downloader = HTTPDownloader(mock_session, own_session=True)

    assert downloader._session is mock_session
    assert downloader._own_session is True
    assert downloader._closed is False
    assert downloader.signals is not None


def test_downloader_init_with_external_session(mock_session):
    """HTTPDownloader initializes correctly with external session."""
    downloader = HTTPDownloader(mock_session, own_session=False)

    assert downloader._session is mock_session
    assert downloader._own_session is False
    assert downloader._closed is False


# Factory Method Test


@pytest.mark.asyncio
async def test_downloader_create_factory():
    """HTTPDownloader.create() produces a working instance."""
    downloader = await HTTPDownloader.create()

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
    """HTTPDownloader only closes session it owns."""
    downloader = HTTPDownloader(mock_session, own_session=own_session)

    await downloader.close()

    if own_session:
        mock_session.close.assert_called_once()
    else:
        mock_session.close.assert_not_called()

    assert downloader._closed is True


@pytest.mark.asyncio
async def test_multiple_close_calls_are_safe(mock_session):
    """HTTPDownloader handles multiple close() calls gracefully."""
    downloader = HTTPDownloader(mock_session, own_session=True)

    await downloader.close()
    await downloader.close()  # Should not error or double-close

    # Session close should only be called once
    assert mock_session.close.call_count == 1
    assert downloader._closed is True


# Signal Registry


def test_downloader_has_signals(mock_session):
    """HTTPDownloader has access to signal registry."""
    downloader = HTTPDownloader(mock_session)

    assert downloader.signals is not None
    # Signal dispatcher is available
    assert hasattr(downloader, "signals")


@pytest.mark.asyncio
async def test_headers_received_emitted_before_body(mock_session, monkeypatch):
    """headers_received fires (with the response headers) before the body is read."""
    import qcrawl.downloaders.http as http_mod
    from qcrawl import signals
    from qcrawl.core.request import Request

    events: list[str] = []
    captured_headers: list[dict[str, str]] = []

    class FakeResp:
        headers = {"Content-Type": "text/html", "Content-Length": "123"}
        status = 200

    class FakeRequestCM:
        async def __aenter__(self):
            return FakeResp()

        async def __aexit__(self, *exc):
            return False

    mock_session.request = Mock(return_value=FakeRequestCM())

    async def fake_from_response(resp, request=None):
        events.append("body_read")  # marks the point the body would be read
        page = Mock()
        page.headers = dict(resp.headers)
        page.content = b"<html></html>"
        return page

    monkeypatch.setattr(http_mod.Page, "from_response", fake_from_response)

    downloader = HTTPDownloader(mock_session, own_session=False)

    async def on_headers(sender, headers=None, request=None, **kwargs):
        events.append("headers")
        captured_headers.append(headers)

    async def on_bytes(sender, data=None, request=None, **kwargs):
        events.append("bytes")

    signals.signals_registry.connect("headers_received", on_headers, sender=downloader, weak=False)
    signals.signals_registry.connect("bytes_received", on_bytes, sender=downloader, weak=False)
    try:
        await downloader.fetch(Request(url="https://example.com"))
    finally:
        signals.signals_registry.disconnect("headers_received", on_headers, sender=downloader)
        signals.signals_registry.disconnect("bytes_received", on_bytes, sender=downloader)

    assert events == ["headers", "body_read", "bytes"]
    assert captured_headers[0]["Content-Type"] == "text/html"
