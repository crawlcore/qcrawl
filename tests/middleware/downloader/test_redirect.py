"""Tests for RedirectMiddleware."""

import pytest

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.redirect import RedirectMiddleware

# Initialization Tests


def test_middleware_init_default():
    """RedirectMiddleware initializes with default max_redirects."""
    middleware = RedirectMiddleware()

    assert middleware.max_redirects == 10


def test_middleware_init_custom_max_redirects():
    """RedirectMiddleware initializes with custom max_redirects."""
    middleware = RedirectMiddleware(max_redirects=5)

    assert middleware.max_redirects == 5


def test_middleware_init_rejects_non_int():
    """RedirectMiddleware rejects non-integer max_redirects."""
    with pytest.raises(TypeError, match="max_redirects must be int"):
        RedirectMiddleware(max_redirects="5")  # type: ignore[arg-type]


def test_middleware_init_rejects_zero():
    """RedirectMiddleware rejects max_redirects less than 1."""
    with pytest.raises(ValueError, match="max_redirects must be >= 1"):
        RedirectMiddleware(max_redirects=0)


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_continues(spider):
    """process_request always continues."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_continues(spider):
    """process_exception always continues."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/page")
    exception = Exception("Test error")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.CONTINUE


# process_response Tests - Non-Redirect


@pytest.mark.asyncio
async def test_process_response_keeps_non_redirect(spider):
    """process_response keeps non-redirect responses."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"success",
        status_code=200,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_keeps_redirect_without_location(spider):
    """process_response keeps redirect without Location header."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=301,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


# process_response Tests - Basic Redirect


@pytest.mark.asyncio
async def test_process_response_handles_301_redirect(spider):
    """process_response retries 301 redirect."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old")
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY
    redirect_request = result.payload
    assert redirect_request.url == "https://example.com/new"
    assert redirect_request.method == "GET"
    assert redirect_request.meta["redirects"] == 1
    assert redirect_request.meta["redirect_urls"] == ["https://example.com/old"]


@pytest.mark.asyncio
async def test_process_response_handles_302_redirect(spider):
    """process_response retries 302 redirect."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old")
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=302,
        headers={"Location": "/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY
    redirect_request = result.payload
    assert redirect_request.url == "https://example.com/new"


@pytest.mark.asyncio
async def test_process_response_handles_303_redirect(spider):
    """process_response retries 303 redirect."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old", method="POST")
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=303,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY
    redirect_request = result.payload
    assert redirect_request.method == "GET"
    assert redirect_request.body is None


# process_response Tests - 307/308 Preserve Method


@pytest.mark.asyncio
async def test_process_response_307_preserves_method(spider):
    """process_response preserves method for 307 redirect."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old", method="POST", body=b"data")
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=307,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY
    redirect_request = result.payload
    assert redirect_request.method == "POST"
    assert redirect_request.body == b"data"


@pytest.mark.asyncio
async def test_process_response_308_preserves_method(spider):
    """process_response preserves method for 308 redirect."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old", method="PUT", body=b"data")
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=308,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    redirect_request = result.payload
    assert redirect_request.method == "PUT"
    assert redirect_request.body == b"data"


# process_response Tests - Header Handling


@pytest.mark.asyncio
async def test_process_response_removes_content_headers_for_get(spider):
    """process_response removes Content-* headers when converting to GET."""
    middleware = RedirectMiddleware()
    request = Request(
        url="https://example.com/old",
        method="POST",
        body=b"data",
        headers={"Content-Type": "application/json", "Content-Length": "4"},
    )
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    redirect_request = result.payload
    assert "Content-Type" not in redirect_request.headers
    assert "Content-Length" not in redirect_request.headers


# process_response Tests - Max Redirects


@pytest.mark.asyncio
async def test_process_response_enforces_max_redirects(spider):
    """process_response stops after max_redirects."""
    middleware = RedirectMiddleware(max_redirects=2)
    request = Request(url="https://example.com/page", meta={"redirects": 2})
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/next"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    # Should keep response instead of retrying
    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_increments_redirect_count(spider):
    """process_response increments redirect count."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/page1", meta={"redirects": 1})
    response = Page(
        url="https://example.com/page1",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/page2"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    redirect_request = result.payload
    assert redirect_request.meta["redirects"] == 2


# process_response Tests - dont_redirect


@pytest.mark.asyncio
async def test_process_response_honors_dont_redirect(spider):
    """process_response honors dont_redirect meta flag."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old", meta={"dont_redirect": True})
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_dont_redirect_must_be_bool(spider):
    """process_response validates dont_redirect is bool."""
    middleware = RedirectMiddleware()
    request = Request(url="https://example.com/old", meta={"dont_redirect": "yes"})
    response = Page(
        url="https://example.com/old",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/new"},
        request=request,
    )

    with pytest.raises(TypeError, match="dont_redirect must be bool"):
        await middleware.process_response(request, response, spider)


# process_response Tests - redirect_urls Tracking


@pytest.mark.asyncio
async def test_process_response_tracks_redirect_urls(spider):
    """process_response tracks redirect URL chain."""
    middleware = RedirectMiddleware()
    request = Request(
        url="https://example.com/page2", meta={"redirect_urls": ["https://example.com/page1"]}
    )
    response = Page(
        url="https://example.com/page2",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/page3"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    redirect_request = result.payload
    assert redirect_request.meta["redirect_urls"] == [
        "https://example.com/page1",
        "https://example.com/page2",
    ]


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_config(spider, caplog):
    """open_spider logs configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = RedirectMiddleware(max_redirects=5)

    await middleware.open_spider(spider)

    assert "max_redirects: 5" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_redirect_chain(spider):
    """Multiple redirects track full chain."""
    middleware = RedirectMiddleware(max_redirects=10)

    # First redirect
    request1 = Request(url="https://example.com/page1")
    response1 = Page(
        url="https://example.com/page1",
        content=b"",
        status_code=301,
        headers={"Location": "https://example.com/page2"},
        request=request1,
    )
    result1 = await middleware.process_response(request1, response1, spider)
    request2 = result1.payload
    assert isinstance(request2, Request)

    # Second redirect
    response2 = Page(
        url="https://example.com/page2",
        content=b"",
        status_code=302,
        headers={"Location": "https://example.com/page3"},
        request=request2,
    )
    result2 = await middleware.process_response(request2, response2, spider)
    request3 = result2.payload

    # Check final state
    assert request3.url == "https://example.com/page3"
    assert request3.meta["redirects"] == 2
    assert request3.meta["redirect_urls"] == [
        "https://example.com/page1",
        "https://example.com/page2",
    ]
