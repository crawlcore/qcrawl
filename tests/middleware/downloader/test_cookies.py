"""Tests for CookiesMiddleware."""

import pytest

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.cookies import CookiesMiddleware

# Initialization Tests


def test_middleware_init():
    """CookiesMiddleware initializes with empty cookie storage."""
    middleware = CookiesMiddleware()

    assert middleware._cookies == {}


# Helper Method Tests


def test_get_domain_extracts_domain(spider):
    """_get_domain extracts domain from URL."""
    middleware = CookiesMiddleware()

    domain = middleware._get_domain("https://example.com/path")

    assert domain == "example.com"


def test_get_spider_id_uses_name(spider):
    """_get_spider_id uses spider.name when available."""
    middleware = CookiesMiddleware()
    spider.name = "test_spider"

    spider_id = middleware._get_spider_id(spider)

    assert spider_id == "test_spider"


def test_get_spider_id_fallback_to_id():
    """_get_spider_id falls back to id() when no name attribute."""
    middleware = CookiesMiddleware()

    # Create a simple mock object without a name attribute
    class MockSpider:
        pass

    mock_spider = MockSpider()
    spider_id = middleware._get_spider_id(mock_spider)  # type: ignore[arg-type]

    assert spider_id == id(mock_spider)


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_no_cookies_continues(spider):
    """process_request continues when no cookies are stored."""
    middleware = CookiesMiddleware()
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Cookie" not in request.headers


@pytest.mark.asyncio
async def test_process_request_adds_cookies_to_header(spider):
    """process_request adds cookies to request headers."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"

    # Manually store a cookie
    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    middleware._cookies[spider_id][domain] = cookie

    request = Request(url="https://example.com/page")
    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Cookie" in request.headers
    assert request.headers["Cookie"] == "session_id=abc123"


@pytest.mark.asyncio
async def test_process_request_adds_multiple_cookies(spider):
    """process_request adds multiple cookies to request."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"

    # Store multiple cookies
    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    cookie["user_id"] = "user456"
    middleware._cookies[spider_id][domain] = cookie

    request = Request(url="https://example.com/page")
    await middleware.process_request(request, spider)

    # Check both cookies are in header (order may vary)
    cookie_header = request.headers["Cookie"]
    assert "session_id=abc123" in cookie_header
    assert "user_id=user456" in cookie_header


@pytest.mark.asyncio
async def test_process_request_preserves_existing_headers(spider):
    """process_request preserves existing request headers."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"

    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    middleware._cookies[spider_id][domain] = cookie

    request = Request(url="https://example.com/page", headers={"User-Agent": "TestBot"})
    await middleware.process_request(request, spider)

    assert request.headers["User-Agent"] == "TestBot"
    assert request.headers["Cookie"] == "session_id=abc123"


# process_response Tests


@pytest.mark.asyncio
async def test_process_response_no_set_cookie_keeps_response(spider):
    """process_response keeps response when no Set-Cookie header."""
    middleware = CookiesMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={"Content-Type": "text/html"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_stores_cookie(spider):
    """process_response stores Set-Cookie from response."""
    middleware = CookiesMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={"Set-Cookie": "session_id=abc123"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"
    assert "session_id" in middleware._cookies[spider_id][domain]
    assert middleware._cookies[spider_id][domain]["session_id"].value == "abc123"


@pytest.mark.asyncio
async def test_process_response_stores_multiple_set_cookies(spider):
    """process_response stores multiple Set-Cookie headers."""
    middleware = CookiesMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={
            "Set-Cookie": "session_id=abc123",
            "set-cookie": "user_id=user456",  # Case-insensitive
        },
        request=request,
    )

    await middleware.process_response(request, response, spider)

    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"
    cookie_jar = middleware._cookies[spider_id][domain]
    assert "session_id" in cookie_jar
    assert "user_id" in cookie_jar


@pytest.mark.asyncio
async def test_process_response_updates_existing_cookie(spider):
    """process_response updates existing cookies."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"

    # Store initial cookie
    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "old_value"
    middleware._cookies[spider_id][domain] = cookie

    # Response with updated cookie
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={"Set-Cookie": "session_id=new_value"},
        request=request,
    )

    await middleware.process_response(request, response, spider)

    assert middleware._cookies[spider_id][domain]["session_id"].value == "new_value"


@pytest.mark.asyncio
async def test_process_response_handles_invalid_cookie(spider):
    """process_response handles invalid Set-Cookie header gracefully."""
    middleware = CookiesMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={"Set-Cookie": "invalid cookie format!!!"},
        request=request,
    )

    # Should not raise
    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_continues(spider):
    """process_exception continues without special handling."""
    middleware = CookiesMiddleware()
    request = Request(url="https://example.com/page")
    exception = Exception("Test error")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.CONTINUE


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_resets_cookies(spider):
    """open_spider resets cookie jar for spider."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"

    # Store some cookies
    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    middleware._cookies[spider_id][domain] = cookie

    # Open spider should reset
    await middleware.open_spider(spider)

    # Cookies should be cleared
    assert spider_id in middleware._cookies
    assert middleware._cookies[spider_id] == {}


@pytest.mark.asyncio
async def test_open_spider_handles_errors(spider):
    """open_spider handles errors gracefully."""
    middleware = CookiesMiddleware()

    # Should not raise even if something goes wrong
    await middleware.open_spider(spider)


# close_spider Tests


@pytest.mark.asyncio
async def test_close_spider_clears_cookies(spider):
    """close_spider removes spider's cookies."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)
    domain = "example.com"

    # Store some cookies
    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    middleware._cookies[spider_id][domain] = cookie

    # Close spider
    await middleware.close_spider(spider)

    # Spider cookies should be removed
    assert spider_id not in middleware._cookies


# clear_cookies Tests


def test_clear_cookies_all(spider):
    """clear_cookies clears all cookies when no spider specified."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)

    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    middleware._cookies[spider_id]["example.com"] = cookie

    middleware.clear_cookies()

    assert middleware._cookies == {}


def test_clear_cookies_for_spider(spider):
    """clear_cookies clears all cookies for specific spider."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)

    from http.cookies import SimpleCookie

    cookie = SimpleCookie()
    cookie["session_id"] = "abc123"
    middleware._cookies[spider_id]["example.com"] = cookie

    middleware.clear_cookies(spider=spider)

    assert middleware._cookies[spider_id] == {}


def test_clear_cookies_for_domain(spider):
    """clear_cookies clears cookies for specific domain."""
    middleware = CookiesMiddleware()
    spider_id = middleware._get_spider_id(spider)

    from http.cookies import SimpleCookie

    cookie1 = SimpleCookie()
    cookie1["session_id"] = "abc123"
    middleware._cookies[spider_id]["example.com"] = cookie1

    cookie2 = SimpleCookie()
    cookie2["user_id"] = "user456"
    middleware._cookies[spider_id]["other.com"] = cookie2

    middleware.clear_cookies(spider=spider, domain="example.com")

    assert "example.com" not in middleware._cookies[spider_id]
    assert "other.com" in middleware._cookies[spider_id]


# Integration Tests


@pytest.mark.asyncio
async def test_cookie_persistence_across_requests(spider):
    """Cookies are persisted across multiple requests to same domain."""
    middleware = CookiesMiddleware()

    # First request receives cookie
    request1 = Request(url="https://example.com/login")
    response1 = Page(
        url="https://example.com/login",
        content=b"<html></html>",
        status_code=200,
        headers={"Set-Cookie": "session_id=abc123"},
        request=request1,
    )
    await middleware.process_response(request1, response1, spider)

    # Second request should include the cookie
    request2 = Request(url="https://example.com/profile")
    await middleware.process_request(request2, spider)

    assert "Cookie" in request2.headers
    assert "session_id=abc123" in request2.headers["Cookie"]


@pytest.mark.asyncio
async def test_cookies_isolated_per_spider(spider):
    """Cookies are isolated per spider instance."""
    from qcrawl.core.spider import Spider

    class OtherSpider(Spider):
        name = "other"
        start_urls = ["https://example.com"]

        async def parse(self, response):
            yield {"url": response.url}

    middleware = CookiesMiddleware()
    spider1 = spider
    spider2 = OtherSpider()

    # Store cookie for spider1
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={"Set-Cookie": "session_id=spider1_cookie"},
        request=request,
    )
    await middleware.process_response(request, response, spider1)

    # Request from spider2 should not have spider1's cookies
    request2 = Request(url="https://example.com/page")
    await middleware.process_request(request2, spider2)

    assert "Cookie" not in request2.headers


@pytest.mark.asyncio
async def test_cookies_isolated_per_domain(spider):
    """Cookies are isolated per domain."""
    middleware = CookiesMiddleware()

    # Store cookie for example.com
    request1 = Request(url="https://example.com/page")
    response1 = Page(
        url="https://example.com/page",
        content=b"<html></html>",
        status_code=200,
        headers={"Set-Cookie": "session_id=example_cookie"},
        request=request1,
    )
    await middleware.process_response(request1, response1, spider)

    # Request to other.com should not have example.com cookies
    request2 = Request(url="https://other.com/page")
    await middleware.process_request(request2, spider)

    assert "Cookie" not in request2.headers
