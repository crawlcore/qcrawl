"""Tests for HttpErrorMiddleware."""

import pytest

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.spider.httperror import HttpErrorMiddleware, IgnoreResponse

# Initialization Tests


def test_middleware_init_default():
    """HttpErrorMiddleware initializes with default allowed codes."""
    middleware = HttpErrorMiddleware()

    assert middleware.default_allowed_codes == list(range(200, 400))


def test_middleware_init_custom():
    """HttpErrorMiddleware initializes with custom allowed codes."""
    middleware = HttpErrorMiddleware(allowed_codes=[200, 201, 404])

    assert middleware.default_allowed_codes == [200, 201, 404]


# Helper Method Tests


def test_get_allowed_codes_default(spider):
    """_get_allowed_codes returns default when no spider config."""
    middleware = HttpErrorMiddleware()

    allowed = middleware._get_allowed_codes(spider)

    assert allowed == set(range(200, 400))


def test_get_allowed_codes_from_spider_list(spider):
    """_get_allowed_codes reads list from spider."""
    middleware = HttpErrorMiddleware()
    spider.HTTPERROR_ALLOWED_CODES = [200, 404, 500]

    allowed = middleware._get_allowed_codes(spider)

    assert allowed == {200, 404, 500}


def test_get_allowed_codes_from_spider_single(spider):
    """_get_allowed_codes handles single code from spider."""
    middleware = HttpErrorMiddleware()
    spider.HTTPERROR_ALLOWED_CODES = 404

    allowed = middleware._get_allowed_codes(spider)

    assert allowed == {404}


def test_get_allowed_codes_allow_all(spider):
    """_get_allowed_codes returns None when HTTPERROR_ALLOW_ALL."""
    middleware = HttpErrorMiddleware()
    spider.HTTPERROR_ALLOW_ALL = True

    allowed = middleware._get_allowed_codes(spider)

    assert allowed is None


def test_should_filter_allowed_status(spider):
    """_should_filter returns False for allowed status."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=200,
        headers={},
        request=request,
    )

    should_filter = middleware._should_filter(response, {200, 404})

    assert should_filter is False


def test_should_filter_disallowed_status(spider):
    """_should_filter returns True for disallowed status."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=500,
        headers={},
        request=request,
    )

    should_filter = middleware._should_filter(response, {200, 404})

    assert should_filter is True


def test_should_filter_allow_all(spider):
    """_should_filter returns False when allowed_codes is None."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=500,
        headers={},
        request=request,
    )

    should_filter = middleware._should_filter(response, None)

    assert should_filter is False


# process_spider_input Tests


@pytest.mark.asyncio
async def test_process_spider_input_allows_success(spider):
    """process_spider_input allows 2xx responses."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"success",
        status_code=200,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert result is None


@pytest.mark.asyncio
async def test_process_spider_input_allows_3xx(spider):
    """process_spider_input allows 3xx redirects."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=301,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert result is None


@pytest.mark.asyncio
async def test_process_spider_input_filters_4xx(spider):
    """process_spider_input filters 4xx errors."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"not found",
        status_code=404,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert isinstance(result, IgnoreResponse)


@pytest.mark.asyncio
async def test_process_spider_input_filters_5xx(spider):
    """process_spider_input filters 5xx errors."""
    middleware = HttpErrorMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"server error",
        status_code=500,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert isinstance(result, IgnoreResponse)


@pytest.mark.asyncio
async def test_process_spider_input_respects_spider_config(spider):
    """process_spider_input respects spider's allowed codes."""
    middleware = HttpErrorMiddleware()
    spider.HTTPERROR_ALLOWED_CODES = [200, 404]

    # 404 should be allowed
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"not found",
        status_code=404,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert result is None


@pytest.mark.asyncio
async def test_process_spider_input_allow_all(spider):
    """process_spider_input allows all when HTTPERROR_ALLOW_ALL=True."""
    middleware = HttpErrorMiddleware()
    spider.HTTPERROR_ALLOW_ALL = True

    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"error",
        status_code=500,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert result is None


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_allowed_codes(spider, caplog):
    """open_spider logs allowed status codes."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = HttpErrorMiddleware(allowed_codes=[200, 201, 404])

    await middleware.open_spider(spider)

    assert "allowed status codes:" in caplog.text
    assert "200" in caplog.text


@pytest.mark.asyncio
async def test_open_spider_logs_allow_all(spider, caplog):
    """open_spider logs when all codes allowed."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = HttpErrorMiddleware()
    spider.HTTPERROR_ALLOW_ALL = True

    await middleware.open_spider(spider)

    assert "all status codes allowed" in caplog.text


# close_spider Tests


@pytest.mark.asyncio
async def test_close_spider_records_stats(spider):
    """close_spider records filtered count in stats."""
    middleware = HttpErrorMiddleware()

    # Simulate filtering some responses
    request1 = Request(url="https://example.com/page1")
    response1 = Page(
        url="https://example.com/page1",
        content=b"",
        status_code=404,
        headers={},
        request=request1,
    )
    await middleware.process_spider_input(response1, spider)

    request2 = Request(url="https://example.com/page2")
    response2 = Page(
        url="https://example.com/page2",
        content=b"",
        status_code=500,
        headers={},
        request=request2,
    )
    await middleware.process_spider_input(response2, spider)

    # Close spider
    await middleware.close_spider(spider)

    # Should have recorded 2 filtered responses
    assert middleware._filtered_count == 2


# Integration Tests


@pytest.mark.asyncio
async def test_filters_multiple_error_codes(spider):
    """Middleware filters various error codes."""
    middleware = HttpErrorMiddleware()

    # Test various status codes
    test_cases = [
        (200, False),  # 2xx allowed
        (301, False),  # 3xx allowed
        (404, True),  # 4xx filtered
        (500, True),  # 5xx filtered
    ]

    for status, should_be_filtered in test_cases:
        request = Request(url=f"https://example.com/status{status}")
        response = Page(
            url=f"https://example.com/status{status}",
            content=b"",
            status_code=status,
            headers={},
            request=request,
        )

        result = await middleware.process_spider_input(response, spider)

        if should_be_filtered:
            assert isinstance(result, IgnoreResponse), f"Status {status} should be filtered"
        else:
            assert result is None, f"Status {status} should not be filtered"


@pytest.mark.asyncio
async def test_custom_allowed_codes(spider):
    """Custom allowed codes work correctly."""
    middleware = HttpErrorMiddleware(allowed_codes=[200, 404, 500])

    # 404 and 500 should be allowed with custom config
    request404 = Request(url="https://example.com/not-found")
    response404 = Page(
        url="https://example.com/not-found",
        content=b"",
        status_code=404,
        headers={},
        request=request404,
    )
    result404 = await middleware.process_spider_input(response404, spider)
    assert result404 is None

    request500 = Request(url="https://example.com/error")
    response500 = Page(
        url="https://example.com/error",
        content=b"",
        status_code=500,
        headers={},
        request=request500,
    )
    result500 = await middleware.process_spider_input(response500, spider)
    assert result500 is None

    # But 503 should be filtered
    request503 = Request(url="https://example.com/unavailable")
    response503 = Page(
        url="https://example.com/unavailable",
        content=b"",
        status_code=503,
        headers={},
        request=request503,
    )
    result503 = await middleware.process_spider_input(response503, spider)
    assert isinstance(result503, IgnoreResponse)


def test_should_filter_missing_status_code():
    """_should_filter returns False when response has no status_code."""
    middleware = HttpErrorMiddleware()

    # Create a mock response without status_code attribute
    class MockResponse:
        pass

    response = MockResponse()
    should_filter = middleware._should_filter(response, {200, 404})  # type: ignore[arg-type]

    assert should_filter is False


@pytest.mark.asyncio
async def test_open_spider_logs_no_allowed_codes(spider, caplog):
    """open_spider logs when no codes are allowed (empty set)."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = HttpErrorMiddleware()
    # Set empty list on spider (not middleware init, which would use default)
    spider.HTTPERROR_ALLOWED_CODES = []

    await middleware.open_spider(spider)

    assert "no status codes allowed" in caplog.text


@pytest.mark.asyncio
async def test_open_spider_debug_logging(spider, caplog):
    """open_spider logs status code ranges at debug level."""
    import logging

    caplog.set_level(logging.DEBUG)
    middleware = HttpErrorMiddleware(allowed_codes=[200, 201, 202, 404, 500, 501, 502])

    await middleware.open_spider(spider)

    # Should log ranges in debug mode
    assert "HttpErrorMiddleware: allowed_codes=" in caplog.text
    # Should show ranges like "200-202" and individual codes
    log_text = caplog.text
    assert "200-202" in log_text or ("200" in log_text and "201" in log_text)


@pytest.mark.asyncio
async def test_process_spider_input_sends_signal(spider):
    """process_spider_input sends request_dropped signal when filtering."""
    middleware = HttpErrorMiddleware()

    # Track signal emissions
    signal_calls = []

    class MockSignalDispatcher:
        async def send_async(self, signal_name, **kwargs):
            signal_calls.append((signal_name, kwargs))

    spider.signals = MockSignalDispatcher()

    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"error",
        status_code=500,
        headers={},
        request=request,
    )

    result = await middleware.process_spider_input(response, spider)

    assert isinstance(result, IgnoreResponse)
    assert len(signal_calls) == 1
    assert signal_calls[0][0] == "request_dropped"
    assert signal_calls[0][1]["request"] is request


@pytest.mark.asyncio
async def test_process_spider_input_handles_signal_error(spider, caplog):
    """process_spider_input handles signal sending errors gracefully."""
    import logging

    caplog.set_level(logging.ERROR)
    middleware = HttpErrorMiddleware()

    # Mock dispatcher that raises exception
    class BrokenSignalDispatcher:
        async def send_async(self, signal_name, **kwargs):
            raise RuntimeError("Signal dispatcher error")

    spider.signals = BrokenSignalDispatcher()

    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"error",
        status_code=404,
        headers={},
        request=request,
    )

    # Should still filter the response despite signal error
    result = await middleware.process_spider_input(response, spider)

    assert isinstance(result, IgnoreResponse)
    # Should log the error
    assert "Error sending request_dropped signal" in caplog.text
