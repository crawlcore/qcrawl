"""Tests for RetryMiddleware."""

import aiohttp
import pytest

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.retry import RetryMiddleware

# Initialization Tests


def test_middleware_init_default():
    """RetryMiddleware initializes with defaults."""
    middleware = RetryMiddleware()

    assert middleware.max_retries == 3
    assert middleware.retry_http_codes == {429, 500, 502, 503, 504}
    assert middleware.priority_adjust == -1
    assert middleware.backoff_base == 1.0
    assert middleware.backoff_max == 60.0
    assert middleware.backoff_jitter == 0.3


def test_middleware_init_custom():
    """RetryMiddleware initializes with custom parameters."""
    middleware = RetryMiddleware(
        max_retries=5,
        retry_http_codes=[503, 504],
        priority_adjust=-2,
        backoff_base=2.0,
        backoff_max=120.0,
        backoff_jitter=0.5,
    )

    assert middleware.max_retries == 5
    assert middleware.retry_http_codes == {503, 504}
    assert middleware.priority_adjust == -2
    assert middleware.backoff_base == 2.0
    assert middleware.backoff_max == 120.0
    assert middleware.backoff_jitter == 0.5


# Delay Computation Tests


def test_compute_delay_exponential_backoff():
    """_compute_delay uses exponential backoff."""
    middleware = RetryMiddleware(backoff_base=1.0, backoff_jitter=0.0)

    # retry_count=0 -> 1.0 * 2^0 = 1.0
    assert middleware._compute_delay(0) == 1.0
    # retry_count=1 -> 1.0 * 2^1 = 2.0
    assert middleware._compute_delay(1) == 2.0
    # retry_count=2 -> 1.0 * 2^2 = 4.0
    assert middleware._compute_delay(2) == 4.0


def test_compute_delay_respects_max():
    """_compute_delay respects backoff_max."""
    middleware = RetryMiddleware(backoff_base=1.0, backoff_max=5.0, backoff_jitter=0.0)

    # retry_count=10 -> 1.0 * 2^10 = 1024, capped at 5.0
    assert middleware._compute_delay(10) == 5.0


def test_compute_delay_uses_retry_after_header():
    """_compute_delay uses Retry-After header when provided."""
    middleware = RetryMiddleware(backoff_base=1.0, backoff_jitter=0.0)

    delay = middleware._compute_delay(0, header_retry_after=10)

    assert delay == 10.0


def test_compute_delay_jitter_varies():
    """_compute_delay with jitter produces varied delays."""
    middleware = RetryMiddleware(backoff_base=10.0, backoff_jitter=0.3)

    delays = [middleware._compute_delay(0) for _ in range(10)]

    # All delays should be within [7.0, 13.0] (10 +/- 30%)
    assert all(7.0 <= d <= 13.0 for d in delays)
    # Should have some variation
    assert len(set(delays)) > 1


# Retry Count Helpers


def test_get_retry_count_default_zero(spider):
    """_get_retry_count returns 0 when no retry_count in meta."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page")

    count = middleware._get_retry_count(request)

    assert count == 0


def test_get_retry_count_reads_from_meta(spider):
    """_get_retry_count reads retry_count from meta."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page", meta={"retry_count": 2})

    count = middleware._get_retry_count(request)

    assert count == 2


def test_get_retry_count_rejects_non_int(spider):
    """_get_retry_count raises TypeError for non-int retry_count."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page", meta={"retry_count": "2"})

    with pytest.raises(TypeError, match="retry_count must be int"):
        middleware._get_retry_count(request)


# Make Retry Request Tests


def test_make_retry_request_increments_count(spider):
    """_make_retry_request increments retry_count."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page", meta={"retry_count": 1})

    retry_request = middleware._make_retry_request(request, 2.0)

    assert retry_request.meta["retry_count"] == 2
    assert retry_request.meta["retry_delay"] == 2.0


def test_make_retry_request_adjusts_priority(spider):
    """_make_retry_request adjusts priority."""
    middleware = RetryMiddleware(priority_adjust=-2)
    request = Request(url="https://example.com/page", priority=10)

    retry_request = middleware._make_retry_request(request, 1.0)

    assert retry_request.priority == 8


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_continues(spider):
    """process_request always continues."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_retries_client_error(spider):
    """process_exception retries on aiohttp.ClientError."""
    middleware = RetryMiddleware(max_retries=3)
    request = Request(url="https://example.com/page")
    exception = aiohttp.ClientError("Connection failed")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.RETRY
    retry_request = result.payload
    assert retry_request.meta["retry_count"] == 1


@pytest.mark.asyncio
async def test_process_exception_retries_timeout_error(spider):
    """process_exception retries on asyncio.TimeoutError."""
    middleware = RetryMiddleware(max_retries=3)
    request = Request(url="https://example.com/page")
    exception = TimeoutError()

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.RETRY


@pytest.mark.asyncio
async def test_process_exception_drops_after_max_retries(spider):
    """process_exception drops request after max_retries."""
    middleware = RetryMiddleware(max_retries=2)
    request = Request(url="https://example.com/page", meta={"retry_count": 2})
    exception = aiohttp.ClientError("Connection failed")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.DROP


@pytest.mark.asyncio
async def test_process_exception_continues_for_non_transient(spider):
    """process_exception continues for non-transient exceptions."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page")
    exception = ValueError("Not a network error")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.CONTINUE


# process_response Tests


@pytest.mark.asyncio
async def test_process_response_keeps_success(spider):
    """process_response keeps successful responses."""
    middleware = RetryMiddleware()
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
async def test_process_response_retries_500(spider):
    """process_response retries 500 status."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"error",
        status_code=500,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY
    retry_request = result.payload
    assert retry_request.meta["retry_count"] == 1


@pytest.mark.asyncio
async def test_process_response_retries_503(spider):
    """process_response retries 503 status."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"service unavailable",
        status_code=503,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY


@pytest.mark.asyncio
async def test_process_response_honors_retry_after(spider):
    """process_response honors Retry-After header."""
    middleware = RetryMiddleware(backoff_jitter=0.0)
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"rate limited",
        status_code=429,
        headers={"Retry-After": "30"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    retry_request = result.payload
    assert retry_request.meta["retry_delay"] == 30.0


@pytest.mark.asyncio
async def test_process_response_keeps_after_max_retries(spider):
    """process_response keeps response after max_retries."""
    middleware = RetryMiddleware(max_retries=2)
    request = Request(url="https://example.com/page", meta={"retry_count": 2})
    response = Page(
        url="https://example.com/page",
        content=b"error",
        status_code=500,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_ignores_non_retry_codes(spider):
    """process_response keeps responses with non-retry status codes."""
    middleware = RetryMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"not found",
        status_code=404,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_config(spider, caplog):
    """open_spider logs configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = RetryMiddleware(
        max_retries=5,
        retry_http_codes=[500, 503],
        priority_adjust=-2,
        backoff_base=2.0,
        backoff_max=120.0,
        backoff_jitter=0.5,
    )

    await middleware.open_spider(spider)

    assert "max_retries=5" in caplog.text
    assert "retry_http_codes=[500, 503]" in caplog.text
    assert "priority_adjust=-2" in caplog.text
    assert "backoff_base=2.000" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_retry_flow_network_error(spider):
    """Full retry flow for network errors."""
    middleware = RetryMiddleware(max_retries=3, backoff_jitter=0.0)

    # First attempt fails with network error
    request = Request(url="https://example.com/page")
    exception = aiohttp.ClientError("Connection failed")
    result = await middleware.process_exception(request, exception, spider)

    # Should retry
    assert result.action == Action.RETRY
    retry_request = result.payload
    assert retry_request.meta["retry_count"] == 1
    assert retry_request.meta["retry_delay"] == 1.0  # 1.0 * 2^0


@pytest.mark.asyncio
async def test_retry_flow_http_error(spider):
    """Full retry flow for HTTP errors."""
    middleware = RetryMiddleware(max_retries=3, backoff_jitter=0.0)

    # First attempt gets 503
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"",
        status_code=503,
        headers={},
        request=request,
    )
    result = await middleware.process_response(request, response, spider)

    # Should retry
    assert result.action == Action.RETRY
    retry_request = result.payload
    assert retry_request.meta["retry_count"] == 1
