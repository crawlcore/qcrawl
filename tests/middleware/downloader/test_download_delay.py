"""Tests for DownloadDelayMiddleware."""

import asyncio
import time

import pytest

from qcrawl.core.request import Request
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.download_delay import DownloadDelayMiddleware

# Note: Uses 'http_response' fixture from tests/middleware/conftest.py

# Initialization Tests


def test_middleware_init_default():
    """DownloadDelayMiddleware initializes with default delay."""
    middleware = DownloadDelayMiddleware()

    assert middleware._delay == 0.25
    assert middleware._last == {}


def test_middleware_init_custom_delay():
    """DownloadDelayMiddleware initializes with custom delay."""
    middleware = DownloadDelayMiddleware(delay_per_domain=1.5)

    assert middleware._delay == 1.5


def test_middleware_init_zero_delay():
    """DownloadDelayMiddleware accepts zero delay."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0)

    assert middleware._delay == 0.0


@pytest.mark.parametrize("invalid_value", [None, [], "invalid"])
def test_middleware_init_invalid_type(invalid_value):
    """DownloadDelayMiddleware rejects non-numeric delay."""
    with pytest.raises(TypeError, match="delay_per_domain must be a number"):
        DownloadDelayMiddleware(delay_per_domain=invalid_value)


def test_middleware_init_negative_delay():
    """DownloadDelayMiddleware rejects negative delay."""
    with pytest.raises(ValueError, match="delay_per_domain must be >= 0"):
        DownloadDelayMiddleware(delay_per_domain=-1.0)


# Domain Key Tests


def test_domain_key_extracts_domain(spider):
    """_domain_key extracts domain from URL."""
    middleware = DownloadDelayMiddleware()

    key = middleware._domain_key("https://example.com/path")

    assert key == "example.com"


def test_domain_key_fallback_on_error(spider):
    """_domain_key returns 'default' on parsing error."""
    middleware = DownloadDelayMiddleware()

    key = middleware._domain_key("not-a-url")

    assert key == "default"


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_no_delay_on_first_request(spider):
    """process_request does not delay first request to domain."""
    middleware = DownloadDelayMiddleware(delay_per_domain=1.0)
    request = Request(url="https://example.com/page")

    start = time.monotonic()
    result = await middleware.process_request(request, spider)
    elapsed = time.monotonic() - start

    assert result.action == Action.CONTINUE
    assert elapsed < 0.1  # Should be immediate


@pytest.mark.asyncio
async def test_process_request_stores_domain_key_in_meta(spider):
    """process_request stores domain key in request.meta."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")

    await middleware.process_request(request, spider)

    assert "_domain_delay_key" in request.meta
    assert request.meta["_domain_delay_key"] == "example.com"


@pytest.mark.asyncio
async def test_process_request_enforces_delay_on_second_request(spider, http_response):
    """process_request enforces delay on subsequent requests."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.1)
    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2")

    # First request - no delay
    await middleware.process_request(request1, spider)
    await middleware.process_response(request1, http_response, spider)

    # Second request - should delay
    start = time.monotonic()
    await middleware.process_request(request2, spider)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.09  # At least ~0.1s (allowing for timing precision)


@pytest.mark.asyncio
async def test_process_request_no_delay_for_different_domains(spider, http_response):
    """process_request does not delay requests to different domains."""
    middleware = DownloadDelayMiddleware(delay_per_domain=1.0)
    request1 = Request(url="https://example.com/page")
    request2 = Request(url="https://other.com/page")

    # First request
    await middleware.process_request(request1, spider)
    await middleware.process_response(request1, http_response, spider)

    # Second request to different domain - should not delay
    start = time.monotonic()
    await middleware.process_request(request2, spider)
    elapsed = time.monotonic() - start

    assert elapsed < 0.05  # Should be immediate


@pytest.mark.asyncio
async def test_process_request_honors_retry_delay_in_meta(spider, http_response):
    """process_request uses retry_delay from request.meta when present."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.05)
    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2", meta={"retry_delay": 0.15})

    # First request
    await middleware.process_request(request1, spider)
    await middleware.process_response(request1, http_response, spider)

    # Second request with higher retry_delay - should use that instead
    start = time.monotonic()
    await middleware.process_request(request2, spider)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.13  # Should use retry_delay (0.15s), allow timing precision tolerance


@pytest.mark.asyncio
async def test_process_request_uses_global_delay_when_retry_delay_smaller(spider, http_response):
    """process_request uses global delay when retry_delay is smaller."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.15)
    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2", meta={"retry_delay": 0.05})

    # First request
    await middleware.process_request(request1, spider)
    await middleware.process_response(request1, http_response, spider)

    # Second request with lower retry_delay - should use global delay
    start = time.monotonic()
    await middleware.process_request(request2, spider)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.13  # Should use global delay (0.15s, allowing for timing variance)


@pytest.mark.asyncio
async def test_process_request_ignores_invalid_retry_delay(spider, http_response):
    """process_request ignores non-numeric retry_delay."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.1)
    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2", meta={"retry_delay": "invalid"})

    # First request
    await middleware.process_request(request1, spider)
    await middleware.process_response(request1, http_response, spider)

    # Second request with invalid retry_delay - should use global delay
    start = time.monotonic()
    await middleware.process_request(request2, spider)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.09  # Should use global delay


@pytest.mark.asyncio
async def test_process_request_no_delay_when_zero(spider):
    """process_request skips delay when delay is zero."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.0)
    request = Request(url="https://example.com/page")

    start = time.monotonic()
    await middleware.process_request(request, spider)
    elapsed = time.monotonic() - start

    assert elapsed < 0.01  # Should be immediate


# process_response Tests


@pytest.mark.asyncio
async def test_process_response_updates_last_timestamp(spider, http_response):
    """process_response updates last download timestamp."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")

    await middleware.process_request(request, spider)

    before = time.monotonic()
    result = await middleware.process_response(request, http_response, spider)
    after = time.monotonic()

    assert result.action == Action.KEEP
    assert result.payload is http_response
    assert "example.com" in middleware._last
    assert before <= middleware._last["example.com"] <= after


@pytest.mark.asyncio
async def test_process_response_removes_domain_key_from_meta(spider, http_response):
    """process_response removes _domain_delay_key from meta."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")

    await middleware.process_request(request, spider)
    assert "_domain_delay_key" in request.meta

    await middleware.process_response(request, http_response, spider)

    assert "_domain_delay_key" not in request.meta


@pytest.mark.asyncio
async def test_process_response_handles_missing_meta(spider, http_response):
    """process_response handles requests without meta gracefully."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")
    request.meta = None  # type: ignore[assignment]

    # Should not raise
    result = await middleware.process_response(request, http_response, spider)

    assert result.action == Action.KEEP


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_updates_last_timestamp(spider):
    """process_exception updates last download timestamp."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")
    exception = Exception("Test error")

    await middleware.process_request(request, spider)

    before = time.monotonic()
    result = await middleware.process_exception(request, exception, spider)
    after = time.monotonic()

    assert result.action == Action.CONTINUE
    assert "example.com" in middleware._last
    assert before <= middleware._last["example.com"] <= after


@pytest.mark.asyncio
async def test_process_exception_removes_domain_key_from_meta(spider):
    """process_exception removes _domain_delay_key from meta."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")
    exception = Exception("Test error")

    await middleware.process_request(request, spider)
    assert "_domain_delay_key" in request.meta

    await middleware.process_exception(request, exception, spider)

    assert "_domain_delay_key" not in request.meta


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_delay(spider, caplog):
    """open_spider logs the delay configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = DownloadDelayMiddleware(delay_per_domain=1.5)

    await middleware.open_spider(spider)

    assert "delay_per_domain: 1.500 seconds" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_sequential_requests_enforce_delay(spider, http_response):
    """Sequential requests to same domain enforce minimum delay."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.1)
    requests = [Request(url=f"https://example.com/page{i}") for i in range(3)]

    overall_start = time.monotonic()

    for req in requests:
        await middleware.process_request(req, spider)
        await middleware.process_response(req, http_response, spider)

    overall_elapsed = time.monotonic() - overall_start

    # Should take at least 2 delays (no delay on first, then 2 x 0.1s)
    assert overall_elapsed >= 0.18


@pytest.mark.asyncio
async def test_concurrent_requests_queue_properly(spider, http_response):
    """Concurrent requests to same domain queue with proper delays."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.1)

    async def make_request(i):
        """Make a request and record timing."""
        request = Request(url=f"https://example.com/page{i}")
        await middleware.process_request(request, spider)
        await middleware.process_response(request, http_response, spider)

    start = time.monotonic()
    await asyncio.gather(make_request(1), make_request(2), make_request(3))
    elapsed = time.monotonic() - start

    # Should serialize to respect delay (at least 2 delays for 3 requests)
    assert elapsed >= 0.18


@pytest.mark.asyncio
async def test_mixed_domains_interleave(spider, http_response):
    """Requests to different domains can interleave without delay."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.5)

    async def make_request(domain, page):
        """Make a request to specific domain."""
        request = Request(url=f"https://{domain}/page{page}")
        await middleware.process_request(request, spider)
        await middleware.process_response(request, http_response, spider)

    start = time.monotonic()
    await asyncio.gather(
        make_request("example.com", 1),
        make_request("other.com", 1),
        make_request("third.com", 1),
    )
    elapsed = time.monotonic() - start

    # Should be fast since all different domains (no cross-domain delays)
    assert elapsed < 0.2


# Edge Cases


@pytest.mark.asyncio
async def test_request_without_meta_initializes_meta(spider):
    """process_request initializes meta dict when not present."""
    middleware = DownloadDelayMiddleware()
    request = Request(url="https://example.com/page")
    request.meta = None  # type: ignore[assignment]

    await middleware.process_request(request, spider)

    assert isinstance(request.meta, dict)
    assert "_domain_delay_key" in request.meta


@pytest.mark.asyncio
async def test_retry_delay_boolean_ignored(spider, http_response):
    """process_request ignores boolean retry_delay (edge case)."""
    middleware = DownloadDelayMiddleware(delay_per_domain=0.1)
    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2", meta={"retry_delay": True})

    await middleware.process_request(request1, spider)
    await middleware.process_response(request1, http_response, spider)

    # Should use global delay, not treat True as 1.0
    start = time.monotonic()
    await middleware.process_request(request2, spider)
    elapsed = time.monotonic() - start

    assert elapsed >= 0.09  # Uses global delay
