"""Tests for ConcurrencyMiddleware."""

import asyncio

import pytest

from qcrawl.core.request import Request
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.concurrency import ConcurrencyMiddleware

# Initialization Tests


def test_middleware_init_default():
    """ConcurrencyMiddleware initializes with default concurrency."""
    middleware = ConcurrencyMiddleware()

    assert middleware._concurrency == 2
    assert middleware._slots == {}


def test_middleware_init_custom_concurrency():
    """ConcurrencyMiddleware initializes with custom concurrency."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=5)

    assert middleware._concurrency == 5


@pytest.mark.parametrize("invalid_value", [0, -1, "2", None, 1.5])
def test_middleware_init_invalid_concurrency(invalid_value):
    """ConcurrencyMiddleware rejects invalid concurrency values."""
    with pytest.raises(ValueError, match="concurrency_per_domain must be an integer >= 1"):
        ConcurrencyMiddleware(concurrency_per_domain=invalid_value)


# from_crawler Tests


@pytest.mark.asyncio
async def test_from_crawler_uses_settings(crawler):
    """from_crawler creates middleware with concurrency from settings."""
    crawler.runtime_settings = crawler.runtime_settings.with_overrides(
        {"CONCURRENCY_PER_DOMAIN": 10}
    )

    middleware = ConcurrencyMiddleware.from_crawler(crawler)

    assert middleware._concurrency == 10


@pytest.mark.asyncio
async def test_from_crawler_defaults_to_2(crawler):
    """from_crawler defaults to concurrency of 2 when setting not present."""
    middleware = ConcurrencyMiddleware.from_crawler(crawler)

    assert middleware._concurrency == 2


# Domain Extraction Tests


@pytest.mark.asyncio
async def test_get_domain_extracts_correctly(spider):
    """_get_domain extracts domain from URL."""
    middleware = ConcurrencyMiddleware()

    domain = middleware._get_domain("https://example.com/path")

    assert domain == "example.com"


@pytest.mark.asyncio
async def test_get_domain_fallback_on_error(spider):
    """_get_domain returns 'default' on parsing error."""
    middleware = ConcurrencyMiddleware()

    domain = middleware._get_domain("not-a-url")

    assert domain == "default"


# Slot Management Tests


@pytest.mark.asyncio
async def test_get_slot_creates_new_slot(spider):
    """_get_slot creates new slot for unseen domain."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=3)

    slot = middleware._get_slot("https://example.com/page")

    assert slot.domain == "example.com"
    assert slot.semaphore._value == 3
    assert "example.com" in middleware._slots


@pytest.mark.asyncio
async def test_get_slot_reuses_existing_slot(spider):
    """_get_slot reuses existing slot for same domain."""
    middleware = ConcurrencyMiddleware()

    slot1 = middleware._get_slot("https://example.com/page1")
    slot2 = middleware._get_slot("https://example.com/page2")

    assert slot1 is slot2


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_acquires_slot(spider):
    """process_request acquires semaphore slot."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=2)
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    slot = middleware._get_slot(request.url)
    assert slot.semaphore._value == 1  # One slot acquired


@pytest.mark.asyncio
async def test_process_request_stores_slot_in_meta(spider):
    """process_request stores slot reference in request.meta."""
    middleware = ConcurrencyMiddleware()
    request = Request(url="https://example.com/page")

    await middleware.process_request(request, spider)

    assert "_concurrency_slot" in request.meta
    assert request.meta["_concurrency_slot"] is middleware._get_slot(request.url)


@pytest.mark.asyncio
async def test_process_request_blocks_when_limit_reached(spider):
    """process_request blocks when concurrency limit is reached."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=1)
    request1 = Request(url="https://example.com/page1")
    request2 = Request(url="https://example.com/page2")

    # First request acquires the slot
    await middleware.process_request(request1, spider)

    # Second request should block
    task = asyncio.create_task(middleware.process_request(request2, spider))
    await asyncio.sleep(0.01)  # Give task time to start

    assert not task.done()  # Task is still waiting

    # Release the slot from first request
    middleware._release_slot(request1)
    await asyncio.sleep(0.01)  # Allow second task to proceed

    assert task.done()  # Now second task can complete


# process_response Tests


@pytest.mark.asyncio
async def test_process_response_releases_slot(spider, http_response):
    """process_response releases the acquired slot."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=2)
    request = Request(url="https://example.com/page")

    # Acquire slot
    await middleware.process_request(request, spider)
    slot = middleware._get_slot(request.url)
    assert slot.semaphore._value == 1

    # Release slot
    result = await middleware.process_response(request, http_response, spider)

    assert result.action == Action.KEEP
    assert result.payload is http_response
    assert slot.semaphore._value == 2  # Slot released


@pytest.mark.asyncio
async def test_process_response_keeps_response(spider, http_response):
    """process_response keeps the response."""
    middleware = ConcurrencyMiddleware()
    request = Request(url="https://example.com/page")
    await middleware.process_request(request, spider)

    result = await middleware.process_response(request, http_response, spider)

    assert result.action == Action.KEEP
    assert result.payload is http_response


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_releases_slot(spider):
    """process_exception releases the acquired slot."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=2)
    request = Request(url="https://example.com/page")

    # Acquire slot
    await middleware.process_request(request, spider)
    slot = middleware._get_slot(request.url)
    assert slot.semaphore._value == 1

    # Release slot on exception
    exception = Exception("Test error")
    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.CONTINUE
    assert slot.semaphore._value == 2  # Slot released


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_clears_slots(spider):
    """open_spider clears all domain slots."""
    middleware = ConcurrencyMiddleware()
    request = Request(url="https://example.com/page")

    # Create some slots
    await middleware.process_request(request, spider)
    assert len(middleware._slots) > 0

    # Open spider should clear slots
    await middleware.open_spider(spider)

    assert middleware._slots == {}


# Integration Tests


@pytest.mark.asyncio
async def test_concurrent_requests_respect_limit(spider):
    """Multiple concurrent requests respect per-domain limit."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=2)
    requests = [Request(url=f"https://example.com/page{i}") for i in range(5)]

    async def process_request_delayed(req):
        """Process request and hold for a moment."""
        await middleware.process_request(req, spider)
        await asyncio.sleep(0.05)
        middleware._release_slot(req)

    # Start all 5 requests concurrently
    tasks = [asyncio.create_task(process_request_delayed(req)) for req in requests]

    # Check that no more than 2 are running at once
    await asyncio.sleep(0.01)
    slot = middleware._get_slot("https://example.com/page1")
    # At most 2 slots acquired (2 concurrency limit)
    assert slot.semaphore._value == 0

    # Wait for all to complete
    await asyncio.gather(*tasks)

    # All slots should be released
    assert slot.semaphore._value == 2


@pytest.mark.asyncio
async def test_different_domains_use_different_slots(spider):
    """Requests to different domains use separate concurrency slots."""
    middleware = ConcurrencyMiddleware(concurrency_per_domain=1)
    request1 = Request(url="https://example.com/page")
    request2 = Request(url="https://other.com/page")

    # Both requests should succeed immediately (different domains)
    await middleware.process_request(request1, spider)
    await middleware.process_request(request2, spider)

    # Check that both domains have their own slots
    assert len(middleware._slots) == 2
    assert "example.com" in middleware._slots
    assert "other.com" in middleware._slots


# Edge Cases


@pytest.mark.asyncio
async def test_release_slot_without_meta(spider):
    """_release_slot handles requests without meta gracefully."""
    middleware = ConcurrencyMiddleware()
    request = Request(url="https://example.com/page")
    request.meta = None  # type: ignore[assignment]

    # Should not raise
    middleware._release_slot(request)


@pytest.mark.asyncio
async def test_release_slot_without_slot_in_meta(spider):
    """_release_slot handles requests without slot in meta gracefully."""
    middleware = ConcurrencyMiddleware()
    request = Request(url="https://example.com/page")
    request.meta = {"other_key": "value"}

    # Should not raise
    middleware._release_slot(request)
