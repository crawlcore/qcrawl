"""Tests for qcrawl.core.queues.redis.RedisQueue

These tests require a running Redis server.
RedisQueue requires Redis 7.4+ for per-item TTL support (ZADD...EX, HEXPIRE).

Start Redis locally:
    docker run -d -p 6379:6379 redis:latest

Tests are automatically skipped if Redis server is not running on localhost:6379.
"""

import asyncio

import pytest
import redis.asyncio as aioredis

from qcrawl.core.queues.redis import RedisQueue
from qcrawl.core.request import Request


async def is_redis_running(url: str = "redis://localhost:6379/0") -> bool:
    """Check if Redis server is running."""
    try:
        client = aioredis.from_url(url, decode_responses=False)
        await client.ping()
        await client.aclose()
        return True
    except Exception:
        return False


@pytest.fixture
async def redis_queue():
    """Fixture providing a clean RedisQueue for testing."""
    # Check if Redis is running
    if not await is_redis_running():
        pytest.skip("Redis server not running on localhost:6379")

    # Create queue with test namespace
    queue = RedisQueue(
        url="redis://localhost:6379/0",
        namespace="qcrawl_test",
        dedupe=False,  # Start with simple non-deduping queue
    )

    # Clear any existing test data
    await queue.clear()

    yield queue

    # Cleanup
    await queue.clear()
    await queue.close()


@pytest.mark.asyncio
async def test_redis_basic_put_get(redis_queue):
    """RedisQueue basic put and get operations."""
    req = Request(url="https://example.com/test")

    await redis_queue.put(req, priority=0)
    retrieved = await redis_queue.get()

    assert retrieved.url == req.url


@pytest.mark.asyncio
async def test_redis_priority_ordering(redis_queue):
    """RedisQueue returns items in priority order (higher priority first)."""
    r_low = Request(url="https://example.com/low")
    r_high = Request(url="https://example.com/high")
    r_mid = Request(url="https://example.com/mid")

    # Add in mixed order
    await redis_queue.put(r_mid, priority=5)
    await redis_queue.put(r_low, priority=1)
    await redis_queue.put(r_high, priority=10)  # Highest priority = first out

    # Should get in priority order (high to low)
    assert (await redis_queue.get()).url == "https://example.com/high"
    assert (await redis_queue.get()).url == "https://example.com/mid"
    assert (await redis_queue.get()).url == "https://example.com/low"


@pytest.mark.asyncio
async def test_redis_size(redis_queue):
    """RedisQueue tracks size correctly."""
    assert await redis_queue.size() == 0

    await redis_queue.put(Request(url="https://example.com/1"), priority=0)
    await redis_queue.put(Request(url="https://example.com/2"), priority=0)

    assert await redis_queue.size() == 2

    await redis_queue.get()
    assert await redis_queue.size() == 1


@pytest.mark.asyncio
async def test_redis_clear(redis_queue):
    """RedisQueue clear removes all items."""
    await redis_queue.put(Request(url="https://example.com/1"), priority=0)
    await redis_queue.put(Request(url="https://example.com/2"), priority=0)

    assert await redis_queue.size() == 2

    await redis_queue.clear()
    assert await redis_queue.size() == 0


@pytest.mark.asyncio
async def test_redis_with_deduplication():
    """RedisQueue with dedupe enabled prevents duplicate requests."""
    if not await is_redis_running():
        pytest.skip("Redis server not running")

    queue = RedisQueue(
        url="redis://localhost:6379/0",
        namespace="qcrawl_test_dedupe",
        dedupe=True,  # Enable deduplication
    )

    try:
        await queue.clear()

        req = Request(url="https://example.com/same")

        # First put should succeed
        await queue.put(req, priority=0)
        assert await queue.size() == 1

        # Second put of same URL should be deduplicated
        await queue.put(req, priority=0)
        assert await queue.size() == 1  # Still only 1 item

        # Get should return the single item
        retrieved = await queue.get()
        assert retrieved.url == req.url
        assert await queue.size() == 0

    finally:
        await queue.clear()
        await queue.close()


@pytest.mark.asyncio
async def test_redis_serialization_roundtrip(redis_queue):
    """RedisQueue correctly serializes and deserializes complex requests."""
    req = Request(
        url="https://example.com/complex",
        method="POST",
        headers={"User-Agent": "test", "X-Custom": "value"},
        meta={"depth": 2, "custom": "data"},
        body=b"request body",
        priority=5,
    )

    await redis_queue.put(req, priority=req.priority)
    retrieved = await redis_queue.get()

    assert retrieved.url == req.url
    assert retrieved.method == req.method
    assert retrieved.headers == req.headers
    assert retrieved.meta == req.meta
    assert retrieved.body == req.body


@pytest.mark.asyncio
async def test_redis_empty_get_timeout():
    """RedisQueue.get() with timeout on empty queue."""
    if not await is_redis_running():
        pytest.skip("Redis server not running")

    queue = RedisQueue(url="redis://localhost:6379/0", namespace="qcrawl_test_timeout")

    try:
        await queue.clear()

        # Getting from empty queue should timeout/raise
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(queue.get(), timeout=0.5)

    finally:
        await queue.close()
