"""Integration tests for qcrawl.core.queues.redis.RedisQueue

Following CLAUDE.md testing approach:
- Use testcontainers for real Redis (mock at boundaries)
- Tests marked with @pytest.mark.integration
- Test actual Redis behavior, not mocked implementation

Requires Docker to run these tests.
RedisQueue requires Redis 7.4+ for per-item TTL support (ZADD...EX, HEXPIRE).
"""

import pytest
from testcontainers.redis import RedisContainer

from qcrawl.core.queues.redis import RedisQueue
from qcrawl.core.request import Request

# Fixtures


@pytest.fixture(scope="module")
def redis_server():
    """Start Redis container for testing.

    Uses testcontainers to automatically start/stop Redis in Docker.
    """
    container = RedisContainer("redis:latest")
    container.start()

    # Get connection URL
    host = container.get_container_host_ip()
    port = container.get_exposed_port(6379)
    redis_url = f"redis://{host}:{port}/0"

    yield redis_url

    container.stop()


@pytest.fixture
async def redis_queue(redis_server):
    """Fixture providing a clean RedisQueue for testing."""
    # Create queue with test namespace
    queue = RedisQueue(
        url=redis_server,
        namespace="qcrawl_test",
        dedupe=False,  # Start with simple non-deduping queue
    )

    # Clear any existing test data
    await queue.clear()

    yield queue

    # Cleanup
    await queue.clear()
    await queue.close()


@pytest.fixture
async def redis_queue_dedupe(redis_server):
    """Fixture providing RedisQueue with deduplication enabled."""
    queue = RedisQueue(
        url=redis_server,
        namespace="qcrawl_test_dedupe",
        dedupe=True,  # Enable deduplication
    )

    await queue.clear()

    yield queue

    await queue.clear()
    await queue.close()


# Integration Tests


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_basic_put_get(redis_queue):
    """RedisQueue basic put and get operations against real Redis."""
    req = Request(url="https://example.com/test")

    await redis_queue.put(req, priority=0)
    retrieved = await redis_queue.get()

    assert retrieved.url == req.url


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_priority_ordering(redis_queue):
    """RedisQueue returns items in priority order against real Redis."""
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_size(redis_queue):
    """RedisQueue tracks size correctly with real Redis."""
    assert await redis_queue.size() == 0

    await redis_queue.put(Request(url="https://example.com/1"), priority=0)
    await redis_queue.put(Request(url="https://example.com/2"), priority=0)

    assert await redis_queue.size() == 2

    await redis_queue.get()
    assert await redis_queue.size() == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_clear(redis_queue):
    """RedisQueue clear removes all items from real Redis."""
    await redis_queue.put(Request(url="https://example.com/1"), priority=0)
    await redis_queue.put(Request(url="https://example.com/2"), priority=0)

    assert await redis_queue.size() == 2

    await redis_queue.clear()
    assert await redis_queue.size() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_with_deduplication(redis_queue_dedupe):
    """RedisQueue with dedupe prevents duplicate requests in real Redis."""
    req = Request(url="https://example.com/same")

    # First put should succeed
    await redis_queue_dedupe.put(req, priority=0)
    assert await redis_queue_dedupe.size() == 1

    # Second put of same URL should be deduplicated
    await redis_queue_dedupe.put(req, priority=0)
    assert await redis_queue_dedupe.size() == 1  # Still only 1 item

    # Get should return the single item
    retrieved = await redis_queue_dedupe.get()
    assert retrieved.url == req.url
    assert await redis_queue_dedupe.size() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_serialization_roundtrip(redis_queue):
    """RedisQueue correctly serializes/deserializes complex requests with real Redis."""
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_empty_get_timeout(redis_server):
    """RedisQueue.get() with timeout on empty real Redis queue."""
    import asyncio

    queue = RedisQueue(url=redis_server, namespace="qcrawl_test_timeout")

    try:
        await queue.clear()

        # Getting from empty queue should timeout
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(queue.get(), timeout=0.5)

    finally:
        await queue.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_concurrent_operations(redis_queue):
    """RedisQueue handles concurrent put/get operations with real Redis."""
    import asyncio

    # Add multiple requests concurrently
    requests = [Request(url=f"https://example.com/{i}") for i in range(10)]

    # Put all requests concurrently
    await asyncio.gather(*[redis_queue.put(req, priority=i) for i, req in enumerate(requests)])

    assert await redis_queue.size() == 10

    # Get all requests concurrently
    retrieved = await asyncio.gather(*[redis_queue.get() for _ in range(10)])

    assert len(retrieved) == 10
    assert await redis_queue.size() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_namespace_isolation(redis_server):
    """Different namespaces in Redis don't interfere with each other."""
    queue_a = RedisQueue(url=redis_server, namespace="namespace_a")
    queue_b = RedisQueue(url=redis_server, namespace="namespace_b")

    try:
        await queue_a.clear()
        await queue_b.clear()

        # Add to queue A
        await queue_a.put(Request(url="https://example.com/a"), priority=0)

        # Queue A should have 1 item, Queue B should be empty
        assert await queue_a.size() == 1
        assert await queue_b.size() == 0

        # Get from queue A works
        req_a = await queue_a.get()
        assert req_a.url == "https://example.com/a"

    finally:
        await queue_a.clear()
        await queue_b.clear()
        await queue_a.close()
        await queue_b.close()
