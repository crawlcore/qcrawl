"""Tests for qcrawl.core.scheduler.Scheduler"""

import asyncio

import pytest

from qcrawl.core.queues.memory import MemoryPriorityQueue
from qcrawl.core.request import Request
from qcrawl.core.scheduler import Scheduler
from qcrawl.utils.fingerprint import RequestFingerprinter


@pytest.fixture
def scheduler():
    """Fixture providing a Scheduler with memory queue."""
    queue = MemoryPriorityQueue()
    fingerprinter = RequestFingerprinter()
    return Scheduler(queue=queue, fingerprinter=fingerprinter)


def test_init_requires_queue_and_fingerprinter():
    """Scheduler requires queue and fingerprinter."""
    with pytest.raises(ValueError, match="queue is required"):
        Scheduler(queue=None, fingerprinter=RequestFingerprinter())

    with pytest.raises(ValueError, match="fingerprinter is required"):
        Scheduler(queue=MemoryPriorityQueue(), fingerprinter=None)


@pytest.mark.asyncio
async def test_add_and_get_basic(scheduler):
    """Scheduler add and get operations."""
    req = Request(url="https://example.com/test")
    await scheduler.add(req)

    retrieved = await scheduler.get()
    assert retrieved.url == req.url
    assert scheduler.pending == 1


@pytest.mark.asyncio
async def test_add_string_url(scheduler):
    """Scheduler accepts string URLs and converts to Request."""
    await scheduler.add("https://example.com/page")

    req = await scheduler.get()
    assert "example.com/page" in req.url
    assert req.priority == 0


@pytest.mark.asyncio
async def test_deduplication(scheduler):
    """Scheduler deduplicates requests by fingerprint."""
    await scheduler.add(Request(url="https://example.com/same"))
    await scheduler.add(Request(url="https://example.com/same"))

    assert await scheduler.qsize() == 1


@pytest.mark.asyncio
async def test_direct_delivery_to_waiting_consumer(scheduler):
    """Scheduler delivers requests directly to waiting consumers."""
    # Start get() before add() - consumer is waiting
    get_task = asyncio.create_task(scheduler.get())
    await asyncio.sleep(0.01)  # Let get() register as waiter

    await scheduler.add(Request(url="https://example.com/direct"))

    req = await get_task
    assert "direct" in req.url
    assert await scheduler.qsize() == 0  # Not queued, delivered directly


@pytest.mark.asyncio
async def test_task_done_and_join(scheduler):
    """Scheduler task_done marks work complete and join waits."""
    await scheduler.add(Request(url="https://example.com/task"))
    await scheduler.get()

    # Work pending, join should block
    join_task = asyncio.create_task(scheduler.join())
    await asyncio.sleep(0.01)
    assert not join_task.done()

    # Mark done, join completes
    scheduler.task_done()
    await join_task
    assert scheduler.pending == 0


@pytest.mark.asyncio
async def test_task_done_too_many_times_raises(scheduler):
    """Scheduler task_done raises if called too many times."""
    await scheduler.add(Request(url="https://example.com/test"))
    await scheduler.get()
    scheduler.task_done()

    with pytest.raises(ValueError, match="task_done\\(\\) called too many times"):
        scheduler.task_done()


@pytest.mark.asyncio
async def test_close_prevents_add(scheduler):
    """Scheduler close prevents new adds."""
    await scheduler.close()

    await scheduler.add(Request(url="https://example.com/ignored"))
    assert await scheduler.qsize() == 0


@pytest.mark.asyncio
async def test_close_cancels_waiters(scheduler):
    """Scheduler close cancels waiting consumers."""
    get_task = asyncio.create_task(scheduler.get())
    await asyncio.sleep(0.01)

    await scheduler.close()

    with pytest.raises(asyncio.CancelledError):
        await get_task


@pytest.mark.asyncio
async def test_stats(scheduler):
    """Scheduler stats returns monitoring info."""
    await scheduler.add(Request(url="https://example.com/1"))
    await scheduler.add(Request(url="https://example.com/2"))

    stats = await scheduler.stats()
    assert stats["queued"] == 2
    assert stats["pending"] == 2
    assert stats["seen"] == 2
    assert stats["waiting_consumers"] == 0
    assert stats["closed"] == 0


@pytest.mark.asyncio
async def test_async_context_manager(scheduler):
    """Scheduler works as async context manager."""
    async with scheduler as s:
        assert s is scheduler
        await s.add(Request(url="https://example.com/ctx"))
        await s.get()
        s.task_done()

    # After exit, scheduler is closed
    assert scheduler._closed is True


@pytest.mark.asyncio
async def test_queue_full_emits_request_dropped(scheduler, monkeypatch):
    """A full-queue drop emits request_dropped and leaves no pending work."""
    from qcrawl import signals

    dropped: list[Request] = []

    async def on_dropped(sender, request=None, exception=None, **kwargs):
        dropped.append(request)

    async def _raise_full(request, priority=0):
        raise asyncio.QueueFull

    monkeypatch.setattr(scheduler.queue, "put", _raise_full)
    signals.signals_registry.connect("request_dropped", on_dropped, sender=scheduler, weak=False)
    try:
        await scheduler.add(Request(url="https://example.com/overflow"))
    finally:
        signals.signals_registry.disconnect("request_dropped", on_dropped, sender=scheduler)

    assert len(dropped) == 1
    assert dropped[0].url == "https://example.com/overflow"
    assert scheduler.pending == 0
    assert await scheduler.qsize() == 0
