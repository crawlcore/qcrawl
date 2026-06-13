import asyncio
import logging
from collections import deque
from contextlib import suppress

from qcrawl import signals
from qcrawl.core.queue import RequestQueue
from qcrawl.core.request import Request
from qcrawl.utils.fingerprint import RequestFingerprinter

logger = logging.getLogger(__name__)


class Scheduler:
    """Async request scheduler with deduplication, priority, and fair consumer delivery.

    Features:
        - Deduplication via RequestFingerprinter
        - Priority-based ordering
        - Direct delivery to waiting consumers
        - Accurate pending work tracking
        - Graceful shutdown with backpressure
    """

    __slots__ = (
        "queue",
        "fingerprinter",
        "seen",
        "_waiters",
        "_closed",
        "_pending",
        "_finished",
        "signals",
    )

    def __init__(
        self,
        queue: RequestQueue | None = None,
        fingerprinter: RequestFingerprinter | None = None,
    ) -> None:
        if queue is None:
            raise ValueError("queue is required")
        if fingerprinter is None:
            raise ValueError("fingerprinter is required")

        self.queue = queue
        self.fingerprinter = fingerprinter
        self.seen: set[bytes] = set()
        self._waiters: deque[asyncio.Future[Request]] = deque()
        self._closed: bool = False
        self._pending: int = 0
        self._finished: asyncio.Event = asyncio.Event()
        self._finished.set()
        self.signals = signals.signals_registry.for_sender(self)

    async def add(self, request: Request | str) -> None:
        """Add request to scheduler (idempotent).

        URLs are normalized and deduplicated.
        If a consumer is waiting, the request is delivered directly without queuing.

        Features:
            - Normalizes and deduplicates via fingerprint
            - Delivers directly to waiting consumer if available
            - Enqueues via abstract `queue.put()` (async)
            - No-op if closed or duplicate
        """
        if self._closed:
            url = request.url if isinstance(request, Request) else request
            logger.debug("Ignoring add() after close: %s", url)
            return

        if isinstance(request, str):
            request = Request(url=request, priority=0)

        fp = self.fingerprinter.fingerprint_bytes(request)
        if fp in self.seen:
            return
        self.seen.add(fp)

        if self._pending == 0:
            self._finished.clear()
        self._pending += 1

        # Direct delivery to first non-cancelled waiter
        delivered = False
        while self._waiters and not delivered:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(request)
                delivered = True

        await self.signals.send_async("request_scheduled", request=request)

        if delivered:
            return

        # Enqueue via abstract queue
        try:
            await self.queue.put(request, priority=request.priority)
        except asyncio.QueueFull:
            logger.warning("Queue full, dropping request: %s", request.url)
            self._pending -= 1
            if self._pending == 0:
                self._finished.set()
            self.seen.discard(fp)
            await self.signals.send_async("request_dropped", request=request, exception=None)

    async def get(self) -> Request:
        """Get next request from scheduler (highest priority first).

        Blocks until a request is available. If no requests are queued,
        registers as a waiter to receive direct delivery.

        Raises:
            asyncio.CancelledError: Scheduler is closed and empty
        """
        if self._closed and await self.queue.size() == 0:
            raise asyncio.CancelledError

        if await self.queue.size() > 0:
            return await self.queue.get()

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Request] = loop.create_future()
        self._waiters.append(fut)

        try:
            return await fut
        finally:
            with suppress(ValueError):
                self._waiters.remove(fut)
            if not fut.done():
                fut.cancel()

    def task_done(self) -> None:
        """Mark one request as processed.

        Must be called exactly once per `get()`.
        Triggers `_finished` when all work is done.
        """
        if self._pending == 0:
            raise ValueError("task_done() called too many times")
        self._pending -= 1
        if self._pending == 0:
            self._finished.set()

    async def join(self) -> None:
        """Wait until all pending work is complete.

        Blocks until all requests retrieved via `get()` have been marked
        done via `task_done()`.
        """
        await self._finished.wait()

    async def qsize(self) -> int:
        """Get number of queued requests.

        Features:
            - Does not include requests already given to consumers
            - Does not include pending work count
        """
        return int(await self.queue.size())

    async def close(self) -> None:
        """Close scheduler and signal shutdown.

        Features:
           - Prevents new `add()` calls
           - Cancels all waiting consumers
           - Closes underlying queue
        """
        if self._closed:
            return

        self._closed = True

        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                fut.cancel()

        await self.queue.close()

    @property
    def pending(self) -> int:
        """Number of requests currently being processed.

        This counts requests that have been retrieved via `get()` but not yet
        marked complete via `task_done()`.
        """
        return self._pending

    async def stats(self) -> dict[str, int]:
        """Get scheduler statistics for monitoring and debugging."""
        size = await self.queue.size()
        return {
            "queued": size,
            "pending": self._pending,
            "seen": len(self.seen),
            "waiting_consumers": len(self._waiters),
            "closed": int(self._closed),
        }

    async def __aenter__(self) -> "Scheduler":
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit async context manager and perform cleanup."""
        await self.close()
        await self.join()
        return False
