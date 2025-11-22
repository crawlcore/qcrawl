import asyncio
import contextlib
import logging
from itertools import count

from qcrawl.core.queue import RequestQueue
from qcrawl.core.request import Request

logger = logging.getLogger(__name__)


class MemoryPriorityQueue(RequestQueue):
    """In-memory implementation of `RequestQueue` using `asyncio.PriorityQueue`.

    Storage format:
      - Items are stored as tuples `(priority: int, counter: int, payload: bytes)`.
      - Payload is MessagePack-encoded `Request` bytes produced by `Request.to_bytes()`.
      - Lower numeric `priority` values are processed first.
      - `counter` (monotonic integer) preserves FIFO order among items with identical priority.

    Concurrency:
      - Built with asyncio primitives and intended to be used from a single event loop.
      - FIFO tie-breaking for equal priority is preserved even with concurrent producers/consumers.

    Errors:
      - Raises `ValueError` if `maxsize < 0`.
      - Unexpected keyword arguments at construction raise `TypeError`.
    """

    def __init__(self, maxsize: int = 0, **kwargs: object) -> None:
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        if kwargs:
            keys = ", ".join(str(k) for k in kwargs)
            raise TypeError(f"Unexpected keyword argument(s) for MemoryPriorityQueue: {keys}")

        # store tuples: (priority: int, counter: int, payload: bytes)
        # lower numeric priority => processed first
        self._pq: asyncio.PriorityQueue[tuple[int, int, bytes]] = asyncio.PriorityQueue(
            maxsize=maxsize
        )
        self._counter = count()
        self._closed: bool = False

    async def put(self, request: Request, priority: int = 0) -> None:
        """Enqueue `request` with the given `priority`.

        Lower numeric values indicate higher priority. If the queue is closed,
        the call is ignored (no-op). Otherwise this call awaits until space is available.

        """
        if self._closed:
            logger.debug(
                "Put called after close(); ignoring request: %s", getattr(request, "url", None)
            )
            return

        payload = request.to_bytes()
        if not isinstance(payload, bytes):
            raise TypeError("Request.to_bytes() did not return bytes")

        await self._pq.put((priority, next(self._counter), payload))

    async def get(self) -> Request:
        """Await and return the next `Request`.

        Decodes stored MessagePack bytes via `Request.from_bytes()`. Decoding
        errors are logged and wrapped as `RuntimeError` to surface deserialization issues.

        If queue is closed and empty, raises `asyncio.CancelledError` to indicate shutdown.

        Behavior:
          - If items exist, return the highest-priority item (lowest numeric priority).
          - If queue is closed and empty, raise `asyncio.CancelledError` to indicate shutdown.
        """
        if self._closed and self._pq.empty():
            raise asyncio.CancelledError

        _, _, payload = await self._pq.get()
        try:
            if isinstance(payload, bytes):
                try:
                    req = Request.from_bytes(payload)
                    return req
                except Exception as exc:
                    logger.exception("Failed to decode Request from bytes payload")
                    raise RuntimeError("Failed to decode in-memory request payload") from exc
            else:
                # Defensive: should not happen with current invariant
                raise TypeError(f"Unexpected payload type in queue: {type(payload)!r}")
        finally:
            with contextlib.suppress(Exception):
                self._pq.task_done()

    async def size(self) -> int:
        """Return the number of items currently queued (non-blocking)."""
        return int(self._pq.qsize())

    def maxsize(self) -> int:
        """Return the queue's maximum capacity.

        Returns:
            int: Maximum number of items the queue can hold. A value of `0`
            denotes an unbounded queue (no fixed capacity).
        """
        return self._pq.maxsize

    async def clear(self) -> None:
        """Remove all queued items, draining synchronously."""
        while True:
            try:
                self._pq.get_nowait()
                with contextlib.suppress(Exception):
                    self._pq.task_done()
            except asyncio.QueueEmpty:
                break

    async def close(self) -> None:
        """Mark the queue closed.

        After calling:
          - `put()` becomes a no-op.
          - `get()` will return remaining items until the queue is drained, then raise
            `asyncio.CancelledError` to notify consumers to stop.
        """
        self._closed = True

    def __repr__(self) -> str:
        return f"<MemoryPriorityQueue size={self._pq.qsize()} maxsize={self._pq.maxsize} closed={self._closed}>"
