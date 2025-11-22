import asyncio
from itertools import count

import pytest

from qcrawl.core.queue import RequestQueue
from qcrawl.core.queues import factory as queue_factory
from qcrawl.core.request import Request


class FakeRedisQueue(RequestQueue):
    """In-test fake that mimics the external Redis-backed queue API but stores
    serialized bytes internally. This lets tests exercise serialization/
    deserialization and queue semantics without a networked Redis server.
    """

    def __init__(self, maxsize: int = 0, **kwargs: object) -> None:
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        if kwargs:
            keys = ", ".join(str(k) for k in kwargs)
            raise TypeError(f"Unexpected keyword argument(s) for FakeRedisQueue: {keys}")

        self._pq: asyncio.PriorityQueue[tuple[int, int, bytes]] = asyncio.PriorityQueue(
            maxsize=maxsize
        )
        self._counter = count()
        self._closed = False

    async def put(self, request: Request, priority: int = 0) -> None:
        if self._closed:
            return
        payload = request.to_bytes()
        if not isinstance(payload, bytes):
            raise TypeError("Request.to_bytes() did not return bytes")
        await self._pq.put((priority, next(self._counter), payload))

    async def get(self) -> Request:
        if self._closed and self._pq.empty():
            raise asyncio.CancelledError
        _, _, payload = await self._pq.get()
        try:
            if isinstance(payload, bytes):
                try:
                    req = Request.from_bytes(payload)
                    return req
                except Exception as exc:
                    raise RuntimeError("Failed to decode in-memory request payload") from exc
            else:
                raise TypeError(f"Unexpected payload type in queue: {type(payload)!r}")
        finally:
            with __import__("contextlib").suppress(Exception):
                self._pq.task_done()

    async def size(self) -> int:
        return int(self._pq.qsize())

    def maxsize(self) -> int:
        return self._pq.maxsize

    async def clear(self) -> None:
        while True:
            try:
                self._pq.get_nowait()
                with __import__("contextlib").suppress(Exception):
                    self._pq.task_done()
            except asyncio.QueueEmpty:
                break

    async def close(self) -> None:
        self._closed = True

    def __repr__(self) -> str:
        return f"<FakeRedisQueue size={self._pq.qsize()} maxsize={self._pq.maxsize} closed={self._closed}>"


@pytest.fixture
def fake_redis_backend() -> str:
    """Returns the dotted path to FakeRedisQueue for use with create_queue."""
    return "tests.core.queues.test_redis_queue.FakeRedisQueue"


def test_redis_backend_via_factory_and_basic_order(fake_redis_backend: str) -> None:
    q = asyncio.run(queue_factory.create_queue(fake_redis_backend))

    # basic priority + FIFO tie-break behavior
    r_low = Request(url="http://low.example")
    r_p5 = Request(url="http://p5.example")
    r_p1_a = Request(url="http://p1-a.example")
    r_p1_b = Request(url="http://p1-b.example")

    asyncio.run(q.put(r_low, priority=5))
    asyncio.run(q.put(r_p1_a, priority=1))
    asyncio.run(q.put(r_p1_b, priority=1))
    asyncio.run(q.put(r_p5, priority=5))

    got = asyncio.run(q.get())
    assert got.url == r_p1_a.url

    got = asyncio.run(q.get())
    assert got.url == r_p1_b.url

    got = asyncio.run(q.get())
    assert got.url in {r_low.url, r_p5.url}

    got = asyncio.run(q.get())
    assert got.url in {r_low.url, r_p5.url}


def test_redis_queue_clear_and_size(fake_redis_backend: str) -> None:
    q = asyncio.run(queue_factory.create_queue(fake_redis_backend))

    asyncio.run(q.put(Request(url="http://one"), priority=0))
    asyncio.run(q.put(Request(url="http://two"), priority=0))

    assert asyncio.run(q.size()) == 2

    asyncio.run(q.clear())
    assert asyncio.run(q.size()) == 0


def test_redis_queue_close_and_cancel(fake_redis_backend: str) -> None:
    q = asyncio.run(queue_factory.create_queue(fake_redis_backend))

    # close should mark queue closed
    asyncio.run(q.close())

    # put after close is a no-op
    asyncio.run(q.put(Request(url="http://ignored"), priority=0))
    assert asyncio.run(q.size()) == 0

    # get on closed+empty queue should raise asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(q.get())


def test_redis_queue_raises_runtimeerror_on_decode_failure(fake_redis_backend: str) -> None:
    q = asyncio.run(queue_factory.create_queue(fake_redis_backend))

    class BrokenRequest(Request):
        def to_bytes(self) -> bytes:
            return b"not-valid-serialized-request"

        @classmethod
        def from_bytes(cls, data: bytes) -> "BrokenRequest":
            raise RuntimeError("Deserialization failed")

    bad_req = BrokenRequest(url="http://broken.example", priority=0)

    asyncio.run(q.put(bad_req))

    with pytest.raises(RuntimeError):
        asyncio.run(q.get())
