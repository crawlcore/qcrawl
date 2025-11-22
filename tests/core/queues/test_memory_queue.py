import asyncio

import pytest

from qcrawl.core.queues.memory import MemoryPriorityQueue
from qcrawl.core.request import Request


def test_init_validation_raises_on_invalid_args() -> None:
    with pytest.raises(ValueError):
        MemoryPriorityQueue(maxsize=-1)

    with pytest.raises(TypeError):
        # unexpected kwargs should surface as TypeError
        MemoryPriorityQueue(foo=1)


def test_put_get_order_and_fifo_tiebreak() -> None:
    q = MemoryPriorityQueue()

    r_low = Request(url="http://low.example")  # placeholder (Request.priority defaults to 0)
    r_p5 = Request(url="http://p5.example")  # used with explicit priority=5 when enqueued
    r_p1_a = Request(url="http://p1-a.example")  # first priority-1 item (FIFO order matters)
    r_p1_b = Request(url="http://p1-b.example")  # second priority-1 item

    # enqueue in mixed order and with explicit priorities
    asyncio.run(q.put(r_low, priority=5))
    asyncio.run(q.put(r_p1_a, priority=1))
    asyncio.run(q.put(r_p1_b, priority=1))
    asyncio.run(q.put(r_p5, priority=5))

    # expected: priority 1 items first (in FIFO order), then priority 5 items (FIFO)
    got = asyncio.run(q.get())
    assert got.url == r_p1_a.url

    got = asyncio.run(q.get())
    assert got.url == r_p1_b.url

    got = asyncio.run(q.get())
    assert got.url in {r_low.url, r_p5.url}  # both priority 5; order preserved by insertion counter

    got = asyncio.run(q.get())
    # last remaining item
    assert got.url in {r_low.url, r_p5.url}


def test_clear_and_size_behavior() -> None:
    q = MemoryPriorityQueue()

    asyncio.run(q.put(Request(url="http://one"), priority=0))
    asyncio.run(q.put(Request(url="http://two"), priority=0))

    assert asyncio.run(q.size()) == 2

    asyncio.run(q.clear())
    assert asyncio.run(q.size()) == 0


def test_close_makes_put_noop_and_get_raises_cancelled() -> None:
    q = MemoryPriorityQueue()

    # close should mark queue closed
    asyncio.run(q.close())

    # put after close is a no-op (does not raise)
    asyncio.run(q.put(Request(url="http://ignored"), priority=0))
    assert asyncio.run(q.size()) == 0

    # get on closed+empty queue should raise asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(q.get())


def test_get_raises_runtimeerror_on_decode_failure() -> None:
    q = MemoryPriorityQueue()

    class BrokenRequest(Request):
        def to_bytes(self) -> bytes:
            return b"this-is-not-valid-serialized-request"

        @classmethod
        def from_bytes(cls, data: bytes) -> "BrokenRequest":
            raise RuntimeError("Deserialization failed")

    bad_req = BrokenRequest(url="http://broken.example", priority=0)

    asyncio.run(q.put(bad_req))

    with pytest.raises(RuntimeError):
        asyncio.run(q.get())
