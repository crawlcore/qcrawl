"""Tests for qcrawl.core.queues.disk.DiskQueue"""

import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from qcrawl.core.queues.disk import DiskQueue
from qcrawl.core.request import Request


@pytest.fixture
def temp_queue_dir(tmp_path):
    """Provide a temporary directory for queue tests."""
    queue_dir = tmp_path / "test_queue"
    yield queue_dir
    # Cleanup after test
    if queue_dir.exists():
        shutil.rmtree(queue_dir)


# Initialization Tests


def test_init_validation_raises_on_invalid_args() -> None:
    """Test that invalid constructor arguments raise appropriate errors."""
    with pytest.raises(ValueError, match="maxsize must be >= 0"):
        DiskQueue(maxsize=-1)

    with pytest.raises(TypeError, match="Unexpected keyword argument"):
        DiskQueue(foo=1)


def test_init_with_custom_path(temp_queue_dir) -> None:
    """Test initialization with custom path."""
    queue = DiskQueue(path=temp_queue_dir, maxsize=0)
    assert queue._path == temp_queue_dir
    assert queue._maxsize == 0


def test_init_with_default_path() -> None:
    """Test initialization with default system temp path."""
    queue = DiskQueue()
    # Should use system temp directory
    assert "qcrawl_queue" in str(queue._path)
    assert queue._path.parent == Path(tempfile.gettempdir())


# Queue Operations Tests


@pytest.mark.asyncio
async def test_put_get_order_and_fifo_tiebreak(temp_queue_dir) -> None:
    """Test priority ordering and FIFO tiebreaking."""
    q = DiskQueue(path=temp_queue_dir)

    r_low = Request(url="http://low.example")
    r_p5 = Request(url="http://p5.example")
    r_p1_a = Request(url="http://p1-a.example")
    r_p1_b = Request(url="http://p1-b.example")

    await q.put(r_low, priority=5)
    await q.put(r_p1_a, priority=1)
    await q.put(r_p1_b, priority=1)
    await q.put(r_p5, priority=5)

    # Lower priority number = higher priority
    # FIFO for same priority
    assert (await q.get()).url == "http://p1-a.example/"
    assert (await q.get()).url == "http://p1-b.example/"
    assert (await q.get()).url == "http://low.example/"
    assert (await q.get()).url == "http://p5.example/"

    await q.close()


@pytest.mark.asyncio
async def test_clear_and_size_behavior(temp_queue_dir) -> None:
    """Test queue size tracking and clear() functionality."""
    q = DiskQueue(path=temp_queue_dir)

    await q.put(Request(url="http://one"), priority=0)
    await q.put(Request(url="http://two"), priority=0)

    assert await q.size() == 2

    await q.clear()
    assert await q.size() == 0

    # Verify files are deleted
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 0

    await q.close()


@pytest.mark.asyncio
async def test_close_makes_put_noop_and_get_raises_cancelled(temp_queue_dir) -> None:
    """Test that close() makes put() a no-op and get() raises CancelledError when empty."""
    q = DiskQueue(path=temp_queue_dir)

    await q.close()

    # put after close is ignored (no-op)
    await q.put(Request(url="http://ignored"), priority=0)
    assert await q.size() == 0

    # get on closed + empty queue raises CancelledError
    with pytest.raises(asyncio.CancelledError):
        await q.get()


# Persistence Tests (unique to disk queue)


@pytest.mark.asyncio
async def test_persistence_after_close_and_reopen(temp_queue_dir) -> None:
    """Test requests persist after closing and reopening queue."""
    # Create queue and add requests
    q1 = DiskQueue(path=temp_queue_dir)
    await q1.put(Request(url="http://one.example"), priority=0)
    await q1.put(Request(url="http://two.example"), priority=5)
    await q1.put(Request(url="http://three.example"), priority=10)
    assert await q1.size() == 3
    await q1.close()

    # Reopen queue - should recover all requests
    q2 = DiskQueue(path=temp_queue_dir)
    assert await q2.size() == 3

    # Verify order is preserved
    r1 = await q2.get()
    assert r1.url == "http://one.example/"
    assert await q2.size() == 2

    await q2.close()


@pytest.mark.asyncio
async def test_recovery_rebuilds_queue_state(temp_queue_dir) -> None:
    """Test queue state is correctly rebuilt from disk files."""
    # Create queue and add requests with different priorities
    q1 = DiskQueue(path=temp_queue_dir)
    await q1.put(Request(url="http://priority-10.example"), priority=10)
    await q1.put(Request(url="http://priority-5.example"), priority=5)
    await q1.put(Request(url="http://priority-0.example"), priority=0)
    await q1.close()

    # Verify files exist
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 3

    # Reopen and verify priority order is maintained
    q2 = DiskQueue(path=temp_queue_dir)
    assert await q2.size() == 3

    # Should come out in priority order
    assert (await q2.get()).url == "http://priority-0.example/"
    assert (await q2.get()).url == "http://priority-5.example/"
    assert (await q2.get()).url == "http://priority-10.example/"

    await q2.close()


@pytest.mark.asyncio
async def test_counter_continues_after_recovery(temp_queue_dir) -> None:
    """Test that counter continues from max value after recovery."""
    # Create queue, add request, close
    q1 = DiskQueue(path=temp_queue_dir)
    await q1.put(Request(url="http://first.example"), priority=0)
    await q1.close()

    # Reopen and add another request
    q2 = DiskQueue(path=temp_queue_dir)
    await q2.put(Request(url="http://second.example"), priority=0)

    # Counter should have continued, creating different filenames
    files = sorted(temp_queue_dir.glob("*.req"))
    assert len(files) == 2
    # Files should have different counter values
    assert files[0].stem != files[1].stem

    await q2.close()


@pytest.mark.asyncio
async def test_corrupted_files_are_skipped(temp_queue_dir) -> None:
    """Test that corrupted/invalid files are skipped during recovery."""
    # Create queue and add valid request
    q1 = DiskQueue(path=temp_queue_dir)
    await q1.put(Request(url="http://valid.example"), priority=0)
    await q1.close()

    # Manually create invalid files
    (temp_queue_dir / "invalid.req").write_text("not a valid msgpack file")
    (temp_queue_dir / "bad_format.req").write_text("also invalid")
    (temp_queue_dir / "no_underscore.req").write_bytes(b"invalid")

    # Reopen - should skip invalid files and only load valid one
    q2 = DiskQueue(path=temp_queue_dir)
    assert await q2.size() == 1

    req = await q2.get()
    assert req.url == "http://valid.example/"

    await q2.close()


# Error Handling Tests


@pytest.mark.asyncio
async def test_get_raises_runtimeerror_on_decode_failure(temp_queue_dir) -> None:
    """Test that deserialization failures are caught and raise RuntimeError."""
    q = DiskQueue(path=temp_queue_dir)

    class BrokenRequest(Request):
        def to_bytes(self) -> bytes:
            return b"this-is-not-valid-msgpack"

        @classmethod
        def from_bytes(cls, data: bytes) -> "BrokenRequest":
            raise RuntimeError("Deserialization failed")

    bad_req = BrokenRequest(url="http://broken.example")
    await q.put(bad_req)

    with pytest.raises(RuntimeError, match="Failed to read request from"):
        await q.get()

    await q.close()


@pytest.mark.asyncio
async def test_handles_missing_directory(temp_queue_dir) -> None:
    """Test queue creates directory if it doesn't exist."""
    # Use a path that doesn't exist yet
    queue_path = temp_queue_dir / "subdir" / "queue"
    q = DiskQueue(path=queue_path)

    # Should create directory on first operation
    await q.put(Request(url="http://example.com"), priority=0)

    assert queue_path.exists()
    assert queue_path.is_dir()

    await q.close()


@pytest.mark.asyncio
async def test_maxsize_enforcement(temp_queue_dir) -> None:
    """Test that maxsize limit is enforced."""
    q = DiskQueue(path=temp_queue_dir, maxsize=2)

    # Add up to maxsize
    await q.put(Request(url="http://one.example"), priority=0)
    await q.put(Request(url="http://two.example"), priority=0)
    assert await q.size() == 2

    # Try to add beyond maxsize - should be dropped
    await q.put(Request(url="http://three.example"), priority=0)
    assert await q.size() == 2  # Still 2, third was dropped

    await q.close()


# Protocol Tests


@pytest.mark.asyncio
async def test_async_iteration_protocol(temp_queue_dir) -> None:
    """Test RequestQueue async iteration protocol (__aiter__, __anext__)."""
    q = DiskQueue(path=temp_queue_dir)

    await q.put(Request(url="http://first.example"), priority=0)
    await q.put(Request(url="http://second.example"), priority=0)
    await q.close()

    urls = []
    async for req in q:
        urls.append(req.url)

    assert len(urls) == 2
    assert "first.example" in urls[0]
    assert "second.example" in urls[1]


def test_maxsize_method() -> None:
    """Test maxsize() method returns configured value."""
    q = DiskQueue(maxsize=50)
    assert q.maxsize() == 50


def test_repr(temp_queue_dir) -> None:
    """Test __repr__ shows class name, path, and maxsize."""
    q = DiskQueue(path=temp_queue_dir, maxsize=100)
    repr_str = repr(q)

    assert "DiskQueue" in repr_str
    assert str(temp_queue_dir) in repr_str
    assert "maxsize=100" in repr_str


# File Management Tests


@pytest.mark.asyncio
async def test_files_are_deleted_after_get(temp_queue_dir) -> None:
    """Test that files are deleted from disk after get()."""
    q = DiskQueue(path=temp_queue_dir)

    await q.put(Request(url="http://one.example"), priority=0)
    await q.put(Request(url="http://two.example"), priority=0)

    # Verify files exist
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 2

    # Get one request
    await q.get()

    # One file should be deleted
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 1

    # Get second request
    await q.get()

    # All files should be deleted
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 0

    await q.close()


@pytest.mark.asyncio
async def test_clear_deletes_all_files(temp_queue_dir) -> None:
    """Test that clear() removes all queue files from disk."""
    q = DiskQueue(path=temp_queue_dir)

    # Add multiple requests
    for i in range(5):
        await q.put(Request(url=f"http://example{i}.com"), priority=i)

    # Verify files exist
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 5

    # Clear queue
    await q.clear()

    # All files should be deleted
    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 0
    assert await q.size() == 0

    await q.close()


@pytest.mark.asyncio
async def test_filename_format(temp_queue_dir) -> None:
    """Test that filenames follow expected format: priority_counter.req"""
    q = DiskQueue(path=temp_queue_dir)

    await q.put(Request(url="http://example.com"), priority=5)
    await q.close()

    files = list(temp_queue_dir.glob("*.req"))
    assert len(files) == 1

    filename = files[0].name
    # Format: {priority:010d}_{counter:010d}.req
    assert filename.endswith(".req")
    parts = filename[:-4].split("_")
    assert len(parts) == 2
    assert parts[0] == "0000000005"  # priority=5 formatted with 10 digits
    assert parts[1].isdigit()  # counter is numeric
