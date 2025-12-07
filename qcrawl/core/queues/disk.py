"""Disk-based persistent queue implementation using file storage."""

import asyncio
import contextlib
import logging
import tempfile
from itertools import count
from logging import DEBUG
from pathlib import Path

import aiofiles
import aiofiles.os

from qcrawl.core.queue import RequestQueue
from qcrawl.core.request import Request

logger = logging.getLogger(__name__)


def _get_default_queue_path() -> Path:
    """Get default queue path that works on all platforms.

    Returns:
        Path object for queue directory (e.g., /tmp/qcrawl_queue on Unix,
        C:\\Users\\<user>\\AppData\\Local\\Temp\\qcrawl_queue on Windows)
    """
    return Path(tempfile.gettempdir()) / "qcrawl_queue"


class DiskQueue(RequestQueue):
    """Disk-based implementation of `RequestQueue` for persistent storage.

    Storage format:
      - Requests are stored as individual files in a directory
      - Files are named: `{priority:010d}_{counter:010d}.req`
      - Each file contains MessagePack-encoded `Request` bytes
      - Lower numeric `priority` values are processed first
      - `counter` (monotonic integer) preserves FIFO order

    Persistence:
      - Queue survives crashes and restarts
      - Automatically recovers state on initialization
      - Requests are durable once written to disk

    Concurrency:
      - Thread-safe for single event loop
      - Uses asyncio locks for file operations
      - Directory scanning on startup rebuilds queue state

    Errors:
      - Raises `ValueError` if `maxsize < 0` or path is invalid
      - Raises `RuntimeError` if directory cannot be created
    """

    def __init__(
        self,
        path: str | Path | None = None,
        maxsize: int = 0,
        **kwargs: object,
    ) -> None:
        """Initialize disk-based queue.

        Args:
            path: Directory path for storing queue files (None = use system temp dir)
            maxsize: Maximum queue size (0 = unlimited)
            **kwargs: Unused (raises TypeError if provided)

        Raises:
            ValueError: If maxsize < 0
            TypeError: If unexpected kwargs provided
        """
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        if kwargs:
            keys = ", ".join(str(k) for k in kwargs)
            raise TypeError(f"Unexpected keyword argument(s) for DiskQueue: {keys}")

        self._path = Path(path) if path is not None else _get_default_queue_path()
        self._maxsize = maxsize
        self._counter = count()
        self._closed = False
        self._lock = asyncio.Lock()
        self._get_lock = asyncio.Lock()

        # In-memory priority queue for fast access
        # Items: (priority, counter, filename)
        self._pq: asyncio.PriorityQueue[tuple[int, int, str]] = asyncio.PriorityQueue()

        # Track if initialized
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Ensure queue directory exists and state is loaded."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            # Create directory if it doesn't exist
            try:
                await aiofiles.os.makedirs(self._path, exist_ok=True)
            except Exception as exc:
                raise RuntimeError(f"Failed to create queue directory: {self._path}") from exc

            # Scan directory and rebuild queue state
            await self._rebuild_queue()
            self._initialized = True

            if logger.isEnabledFor(DEBUG):
                logger.debug("DiskQueue initialized at %s", self._path)

    async def _rebuild_queue(self) -> None:
        """Scan directory and rebuild in-memory queue from disk files."""
        try:
            files = await aiofiles.os.listdir(self._path)
        except Exception as exc:
            logger.warning("Failed to list queue directory: %s", exc)
            return

        # Parse filenames and add to priority queue
        max_counter = -1
        recovered_count = 0

        for filename in files:
            if not filename.endswith(".req"):
                continue

            try:
                # Parse: {priority:010d}_{counter:010d}.req
                base = filename[:-4]  # Remove .req
                parts = base.split("_")
                if len(parts) != 2:
                    logger.warning("Invalid queue file format: %s", filename)
                    continue

                priority = int(parts[0])
                counter_val = int(parts[1])

                # Add to in-memory queue
                await self._pq.put((priority, counter_val, filename))
                max_counter = max(max_counter, counter_val)
                recovered_count += 1

            except (ValueError, IndexError) as exc:
                logger.warning("Failed to parse queue file %s: %s", filename, exc)
                continue

        # Update counter to avoid collisions
        if max_counter >= 0:
            self._counter = count(max_counter + 1)

        if recovered_count > 0:
            logger.info("Recovered %d requests from disk queue", recovered_count)

    def _make_filename(self, priority: int, counter_val: int) -> str:
        """Generate filename for request.

        Args:
            priority: Request priority
            counter_val: Request counter value

        Returns:
            Filename in format: {priority:010d}_{counter:010d}.req
        """
        return f"{priority:010d}_{counter_val:010d}.req"

    async def put(self, request: Request, priority: int = 0) -> None:
        """Enqueue request to disk.

        Args:
            request: Request to enqueue
            priority: Priority value (lower = higher priority)
        """
        await self._ensure_initialized()

        if self._closed:
            if logger.isEnabledFor(DEBUG):
                logger.debug(
                    "Put called after close(); ignoring request: %s", getattr(request, "url", None)
                )
            return

        # Check maxsize
        if self._maxsize > 0 and await self.size() >= self._maxsize:
            logger.warning(
                "Queue full (maxsize=%d), dropping request: %s", self._maxsize, request.url
            )
            return

        # Serialize request
        payload = request.to_bytes()
        if not isinstance(payload, bytes):
            raise TypeError("Request.to_bytes() did not return bytes")

        # Generate filename
        counter_val = next(self._counter)
        filename = self._make_filename(priority, counter_val)
        filepath = self._path / filename

        # Write to disk
        async with self._lock:
            try:
                async with aiofiles.open(filepath, "wb") as f:
                    await f.write(payload)
            except Exception as exc:
                logger.error("Failed to write request to disk: %s", exc)
                raise RuntimeError(f"Failed to write request to {filepath}") from exc

            # Add to in-memory queue
            await self._pq.put((priority, counter_val, filename))

    async def get(self) -> Request:
        """Get next request from queue.

        Returns:
            Next request (highest priority first)

        Raises:
            asyncio.CancelledError: If queue is closed and empty
            RuntimeError: If failed to read or decode request
        """
        await self._ensure_initialized()

        async with self._get_lock:
            if self._closed and self._pq.empty():
                raise asyncio.CancelledError

            # Get next item from priority queue
            _, _, filename = await self._pq.get()
            filepath = self._path / filename

            try:
                # Read from disk
                async with aiofiles.open(filepath, "rb") as f:
                    payload = await f.read()

                # Decode request
                if not isinstance(payload, bytes):
                    raise TypeError(f"Expected bytes from file, got {type(payload)}")

                request = Request.from_bytes(payload)

                # Delete file after successful read
                try:
                    await aiofiles.os.remove(filepath)
                except Exception as exc:
                    logger.warning("Failed to delete queue file %s: %s", filename, exc)

                return request

            except Exception as exc:
                logger.exception("Failed to read request from disk: %s", filename)
                raise RuntimeError(f"Failed to read request from {filepath}") from exc
            finally:
                with contextlib.suppress(Exception):
                    self._pq.task_done()

    async def size(self) -> int:
        """Return number of items in queue.

        Returns:
            Current queue size
        """
        await self._ensure_initialized()
        return int(self._pq.qsize())

    def maxsize(self) -> int:
        """Return maximum queue capacity.

        Returns:
            Maximum size (0 = unlimited)
        """
        return self._maxsize

    async def clear(self) -> None:
        """Remove all items from queue and delete files."""
        await self._ensure_initialized()

        async with self._lock:
            # Clear in-memory queue and delete files
            while not self._pq.empty():
                try:
                    _, _, filename = self._pq.get_nowait()
                    filepath = self._path / filename

                    try:
                        await aiofiles.os.remove(filepath)
                    except FileNotFoundError:
                        pass  # Already deleted
                    except Exception as exc:
                        logger.warning("Failed to delete queue file %s: %s", filename, exc)

                    with contextlib.suppress(Exception):
                        self._pq.task_done()

                except asyncio.QueueEmpty:
                    break

    async def close(self) -> None:
        """Mark queue as closed.

        After closing:
          - put() becomes no-op
          - get() returns remaining items then raises CancelledError
        """
        self._closed = True

        if logger.isEnabledFor(DEBUG):
            logger.debug("DiskQueue closed")

    def __repr__(self) -> str:
        size = self._pq.qsize() if self._initialized else "?"
        return f"<DiskQueue path={self._path} size={size} maxsize={self._maxsize} closed={self._closed}>"
