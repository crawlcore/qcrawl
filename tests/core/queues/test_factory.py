"""Tests for qcrawl.core.queues.factory.create_queue"""

import pytest

from qcrawl.core.queues import factory


@pytest.mark.asyncio
async def test_create_queue_with_valid_memory_backend():
    """create_queue instantiates MemoryPriorityQueue."""
    q = await factory.create_queue("qcrawl.core.queues.memory.MemoryPriorityQueue")
    assert q is not None
    assert q.__class__.__name__ == "MemoryPriorityQueue"


@pytest.mark.asyncio
async def test_create_queue_with_valid_disk_backend(tmp_path):
    """create_queue instantiates DiskQueue."""
    queue_path = tmp_path / "test_queue"
    q = await factory.create_queue("qcrawl.core.queues.disk.DiskQueue", path=queue_path)
    assert q is not None
    assert q.__class__.__name__ == "DiskQueue"
    await q.close()


@pytest.mark.asyncio
async def test_create_queue_with_init_kwargs():
    """create_queue passes init_kwargs to backend."""
    q = await factory.create_queue("qcrawl.core.queues.memory.MemoryPriorityQueue", maxsize=100)
    assert q.maxsize() == 100


@pytest.mark.asyncio
async def test_create_queue_invalid_backend_format():
    """create_queue raises ValueError for invalid backend format."""
    with pytest.raises(ValueError, match="must be a dotted class path"):
        await factory.create_queue("invalid")

    with pytest.raises(ValueError, match="must be a dotted class path"):
        await factory.create_queue("")


@pytest.mark.asyncio
async def test_create_queue_module_not_found():
    """create_queue raises ImportError if module doesn't exist."""
    with pytest.raises(ImportError, match="Could not import module"):
        await factory.create_queue("nonexistent.module.Class")


@pytest.mark.asyncio
async def test_create_queue_class_not_found():
    """create_queue raises ImportError if class doesn't exist in module."""
    with pytest.raises(ImportError, match="has no attribute"):
        await factory.create_queue("qcrawl.core.queues.memory.NonExistentClass")


@pytest.mark.asyncio
async def test_create_queue_not_a_class():
    """create_queue raises TypeError if backend is not a class."""
    # Try to use a function instead of a class
    with pytest.raises(TypeError, match="is not a class"):
        await factory.create_queue("qcrawl.core.queues.factory.create_queue")


@pytest.mark.asyncio
async def test_create_queue_not_requestqueue_subclass():
    """create_queue raises TypeError if backend doesn't subclass RequestQueue."""
    # Use a regular class that doesn't subclass RequestQueue
    with pytest.raises(TypeError, match="must subclass RequestQueue"):
        await factory.create_queue("qcrawl.core.request.Request")


@pytest.mark.asyncio
async def test_create_queue_instantiation_error():
    """create_queue raises TypeError if backend instantiation fails."""
    # MemoryPriorityQueue doesn't accept invalid_arg
    with pytest.raises(TypeError, match="Failed to instantiate backend"):
        await factory.create_queue(
            "qcrawl.core.queues.memory.MemoryPriorityQueue", invalid_arg=True
        )
