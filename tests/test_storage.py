"""Tests for qcrawl.storage"""

import pytest

from qcrawl.storage import FileStorage, Storage

# Storage Base Class Tests


def test_storage_init():
    """Storage initializes with URI."""
    storage = Storage(uri="file:///tmp/data")

    assert storage.uri == "file:///tmp/data"


@pytest.mark.asyncio
async def test_storage_write_not_implemented():
    """Storage.write raises NotImplementedError."""
    storage = Storage(uri="test")

    with pytest.raises(NotImplementedError):
        await storage.write(b"data", "path.txt")


@pytest.mark.asyncio
async def test_storage_read_not_implemented():
    """Storage.read raises NotImplementedError."""
    storage = Storage(uri="test")

    with pytest.raises(NotImplementedError):
        await storage.read("path.txt")


@pytest.mark.asyncio
async def test_storage_exists_not_implemented():
    """Storage.exists raises NotImplementedError."""
    storage = Storage(uri="test")

    with pytest.raises(NotImplementedError):
        await storage.exists("path.txt")


@pytest.mark.asyncio
async def test_storage_close_not_implemented():
    """Storage.close raises NotImplementedError."""
    storage = Storage(uri="test")

    with pytest.raises(NotImplementedError):
        await storage.close()


# FileStorage Initialization Tests


def test_file_storage_creates_root_directory(tmp_path):
    """FileStorage creates root directory if it doesn't exist."""
    root = tmp_path / "storage"
    assert not root.exists()

    _ = FileStorage(root=root)

    assert root.exists()
    assert root.is_dir()


def test_file_storage_with_existing_directory(tmp_path):
    """FileStorage works with existing directory."""
    root = tmp_path / "existing"
    root.mkdir()

    storage = FileStorage(root=root)

    assert storage.root == root


def test_file_storage_converts_str_to_path(tmp_path):
    """FileStorage converts string root to Path."""
    root_str = str(tmp_path / "storage")

    storage = FileStorage(root=root_str)  # type: ignore[arg-type]

    assert isinstance(storage.root, type(tmp_path))


# FileStorage Write Tests


@pytest.mark.asyncio
async def test_file_storage_write_creates_file(tmp_path):
    """FileStorage.write creates new file with data."""
    storage = FileStorage(root=tmp_path)
    data = b"Hello, World!"

    await storage.write(data, "test.txt")

    file_path = tmp_path / "test.txt"
    assert file_path.exists()
    assert file_path.read_bytes() == data


@pytest.mark.asyncio
async def test_file_storage_write_creates_subdirectories(tmp_path):
    """FileStorage.write creates missing subdirectories."""
    storage = FileStorage(root=tmp_path)
    data = b"content"

    await storage.write(data, "subdir/nested/file.txt")

    file_path = tmp_path / "subdir" / "nested" / "file.txt"
    assert file_path.exists()
    assert file_path.read_bytes() == data


@pytest.mark.asyncio
async def test_file_storage_write_appends_data(tmp_path):
    """FileStorage.write appends to existing file."""
    storage = FileStorage(root=tmp_path)

    await storage.write(b"Line 1\n", "append.txt")
    await storage.write(b"Line 2\n", "append.txt")
    await storage.write(b"Line 3\n", "append.txt")

    file_path = tmp_path / "append.txt"
    assert file_path.read_bytes() == b"Line 1\nLine 2\nLine 3\n"


# FileStorage Read Tests


@pytest.mark.asyncio
async def test_file_storage_read_existing_file(tmp_path):
    """FileStorage.read returns file contents."""
    storage = FileStorage(root=tmp_path)
    file_path = tmp_path / "data.txt"
    expected = b"File contents here"
    file_path.write_bytes(expected)

    result = await storage.read("data.txt")

    assert result == expected


@pytest.mark.asyncio
async def test_file_storage_read_from_subdirectory(tmp_path):
    """FileStorage.read reads from subdirectories."""
    storage = FileStorage(root=tmp_path)
    subdir = tmp_path / "sub" / "dir"
    subdir.mkdir(parents=True)
    file_path = subdir / "nested.txt"
    file_path.write_bytes(b"nested content")

    result = await storage.read("sub/dir/nested.txt")

    assert result == b"nested content"


@pytest.mark.asyncio
async def test_file_storage_read_nonexistent_raises_error(tmp_path):
    """FileStorage.read raises FileNotFoundError for missing file."""
    storage = FileStorage(root=tmp_path)

    with pytest.raises(FileNotFoundError):
        await storage.read("nonexistent.txt")


# FileStorage Exists Tests


@pytest.mark.asyncio
async def test_file_storage_exists_returns_true_for_existing(tmp_path):
    """FileStorage.exists returns True for existing file."""
    storage = FileStorage(root=tmp_path)
    file_path = tmp_path / "exists.txt"
    file_path.write_bytes(b"data")

    result = await storage.exists("exists.txt")

    assert result is True


@pytest.mark.asyncio
async def test_file_storage_exists_returns_false_for_missing(tmp_path):
    """FileStorage.exists returns False for missing file."""
    storage = FileStorage(root=tmp_path)

    result = await storage.exists("missing.txt")

    assert result is False


@pytest.mark.asyncio
async def test_file_storage_exists_checks_subdirectories(tmp_path):
    """FileStorage.exists checks files in subdirectories."""
    storage = FileStorage(root=tmp_path)
    subdir = tmp_path / "sub"
    subdir.mkdir()
    (subdir / "file.txt").write_bytes(b"data")

    result = await storage.exists("sub/file.txt")

    assert result is True


# FileStorage Close Tests


@pytest.mark.asyncio
async def test_file_storage_close_returns_none(tmp_path):
    """FileStorage.close is a no-op returning None."""
    storage = FileStorage(root=tmp_path)

    # Close is a no-op, just verify it doesn't raise
    await storage.close()


# Integration Tests


@pytest.mark.asyncio
async def test_file_storage_write_exists_read_cycle(tmp_path):
    """Integration: write → exists → read cycle works correctly."""
    storage = FileStorage(root=tmp_path)
    path = "integration/test.dat"
    data = b"Integration test data"

    # Write data
    await storage.write(data, path)

    # Check existence
    exists = await storage.exists(path)
    assert exists is True

    # Read data back
    read_data = await storage.read(path)
    assert read_data == data


@pytest.mark.asyncio
async def test_file_storage_multiple_writes_multiple_files(tmp_path):
    """Integration: multiple writes to different files work independently."""
    storage = FileStorage(root=tmp_path)

    await storage.write(b"File 1", "file1.txt")
    await storage.write(b"File 2", "dir1/file2.txt")
    await storage.write(b"File 3", "dir2/nested/file3.txt")

    assert await storage.read("file1.txt") == b"File 1"
    assert await storage.read("dir1/file2.txt") == b"File 2"
    assert await storage.read("dir2/nested/file3.txt") == b"File 3"
