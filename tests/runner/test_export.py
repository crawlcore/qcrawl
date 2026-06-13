"""Tests for qcrawl.runner.export - Exporter factory and handler registration

Tests focus on the following behavior:
- Exporter selection based on format (ndjson, json, csv, xml)
- Buffer size and mode parameter handling
- Export handler registration with signal system
- File and stdout output integration
"""

import pytest

from qcrawl.core.item import Item
from qcrawl.runner.export import build_exporter

# Exporter Selection Tests


def test_build_exporter_ndjson():
    """build_exporter returns JsonLinesExporter for ndjson format."""
    exporter = build_exporter("ndjson")

    # Serialize an item to verify it's the right exporter type
    item = Item(data={"test": "value"})
    result = exporter.serialize_item(item)

    assert result == b'{"test":"value"}\n', "Should produce NDJSON format"


def test_build_exporter_json_buffered():
    """build_exporter returns JsonBufferedExporter for json+buffered mode."""
    exporter = build_exporter("json", mode="buffered", buffer_size=2)

    item1 = Item(data={"id": 1})
    item2 = Item(data={"id": 2})

    # First item should buffer (return None)
    result1 = exporter.serialize_item(item1)
    assert result1 is None, "Should buffer first item"

    # Second item should trigger flush
    result2 = exporter.serialize_item(item2)
    assert result2 is not None, "Should flush when buffer full"
    assert b'"id":' in result2, "Should contain JSON data"


def test_build_exporter_json_stream():
    """build_exporter returns JsonLinesExporter for json+stream mode."""
    exporter = build_exporter("json", mode="stream")

    item = Item(data={"test": "value"})
    result = exporter.serialize_item(item)

    # Stream mode should use JsonLinesExporter (immediate output)
    assert result == b'{"test":"value"}\n', "Should produce NDJSON in stream mode"


def test_build_exporter_csv():
    """build_exporter returns CsvExporter for csv format."""
    exporter = build_exporter("csv")

    item = Item(data={"name": "test", "value": "123"})
    result = exporter.serialize_item(item)

    # CSV exporter outputs each item immediately with header
    assert result is not None, "CSV should output data"
    assert b"name,value" in result or b"name" in result, "Should have CSV header or data"


def test_build_exporter_xml():
    """build_exporter returns XmlExporter for xml format."""
    exporter = build_exporter("xml")

    item = Item(data={"test": "value"})
    exporter.serialize_item(item)
    result = exporter.close()

    assert result is not None, "Should return XML data"
    assert b"<items>" in result, "Should have XML structure"
    assert b"<test>value</test>" in result, "Should have XML data"


@pytest.mark.parametrize(
    "format_name,expected_contains",
    [
        ("ndjson", b'{"test":"value"}\n'),
        ("NDJSON", b'{"test":"value"}\n'),  # Case insensitive
        ("json", None),  # Buffered returns None until buffer full
    ],
)
def test_build_exporter_case_insensitive(format_name, expected_contains):
    """build_exporter handles format names case-insensitively."""
    exporter = build_exporter(format_name)
    item = Item(data={"test": "value"})
    result = exporter.serialize_item(item)

    if expected_contains:
        assert result == expected_contains


def test_build_exporter_unknown_format():
    """build_exporter raises ValueError for unknown format."""
    with pytest.raises(ValueError, match="Unknown export format: 'invalid'"):
        build_exporter("invalid")


def test_build_exporter_none_format():
    """build_exporter raises ValueError for None format."""
    with pytest.raises(ValueError, match="Unknown export format: None"):
        build_exporter(None)


# Buffer Size Tests


def test_build_exporter_custom_buffer_size():
    """build_exporter respects custom buffer_size parameter."""
    exporter = build_exporter("json", mode="buffered", buffer_size=3)

    # Add 2 items - should buffer
    exporter.serialize_item(Item(data={"id": 1}))
    result = exporter.serialize_item(Item(data={"id": 2}))
    assert result is None, "Should buffer with size 2 < 3"

    # Add 3rd item - should flush
    result = exporter.serialize_item(Item(data={"id": 3}))
    assert result is not None, "Should flush when buffer reaches size 3"
    # Check for all 3 items in output (with or without spaces in JSON formatting)
    assert isinstance(result, bytes), "Should return bytes"
    assert b'"id"' in result, "Should contain JSON data"
    assert result.count(b'"id"') == 3, "Should contain all 3 items"


# Mode Parameter Tests


@pytest.mark.parametrize("mode", ["buffered", "BUFFERED", "Buffered"])
def test_build_exporter_mode_case_insensitive(mode):
    """build_exporter handles mode parameter case-insensitively."""
    exporter = build_exporter("json", mode=mode, buffer_size=1)
    item = Item(data={"test": "value"})

    # Buffered mode should return result when buffer is full
    result = exporter.serialize_item(item)
    assert result is not None, "Buffered mode should work regardless of case"


# Export Handler Integration Tests


@pytest.mark.asyncio
async def test_register_export_handlers_file_export(tmp_path):
    """register_export_handlers writes items to file when item_scraped signal fires."""
    from types import SimpleNamespace

    from qcrawl.runner.export import register_export_handlers
    from qcrawl.signals import SignalRegistry

    # Arrange - Set up exporter, dispatcher, and file path
    output_file = tmp_path / "output.ndjson"
    exporter = build_exporter("ndjson")
    registry = SignalRegistry()
    dispatcher = registry.for_sender(None)
    crawler = SimpleNamespace(_cli_signal_handlers=[])

    # Register handlers - this is the function we're testing
    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    # Act - Emit item_scraped signal
    test_item = Item(data={"name": "test", "value": 123})
    await dispatcher.send_async("item_scraped", item=test_item, spider=None)

    # Emit spider_closed to finalize export
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    # Assert - Check file was written
    assert output_file.exists(), "Output file should be created"

    content = output_file.read_text()
    assert "test" in content, "File should contain scraped data"
    assert "123" in content, "File should contain item values"


@pytest.mark.asyncio
async def test_register_export_handlers_stdout_export(capsys):
    """register_export_handlers writes items to stdout when file_path is '-'."""
    from pathlib import Path
    from types import SimpleNamespace

    from qcrawl.runner.export import register_export_handlers
    from qcrawl.signals import SignalRegistry

    # Arrange - Set up exporter for stdout
    exporter = build_exporter("ndjson")
    registry = SignalRegistry()
    dispatcher = registry.for_sender(None)
    crawler = SimpleNamespace(_cli_signal_handlers=[])

    # Use "-" as file_path to indicate stdout
    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=Path("-"),
    )

    # Act - Emit item_scraped signal
    test_item = Item(data={"output": "stdout"})
    await dispatcher.send_async("item_scraped", item=test_item, spider=None)

    # Emit spider_closed
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    # Assert - Check stdout output
    captured = capsys.readouterr()
    assert "stdout" in captured.out, "Should write to stdout"


@pytest.mark.asyncio
async def test_register_export_handlers_multiple_items(tmp_path):
    """register_export_handlers writes multiple items correctly."""
    from types import SimpleNamespace

    from qcrawl.runner.export import register_export_handlers
    from qcrawl.signals import SignalRegistry

    # Arrange
    output_file = tmp_path / "multi.ndjson"
    exporter = build_exporter("ndjson")
    registry = SignalRegistry()
    dispatcher = registry.for_sender(None)
    crawler = SimpleNamespace(_cli_signal_handlers=[])

    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    # Act - Emit multiple items
    items = [
        Item(data={"id": 1, "name": "first"}),
        Item(data={"id": 2, "name": "second"}),
        Item(data={"id": 3, "name": "third"}),
    ]

    for item in items:
        await dispatcher.send_async("item_scraped", item=item, spider=None)

    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    # Assert - Check all items were written
    content = output_file.read_text()
    lines = [line for line in content.split("\n") if line.strip()]

    assert len(lines) == 3, "Should have 3 items"
    assert "first" in content and "second" in content and "third" in content


@pytest.mark.asyncio
async def test_register_export_handlers_buffered_json(tmp_path):
    """register_export_handlers handles buffered JSON export correctly."""
    from types import SimpleNamespace

    from qcrawl.runner.export import register_export_handlers
    from qcrawl.signals import SignalRegistry

    # Arrange - Use buffered JSON exporter
    output_file = tmp_path / "buffered.json"
    exporter = build_exporter("json", mode="buffered", buffer_size=2)
    registry = SignalRegistry()
    dispatcher = registry.for_sender(None)
    crawler = SimpleNamespace(_cli_signal_handlers=[])

    register_export_handlers(
        dispatcher=dispatcher,
        exporter=exporter,
        crawler=crawler,
        storage=None,
        file_path=output_file,
    )

    # Act - Add items
    await dispatcher.send_async("item_scraped", item=Item(data={"id": 1}), spider=None)
    await dispatcher.send_async("item_scraped", item=Item(data={"id": 2}), spider=None)
    await dispatcher.send_async("spider_closed", spider=None, reason="finished")

    # Assert - Should have JSON array format
    content = output_file.read_text()
    assert '"id"' in content, "Should contain JSON data"
    # Buffered mode produces JSON arrays
    assert "[" in content or "{" in content, "Should have JSON structure"


@pytest.mark.asyncio
async def test_register_export_handlers_requires_path_or_storage():
    """register_export_handlers raises TypeError when neither file_path nor storage provided."""
    from types import SimpleNamespace

    from qcrawl.runner.export import register_export_handlers
    from qcrawl.signals import SignalRegistry

    exporter = build_exporter("ndjson")
    registry = SignalRegistry()
    dispatcher = registry.for_sender(None)
    crawler = SimpleNamespace(_cli_signal_handlers=[])

    # Both storage and file_path are None - should raise
    with pytest.raises(
        TypeError,
        match="register_export_handlers requires either a Storage instance or a Path file_path",
    ):
        register_export_handlers(
            dispatcher=dispatcher,
            exporter=exporter,
            crawler=crawler,
            storage=None,
            file_path=None,
        )
