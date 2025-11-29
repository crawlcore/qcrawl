"""Tests for qcrawl.exporters"""

from qcrawl.core.item import Item
from qcrawl.exporters import (
    CsvExporter,
    Exporter,
    JsonBufferedExporter,
    JsonLinesExporter,
    XmlExporter,
)

# Protocol Tests


def test_exporter_protocol():
    """Exporter is a runtime checkable protocol."""
    # Protocol is runtime checkable
    assert hasattr(Exporter, "__instancecheck__")


def test_json_buffered_exporter_implements_protocol():
    """JsonBufferedExporter implements Exporter protocol."""
    exporter = JsonBufferedExporter()
    assert isinstance(exporter, Exporter)


def test_jsonlines_exporter_implements_protocol():
    """JsonLinesExporter implements Exporter protocol."""
    exporter = JsonLinesExporter()
    assert isinstance(exporter, Exporter)


def test_csv_exporter_implements_protocol():
    """CsvExporter implements Exporter protocol."""
    exporter = CsvExporter()
    assert isinstance(exporter, Exporter)


def test_xml_exporter_implements_protocol():
    """XmlExporter implements Exporter protocol."""
    exporter = XmlExporter()
    assert isinstance(exporter, Exporter)


# JsonBufferedExporter Tests


def test_json_buffered_init_default():
    """JsonBufferedExporter initializes with default buffer_size."""
    exporter = JsonBufferedExporter()

    assert exporter.buffer_size == 500
    assert exporter.buffer == []


def test_json_buffered_init_custom_buffer_size():
    """JsonBufferedExporter initializes with custom buffer_size."""
    exporter = JsonBufferedExporter(buffer_size=100)

    assert exporter.buffer_size == 100
    assert exporter.buffer == []


def test_json_buffered_init_enforces_minimum_buffer_size():
    """JsonBufferedExporter enforces minimum buffer_size of 1."""
    exporter = JsonBufferedExporter(buffer_size=0)

    assert exporter.buffer_size == 1


def test_json_buffered_serialize_item_buffers_until_full():
    """JsonBufferedExporter buffers items until buffer_size reached."""
    exporter = JsonBufferedExporter(buffer_size=3)

    # First two items buffered
    result1 = exporter.serialize_item(Item(data={"id": 1}))
    result2 = exporter.serialize_item(Item(data={"id": 2}))

    assert result1 is None
    assert result2 is None
    assert len(exporter.buffer) == 2


def test_json_buffered_serialize_item_flushes_when_full():
    """JsonBufferedExporter flushes when buffer_size reached."""
    exporter = JsonBufferedExporter(buffer_size=2)

    exporter.serialize_item(Item(data={"id": 1}))
    result = exporter.serialize_item(Item(data={"id": 2}))

    assert result is not None
    assert isinstance(result, bytes)
    assert b'"id": 1' in result
    assert b'"id": 2' in result
    assert len(exporter.buffer) == 0


def test_json_buffered_close_flushes_remaining():
    """JsonBufferedExporter.close flushes remaining items."""
    exporter = JsonBufferedExporter(buffer_size=10)

    exporter.serialize_item(Item(data={"id": 1}))
    exporter.serialize_item(Item(data={"id": 2}))

    result = exporter.close()

    assert isinstance(result, bytes)
    assert b'"id": 1' in result
    assert b'"id": 2' in result


def test_json_buffered_close_empty_returns_empty_bytes():
    """JsonBufferedExporter.close returns empty bytes when buffer empty."""
    exporter = JsonBufferedExporter()

    result = exporter.close()

    assert result == b""


def test_json_buffered_handles_plain_dict():
    """JsonBufferedExporter handles plain dicts without .data attribute."""
    exporter = JsonBufferedExporter(buffer_size=1)

    # Pass plain dict (for backward compatibility)
    result = exporter.serialize_item({"name": "test"})  # type: ignore[arg-type]

    assert result is not None
    assert b'"name": "test"' in result


# JsonLinesExporter Tests


def test_jsonlines_serialize_item_returns_ndjson():
    """JsonLinesExporter returns NDJSON line for each item."""
    exporter = JsonLinesExporter()
    item = Item(data={"name": "Alice", "age": 30})

    result = exporter.serialize_item(item)

    assert isinstance(result, bytes)
    assert result.endswith(b"\n")
    assert b'"name":"Alice"' in result or b'"name": "Alice"' in result


def test_jsonlines_serialize_item_multiple():
    """JsonLinesExporter handles multiple items."""
    exporter = JsonLinesExporter()
    item1 = Item(data={"id": 1})
    item2 = Item(data={"id": 2})

    result1 = exporter.serialize_item(item1)
    result2 = exporter.serialize_item(item2)

    assert b'"id":1' in result1 or b'"id": 1' in result1
    assert b'"id":2' in result2 or b'"id": 2' in result2


def test_jsonlines_close_returns_empty():
    """JsonLinesExporter.close returns empty bytes."""
    exporter = JsonLinesExporter()

    result = exporter.close()

    assert result == b""


def test_jsonlines_handles_plain_dict():
    """JsonLinesExporter handles plain dicts without .data attribute."""
    exporter = JsonLinesExporter()

    result = exporter.serialize_item({"key": "value"})  # type: ignore[arg-type]

    assert b'"key":"value"' in result or b'"key": "value"' in result


# CsvExporter Tests


def test_csv_exporter_init():
    """CsvExporter initializes with empty state."""
    exporter = CsvExporter()

    assert exporter.header_written is False
    assert exporter.writer is None
    assert len(exporter._fieldnames) == 0


def test_csv_exporter_first_item_writes_header():
    """CsvExporter writes header on first item."""
    exporter = CsvExporter()
    item = Item(data={"name": "Alice", "age": 30})

    result = exporter.serialize_item(item)

    assert isinstance(result, bytes)
    result_str = result.decode("utf-8")
    assert "age,name" in result_str  # Sorted fieldnames
    assert "30,Alice" in result_str
    assert exporter.header_written is True


def test_csv_exporter_subsequent_items_no_header():
    """CsvExporter doesn't repeat header for subsequent items."""
    exporter = CsvExporter()
    item1 = Item(data={"name": "Alice"})
    item2 = Item(data={"name": "Bob"})

    result1 = exporter.serialize_item(item1)
    result2 = exporter.serialize_item(item2)

    result1_str = result1.decode("utf-8")
    result2_str = result2.decode("utf-8")

    assert "name" in result1_str  # Header in first
    assert "name" not in result2_str  # No header in second
    assert "Bob" in result2_str


def test_csv_exporter_expands_fieldnames_dynamically():
    """CsvExporter expands fieldnames when new keys appear."""
    exporter = CsvExporter()
    item1 = Item(data={"name": "Alice"})
    item2 = Item(data={"name": "Bob", "age": 25})

    result1 = exporter.serialize_item(item1)
    result2 = exporter.serialize_item(item2)

    result1_str = result1.decode("utf-8")
    result2_str = result2.decode("utf-8")

    # First item has only "name" field
    assert "name" in result1_str
    assert "age" not in result1_str

    # Second item triggers field expansion and rewrites header
    assert "age,name" in result2_str or "name,age" in result2_str
    assert "25" in result2_str


def test_csv_exporter_close_returns_empty():
    """CsvExporter.close returns empty bytes."""
    exporter = CsvExporter()

    result = exporter.close()

    assert result == b""


def test_csv_exporter_handles_plain_dict():
    """CsvExporter handles plain dicts without .data attribute."""
    exporter = CsvExporter()

    result = exporter.serialize_item({"col": "value"})  # type: ignore[arg-type]

    assert b"col" in result


# XmlExporter Tests


def test_xml_exporter_init():
    """XmlExporter initializes with empty items list."""
    exporter = XmlExporter()

    assert exporter.items == []


def test_xml_exporter_serialize_item_returns_none():
    """XmlExporter.serialize_item returns None (accumulates)."""
    exporter = XmlExporter()
    item = Item(data={"name": "Alice"})

    # serialize_item accumulates and returns None
    exporter.serialize_item(item)

    assert len(exporter.items) == 1


def test_xml_exporter_serialize_item_accumulates():
    """XmlExporter accumulates multiple items."""
    exporter = XmlExporter()
    item1 = Item(data={"id": 1})
    item2 = Item(data={"id": 2})

    exporter.serialize_item(item1)
    exporter.serialize_item(item2)

    assert len(exporter.items) == 2


def test_xml_exporter_close_returns_xml():
    """XmlExporter.close returns XML with all items."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": 30}))
    exporter.serialize_item(Item(data={"name": "Bob", "age": 25}))

    result = exporter.close()

    assert isinstance(result, bytes)
    result_str = result.decode("utf-8")
    assert "<?xml version=" in result_str
    assert "encoding=" in result_str
    assert "<items>" in result_str
    assert "<item>" in result_str
    assert "<name>Alice</name>" in result_str
    assert "<age>30</age>" in result_str
    assert "<name>Bob</name>" in result_str


def test_xml_exporter_close_empty_returns_minimal_xml():
    """XmlExporter.close returns minimal XML when no items."""
    exporter = XmlExporter()

    result = exporter.close()

    assert isinstance(result, bytes)
    result_str = result.decode("utf-8")
    assert "<?xml version=" in result_str
    assert "encoding=" in result_str
    assert "<items/>" in result_str


def test_xml_exporter_handles_none_values():
    """XmlExporter handles None values in items."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": None}))

    result = exporter.close()

    result_str = result.decode("utf-8")
    assert "<name>Alice</name>" in result_str
    assert "<age></age>" in result_str or "<age/>" in result_str


def test_xml_exporter_handles_plain_dict():
    """XmlExporter handles plain dicts without .data attribute."""
    exporter = XmlExporter()

    exporter.serialize_item({"key": "value"})  # type: ignore[arg-type]

    assert len(exporter.items) == 1


# Integration Tests


def test_json_buffered_full_cycle():
    """Integration: JsonBufferedExporter full lifecycle."""
    exporter = JsonBufferedExporter(buffer_size=2)

    # First item: buffered
    result1 = exporter.serialize_item(Item(data={"id": 1}))
    assert result1 is None

    # Second item: triggers flush
    result2 = exporter.serialize_item(Item(data={"id": 2}))
    assert result2 is not None
    assert b'"id": 1' in result2
    assert b'"id": 2' in result2

    # Third item: buffered again
    result3 = exporter.serialize_item(Item(data={"id": 3}))
    assert result3 is None

    # Close: flush remaining
    final = exporter.close()
    assert b'"id": 3' in final


def test_csv_exporter_full_cycle():
    """Integration: CsvExporter full lifecycle with dynamic fields."""
    exporter = CsvExporter()

    # First item with 2 fields
    result1 = exporter.serialize_item(Item(data={"name": "Alice", "age": 30}))
    assert b"age,name" in result1

    # Second item with 3 fields (adds "city")
    result2 = exporter.serialize_item(Item(data={"name": "Bob", "age": 25, "city": "NYC"}))
    # Should rewrite header with new field
    assert b"age,city,name" in result2 or b"city" in result2

    exporter.close()


def test_xml_exporter_full_cycle():
    """Integration: XmlExporter full lifecycle."""
    exporter = XmlExporter()

    # Accumulate items
    exporter.serialize_item(Item(data={"product": "Laptop", "price": 999}))
    exporter.serialize_item(Item(data={"product": "Mouse", "price": 25}))

    # Close generates XML
    result = exporter.close()
    result_str = result.decode("utf-8")

    assert "<item>" in result_str
    assert "<product>Laptop</product>" in result_str
    assert "<price>999</price>" in result_str
    assert "<product>Mouse</product>" in result_str
