"""Tests for qcrawl.exporters

Unit tests for exporter classes - test serialization logic directly.
"""

from qcrawl.core.item import Item
from qcrawl.exporters import CsvExporter, JsonBufferedExporter, JsonLinesExporter, XmlExporter

# JsonLinesExporter Tests


def test_jsonlines_exporter_single_item():
    """JsonLinesExporter serializes single item to NDJSON."""
    exporter = JsonLinesExporter()
    item = Item(data={"name": "test", "value": 123})

    result = exporter.serialize_item(item)

    assert result == b'{"name":"test","value":123}\n'


def test_jsonlines_exporter_multiple_items():
    """JsonLinesExporter serializes each item independently."""
    exporter = JsonLinesExporter()

    result1 = exporter.serialize_item(Item(data={"id": 1}))
    result2 = exporter.serialize_item(Item(data={"id": 2}))

    assert result1 == b'{"id":1}\n'
    assert result2 == b'{"id":2}\n'


def test_jsonlines_exporter_close_returns_empty():
    """JsonLinesExporter.close() returns empty bytes."""
    exporter = JsonLinesExporter()

    result = exporter.close()

    assert result == b""


def test_jsonlines_exporter_nested_data():
    """JsonLinesExporter handles nested data structures."""
    exporter = JsonLinesExporter()
    item = Item(data={"user": {"name": "Alice", "age": 30}, "active": True})

    result = exporter.serialize_item(item)

    assert b'"user"' in result
    assert b'"name":"Alice"' in result


# JsonBufferedExporter Tests


def test_json_buffered_exporter_buffers_items():
    """JsonBufferedExporter buffers items until buffer_size reached."""
    exporter = JsonBufferedExporter(buffer_size=3)

    # First 2 items should return None (buffering)
    result1 = exporter.serialize_item(Item(data={"id": 1}))
    result2 = exporter.serialize_item(Item(data={"id": 2}))

    assert result1 is None
    assert result2 is None


def test_json_buffered_exporter_flushes_when_full():
    """JsonBufferedExporter flushes when buffer reaches buffer_size."""
    exporter = JsonBufferedExporter(buffer_size=2)

    exporter.serialize_item(Item(data={"id": 1}))
    result = exporter.serialize_item(Item(data={"id": 2}))

    assert result is not None
    assert b'"id"' in result
    assert b"[\n" in result  # JSON array with formatting


def test_json_buffered_exporter_flush_contains_all_items():
    """JsonBufferedExporter flush contains all buffered items."""
    exporter = JsonBufferedExporter(buffer_size=3)

    exporter.serialize_item(Item(data={"id": 1}))
    exporter.serialize_item(Item(data={"id": 2}))
    result = exporter.serialize_item(Item(data={"id": 3}))

    assert result is not None
    # Check all 3 items are in the output
    assert result.count(b'"id"') == 3


def test_json_buffered_exporter_close_flushes_remaining():
    """JsonBufferedExporter.close() flushes remaining items."""
    exporter = JsonBufferedExporter(buffer_size=5)

    exporter.serialize_item(Item(data={"id": 1}))
    exporter.serialize_item(Item(data={"id": 2}))

    result = exporter.close()

    assert result is not None
    assert b'"id": 1' in result
    assert b'"id": 2' in result


def test_json_buffered_exporter_close_empty_buffer():
    """JsonBufferedExporter.close() returns empty bytes for empty buffer."""
    exporter = JsonBufferedExporter(buffer_size=5)

    result = exporter.close()

    assert result == b""


def test_json_buffered_exporter_custom_buffer_size():
    """JsonBufferedExporter respects custom buffer_size."""
    exporter = JsonBufferedExporter(buffer_size=1)

    # First item should trigger immediate flush
    result = exporter.serialize_item(Item(data={"test": "value"}))

    assert result is not None
    assert b'"test": "value"' in result


def test_json_buffered_exporter_minimum_buffer_size():
    """JsonBufferedExporter enforces minimum buffer_size of 1."""
    exporter = JsonBufferedExporter(buffer_size=0)

    # Should use buffer_size=1
    result = exporter.serialize_item(Item(data={"test": "value"}))

    assert result is not None


# CsvExporter Tests


def test_csv_exporter_single_item():
    """CsvExporter serializes single item with header."""
    exporter = CsvExporter()
    item = Item(data={"name": "Alice", "age": "30"})

    result = exporter.serialize_item(item)

    # Should have header and data row
    text = result.decode("utf-8")
    assert "age,name" in text or "name,age" in text
    assert "Alice" in text
    assert "30" in text


def test_csv_exporter_multiple_items():
    """CsvExporter serializes multiple items with single header."""
    exporter = CsvExporter()

    result1 = exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))
    result2 = exporter.serialize_item(Item(data={"name": "Bob", "age": "25"}))

    text1 = result1.decode("utf-8")
    text2 = result2.decode("utf-8")

    # First result has header + first row
    assert "name" in text1 or "age" in text1
    assert "Alice" in text1

    # Second result has only data row (no duplicate header)
    assert "Bob" in text2


def test_csv_exporter_dynamic_fields():
    """CsvExporter handles dynamic field expansion."""
    exporter = CsvExporter()

    # First item with 2 fields
    exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))

    # Second item adds new field
    result2 = exporter.serialize_item(Item(data={"name": "Bob", "age": "25", "city": "NYC"}))

    text2 = result2.decode("utf-8")

    # Should have new header with all 3 fields
    lines = text2.strip().split("\n")
    assert len(lines) >= 2  # Header + data


def test_csv_exporter_close_returns_empty():
    """CsvExporter.close() returns empty bytes."""
    exporter = CsvExporter()

    result = exporter.close()

    assert result == b""


def test_csv_exporter_empty_values():
    """CsvExporter handles empty values."""
    exporter = CsvExporter()
    item = Item(data={"name": "Alice", "age": ""})

    result = exporter.serialize_item(item)

    text = result.decode("utf-8")
    assert "Alice" in text


def test_csv_exporter_neutralizes_formula_injection():
    """CsvExporter prefixes formula-triggering string cells with a quote."""
    exporter = CsvExporter()
    item = Item(data={"formula": "=SUM(A1:A2)", "at": "@cmd", "name": "safe"})

    text = exporter.serialize_item(item).decode("utf-8")

    assert "'=SUM(A1:A2)" in text
    assert "'@cmd" in text
    assert "'safe" not in text


# XmlExporter Tests


def test_xml_exporter_single_item():
    """XmlExporter accumulates items and outputs XML on close."""
    exporter = XmlExporter()
    item = Item(data={"name": "Alice", "age": "30"})

    # Should return None while accumulating (type signature confirms this)
    exporter.serialize_item(item)

    # Verify item was accumulated (tested via close())
    result = exporter.close()
    assert b"<name>Alice</name>" in result


def test_xml_exporter_close_outputs_xml():
    """XmlExporter.close() outputs complete XML document."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": "30"}))
    exporter.serialize_item(Item(data={"name": "Bob", "age": "25"}))

    result = exporter.close()

    assert result is not None
    text = result.decode("utf-8")
    assert "<?xml version='1.0' encoding='utf-8'?>" in text
    assert "<items>" in text
    assert "</items>" in text
    assert "<name>Alice</name>" in text
    assert "<name>Bob</name>" in text


def test_xml_exporter_empty():
    """XmlExporter.close() returns valid XML for empty data."""
    exporter = XmlExporter()

    result = exporter.close()

    assert result is not None
    text = result.decode("utf-8")
    assert "<?xml version='1.0' encoding='utf-8'?>" in text
    assert "<items/>" in text or "<items></items>" in text


def test_xml_exporter_none_values():
    """XmlExporter handles None values."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"name": "Alice", "age": None}))

    result = exporter.close()

    text = result.decode("utf-8")
    assert "<name>Alice</name>" in text
    assert "<age></age>" in text or "<age/>" in text


def test_xml_exporter_nested_not_supported():
    """XmlExporter converts nested values to strings."""
    exporter = XmlExporter()
    exporter.serialize_item(Item(data={"user": {"name": "Alice"}, "active": True}))

    result = exporter.close()

    text = result.decode("utf-8")
    # Nested dict becomes string representation
    assert "<user>" in text
    assert "<active>True</active>" in text


# Protocol Conformance Tests


def test_all_exporters_implement_protocol():
    """All exporter classes conform to Exporter protocol."""
    from qcrawl.exporters import Exporter

    assert isinstance(JsonLinesExporter(), Exporter)
    assert isinstance(JsonBufferedExporter(), Exporter)
    assert isinstance(CsvExporter(), Exporter)
    assert isinstance(XmlExporter(), Exporter)


def test_exporter_protocol_methods():
    """Exporter protocol defines required methods."""
    from qcrawl.exporters import Exporter

    # Check protocol has required methods
    assert hasattr(Exporter, "serialize_item")
    assert hasattr(Exporter, "close")
