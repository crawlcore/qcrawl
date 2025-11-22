from __future__ import annotations

import csv
import io
from typing import Protocol, runtime_checkable

import lxml.etree as ET
import orjson

from qcrawl.core.item import Item


@runtime_checkable
class Exporter(Protocol):
    """Structural protocol for qcrawl exporters."""

    def serialize_item(self, item: Item) -> bytes | str | None:
        """Serialize a single item and return data to write (or None if buffered)."""
        ...

    def close(self) -> bytes | None:
        """Return final chunk of data on spider close (e.g. closing array, footer)."""
        ...


class JsonBufferedExporter:
    """Buffered JSON exporter that accumulates items in a buffer and writes them as a JSON array.

    Args:
        buffer_size (int): Number of items to buffer before flushing.

    Behavior:
      - Items are buffered in-memory. When buffer reaches `buffer_size` it is
        serialized with `orjson.OPT_INDENT_2` for readable UTF-8 bytes.
      - `serialize_item` returns `None` while buffering; when a flush occurs it
        returns `bytes` to write.
      - `close()` flushes any remaining items and returns bytes (or b"" if none).
    """

    def __init__(self, buffer_size: int = 500) -> None:
        self.buffer_size = max(1, int(buffer_size))
        self.buffer: list[object] = []

    def serialize_item(self, item: Item) -> bytes | None:
        data = item.data if hasattr(item, "data") else item
        self.buffer.append(data)

        if len(self.buffer) >= self.buffer_size:
            return self._flush()
        return None

    def _flush(self) -> bytes:
        if not self.buffer:
            return b""
        out: bytes = orjson.dumps(self.buffer, option=orjson.OPT_INDENT_2) + b"\n"
        self.buffer.clear()
        return out

    def close(self) -> bytes:
        """Finalize export and return any remaining serialized data."""
        return self._flush()


class JsonLinesExporter:
    """NDJSON exporter (one JSON object per line).

    Behavior:
      - Each `serialize_item` call returns a single newline-terminated JSON line as bytes.
      - `close()` is a no-op and returns empty bytes.
    """

    def serialize_item(self, item: Item) -> bytes:
        data = item.data if hasattr(item, "data") else item
        result: bytes = orjson.dumps(data)
        return result + b"\n"

    def close(self) -> bytes:
        """Finalize export and return any remaining serialized data."""
        return b""


class CsvExporter:
    """CSV exporter that writes items as rows to a CSV file.

    Attributes:
        header_written (bool): Whether the header row has been written.
        writer (csv.DictWriter): CSV writer instance.
        output (io.StringIO): In-memory output buffer.

    Methods:
        serialize_item(item) -> bytes
            Write item as a CSV row and return bytes.
        close() -> bytes
            No-op; returns empty bytes.
    """

    def __init__(self) -> None:
        self.header_written = False
        self.writer: csv.DictWriter[str] | None = None
        self.output = io.StringIO()
        self._fieldnames: set[str] = set()

    def serialize_item(self, item: Item) -> bytes:
        data = dict(item.data) if hasattr(item, "data") else dict(item)

        # Dynamically expand fieldnames if new keys appear
        new_fields = data.keys() - self._fieldnames
        if new_fields and self.writer is not None:
            self._fieldnames.update(new_fields)
            self.writer = csv.DictWriter(self.output, fieldnames=sorted(self._fieldnames))
            self.writer.writeheader()
            self.header_written = True

        if self.writer is None:
            self._fieldnames.update(data.keys())
            self.writer = csv.DictWriter(self.output, fieldnames=sorted(self._fieldnames))
            self.writer.writeheader()
            self.header_written = True

        self.writer.writerow(data)

        result = self.output.getvalue()
        self.output.seek(0)
        self.output.truncate(0)
        return result.encode("utf-8")

    def close(self) -> bytes:
        """Finalize export and return any remaining serialized data."""
        return b""


class XmlExporter:
    """XML exporter that accumulates items and writes them as XML elements.

    Behavior:
      - Items are collected in-memory and emitted as an XML document on `close()`.
      - `serialize_item` returns None while accumulating.
      - `close()` returns UTF-8 encoded XML bytes (pretty-printed using lxml).
    """

    def __init__(self) -> None:
        self.items: list[dict[str, str]] = []

    def serialize_item(self, item: Item) -> None:
        data = item.data if hasattr(item, "data") else item
        self.items.append(dict(data))
        return None

    def close(self) -> bytes:
        """Finalize export and return any remaining serialized data."""
        if not self.items:
            return b'<?xml version="1.0" encoding="UTF-8"?>\n<items/>\n'

        root = ET.Element("items")
        for data in self.items:
            item_elem = ET.SubElement(root, "item")
            for k, v in data.items():
                child = ET.SubElement(item_elem, str(k))
                child.text = "" if v is None else str(v)

        return (
            ET.tostring(
                root,
                encoding="utf-8",
                pretty_print=True,
                xml_declaration=True,
            )
            + b"\n"
        )
