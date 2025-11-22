# python
# File: `qcrawl/runner/export.py`

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles

from qcrawl import exporters as _exporter
from qcrawl import signals
from qcrawl.pipelines.manager import PipelineManager
from qcrawl.storage import Storage

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from qcrawl.core.item import Item
    from qcrawl.core.spider import Spider


def _sync_write_bytes(file_obj: object, data: bytes) -> None:
    if data is None:
        return
    try:
        if hasattr(file_obj, "buffer") and file_obj.buffer is not None:
            file_obj.buffer.write(data)
        else:
            file_obj.write(data.decode("utf-8"))
        if hasattr(file_obj, "flush"):
            with contextlib.suppress(Exception):
                file_obj.flush()
    except Exception:
        with contextlib.suppress(Exception):
            file_obj.write(data.decode("utf-8"))


async def _write_to_storage(storage: Storage, relpath: str, data: bytes | str | None) -> None:
    if data is None:
        return
    b = data.encode("utf-8") if isinstance(data, str) else bytes(data)
    try:
        await storage.write(b, relpath)
    except Exception:
        logger.exception("Failed to write export data to storage %s", relpath)


def build_exporter(
    format: str | None, mode: str = "buffered", buffer_size: int = 500
) -> _exporter.Exporter:
    """Return an exporter instance for the given format/mode."""
    fmt = (format or "").lower()
    m = (mode or "").lower()

    if fmt == "ndjson":
        return _exporter.JsonLinesExporter()
    if fmt == "json":
        if m == "buffered":
            return _exporter.JsonBufferedExporter(buffer_size)
        return _exporter.JsonLinesExporter()
    if fmt == "csv":
        return _exporter.CsvExporter()
    if fmt == "xml":
        return _exporter.XmlExporter()

    raise ValueError(f"Unknown export format: {format!r}")


def register_export_handlers(
    dispatcher: signals.SignalDispatcher,
    exporter: _exporter.Exporter,
    pipeline_mgr: PipelineManager | None,
    crawler: object,
    *,
    storage: Storage | None,
    file_path: Path | None,
    storage_relpath: str | None = None,
) -> None:
    """Wire export handlers to the provided `dispatcher`."""
    crawler._cli_signal_handlers = getattr(crawler, "_cli_signal_handlers", [])

    is_storage = storage is not None
    is_path = file_path is not None

    if not (is_storage or is_path):
        raise TypeError(
            "register_export_handlers requires either a Storage instance or a Path file_path"
        )
    if is_storage and is_path:
        raise TypeError("Provide only one of storage or file_path, not both")

    if is_storage and not storage_relpath:
        raise ValueError("storage_relpath is required when passing a Storage instance")

    path_str: str | None = str(file_path) if is_path else None

    aiofile_handle: object | None = None

    async def _ensure_aiofile() -> None:
        nonlocal aiofile_handle
        if aiofile_handle is not None:
            return
        if not path_str:
            logger.error("No export path provided for aiofiles")
            return

        if str(path_str).lower() in {"-", "stdout"}:
            aiofile_handle = sys.stdout
            return

        try:
            aiofile_handle = await aiofiles.open(path_str, mode="ab")
        except Exception:
            logger.exception("Failed to open export file %s with aiofiles", path_str)
            aiofile_handle = None

    async def _aiofile_write(data: bytes | None) -> None:
        nonlocal aiofile_handle
        if data is None:
            return
        await _ensure_aiofile()
        if not path_str:
            logger.error("No export path provided for %s", path_str)
            return

        if str(path_str).lower() in {"-", "stdout"}:
            try:
                await asyncio.to_thread(_sync_write_bytes, sys.stdout, data)
            except Exception:
                logger.exception("Failed to write export data to stdout")
            return

        if aiofile_handle is None:
            logger.error("No aiofile handle available for %s", path_str)
            return
        try:
            await aiofile_handle.write(data)
            await aiofile_handle.flush()
        except Exception:
            logger.exception("aiofiles write failed for %s", path_str)

    async def _aiofile_close() -> None:
        nonlocal aiofile_handle
        if aiofile_handle is None:
            return
        try:
            if aiofile_handle is sys.stdout:
                return
            await aiofile_handle.close()
        except Exception:
            logger.exception("Failed to close aiofile for %s", path_str)
        finally:
            aiofile_handle = None

    async def _on_item_scraped(
        sender: object, item: Item, spider: Spider | None = None, **kwargs: object
    ) -> None:
        # Prefer a local variable to avoid reassigning annotated param with raw sender.
        local_spider = spider or sender
        try:
            processed: Item | None = item
            if pipeline_mgr is not None:
                try:
                    # pipeline_mgr.process_item expects a Spider; the dispatcher may pass sender.
                    # Use a type-ignore here to avoid runtime imports / circular references.
                    processed = await pipeline_mgr.process_item(item, local_spider)  # type: ignore[arg-type]
                except Exception:
                    return

            if processed is None:
                return

            try:
                data = exporter.serialize_item(processed)
            except Exception:
                logger.exception(
                    "Exporter failed to serialize item for %s", getattr(local_spider, "name", None)
                )
                return

            if is_storage:
                assert storage is not None
                assert storage_relpath is not None
                await _write_to_storage(storage, storage_relpath, data)
            else:
                if data is None:
                    return
                b = data.encode("utf-8") if isinstance(data, str) else bytes(data)
                await _aiofile_write(b)
        except Exception:
            logger.exception("Failed to export item for %s", getattr(local_spider, "name", None))

    async def _on_spider_closed(
        sender: object, spider: Spider | None = None, reason: str | None = None, **kwargs: object
    ) -> None:
        local_spider = spider or sender
        try:
            try:
                out = exporter.close()
            except Exception:
                logger.exception(
                    "Error closing exporter for %s", getattr(local_spider, "name", None)
                )
                out = None

            if is_storage:
                assert storage is not None
                assert storage_relpath is not None
                await _write_to_storage(storage, storage_relpath, out)
            else:
                if out is not None:
                    b = out.encode("utf-8") if isinstance(out, str) else bytes(out)
                    await _aiofile_write(b)
                await _aiofile_close()
        except Exception:
            logger.exception(
                "Failed to finalize export for %s", getattr(local_spider, "name", None)
            )

    dispatcher.connect("item_scraped", _on_item_scraped, weak=False)
    dispatcher.connect("spider_closed", _on_spider_closed, weak=False)

    crawler._cli_signal_handlers.extend(
        [("item_scraped", _on_item_scraped), ("spider_closed", _on_spider_closed)]
    )
