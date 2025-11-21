from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path

from qcrawl import signals
from qcrawl.core.crawler import Crawler
from qcrawl.runner.export import build_exporter, register_export_handlers
from qcrawl.runner.pipelines import wire_pipeline_manager
from qcrawl.settings import Settings as RuntimeSettings
from qcrawl.storage import FileStorage, Storage

logger = logging.getLogger(__name__)


# Guard to prevent accidental re-entrant/duplicate runs in the same process.
_run_lock: asyncio.Lock | None = None


async def run(
    spider_cls,
    args,
    spider_settings: object | None,
    runtime_settings: RuntimeSettings,
) -> None:
    global _run_lock
    # lazy-create lock to avoid requiring an event loop at import time
    if _run_lock is None:
        _run_lock = asyncio.Lock()

    # if another run is active, warn and skip starting a second one
    if _run_lock.locked():
        logger.warning("Run already in progress in this process; skipping duplicate invocation")
        return

    async with _run_lock:
        """Shared async runner used by CLI and programmatic callers.

        - `spider_settings` is duck-typed: it may be a SpiderConfig instance, a dict,
          or any object exposing `spider_args` and (optionally) other attributes.
        - `args` is expected to be an argparse.Namespace-like object (only attributes used here).
        """
        # Extract extra constructor args in a permissive way
        extra_args = {}
        try:
            if spider_settings is not None:
                extra_args = getattr(spider_settings, "spider_args", {})
                if extra_args is None:
                    extra_args = {}
                elif not isinstance(extra_args, dict):
                    # Allow plain dict-like or fallback to {}
                    try:
                        extra_args = dict(extra_args)
                    except Exception:
                        extra_args = {}
        except Exception:
            extra_args = {}

        # Instantiate spider with permissive fallback (do not special-case runtime keys)
        try:
            spider_obj = spider_cls(**extra_args)
            if not hasattr(spider_obj, "parse"):
                raise TypeError("Spider factory returned unexpected object")
            spider = spider_obj
        except TypeError:
            spider = spider_cls()
            for k, v in extra_args.items():
                with contextlib.suppress(Exception):
                    setattr(spider, k, v)

        # Apply simple -s overrides onto spider instance
        for key, val in getattr(args, "setting", []):
            with contextlib.suppress(Exception):
                setattr(spider, key, val)

        crawler = Crawler(spider, runtime_settings=runtime_settings)

        # create queue backend
        backend = getattr(runtime_settings, "QUEUE_BACKEND", None) or "memory"
        try:
            from qcrawl.core.queues.factory import create_queue
        except Exception as e:
            logger.error("Queue factory import failed: %s", e)
            raise SystemExit(2) from e

        try:
            queue = await create_queue(str(backend))
            crawler.queue = queue
        except Exception as e:
            logger.error("Failed to create queue backend %s: %s", backend, e)
            raise SystemExit(2) from e

        global_dispatcher = signals.signals_dispatcher
        crawler._cli_signal_handlers = []

        # Pipeline wiring (runtime-settings driven) - shared helper
        pipeline_mgr = wire_pipeline_manager(runtime_settings, crawler)

        # Exporter wiring (register handlers that invoke pipelines before writing)
        # CLI args take precedence; if not provided, consult spider custom_settings.
        export_path = getattr(args, "export", None)
        export_format = getattr(args, "export_format", None)
        export_mode = getattr(args, "export_mode", None)
        export_buffer_size = getattr(args, "export_buffer_size", None)

        storage_obj: Storage | None = None
        storage_relpath: str | None = None

        if not export_path:
            # Merge spider-level custom_settings: instance overrides class
            cs_cls = getattr(spider_cls, "custom_settings", {}) or {}
            cs_inst = getattr(spider, "custom_settings", {}) or {}
            cs = dict(cs_cls)
            cs.update(cs_inst)

            # FORMATTER config (spider-level)
            fmt_cfg = cs.get("FORMATTER")
            if isinstance(fmt_cfg, dict):
                export_format = export_format or fmt_cfg.get("format")
                export_mode = export_mode or fmt_cfg.get("mode")
                export_buffer_size = export_buffer_size or fmt_cfg.get("buffer_size")

            # STORAGE config (spider-level)
            st_cfg = cs.get("STORAGE")
            if isinstance(st_cfg, dict):
                backend_name = (st_cfg.get("backend") or "").strip()
                path = (
                    st_cfg.get("path")
                    or st_cfg.get("PATH")
                    or st_cfg.get("file")
                    or st_cfg.get("File")
                )
                if path and backend_name and backend_name.lower().startswith("file"):
                    p = Path(path)
                    # FileStorage.root should be directory root; write uses filename as relpath
                    storage_obj = FileStorage(root=p.parent)
                    storage_relpath = p.name
                    # treat this as our export target (no separate export_path)
                    export_path = path

        exporter = None
        if export_path:
            # Build exporter with resolved values (fall back to defaults)
            exporter = build_exporter(
                export_format or "ndjson", export_mode or "buffered", export_buffer_size or 500
            )

            # If storage backend configured, pass storage to register_export_handlers
            # CLI args take precedence; if not provided, consult spider custom_settings.
            if storage_obj is not None:
                register_export_handlers(
                    global_dispatcher,
                    exporter,
                    pipeline_mgr,
                    crawler,
                    storage_obj,
                    storage_relpath=storage_relpath,
                )
                try:
                    await crawler.crawl()
                except Exception:
                    logger.exception("Run failed")
                    raise
            else:
                # Normalize sentinel for stdout or a normal filesystem path.
                try:
                    # Ensure parent directory exists (no-op for stdout sentinel)
                    p = Path(export_path)
                    if p.suffix:
                        p.parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    logger.debug(
                        "Could not ensure parent directory for export path %s", export_path
                    )

                # Pass the path string (including '-'/'stdout') so register_export_handlers
                # can open via aiofiles or route to stdout.
                try:
                    register_export_handlers(
                        global_dispatcher, exporter, pipeline_mgr, crawler, str(export_path)
                    )
                    try:
                        await crawler.crawl()
                    except Exception:
                        logger.exception("Run failed")
                        raise
                except Exception as e:
                    logger.exception("Failed to prepare/open export %s: %s", export_path, e)
                    raise
        else:
            # No exporter, still run with pipeline_mgr present
            try:
                await crawler.crawl()
            except Exception:
                logger.exception("Run failed")
                raise
