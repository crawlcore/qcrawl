from __future__ import annotations

import importlib
import inspect
import logging
from typing import TYPE_CHECKING

from qcrawl.pipelines.base import DropItem, ItemPipeline

if TYPE_CHECKING:
    from collections.abc import Mapping

    from qcrawl.core.item import Item
    from qcrawl.core.spider import Spider
    from qcrawl.settings import Settings as RuntimeSettings

logger = logging.getLogger(__name__)


class PipelineManager:
    """Orchestrate item pipeline chain execution.

    - Pipelines must be instances of `ItemPipeline`.
    - Pipeline methods (`process_item`, `open_spider`, `close_spider`) are
      expected to be `async def` coroutine functions; attempts to register
      sync callables will raise `TypeError`.
    """

    def __init__(self, pipelines: list[ItemPipeline] | None = None) -> None:
        self.pipelines: list[ItemPipeline] = list(pipelines) if pipelines else []

    def add_pipeline(self, pipeline: ItemPipeline) -> None:
        """Add pipeline to the chain.

        Raises:
            TypeError: if `pipeline` is not an `ItemPipeline` instance or if
            required hooks are not async coroutine functions.
        """
        if not isinstance(pipeline, ItemPipeline):
            raise TypeError(f"Pipeline must be ItemPipeline instance, got {type(pipeline)!r}")

        # Validate async hook signatures
        for hook in ("process_item", "open_spider", "close_spider"):
            fn = getattr(pipeline, hook, None)
            if fn is None:
                continue
            if not inspect.iscoroutinefunction(fn):
                raise TypeError(f"{pipeline!r}.{hook} must be `async def` coroutine function")

        self.pipelines.append(pipeline)

    async def process_item(self, item: Item, spider: Spider) -> Item | None:
        """Run an item through the pipeline chain.

        Returns the processed item (possibly transformed), or `None` if a
        pipeline deliberately dropped it by raising `DropItem`.

        `DropItem` is the ONLY way to drop an item. Any other exception is a bug
        in a pipeline, not an intentional drop: it is logged with a full
        traceback and re-raised so the caller surfaces it, rather than being
        silently swallowed into a `None` (which would be indistinguishable from
        a deliberate drop and would silently lose scraped data).
        """
        current = item
        for pipeline in self.pipelines:
            try:
                current = await pipeline.process_item(current, spider)
            except DropItem as exc:
                logger.info(
                    "Item dropped by %s: %s",
                    pipeline.__class__.__name__,
                    getattr(exc, "reason", exc),
                )
                return None
            except Exception:
                logger.exception(
                    "Unhandled error in pipeline %s while processing item; re-raising "
                    "(raise DropItem to drop an item intentionally)",
                    pipeline.__class__.__name__,
                )
                raise

            # Treat explicit None as a drop (be permissive about returned types)
            if current is None:
                logger.debug(
                    "Pipeline %s returned None; item treated as dropped",
                    pipeline.__class__.__name__,
                )
                return None

        return current

    async def open_spider(self, spider: Spider) -> None:
        """Call `open_spider` on all pipelines (safe to call multiple times)."""
        for pipeline in self.pipelines:
            try:
                await pipeline.open_spider(spider)
            except Exception:
                logger.exception("Error opening pipeline %s", pipeline.__class__.__name__)

    async def close_spider(self, spider: Spider) -> None:
        """Call `close_spider` on all pipelines and swallow errors while logging."""
        for pipeline in self.pipelines:
            try:
                await pipeline.close_spider(spider)
            except Exception:
                logger.exception("Error closing pipeline %s", pipeline.__class__.__name__)

    @classmethod
    def from_settings(
        cls, settings: RuntimeSettings | Mapping[str, object] | None
    ) -> PipelineManager:
        """Create PipelineManager from a runtime settings snapshot or plain dict.

        Reads the canonical `PIPELINES` setting: a dict mapping dotted pipeline
        path strings -> integer order (i.e. `dict[str, int]`). Entries that do
        not conform are ignored with a debug log.

        Sorting is stable: when priorities tie, the original input order is preserved.
        """
        pm = cls()

        if settings is None:
            return pm

        # Canonical settings key is UPPERCASE PIPELINES; get_setting resolves it
        # case-insensitively for Settings objects and plain dicts alike.
        from qcrawl.utils.settings import get_setting

        val = get_setting(settings, "PIPELINES")
        if not val:
            return pm

        if not isinstance(val, dict):
            logger.debug(
                "Ignoring pipeline setting pipelines: expected dict[str,int], got %r", type(val)
            )
            return pm

        try:
            items = list(val.items())
            normalized: list[tuple[str, int, int]] = []
            for i, (k, v) in enumerate(items):
                if not isinstance(k, str):
                    logger.debug("Skipping non-string pipeline key %r in pipelines", k)
                    continue
                try:
                    order = int(v)
                except Exception:
                    logger.debug(
                        "Skipping pipeline %r with non-integer order %r in pipelines", k, v
                    )
                    continue
                # Keep original index `i` for stable sorting when orders tie
                normalized.append((k, order, i))

            if not normalized:
                return pm

            # Stable sort: primary by order, secondary by original index
            normalized.sort(key=lambda kv: (kv[1], kv[2]))
            tokens = [k for k, _, _ in normalized]

            for token in tokens:
                # Expect dotted path string: module.Class
                try:
                    module_name, cls_name = token.rsplit(".", 1)
                except Exception:
                    logger.debug("Invalid pipeline token (expected dotted path): %r", token)
                    continue

                try:
                    mod = importlib.import_module(module_name)
                    resolved = getattr(mod, cls_name, None)
                    if resolved is None:
                        logger.debug(
                            "Pipeline class %r not found in module %s", cls_name, module_name
                        )
                        continue

                    # Only support classes that subclass ItemPipeline
                    if inspect.isclass(resolved) and issubclass(resolved, ItemPipeline):
                        try:
                            pm.add_pipeline(resolved())
                        except Exception:
                            logger.exception("Failed to instantiate pipeline class %r", resolved)
                        continue

                    logger.debug(
                        "Skipping pipeline %r: resolved object is not an ItemPipeline subclass",
                        token,
                    )
                except Exception:
                    logger.exception("Failed to import pipeline %r from settings", token)
        except Exception:
            logger.exception("Error processing pipeline settings attribute pipelines")

        return pm

    def __repr__(self) -> str:
        return f"PipelineManager(pipelines={len(self.pipelines)})"
