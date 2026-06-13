from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import TYPE_CHECKING

from qcrawl import signals
from qcrawl.core.engine import CrawlEngine
from qcrawl.core.queue import RequestQueue
from qcrawl.core.queues.memory import MemoryPriorityQueue
from qcrawl.core.scheduler import Scheduler
from qcrawl.core.spider import Spider
from qcrawl.core.stats import StatsCollector
from qcrawl.downloaders import DownloadHandlerManager
from qcrawl.middleware import DownloaderMiddleware
from qcrawl.middleware.base import SpiderMiddleware
from qcrawl.pipelines.manager import PipelineManager
from qcrawl.utils.fingerprint import RequestFingerprinter
from qcrawl.utils.settings import resolve_dotted_path

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from qcrawl.settings import Settings as RuntimeSettings


class Crawler:
    """High-level crawler API with lifecycle and middleware management.

    Responsibilities
        - Accepts a `Spider` instance and an immutable `RuntimeSettings` snapshot.
        - Creates and wires DownloadHandlerManager, Scheduler, and CrawlEngine when `crawl()` is run.
        - Accepts middleware registrations (instances, classes, or factories) before crawl.
        - Registers defensive global stats handlers so StatsCollector receives runtime signals.
        - Ensures deterministic cleanup: disconnects only handlers it actually connected,
          closes asynchronous resources and drops references for GC.
    """

    def __init__(self, spider: Spider, runtime_settings: RuntimeSettings) -> None:
        self.spider = spider
        self.runtime_settings = runtime_settings
        self.queue: RequestQueue | None = None
        self.handler_manager: DownloadHandlerManager | None = None
        self.scheduler: Scheduler | None = None
        self.engine: CrawlEngine | None = None
        self._pending_middlewares: list[object] = []
        self.stats = StatsCollector()
        self.pipeline_mgr: PipelineManager | None = None

        # Record connected global handlers for deterministic cleanup
        self._stats_handlers: list[tuple[str, Callable[..., Awaitable[object | None]]]] = []
        self._cli_signal_handlers: list[tuple[str, Callable[..., Awaitable[object | None]]]] = []

        self._finalized: bool = False
        self.signals = signals.signals_registry.for_sender(self)

        self._register_default_middlewares()

    def add_middleware(self, middleware: object) -> None:
        """Register a middleware before crawl starts.

        Accepts:
          - an instantiated DownloaderMiddleware or SpiderMiddleware
          - a middleware class (subclass of DownloaderMiddleware or SpiderMiddleware)
          - a factory callable (see `_resolve_middleware` / spider factory logic for supported signatures)

        Raises:
          - RuntimeError if called after the engine has been created (i.e., after crawl began).
        """
        if self.engine is not None:
            raise RuntimeError("Cannot add middleware after crawl() has started")
        self._pending_middlewares.append(middleware)

    def _resolve_downloader_middleware(self, mw) -> DownloaderMiddleware:
        """Resolve downloader middleware with preference: from_crawler -> settings -> spider -> ().

        Raises TypeError on invalid forms or when the resolved object is not a DownloaderMiddleware.
        """

        inst: DownloaderMiddleware | None = None

        # 1. Class-level from_crawler(crawler)
        if inspect.isclass(mw) and hasattr(mw, "from_crawler"):
            try:
                inst = mw.from_crawler(self)
            except Exception as e:
                raise TypeError(f"{mw!r}.from_crawler failed: {e}") from e
            if not isinstance(inst, DownloaderMiddleware):
                raise TypeError(f"{mw!r}.from_crawler did not return DownloaderMiddleware")
            # validate hooks below
            mw = inst

        # 2. Instance
        if isinstance(mw, DownloaderMiddleware):
            inst = mw
        # 3. Class
        elif inspect.isclass(mw) and issubclass(mw, DownloaderMiddleware):
            inst = mw()
        # 4. Factory callable: settings -> spider -> ()
        elif callable(mw):
            inst = None
            for args in ((self.runtime_settings,), (self.spider,), ()):
                try:
                    candidate = mw(*args)
                except TypeError:
                    continue
                if isinstance(candidate, DownloaderMiddleware):
                    inst = candidate
                    break
                # If factory returned non-middleware, treat as error so caller can fallback
                if candidate is not None:
                    raise TypeError(
                        f"Factory {mw!r} returned non-DownloaderMiddleware: {type(candidate)!r}"
                    )
            if inst is None:
                raise TypeError(
                    f"Factory {mw!r} doesn't accept runtime_settings/spider/() or returned invalid value"
                )
        else:
            raise TypeError(f"Invalid DownloaderMiddleware: {mw!r}")

        # Validate downloader phase hooks and lifecycle hooks are async (when present).
        for hook in ("process_request", "process_response", "process_exception"):
            fn = getattr(inst, hook, None)
            if fn is not None and not inspect.iscoroutinefunction(fn):
                raise TypeError(f"{inst.__class__.__name__}.{hook} must be `async def`")

        for lifecycle in ("open_spider", "close_spider"):
            fn = getattr(inst, lifecycle, None)
            if fn is not None and not inspect.iscoroutinefunction(fn):
                raise TypeError(f"{inst.__class__.__name__}.{lifecycle} must be `async def`")

        return inst

    def _resolve_spider_middleware(self, mw) -> SpiderMiddleware:
        """Resolve spider middleware with preference: from_crawler -> spider -> settings -> ().

        Raises TypeError on invalid forms or when the resolved object is not a SpiderMiddleware.
        """

        inst: SpiderMiddleware | None = None

        # 1. Class-level from_crawler(crawler)
        if inspect.isclass(mw) and hasattr(mw, "from_crawler"):
            try:
                inst = mw.from_crawler(self)
            except Exception as e:
                raise TypeError(f"{mw!r}.from_crawler failed: {e}") from e
            if not isinstance(inst, SpiderMiddleware):
                raise TypeError(f"{mw!r}.from_crawler did not return SpiderMiddleware")
            mw = inst

        # 2. Instance
        if isinstance(mw, SpiderMiddleware):
            inst = mw
        # 3. Class
        elif inspect.isclass(mw) and issubclass(mw, SpiderMiddleware):
            inst = mw()
        # 4. Factory callable: spider -> settings -> ()
        elif callable(mw):
            inst = None
            for args in ((self.spider,), (self.runtime_settings,), ()):
                try:
                    candidate = mw(*args)
                except TypeError:
                    continue
                if isinstance(candidate, SpiderMiddleware):
                    inst = candidate
                    break
                if candidate is not None:
                    raise TypeError(
                        f"Factory {mw!r} returned non-SpiderMiddleware: {type(candidate)!r}"
                    )
            if inst is None:
                raise TypeError(
                    f"Factory {mw!r} doesn't accept spider/runtime_settings/() or returned invalid value"
                )
        else:
            raise TypeError(f"Invalid SpiderMiddleware: {mw!r}")

        # Validate lifecycle hooks (must be async if present)
        for lifecycle in ("open_spider", "close_spider"):
            fn = getattr(inst, lifecycle, None)
            if fn is not None and not inspect.iscoroutinefunction(fn):
                raise TypeError(f"{inst.__class__.__name__}.{lifecycle} must be `async def`")

        return inst

    async def __aenter__(self) -> Crawler:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self._finalize_spider()
        await self._cleanup_resources()
        return False

    async def _cleanup_resources(self) -> None:
        """Close and release async resources. Safe to call multiple times."""
        registry = signals.signals_registry

        # Disconnect stats handlers recorded during setup
        try:
            for signal_name, handler in getattr(self, "_stats_handlers", []):
                try:
                    registry.disconnect(signal_name, handler, sender=None)
                except Exception:
                    logger.exception(
                        "Error disconnecting stats handler %s for %s", handler, signal_name
                    )
            self._stats_handlers = []
        except Exception:
            logger.exception("Error while cleaning up stats handlers")

        # Disconnect CLI-installed handlers if any
        try:
            for signal_name, handler in getattr(self, "_cli_signal_handlers", []):
                try:
                    registry.disconnect(signal_name, handler, sender=None)
                except Exception:
                    logger.exception(
                        "Error disconnecting CLI handler %s for %s", handler, signal_name
                    )
            self._cli_signal_handlers = []
        except Exception:
            logger.exception("Error while cleaning up CLI signal handlers")

        if self.handler_manager:
            await self.handler_manager.close()
            self.handler_manager = None
        if self.scheduler:
            await self.scheduler.close()
            self.scheduler = None

        # Drop queue reference held by crawler to allow GC.
        try:
            self.queue = None
        except Exception:
            logger.exception("Error clearing crawler.queue reference")

    async def _call_middlewares_open_spider(self) -> None:
        """Call `open_spider(spider)` on downloader and spider middlewares.

        Assumes middleware lifecycle hooks are async (validated at registration).
        Errors are logged and do not stop the startup sequence.
        """
        if not self.engine:
            return
        mws = list(getattr(self.engine, "middlewares", [])) + list(
            getattr(self.engine._mw_manager, "spider", [])
        )
        for mw in mws:
            fn = getattr(mw, "open_spider", None)
            if fn is None:
                continue
            try:
                # lifecycle hooks are validated to be coroutine functions at registration
                await fn(self.spider)
            except Exception:
                logger.exception("Error in middleware %s.open_spider", mw.__class__.__name__)

    async def _call_middlewares_close_spider(self) -> None:
        """Call `close_spider(spider)` on downloader and spider middlewares.

        Closed in reverse registration order to be safe. Assumes async hooks.
        Errors are logged and swallowed.
        """
        if not self.engine:
            return
        # Close in reverse registration order to be safe
        mws = list(reversed(getattr(self.engine, "middlewares", []))) + list(
            reversed(getattr(self.engine._mw_manager, "spider", []))
        )
        for mw in mws:
            fn = getattr(mw, "close_spider", None)
            if fn is None:
                continue
            try:
                # lifecycle hooks are validated to be coroutine functions at registration
                await fn(self.spider)
            except Exception:
                logger.exception("Error in middleware %s.close_spider", mw.__class__.__name__)

    async def _finalize_spider(self) -> None:
        """Run spider close hook, emit spider_closed, run middleware close hooks, and drop references. Idempotent."""
        if self._finalized:
            return
        self._finalized = True

        try:
            try:
                if self.spider is not None and hasattr(self.spider, "close_spider"):
                    await self.spider.close_spider(self.engine, reason=None)
            except Exception:
                logger.exception("Error in spider.close_spider hook")

            try:
                if self.spider is not None and getattr(self.spider, "signals", None) is not None:
                    await self.spider.signals.send_async(
                        "spider_closed", spider=self.spider, reason=None
                    )
            except Exception:
                logger.exception("Error sending spider_closed signal")

            # Call middleware close hooks (modern API) after spider close
            try:
                await self._call_middlewares_close_spider()
            except Exception:
                logger.exception("Error closing middleware hooks")

            # Close the item pipeline (owned by the Crawler).
            try:
                if self.pipeline_mgr is not None:
                    await self.pipeline_mgr.close_spider(self.spider)
            except Exception:
                logger.exception("Error closing pipeline manager")

            # print final stats snapshot
            logger.info("Final stats:\n%s", self.stats.log_stats())

        except Exception:
            logger.exception("Unexpected error finalizing spider")
        finally:
            try:
                if self.spider is not None:
                    if hasattr(self.spider, "engine"):
                        self.spider.engine = None
                    if hasattr(self.spider, "crawler"):
                        self.spider.crawler = None
                self.engine = None
            except Exception:
                logger.exception("Error clearing spider/engine references")

    def _register_default_middlewares(self) -> None:
        """Queue middlewares from runtime settings into _pending_middlewares."""
        try:
            for setting_name in ("DOWNLOADER_MIDDLEWARES", "SPIDER_MIDDLEWARES"):
                try:
                    mapping = getattr(self.runtime_settings, setting_name, None) or {}
                    if not isinstance(mapping, dict):
                        continue

                    items = list(mapping.items())
                    normalized: list[tuple[str, int, int]] = []
                    for i, (token, order) in enumerate(items):
                        try:
                            ord_int = int(order)
                        except Exception:
                            logger.debug(
                                "Skipping middleware %r with non-integer order %r in %s",
                                token,
                                order,
                                setting_name,
                            )
                            continue
                        normalized.append((token, ord_int, i))

                    if not normalized:
                        continue

                    normalized.sort(key=lambda t: (t[1], t[2]))
                    for token, _, _ in normalized:
                        try:
                            resolved = resolve_dotted_path(token, token_name=f"middleware {token}")
                            # queue the resolved object (class or callable) or token for later resolution.
                            self._pending_middlewares.append(resolved)
                        except Exception as e:
                            logger.warning(
                                "Failed to queue %s middleware %s: %s", setting_name, token, e
                            )
                except Exception:
                    logger.exception("Error loading middlewares from settings %s", setting_name)
        except Exception:
            logger.exception("Unexpected error in _register_default_middlewares")

    def _register_pending_middlewares(self) -> None:
        # middleware registration: resolve pending middlewares and install them
        assert self.engine is not None, "Engine must be initialized before registering middlewares"
        for mw in list(self._pending_middlewares):
            try:
                # Prefer resolving as DownloaderMiddleware
                try:
                    dl_inst = self._resolve_downloader_middleware(mw)
                    self.engine.add_middleware(dl_inst)
                    continue
                except TypeError:
                    # not a downloader middleware, fallthrough to spider middleware
                    pass

                # Try resolving as SpiderMiddleware
                try:
                    sp_inst = self._resolve_spider_middleware(mw)
                    # append to spider middleware chain managed by MiddlewareManager
                    self.engine._mw_manager.spider.append(sp_inst)
                    continue
                except TypeError:
                    # not a spider middleware either
                    pass

                logger.warning("Skipping invalid middleware registration: %r", mw)
            except Exception:
                logger.exception("Error registering middleware %r", mw)

        # Clear pending middlewares after registration
        self._pending_middlewares = []

    async def crawl(self) -> None:
        """Execute the full crawl workflow.

        Workflow:
          1. Apply per-spider Settings snapshot (Settings.with_overrides) FIRST.
          2. Build RequestFingerprinter using runtime settings.
          3. Create DownloadHandlerManager, Scheduler, and CrawlEngine.
          4. Resolve and install pending downloader and spider middlewares.
          5. Register stats handlers so StatsCollector receives signals.
          6. Call middleware.open_spider(), spider.open_spider(), emit spider_opened, then run engine.crawl().
          7. Ensure finalization and cleanup in all cases.
        """
        logger.info(f"Starting spider: {self.spider.name}")

        try:
            # Build final settings FIRST (before creating any components)
            final_settings = self._build_final_settings()

            # Assign finalized settings to both spider and crawler
            self.spider.runtime_settings = final_settings  # type: ignore[assignment]
            self.runtime_settings = final_settings

            # Create core components using finalized settings
            fingerprinter = RequestFingerprinter()

            # Create download handler manager
            self.handler_manager = DownloadHandlerManager(
                handler_configs=getattr(final_settings, "DOWNLOAD_HANDLERS", {}),
                settings=final_settings,
            )

            self.scheduler = Scheduler(
                queue=(getattr(self, "queue", None) or MemoryPriorityQueue()),
                fingerprinter=fingerprinter,
            )

            self.engine = CrawlEngine(
                scheduler=self.scheduler,
                handler_manager=self.handler_manager,
                spider=self.spider,
            )
            self.engine.crawler = self

            # Wire spider references to engine and crawler
            self.spider.engine = self.engine  # type: ignore[assignment]
            self.spider.crawler = self  # type: ignore[assignment]
            self.spider.signals = signals.signals_registry.for_sender(self.spider)  # type: ignore[assignment]

            # Register middlewares (they will read from finalized settings via from_crawler)
            try:
                self._register_pending_middlewares()
            except Exception:
                logger.exception("Failed to register pending middlewares")

            self._setup_stats_handlers()

            # Build the item pipeline on the engine's direct path. PIPELINES is a
            # settings-driven concern owned by the Crawler; pipelines run as an
            # explicit step before item_scraped/item_dropped are emitted, so they
            # apply whether or not anything subscribes to those signals.
            self.pipeline_mgr = PipelineManager.from_settings(final_settings)
            self.engine._item_processor = self.pipeline_mgr.process_item

            # Lifecycle hooks and crawl execution
            await self._call_middlewares_open_spider()
            await self.pipeline_mgr.open_spider(self.spider)
            await self.spider.open_spider(self.engine)
            await self.spider.signals.send_async("spider_opened", spider=self.spider)

            # Run the engine
            await self.engine.crawl()

        finally:
            await self._finalize_spider()
            await self._cleanup_resources()

    def _build_final_settings(self) -> RuntimeSettings:
        """Build final settings by merging base settings with spider custom_settings.

        Returns:
            Final settings with spider overrides applied.
        """
        base_settings = self.runtime_settings
        overrides: dict[str, object] = {}

        # Collect class-level custom_settings
        cls_cs = getattr(self.spider.__class__, "custom_settings", {}) or {}
        if isinstance(cls_cs, dict):
            overrides.update({k: v for k, v in cls_cs.items() if v is not None})

        # Collect instance-level custom_settings (from __init__)
        inst_cs = getattr(self.spider, "custom_settings", {}) or {}
        if isinstance(inst_cs, dict):
            overrides.update({k: v for k, v in inst_cs.items() if v is not None})

        # No overrides, return base settings unchanged
        if not overrides:
            return base_settings

        try:
            # Get available runtime setting keys (uppercase)
            runtime_keys: set[str] = set()
            try:
                # Use asdict() to get all settings fields, not just the subset from to_dict()
                runtime_keys = {k.upper() for k in asdict(base_settings)}
            except Exception:
                logger.warning("Could not read runtime settings keys")
                return base_settings

            # Filter and normalize overrides to match runtime settings
            filtered: dict[str, object] = {}
            for k, v in overrides.items():
                if isinstance(k, str) and k.upper() in runtime_keys:
                    filtered[k.upper()] = v  # Normalize to uppercase
                else:
                    logger.debug("Spider custom_setting '%s' not in RuntimeSettings, skipping", k)

            # No valid overrides found
            if not filtered:
                return base_settings

            # Apply overrides using Settings.with_overrides()
            try:
                return base_settings.with_overrides(filtered)
            except Exception:
                logger.exception("Failed to apply spider custom_settings; using base settings")
                return base_settings

        except Exception:
            logger.exception("Error building final settings; using base settings")
            return base_settings

    def _setup_stats_handlers(self) -> None:
        """Connect stats collector to global signals dispatcher defensively."""

        async def on_spider_opened(sender, spider=None, **kwargs):
            spider = spider or sender
            self.stats.open_spider(spider)

        async def on_spider_closed(sender, spider=None, reason=None, **kwargs):
            spider = spider or sender
            self.stats.close_spider(spider, reason=str(reason) if reason else "finished")

        async def on_item_scraped(sender, item, spider=None, **kwargs):
            spider = spider or sender
            self.stats.inc_value("pipeline/item_scraped_count")

        async def on_item_dropped(sender, item=None, spider=None, **kwargs):
            spider = spider or sender
            self.stats.inc_value("pipeline/item_dropped_count")

        async def on_request_scheduled(sender, request, spider=None, **kwargs):
            spider = spider or sender
            self.stats.inc_value("scheduler/request_scheduled_count")

        async def on_request_reached_downloader(sender, request, spider=None, **kwargs):
            spider = spider or sender
            self.stats.inc_value("downloader/request_downloaded_count")

        async def on_response_received(sender, response, request, spider=None, **kwargs):
            try:
                spider = spider or sender
                self.stats.inc_value("downloader/response_status_count")
                self.stats.inc_value(
                    f"downloader/response_status_{int(getattr(response, 'status_code', 0))}"
                )
            except Exception:
                logger.exception("Error updating response stats")

        async def on_request_dropped(sender, request, exception, spider=None, **kwargs):
            spider = spider or sender
            self.stats.inc_value("scheduler/dequeued")
            self.stats.inc_value("engine/error_count")

        async def on_bytes_received(sender, data, request, **kwargs):
            try:
                self.stats.inc_value(
                    "downloader/bytes_downloaded", count=len(data) if data is not None else 0
                )
            except Exception:
                logger.exception("Error updating bytes_downloaded stat")

        def _try_connect(signal_name: str, handler):
            try:
                signals.signals_dispatcher.connect(signal_name, handler, weak=False)
                self._stats_handlers.append((signal_name, handler))
            except Exception:
                logger.exception(
                    "Failed to connect stats handler %s for signal %s", handler, signal_name
                )

        self._stats_handlers = []
        _try_connect("spider_opened", on_spider_opened)
        _try_connect("spider_closed", on_spider_closed)
        _try_connect("item_scraped", on_item_scraped)
        _try_connect("item_dropped", on_item_dropped)
        _try_connect("request_scheduled", on_request_scheduled)
        _try_connect("request_reached_downloader", on_request_reached_downloader)
        _try_connect("response_received", on_response_received)
        _try_connect("request_dropped", on_request_dropped)
        _try_connect("bytes_received", on_bytes_received)
