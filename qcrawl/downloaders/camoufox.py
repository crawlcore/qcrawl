"""Camoufox-based downloader for JavaScript rendering and anti-bot evasion.

This downloader uses Camoufox (stealth Firefox browser) for sites that:
- Require JavaScript rendering
- Have anti-bot detection
- Need browser fingerprint evasion

Features:
- Named context management with pre-creation
- Page methods execution
- Event handlers registration
- Request abort predicate
- Header processing modes
- Remote browser (CDP) support
- Page object in response meta

Note: Requires the 'camoufox' package to be installed.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging

from qcrawl import signals
from qcrawl.core.page import PageMethod
from qcrawl.core.request import Request
from qcrawl.core.response import Page

# Optional camoufox import - fallback types if not installed
AsyncCamoufox: type[object] = object
AsyncNewBrowser: type[object] = object
AsyncPage: type[object] = object

try:
    _camoufox_mod = importlib.import_module("camoufox.async_api")
    AsyncCamoufox = getattr(_camoufox_mod, "AsyncCamoufox", object)
    AsyncNewBrowser = getattr(_camoufox_mod, "AsyncNewBrowser", object)
    AsyncPage = getattr(_camoufox_mod, "AsyncPage", object)
except Exception:
    # Camoufox extras not installed — keep fallbacks
    pass

logger = logging.getLogger(__name__)


class CamoufoxDownloader:
    """Async browser-based downloader for Camoufox browser.

    Responsibilities:
      - Manage named browser contexts (pre-created at startup)
      - Execute page methods and register event handlers
      - Support request abort predicates and header processing
      - Render JavaScript and return final DOM content
      - Emit response/bytes signals
      - Handle remote browser connections (CDP)

    Architecture:
      - Global context limit via semaphore (CAMOUFOX_MAX_CONTEXTS)
      - Per-context page limits via individual semaphores (CAMOUFOX_MAX_PAGES_PER_CONTEXT)
      - All contexts from CAMOUFOX_CONTEXTS are pre-created at startup
      - Supports remote browser via CDP URL or local browser launch

    Meta Keys Supported:
      - camoufox_context: str - Named context to use (must be pre-defined)
      - camoufox_include_page: bool - Include Page object in response.meta['camoufox_page']
      - camoufox_page_methods: list[dict] - Page actions to execute
      - camoufox_page_event_handlers: dict[str, callable] - Event listeners to register
      - camoufox_page_goto_kwargs: dict - Navigation options (wait_until, timeout, etc.)
    """

    __slots__ = (
        "_browser",
        "_closed",
        "signals",
        "_own_browser",
        "_contexts",
        "_context_configs",
        "_context_semaphore",
        "_page_semaphores",
        "_max_contexts",
        "_max_pages_per_context",
        "_default_timeout",
        "_launch_options",
        "_abort_request",
        "_process_request_headers",
    )

    def __init__(
        self,
        browser: object,
        *,
        own_browser: bool = True,
        contexts: dict[str, dict[str, object]],
        max_contexts: int = 10,
        max_pages_per_context: int = 5,
        default_timeout: float = 30000.0,
        launch_options: dict[str, object] | None = None,
        abort_request: object | None = None,
        process_request_headers: str | object = "use_qcrawl_headers",
    ) -> None:
        """Initialize downloader with a Camoufox browser instance.

        Args:
            browser: Camoufox browser instance (AsyncNewBrowser)
            own_browser: Whether this downloader owns the browser (will close it)
            contexts: Named context configurations (will be pre-created)
            max_contexts: Global maximum concurrent browser contexts
            max_pages_per_context: Maximum concurrent pages per context
            default_timeout: Default page load timeout in milliseconds
            launch_options: Browser launch options
            abort_request: Optional callable to filter/abort requests
            process_request_headers: Header processing mode
        """
        self._browser = browser
        self._own_browser = bool(own_browser)
        self.signals = signals.signals_registry.for_sender(self)
        self._closed = False

        # Context management
        self._contexts: dict[str, object] = {}  # name -> context instance
        self._context_configs = contexts or {}
        self._context_semaphore = asyncio.Semaphore(max_contexts)
        self._page_semaphores: dict[str, asyncio.Semaphore] = {}
        self._max_contexts = max_contexts
        self._max_pages_per_context = max_pages_per_context

        # Settings
        self._default_timeout = default_timeout
        self._launch_options = launch_options or {}
        self._abort_request = abort_request
        self._process_request_headers = process_request_headers

    @classmethod
    async def create(
        cls,
        *,
        settings: dict[str, object] | None = None,
    ) -> CamoufoxDownloader:
        """Create downloader with an active Camoufox browser and pre-created contexts.

        Settings dict keys (from CAMOUFOX_* settings):
          - contexts: Named context configurations
          - max_contexts: Global context limit
          - max_pages_per_context: Pages per context limit
          - default_timeout: Page load timeout in milliseconds
          - launch_options: Browser launch options (headless, args, etc.)
          - cdp_url: Remote browser CDP endpoint (if provided, connects instead of launching)
          - abort_request: Callable to filter requests
          - process_request_headers: Header processing mode

        Returns:
            CamoufoxDownloader instance with active browser and pre-created contexts
        """
        cfg = settings or {}

        # Extract settings
        contexts = cfg.get("contexts", {"default": {}})
        max_contexts = cfg.get("max_contexts", 10)
        max_pages_per_context = cfg.get("max_pages_per_context", 5)
        default_timeout = cfg.get("default_timeout", 30000.0)
        launch_options = cfg.get("launch_options", {})
        cdp_url = cfg.get("cdp_url")
        abort_request = cfg.get("abort_request")
        process_request_headers = cfg.get("process_request_headers", "use_qcrawl_headers")

        # Type conversions
        if not isinstance(contexts, dict):
            contexts = {"default": {}}
        max_contexts = int(max_contexts) if isinstance(max_contexts, int) else 10
        max_pages_per_context = (
            int(max_pages_per_context) if isinstance(max_pages_per_context, int) else 5
        )
        default_timeout = (
            float(default_timeout) if isinstance(default_timeout, (int, float)) else 30000.0
        )
        if not isinstance(launch_options, dict):
            launch_options = {}

        # Launch or connect to browser
        if cdp_url:
            # Connect to existing browser via CDP
            logger.info("Connecting to remote Camoufox browser at %s", cdp_url)
            browser = await AsyncCamoufox.connect(cdp_url)
            own_browser = False
        else:
            # Launch new browser using async context manager
            logger.info("Launching Camoufox browser with options: %s", launch_options)
            camoufox_cm = AsyncCamoufox(**launch_options)
            browser = await camoufox_cm.__aenter__()
            own_browser = True

        # Create downloader instance
        downloader = cls(
            browser,
            own_browser=own_browser,
            contexts=contexts,  # type: ignore[arg-type]
            max_contexts=max_contexts,
            max_pages_per_context=max_pages_per_context,
            default_timeout=default_timeout,
            launch_options=launch_options,
            abort_request=abort_request,
            process_request_headers=process_request_headers,
        )

        # Pre-create all named contexts
        await downloader._create_all_contexts()

        logger.info(
            "Camoufox downloader created with %d pre-created contexts: %s",
            len(downloader._contexts),
            list(downloader._contexts.keys()),
        )

        return downloader

    async def _create_all_contexts(self) -> None:
        """Pre-create all named contexts defined in CAMOUFOX_CONTEXTS.

        Each context is created once and reused for all requests.
        Creates a per-context semaphore to limit concurrent pages.
        """
        for name, config in self._context_configs.items():
            try:
                # Create context with configuration
                context = await self._browser.new_context(**config)
                self._contexts[name] = context

                # Create per-context page semaphore
                self._page_semaphores[name] = asyncio.Semaphore(self._max_pages_per_context)

                logger.debug("Pre-created context %r with config: %s", name, config)
            except Exception:
                logger.exception("Failed to create context %r", name)
                raise

    def _get_context(self, name: str = "default") -> object:
        """Get pre-created context by name.

        Args:
            name: Context name from CAMOUFOX_CONTEXTS

        Returns:
            Browser context instance

        Raises:
            RuntimeError: If context name is undefined
        """
        if name not in self._contexts:
            raise RuntimeError(
                f"Context {name!r} not found. "
                f"Available contexts: {list(self._contexts.keys())}. "
                f"Define contexts in CAMOUFOX_CONTEXTS setting."
            )
        return self._contexts[name]

    async def fetch(
        self,
        request: Request | str,
        *,
        spider: object | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 180.0,
    ) -> Page:
        """Fetch request by rendering page with browser.

        Supports advanced features via request.meta:
          - camoufox_context: Named context to use
          - camoufox_include_page: Include Page object in response.meta
          - camoufox_page_methods: List of page actions to execute
          - camoufox_page_event_handlers: Event listeners to register
          - camoufox_page_goto_kwargs: Navigation options

        Args:
            request: Request object or URL string
            spider: Spider instance (for accessing runtime settings)
            headers: Additional headers to set
            timeout: Request timeout in seconds (converted to milliseconds)

        Returns:
            Page object with rendered HTML content

        Raises:
            RuntimeError: If downloader is closed or context undefined
            asyncio.TimeoutError: If page load exceeds timeout
            Exception: Other browser-related errors
        """
        if self._closed:
            raise RuntimeError("Cannot fetch: downloader is closed")

        if isinstance(request, str):
            request = Request(url=request)

        # Extract meta keys
        context_name = request.meta.get("camoufox_context", "default")
        include_page = request.meta.get("camoufox_include_page", False)
        page_methods = request.meta.get("camoufox_page_methods", [])
        event_handlers = request.meta.get("camoufox_page_event_handlers", {})
        goto_kwargs = request.meta.get("camoufox_page_goto_kwargs", {})

        # Get context and its page semaphore
        context = self._get_context(str(context_name) if context_name else "default")
        page_semaphore = self._page_semaphores[str(context_name) if context_name else "default"]

        # Acquire per-context page semaphore
        async with page_semaphore:
            page: object | None = None

            try:
                # Create new page in context
                page = await context.new_page()

                # Set timeout
                page_timeout = timeout * 1000  # Convert seconds to milliseconds
                if hasattr(page, "set_default_timeout"):
                    page.set_default_timeout(page_timeout)

                # Register event handlers
                if event_handlers and isinstance(event_handlers, dict):
                    await self._register_event_handlers(page, event_handlers)

                # Set extra headers if provided
                processed_headers = self._process_headers(request, spider, headers)
                if processed_headers:
                    await context.set_extra_http_headers(processed_headers)

                # Execute page methods (before navigation)
                if page_methods and isinstance(page_methods, list):
                    await self._execute_page_methods(page, page_methods, before_navigation=True)

                # Prepare navigation kwargs
                nav_kwargs = self._prepare_goto_kwargs(
                    goto_kwargs if isinstance(goto_kwargs, dict) else {}, page_timeout
                )

                # Navigate to URL
                response = None
                if hasattr(page, "goto"):
                    response = await page.goto(request.url, **nav_kwargs)

                # Execute page methods (after navigation)
                if page_methods and isinstance(page_methods, list):
                    await self._execute_page_methods(page, page_methods, before_navigation=False)

                # Get final rendered HTML
                content = ""
                if hasattr(page, "content"):
                    content = await page.content()

                # Build Page object
                page_url = str(page.url) if hasattr(page, "url") else request.url
                status_code = response.status if (response and hasattr(response, "status")) else 200
                result = Page(
                    url=page_url,
                    status_code=status_code,
                    headers={},  # Camoufox doesn't easily expose response headers
                    content=content.encode("utf-8"),
                    request=request,
                )

                # Include page object in meta if requested
                if include_page:
                    result.meta["camoufox_page"] = page

                # Store page methods with results in meta
                if page_methods:
                    result.meta["camoufox_page_methods"] = page_methods

                # Emit signals
                try:
                    await self.signals.send_async(
                        "response_received",
                        response=result,
                        request=request,
                    )
                    await self.signals.send_async(
                        "bytes_received",
                        data=result.content,
                        request=request,
                    )
                except Exception:
                    logger.exception("Error dispatching signal for %s", request.url)

                return result

            except TimeoutError:
                logger.error(
                    "Browser timeout after %.1fms for %s",
                    page_timeout,
                    getattr(request, "url", None),
                )
                raise

            except Exception:
                logger.exception("Browser error fetching %s", getattr(request, "url", None))
                raise

            finally:
                # Clean up page (unless included in response.meta)
                if page and not include_page:
                    try:
                        await page.close()
                    except Exception:
                        logger.exception("Error closing page for %s", getattr(request, "url", None))

    async def _execute_page_methods(
        self, page: object, methods: list[PageMethod | dict[str, object]], before_navigation: bool
    ) -> None:
        """Execute page methods in sequence.

        Page methods can be executed before or after navigation based on their timing.
        Supports both PageMethod objects and dict descriptors for backward compatibility.

        Args:
            page: Camoufox page instance
            methods: List of PageMethod objects or dict descriptors
            before_navigation: If True, only execute methods marked for "before" timing
        """
        for method_obj in methods:
            # Convert dict to PageMethod if needed
            if isinstance(method_obj, dict):
                method_obj = PageMethod.from_dict(method_obj)
            elif not isinstance(method_obj, PageMethod):
                logger.warning("Invalid page method type: %s", type(method_obj))
                continue

            # Filter by timing
            if before_navigation and method_obj.timing != "before":
                continue
            if not before_navigation and method_obj.timing == "before":
                continue

            try:
                # Execute page method
                method_name = method_obj.method
                if not isinstance(method_name, str):
                    logger.warning("Page method name must be string, got: %s", type(method_name))
                    continue

                page_method = getattr(page, method_name, None)
                if page_method is None:
                    logger.warning("Page has no method %r", method_name)
                    continue

                # Execute method
                if inspect.iscoroutinefunction(page_method):
                    result = await page_method(*method_obj.args, **method_obj.kwargs)
                else:
                    result = page_method(*method_obj.args, **method_obj.kwargs)

                logger.debug(
                    "Executed page method %r with args=%s, kwargs=%s -> %s",
                    method_name,
                    method_obj.args,
                    method_obj.kwargs,
                    result,
                )

                # Store result in PageMethod object
                method_obj.result = result

            except Exception:
                logger.exception(
                    "Error executing page method %r",
                    method_obj.method,
                )
                raise

    async def _register_event_handlers(self, page: object, handlers: dict[str, object]) -> None:
        """Register event handlers on page.

        Args:
            page: Camoufox page instance
            handlers: Mapping of event_name -> handler_callable
        """
        for event_name, handler in handlers.items():
            if not callable(handler):
                logger.warning("Event handler for %r is not callable: %s", event_name, handler)
                continue

            try:
                page.on(event_name, handler)
                logger.debug("Registered event handler for %r", event_name)
            except Exception:
                logger.exception("Error registering event handler for %r", event_name)
                raise

    def _process_headers(
        self, request: Request, spider: object | None, headers: dict[str, str] | None
    ) -> dict[str, str]:
        """Process request headers based on CAMOUFOX_PROCESS_REQUEST_HEADERS mode.

        Modes:
          - "use_qcrawl_headers": Merge qCrawl headers with provided headers
          - "ignore": Return empty dict (let browser handle headers)
          - callable: Custom function(request, default_headers) -> dict

        Args:
            request: Request object
            spider: Spider instance
            headers: Additional headers

        Returns:
            Processed headers dict
        """
        mode = self._process_request_headers

        if mode == "ignore":
            return {}

        if mode == "use_qcrawl_headers":
            # Get default headers from spider settings
            default_headers: dict[str, str] = {}
            if spider and hasattr(spider, "runtime_settings"):
                runtime_headers = getattr(spider.runtime_settings, "DEFAULT_REQUEST_HEADERS", {})
                if isinstance(runtime_headers, dict):
                    default_headers = dict(runtime_headers)

            # Merge: default < request.headers < provided headers
            result: dict[str, str] = dict(default_headers)
            if (
                hasattr(request, "headers")
                and request.headers
                and isinstance(request.headers, dict)
            ):
                result.update(request.headers)
            if headers:
                result.update(headers)
            return result

        if callable(mode):
            # Custom header processor
            try:
                default_headers_obj: dict[str, str] = {}
                if spider and hasattr(spider, "runtime_settings"):
                    runtime_headers_obj = getattr(
                        spider.runtime_settings, "DEFAULT_REQUEST_HEADERS", {}
                    )
                    if isinstance(runtime_headers_obj, dict):
                        default_headers_obj = dict(runtime_headers_obj)
                result_obj = mode(request, default_headers_obj)
                if isinstance(result_obj, dict):
                    return result_obj
                return {}
            except Exception:
                logger.exception("Error in custom header processor")
                return {}

        logger.warning("Unknown header processing mode: %r", mode)
        return {}

    def _prepare_goto_kwargs(
        self, goto_kwargs: dict[str, object], default_timeout: float
    ) -> dict[str, object]:
        """Prepare navigation kwargs with defaults.

        Args:
            goto_kwargs: User-provided navigation options
            default_timeout: Default timeout in milliseconds

        Returns:
            Merged navigation kwargs
        """
        result: dict[str, object] = {
            "wait_until": "domcontentloaded",
            "timeout": default_timeout,
        }
        result.update(goto_kwargs)
        return result

    async def close(self) -> None:
        """Close browser contexts and browser instance.

        Only closes the browser if this downloader owns it.
        Safe to call multiple times (idempotent).
        """
        if self._closed:
            return

        self._closed = True

        # Close all contexts
        for name, context in list(self._contexts.items()):
            try:
                await context.close()
                logger.debug("Closed context %r", name)
            except Exception:
                logger.exception("Error closing context %r", name)

        self._contexts.clear()
        self._page_semaphores.clear()

        # Close browser if owned
        try:
            if self._own_browser and self._browser is not None:
                await self._browser.close()
                logger.debug("Camoufox browser closed")
        except Exception:
            logger.exception("Error closing Camoufox browser")

    async def __aenter__(self) -> CamoufoxDownloader:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit - ensures cleanup."""
        await self.close()
        return False

    @property
    def is_closed(self) -> bool:
        """Check if downloader is closed."""
        return self._closed


__all__ = ["CamoufoxDownloader"]
