import asyncio
import logging
from contextlib import suppress

import aiohttp

from qcrawl import signals
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.signals import SignalRegistry

logger = logging.getLogger(__name__)


class HTTPDownloader:
    """Async HTTP downloader using aiohttp.

    Responsibilities:
      - Own an aiohttp.ClientSession and provide `fetch()` for requests.
      - Emit response/bytes signals via a sender-bound dispatcher.
      - Manage a lightweight health task for diagnostics.

    Notes:
      - Accepts optional `settings` mapping to tune connector/session behavior.
      - If an external `session` is provided, the downloader will not close it.
      - Supports optional rotation of owned sessions after `force_close_after` requests
        to mitigate long-lived connection/resource leaks.
    """

    __slots__ = (
        "_session",
        "_closed",
        "_health_task",
        "signals",
        "_own_session",
        "_request_count",
        "_force_close_after",
        "_rotate_lock",
    )

    def __init__(self, session: aiohttp.ClientSession, *, own_session: bool = True) -> None:
        self._session = session
        self._own_session = bool(own_session)
        self.signals = signals.signals_registry.for_sender(self)
        self._closed = False
        self._health_task: asyncio.Task[None] | None = None

        # Rotation & bookkeeping
        self._request_count: int = 0
        self._force_close_after: int | None = None
        self._rotate_lock: asyncio.Lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        *,
        signal_dispatcher: SignalRegistry | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        connector: aiohttp.BaseConnector | None = None,
        session: aiohttp.ClientSession | None = None,
        settings: dict[str, int | bool | float] | None = None,
    ) -> "HTTPDownloader":
        """Create downloader with an active aiohttp session.

        Accepts `settings` mapping with optional keys:
          - max_connections -> connector.limit
          - max_connections_per_host -> connector.limit_per_host
          - dns_cache_ttl -> connector.ttl_dns_cache
          - enable_cleanup_closed -> connector.enable_cleanup_closed
          - keepalive_timeout -> connector.keepalive_timeout
          - force_close_after -> integer, rotate session after this many requests

        If `session` is provided it will be used and not closed by the downloader.
        """
        if timeout is None:
            timeout = aiohttp.ClientTimeout(total=180.0)

        cfg = settings or {}

        # Only create connector when not provided
        if connector is None:
            # Extract values with defaults
            limit = 100
            limit_per_host = 10
            ttl_dns_cache = 300
            enable_cleanup_closed = True
            keepalive_timeout = 60.0

            if "max_connections" in cfg:
                val = cfg["max_connections"]
                if isinstance(val, int):
                    limit = val

            if "max_connections_per_host" in cfg:
                val = cfg["max_connections_per_host"]
                if isinstance(val, int):
                    limit_per_host = val

            if "dns_cache_ttl" in cfg:
                val = cfg["dns_cache_ttl"]
                if isinstance(val, int):
                    ttl_dns_cache = val

            if "enable_cleanup_closed" in cfg:
                val = cfg["enable_cleanup_closed"]
                if isinstance(val, bool):
                    enable_cleanup_closed = val

            if "keepalive_timeout" in cfg:
                val = cfg["keepalive_timeout"]
                if isinstance(val, (int, float)):
                    keepalive_timeout = float(val)

            connector = aiohttp.TCPConnector(
                limit=limit,
                limit_per_host=limit_per_host,
                ttl_dns_cache=ttl_dns_cache,
                enable_cleanup_closed=enable_cleanup_closed,
                keepalive_timeout=keepalive_timeout,
            )

        own = False
        if session is None:
            own = True
            session = aiohttp.ClientSession(timeout=timeout, connector=connector)

        downloader = cls(session, own_session=own)

        # Apply rotation config if provided and downloader owns the session
        if own:
            fc = cfg.get("force_close_after")
            if fc is not None and isinstance(fc, int):
                downloader._force_close_after = fc

        if signal_dispatcher is not None:
            if not isinstance(signal_dispatcher, SignalRegistry):
                raise TypeError(
                    "signal_dispatcher must be a SignalRegistry or None; "
                    "pass signals.signals_registry or None"
                )
            downloader.signals = signal_dispatcher.for_sender(downloader)

        # Only create health task for sessions owned by this downloader
        if downloader._own_session:
            downloader._health_task = asyncio.create_task(
                downloader._health_check_loop(), name="downloader-health"
            )

        return downloader

    async def _health_check_loop(self):
        """Monitor connection pool health every 30s."""
        while not self._closed:
            await asyncio.sleep(30)
            try:
                connector = self._session.connector
                if connector is not None and hasattr(connector, "_conns"):
                    total_conns = sum(len(v) for v in connector._conns.values())
                    limit = getattr(connector, "limit", 0)
                    if limit and total_conns > limit * 0.9:
                        logger.warning(
                            "Connection pool near capacity: %d/%d (%.1f%%)",
                            total_conns,
                            limit,
                            (total_conns / limit) * 100,
                        )
            except Exception:
                logger.exception("Error in health check")

    async def _rotate_session(self) -> None:
        """Create a fresh session/connector and close the old one (owned sessions only)."""
        if not self._own_session:
            return

        async with self._rotate_lock:
            # Another coroutine may have rotated already
            if self._request_count == 0:
                return

            old_session = self._session
            # Build a new connector mirroring old connector settings where possible.
            try:
                old_conn = getattr(old_session, "connector", None)
                new_connector = None
                if old_conn is not None:
                    # Try to copy known attributes; fallback to default connector
                    try:
                        new_connector = aiohttp.TCPConnector(
                            limit=getattr(old_conn, "limit", 0),
                            limit_per_host=getattr(old_conn, "limit_per_host", 0),
                            ttl_dns_cache=getattr(old_conn, "ttl_dns_cache", 0),
                            enable_cleanup_closed=getattr(old_conn, "enable_cleanup_closed", True),
                            keepalive_timeout=getattr(old_conn, "keepalive_timeout", None),
                        )
                    except Exception:
                        new_connector = aiohttp.TCPConnector()
                else:
                    new_connector = aiohttp.TCPConnector()

                # Create new session and swap
                new_session = aiohttp.ClientSession(
                    timeout=old_session.timeout, connector=new_connector
                )
                self._session = new_session

                # reset request counter after swap
                self._request_count = 0

                # refresh health task to monitor new connector
                if self._health_task:
                    self._health_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await self._health_task
                    self._health_task = asyncio.create_task(
                        self._health_check_loop(), name="downloader-health"
                    )

                # Close old session asynchronously (best-effort)
                async def _close_old(s):
                    try:
                        await s.close()
                    except Exception:
                        logger.exception("Error closing rotated downloader session")

                asyncio.create_task(_close_old(old_session))
            except Exception:
                logger.exception("Failed to rotate downloader session")

    async def fetch(
        self,
        request: Request | str,
        *,
        spider: object | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 180.0,
    ) -> Page:
        """Fetch request using aiohttp session and emit response/bytes signals.

        Respects:
          - downloader-owned session rotation after `_force_close_after` requests
          - header merging: runtime defaults, explicit headers arg, per-request headers
        """
        if self._closed:
            raise RuntimeError("Cannot fetch: downloader is closed")

        if isinstance(request, str):
            request = Request(url=request)

        # Build final headers by merging sources with defensive coercion
        final_headers: dict[str, str] = {}

        try:
            rs = getattr(spider, "runtime_settings", None)
            if rs is not None:
                base = getattr(rs, "DEFAULT_REQUEST_HEADERS", {}) or {}
                if isinstance(base, dict):
                    for k, v in base.items():
                        final_headers[str(k)] = str(v)
                ua = getattr(rs, "user_agent", None)
                if ua and "User-Agent" not in final_headers:
                    final_headers["User-Agent"] = str(ua)
        except Exception:
            # defensive: ignore header assembly errors and proceed
            pass

        try:
            if headers:
                for k, v in headers.items():
                    final_headers[str(k)] = str(v)
        except Exception:
            pass

        try:
            req_hdrs = getattr(request, "headers", None) or {}
            for k, v in req_hdrs.items():
                final_headers[str(k)] = str(v)
        except Exception:
            pass

        try:
            async with asyncio.timeout(timeout):
                async with self._session.request(
                    request.method, request.url, headers=final_headers, data=request.body
                ) as resp:
                    # Headers are available before the body is read — emit
                    # headers_received here so handlers can act pre-body.
                    try:
                        await self.signals.send_async(
                            "headers_received",
                            headers=dict(resp.headers),
                            request=request,
                        )
                    except Exception:
                        logger.exception("Error dispatching headers_received for %s", request.url)

                    page = await Page.from_response(resp, request=request)
                    try:
                        await self.signals.send_async(
                            "response_received",
                            response=page,
                            request=request,
                        )
                        await self.signals.send_async(
                            "bytes_received",
                            data=page.content,
                            request=request,
                        )
                    except Exception:
                        logger.exception("Error dispatching signal for %s", request.url)

                    # Bookkeeping: increment request count and rotate session if needed
                    if self._own_session and self._force_close_after:
                        try:
                            self._request_count += 1
                            if self._request_count >= self._force_close_after:
                                # rotate in background to avoid blocking the current response path
                                asyncio.create_task(self._rotate_session())
                        except Exception:
                            logger.exception("Error updating downloader request counter")

                    return page

        except TimeoutError:
            logger.error("Timeout after %.1fs for %s", timeout, getattr(request, "url", None))
            raise

        except aiohttp.ClientError as err:
            logger.error("HTTP error for %s: %s", getattr(request, "url", None), err)
            raise

        except Exception:
            logger.exception("Unexpected error fetching %s", getattr(request, "url", None))
            raise

    async def close(self) -> None:
        """Close aiohttp session and cancel health task.

        Only closes the underlying session if this Downloader created it.
        """
        if self._closed:
            return

        self._closed = True

        if self._health_task:
            self._health_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._health_task
            self._health_task = None

        try:
            if self._own_session and self._session is not None and not self._session.closed:
                await self._session.close()
                logger.debug("Downloader session closed")
        except Exception:
            logger.exception("Error closing downloader session")

    async def __aenter__(self) -> "HTTPDownloader":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    @property
    def is_closed(self) -> bool:
        return self._closed


__all__ = ["HTTPDownloader"]
