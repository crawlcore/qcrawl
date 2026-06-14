import ipaddress
import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from qcrawl.core.request import Request
from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult

if TYPE_CHECKING:
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class HttpProxyMiddleware(DownloaderMiddleware):
    """Handle HTTP/HTTPS proxy configuration with IPv6 support.

    Features:
    - Determine and set per-request proxy in `request.meta['proxy']`.
    - Respect explicit per-request `proxy` override.
    - Use middleware constructor defaults or spider attributes for configuration.
      Supported spider attributes: `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`.
    - Honor `NO_PROXY` entries: CIDR ranges, exact IPs, domain suffixes and wildcards.
    - Normalize IPv4/IPv6 addresses (brackets, zone IDs) for matching.
    - Provide `get_proxy_for_url()` helper for external lookup.
    - Safe logging of proxy host/port (hide credentials).
    """

    def __init__(
        self,
        http_proxy: str | None = None,
        https_proxy: str | None = None,
        no_proxy: list[str] | None = None,
    ) -> None:
        self.http_proxy = http_proxy
        self.https_proxy = https_proxy
        self.no_proxy = no_proxy or []

    def _get_setting(
        self, spider: "Spider", name: str, default: str | list[str] | None
    ) -> str | list[str] | None:
        """Get spider attribute with fallback to middleware default.

        Does not consult environment variables.
        """
        value = getattr(spider, name, None)
        if value is None:
            return default
        if name == "NO_PROXY" and isinstance(value, str):
            return [d.strip() for d in value.split(",") if d.strip()]
        if isinstance(value, (str, list)):
            return value
        return default

    def _normalize_ip(self, addr: str) -> str | None:
        """Normalize IP address, handling IPv6 brackets and zone IDs."""
        addr = addr.strip().strip("[]").split("%")[0]
        try:
            return str(ipaddress.ip_address(addr))
        except ValueError:
            return None

    def _matches_cidr(self, ip: str, cidr: str) -> bool:
        """Check if IP is in CIDR range."""
        try:
            return ipaddress.ip_address(ip) in ipaddress.ip_network(cidr, strict=False)
        except ValueError:
            return False

    def _should_bypass_proxy(self, url: str, no_proxy_list: list[str]) -> bool:
        """Check if URL should bypass proxy."""
        if not no_proxy_list:
            return False

        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return False

        normalized_ip = self._normalize_ip(hostname)

        for entry in no_proxy_list:
            entry = entry.strip()
            if not entry:
                continue

            # CIDR match (IPv4/IPv6)
            if "/" in entry:
                if normalized_ip and self._matches_cidr(normalized_ip, entry):
                    logger.debug("Bypassing proxy for %s (CIDR %s)", url, entry)
                    return True
                continue

            # IP exact match
            entry_ip = self._normalize_ip(entry)
            if normalized_ip and entry_ip and normalized_ip == entry_ip:
                logger.debug("Bypassing proxy for %s (IP match)", url)
                return True

            # Domain matching (skip if either is an IP)
            if normalized_ip or entry_ip:
                continue

            hostname_lower = hostname.lower()
            entry_lower = entry.lower()

            # Exact domain, suffix (.example.com), or wildcard (*.example.com)
            if (
                hostname_lower == entry_lower
                or hostname_lower.endswith(
                    entry_lower if entry_lower.startswith(".") else f".{entry_lower}"
                )
                or (entry_lower.startswith("*.") and hostname_lower.endswith(entry_lower[1:]))
            ):
                logger.debug("Bypassing proxy for %s (domain match %s)", url, entry)
                return True

        return False

    async def process_request(self, request: Request, spider: "Spider") -> MiddlewareResult:
        """Set `request.meta['proxy']` to the selected proxy URL or `None` to bypass.
        Returns MiddlewareResult.continue_() to let engine proceed.
        """
        # Respect explicit per-request override
        if "proxy" in request.meta:
            return MiddlewareResult.continue_()

        scheme = urlparse(request.url).scheme
        if scheme not in ("http", "https"):
            return MiddlewareResult.continue_()

        # Force list[str] for NO_PROXY
        no_proxy_raw = self._get_setting(spider, "NO_PROXY", self.no_proxy)
        no_proxy_list: list[str] = (
            [d.strip() for d in no_proxy_raw.split(",") if d.strip()]
            if isinstance(no_proxy_raw, str)
            else no_proxy_raw or []
        )

        if self._should_bypass_proxy(request.url, no_proxy_list):
            request.meta["proxy"] = None
            return MiddlewareResult.continue_()

        proxy_key = "HTTP_PROXY" if scheme == "http" else "HTTPS_PROXY"
        proxy_default = self.http_proxy if scheme == "http" else self.https_proxy
        proxy_raw = self._get_setting(spider, proxy_key, proxy_default)

        # Force str for proxy URL
        proxy_url: str | None = proxy_raw if isinstance(proxy_raw, str) else None
        if not proxy_url:
            return MiddlewareResult.continue_()

        request.meta["proxy"] = proxy_url

        # Stats: proxy used
        spider.crawler.stats.inc("proxy/requests")
        spider.crawler.stats.inc(f"proxy/requests/{scheme}")

        # Log safe proxy URL (hide credentials)
        try:
            parsed = urlparse(proxy_url)
            safe_host = parsed.hostname or "unknown"
            safe_port = parsed.port or (80 if parsed.scheme == "http" else 443)
            safe_url = f"{parsed.scheme}://{safe_host}:{safe_port}"
        except Exception:
            safe_url = proxy_url

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Using proxy %s for %s", safe_url, request.url)

        return MiddlewareResult.continue_()

    async def process_response(
        self, request: Request, response, spider: "Spider"
    ) -> MiddlewareResult:
        if "proxy" in request.meta:
            if 200 <= response.status_code < 400:
                spider.crawler.stats.inc("proxy/success")
            else:
                spider.crawler.stats.inc("proxy/failure")

        # No response-time proxy handling; continue processing
        return MiddlewareResult.continue_()

    async def process_exception(
        self, request: Request, exception: BaseException, spider: "Spider"
    ) -> MiddlewareResult:
        if "proxy" in request.meta:
            spider.crawler.stats.inc("proxy/errors")
            try:
                proxy = request.meta["proxy"]
                if not isinstance(proxy, str):
                    raise TypeError(
                        f"Proxy URL must be a string, got {type(proxy).__name__}. "
                        f"Please provide proxy URL in string format (e.g., 'http://proxy.example.com:8080')"
                    )
                parsed = urlparse(proxy)
                key = f"{parsed.hostname}:{parsed.port or 8080}"
                spider.crawler.stats.inc(f"proxy/errors/{key}")
            except Exception:
                pass

        # Not handling exceptions here; let other middlewares decide
        return MiddlewareResult.continue_()

    def get_proxy_for_url(self, url: str, spider: "Spider") -> str | None:
        """Get proxy URL for given URL (helper used outside middleware chain)."""
        no_proxy_raw = self._get_setting(spider, "NO_PROXY", self.no_proxy)
        no_proxy_list: list[str] = (
            [d.strip() for d in no_proxy_raw.split(",") if d.strip()]
            if isinstance(no_proxy_raw, str)
            else no_proxy_raw or []
        )

        if self._should_bypass_proxy(url, no_proxy_list):
            return None

        scheme = urlparse(url).scheme
        if scheme not in ("http", "https"):
            return None

        proxy_key = "HTTP_PROXY" if scheme == "http" else "HTTPS_PROXY"
        proxy_default = self.http_proxy if scheme == "http" else self.https_proxy
        proxy_raw = self._get_setting(spider, proxy_key, proxy_default)

        return proxy_raw if isinstance(proxy_raw, str) else None

    async def open_spider(self, spider: "Spider") -> None:
        """Log configured proxy parameters when the spider opens."""
        logger.info(
            "http_proxy: %s, https_proxy: %s, no_proxy: %s",
            self.http_proxy or "None",
            self.https_proxy or "None",
            self.no_proxy or "None",
        )
