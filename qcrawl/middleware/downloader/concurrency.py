import asyncio
import logging
from typing import TYPE_CHECKING

from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult
from qcrawl.utils.url import get_domain

if TYPE_CHECKING:
    from qcrawl.core.request import Request
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class _DomainSlot:
    """Per-domain concurrency control using a semaphore."""

    __slots__ = ("domain", "semaphore")

    def __init__(self, domain: str, concurrency: int) -> None:
        self.domain = domain
        self.semaphore = asyncio.Semaphore(concurrency)

    async def acquire(self) -> None:
        """Acquire a slot for this domain."""
        await self.semaphore.acquire()

    def release(self) -> None:
        """Release a slot for this domain."""
        try:
            self.semaphore.release()
        except ValueError:
            # Semaphore release called too many times
            logger.warning("Semaphore release underflow for domain %s", self.domain)


class ConcurrencyMiddleware(DownloaderMiddleware):
    """Per-domain concurrency limiter using semaphores.

    Ensures at most N concurrent requests per domain, where N is
    configurable via concurrency_per_domain (default: 2).
    """

    def __init__(self, concurrency_per_domain: int = 2) -> None:
        if not isinstance(concurrency_per_domain, int) or concurrency_per_domain < 1:
            raise ValueError("concurrency_per_domain must be an integer >= 1")

        self._concurrency = concurrency_per_domain
        self._slots: dict[str, _DomainSlot] = {}

    @classmethod
    def from_crawler(cls, crawler):
        """Create middleware instance from crawler, reading settings.

        Args:
            crawler: Crawler instance with runtime_settings

        Returns:
            ConcurrencyMiddleware instance configured from settings
        """
        from qcrawl.settings import Settings

        settings: Settings = getattr(crawler, "runtime_settings", None) or Settings()
        return cls(concurrency_per_domain=settings.CONCURRENCY_PER_DOMAIN)

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL, fallback to 'default' on error."""
        try:
            domain = get_domain(url)
            return domain or "default"
        except Exception:
            return "default"

    def _get_slot(self, url: str) -> _DomainSlot:
        """Get or create a domain slot for the given URL."""
        domain = self._get_domain(url)
        if domain not in self._slots:
            self._slots[domain] = _DomainSlot(domain, self._concurrency)
        return self._slots[domain]

    async def process_request(self, request: "Request", spider: "Spider") -> MiddlewareResult:
        """Acquire a concurrency slot before processing the request."""
        slot = self._get_slot(request.url)
        await slot.acquire()

        # Store slot reference in request metadata for later release
        if not hasattr(request, "meta") or request.meta is None:
            request.meta = {}
        request.meta["_concurrency_slot"] = slot

        return MiddlewareResult.continue_()

    async def process_response(
        self, request: "Request", response, spider: "Spider"
    ) -> MiddlewareResult:
        """Release the concurrency slot after processing the response."""
        self._release_slot(request)
        return MiddlewareResult.keep(response)

    async def process_exception(
        self, request: "Request", exception: BaseException, spider: "Spider"
    ) -> MiddlewareResult:
        """Release the concurrency slot when an exception occurs."""
        self._release_slot(request)
        return MiddlewareResult.continue_()

    def _release_slot(self, request: "Request") -> None:
        """Safely release the concurrency slot stored in request metadata."""
        if not hasattr(request, "meta") or not isinstance(request.meta, dict):
            return

        slot = request.meta.pop("_concurrency_slot", None)
        if isinstance(slot, _DomainSlot):
            slot.release()

    async def open_spider(self, spider: "Spider") -> None:
        """Reset slots when spider opens."""
        self._slots.clear()
        logger.info("concurrency_per_domain=%d", self._concurrency)
