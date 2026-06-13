import logging
from typing import TYPE_CHECKING

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult
from qcrawl.utils.url import get_domain, join_and_normalize

if TYPE_CHECKING:
    from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class RedirectMiddleware(DownloaderMiddleware):
    """Redirect handling middleware for downloader phase.

    Notes:
      - Configuration is provided via constructor only (`max_redirects`).

    Args:
        max_redirects: maximum allowed number of redirect hops (must be int >= 1).

    Metadata and per-request controls:
        - request.meta['dont_redirect'] (bool) -> skip redirect handling for that request
        - request.meta['redirects'] (int) -> current redirect hop count (middleware will increment)
        - request.meta['redirect_urls'] (list[str]) -> previously visited redirect URLs

    Returns:
        - MiddlewareResult.retry(new_request) to reschedule the resolved redirect Request.
        - MiddlewareResult.keep(response) to accept/pass the given response unchanged.
    """

    def __init__(self, max_redirects: int = 10) -> None:
        if not isinstance(max_redirects, int):
            raise TypeError(f"max_redirects must be int, got {type(max_redirects).__name__}")
        if max_redirects < 1:
            raise ValueError(f"max_redirects must be >= 1, got {max_redirects}")
        self.max_redirects = max_redirects

    async def process_request(self, request: Request, spider: "Spider") -> MiddlewareResult:
        """Pre-download hook. RedirectMiddleware does not modify outgoing requests here.

        Always returns MiddlewareResult.continue_().
        """
        return MiddlewareResult.continue_()

    async def process_exception(
        self, request: Request, exception: BaseException, spider: "Spider"
    ) -> MiddlewareResult:
        """Exception hook. RedirectMiddleware does not handle exceptions and returns CONTINUE.

        Returning CONTINUE allows other middlewares or the engine to process the exception.
        """
        return MiddlewareResult.continue_()

    async def process_response(
        self, request: Request, response: Page, spider: "Spider"
    ) -> MiddlewareResult:
        """Handle redirect responses.

        Steps:
          1. If response.status_code is not a redirect code -> KEEP response.
          2. If no Location header -> KEEP response.
          3. Honor per-request opt-out if request.meta['dont_redirect'] is truthy -> KEEP.
          4. Resolve the redirect target via join_and_normalize(response.url, loc).
             On resolution failure, use raw Location.
          5. Clone the original Request and adjust method/body per redirect semantics:
               - 307/308: preserve method and body
               - 301/302/303: set method to GET and drop body; remove related headers
          6. Safely read previous redirect count from request.meta['redirects'] (defaults to 0).
             If the key is present it must be an int (not bool); otherwise a TypeError is raised.
          7. If redirects exceed max_redirects -> KEEP original response.
          8. Append the current response.url to redirect_urls and set meta keys on new Request.
          9. Return MiddlewareResult.retry(new_request) to schedule the redirected request.
        """

        if response.status_code not in {301, 302, 303, 307, 308}:
            return MiddlewareResult.keep(response)

        loc = response.headers.get("Location")
        if not loc:
            return MiddlewareResult.keep(response)

        # Validate meta: missing is okay; if present it must be a dict.
        meta = getattr(request, "meta", None)
        if meta is not None and not isinstance(meta, dict):
            raise TypeError(f"request.meta must be dict or None, got {type(meta).__name__}")

        # Honor per-request opt-out for redirects (if present it must be bool)
        if meta is not None and "dont_redirect" in meta:
            dd = meta["dont_redirect"]
            if not isinstance(dd, bool):
                raise TypeError(f"dont_redirect must be bool when present, got {type(dd).__name__}")
            if dd:
                return MiddlewareResult.keep(response)

        try:
            new_url = join_and_normalize(response.url, loc)
        except Exception as exc:
            logger.debug("Failed to resolve redirect URL %s: %s", loc, exc)
            new_url = loc

        new_req = request.copy(url=new_url)

        # Security: never leak credentials across origins. When the redirect
        # target host differs from the source host, strip sensitive headers that
        # would otherwise ride along to a different origin (the behavior of
        # browsers, curl, and requests).
        if get_domain(new_url) != get_domain(response.url):
            new_req.headers = {
                k: v
                for k, v in (new_req.headers or {}).items()
                if k.lower() not in {"authorization", "cookie", "proxy-authorization"}
            }

        if response.status_code in {307, 308}:
            new_req.method = request.method
            new_req.body = request.body
        else:
            new_req.method = "GET"
            new_req.body = None
            hdrs = dict(new_req.headers or {})
            for h in ("Content-Length", "Content-Type"):
                hdrs.pop(h, None)
                hdrs.pop(h.lower(), None)
            new_req.headers = hdrs

        # Read and validate previous redirect count (default 0).
        prev = 0
        if meta is not None and "redirects" in meta:
            val = meta["redirects"]
            if not isinstance(val, int) or isinstance(val, bool):
                raise TypeError(f"redirects must be int when present, got {type(val).__name__}")
            prev = val

        redirects = prev + 1

        if redirects > self.max_redirects:
            logger.info(
                "Discarding <%s, %s>: max redirects (%d) reached.",
                request.method,
                response.url,
                self.max_redirects,
            )

            spider.crawler.stats.inc_counter("redirect/max_reached")

            return MiddlewareResult.keep(response)

        # Prepare redirect_urls: if present it must be a list
        existing_redirect_urls = None
        if new_req.meta is not None:
            if not isinstance(new_req.meta, dict):
                raise TypeError(
                    f"new request.meta must be dict or None, got {type(new_req.meta).__name__}"
                )
            existing_redirect_urls = new_req.meta.get("redirect_urls", None)
            if existing_redirect_urls is not None and not isinstance(existing_redirect_urls, list):
                raise TypeError(
                    f"redirect_urls must be list when present, got {type(existing_redirect_urls).__name__}"
                )

        redirect_urls: list[str] = existing_redirect_urls.copy() if existing_redirect_urls else []
        redirect_urls.append(response.url)

        if new_req.meta is None:
            new_req.meta = {}
        new_req.meta["redirect_urls"] = redirect_urls
        new_req.meta["redirects"] = redirects

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Redirecting %s → %s (status=%s, hop=%d/%d)",
                response.url,
                new_url,
                response.status_code,
                redirects,
                self.max_redirects,
            )

        return MiddlewareResult.retry(new_req)

    async def open_spider(self, spider: "Spider") -> None:
        """Log configured redirect parameters when the spider opens."""
        logger.info("max_redirects: %d", self.max_redirects)
