from __future__ import annotations

from typing import TYPE_CHECKING

import aiohttp
import orjson
from charset_normalizer import from_bytes

from qcrawl.utils.url import join_and_normalize

if TYPE_CHECKING:
    from qcrawl.core.request import Request


class Page:
    """HTTP response wrapper with synchronous accessors for cached data.

    The `Page` class encapsulates an HTTP response with access to response data
    through cached properties and methods. It provides both direct attribute access
    (url, content, status_code, headers) and lazy-loaded decoded content (text, JSON).
    """

    __slots__ = (
        "url",
        "content",
        "status_code",
        "headers",
        "request",
        "_detected_encoding",
    )

    def __init__(
        self,
        url: str,
        content: bytes,
        status_code: int,
        headers: dict[str, str],
        request: object | None = None,
        encoding: str | None = None,
    ) -> None:
        self.url = url
        self.content = content
        self._detected_encoding = encoding
        self.status_code = status_code
        self.headers = headers
        self.request = request

    def _detect_encoding(self) -> str:
        if self._detected_encoding is not None:
            return self._detected_encoding

        result = from_bytes(self.content, steps=16)
        best = result.best()
        if best is not None and getattr(best, "encoding", None):
            detected = best.encoding
        else:
            detected = "utf-8"

        self._detected_encoding = detected
        return detected

    @classmethod
    async def from_response(
        cls, resp: aiohttp.ClientResponse, request: Request | None = None
    ) -> Page:
        """Create Page from aiohttp response (async factory method).

        This classmethod performs the async reading of the response body and
        pre-decodes the text using the response's charset.

        Notes:
            - Must be called inside `async with session.request(...)` block
            - Reads entire response body into memory
            - Pre-decodes text using response charset or UTF-8 fallback
            - Catches decoding errors and returns empty string
        """
        return cls(
            url=str(resp.url),
            content=await resp.read(),
            status_code=resp.status,
            headers=dict(resp.headers),
            request=request,
            encoding=resp.charset,
        )

    def text(self, encoding: str | None = None) -> str:
        """Decode response content."""
        if encoding is None:
            encoding = self._detect_encoding()
        return self.content.decode(encoding, errors="replace")

    def json(self) -> object:
        """Parse and return JSON from raw bytes."""
        try:
            return orjson.loads(self.content)
        except orjson.JSONDecodeError as exc:
            raise ValueError(f"Failed to parse JSON for {self.url!r}: {exc}") from exc

    def follow(self, href: str) -> str:
        """Resolve relative URL against page URL.

        Joins a relative or absolute URL with the current page URL,
        handling relative paths, absolute paths, and full URLs correctly.
        The result is normalized (fragment removed, trailing slash removed).
        """
        return join_and_normalize(self.url, href)

    def __repr__(self) -> str:
        return f"Page(url={self.url!r}, status={self.status_code}, size={len(self.content)} bytes)"
