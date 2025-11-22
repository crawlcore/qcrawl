import logging
from dataclasses import dataclass, field

from qcrawl.core._msgspec import encode_request
from qcrawl.utils.url import normalize_url

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Request:
    """HTTP request with crawling metadata.

    Attributes
    - url: canonicalized request URL (normalized in `__post_init__`).
    - meta: arbitrary metadata used by scheduler/spiders (copied by callers).
    - headers: mapping of header name -> value (strings).
    - priority: scheduler priority (lower numbers are processed first).
    - method: HTTP method (e.g. "GET", "POST").
    - body: binary payload for the request (strictly `bytes` or `None`).

    Notes
    - Prefer passing immutable `bytes` for `body`. Mutable buffers (`bytearray`)
      are rejected here to ensure stable deduplication and hashing.
    - `to_dict()` omits `body` to keep debug output small and human-readable.
    - `to_bytes()` includes `body` so the serialized form can be round-tripped.
    """

    url: str
    meta: dict[str, object] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    priority: int = 0
    method: str = "GET"
    body: bytes | None = None

    def __post_init__(self) -> None:
        # Normalize URL (defensive)
        try:
            self.url = normalize_url(self.url)
        except Exception as exc:
            logger.debug("normalize_url failed for %s: %s", self.url, exc, exc_info=True)
            self.meta.setdefault("url_normalize_error", str(exc))

        if self.body is None:
            return
        if not isinstance(self.body, bytes):
            raise TypeError("Request.body must be bytes or None")

    def to_dict(self) -> dict[str, object]:
        """Return a minimal dict snapshot intended for inspection and debugging.

        The returned dict is deterministic and excludes `body` for readability.
        Use `to_bytes()` to obtain a full serialized representation that includes
        the binary `body`.
        """
        return {
            "url": self.url,
            "priority": self.priority,
            "headers": dict(self.headers) if self.headers else {},
            "meta": dict(self.meta) if self.meta else {},
            "method": self.method,
            # Intentionally exclude `body` for readability in debug dumps
        }

    def to_bytes(self) -> bytes:
        """Serialize Request to MessagePack bytes via the shared encoder."""
        try:
            return encode_request(self)
        except ImportError as exc:
            raise ImportError(
                "msgspec is required to serialize requests with msgpack. "
                "Install with: pip install 'qcrawl[redis]'"
            ) from exc

    @classmethod
    def from_bytes(cls, data: bytes) -> "Request":
        """Deserialize MessagePack bytes produced by `to_bytes()` back into Request."""
        if not isinstance(data, bytes):
            raise TypeError("Request.from_bytes expects bytes")

        try:
            from qcrawl.core._msgspec import decode_request
        except Exception as exc:
            raise ImportError(
                "msgspec is required to deserialize requests with msgpack. "
                "Install with: pip install 'qcrawl[redis]'"
            ) from exc

        req = decode_request(data)

        if not isinstance(req, cls):
            raise TypeError(
                f"Decoded request type mismatch: expected {cls.__name__}, got {type(req).__name__}. "
                "Ensure the same class is used for to_bytes()/from_bytes() and the centralized decoder."
            )

        return req

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "Request":
        """Create a Request from a plain dictionary.

        Validation & normalization performed:
        - `url` must be a non-empty `str`.
        - `priority` must be an `int` (bool is rejected).
        - `headers` must be a dict; values are coerced to `str`.
        - `meta` must be a dict; shallow-copied.
        - `method` coerced to `str`.
        - `body` if present must be `bytes`.

        Raises TypeError on malformed input.
        """
        if not isinstance(data, dict):
            raise TypeError("Request.from_dict expects a dict")

        url = data.get("url")
        if not isinstance(url, str) or not url:
            raise TypeError("Request.from_dict: 'url' must be a non-empty str")

        # Priority validation: must be int (not bool)
        priority_raw = data.get("priority", 0)
        if isinstance(priority_raw, int) and not isinstance(priority_raw, bool):
            priority = int(priority_raw)
        else:
            raise TypeError("Request.from_dict: 'priority' must be an int")

        headers_raw = data.get("headers", {}) or {}
        if not isinstance(headers_raw, dict):
            raise TypeError("Request.from_dict: 'headers' must be a dict")
        headers = {str(k): str(v) for k, v in headers_raw.items()}

        meta_raw = data.get("meta", {}) or {}
        if not isinstance(meta_raw, dict):
            raise TypeError("Request.from_dict: 'meta' must be a dict")
        meta = dict(meta_raw)

        method = data.get("method", "GET")
        if not isinstance(method, str):
            method = str(method)

        body_field = data.get("body")
        body_bytes: bytes | None = None
        if body_field is not None:
            if isinstance(body_field, bytes):
                body_bytes = body_field
            else:
                raise TypeError("Request.from_dict: 'body' must be bytes when present")

        return cls(
            url=url, meta=meta, headers=headers, priority=priority, method=method, body=body_bytes
        )

    def copy(self, *, url: str | None = None) -> "Request":
        """Return a shallow copy suitable for retries/redirects.

        - Copies meta and headers dicts so callers can mutate safely.
        - Allows overriding url via keyword.
        """
        return Request(
            url=url or self.url,
            meta=dict(self.meta) if self.meta is not None else {},
            headers=dict(self.headers) if self.headers is not None else {},
            priority=int(self.priority),
            method=str(self.method),
            body=self.body,
        )

    def __repr__(self) -> str:
        return f"Request(url={self.url!r}, priority={self.priority}, depth={self.meta.get('depth', 0)})"
