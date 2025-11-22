import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcrawl.core.request import Request

try:
    import msgspec
except ImportError as exc:
    raise ImportError(
        "msgspec is required for Redis support. Install with: pip install 'qcrawl[redis]'"
    ) from exc


# mypy: disable-error-code=unused-ignore
class RequestStruct(msgspec.Struct, kw_only=True):  # type: ignore[call-arg]
    """MessagePack-serializable struct mirroring core.Request for queue persistence."""

    url: str
    method: str = "GET"
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None
    body: bytes | None = None
    priority: int = 0
    retries: int = 0
    timeout_ms: int = 10000
    proxy: str | None = None
    meta: dict[str, object] | None = None
    ts: int = 0


def encode_request(request: "Request") -> bytes:
    """Encode a Request object to MessagePack bytes for queue persistence.

    Args:
        request: The Request object to encode

    Returns:
        MessagePack-encoded bytes
    """

    struct = RequestStruct(
        url=request.url,
        method=request.method,
        headers=request.headers,
        cookies=getattr(request, "cookies", None),
        body=request.body,
        priority=getattr(request, "priority", 0),
        retries=getattr(request, "retries", 0),
        timeout_ms=getattr(request, "timeout_ms", 10000),
        proxy=getattr(request, "proxy", None),
        meta=request.meta,
        ts=getattr(request, "ts", 0) or int(time.time() * 1000),
    )
    return bytes(msgspec.msgpack.encode(struct))


def decode_request(data: bytes) -> "Request":
    """Decode MessagePack bytes back into a Request object.

    Args:
        data: MessagePack-encoded bytes

    Returns:
        Reconstructed Request object

    Raises:
        TypeError: If data is not bytes
        msgspec.DecodeError: If data is invalid or doesn't match schema
    """
    from qcrawl.core.request import Request

    if not isinstance(data, bytes):
        raise TypeError("decode_request expects bytes")

    struct: RequestStruct = msgspec.msgpack.decode(data, type=RequestStruct)

    body = struct.body
    if isinstance(body, bytearray):
        body = bytes(body)

    return Request(
        url=struct.url,
        method=struct.method,
        headers=struct.headers or {},
        body=body,
        priority=struct.priority,
        meta=struct.meta or {},
    )
