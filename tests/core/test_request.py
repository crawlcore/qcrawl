"""Tests for qcrawl.core.request.Request"""

import orjson
import pytest

from qcrawl.core.request import Request


def test_init_with_defaults_and_params():
    """Request initializes with defaults and custom params."""
    # Default
    req = Request(url="https://example.com/page")
    assert req.url == "https://example.com/page"
    assert req.method == "GET"
    assert req.priority == 0
    assert req.meta == {}
    assert req.body is None

    # With params
    req = Request(
        url="https://example.com", method="POST", priority=10, meta={"key": "value"}, body=b"test"
    )
    assert req.method == "POST"
    assert req.priority == 10
    assert req.meta == {"key": "value"}
    assert req.body == b"test"


def test_url_normalization():
    """URL is normalized, errors handled gracefully."""
    # Normalization works
    req = Request(url="HTTP://EXAMPLE.COM/Path")
    assert req.url.startswith("http://example.com")

    # Normalization errors are handled
    req = Request(url="invalid://bad url")
    assert req.url  # Request still created


def test_body_validation():
    """Body accepts bytes/None, rejects other types."""
    Request(url="https://example.com", body=b"bytes ok")
    Request(url="https://example.com", body=None)

    with pytest.raises(TypeError, match="Request.body must be bytes or None"):
        Request(url="https://example.com", body="string")  # type: ignore[arg-type]


def test_to_dict():
    """to_dict() returns dict without body."""
    req = Request(
        url="https://example.com",
        priority=5,
        headers={"X-Custom": "value"},
        meta={"depth": 2},
        body=b"secret",
    )
    d = req.to_dict()

    assert d["url"] == "https://example.com/"  # Normalized with trailing slash
    assert d["priority"] == 5
    assert d["headers"] == {"X-Custom": "value"}
    assert d["meta"] == {"depth": 2}
    assert "body" not in d


def test_serialization_roundtrip():
    """to_bytes() and from_bytes() round-trip."""
    req = Request(url="https://example.com", method="POST", priority=10, body=b"data")

    data = req.to_bytes()
    req2 = Request.from_bytes(data)

    assert req2.url == req.url
    assert req2.method == req.method
    assert req2.priority == req.priority
    assert req2.body == req.body


def test_from_bytes_validation():
    """from_bytes() validates input."""
    with pytest.raises(TypeError, match="Request.from_bytes expects bytes"):
        Request.from_bytes("not bytes")  # type: ignore[arg-type]


def test_from_dict_valid():
    """from_dict() creates Request from dict."""
    req = Request.from_dict(
        {
            "url": "https://example.com",
            "method": "POST",
            "priority": 5,
            "headers": {"X-Custom": "value"},
            "meta": {"depth": 1},
            "body": b"test",
        }
    )

    assert req.url == "https://example.com/"  # Normalized
    assert req.method == "POST"
    assert req.priority == 5
    assert req.body == b"test"


def test_from_dict_validation():
    """from_dict() validates required fields and types."""
    # Missing url
    with pytest.raises(TypeError, match="'url' must be a non-empty str"):
        Request.from_dict({})

    # Invalid priority (bool not allowed)
    with pytest.raises(TypeError, match="'priority' must be an int"):
        Request.from_dict({"url": "https://example.com", "priority": True})

    # Invalid body type
    with pytest.raises(TypeError, match="'body' must be bytes"):
        Request.from_dict({"url": "https://example.com", "body": "string"})


def test_copy():
    """copy() creates shallow copy with independent dicts."""
    req = Request(url="https://example.com", meta={"key": "value"}, headers={"X-Header": "value"})

    req2 = req.copy()
    assert req2.url == req.url
    assert req2.meta == req.meta
    assert req2.headers == req.headers

    # Dicts are copied, not shared
    req2.meta["new"] = "value"
    assert "new" not in req.meta

    # Can override url
    req3 = req.copy(url="https://other.com")
    assert req3.url == "https://other.com/"  # Normalized


def test_repr():
    """__repr__ shows url, priority, and depth."""
    req = Request(url="https://example.com", priority=5, meta={"depth": 2})
    r = repr(req)

    assert "Request(" in r
    assert "https://example.com" in r
    assert "priority=5" in r
    assert "depth=2" in r


# Callback Tests


def test_callback_callable_reduced_to_name():
    """A callable callback is stored as its method name (kept serializable)."""

    class S:
        def parse_detail(self, response):
            pass

    req = Request(url="https://example.com", callback=S().parse_detail)

    assert req.callback == "parse_detail"


def test_callback_string_kept_as_is():
    """A string callback name is stored unchanged."""
    req = Request(url="https://example.com", callback="parse_detail")

    assert req.callback == "parse_detail"


def test_callback_lambda_rejected():
    """A lambda callback is rejected (it cannot be re-resolved after serialization)."""
    with pytest.raises(TypeError, match="named method"):
        Request(url="https://example.com", callback=lambda r: r)


def test_callback_and_cb_kwargs_round_trip_via_bytes():
    """callback and cb_kwargs survive to_bytes/from_bytes."""
    req = Request(url="https://example.com", callback="parse_detail", cb_kwargs={"page": 2})

    restored = Request.from_bytes(req.to_bytes())

    assert restored.callback == "parse_detail"
    assert restored.cb_kwargs == {"page": 2}


# JSON body convenience


def test_json_sets_body_and_content_type():
    """json= serializes to a bytes body and defaults Content-Type."""
    req = Request(url="https://example.com", method="POST", json={"a": 1})

    assert req.body == orjson.dumps({"a": 1})
    assert req.headers["Content-Type"] == "application/json"


def test_json_keeps_explicit_content_type():
    """An explicit Content-Type is not overridden by json=."""
    req = Request(
        url="https://example.com",
        method="POST",
        json={"a": 1},
        headers={"Content-Type": "application/vnd.api+json"},
    )

    assert req.headers["Content-Type"] == "application/vnd.api+json"


def test_json_and_body_are_mutually_exclusive():
    """Passing both json and body raises."""
    with pytest.raises(TypeError, match="either body or json"):
        Request(url="https://example.com", body=b"x", json={"a": 1})
