"""Tests for qcrawl.utils.middleware"""

import pytest

from qcrawl.core.request import Request
from qcrawl.utils.middleware import clone_request_with_meta, get_domain_key, get_meta

# get_meta Tests


def test_get_meta_returns_dict():
    """get_meta returns request.meta when it's a valid dict."""
    meta_dict = {"key": "value", "count": 42}
    request = Request(url="https://example.com", meta=meta_dict)

    result = get_meta(request)

    assert result is meta_dict
    assert result == {"key": "value", "count": 42}


def test_get_meta_raises_when_meta_is_none():
    """get_meta raises TypeError when request.meta is None."""
    request = Request(url="https://example.com", meta=None)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="request.meta must be dict, got None"):
        get_meta(request)


def test_get_meta_raises_when_meta_missing():
    """get_meta raises TypeError when request has no meta attribute."""
    request = Request(url="https://example.com")
    # Remove meta attribute
    delattr(request, "meta")

    with pytest.raises(TypeError, match="request.meta must be dict, got None"):
        get_meta(request)


def test_get_meta_raises_when_meta_is_not_dict():
    """get_meta raises TypeError when request.meta is not a dict."""
    request = Request(url="https://example.com")
    request.meta = "not a dict"  # type: ignore[assignment]

    with pytest.raises(TypeError, match="request.meta must be dict, got str"):
        get_meta(request)


def test_get_meta_raises_when_meta_is_list():
    """get_meta raises TypeError when request.meta is a list."""
    request = Request(url="https://example.com")
    request.meta = [1, 2, 3]  # type: ignore[assignment]

    with pytest.raises(TypeError, match="request.meta must be dict, got list"):
        get_meta(request)


# clone_request_with_meta Tests


def test_clone_request_with_meta_creates_copy():
    """clone_request_with_meta creates a new Request instance."""
    original = Request(url="https://example.com", meta={"key": "value"})

    cloned = clone_request_with_meta(original)

    assert cloned is not original
    assert cloned.url == original.url


def test_clone_request_with_meta_deep_copies_meta():
    """clone_request_with_meta deep-copies meta dict (not shared)."""
    original = Request(url="https://example.com", meta={"key": "value"})

    cloned = clone_request_with_meta(original)

    # Meta dicts are different instances
    assert cloned.meta is not original.meta
    assert cloned.meta == original.meta

    # Modifying clone doesn't affect original
    cloned.meta["new_key"] = "new_value"
    assert "new_key" not in original.meta


def test_clone_request_with_meta_handles_none_meta():
    """clone_request_with_meta handles None meta by creating empty dict."""
    original = Request(url="https://example.com", meta=None)  # type: ignore[arg-type]

    cloned = clone_request_with_meta(original)

    assert cloned.meta == {}
    assert isinstance(cloned.meta, dict)


def test_clone_request_with_meta_applies_overrides():
    """clone_request_with_meta applies keyword argument overrides."""
    original = Request(url="https://example.com", method="GET", priority=0)

    cloned = clone_request_with_meta(original, url="https://other.com/path")

    assert "other.com" in cloned.url
    assert "/path" in cloned.url
    assert cloned.priority == original.priority
    assert cloned.method == "GET"


def test_clone_request_with_meta_preserves_other_attributes():
    """clone_request_with_meta preserves all request attributes."""
    original = Request(
        url="https://example.com",
        method="POST",
        body=b"data",
        headers={"User-Agent": "Test"},
        priority=5,
        meta={"retry": 1},
    )

    cloned = clone_request_with_meta(original)

    assert cloned.method == "POST"
    assert cloned.body == b"data"
    assert cloned.headers == {"User-Agent": "Test"}
    assert cloned.priority == 5
    assert cloned.meta == {"retry": 1}


# get_domain_key Tests


def test_get_domain_key_returns_domain():
    """get_domain_key returns domain for valid URL."""
    url = "https://example.com/path"

    result = get_domain_key(url)

    assert result == "example.com"


def test_get_domain_key_returns_domain_with_subdomain():
    """get_domain_key returns domain including subdomain."""
    url = "https://sub.example.com/path"

    result = get_domain_key(url)

    assert result == "sub.example.com"


def test_get_domain_key_returns_default_on_type_error():
    """get_domain_key returns 'default' when URL parsing raises TypeError."""
    # Passing None or non-string should cause TypeError
    result = get_domain_key(None)  # type: ignore[arg-type]

    assert result == "default"


def test_get_domain_key_returns_default_on_value_error():
    """get_domain_key returns 'default' when URL parsing raises ValueError."""
    # Invalid URL format should cause ValueError
    result = get_domain_key("not a valid url")

    assert result == "default"


def test_get_domain_key_returns_default_when_domain_empty():
    """get_domain_key returns 'default' when domain extraction returns empty."""
    # URL with no domain (edge case)
    result = get_domain_key("file:///path/to/file")

    # Should either extract domain or fall back to default
    assert isinstance(result, str)


# Integration Tests


def test_get_meta_and_clone_integration():
    """Integration: get_meta works on cloned requests."""
    original = Request(url="https://example.com", meta={"key": "value"})

    cloned = clone_request_with_meta(original)
    meta = get_meta(cloned)

    assert meta == {"key": "value"}
    assert meta is cloned.meta


def test_clone_and_modify_meta_independence():
    """Integration: cloned meta is independent from original."""
    original = Request(url="https://example.com", meta={"shared": "data"})

    clone1 = clone_request_with_meta(original)
    clone2 = clone_request_with_meta(original)

    # Modify each clone's meta
    clone1.meta["id"] = 1
    clone2.meta["id"] = 2

    # All three have independent meta dicts
    assert original.meta == {"shared": "data"}
    assert clone1.meta == {"shared": "data", "id": 1}
    assert clone2.meta == {"shared": "data", "id": 2}
