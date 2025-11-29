"""Tests for qcrawl.utils.fingerprint"""

import pytest

from qcrawl.core.request import Request
from qcrawl.utils.fingerprint import RequestFingerprinter

# Initialization Tests


def test_fingerprinter_init_default():
    """RequestFingerprinter initializes with empty param sets."""
    fp = RequestFingerprinter()

    assert fp.ignore_query_params == set()
    assert fp.keep_query_params == set()


def test_fingerprinter_init_with_ignore_params():
    """RequestFingerprinter initializes with ignore_query_params."""
    fp = RequestFingerprinter(ignore_query_params={"session", "timestamp"})

    assert fp.ignore_query_params == {"session", "timestamp"}
    assert fp.keep_query_params == set()


def test_fingerprinter_init_with_keep_params():
    """RequestFingerprinter initializes with keep_query_params."""
    fp = RequestFingerprinter(keep_query_params={"id", "page"})

    assert fp.keep_query_params == {"id", "page"}
    assert fp.ignore_query_params == set()


def test_fingerprinter_init_rejects_both_params():
    """RequestFingerprinter raises ValueError when both ignore and keep are provided."""
    with pytest.raises(
        ValueError, match="Cannot use both ignore_query_params and keep_query_params"
    ):
        RequestFingerprinter(ignore_query_params={"session"}, keep_query_params={"id"})


# fingerprint_bytes Tests


def test_fingerprint_bytes_generates_bytes():
    """fingerprint_bytes returns bytes."""
    fp = RequestFingerprinter()
    request = Request(url="https://example.com")

    result = fp.fingerprint_bytes(request)

    assert isinstance(result, bytes)
    assert len(result) == 16  # Default digest_size


def test_fingerprint_bytes_default_digest_size():
    """fingerprint_bytes uses default digest_size of 16."""
    fp = RequestFingerprinter()
    request = Request(url="https://example.com")

    result = fp.fingerprint_bytes(request)

    assert len(result) == 16


def test_fingerprint_bytes_custom_digest_size():
    """fingerprint_bytes respects custom digest_size."""
    fp = RequestFingerprinter()
    request = Request(url="https://example.com")

    result = fp.fingerprint_bytes(request, digest_size=32)

    assert len(result) == 32


def test_fingerprint_bytes_different_methods_different_fingerprints():
    """fingerprint_bytes generates different fingerprints for different HTTP methods."""
    fp = RequestFingerprinter()
    get_request = Request(url="https://example.com", method="GET")
    post_request = Request(url="https://example.com", method="POST")

    get_fp = fp.fingerprint_bytes(get_request)
    post_fp = fp.fingerprint_bytes(post_request)

    assert get_fp != post_fp


def test_fingerprint_bytes_includes_body():
    """fingerprint_bytes includes request body in fingerprint."""
    fp = RequestFingerprinter()
    request_no_body = Request(url="https://example.com", method="POST")
    request_with_body = Request(url="https://example.com", method="POST", body=b"data")

    fp_no_body = fp.fingerprint_bytes(request_no_body)
    fp_with_body = fp.fingerprint_bytes(request_with_body)

    assert fp_no_body != fp_with_body


def test_fingerprint_bytes_with_sha256():
    """fingerprint_bytes works with sha256 algorithm."""
    fp = RequestFingerprinter()
    request = Request(url="https://example.com")

    result = fp.fingerprint_bytes(request, algorithm="sha256")

    assert isinstance(result, bytes)
    assert len(result) == 32  # SHA256 digest is 32 bytes


def test_fingerprint_bytes_same_request_same_fingerprint():
    """fingerprint_bytes generates same fingerprint for identical requests."""
    fp = RequestFingerprinter()
    request1 = Request(url="https://example.com/page?id=1", method="GET")
    request2 = Request(url="https://example.com/page?id=1", method="GET")

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)

    assert fp1 == fp2


# Query Parameter Filtering Tests


def test_fingerprint_ignores_specified_params():
    """fingerprint_bytes ignores specified query parameters."""
    fp = RequestFingerprinter(ignore_query_params={"session"})

    # These should have the same fingerprint
    request1 = Request(url="https://example.com?id=1&session=abc")
    request2 = Request(url="https://example.com?id=1&session=xyz")

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)

    assert fp1 == fp2


def test_fingerprint_keeps_only_specified_params():
    """fingerprint_bytes keeps only specified query parameters."""
    fp = RequestFingerprinter(keep_query_params={"id"})

    # These should have the same fingerprint (only 'id' matters)
    request1 = Request(url="https://example.com?id=1&session=abc&foo=bar")
    request2 = Request(url="https://example.com?id=1&session=xyz&baz=qux")

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)

    assert fp1 == fp2


def test_fingerprint_different_kept_params_different_fingerprint():
    """fingerprint_bytes generates different fingerprints for different kept params."""
    fp = RequestFingerprinter(keep_query_params={"id"})

    request1 = Request(url="https://example.com?id=1&session=abc")
    request2 = Request(url="https://example.com?id=2&session=abc")

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)

    assert fp1 != fp2


def test_fingerprint_no_query_params():
    """fingerprint_bytes handles URLs without query parameters."""
    fp = RequestFingerprinter()
    request = Request(url="https://example.com/path")

    result = fp.fingerprint_bytes(request)

    assert isinstance(result, bytes)


def test_fingerprint_url_normalization():
    """fingerprint_bytes normalizes URLs before fingerprinting."""
    fp = RequestFingerprinter()

    # These should have the same fingerprint after normalization
    request1 = Request(url="https://example.com/path")
    request2 = Request(url="HTTPS://EXAMPLE.COM/path")

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)

    assert fp1 == fp2


# Integration Tests


def test_fingerprint_deduplication_scenario():
    """Integration: fingerprinter enables request deduplication."""
    fp = RequestFingerprinter(ignore_query_params={"timestamp", "session"})

    # Simulate requests with varying session/timestamp but same core URL
    request1 = Request(url="https://example.com/page?id=1&timestamp=100&session=abc")
    request2 = Request(url="https://example.com/page?id=1&timestamp=200&session=xyz")
    request3 = Request(url="https://example.com/page?id=2&timestamp=300&session=def")

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)
    fp3 = fp.fingerprint_bytes(request3)

    # First two should be duplicates (same id, different ignored params)
    assert fp1 == fp2

    # Third should be different (different id)
    assert fp1 != fp3


def test_fingerprint_with_post_body():
    """Integration: fingerprint includes POST body data."""
    fp = RequestFingerprinter()

    request1 = Request(url="https://example.com/api", method="POST", body=b'{"user": "alice"}')
    request2 = Request(url="https://example.com/api", method="POST", body=b'{"user": "bob"}')
    request3 = Request(url="https://example.com/api", method="POST", body=b'{"user": "alice"}')

    fp1 = fp.fingerprint_bytes(request1)
    fp2 = fp.fingerprint_bytes(request2)
    fp3 = fp.fingerprint_bytes(request3)

    # Different bodies → different fingerprints
    assert fp1 != fp2

    # Same body → same fingerprint
    assert fp1 == fp3


def test_fingerprint_consistent_across_instances():
    """Integration: fingerprint is consistent across fingerprinter instances."""
    fp1 = RequestFingerprinter()
    fp2 = RequestFingerprinter()

    request = Request(url="https://example.com/page?id=1")

    result1 = fp1.fingerprint_bytes(request)
    result2 = fp2.fingerprint_bytes(request)

    assert result1 == result2
