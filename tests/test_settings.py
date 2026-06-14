"""Tests for qcrawl.settings.Settings validation."""

import pytest

from qcrawl.settings import Settings

# Default Value Tests


def test_camoufox_process_request_headers_default():
    """CAMOUFOX_PROCESS_REQUEST_HEADERS defaults to 'use_qcrawl_headers'."""
    settings = Settings()

    assert settings.CAMOUFOX_PROCESS_REQUEST_HEADERS == "use_qcrawl_headers"


def test_rejects_both_query_param_filters():
    """IGNORE_QUERY_PARAMS and KEEP_QUERY_PARAMS cannot both be set."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        Settings(IGNORE_QUERY_PARAMS={"a"}, KEEP_QUERY_PARAMS={"b"})


def test_rejects_noninteger_max_depth():
    """MAX_DEPTH must be an int."""
    with pytest.raises(TypeError, match="MAX_DEPTH must be an int"):
        Settings(MAX_DEPTH="3")  # type: ignore[arg-type]


def test_rejects_negative_max_depth():
    """MAX_DEPTH must be >= 0."""
    with pytest.raises(ValueError, match="max_depth must be >= 0"):
        Settings(MAX_DEPTH=-1)


def test_rejects_nonnumeric_delay_per_domain():
    """DELAY_PER_DOMAIN must be a number."""
    with pytest.raises(TypeError, match="DELAY_PER_DOMAIN must be a number"):
        Settings(DELAY_PER_DOMAIN="x")  # type: ignore[arg-type]


def test_rejects_negative_delay_per_domain():
    """DELAY_PER_DOMAIN must be >= 0."""
    with pytest.raises(ValueError, match="delay_per_domain must be >= 0"):
        Settings(DELAY_PER_DOMAIN=-1.0)


def test_rejects_bad_retry_http_codes():
    """RETRY_HTTP_CODES must be a collection of ints."""
    with pytest.raises(TypeError, match="RETRY_HTTP_CODES must be a collection of ints"):
        Settings(RETRY_HTTP_CODES=500)  # type: ignore[arg-type]


def test_rejects_negative_retry_backoff():
    """RETRY_BACKOFF_* must be >= 0."""
    with pytest.raises(ValueError, match="RETRY_BACKOFF_BASE must be >= 0"):
        Settings(RETRY_BACKOFF_BASE=-1.0)


def test_rejects_noninteger_retry_priority_adjust():
    """RETRY_PRIORITY_ADJUST must be an int."""
    with pytest.raises(TypeError, match="RETRY_PRIORITY_ADJUST must be an int"):
        Settings(RETRY_PRIORITY_ADJUST=1.5)  # type: ignore[arg-type]


# Valid Values Tests


def test_accepts_use_qcrawl_headers():
    """Settings accepts 'use_qcrawl_headers'."""
    settings = Settings().with_overrides({"CAMOUFOX_PROCESS_REQUEST_HEADERS": "use_qcrawl_headers"})

    assert settings.CAMOUFOX_PROCESS_REQUEST_HEADERS == "use_qcrawl_headers"


def test_accepts_ignore():
    """Settings accepts 'ignore'."""
    settings = Settings().with_overrides({"CAMOUFOX_PROCESS_REQUEST_HEADERS": "ignore"})

    assert settings.CAMOUFOX_PROCESS_REQUEST_HEADERS == "ignore"


def test_accepts_callable():
    """Settings accepts callable for custom header processing."""

    def custom_processor(request, default_headers):
        return {"X-Custom": "Header"}

    settings = Settings().with_overrides({"CAMOUFOX_PROCESS_REQUEST_HEADERS": custom_processor})

    assert callable(settings.CAMOUFOX_PROCESS_REQUEST_HEADERS)
    assert settings.CAMOUFOX_PROCESS_REQUEST_HEADERS is custom_processor


# Invalid Values Tests


def test_rejects_invalid_string():
    """Settings rejects invalid string values."""
    with pytest.raises(
        ValueError,
        match="CAMOUFOX_PROCESS_REQUEST_HEADERS must be 'use_qcrawl_headers', 'ignore', or callable",
    ):
        Settings(CAMOUFOX_PROCESS_REQUEST_HEADERS="invalid_mode")


def test_rejects_empty_string():
    """Settings rejects empty string."""
    with pytest.raises(
        ValueError,
        match="CAMOUFOX_PROCESS_REQUEST_HEADERS must be 'use_qcrawl_headers', 'ignore', or callable",
    ):
        Settings(CAMOUFOX_PROCESS_REQUEST_HEADERS="")
