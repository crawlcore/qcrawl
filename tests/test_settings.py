"""Tests for qcrawl.settings.Settings validation."""

import pytest

from qcrawl.settings import Settings

# Default Value Tests


def test_camoufox_process_request_headers_default():
    """CAMOUFOX_PROCESS_REQUEST_HEADERS defaults to 'use_qcrawl_headers'."""
    settings = Settings()

    assert settings.CAMOUFOX_PROCESS_REQUEST_HEADERS == "use_qcrawl_headers"


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
