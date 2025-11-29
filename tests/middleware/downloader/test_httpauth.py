"""Tests for HttpAuthMiddleware."""

import base64

import pytest

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.httpauth import HttpAuthMiddleware

# Initialization Tests


def test_middleware_init_default():
    """HttpAuthMiddleware initializes with defaults."""
    middleware = HttpAuthMiddleware()

    assert middleware._credentials == {}
    assert middleware.auth_type == "basic"
    assert middleware.digest_qop_auth_int is False


def test_middleware_init_with_credentials():
    """HttpAuthMiddleware initializes with credentials."""
    creds = {"example.com": ("user", "pass")}
    middleware = HttpAuthMiddleware(credentials=creds)

    assert middleware._credentials == creds


def test_middleware_init_digest_auth():
    """HttpAuthMiddleware initializes with digest auth."""
    middleware = HttpAuthMiddleware(auth_type="digest")

    assert middleware.auth_type == "digest"


def test_middleware_init_invalid_auth_type():
    """HttpAuthMiddleware rejects invalid auth_type."""
    with pytest.raises(ValueError, match="auth_type must be 'basic' or 'digest'"):
        HttpAuthMiddleware(auth_type="invalid")


# Credential Management Tests


def test_add_credentials():
    """add_credentials adds domain credentials."""
    middleware = HttpAuthMiddleware()

    middleware.add_credentials("example.com", "user", "pass")

    assert middleware._credentials["example.com"] == ("user", "pass")


def test_add_credentials_normalizes_domain():
    """add_credentials lowercases domain."""
    middleware = HttpAuthMiddleware()

    middleware.add_credentials("Example.COM", "user", "pass")

    assert middleware._credentials["example.com"] == ("user", "pass")


def test_remove_credentials():
    """remove_credentials removes domain credentials."""
    middleware = HttpAuthMiddleware(credentials={"example.com": ("user", "pass")})

    middleware.remove_credentials("example.com")

    assert "example.com" not in middleware._credentials


def test_clear_credentials():
    """clear_credentials removes all credentials."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user1", "pass1"), "other.com": ("user2", "pass2")}
    )

    middleware.clear_credentials()

    assert middleware._credentials == {}


# Basic Auth Tests


def test_encode_basic_auth():
    """_encode_basic_auth encodes credentials correctly."""
    middleware = HttpAuthMiddleware()

    auth_header = middleware._encode_basic_auth("user", "pass")

    expected = "Basic " + base64.b64encode(b"user:pass").decode("ascii")
    assert auth_header == expected


@pytest.mark.asyncio
async def test_process_request_adds_basic_auth(spider):
    """process_request adds Basic auth header."""
    middleware = HttpAuthMiddleware(credentials={"example.com": ("user", "pass")})
    request = Request(url="https://example.com/api")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Authorization" in request.headers
    assert request.headers["Authorization"].startswith("Basic ")


@pytest.mark.asyncio
async def test_process_request_no_auth_for_unknown_domain(spider):
    """process_request skips auth for domains without credentials."""
    middleware = HttpAuthMiddleware(credentials={"example.com": ("user", "pass")})
    request = Request(url="https://other.com/api")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Authorization" not in request.headers


@pytest.mark.asyncio
async def test_process_request_per_request_auth_override(spider):
    """process_request uses per-request auth credentials."""
    middleware = HttpAuthMiddleware(credentials={"example.com": ("default_user", "default_pass")})
    request = Request(url="https://example.com/api", meta={"auth": ("custom_user", "custom_pass")})

    await middleware.process_request(request, spider)

    assert "Authorization" in request.headers
    # Decode and check it's the custom credentials
    auth_value = request.headers["Authorization"]
    assert auth_value.startswith("Basic ")
    decoded = base64.b64decode(auth_value[6:]).decode("utf-8")
    assert decoded == "custom_user:custom_pass"


@pytest.mark.asyncio
async def test_process_request_invalid_auth_tuple_logged(spider):
    """process_request handles invalid auth tuple gracefully."""
    middleware = HttpAuthMiddleware()
    request = Request(url="https://example.com/api", meta={"auth": "invalid"})

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Authorization" not in request.headers


@pytest.mark.asyncio
async def test_process_request_digest_auth_skips_proactive_header(spider):
    """process_request does not add proactive auth for digest."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "pass")}, auth_type="digest"
    )
    request = Request(url="https://example.com/api")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Authorization" not in request.headers


# Digest Auth Tests


def test_parse_digest_challenge():
    """_parse_digest_challenge parses WWW-Authenticate header."""
    middleware = HttpAuthMiddleware()
    header = 'Digest realm="api", nonce="abc123", qop="auth", opaque="xyz"'

    challenge = middleware._parse_digest_challenge(header)

    assert challenge["realm"] == "api"
    assert challenge["nonce"] == "abc123"
    assert challenge["qop"] == "auth"
    assert challenge["opaque"] == "xyz"


def test_parse_digest_challenge_handles_spaces():
    """_parse_digest_challenge handles varied spacing."""
    middleware = HttpAuthMiddleware()
    header = 'Digest realm = "api" , nonce = "abc123"'

    challenge = middleware._parse_digest_challenge(header)

    assert challenge["realm"] == "api"
    assert challenge["nonce"] == "abc123"


def test_parse_digest_challenge_non_digest():
    """_parse_digest_challenge returns empty for non-Digest."""
    middleware = HttpAuthMiddleware()

    challenge = middleware._parse_digest_challenge("Basic realm=test")

    assert challenge == {}


@pytest.mark.asyncio
async def test_process_response_keeps_non_401(spider):
    """process_response keeps responses that are not 401."""
    middleware = HttpAuthMiddleware()
    request = Request(url="https://example.com/api")
    response = Page(
        url="https://example.com/api",
        content=b"success",
        status_code=200,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_digest_401_without_credentials(spider):
    """process_response keeps 401 when no credentials available."""
    middleware = HttpAuthMiddleware(auth_type="digest")
    request = Request(url="https://example.com/api")
    response = Page(
        url="https://example.com/api",
        content=b"unauthorized",
        status_code=401,
        headers={"WWW-Authenticate": 'Digest realm="api", nonce="abc123"'},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_digest_401_retries_with_auth(spider):
    """process_response retries 401 with Digest auth."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "pass")}, auth_type="digest"
    )
    request = Request(url="https://example.com/api")
    response = Page(
        url="https://example.com/api",
        content=b"unauthorized",
        status_code=401,
        headers={"WWW-Authenticate": 'Digest realm="api", nonce="abc123", qop="auth"'},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.RETRY
    assert isinstance(result.payload, Request)
    assert "Authorization" in result.payload.headers
    assert result.payload.headers["Authorization"].startswith("Digest ")
    assert "_digest_retry" in result.payload.meta


@pytest.mark.asyncio
async def test_process_response_digest_prevents_infinite_retry(spider):
    """process_response does not retry if already retried."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "pass")}, auth_type="digest"
    )
    request = Request(url="https://example.com/api", meta={"_digest_retry": True})
    response = Page(
        url="https://example.com/api",
        content=b"unauthorized",
        status_code=401,
        headers={"WWW-Authenticate": 'Digest realm="api", nonce="abc123"'},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_digest_invalid_challenge(spider):
    """process_response handles invalid digest challenge."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "pass")}, auth_type="digest"
    )
    request = Request(url="https://example.com/api")
    response = Page(
        url="https://example.com/api",
        content=b"unauthorized",
        status_code=401,
        headers={"WWW-Authenticate": "Digest invalid"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP


@pytest.mark.asyncio
async def test_process_response_basic_auth_keeps_401(spider):
    """process_response with basic auth keeps 401 (no retry)."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "pass")}, auth_type="basic"
    )
    request = Request(url="https://example.com/api")
    response = Page(
        url="https://example.com/api",
        content=b"unauthorized",
        status_code=401,
        headers={},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_continues(spider):
    """process_exception continues without special handling."""
    middleware = HttpAuthMiddleware()
    request = Request(url="https://example.com/api")
    exception = Exception("Test error")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.CONTINUE


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_config(spider, caplog):
    """open_spider logs configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "pass"), "other.com": ("u2", "p2")},
        auth_type="digest",
        digest_qop_auth_int=True,
    )

    await middleware.open_spider(spider)

    assert "credentials: 2 domains" in caplog.text
    assert "auth_type: digest" in caplog.text
    assert "digest_qop_auth_int: True" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_basic_auth_full_flow(spider):
    """Basic auth adds header to request."""
    middleware = HttpAuthMiddleware(credentials={"example.com": ("testuser", "testpass")})

    request = Request(url="https://example.com/protected")
    await middleware.process_request(request, spider)

    assert "Authorization" in request.headers
    auth_value = request.headers["Authorization"]
    decoded = base64.b64decode(auth_value[6:]).decode("utf-8")
    assert decoded == "testuser:testpass"


@pytest.mark.asyncio
async def test_digest_auth_full_flow(spider):
    """Digest auth retries 401 with proper header."""
    middleware = HttpAuthMiddleware(
        credentials={"example.com": ("user", "secret")}, auth_type="digest"
    )

    # Initial request has no auth
    request = Request(url="https://example.com/protected")
    result = await middleware.process_request(request, spider)
    assert "Authorization" not in request.headers

    # Server responds with 401 + challenge
    response = Page(
        url="https://example.com/protected",
        content=b"",
        status_code=401,
        headers={
            "WWW-Authenticate": 'Digest realm="Protected Area", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", qop="auth"'
        },
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    # Should retry with Digest auth
    assert result.action == Action.RETRY
    retry_request = result.payload
    assert "Authorization" in retry_request.headers
    assert retry_request.headers["Authorization"].startswith("Digest ")
    assert 'username="user"' in retry_request.headers["Authorization"]
