"""Tests for HttpProxyMiddleware."""

import pytest

from qcrawl.core.request import Request
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.httpproxy import HttpProxyMiddleware

# Initialization Tests


def test_middleware_init_default():
    """HttpProxyMiddleware initializes with empty defaults."""
    middleware = HttpProxyMiddleware()

    assert middleware.http_proxy is None
    assert middleware.https_proxy is None
    assert middleware.no_proxy == []


def test_middleware_init_with_proxies():
    """HttpProxyMiddleware initializes with proxy URLs."""
    middleware = HttpProxyMiddleware(
        http_proxy="http://proxy.example.com:8080", https_proxy="https://proxy.example.com:8443"
    )

    assert middleware.http_proxy == "http://proxy.example.com:8080"
    assert middleware.https_proxy == "https://proxy.example.com:8443"


def test_middleware_init_with_no_proxy():
    """HttpProxyMiddleware initializes with no_proxy list."""
    middleware = HttpProxyMiddleware(no_proxy=["localhost", "127.0.0.1", "*.local"])

    assert middleware.no_proxy == ["localhost", "127.0.0.1", "*.local"]


# IP Normalization Tests


def test_normalize_ip_ipv4():
    """_normalize_ip normalizes IPv4 addresses."""
    middleware = HttpProxyMiddleware()

    result = middleware._normalize_ip("192.168.1.1")

    assert result == "192.168.1.1"


def test_normalize_ip_ipv6():
    """_normalize_ip normalizes IPv6 addresses."""
    middleware = HttpProxyMiddleware()

    result = middleware._normalize_ip("2001:db8::1")

    assert result == "2001:db8::1"


def test_normalize_ip_ipv6_with_brackets():
    """_normalize_ip strips brackets from IPv6."""
    middleware = HttpProxyMiddleware()

    result = middleware._normalize_ip("[2001:db8::1]")

    assert result == "2001:db8::1"


def test_normalize_ip_with_zone_id():
    """_normalize_ip strips zone ID from IPv6."""
    middleware = HttpProxyMiddleware()

    result = middleware._normalize_ip("fe80::1%eth0")

    assert result == "fe80::1"


def test_normalize_ip_invalid():
    """_normalize_ip returns None for invalid IP."""
    middleware = HttpProxyMiddleware()

    result = middleware._normalize_ip("not-an-ip")

    assert result is None


# CIDR Matching Tests


def test_matches_cidr_ipv4():
    """_matches_cidr matches IPv4 in CIDR range."""
    middleware = HttpProxyMiddleware()

    assert middleware._matches_cidr("192.168.1.10", "192.168.1.0/24") is True
    assert middleware._matches_cidr("192.168.2.10", "192.168.1.0/24") is False


def test_matches_cidr_ipv6():
    """_matches_cidr matches IPv6 in CIDR range."""
    middleware = HttpProxyMiddleware()

    assert middleware._matches_cidr("2001:db8::10", "2001:db8::/32") is True
    assert middleware._matches_cidr("2001:db9::10", "2001:db8::/32") is False


def test_matches_cidr_invalid():
    """_matches_cidr returns False for invalid CIDR."""
    middleware = HttpProxyMiddleware()

    assert middleware._matches_cidr("192.168.1.1", "invalid/cidr") is False


# Proxy Bypass Tests


def test_should_bypass_proxy_exact_domain():
    """_should_bypass_proxy matches exact domain."""
    middleware = HttpProxyMiddleware()

    assert middleware._should_bypass_proxy("https://example.com/path", ["example.com"]) is True
    assert middleware._should_bypass_proxy("https://other.com/path", ["example.com"]) is False


def test_should_bypass_proxy_domain_suffix():
    """_should_bypass_proxy matches domain suffix."""
    middleware = HttpProxyMiddleware()

    assert middleware._should_bypass_proxy("https://api.example.com/path", [".example.com"]) is True
    assert middleware._should_bypass_proxy("https://example.com/path", [".example.com"]) is False


def test_should_bypass_proxy_wildcard():
    """_should_bypass_proxy matches wildcard."""
    middleware = HttpProxyMiddleware()

    assert (
        middleware._should_bypass_proxy("https://api.example.com/path", ["*.example.com"]) is True
    )
    assert middleware._should_bypass_proxy("https://example.com/path", ["*.example.com"]) is False


def test_should_bypass_proxy_ip_exact():
    """_should_bypass_proxy matches exact IP."""
    middleware = HttpProxyMiddleware()

    assert middleware._should_bypass_proxy("http://192.168.1.1/path", ["192.168.1.1"]) is True
    assert middleware._should_bypass_proxy("http://192.168.1.2/path", ["192.168.1.1"]) is False


def test_should_bypass_proxy_cidr():
    """_should_bypass_proxy matches CIDR range."""
    middleware = HttpProxyMiddleware()

    assert middleware._should_bypass_proxy("http://192.168.1.10/path", ["192.168.1.0/24"]) is True
    assert middleware._should_bypass_proxy("http://192.168.2.10/path", ["192.168.1.0/24"]) is False


def test_should_bypass_proxy_empty_list():
    """_should_bypass_proxy returns False for empty list."""
    middleware = HttpProxyMiddleware()

    assert middleware._should_bypass_proxy("https://example.com/path", []) is False


def test_should_bypass_proxy_case_insensitive():
    """_should_bypass_proxy is case-insensitive for domains."""
    middleware = HttpProxyMiddleware()

    assert middleware._should_bypass_proxy("https://EXAMPLE.COM/path", ["example.com"]) is True


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_no_proxy_configured(spider):
    """process_request continues when no proxy configured."""
    middleware = HttpProxyMiddleware()
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "proxy" not in request.meta


@pytest.mark.asyncio
async def test_process_request_sets_http_proxy(spider):
    """process_request sets proxy for HTTP requests."""
    middleware = HttpProxyMiddleware(http_proxy="http://proxy.example.com:8080")
    request = Request(url="http://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert request.meta["proxy"] == "http://proxy.example.com:8080"


@pytest.mark.asyncio
async def test_process_request_sets_https_proxy(spider):
    """process_request sets proxy for HTTPS requests."""
    middleware = HttpProxyMiddleware(https_proxy="http://proxy.example.com:8443")
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert request.meta["proxy"] == "http://proxy.example.com:8443"


@pytest.mark.asyncio
async def test_process_request_respects_per_request_proxy(spider):
    """process_request does not override per-request proxy."""
    middleware = HttpProxyMiddleware(http_proxy="http://default.proxy.com:8080")
    request = Request(url="http://example.com/page", meta={"proxy": "http://custom.proxy.com:8080"})

    await middleware.process_request(request, spider)

    assert request.meta["proxy"] == "http://custom.proxy.com:8080"


@pytest.mark.asyncio
async def test_process_request_bypasses_for_no_proxy_domain(spider):
    """process_request bypasses proxy for no_proxy domains."""
    middleware = HttpProxyMiddleware(
        http_proxy="http://proxy.example.com:8080", no_proxy=["localhost"]
    )
    request = Request(url="http://localhost/page")

    await middleware.process_request(request, spider)

    assert request.meta["proxy"] is None


@pytest.mark.asyncio
async def test_process_request_bypasses_for_no_proxy_cidr(spider):
    """process_request bypasses proxy for no_proxy CIDR."""
    middleware = HttpProxyMiddleware(
        http_proxy="http://proxy.example.com:8080", no_proxy=["192.168.0.0/16"]
    )
    request = Request(url="http://192.168.1.1/page")

    await middleware.process_request(request, spider)

    assert request.meta["proxy"] is None


@pytest.mark.asyncio
async def test_process_request_uses_spider_settings(spider):
    """process_request uses spider proxy settings."""
    spider.HTTP_PROXY = "http://spider.proxy.com:8080"
    middleware = HttpProxyMiddleware()
    request = Request(url="http://example.com/page")

    await middleware.process_request(request, spider)

    assert request.meta["proxy"] == "http://spider.proxy.com:8080"


@pytest.mark.asyncio
async def test_process_request_middleware_overrides_spider(spider):
    """process_request middleware settings don't override spider."""
    spider.HTTP_PROXY = "http://spider.proxy.com:8080"
    middleware = HttpProxyMiddleware(http_proxy="http://middleware.proxy.com:8080")
    request = Request(url="http://example.com/page")

    await middleware.process_request(request, spider)

    # Spider setting takes precedence
    assert request.meta["proxy"] == "http://spider.proxy.com:8080"


@pytest.mark.asyncio
async def test_process_request_ignores_non_http_schemes(spider):
    """process_request ignores non-HTTP/HTTPS schemes."""
    middleware = HttpProxyMiddleware(http_proxy="http://proxy.example.com:8080")
    request = Request(url="ftp://example.com/file")

    await middleware.process_request(request, spider)

    assert "proxy" not in request.meta


# get_proxy_for_url Tests


def test_get_proxy_for_url_http(spider):
    """get_proxy_for_url returns HTTP proxy."""
    middleware = HttpProxyMiddleware(http_proxy="http://proxy.example.com:8080")

    result = middleware.get_proxy_for_url("http://example.com/page", spider)

    assert result == "http://proxy.example.com:8080"


def test_get_proxy_for_url_https(spider):
    """get_proxy_for_url returns HTTPS proxy."""
    middleware = HttpProxyMiddleware(https_proxy="http://proxy.example.com:8443")

    result = middleware.get_proxy_for_url("https://example.com/page", spider)

    assert result == "http://proxy.example.com:8443"


def test_get_proxy_for_url_bypassed(spider):
    """get_proxy_for_url returns None for bypassed URLs."""
    middleware = HttpProxyMiddleware(
        http_proxy="http://proxy.example.com:8080", no_proxy=["localhost"]
    )

    result = middleware.get_proxy_for_url("http://localhost/page", spider)

    assert result is None


def test_get_proxy_for_url_no_proxy_configured(spider):
    """get_proxy_for_url returns None when no proxy configured."""
    middleware = HttpProxyMiddleware()

    result = middleware.get_proxy_for_url("http://example.com/page", spider)

    assert result is None


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_config(spider, caplog):
    """open_spider logs proxy configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = HttpProxyMiddleware(
        http_proxy="http://proxy.example.com:8080", no_proxy=["localhost"]
    )

    await middleware.open_spider(spider)

    assert "http_proxy: http://proxy.example.com:8080" in caplog.text
    assert "no_proxy: ['localhost']" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_full_proxy_flow(spider):
    """Full proxy flow sets proxy in request meta."""
    middleware = HttpProxyMiddleware(
        http_proxy="http://proxy.example.com:8080",
        https_proxy="http://proxy.example.com:8443",
        no_proxy=["localhost", "192.168.0.0/16"],
    )

    # HTTP request - should use http_proxy
    http_request = Request(url="http://example.com/page")
    await middleware.process_request(http_request, spider)
    assert http_request.meta["proxy"] == "http://proxy.example.com:8080"

    # HTTPS request - should use https_proxy
    https_request = Request(url="https://example.com/page")
    await middleware.process_request(https_request, spider)
    assert https_request.meta["proxy"] == "http://proxy.example.com:8443"

    # Localhost - should bypass
    local_request = Request(url="http://localhost/page")
    await middleware.process_request(local_request, spider)
    assert local_request.meta["proxy"] is None

    # Private IP - should bypass
    private_request = Request(url="http://192.168.1.1/page")
    await middleware.process_request(private_request, spider)
    assert private_request.meta["proxy"] is None
