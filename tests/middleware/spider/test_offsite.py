"""Tests for OffsiteMiddleware."""

import pytest

from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.spider.offsite import OffsiteMiddleware

# Initialization Tests


def test_middleware_init():
    """OffsiteMiddleware initializes successfully."""
    middleware = OffsiteMiddleware()

    assert middleware._dropped_count == 0


# Helper Method Tests


def test_normalize_domain():
    """_normalize_domain lowercases and removes port."""
    middleware = OffsiteMiddleware()

    assert middleware._normalize_domain("Example.COM") == "example.com"
    assert middleware._normalize_domain("example.com:8080") == "example.com"
    assert middleware._normalize_domain("EXAMPLE.COM:443") == "example.com"


def test_normalize_domain_empty():
    """_normalize_domain handles empty netloc."""
    middleware = OffsiteMiddleware()

    assert middleware._normalize_domain("") == ""


def test_get_allowed_domains_from_list(spider):
    """_get_allowed_domains reads list from spider."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com", "other.com"]

    allowed = middleware._get_allowed_domains(spider)

    assert allowed == {"example.com", "other.com"}


def test_get_allowed_domains_from_string(spider):
    """_get_allowed_domains handles single domain string."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = "example.com"

    allowed = middleware._get_allowed_domains(spider)

    assert allowed == {"example.com"}


def test_get_allowed_domains_from_start_urls(spider):
    """_get_allowed_domains extracts from start_urls."""
    middleware = OffsiteMiddleware()
    spider.start_urls = ["https://example.com/page", "https://other.com/page"]

    allowed = middleware._get_allowed_domains(spider)

    assert allowed == {"example.com", "other.com"}


def test_get_allowed_domains_no_config(spider):
    """_get_allowed_domains returns None when no config."""
    middleware = OffsiteMiddleware()
    spider.start_urls = []

    allowed = middleware._get_allowed_domains(spider)

    assert allowed is None


def test_extract_domain():
    """_extract_domain extracts domain from URL."""
    middleware = OffsiteMiddleware()

    assert middleware._extract_domain("https://example.com/page") == "example.com"
    assert middleware._extract_domain("http://Example.COM:8080/page") == "example.com"


def test_extract_domain_invalid():
    """_extract_domain returns None for invalid URL."""
    middleware = OffsiteMiddleware()

    assert middleware._extract_domain("not-a-url") is None


def test_is_offsite_same_domain():
    """_is_offsite returns False for same domain."""
    middleware = OffsiteMiddleware()

    assert middleware._is_offsite("https://example.com/page", {"example.com"}) is False


def test_is_offsite_different_domain():
    """_is_offsite returns True for different domain."""
    middleware = OffsiteMiddleware()

    assert middleware._is_offsite("https://other.com/page", {"example.com"}) is True


def test_is_offsite_subdomain():
    """_is_offsite allows subdomains."""
    middleware = OffsiteMiddleware()

    # api.example.com should be allowed when example.com is in allowed
    assert middleware._is_offsite("https://api.example.com/page", {"example.com"}) is False


def test_is_offsite_parent_domain():
    """_is_offsite allows parent domain when subdomain is allowed."""
    middleware = OffsiteMiddleware()

    # example.com should be allowed when api.example.com is in allowed
    assert middleware._is_offsite("https://example.com/page", {"api.example.com"}) is False


def test_is_offsite_invalid_url():
    """_is_offsite returns True for invalid URL."""
    middleware = OffsiteMiddleware()

    assert middleware._is_offsite("invalid", {"example.com"}) is True


# process_spider_output Tests - Items


@pytest.mark.asyncio
async def test_process_spider_output_passes_items(spider, http_response):
    """process_spider_output passes items unchanged."""
    middleware = OffsiteMiddleware()

    async def spider_output():
        yield Item(data={"title": "Test"}, metadata={})

    results = []
    async for item in middleware.process_spider_output(http_response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1
    assert isinstance(results[0], Item)


# process_spider_output Tests - No Filtering


@pytest.mark.asyncio
async def test_process_spider_output_no_filtering_when_no_allowed_domains(spider):
    """process_spider_output passes all requests when no allowed_domains."""
    middleware = OffsiteMiddleware()
    spider.start_urls = []  # No allowed domains
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://anywhere.com/page")
        yield Request(url="https://other.com/page")

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    # All requests should pass through
    assert len(results) == 2


# process_spider_output Tests - Request Objects


@pytest.mark.asyncio
async def test_process_spider_output_allows_onsite_request(spider):
    """process_spider_output allows requests to allowed domain."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/child")

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1


@pytest.mark.asyncio
async def test_process_spider_output_filters_offsite_request(spider):
    """process_spider_output filters requests to other domains."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://other.com/page")

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    # Should be filtered
    assert len(results) == 0


@pytest.mark.asyncio
async def test_process_spider_output_allows_subdomain(spider):
    """process_spider_output allows subdomains."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://api.example.com/data")

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1


# process_spider_output Tests - String URLs


@pytest.mark.asyncio
async def test_process_spider_output_converts_onsite_strings(spider):
    """process_spider_output converts onsite string URLs to Request."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent", meta={"depth": 1})
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield "https://example.com/child"

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    assert len(results) == 1
    assert isinstance(results[0], Request)
    assert results[0].url == "https://example.com/child"
    assert results[0].meta["depth"] == 2


@pytest.mark.asyncio
async def test_process_spider_output_filters_offsite_strings(spider):
    """process_spider_output filters offsite string URLs."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield "https://other.com/page"

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    # Should be filtered
    assert len(results) == 0


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_allowed_domains(spider, caplog):
    """open_spider logs allowed domains."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com", "other.com"]

    await middleware.open_spider(spider)

    assert "allowed_domains=" in caplog.text
    assert "example.com" in caplog.text


@pytest.mark.asyncio
async def test_open_spider_logs_all_domains_allowed(spider, caplog):
    """open_spider logs when all domains allowed."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = OffsiteMiddleware()
    spider.start_urls = []

    await middleware.open_spider(spider)

    assert "all domains allowed" in caplog.text


# close_spider Tests


@pytest.mark.asyncio
async def test_close_spider_records_stats(spider):
    """close_spider records dropped count in stats."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://other.com/page1")
        yield Request(url="https://another.com/page2")

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    # Close spider
    await middleware.close_spider(spider)

    # Should have recorded 2 dropped requests
    assert middleware._dropped_count == 2


# Integration Tests


@pytest.mark.asyncio
async def test_mixed_onsite_offsite_requests(spider):
    """Correctly handles mix of onsite and offsite requests."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/page1")  # onsite
        yield Request(url="https://other.com/page2")  # offsite
        yield Request(url="https://api.example.com/data")  # onsite subdomain
        yield Request(url="https://third.com/page3")  # offsite

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    # Only 2 onsite requests should pass
    assert len(results) == 2
    assert isinstance(results[0], Request)
    assert results[0].url == "https://example.com/page1"
    assert isinstance(results[1], Request)
    assert results[1].url == "https://api.example.com/data"


@pytest.mark.asyncio
async def test_multiple_allowed_domains(spider):
    """Works correctly with multiple allowed domains."""
    middleware = OffsiteMiddleware()
    spider.ALLOWED_DOMAINS = ["example.com", "partner.org"]
    parent_request = Request(url="https://example.com/parent")
    response = Page(
        url="https://example.com/parent",
        content=b"",
        status_code=200,
        headers={},
        request=parent_request,
    )

    async def spider_output():
        yield Request(url="https://example.com/page1")
        yield Request(url="https://partner.org/page2")
        yield Request(url="https://other.com/page3")

    results = []
    async for item in middleware.process_spider_output(response, spider_output(), spider):
        results.append(item)

    # First two should pass, third filtered
    assert len(results) == 2
