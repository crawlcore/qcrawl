"""Tests for RobotsTxtMiddleware."""

import pytest

from qcrawl.core.request import Request
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.robotstxt import RobotsTxtMiddleware

# Initialization Tests


def test_middleware_init_default():
    """RobotsTxtMiddleware initializes with defaults."""
    middleware = RobotsTxtMiddleware()

    assert middleware.obey is True
    assert middleware.user_agent is None
    assert middleware.cache_ttl == 3600.0


def test_middleware_init_custom():
    """RobotsTxtMiddleware initializes with custom parameters."""
    middleware = RobotsTxtMiddleware(
        obey_robots_txt=False, user_agent="CustomBot/1.0", cache_ttl=7200.0
    )

    assert middleware.obey is False
    assert middleware.user_agent == "CustomBot/1.0"
    assert middleware.cache_ttl == 7200.0


# User Agent Resolution Tests


def test_resolve_user_agent_uses_middleware_value(spider):
    """_resolve_user_agent uses middleware user_agent when set."""
    middleware = RobotsTxtMiddleware(user_agent="MiddlewareBot/1.0")

    ua = middleware._resolve_user_agent(spider)

    assert ua == "MiddlewareBot/1.0"


def test_resolve_user_agent_fallback_to_default(spider):
    """_resolve_user_agent falls back to default."""
    middleware = RobotsTxtMiddleware()

    ua = middleware._resolve_user_agent(spider)

    # Should use default or spider's USER_AGENT
    assert isinstance(ua, str)
    assert len(ua) > 0


# process_request Tests - obey=False


@pytest.mark.asyncio
async def test_process_request_continues_when_not_obeying(spider):
    """process_request continues when obey_robots_txt is False."""
    middleware = RobotsTxtMiddleware(obey_robots_txt=False)
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE


# process_request Tests - Basic Behavior


@pytest.mark.asyncio
async def test_process_request_continues_for_allowed_url(spider, monkeypatch):
    """process_request continues when URL is allowed by robots.txt."""

    middleware = RobotsTxtMiddleware()

    # Mock parser that allows all URLs
    class MockParser:
        def can_fetch(self, ua, url):
            return True

    async def mock_ensure_parser(domain_base):
        return MockParser()

    monkeypatch.setattr(middleware, "_ensure_parser", mock_ensure_parser)

    request = Request(url="https://example.com/allowed")
    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE


@pytest.mark.asyncio
async def test_process_request_drops_for_blocked_url(spider, monkeypatch):
    """process_request drops when URL is blocked by robots.txt."""
    middleware = RobotsTxtMiddleware()

    # Mock parser that blocks all URLs
    class MockParser:
        def can_fetch(self, ua, url):
            return False

    async def mock_ensure_parser(domain_base):
        return MockParser()

    monkeypatch.setattr(middleware, "_ensure_parser", mock_ensure_parser)

    request = Request(url="https://example.com/blocked")
    result = await middleware.process_request(request, spider)

    assert result.action == Action.DROP


@pytest.mark.asyncio
async def test_process_request_continues_when_no_parser(spider, monkeypatch):
    """process_request continues when robots.txt unavailable."""
    middleware = RobotsTxtMiddleware()

    async def mock_ensure_parser(domain_base):
        return None

    monkeypatch.setattr(middleware, "_ensure_parser", mock_ensure_parser)

    request = Request(url="https://example.com/page")
    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_config(spider, caplog):
    """open_spider logs configuration."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = RobotsTxtMiddleware(obey_robots_txt=True, user_agent="TestBot/1.0")

    await middleware.open_spider(spider)

    assert "obey=True" in caplog.text
    assert "user_agent=TestBot/1.0" in caplog.text


# close_spider Tests


@pytest.mark.asyncio
async def test_close_spider_clears_caches(spider, monkeypatch):
    """close_spider clears parser caches."""
    middleware = RobotsTxtMiddleware()

    # Add some mock data to caches
    middleware._cache["example.com"] = (0.0, None)
    middleware._locks["example.com"] = object()  # type: ignore[assignment]

    await middleware.close_spider(spider)

    assert middleware._cache == {}
    assert middleware._locks == {}


# Integration Tests


@pytest.mark.asyncio
async def test_robots_txt_allows_url(spider, monkeypatch):
    """Full flow: robots.txt allows URL."""
    middleware = RobotsTxtMiddleware(user_agent="TestBot")

    class MockParser:
        def can_fetch(self, ua, url):
            # Allow /public/* but not /private/*
            return "/private/" not in url

    async def mock_ensure_parser(domain_base):
        return MockParser()

    monkeypatch.setattr(middleware, "_ensure_parser", mock_ensure_parser)

    # Public URL should be allowed
    public_request = Request(url="https://example.com/public/page")
    result = await middleware.process_request(public_request, spider)
    assert result.action == Action.CONTINUE

    # Private URL should be blocked
    private_request = Request(url="https://example.com/private/admin")
    result = await middleware.process_request(private_request, spider)
    assert result.action == Action.DROP
