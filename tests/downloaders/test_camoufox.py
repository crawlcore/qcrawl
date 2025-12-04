"""Unit tests for qcrawl.downloaders.camoufox.CamoufoxDownloader"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from qcrawl.core.page import PageMethod
from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.downloaders.camoufox import CamoufoxDownloader

# Fixtures


@pytest.fixture
def mock_page():
    """Provide a mock Camoufox page."""
    page = Mock()
    page.goto = AsyncMock(return_value=Mock(status=200))
    page.content = AsyncMock(return_value="<html><body>Test</body></html>")
    page.url = "https://example.com"
    page.close = AsyncMock()
    page.set_default_timeout = Mock()
    page.on = Mock()
    return page


@pytest.fixture
def mock_context(mock_page):
    """Provide a mock Camoufox browser context."""
    context = Mock()
    context.new_page = AsyncMock(return_value=mock_page)
    context.close = AsyncMock()
    context.set_extra_http_headers = AsyncMock()
    return context


@pytest.fixture
def mock_browser(mock_context):
    """Provide a mock Camoufox browser."""
    browser = Mock()
    browser.new_context = AsyncMock(return_value=mock_context)
    browser.close = AsyncMock()
    return browser


@pytest.fixture
def camoufox_downloader(mock_browser):
    """Provide a CamoufoxDownloader instance with mocked browser."""
    return CamoufoxDownloader(
        browser=mock_browser,
        own_browser=True,
        contexts={"default": {}},
        max_contexts=10,
        max_pages_per_context=5,
        default_timeout=30000.0,
    )


# Initialization Tests


def test_downloader_initializes_correctly(mock_browser):
    """CamoufoxDownloader initializes with all required components."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        own_browser=True,
        contexts={"default": {}, "mobile": {"viewport": {"width": 375, "height": 667}}},
        max_contexts=10,
        max_pages_per_context=5,
        default_timeout=30000.0,
    )

    assert downloader._browser is mock_browser
    assert downloader._own_browser is True
    assert downloader._closed is False
    assert downloader.signals is not None
    assert len(downloader._context_configs) == 2
    assert downloader._max_contexts == 10
    assert downloader._max_pages_per_context == 5
    assert downloader._default_timeout == 30000.0


def test_downloader_init_with_external_browser(mock_browser):
    """CamoufoxDownloader initializes correctly with external browser."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        own_browser=False,
        contexts={"default": {}},
    )

    assert downloader._browser is mock_browser
    assert downloader._own_browser is False
    assert downloader._closed is False


# Factory Method Tests


@pytest.mark.asyncio
async def test_create_with_local_browser():
    """CamoufoxDownloader.create() launches local browser."""
    mock_browser_instance = Mock()
    mock_browser_instance.new_context = AsyncMock(return_value=Mock())
    mock_browser_instance.close = AsyncMock()

    with patch("qcrawl.downloaders.camoufox.AsyncCamoufox") as mock_camoufox_class:
        # Mock AsyncCamoufox to return an async context manager
        mock_cm = Mock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_browser_instance)
        mock_cm.__aexit__ = AsyncMock()
        mock_camoufox_class.return_value = mock_cm

        downloader = await CamoufoxDownloader.create(
            settings={
                "contexts": {"default": {}},
                "max_contexts": 10,
                "max_pages_per_context": 5,
                "default_timeout": 30000.0,
                "launch_options": {"headless": True},
            }
        )

        try:
            assert downloader._browser is mock_browser_instance
            assert downloader._own_browser is True
            assert len(downloader._contexts) == 1
            assert "default" in downloader._contexts
        finally:
            await downloader.close()


@pytest.mark.asyncio
async def test_create_with_cdp_url():
    """CamoufoxDownloader.create() connects to remote browser via CDP."""
    mock_browser_instance = Mock()
    mock_browser_instance.new_context = AsyncMock(return_value=Mock())
    mock_browser_instance.close = AsyncMock()

    with patch("qcrawl.downloaders.camoufox.AsyncCamoufox") as mock_camoufox_class:
        mock_camoufox_class.connect = AsyncMock(return_value=mock_browser_instance)

        downloader = await CamoufoxDownloader.create(
            settings={
                "contexts": {"default": {}},
                "cdp_url": "http://localhost:9222",
            }
        )

        try:
            assert downloader._browser is mock_browser_instance
            assert downloader._own_browser is False
            mock_camoufox_class.connect.assert_called_once_with("http://localhost:9222")
        finally:
            await downloader.close()


# Context Management Tests


@pytest.mark.asyncio
async def test_create_all_contexts(mock_browser):
    """_create_all_contexts() pre-creates all named contexts."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        contexts={"default": {}, "mobile": {"viewport": {"width": 375}}},
    )

    await downloader._create_all_contexts()

    assert len(downloader._contexts) == 2
    assert "default" in downloader._contexts
    assert "mobile" in downloader._contexts
    assert len(downloader._page_semaphores) == 2
    assert mock_browser.new_context.call_count == 2


def test_get_context_returns_existing(camoufox_downloader, mock_context):
    """_get_context() returns pre-created context."""
    camoufox_downloader._contexts["default"] = mock_context

    result = camoufox_downloader._get_context("default")

    assert result is mock_context


def test_get_context_raises_for_undefined(camoufox_downloader):
    """_get_context() raises RuntimeError for undefined context."""
    with pytest.raises(RuntimeError, match="Context 'invalid' not found"):
        camoufox_downloader._get_context("invalid")


# Fetch Method Tests


@pytest.mark.asyncio
async def test_fetch_basic_request(camoufox_downloader, mock_context, mock_page):
    """fetch() handles basic request successfully."""
    await camoufox_downloader._create_all_contexts()

    request = Request(url="https://example.com")
    result = await camoufox_downloader.fetch(request)

    assert isinstance(result, Page)
    assert result.url == "https://example.com"
    assert result.status_code == 200
    assert b"Test" in result.content
    assert result.request is request

    mock_context.new_page.assert_called_once()
    mock_page.goto.assert_called_once()
    mock_page.close.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_with_string_url(camoufox_downloader, mock_context, mock_page):
    """fetch() accepts string URL and converts to Request."""
    await camoufox_downloader._create_all_contexts()

    result = await camoufox_downloader.fetch("https://example.com")

    assert isinstance(result, Page)
    assert result.url == "https://example.com"
    mock_page.goto.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_with_named_context(mock_browser):
    """fetch() uses named context from meta."""
    # Create downloader with multiple contexts
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        contexts={"default": {}, "mobile": {"viewport": {"width": 375}}},
    )

    await downloader._create_all_contexts()

    mobile_page = Mock()
    mobile_page.goto = AsyncMock(return_value=Mock(status=200))
    mobile_page.content = AsyncMock(return_value="<html>Mobile</html>")
    mobile_page.url = "https://example.com"
    mobile_page.close = AsyncMock()
    mobile_page.set_default_timeout = Mock()

    # Override the mobile context's new_page to return our mock
    mobile_context = downloader._contexts["mobile"]
    mobile_context.new_page = AsyncMock(return_value=mobile_page)
    mobile_context.set_extra_http_headers = AsyncMock()

    request = Request(url="https://example.com", meta={"camoufox_context": "mobile"})
    result = await downloader.fetch(request)

    assert isinstance(result, Page)
    mobile_context.new_page.assert_called_once()
    mobile_page.goto.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_includes_page_in_meta(camoufox_downloader, mock_context, mock_page):
    """fetch() includes page object in response meta when requested."""
    await camoufox_downloader._create_all_contexts()

    request = Request(url="https://example.com", meta={"camoufox_include_page": True})
    result = await camoufox_downloader.fetch(request)

    assert "camoufox_page" in result.meta
    assert result.meta["camoufox_page"] is mock_page
    # Page should NOT be closed when included in meta
    mock_page.close.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_raises_when_closed(camoufox_downloader):
    """fetch() raises RuntimeError when downloader is closed."""
    camoufox_downloader._closed = True

    with pytest.raises(RuntimeError, match="Cannot fetch: downloader is closed"):
        await camoufox_downloader.fetch("https://example.com")


# Page Methods Execution Tests


@pytest.mark.asyncio
async def test_execute_page_methods_after_navigation(camoufox_downloader, mock_context, mock_page):
    """_execute_page_methods() executes methods after navigation."""
    await camoufox_downloader._create_all_contexts()

    # Add a method to mock page
    mock_page.wait_for_selector = AsyncMock(return_value=Mock())

    request = Request(
        url="https://example.com",
        meta={
            "camoufox_page_methods": [
                {"method": "wait_for_selector", "args": [".content"], "timing": "after"}
            ]
        },
    )

    await camoufox_downloader.fetch(request)

    mock_page.wait_for_selector.assert_called_once_with(".content")


@pytest.mark.asyncio
async def test_execute_page_methods_before_navigation():
    """_execute_page_methods() filters methods by timing."""
    mock_page = Mock()
    mock_page.set_viewport_size = Mock()

    downloader = CamoufoxDownloader(browser=Mock(), contexts={"default": {}})

    methods: list[dict[str, object] | PageMethod] = [
        {"method": "set_viewport_size", "kwargs": {"width": 1920}, "timing": "before"},
        {"method": "other_method", "timing": "after"},
    ]

    await downloader._execute_page_methods(mock_page, methods, before_navigation=True)

    # Only "before" method should be called
    mock_page.set_viewport_size.assert_called_once_with(width=1920)


# Event Handlers Tests


@pytest.mark.asyncio
async def test_register_event_handlers(camoufox_downloader):
    """_register_event_handlers() registers handlers on page."""
    mock_page = Mock()
    mock_page.on = Mock()

    handler1 = Mock()
    handler2 = Mock()

    handlers = {"console": handler1, "dialog": handler2}

    await camoufox_downloader._register_event_handlers(mock_page, handlers)

    assert mock_page.on.call_count == 2
    mock_page.on.assert_any_call("console", handler1)
    mock_page.on.assert_any_call("dialog", handler2)


@pytest.mark.asyncio
async def test_register_event_handlers_skips_non_callable(camoufox_downloader):
    """_register_event_handlers() skips non-callable handlers."""
    mock_page = Mock()
    mock_page.on = Mock()

    handlers = {"console": "not_callable", "dialog": Mock()}

    await camoufox_downloader._register_event_handlers(mock_page, handlers)

    # Only callable handler should be registered
    assert mock_page.on.call_count == 1


# Header Processing Tests


def test_process_headers_use_qcrawl_headers(camoufox_downloader):
    """_process_headers() merges headers when mode is use_qcrawl_headers."""
    camoufox_downloader._process_request_headers = "use_qcrawl_headers"

    mock_spider = Mock()
    mock_spider.runtime_settings = Mock()
    mock_spider.runtime_settings.DEFAULT_REQUEST_HEADERS = {
        "Accept": "text/html",
        "User-Agent": "qCrawl/1.0",
    }

    request = Request(url="https://example.com", headers={"Custom": "Value"})
    additional_headers = {"Extra": "Header"}

    result = camoufox_downloader._process_headers(request, mock_spider, additional_headers)

    assert result["Accept"] == "text/html"
    assert result["User-Agent"] == "qCrawl/1.0"
    assert result["Custom"] == "Value"
    assert result["Extra"] == "Header"


def test_process_headers_ignore_mode(camoufox_downloader):
    """_process_headers() returns empty dict when mode is ignore."""
    camoufox_downloader._process_request_headers = "ignore"

    request = Request(url="https://example.com", headers={"Custom": "Value"})
    result = camoufox_downloader._process_headers(request, None, None)

    assert result == {}


def test_process_headers_callable_mode(camoufox_downloader):
    """_process_headers() uses callable processor."""

    def custom_processor(request, default_headers):
        return {"X-Custom": "ProcessedValue"}

    camoufox_downloader._process_request_headers = custom_processor

    request = Request(url="https://example.com")
    result = camoufox_downloader._process_headers(request, None, None)

    assert result == {"X-Custom": "ProcessedValue"}


# Navigation kwargs Tests


def test_prepare_goto_kwargs_with_defaults(camoufox_downloader):
    """_prepare_goto_kwargs() sets default values."""
    result = camoufox_downloader._prepare_goto_kwargs({}, 30000.0)

    assert result["wait_until"] == "domcontentloaded"
    assert result["timeout"] == 30000.0


def test_prepare_goto_kwargs_merges_user_values(camoufox_downloader):
    """_prepare_goto_kwargs() merges user-provided values."""
    user_kwargs = {"wait_until": "networkidle", "referer": "https://google.com"}

    result = camoufox_downloader._prepare_goto_kwargs(user_kwargs, 30000.0)

    assert result["wait_until"] == "networkidle"
    assert result["timeout"] == 30000.0
    assert result["referer"] == "https://google.com"


# Cleanup Tests


@pytest.mark.asyncio
async def test_close_owned_browser(mock_browser):
    """close() closes browser when downloader owns it."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        own_browser=True,
        contexts={"default": {}},
    )

    await downloader.close()

    assert downloader._closed is True
    mock_browser.close.assert_called_once()


@pytest.mark.asyncio
async def test_close_external_browser(mock_browser):
    """close() does not close external browser."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        own_browser=False,
        contexts={"default": {}},
    )

    await downloader.close()

    assert downloader._closed is True
    mock_browser.close.assert_not_called()


@pytest.mark.asyncio
async def test_close_closes_all_contexts(mock_browser, mock_context):
    """close() closes all pre-created contexts."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        contexts={"default": {}, "mobile": {}},
    )

    context1 = Mock()
    context1.close = AsyncMock()
    context2 = Mock()
    context2.close = AsyncMock()

    downloader._contexts["default"] = context1
    downloader._contexts["mobile"] = context2

    await downloader.close()

    context1.close.assert_called_once()
    context2.close.assert_called_once()
    assert len(downloader._contexts) == 0


@pytest.mark.asyncio
async def test_multiple_close_calls_are_safe(mock_browser):
    """close() handles multiple calls gracefully."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        own_browser=True,
        contexts={"default": {}},
    )

    await downloader.close()
    await downloader.close()  # Should not error

    # Browser close should only be called once
    assert mock_browser.close.call_count == 1
    assert downloader._closed is True


# Signal Registry


def test_downloader_has_signals(camoufox_downloader):
    """CamoufoxDownloader has access to signal registry."""
    assert camoufox_downloader.signals is not None
    assert hasattr(camoufox_downloader, "signals")


# Async Context Manager


@pytest.mark.asyncio
async def test_async_context_manager(mock_browser):
    """CamoufoxDownloader works as async context manager."""
    downloader = CamoufoxDownloader(
        browser=mock_browser,
        own_browser=True,
        contexts={"default": {}},
    )

    async with downloader as dm:
        assert dm is downloader
        assert not dm._closed

    # Should be closed after exit
    assert downloader._closed is True
    mock_browser.close.assert_called_once()
