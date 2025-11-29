"""Tests for HttpCompressionMiddleware."""

import gzip
import zlib

import pytest

from qcrawl.core.request import Request
from qcrawl.core.response import Page
from qcrawl.middleware.base import Action
from qcrawl.middleware.downloader.httpcompression import HttpCompressionMiddleware

# Initialization Tests


def test_middleware_init_default():
    """HttpCompressionMiddleware initializes with zstd enabled by default."""
    middleware = HttpCompressionMiddleware()

    # enable_zstd depends on runtime zstd availability
    assert isinstance(middleware.enable_zstd, bool)


def test_middleware_init_zstd_disabled():
    """HttpCompressionMiddleware can disable zstd."""
    middleware = HttpCompressionMiddleware(enable_zstd=False)

    assert middleware.enable_zstd is False


# process_request Tests


@pytest.mark.asyncio
async def test_process_request_adds_accept_encoding(spider):
    """process_request adds Accept-Encoding header."""
    middleware = HttpCompressionMiddleware(enable_zstd=False)
    request = Request(url="https://example.com/page")

    result = await middleware.process_request(request, spider)

    assert result.action == Action.CONTINUE
    assert "Accept-Encoding" in request.headers
    assert "gzip" in request.headers["Accept-Encoding"]
    assert "deflate" in request.headers["Accept-Encoding"]


@pytest.mark.asyncio
async def test_process_request_includes_zstd_when_enabled(spider):
    """process_request includes zstd in Accept-Encoding when enabled."""
    middleware = HttpCompressionMiddleware(enable_zstd=True)
    request = Request(url="https://example.com/page")

    await middleware.process_request(request, spider)

    if middleware.enable_zstd:  # Only if zstd is actually available
        assert "zstd" in request.headers["Accept-Encoding"]


@pytest.mark.asyncio
async def test_process_request_preserves_existing_headers(spider):
    """process_request preserves other request headers."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page", headers={"User-Agent": "TestBot"})

    await middleware.process_request(request, spider)

    assert request.headers["User-Agent"] == "TestBot"


@pytest.mark.asyncio
async def test_process_request_does_not_override_accept_encoding(spider):
    """process_request uses setdefault for Accept-Encoding."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page", headers={"Accept-Encoding": "identity"})

    await middleware.process_request(request, spider)

    assert request.headers["Accept-Encoding"] == "identity"


# process_response Tests - No Compression


@pytest.mark.asyncio
async def test_process_response_no_compression_keeps_response(spider):
    """process_response keeps response without Content-Encoding."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"<html>test</html>",
        status_code=200,
        headers={"Content-Type": "text/html"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_empty_content_encoding_keeps_response(spider):
    """process_response keeps response with empty Content-Encoding."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"test",
        status_code=200,
        headers={"Content-Encoding": ""},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


# process_response Tests - Gzip


@pytest.mark.asyncio
async def test_process_response_decompresses_gzip(spider):
    """process_response decompresses gzip content."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"<html>This is compressed content</html>"
    compressed_body = gzip.compress(original_body)

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "gzip", "Content-Length": str(len(compressed_body))},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    new_response = result.payload
    assert new_response.content == original_body
    assert "Content-Encoding" not in new_response.headers
    assert "content-encoding" not in new_response.headers


@pytest.mark.asyncio
async def test_process_response_updates_content_length_gzip(spider):
    """process_response updates Content-Length after decompression."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test content"
    compressed_body = gzip.compress(original_body)

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "gzip", "Content-Length": str(len(compressed_body))},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.headers["Content-Length"] == str(len(original_body))


@pytest.mark.asyncio
async def test_process_response_handles_x_gzip(spider):
    """process_response handles x-gzip encoding."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test"
    compressed_body = gzip.compress(original_body)

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "x-gzip"},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.content == original_body


# process_response Tests - Deflate


@pytest.mark.asyncio
async def test_process_response_decompresses_deflate(spider):
    """process_response decompresses deflate content."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test content"
    compressed_body = zlib.compress(original_body)

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "deflate"},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.content == original_body


@pytest.mark.asyncio
async def test_process_response_handles_raw_deflate(spider):
    """process_response handles raw deflate (no zlib wrapper)."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test content"
    # Raw deflate without zlib header
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed_body = compressor.compress(original_body) + compressor.flush()

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "deflate"},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.content == original_body


# process_response Tests - Multiple Encodings


@pytest.mark.asyncio
async def test_process_response_handles_multiple_encodings(spider):
    """process_response handles stacked encodings."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test content"

    # Apply deflate, then gzip (compress in this order)
    compressed_once = zlib.compress(original_body)
    compressed_twice = gzip.compress(compressed_once)

    # Content-Encoding lists encodings in reverse order (gzip, deflate)
    # This means: decompress gzip first, then deflate
    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "gzip, deflate"},
        content=compressed_twice,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    # Should decompress in reverse order: gzip first, then deflate
    new_response = result.payload
    assert new_response.content == original_body


# process_response Tests - Unknown/Error Cases


@pytest.mark.asyncio
async def test_process_response_unknown_encoding_keeps_response(spider):
    """process_response keeps response with unknown encoding."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"test",
        status_code=200,
        headers={"Content-Encoding": "unknown"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    assert result.action == Action.KEEP
    assert result.payload is response


@pytest.mark.asyncio
async def test_process_response_decompression_error_keeps_response(spider):
    """process_response keeps response when decompression fails."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    response = Page(
        url="https://example.com/page",
        content=b"not actually gzip compressed",
        status_code=200,
        headers={"Content-Encoding": "gzip"},
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    # Should keep original response on error
    assert result.action == Action.KEEP
    assert result.payload is response


# process_response Tests - Case Insensitive Headers


@pytest.mark.asyncio
async def test_process_response_case_insensitive_content_encoding(spider):
    """process_response handles case-insensitive Content-Encoding."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test"
    compressed_body = gzip.compress(original_body)

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"content-encoding": "gzip"},  # lowercase
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.content == original_body


@pytest.mark.asyncio
async def test_process_response_lowercase_content_length_updated(spider):
    """process_response updates lowercase content-length."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    original_body = b"test"
    compressed_body = gzip.compress(original_body)

    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"content-encoding": "gzip", "content-length": str(len(compressed_body))},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.headers["content-length"] == str(len(original_body))


# process_exception Tests


@pytest.mark.asyncio
async def test_process_exception_continues(spider):
    """process_exception continues without special handling."""
    middleware = HttpCompressionMiddleware()
    request = Request(url="https://example.com/page")
    exception = Exception("Test error")

    result = await middleware.process_exception(request, exception, spider)

    assert result.action == Action.CONTINUE


# open_spider Tests


@pytest.mark.asyncio
async def test_open_spider_logs_start(spider, caplog):
    """open_spider logs startup."""
    import logging

    caplog.set_level(logging.INFO)
    middleware = HttpCompressionMiddleware()

    await middleware.open_spider(spider)

    assert "started" in caplog.text


# Integration Tests


@pytest.mark.asyncio
async def test_full_compression_flow(spider):
    """Full flow: request adds Accept-Encoding, response decompresses."""
    middleware = HttpCompressionMiddleware()

    # Request phase
    request = Request(url="https://example.com/page")
    await middleware.process_request(request, spider)
    assert "Accept-Encoding" in request.headers

    # Response phase
    original_body = b"<html><body>Test Content</body></html>"
    compressed_body = gzip.compress(original_body)
    response = Page(
        url="https://example.com/page",
        status_code=200,
        headers={"Content-Encoding": "gzip"},
        content=compressed_body,
        request=request,
    )

    result = await middleware.process_response(request, response, spider)

    new_response = result.payload
    assert new_response.content == original_body
    assert "Content-Encoding" not in new_response.headers
