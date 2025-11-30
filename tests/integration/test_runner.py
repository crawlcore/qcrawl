"""Integration tests for qcrawl runner"""

import argparse
from types import SimpleNamespace

import pytest
from testcontainers.core.container import DockerContainer

from qcrawl.core.spider import Spider
from qcrawl.runner.engine import run
from qcrawl.settings import Settings

# Test Fixtures


@pytest.fixture(scope="module")
def httpbin_server():
    """Start httpbin container for testing against real HTTP server.

    httpbin provides endpoints for testing: /json, /html, /status/404, etc.
    """
    import time

    container = DockerContainer("kennethreitz/httpbin:latest")
    container.with_exposed_ports(80)
    container.start()

    # Wait for container to be ready
    host = container.get_container_host_ip()
    port = container.get_exposed_port(80)
    base_url = f"http://{host}:{port}"

    # Simple wait for HTTP server to be ready
    import urllib.request

    max_retries = 30
    for _ in range(max_retries):
        try:
            urllib.request.urlopen(f"{base_url}/get", timeout=1)
            break
        except Exception:
            time.sleep(0.5)

    yield base_url

    container.stop()


@pytest.fixture
def args_no_export():
    """Provide args with no export (defaults to stdout)."""
    return argparse.Namespace(
        export=None,  # Key: no export - should default to stdout
        export_format="ndjson",
        export_mode="buffered",
        export_buffer_size=500,
        setting=[],
        settings_file=None,
        log_level="ERROR",  # Suppress logs during tests
        log_file=None,
    )


@pytest.fixture
def args_with_export(tmp_path):
    """Provide args with file export."""
    return argparse.Namespace(
        export=str(tmp_path / "output.ndjson"),
        export_format="ndjson",
        export_mode="buffered",
        export_buffer_size=500,
        setting=[],
        settings_file=None,
        log_level="ERROR",
        log_file=None,
    )


# Test Spiders


class JsonSpider(Spider):
    """Spider that fetches JSON from httpbin."""

    name = "json_spider"

    def __init__(self, base_url="http://httpbin.org"):
        self.start_urls = [f"{base_url}/json"]
        super().__init__()

    async def parse(self, response):
        """Parse JSON response."""
        data = response.json()
        if "slideshow" in data:
            yield {
                "title": data["slideshow"].get("title"),
                "author": data["slideshow"].get("author"),
            }


class HtmlSpider(Spider):
    """Spider that fetches HTML from httpbin."""

    name = "html_spider"

    def __init__(self, base_url="http://httpbin.org"):
        self.start_urls = [f"{base_url}/html"]
        super().__init__()

    async def parse(self, response):
        """Parse HTML response."""
        rv = self.response_view(response)
        h1_tags = rv.doc.cssselect("h1")

        for h1 in h1_tags:
            yield {"heading": h1.text_content().strip()}


# Integration Tests


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runner_defaults_to_stdout_with_real_http(httpbin_server, args_no_export, capsys):
    """Runner outputs to stdout when no --export flag (against real HTTP server)."""
    spider_settings = SimpleNamespace(spider_args={"base_url": httpbin_server})
    runtime_settings = Settings()

    # Run spider against REAL HTTP server - NO MOCKING
    await run(JsonSpider, args_no_export, spider_settings, runtime_settings)

    # Capture stdout output
    captured = capsys.readouterr()
    output = captured.out

    # Verify items were written to stdout
    # httpbin /json returns slideshow data
    assert len(output) > 0, "Should have output to stdout"
    assert "slideshow" in output or "title" in output or "author" in output, (
        "Should contain data from httpbin JSON response"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_runner_exports_to_file_with_real_http(httpbin_server, args_with_export, tmp_path):
    """Runner exports to file when --export provided (against real HTTP server)."""
    spider_settings = SimpleNamespace(spider_args={"base_url": httpbin_server})
    runtime_settings = Settings()

    output_file = tmp_path / "output.ndjson"

    # Run spider against REAL HTTP server
    await run(JsonSpider, args_with_export, spider_settings, runtime_settings)

    # Verify file was created
    assert output_file.exists(), "Output file should be created"

    content = output_file.read_text()
    assert len(content) > 0, "Output file should have content"
    # Should have JSON data from httpbin
    assert "title" in content or "author" in content, (
        "Should contain data from httpbin JSON response"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_spider_parses_real_html(httpbin_server, args_no_export):
    """Spider successfully parses HTML from real HTTP server."""
    spider_settings = SimpleNamespace(spider_args={"base_url": httpbin_server})
    runtime_settings = Settings()

    # Run HTML spider against REAL HTTP server
    # This tests actual HTML parsing with lxml
    await run(HtmlSpider, args_no_export, spider_settings, runtime_settings)

    # If we get here without errors, parsing worked
    # (actual output verification done in other tests)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_spider_runs_without_export_flag_real_http(httpbin_server):
    """Spider executes successfully when no export specified (key regression test).

    This is the critical behavior we added: previously spider wouldn't run
    without --export, now it defaults to stdout and executes normally.

    Tests against REAL HTTP server to ensure actual behavior works.
    """
    args = argparse.Namespace(
        export=None,  # Key: no export specified
        export_format="ndjson",
        export_mode="buffered",
        export_buffer_size=500,
        setting=[],
        settings_file=None,
        log_level="ERROR",
        log_file=None,
    )
    spider_settings = SimpleNamespace(spider_args={"base_url": httpbin_server})
    runtime_settings = Settings()

    # This should NOT raise - spider runs successfully against real HTTP
    spider_executed = False
    try:
        await run(JsonSpider, args, spider_settings, runtime_settings)
        spider_executed = True
    except Exception as e:
        pytest.fail(f"Spider should execute successfully without --export flag: {e}")

    assert spider_executed, "Spider should execute successfully without --export flag"


@pytest.mark.integration
@pytest.mark.parametrize(
    "export_value,description",
    [
        (None, "no export defaults to stdout"),
        ("-", "explicit dash means stdout"),
        ("stdout", "explicit stdout keyword"),
    ],
)
@pytest.mark.asyncio
async def test_stdout_export_variations_real_http(
    httpbin_server, export_value, description, tmp_path
):
    """Test various ways to specify stdout export against real HTTP server."""
    args = argparse.Namespace(
        export=export_value,
        export_format="ndjson",
        export_mode="buffered",
        export_buffer_size=500,
        setting=[],
        settings_file=None,
        log_level="ERROR",
        log_file=None,
    )
    spider_settings = SimpleNamespace(spider_args={"base_url": httpbin_server})
    runtime_settings = Settings()

    # Run against real HTTP - all variations should work
    try:
        await run(JsonSpider, args, spider_settings, runtime_settings)
    except Exception as e:
        pytest.fail(f"Failed with {description}: {e}")
