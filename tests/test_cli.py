"""Tests for qcrawl.cli"""

import sys
import uuid

import pytest

import qcrawl.cli as cli


@pytest.fixture
def sample_argv():
    """Provide sample CLI arguments."""
    return [
        "qcrawl",
        "mypkg:MySpider",
        "--export",
        "out.ndjson",
        "--export-format",
        "ndjson",
        "-s",
        "foo=bar",
    ]


# Argument Parsing Tests


def test_parse_args_basic(monkeypatch, sample_argv):
    """parse_args correctly parses CLI arguments."""
    monkeypatch.setattr(sys, "argv", sample_argv)

    args = cli.parse_args()

    assert args.spider == "mypkg:MySpider"
    assert args.export == "out.ndjson"
    assert args.export_format == "ndjson"
    assert ("foo", "bar") in args.setting


# Main Function Tests


def test_main_invokes_runner(monkeypatch, tmp_path, dummy_spider, run_coro_sync):
    """main() invokes run_async with correct parameters."""
    monkeypatch.setattr(
        sys, "argv", ["qcrawl", "dummy:DummySpider", "--export", str(tmp_path / "out.ndjson")]
    )

    # Avoid real logging/file system side-effects
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(cli, "ensure_output_dir", lambda *a, **k: None)
    monkeypatch.setattr(cli, "load_spider_class", lambda path: type(dummy_spider))

    # Capture arguments passed to run_async
    recorded = []

    async def fake_run_async(spider_cls, args, settings, runtime_settings):
        recorded.append((spider_cls, args, settings, runtime_settings))

    monkeypatch.setattr(cli, "run_async", fake_run_async)
    monkeypatch.setattr(cli.asyncio, "run", run_coro_sync)

    # Call main
    cli.main()

    assert len(recorded) == 1
    spider_cls, args_ns, spider_settings, runtime_settings = recorded[0]
    assert spider_cls is type(dummy_spider)
    assert args_ns.export == str(tmp_path / "out.ndjson")


def test_main_without_export_flag(monkeypatch, dummy_spider, run_coro_sync):
    """main() runs successfully without --export flag (defaults to stdout in runner)."""
    monkeypatch.setattr(sys, "argv", ["qcrawl", "dummy:DummySpider"])

    # Avoid real logging/file system side-effects
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(cli, "ensure_output_dir", lambda *a, **k: None)
    monkeypatch.setattr(cli, "load_spider_class", lambda path: type(dummy_spider))

    # Capture arguments passed to run_async
    recorded = []

    async def fake_run_async(spider_cls, args, settings, runtime_settings):
        recorded.append((spider_cls, args, settings, runtime_settings))

    monkeypatch.setattr(cli, "run_async", fake_run_async)
    monkeypatch.setattr(cli.asyncio, "run", run_coro_sync)

    # Call main
    cli.main()

    # Verify run_async was called (spider runs even without --export)
    assert len(recorded) == 1
    spider_cls, args_ns, spider_settings, runtime_settings = recorded[0]
    assert spider_cls is type(dummy_spider)
    # No --export provided, args.export should be None (runner defaults to stdout)
    assert args_ns.export is None


# Spider Loading Tests


def test_load_spider_class_adds_cwd_to_syspath(tmp_path, monkeypatch):
    """load_spider_class adds CWD to sys.path."""
    # Create a spider module with unique name
    unique_name = f"test_spider_{uuid.uuid4().hex[:8]}"
    spider_file = tmp_path / f"{unique_name}.py"
    spider_file.write_text(
        """
from qcrawl.core.spider import Spider

class TestSpider(Spider):
    name = "test"
    start_urls = ["http://example.com"]

    async def parse(self, response):
        if False:
            yield
"""
    )

    monkeypatch.chdir(tmp_path)
    original_path = sys.path.copy()
    original_modules = sys.modules.copy()

    try:
        spider_cls = cli.load_spider_class(f"{unique_name}:TestSpider")

        assert str(tmp_path) in sys.path
        assert spider_cls.__name__ == "TestSpider"
        assert spider_cls.name == "test"
    finally:
        sys.path[:] = original_path
        for key in list(sys.modules.keys()):
            if key not in original_modules:
                del sys.modules[key]


def test_load_spider_class_with_dotted_module_path():
    """load_spider_class works with dotted module paths."""
    original_path = sys.path.copy()

    try:
        # Load using dotted path
        from tests.conftest import DummySpider

        spider_cls = cli.load_spider_class("tests.conftest:DummySpider")

        import os

        assert os.getcwd() in sys.path
        assert spider_cls is DummySpider
    finally:
        sys.path[:] = original_path
