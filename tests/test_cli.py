import asyncio
import sys

import qcrawl.cli as cli
from qcrawl.core.spider import Spider


class DummySpider(Spider):
    name = "dummy"
    start_urls = ["http://example.com"]

    async def parse(self, response):
        if False:
            yield


def _run_coro_sync(coro):
    """Helper to execute a coroutine synchronously (used to patch asyncio.run in tests)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_parse_args_basic(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qcrawl",
            "mypkg:MySpider",
            "--export",
            "out.ndjson",
            "--export-format",
            "ndjson",
            "-s",
            "foo=bar",
        ],
    )
    args = cli.parse_args()
    assert args.spider == "mypkg:MySpider"
    assert args.export == "out.ndjson"
    assert args.export_format == "ndjson"
    # ensure -s produced a tuple list with parsed value
    assert ("foo", "bar") in args.setting


def test_main_invokes_runner(monkeypatch, tmp_path):
    # Prepare argv to simulate CLI invocation
    monkeypatch.setattr(
        sys, "argv", ["qcrawl", "dummy:DummySpider", "--export", str(tmp_path / "out.ndjson")]
    )

    # Avoid real logging/file system side-effects
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(cli, "ensure_output_dir", lambda *a, **k: None)

    # Force load_spider_class to return our DummySpider class
    monkeypatch.setattr(cli, "load_spider_class", lambda path: DummySpider)

    # Capture the arguments passed to run_async
    recorded = []

    async def fake_run_async(spider_cls, args, settings, runtime_settings):
        recorded.append((spider_cls, args, settings, runtime_settings))

    # Patch the run_async coroutine and asyncio.run to execute it synchronously
    monkeypatch.setattr(cli, "run_async", fake_run_async)
    monkeypatch.setattr(cli.asyncio, "run", lambda coro: _run_coro_sync(coro))

    # Call main() — should return normally after the patched run_async completes
    cli.main()

    assert len(recorded) == 1
    spider_cls, args_ns, spider_settings, runtime_settings = recorded[0]
    assert spider_cls is DummySpider
    assert args_ns.export == str(tmp_path / "out.ndjson")


def test_load_spider_class_adds_cwd_to_syspath(tmp_path, monkeypatch):
    """Test that load_spider_class automatically adds CWD to sys.path"""

    # Create a spider module in a temp directory
    spider_file = tmp_path / "test_spider.py"
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

    # Change to the temp directory
    monkeypatch.chdir(tmp_path)

    # Save original sys.path
    original_path = sys.path.copy()

    try:
        # Load spider class with module path
        spider_cls = cli.load_spider_class("test_spider:TestSpider")

        # Verify CWD was added to sys.path
        assert str(tmp_path) in sys.path

        # Verify we got the correct spider class
        assert spider_cls.__name__ == "TestSpider"
        assert spider_cls.name == "test"
    finally:
        # Restore sys.path
        sys.path[:] = original_path


def test_load_spider_class_with_dotted_module_path(monkeypatch):
    """Test that dotted module paths work correctly with CWD added"""
    # Save original sys.path
    original_path = sys.path.copy()

    try:
        # Load our DummySpider using dotted path
        spider_cls = cli.load_spider_class("tests.test_cli:DummySpider")

        # Verify CWD was added
        import os

        assert os.getcwd() in sys.path

        # Verify we got the correct spider class
        assert spider_cls is DummySpider
    finally:
        # Restore sys.path
        sys.path[:] = original_path
