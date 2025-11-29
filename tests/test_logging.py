import logging
import sys

from qcrawl.runner.logging import setup_logging
from qcrawl.settings import Settings

# setup_logging Function Tests


def test_setup_logging_defaults():
    """Test setup_logging with default parameters."""
    setup_logging()

    logger = logging.getLogger("qcrawl.test")
    assert logger.level == logging.INFO or logger.getEffectiveLevel() == logging.INFO


def test_setup_logging_custom_level():
    """Test setup_logging with custom log level."""
    setup_logging(level="DEBUG")

    logger = logging.getLogger("qcrawl")
    assert logger.level == logging.DEBUG


def test_setup_logging_custom_format(capfd):
    """Test setup_logging with custom format string."""
    custom_format = "[%(levelname)s] %(message)s"
    setup_logging(level="INFO", log_format=custom_format)

    # Create a logger and emit a message
    logger = logging.getLogger("qcrawl.test_format")
    logger.info("Test message")

    # Capture stdout
    captured = capfd.readouterr()
    # Should contain the message without timestamp or logger name
    assert "[INFO] Test message" in captured.out


def test_setup_logging_custom_dateformat(capfd):
    """Test setup_logging with custom date format."""
    custom_dateformat = "%Y-%m-%d"
    setup_logging(
        level="INFO",
        log_format="%(asctime)s %(message)s",
        log_dateformat=custom_dateformat,
    )

    logger = logging.getLogger("qcrawl.test_dateformat")
    logger.info("Date test")

    captured = capfd.readouterr()
    # Should contain date in YYYY-MM-DD format (no time)
    import re

    assert re.search(r"\d{4}-\d{2}-\d{2} Date test", captured.out)


def test_setup_logging_file_handler(tmp_path):
    """Test setup_logging with file handler."""
    log_file = tmp_path / "test.log"
    setup_logging(level="INFO", log_file=str(log_file))

    logger = logging.getLogger("qcrawl.test_file")
    logger.info("File test message")

    # Check that file was created and contains the message
    assert log_file.exists()
    content = log_file.read_text()
    assert "File test message" in content
    assert "qcrawl.test_file" in content


def test_setup_logging_all_custom(tmp_path):
    """Test setup_logging with all parameters customized."""
    log_file = tmp_path / "custom.log"
    custom_format = "%(levelname)-8s | %(name)s | %(message)s"
    custom_dateformat = "%H:%M:%S"

    setup_logging(
        level="DEBUG",
        log_file=str(log_file),
        log_format=custom_format,
        log_dateformat=custom_dateformat,
    )

    logger = logging.getLogger("qcrawl.test_all")
    logger.debug("Debug message")
    logger.info("Info message")

    content = log_file.read_text()
    assert "DEBUG    | qcrawl.test_all | Debug message" in content
    assert "INFO     | qcrawl.test_all | Info message" in content


def test_setup_logging_level_normalization():
    """Test that setup_logging normalizes level strings."""
    # Test with lowercase
    setup_logging(level="debug")
    logger = logging.getLogger("qcrawl")
    assert logger.level == logging.DEBUG

    # Test with integer
    setup_logging(level=logging.WARNING)
    logger = logging.getLogger("qcrawl")
    assert logger.level == logging.WARNING


# Settings Tests


def test_settings_default_logging_values():
    """Test that Settings has correct default logging values."""
    settings = Settings()

    assert settings.LOG_LEVEL == "INFO"
    assert settings.LOG_FILE is None
    assert settings.LOG_FORMAT == "%(asctime)s %(levelname)s %(name)s: %(message)s"
    assert settings.LOG_DATEFORMAT is None


def test_settings_custom_logging_values():
    """Test Settings with custom logging values."""
    settings = Settings(
        LOG_LEVEL="DEBUG",
        LOG_FILE="/tmp/test.log",
        LOG_FORMAT="[%(levelname)s] %(message)s",
        LOG_DATEFORMAT="%Y-%m-%d %H:%M:%S",
    )

    assert settings.LOG_LEVEL == "DEBUG"
    assert settings.LOG_FILE == "/tmp/test.log"
    assert settings.LOG_FORMAT == "[%(levelname)s] %(message)s"
    assert settings.LOG_DATEFORMAT == "%Y-%m-%d %H:%M:%S"


def test_settings_to_dict_includes_logging():
    """Test that Settings.to_dict() includes logging settings."""
    settings = Settings(
        LOG_LEVEL="WARNING",
        LOG_FORMAT="custom format",
        LOG_DATEFORMAT="%H:%M:%S",
    )

    settings_dict = settings.to_dict()
    assert settings_dict["LOG_LEVEL"] == "WARNING"
    assert settings_dict["LOG_FORMAT"] == "custom format"
    assert settings_dict["LOG_DATEFORMAT"] == "%H:%M:%S"


def test_settings_with_overrides_logging():
    """Test Settings.with_overrides() for logging settings."""
    base = Settings()
    overrides: dict[str, object] = {
        "LOG_LEVEL": "ERROR",
        "LOG_FORMAT": "%(message)s",
        "LOG_DATEFORMAT": "%Y",
    }

    new_settings = base.with_overrides(overrides)

    assert new_settings.LOG_LEVEL == "ERROR"
    assert new_settings.LOG_FORMAT == "%(message)s"
    assert new_settings.LOG_DATEFORMAT == "%Y"
    # Base should be unchanged (immutable)
    assert base.LOG_LEVEL == "INFO"


def test_settings_case_insensitive_logging_keys():
    """Test that logging settings can be set with different cases."""
    base = Settings()

    # Test lowercase
    new1 = base.with_overrides({"log_level": "DEBUG", "log_format": "test"})
    assert new1.LOG_LEVEL == "DEBUG"
    assert new1.LOG_FORMAT == "test"

    # Test mixed case
    new2 = base.with_overrides({"Log_Level": "WARNING", "Log_Format": "test2"})
    assert new2.LOG_LEVEL == "WARNING"
    assert new2.LOG_FORMAT == "test2"


# Settings.load() Tests


def test_settings_load_from_env(monkeypatch):
    """Test loading logging settings from environment variables."""
    monkeypatch.setenv("QCRAWL_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("QCRAWL_LOG_FORMAT", "%(levelname)s: %(message)s")
    monkeypatch.setenv("QCRAWL_LOG_DATEFORMAT", "%Y-%m-%d")

    settings = Settings.load()

    assert settings.LOG_LEVEL == "ERROR"
    assert settings.LOG_FORMAT == "%(levelname)s: %(message)s"
    assert settings.LOG_DATEFORMAT == "%Y-%m-%d"


def test_settings_load_from_toml(tmp_path):
    """Test loading logging settings from TOML config file."""
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
LOG_LEVEL = "WARNING"
LOG_FILE = "/var/log/qcrawl.log"
LOG_FORMAT = "[%(levelname)s] %(name)s: %(message)s"
LOG_DATEFORMAT = "%H:%M:%S"
""")

    settings = Settings.load(config_file=str(config_file))

    assert settings.LOG_LEVEL == "WARNING"
    assert settings.LOG_FILE == "/var/log/qcrawl.log"
    assert settings.LOG_FORMAT == "[%(levelname)s] %(name)s: %(message)s"
    assert settings.LOG_DATEFORMAT == "%H:%M:%S"


def test_settings_load_cli_overrides_env(tmp_path, monkeypatch):
    """Test that CLI overrides take precedence over environment variables."""
    # Set env vars
    monkeypatch.setenv("QCRAWL_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("QCRAWL_LOG_FORMAT", "env format")

    # Load with CLI overrides
    settings = Settings.load(
        log_level="ERROR",
        log_format="cli format",
    )

    # CLI should win
    assert settings.LOG_LEVEL == "ERROR"
    assert settings.LOG_FORMAT == "cli format"


def test_settings_load_priority_order(tmp_path, monkeypatch):
    """Test the full priority order: config file < env < CLI."""
    # Create config file
    config_file = tmp_path / "config.toml"
    config_file.write_text('LOG_LEVEL = "INFO"\nLOG_FORMAT = "file format"')

    # Set env var (should override file)
    monkeypatch.setenv("QCRAWL_LOG_FORMAT", "env format")

    # Load with CLI override (should override all)
    settings = Settings.load(
        config_file=str(config_file),
        log_level="ERROR",  # CLI override
    )

    assert settings.LOG_LEVEL == "ERROR"  # From CLI
    assert settings.LOG_FORMAT == "env format"  # From env (overrides file)


# Integration Tests


def test_cli_uses_settings_for_logging(monkeypatch, tmp_path):
    """Test that CLI properly uses settings for logging configuration."""
    import qcrawl.cli as cli
    from qcrawl.core.spider import Spider

    # Create a minimal spider
    class LocalTestSpider(Spider):
        name = "test"
        start_urls = ["http://example.com"]

        async def parse(self, response):
            if False:
                yield

    # Create config file with custom logging
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
LOG_FORMAT = "[TEST] %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d"
""")

    # Mock sys.argv - explicitly set log-level to override default
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qcrawl",
            "test:TestSpider",
            "--settings-file",
            str(config_file),
            "--log-level",
            "DEBUG",
        ],
    )

    # Track what was passed to setup_logging
    setup_logging_calls = []
    original_setup = cli.setup_logging

    def mock_setup_logging(*args, **kwargs):
        setup_logging_calls.append((args, kwargs))
        # Call original to avoid breaking other things
        original_setup(*args, **kwargs)

    monkeypatch.setattr(cli, "setup_logging", mock_setup_logging)
    monkeypatch.setattr(cli, "load_spider_class", lambda path: LocalTestSpider)
    monkeypatch.setattr(cli, "ensure_output_dir", lambda *a: None)

    # Mock run_async to avoid actually running the spider
    async def fake_run(*args):
        pass

    monkeypatch.setattr(cli, "run_async", fake_run)

    # Mock asyncio.run to properly handle the coroutine
    def mock_asyncio_run(coro):
        # Close the coroutine to avoid the warning
        coro.close()
        return None

    monkeypatch.setattr(cli.asyncio, "run", mock_asyncio_run)

    # Run main
    cli.main()

    # Verify setup_logging was called with correct settings
    assert len(setup_logging_calls) == 1
    args, kwargs = setup_logging_calls[0]
    assert args[0] == "DEBUG"  # LOG_LEVEL from CLI
    assert args[2] == "[TEST] %(message)s"  # LOG_FORMAT from config
    assert args[3] == "%Y-%m-%d"  # LOG_DATEFORMAT from config


def test_logging_format_actually_applied(tmp_path, capfd):
    """End-to-end test that custom format is actually used."""
    # Create settings with custom format
    settings = Settings(
        LOG_LEVEL="INFO",
        LOG_FORMAT="CUSTOM: %(message)s",
    )

    # Setup logging with these settings
    setup_logging(
        settings.LOG_LEVEL,
        settings.LOG_FILE,
        settings.LOG_FORMAT,
        settings.LOG_DATEFORMAT,
    )

    # Log a message
    logger = logging.getLogger("qcrawl.integration_test")
    logger.info("Integration test message")

    # Verify the custom format was used
    captured = capfd.readouterr()
    assert "CUSTOM: Integration test message" in captured.out
    # Should NOT contain the default format elements
    assert "qcrawl.integration_test:" not in captured.out
