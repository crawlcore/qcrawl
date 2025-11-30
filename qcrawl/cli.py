import argparse
import asyncio
import importlib
import logging
import os
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType, SimpleNamespace

import orjson

from qcrawl.core.spider import Spider
from qcrawl.runner import ensure_output_dir, run_async, setup_logging
from qcrawl.settings import Settings as RuntimeSettings
from qcrawl.utils.settings import parse_literal


def main() -> None:
    """CLI entrypoint for `qcrawl`.

    Responsibilities:
        - Parse CLI arguments.
        - Configure logging and output directories.
        - Load spider config from a TOML file (if provided).
        - Build runtime `Settings` snapshot and import the spider class.
        - Run the crawl using the async runner.

    Exits with SystemExit on fatal errors.
    """
    args = parse_args()

    # Load runtime settings early to get logging configuration
    runtime_settings = RuntimeSettings.load(
        config_file=args.settings_file, log_level=args.log_level, log_file=args.log_file
    )

    # Setup logging with format from settings
    setup_logging(
        runtime_settings.LOG_LEVEL,
        runtime_settings.LOG_FILE,
        runtime_settings.LOG_FORMAT,
        runtime_settings.LOG_DATEFORMAT,
    )
    ensure_output_dir(args.export)

    settings = SpiderConfig()
    if args.settings_file:
        try:
            settings = SpiderConfig.from_file(args.settings_file)
        except Exception as e:
            logging.error("Failed to load settings file %s: %s", args.settings_file, e)
            raise SystemExit(2) from e
    settings.merge_cli(args)

    try:
        spider_cls = load_spider_class(args.spider)
    except Exception as e:
        logging.error("Failed to load spider %s: %s", args.spider, e)
        raise SystemExit(2) from e

    try:
        spider_settings_ns = SimpleNamespace(spider_args=settings.spider_args)
        asyncio.run(run_async(spider_cls, args, spider_settings_ns, runtime_settings))
    except KeyboardInterrupt:
        print("\nInterrupted, exiting...")
        raise SystemExit(130) from None


@dataclass
class SpiderConfig:
    """Simple container for per-spider configuration loaded from file or CLI.

    Fields:
      - spider_args: dict of constructor args / spider attributes to set.
      - concurrency, concurrency_per_domain, delay_per_domain, max_depth: optional runtime hints.

    Use `from_file()` to load configuration from a TOML file and `merge_cli()` to apply CLI-supplied overrides.
    """

    spider_args: dict[str, object] = field(default_factory=dict)
    concurrency: int | None = None
    concurrency_per_domain: int | None = None
    delay_per_domain: float | None = None
    max_depth: int | None = None

    @classmethod
    def from_file(cls, path: str) -> "SpiderConfig":
        """Load spider configuration from a TOML file.

        The file must be a TOML document with a top-level mapping (suffix `.toml`).

        Returns:
            SpiderConfig instance populated from file data.

        Raises:
            FileNotFoundError if the file does not exist.
            ValueError if the file is not a TOML file.
            Any parsing exceptions from `tomllib`.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Settings file not found: {path}")
        if p.suffix.lower() != ".toml":
            raise ValueError("Settings file must be a TOML file with a .toml suffix")
        text = p.read_text(encoding="utf-8")
        data = tomllib.loads(text) or {}
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "SpiderConfig":
        """Create SpiderConfig from a plain mapping.

        Performs permissive numeric coercion for concurrency/time values.
        """
        spider_args = data.get("spider_args", {})
        if not isinstance(spider_args, dict):
            spider_args = {}

        def to_int(val) -> int | None:
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                return int(val)
            return None

        def to_float(val) -> float | None:
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                return float(val)
            return None

        return cls(
            spider_args=spider_args,
            concurrency=to_int(data.get("concurrency")),
            concurrency_per_domain=to_int(data.get("concurrency_per_domain")),
            delay_per_domain=to_float(data.get("delay_per_domain")),
            max_depth=to_int(data.get("max_depth")),
        )

    def merge_cli(self, args: argparse.Namespace) -> None:
        """Merge CLI `--setting` pairs and explicit CLI flags into this config.

        CLI `-s KEY=VALUE` entries are appended to `spider_args`. Explicit flags
        like `--concurrency` override the corresponding attribute and are also
        copied into `spider_args`.
        """
        for key, value in getattr(args, "setting", []):
            self.spider_args[key] = value
        for attr in ("concurrency", "concurrency_per_domain", "delay_per_domain", "max_depth"):
            val = getattr(args, attr, None)
            if val is not None:
                setattr(self, attr, val)
                self.spider_args[attr] = val


class KeyValueListAction(argparse.Action):
    """Argparse action that accumulates `KEY=VALUE` pairs into a list.

    Stores a list of `(key, value)` tuples on the destination attribute.
    Values are parsed via the private `_parse_kv` helper.
    """

    @staticmethod
    def _parse_kv(s: str) -> tuple[str, object]:
        """Parse a single KEY=VALUE string used by the `--setting` option.

        Behaviour:
          - KEY and VALUE are split at the first '='.
          - If VALUE starts with '{' or '[', attempt JSON parse (orjson).
          - Otherwise use `parse_literal` to coerce booleans/numbers/None/strings.

        Raises:
          argparse.ArgumentTypeError on malformed input.
        """
        if "=" not in s:
            raise argparse.ArgumentTypeError("must be KEY=VALUE")
        key, val = s.split("=", 1)
        key = key.strip()
        raw = val.strip()

        if raw.startswith("{") or raw.startswith("["):
            try:
                return key, orjson.loads(raw)
            except Exception:
                pass
        return key, parse_literal(raw)

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is None:
            setattr(namespace, self.dest, [])
        target: list[tuple[str, object]] = getattr(namespace, self.dest)
        if not isinstance(values, str):
            raise argparse.ArgumentTypeError("Invalid setting value")
        try:
            pair = self._parse_kv(values)
        except Exception as e:
            raise argparse.ArgumentError(self, f"Invalid setting {values!r}: {e}") from e
        target.append(pair)


def load_spider_class(path: str) -> type[Spider]:
    """Import and return a Spider class given a dotted/path string.

    Accepted formats:
      - module:Class (preferred)
      - module.Class
      - module (module must export a Spider subclass named `Spider`)

    Automatically adds CWD to sys.path to allow imports without PYTHONPATH manipulation.

    Raises:
      ImportError / TypeError on failure.
    """
    # Add CWD to sys.path if not already present
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    # Parse module and class name
    if ":" in path:
        mod_name, cls_name = path.split(":", 1)
    elif "." in path:
        mod_name, cls_name = path.rsplit(".", 1)
    else:
        mod_name, cls_name = path, "Spider"

    module: ModuleType = importlib.import_module(mod_name)
    cls = getattr(module, cls_name, None)
    if cls is None:
        raise ImportError(f"Module {mod_name!r} has no attribute {cls_name!r}")
    if not isinstance(cls, type) or not issubclass(cls, Spider):
        raise TypeError(f"{cls_name!r} is not a subclass of Spider")
    return cls


def parse_args() -> argparse.Namespace:
    """Construct and parse the command-line arguments for the CLI.

    Returns:
      argparse.Namespace with parsed values.
    """
    parser = argparse.ArgumentParser(
        description="Run a qcrawl Spider", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    g_spider = parser.add_argument_group("Spider & Crawl Settings")
    g_spider.add_argument(
        "spider", help="Spider path: module:Class, module.Class, or module (exports Spider)"
    )
    g_spider.add_argument(
        "--setting",
        "-s",
        action=KeyValueListAction,
        default=[],
        help="Set spider setting / constructor arg: KEY=VALUE (use multiple -s to set multiple values)",
    )

    g_output = parser.add_argument_group("Output & Export")
    g_output.add_argument(
        "--export", help="Export destination (defaults to stdout; use local path or '-'/'stdout')"
    )
    g_output.add_argument(
        "--export-format", default="ndjson", choices=["ndjson", "json", "csv", "xml"]
    )
    g_output.add_argument("--export-mode", default="buffered", choices=["buffered", "stream"])
    g_output.add_argument("--export-buffer-size", type=int, default=500)

    g_config = parser.add_argument_group("Configuration")
    g_config.add_argument(
        "--settings-file",
        help="Load settings from TOML (applies to runtime Settings and spider config).",
    )

    g_log = parser.add_argument_group("Logging & Debugging")
    g_log.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    )
    g_log.add_argument("--log-file", help="Log to file")

    g_help = parser.add_argument_group("Help & Version")
    from qcrawl import __version__

    g_help.add_argument("--version", action="version", version=f"qcrawl {__version__}")

    return parser.parse_args()
