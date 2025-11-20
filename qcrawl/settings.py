from __future__ import annotations

import contextlib
import logging
import os
from dataclasses import asdict, dataclass, field
from enum import IntEnum
from pathlib import Path

import orjson
import yaml
from yarl import URL

from qcrawl.middleware.base import DownloaderMiddleware, SpiderMiddleware
from qcrawl.middleware.downloader import (
    CookiesMiddleware,
    DownloadDelayMiddleware,
    HttpCompressionMiddleware,
    RedirectMiddleware,
    RetryMiddleware,
    RobotsTxtMiddleware,
)
from qcrawl.middleware.spider import (
    DepthMiddleware,
    OffsiteMiddleware,
)

logger = logging.getLogger(__name__)


def _map_keys_to_canonical(src: dict[str, object] | None, base_keys: set[str]) -> dict[str, object]:
    """Map incoming keys case-insensitively to canonical dataclass field names.

    - Always compare keys by `.upper()` against `base_keys`.
    - Preserve non-string keys and skip None values.
    """
    if not src:
        return {}
    upper_map = {bk.upper(): bk for bk in base_keys}
    out: dict[str, object] = {}
    for k, v in src.items():
        if v is None:
            continue
        if not isinstance(k, str):
            out[k] = v
            continue
        mapped = upper_map.get(k.upper(), k)
        out[mapped] = v
    return out


class Priority(IntEnum):
    DEFAULT = 0  # lowest priority: library defaults
    CONFIG_FILE = 10  # config file (Settings.load(config_file=...))
    ENV = 20  # environment variables (QCRAWL_*)
    SPIDER = 30  # spider-level custom_settings / instance overrides
    CLI = 40  # CLI (--setting argument)
    EXPLICIT = 100  # highest priority: programmatic / runtime explicit overrides


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings for qCrawl.

    Canonical field names are UPPERCASE. Use Settings.load() to create from file/env/CLI.
    """

    # Queue settings
    QUEUE_BACKEND: str = "memory"
    QUEUE_URL: str | None = None
    QUEUE_KEY: str = "qcrawl:queue"
    QUEUE_MAXSIZE: int | None = None

    # Credentials (masked in repr)
    QUEUE_USERNAME: str | None = field(default=None, repr=False)
    QUEUE_PASSWORD: str | None = field(default=None, repr=False)

    # Spider settings
    CONCURRENCY: int = 10
    CONCURRENCY_PER_DOMAIN: int = 2
    DELAY_PER_DOMAIN: float = 0.25
    MAX_DEPTH: int = 0  # 0 = unlimited
    TIMEOUT: float = 30.0
    MAX_RETRIES: int = 3
    USER_AGENT: str = "qCrawl/1.0"

    # Default headers for outgoing requests
    DEFAULT_REQUEST_HEADERS: dict[str, str] = field(
        default_factory=lambda: {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en",
        }
    )

    # Pipeline settings
    PIPELINES: dict[str, int] | None = None

    # Downloader settings
    DOWNLOADER_SETTINGS: dict[str, int | bool | float] | None = field(
        default_factory=lambda: {
            "max_connections": 200,
            "max_connections_per_host": 10,
            "dns_cache_ttl": 300,
            "enable_cleanup_closed": True,
            "keepalive_timeout": 60.0,
            "force_close_after": 1000,
        }
    )

    # Middleware with priority (higher = earlier)
    DOWNLOADER_MIDDLEWARES: dict[type[DownloaderMiddleware], int] = field(
        default_factory=lambda: {
            HttpCompressionMiddleware: 100,
            RobotsTxtMiddleware: 200,
            DownloadDelayMiddleware: 300,
            RetryMiddleware: 400,
            RedirectMiddleware: 500,
            CookiesMiddleware: 600,
        }
    )

    SPIDER_MIDDLEWARES: dict[type[SpiderMiddleware], int] = field(
        default_factory=lambda: {
            DepthMiddleware: 100,
            OffsiteMiddleware: 200,
        }
    )

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str | None = None

    def __post_init__(self) -> None:
        """Validation only - no loading, no mutations."""
        if self.CONCURRENCY < 1 or self.CONCURRENCY > 10000:
            raise ValueError(f"concurrency must be 1-10000, got {self.CONCURRENCY}")

        if self.CONCURRENCY_PER_DOMAIN < 1:
            raise ValueError(
                f"concurrency_per_domain must be >= 1, got {self.CONCURRENCY_PER_DOMAIN}"
            )
        if self.CONCURRENCY_PER_DOMAIN > self.CONCURRENCY:
            raise ValueError("concurrency_per_domain cannot exceed concurrency")

        if self.TIMEOUT <= 0:
            raise ValueError(f"timeout must be > 0, got {self.TIMEOUT}")

        if self.MAX_RETRIES < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.MAX_RETRIES}")

        if self.QUEUE_MAXSIZE is not None and self.QUEUE_MAXSIZE < 0:
            raise ValueError(f"queue_maxsize must be >= 0, got {self.QUEUE_MAXSIZE}")

        if self.PIPELINES is not None:
            if not isinstance(self.PIPELINES, dict):
                raise TypeError("pipelines must be dict or None")
            for k, v in self.PIPELINES.items():
                if not isinstance(v, int):
                    raise TypeError(f"pipelines[{k}] must be int")

        if self.DEFAULT_REQUEST_HEADERS is not None:
            if not isinstance(self.DEFAULT_REQUEST_HEADERS, dict):
                raise TypeError("DEFAULT_REQUEST_HEADERS must be dict or None")
            for hk, hv in self.DEFAULT_REQUEST_HEADERS.items():
                if not isinstance(hk, str) or not isinstance(hv, str):
                    raise TypeError("DEFAULT_REQUEST_HEADERS keys and values must be str")

        if self.DOWNLOADER_SETTINGS is not None:
            if not isinstance(self.DOWNLOADER_SETTINGS, dict):
                raise TypeError("DOWNLOADER_SETTINGS must be dict or None")

            valid_keys = {
                "max_connections",
                "max_connections_per_host",
                "dns_cache_ttl",
                "enable_cleanup_closed",
                "keepalive_timeout",
                "force_close_after",
            }
            invalid = self.DOWNLOADER_SETTINGS.keys() - valid_keys
            if invalid:
                raise ValueError(f"Invalid DOWNLOADER_SETTINGS keys: {invalid}")

            required = {"max_connections", "max_connections_per_host"}
            missing = required - self.DOWNLOADER_SETTINGS.keys()
            if missing:
                raise ValueError(f"Missing DOWNLOADER_SETTINGS keys: {missing}")

            mc = self.DOWNLOADER_SETTINGS["max_connections"]
            mcph = self.DOWNLOADER_SETTINGS["max_connections_per_host"]
            if not isinstance(mc, int):
                raise TypeError("max_connections must be int")
            if not isinstance(mcph, int):
                raise TypeError("max_connections_per_host must be int")

            if mcph == 0:
                logger.warning("max_connections_per_host=0 allows unlimited per host")

    def get_queue_kwargs(self) -> dict[str, object]:
        """Extract queue-specific kwargs for queue factory."""
        kw: dict[str, object] = {}

        if self.QUEUE_URL:
            kw["url"] = self.QUEUE_URL
        if self.QUEUE_KEY:
            kw["key"] = self.QUEUE_KEY
        if self.QUEUE_MAXSIZE is not None:
            kw["maxsize"] = self.QUEUE_MAXSIZE
        if self.QUEUE_USERNAME:
            kw["username"] = self.QUEUE_USERNAME
        if self.QUEUE_PASSWORD:
            kw["password"] = self.QUEUE_PASSWORD

        return kw

    @classmethod
    def load(cls, config_file: str | None = None, **overrides) -> Settings:
        """Load settings by applying layers onto a validated default Settings instance.

        Layers (low → high):
          - builtin defaults (cls())
          - config file (Priority.CONFIG_FILE)
          - environment (Priority.ENV)
          - explicit overrides passed to this function (Priority.CLI)

        Extract credentials from queue_url after layering and apply them only when
        not provided by explicit overrides.
        """
        # Start from validated defaults
        base = cls()

        base_keys = set(asdict(base).keys())

        # Layer 1: Config file
        if config_file:
            file_conf = cls._load_file(config_file)
            file_conf_mapped = _map_keys_to_canonical(file_conf, base_keys)
            base = base.with_overrides(file_conf_mapped, priority=Priority.CONFIG_FILE)

        # Layer 2: Environment variables
        env_config = cls._load_env()
        env_config_mapped = _map_keys_to_canonical(env_config, base_keys)
        base = base.with_overrides(
            {k: v for k, v in env_config_mapped.items() if v is not None}, priority=Priority.ENV
        )

        # Layer 3: Explicit overrides (CLI / programmatic)
        explicit_raw = {k: v for k, v in overrides.items() if v is not None}
        explicit_conf = _map_keys_to_canonical(explicit_raw, base_keys)
        base = base.with_overrides(explicit_conf, priority=Priority.CLI)

        # Layer 4: Extract credentials from URL if present (apply only when not explicitly provided)
        queue_url = getattr(base, "QUEUE_URL", None)
        if queue_url:
            url_config = cls._extract_credentials_from_url(str(queue_url))
            creds_updates: dict[str, object] = {}
            if url_config.get("username") and "QUEUE_USERNAME" not in explicit_conf:
                creds_updates["QUEUE_USERNAME"] = url_config["username"]
            if url_config.get("password") and "QUEUE_PASSWORD" not in explicit_conf:
                creds_updates["QUEUE_PASSWORD"] = url_config["password"]
            # Always replace queue_url with cleaned form when it differs
            if url_config.get("clean_url") and url_config["clean_url"] != queue_url:
                creds_updates["QUEUE_URL"] = url_config["clean_url"]

            if creds_updates:
                base = base.with_overrides(creds_updates, priority=Priority.CONFIG_FILE)

        return base

    @staticmethod
    def _load_file(path: str) -> dict[str, object]:
        """Load config from YAML or JSON file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        text = p.read_text(encoding="utf-8")

        if p.suffix.lower() in {".yaml", ".yml"}:
            data = yaml.safe_load(text) or {}
        else:
            data = orjson.loads(text or "{}")

        if not isinstance(data, dict):
            raise TypeError(f"Config file must contain a dict, got {type(data)}")

        return data

    @staticmethod
    def _load_env() -> dict[str, object]:
        """Load settings from environment variables (namespaced only)."""
        config: dict[str, object] = {}

        # Queue settings
        if backend := os.getenv("QCRAWL_QUEUE_BACKEND"):
            config["QUEUE_BACKEND"] = backend

        if url := os.getenv("QCRAWL_QUEUE_URL"):
            config["QUEUE_URL"] = url

        if key := os.getenv("QCRAWL_QUEUE_KEY"):
            config["QUEUE_KEY"] = key

        if user := os.getenv("QCRAWL_QUEUE_USER"):
            config["QUEUE_USERNAME"] = user

        if pwd := os.getenv("QCRAWL_QUEUE_PASS"):
            config["QUEUE_PASSWORD"] = pwd

        # Spider settings
        if val := os.getenv("QCRAWL_CONCURRENCY"):
            with contextlib.suppress(ValueError):
                config["CONCURRENCY"] = int(val)

        if val := os.getenv("QCRAWL_CONCURRENCY_PER_DOMAIN"):
            with contextlib.suppress(ValueError):
                config["CONCURRENCY_PER_DOMAIN"] = int(val)

        if val := os.getenv("QCRAWL_DELAY_PER_DOMAIN"):
            with contextlib.suppress(ValueError):
                config["DELAY_PER_DOMAIN"] = float(val)

        if val := os.getenv("QCRAWL_MAX_DEPTH"):
            with contextlib.suppress(ValueError):
                config["MAX_DEPTH"] = int(val)

        if val := os.getenv("QCRAWL_TIMEOUT"):
            with contextlib.suppress(ValueError):
                config["TIMEOUT"] = float(val)

        if val := os.getenv("QCRAWL_MAX_RETRIES"):
            with contextlib.suppress(ValueError):
                config["MAX_RETRIES"] = int(val)

        if val := os.getenv("QCRAWL_QUEUE_MAXSIZE"):
            with contextlib.suppress(ValueError):
                config["QUEUE_MAXSIZE"] = int(val)

        if val := os.getenv("QCRAWL_LOG_LEVEL"):
            config["LOG_LEVEL"] = val

        if val := os.getenv("QCRAWL_LOG_FILE"):
            config["LOG_FILE"] = val

        return config

    @staticmethod
    def _extract_credentials_from_url(url: str) -> dict[str, str | None]:
        """Extract username/password from URL and return clean URL."""
        try:
            parsed = URL(url)

            username = parsed.user
            password = parsed.password

            # Build clean URL without credentials
            scheme = parsed.scheme or ""
            host = parsed.host or ""
            port = f":{parsed.port}" if parsed.port else ""
            path = str(parsed.path) or ""
            query = f"?{parsed.query_string}" if parsed.query_string else ""
            fragment = f"#{parsed.fragment}" if parsed.fragment else ""

            clean_url = f"{scheme}://{host}{port}{path}{query}{fragment}"

            return {"username": username, "password": password, "clean_url": clean_url}
        except Exception:
            # Parse failed, return original
            return {"username": None, "password": None, "clean_url": url}

    def to_dict(self) -> dict[str, object]:
        """Serializable snapshot with masked secrets (legacy lowercase keys)."""
        return {
            "queue_backend": self.QUEUE_BACKEND,
            "queue_url": self.QUEUE_URL,
            "queue_key": self.QUEUE_KEY,
            "queue_username": self.QUEUE_USERNAME,
            "queue_password": "*****" if self.QUEUE_PASSWORD else None,
            "concurrency": self.CONCURRENCY,
            "concurrency_per_domain": self.CONCURRENCY_PER_DOMAIN,
            "delay_per_domain": self.DELAY_PER_DOMAIN,
            "max_depth": self.MAX_DEPTH,
            "timeout": self.TIMEOUT,
            "max_retries": self.MAX_RETRIES,
            "log_level": self.LOG_LEVEL,
        }

    def to_json(self) -> bytes:
        """Fast JSON serialization using orjson (returns bytes)."""
        return bytes(
            orjson.dumps(
                self.to_dict(),
                option=orjson.OPT_INDENT_2,
            )
        )

    def with_overrides(
        self, overrides: dict[str, object] | None, *, priority: Priority | None = None
    ) -> Settings:
        """Return a new Settings instance with values from `overrides` applied.

        - Does not mutate `self`.
        - Unknown keys are ignored with a warning.
        - If both current value and override are dicts, they are merged shallowly.
        - Constructor is called to reuse existing validation in __post_init__.
        - Accepts overrides case-insensitively by mapping to canonical UPPERCASE names.
        """
        if not overrides:
            return self

        base = asdict(self)
        merged = dict(base)

        mapped = _map_keys_to_canonical(overrides, set(base.keys()))

        for k, v in mapped.items():
            if k not in base:
                if priority:
                    logger.warning(
                        "Ignoring unknown setting %r in overrides (source=%s)", k, priority
                    )
                else:
                    logger.warning("Ignoring unknown setting %r in overrides", k)
                continue

            cur = base[k]
            if isinstance(cur, dict) and isinstance(v, dict):
                new = dict(cur)
                new.update(v)
                merged[k] = new
            else:
                merged[k] = v

        try:
            return type(self)(**merged)
        except Exception:
            if priority:
                logger.exception(
                    "Failed to create Settings from overrides (source=%s); returning original Settings",
                    priority,
                )
            else:
                logger.exception(
                    "Failed to create Settings from overrides; returning original Settings"
                )
            return self
