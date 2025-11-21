from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from enum import IntEnum

import orjson

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
from qcrawl.utils.settings import (
    ensure_int,
    ensure_str,
    load_config_file,
    load_env,
    map_keys_to_canonical,
    mask_secrets,
    shallow_merge_dicts,
)

logger = logging.getLogger(__name__)


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

    QUEUE_BACKENDS: dict[str, dict[str, int | bool | str | None]] = field(
        default_factory=lambda: {
            "memory": {
                "class": "qcrawl.core.queues.memory.MemoryPriorityQueue",
                "maxsize": 0,
            },
            "redis": {
                "class": "qcrawl.core.queues.redis.RedisQueue",
                "maxsize": 0,
                "url": None,
                "host": "localhost",
                "port": "6379",
                "user": "user",
                "password": "pass",
                "namespace": "qcrawl",
                "ssl": False,
                "dedupe": False,
                "update_priority": False,
                "fingerprint_size": 16,
                "item_ttl": 86_400,
                "dedupe_ttl": 604_800,
                "max_orphan_retries": 10,
            },
        }
    )
    QUEUE_BACKEND: str = "memory"

    LOG_LEVEL: str = "INFO"
    LOG_FILE: str | None = None

    def __post_init__(self) -> None:
        """Validation only - no loading, no mutations."""
        # Numeric ranges
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

        # PIPELINES
        if self.PIPELINES is not None:
            if not isinstance(self.PIPELINES, dict):
                raise TypeError("pipelines must be dict or None")
            for k, v in self.PIPELINES.items():
                if not isinstance(v, int):
                    raise TypeError(f"pipelines[{k}] must be int")

        # DEFAULT_REQUEST_HEADERS keys/values must be str
        if self.DEFAULT_REQUEST_HEADERS is not None:
            if not isinstance(self.DEFAULT_REQUEST_HEADERS, dict):
                raise TypeError("DEFAULT_REQUEST_HEADERS must be dict or None")
            for hk, hv in self.DEFAULT_REQUEST_HEADERS.items():
                # use ensure_str to validate/coerce for safety (will raise on invalid)
                ensure_str(hk, "DEFAULT_REQUEST_HEADERS key")
                ensure_str(hv, "DEFAULT_REQUEST_HEADERS value")

        # DOWNLOADER_SETTINGS validation and keys
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
            invalid = set(self.DOWNLOADER_SETTINGS.keys()) - valid_keys
            if invalid:
                raise ValueError(f"Invalid DOWNLOADER_SETTINGS keys: {invalid}")

            required = {"max_connections", "max_connections_per_host"}
            missing = required - self.DOWNLOADER_SETTINGS.keys()
            if missing:
                raise ValueError(f"Missing DOWNLOADER_SETTINGS keys: {missing}")

            mc = self.DOWNLOADER_SETTINGS["max_connections"]
            mcph = self.DOWNLOADER_SETTINGS["max_connections_per_host"]

            # Use ensure_int to validate numeric types (will raise on invalid)
            ensure_int(mc, "max_connections")
            ensure_int(mcph, "max_connections_per_host")

            if mcph == 0:
                logger.warning("max_connections_per_host=0 allows unlimited per host")

    @classmethod
    def load(cls, config_file: str | None = None, **overrides) -> Settings:
        """Load settings by applying layers onto a validated default Settings instance.

        Layers (low → high):
          - builtin defaults (cls())
          - config file (Priority.CONFIG_FILE)
          - environment (Priority.ENV)
          - explicit overrides passed to this function (Priority.CLI)
        """
        base = cls()

        if config_file:
            file_conf = load_config_file(config_file)
            if not isinstance(file_conf, dict):
                raise TypeError("Config file must yield a dict")
            base = base.with_overrides(file_conf, priority=Priority.CONFIG_FILE)

        env_conf = load_env()
        if env_conf:
            base = base.with_overrides(env_conf, priority=Priority.ENV)

        explicit = {k: v for k, v in overrides.items() if v is not None}
        if explicit:
            base = base.with_overrides(explicit, priority=Priority.CLI)

        return base

    def to_dict(self) -> dict[str, object]:
        """Serializable snapshot using canonical UPPERCASE keys.

        Secrets inside `QUEUE_BACKENDS` are masked via `mask_secrets`.
        """
        qb: dict[str, object] = {}
        for name, cfg in (self.QUEUE_BACKENDS or {}).items():
            if not isinstance(cfg, dict):
                qb[name] = cfg
                continue
            qb[name] = mask_secrets(cfg)

        return {
            "QUEUE_BACKEND": self.QUEUE_BACKEND,
            "QUEUE_BACKENDS": qb,
            "CONCURRENCY": self.CONCURRENCY,
            "CONCURRENCY_PER_DOMAIN": self.CONCURRENCY_PER_DOMAIN,
            "DELAY_PER_DOMAIN": self.DELAY_PER_DOMAIN,
            "MAX_DEPTH": self.MAX_DEPTH,
            "TIMEOUT": self.TIMEOUT,
            "MAX_RETRIES": self.MAX_RETRIES,
            "LOG_LEVEL": self.LOG_LEVEL,
            "LOG_FILE": self.LOG_FILE,
        }

    def to_json(self) -> bytes:
        return bytes(orjson.dumps(self.to_dict(), option=orjson.OPT_INDENT_2))

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

        mapped = map_keys_to_canonical(overrides, set(base.keys()))

        # Warn on unknown keys and collect only known ones for application
        known_applied: dict[str, object] = {}
        for k, v in mapped.items():
            if k not in base:
                if priority:
                    logger.warning(
                        "Ignoring unknown setting %r in overrides (source=%s)", k, priority
                    )
                else:
                    logger.warning("Ignoring unknown setting %r in overrides", k)
                continue
            known_applied[k] = v

        # Shallow-merge dict-valued settings using helper
        merged = shallow_merge_dicts(base, known_applied)

        try:
            return type(self)(**merged)  # type: ignore[arg-type]
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
