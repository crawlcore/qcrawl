from __future__ import annotations

import argparse
import asyncio
import typing
from types import SimpleNamespace
from typing import cast

from qcrawl.runner.engine import run as run_async
from qcrawl.runner.logging import ensure_output_dir, setup_logging
from qcrawl.settings import Settings as RuntimeSettings

if typing.TYPE_CHECKING:
    from qcrawl.core import Spider


class SpiderRunner:
    def __init__(self, settings: dict[str, object] | None = None) -> None:
        self._raw: dict[str, object] = dict(settings or {})

        # logging (cast to expected types for type checkers)
        self.log_level = cast(str | int, self._raw.get("log_level", "INFO"))
        self.log_file = cast(str | None, self._raw.get("log_file"))
        setup_logging(self.log_level, self.log_file)

        # ensure export dir when provided
        ensure_output_dir(cast(str | None, self._raw.get("export")))

        cfg_file = cast(str | None, self._raw.get("settings_file"))

        # runner-only keys to skip when building runtime Settings overrides (UPPERCASE for case-insensitive compare)
        SKIP_KEYS = {
            "LOG_LEVEL",
            "LOG_FILE",
            "SETTING",
            "SETTINGS_FILE",
            "EXPORT",
            "EXPORT_FORMAT",
            "EXPORT_MODE",
            "EXPORT_BUFFER_SIZE",
        }

        # Keep backwards-tolerant filtering for keys provided in any case.
        overrides = {
            k: v
            for k, v in self._raw.items()
            if not (isinstance(k, str) and k.upper() in SKIP_KEYS)
        }

        # create immutable runtime settings snapshot
        self.runtime_settings = RuntimeSettings.load(
            config_file=cfg_file, log_level=self.log_level, log_file=self.log_file, **overrides
        )

    async def crawl(
        self,
        spider_cls: type[Spider],
        **spider_kwargs: object | None,
    ) -> None:
        """Async entrypoint: await this from an existing event loop."""
        args = argparse.Namespace(
            export=self._raw.get("export"),
            export_format=self._raw.get("export_format"),
            export_mode=self._raw.get("export_mode"),
            export_buffer_size=self._raw.get("export_buffer_size"),
            setting=self._raw.get("setting", []),
            settings_file=self._raw.get("settings_file"),
            log_level=self.log_level,
            log_file=self.log_file,
        )

        spider_settings = SimpleNamespace(spider_args=spider_kwargs)

        # Await the shared async runner directly (no asyncio.run here).
        await run_async(spider_cls, args, spider_settings, self.runtime_settings)

    def crawl_sync(
        self,
        spider_cls: type[Spider],
        **spider_kwargs: object | None,
    ) -> None:
        """Synchronous convenience wrapper. Use only from non-async code.

        Raises:
            RuntimeError: if called from inside a running event loop.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            raise RuntimeError(
                "Event loop is already running; call `await SpiderRunner.crawl(...)` instead of `crawl_sync(...)`"
            )

        asyncio.run(self.crawl(spider_cls, **spider_kwargs))
