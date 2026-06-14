from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Callable

import yarl
from lxml import html

from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.response import Page


class Spider(ABC):
    """Abstract base class for user spiders.

    Class attributes:
      - name (str): spider identifier (required)
      - start_urls (list[str]): initial seed URLs (required)
      - allowed_domains (list[str] | None): optional domain whitelist
      - custom_settings (dict[str, object] | None): per-spider class-level settings.
    """

    name: str
    start_urls: list[str]
    allowed_domains: list[str] | None = None
    custom_settings: dict[str, object] = {}

    def __init__(self) -> None:
        if not getattr(self, "name", None) or not isinstance(self.name, str):
            raise TypeError("Spider must define a non-empty `name: str` class attribute")
        if not getattr(self, "start_urls", None) or not isinstance(self.start_urls, list):
            raise TypeError(
                "Spider must define a non-empty `start_urls: list[str]` class attribute"
            )

        self.engine = None
        self.crawler = None
        self.signals = None

        self.runtime_settings = None

    @abstractmethod
    async def parse(self, response: Page) -> AsyncGenerator[Item | str | Request, None]:
        """Parse a response and yield Items, Requests or URL strings."""
        raise NotImplementedError

    async def start_requests(self) -> AsyncGenerator[Request, None]:
        """Produce initial Request stream for the spider using spider defaults.

        Do NOT inject headers here; downloader/middlewares assemble request headers.
        """
        for url in self.start_urls:
            yield Request(url=url, priority=0, meta={"depth": 0})

    async def open_spider(self, engine) -> None:
        """Convenience lifecycle hook called when the spider is opened.

        Contract:
          - `engine` is the CrawlEngine instance.
          - Do NOT reassign `self.signals` here (sender identity must remain stable).
        Default behaviour attaches engine/crawler and reads the crawler-provided runtime_settings.
        """
        self.engine = engine
        self.crawler = getattr(engine, "crawler", None)
        if self.crawler is not None:
            with contextlib.suppress(Exception):
                self.runtime_settings = getattr(self.crawler, "runtime_settings", None)
        return None

    async def close_spider(self, engine, reason: str | None = None) -> None:
        """Lifecycle hook called when spider is closed. Override to release resources."""
        return None

    def response_view(self, response: Page) -> ResponseView:
        """Return a ResponseView bound to this spider (enables rv.follow to apply spider defaults)."""
        return ResponseView(response, spider=self)

    def follow(
        self,
        response: Page,
        href: str,
        priority: int = 0,
        meta: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        callback: Callable[..., object] | str | None = None,
        cb_kwargs: dict[str, object] | None = None,
        json: object | None = None,
    ) -> Request:
        """Convenience wrapper to create a Request resolved against `response` using spider defaults."""
        return self.response_view(response).follow(
            href,
            priority=priority,
            meta=meta,
            headers=headers,
            callback=callback,
            cb_kwargs=cb_kwargs,
            json=json,
        )


class ResponseView:
    """Lightweight response wrapper exposing lxml with crawler helpers."""

    __slots__ = ("response", "spider", "_doc")
    _doc: html.HtmlElement | None

    def __init__(self, response: Page, spider: Spider) -> None:
        self.response = response
        self.spider = spider
        self._doc = None

    @property
    def doc(self):
        """Lazy-loaded lxml document tree."""
        if self._doc is None:
            self._doc = html.fromstring(self.response.content)
        return self._doc

    def follow(
        self,
        href: str,
        priority: int = 0,
        meta: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        callback: Callable[..., object] | str | None = None,
        cb_kwargs: dict[str, object] | None = None,
        json: object | None = None,
    ) -> Request:
        """Resolve URL and create Request (no parsing needed)."""
        try:
            base = yarl.URL(self.response.url)
            abs_url = str(base.join(yarl.URL(href)))
        except Exception:
            try:
                abs_url = str(yarl.URL(href))
            except Exception:
                abs_url = self.response.url.rstrip("/") + "/" + href.lstrip("/")

        return Request(
            url=abs_url,
            priority=priority,
            meta=dict(meta) if meta is not None else {},
            headers=dict(headers) if headers else {},
            callback=callback,
            cb_kwargs=dict(cb_kwargs) if cb_kwargs else {},
            json=json,
        )

    def urljoin(self, url: str) -> str:
        """Resolve a relative URL (no parsing needed)."""
        try:
            base = yarl.URL(self.response.url)
            return str(base.join(yarl.URL(url)))
        except Exception:
            try:
                return str(yarl.URL(url))
            except Exception:
                return self.response.url.rstrip("/") + "/" + url.lstrip("/")
