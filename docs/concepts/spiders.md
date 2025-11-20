
A *Spider* is a class which defines what to crawl (a site or a group of sites), how to perform
the crawl (i.e. follow links) and how to extract structured data from the pages (i.e. scraping items).

## Basic concepts

qCrawl spiders should subclass `qcrawl.core.spider.Spider`, override the async `parse()` method, and define the `name` and `start_urls` attributes.

Required class attributes:

- `name`: str — unique spider identifier.
- `start_urls`: list[str] — initial URLs to crawl.

`parse(response)` — async generator that yields:

- [`Item`](items.md) (container for scraped fields and internal metadata) or plain dict (engine wraps into `Item`).
- `Request` (data class representing an HTTP crawl request).
- `str` URL (engine or middlewares convert to `Request`).

The `parse()` method is the main callback that processes downloaded pages.
It receives a `Page` object representing the HTTP response, and yields `Item` objects, `Request` objects, or string URLs.


Simple example spider that uses css selectors and yields `Item`:
```python
from datetime import datetime, timezone
from lxml import html

from qcrawl.core.spider import Spider, ResponseView
from qcrawl.core.response import Page
from qcrawl.core.item import Item


class QuotesSpider(Spider):
    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response: Page):
        rv = self.response_view(response)

        for q in rv.doc.cssselect(".quote"):
            text_nodes = q.cssselect("span.text")
            author_nodes = q.cssselect("small.author")
            if not text_nodes or not author_nodes:
                continue

            text = text_nodes[0].text_content().strip()
            author = author_nodes[0].text_content().strip()

            ts = datetime.now(timezone.utc).isoformat()

            yield Item(
                data={"text": text, "author": author},
                metadata={"scraped_at": ts})

        next_link = rv.doc.cssselect("li.next a")
        if next_link:
            href = next_link[0].get("href")
            if href:
                yield self.follow(response, href)
```

## Scraping lifecycle

``` mermaid
flowchart LR
  Spider -->|"yield Request / URL / Item"| Scheduler
  Scheduler -->|"next Request"| Engine
  Engine -->|"fetch"| Downloader
  Downloader -->|"Response"| Engine
  Engine -->|"call parse(response)"| Spider

  Spider -.->|"yield Item"| Export@{ shape: bow-rect, label: "Export process" }

  %% Styles
    style Export fill:#333,color:#fff,stroke:#777,stroke-width:2px
```

The simplified scraping cycle works as follows:

1. You generate the initial requests to crawl the first URLs, along with a callback function to handle the downloaded responses. These requests come from the `start_requests()` method of your spider, which by default yields a Request for each URL in the `start_urls: list[str]`, using `parse()` as the default callback.
2. Each request is placed in the scheduler’s queue. The engine pulls the next request, sends it to the downloader, and waits for the response. Once downloaded, the response is passed back to the engine.
3. The engine calls the callback function specified in the request — typically `parse(response)`. Inside this method, you parse the page content (using CSS selectors, XPath) and yield either `Item` objects containing extracted data or new `Request` objects for additional URLs to crawl.
4. Any yielded `Request` / `URL` / `Item` object are returned to the scheduler, enqueued, and processed in the same way — forming a continuous loop until no more requests remain.
5. Any yielded `Item` objects are sent to the export process: [item pipelines](item_pipeline.md) (drop, transform), [exporters](exporters.md) (data formating), and storage backends (save data).
