

!!! quote

    “You don’t learn to walk by following rules. You learn by doing, and by falling over.”
    — Richard Branson

My idea is to create a dedicated repository with various example spiders covering different use-cases.<br>
While I work on that, here are some general example to get you started.

## Common spider

```python title="quotes_css_spider.py"
from cssselect import SelectorError
from qcrawl.core.spider import Spider
from qcrawl.core.item import Item

class QuotesCssSpider(Spider):
    name = "quotes_css"
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "REQUIRED_FIELDS": ["text", "author"],  # used by `ValidationPipeline` if enabled
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html",
            "User-Agent": "qCrawl-Examples/1.0",
        },
        "max_depth": 5,  # spider-level override for `DepthLimitMiddleware`
         "pipelines": {
            "qcrawl.pipelines.validation.ValidationPipeline": 200,
        },
    }

    async def parse(self, response):
        rv = self.response_view(response)

        # Safe cssselect usage: catch SelectorError (unsupported/invalid selectors)
        try:
            quotes = rv.doc.cssselect("div.quote")
        except SelectorError:
            # Skip page gracefully (engine will not treat as an error)
            return

        for q in quotes:
            try:
                text = q.cssselect("span.text")[0].text_content().strip()
                author = q.cssselect("small.author")[0].text_content().strip()
                tags = [t.text_content().strip() for t in q.cssselect("div.tags a.tag")]
            except (IndexError, SelectorError):
                # Missing node or inner selector problem — skip this item
                continue

            yield Item(data={"url": response.url, "text": text, "author": author, "tags": tags})

        # Follow pagination (use rv.follow to resolve correctly)
        next_a = rv.doc.cssselect("li.next a")
        if next_a:
            href = next_a[0].get("href")
            if href:
                yield rv.follow(href)
```

For settings setup including `custom_settings`, see [Settings documentation](../concepts/settings.md).
<br>

## Example pipeline

```python title="enrich_pipeline.py"
from datetime import datetime, timezone
from qcrawl.pipelines.base import ItemPipeline, DropItem

class EnrichPipeline(ItemPipeline):
    """Add scrape metadata and simple normalization to items."""

    async def process_item(self, item, spider):
        # Basic shape validation
        if not hasattr(item, "data") or not isinstance(item.data, dict):
            raise DropItem("invalid item shape")

        # Add scrape timestamp if missing
        item.data.setdefault("scraped_at", datetime.now(timezone.utc).isoformat())

        # Ensure URL exists (many pipelines rely on a stable 'url' field)
        if "url" not in item.data or not item.data["url"]:
            raise DropItem("missing url")

        return item
```

For pipeline setup, see [Item Pipeline documentation](../concepts/item_pipeline.md).
