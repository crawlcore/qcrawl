

!!! quote

    “You don’t learn to walk by following rules. You learn by doing, and by falling over.”
    — Richard Branson

My idea is to create a dedicated repository with various example spiders covering different use-cases.<br>
While I work on that, here are some general examples to get you started.

## CSS selectors + custom settings + middleware configuration

```python title="quotes_css_spider.py"
import logging

from cssselect import SelectorError

from qcrawl.core.item import Item
from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class Quotes(Spider):
    name = "quotes_css"
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "CONCURRENCY": 10,
        "CONCURRENCY_PER_DOMAIN": 10,
        "DELAY_PER_DOMAIN": 0.25,
        "MAX_DEPTH": 0,  # unlimited depth
        "TIMEOUT": 30.0,
        "MAX_RETRIES": 3,
        "USER_AGENT": "qCrawl-Examples/1.0",
        "REQUIRED_FIELDS": ["text", "author"],
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html",
            "User-Agent": "qCrawl-Examples/1.0",
        },
        "PIPELINES": {
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

            yield Item(
                data={"url": response.url, "text": text, "author": author, "tags": tags},
                metadata={"source": "quotes.toscrape.com"},
            )

        # Follow pagination (use rv.follow to resolve correctly)
        next_a = rv.doc.cssselect("li.next a")
        if next_a:
            href = next_a[0].get("href")
            if href:
                yield rv.follow(href)
```

For settings setup including `custom_settings`, see [Settings documentation](../concepts/settings.md).

## XPath selectors

```python title="quotes_xpath_spider.py"
import logging

from qcrawl.core.item import Item
from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class QuotesXPath(Spider):
    name = "quotes_xpath"
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "CONCURRENCY": 10,
        "USER_AGENT": "qCrawl-XPath-Example/1.0",
    }

    async def parse(self, response):
        rv = self.response_view(response)

        # Using XPath selectors instead of CSS
        quotes = rv.doc.xpath("//div[@class='quote']")

        for q in quotes:
            # XPath can extract text directly
            text_nodes = q.xpath(".//span[@class='text']/text()")
            author_nodes = q.xpath(".//small[@class='author']/text()")
            tag_nodes = q.xpath(".//div[@class='tags']/a[@class='tag']/text()")

            # Safely extract values
            if not text_nodes or not author_nodes:
                continue

            text = text_nodes[0].strip()
            author = author_nodes[0].strip()
            tags = [t.strip() for t in tag_nodes]

            yield Item(
                data={"url": response.url, "text": text, "author": author, "tags": tags},
                metadata={"selector_type": "xpath"},
            )

        # Follow pagination using XPath
        next_link = rv.doc.xpath("//li[@class='next']/a/@href")
        if next_link:
            yield rv.follow(next_link[0])
```

**XPath vs CSS Selectors:**

- **XPath**: `//div[@class='quote']` - More powerful, can navigate parent/sibling nodes
- **CSS**: `div.quote` - Simpler syntax, better browser DevTools support


## API / JSON scraping

```python title="api_spider.py"
import logging

from qcrawl.core.item import Item
from qcrawl.core.request import Request
from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class JSONApiSpider(Spider):
    name = "json_api"
    start_urls = ["https://api.github.com/users/github/repos?page=1&per_page=10"]

    custom_settings = {
        "CONCURRENCY": 5,
        "USER_AGENT": "qCrawl-API-Example/1.0",
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json",
        },
    }

    async def parse(self, response):
        # Check if response is JSON
        if not response.headers.get("content-type", "").startswith("application/json"):
            logger.warning(f"Expected JSON but got {response.headers.get('content-type')}")
            return

        # Parse JSON response
        try:
            repos = response.json()
        except Exception as e:
            logger.error(f"Failed to parse JSON from {response.url}: {e}")
            return

        # Extract data from JSON
        for repo in repos:
            yield Item(
                data={
                    "name": repo.get("name"),
                    "full_name": repo.get("full_name"),
                    "description": repo.get("description"),
                    "stars": repo.get("stargazers_count", 0),
                    "forks": repo.get("forks_count", 0),
                    "language": repo.get("language"),
                    "url": repo.get("html_url"),
                },
                metadata={"api_source": "github"},
            )

        # Follow pagination in API (if Link header exists)
        link_header = response.headers.get("Link", "")
        if 'rel="next"' in link_header:
            # Parse Link header to get next URL
            # Format: <https://api.github.com/...?page=2>; rel="next"
            next_url = link_header.split(";")[0].strip("<>")
            yield Request(url=next_url)

        # Alternative: pagination via query params
        # current_page = int(response.url.split("page=")[1].split("&")[0])
        # if len(repos) > 0:  # Has more results
        #     next_page = current_page + 1
        #     yield Request(url=f"https://api.github.com/users/github/repos?page={next_page}&per_page=10")
```

**Key points for API scraping:**

- Use `response.json()` to parse JSON responses
- Check `Content-Type` header to verify JSON
- Handle pagination via Link headers or query parameters
- Set appropriate `Accept` header in `DEFAULT_REQUEST_HEADERS`

## Using request meta (passing data between requests)

```python title="meta_spider.py"
import logging

from qcrawl.core.item import Item
from qcrawl.core.spider import Spider

logger = logging.getLogger(__name__)


class EcommerceSpider(Spider):
    name = "ecommerce_meta"
    start_urls = ["https://books.toscrape.com/"]

    async def parse(self, response):
        """Single parse method handles all page types using meta for routing."""
        rv = self.response_view(response)

        # Use meta to determine what type of page this is
        page_type = response.request.meta.get("page_type", "home")

        if page_type == "home":
            # Parse home page - extract category links
            categories = rv.doc.cssselect("div.side_categories ul.nav-list li ul li a")

            for cat_link in categories:
                category_name = cat_link.text_content().strip()
                href = cat_link.get("href")

                if href:
                    # Mark as category page and pass category name
                    yield rv.follow(
                        href,
                        meta={"page_type": "category", "category": category_name, "page": 1},
                    )

        elif page_type == "category":
            # Parse category page - extract product links
            category = response.request.meta.get("category", "Unknown")
            page = response.request.meta.get("page", 1)

            logger.info(f"Parsing {category} - Page {page}")

            products = rv.doc.cssselect("article.product_pod h3 a")

            for product in products:
                href = product.get("href")
                if href:
                    # Mark as product page and pass category info
                    yield rv.follow(
                        href,
                        meta={"page_type": "product", "category": category, "page": page},
                    )

            # Follow pagination (preserve meta)
            next_page = rv.doc.cssselect("li.next a")
            if next_page:
                href = next_page[0].get("href")
                if href:
                    yield rv.follow(
                        href,
                        meta={"page_type": "category", "category": category, "page": page + 1},
                    )

        elif page_type == "product":
            # Parse product page - extract product details
            category = response.request.meta.get("category", "Unknown")
            page = response.request.meta.get("page", 0)

            title_elem = rv.doc.cssselect("h1")
            price_elem = rv.doc.cssselect("p.price_color")

            if title_elem and price_elem:
                yield Item(
                    data={
                        "title": title_elem[0].text_content().strip(),
                        "price": price_elem[0].text_content().strip(),
                        "url": response.url,
                        "category": category,
                        "found_on_page": page,
                    },
                    metadata={"source": "books.toscrape.com"},
                )
```

**Request meta patterns:**

- **Page routing**: Use `page_type` in meta to route different page types in single `parse()` method
- **Pass context**: Use `meta` to pass category, search query, or other context
- **Track depth**: Pass pagination info (`page`, `offset`) through requests
- **Preserve data**: Meta persists through the entire request chain
- **Type safety**: Always use `.get()` with defaults when accessing meta


## Export formats

qCrawl supports multiple export formats via CLI flags. Use any spider from the examples above:

### JSON Lines (default)
```bash
# One JSON object per line (streaming-friendly, memory efficient)
qcrawl quotes_css_spider.py --export output.jsonl

# Or explicitly specify format
qcrawl quotes_css_spider.py --export output.jsonl --export-format ndjson
```

**Output (output.jsonl):**
```json
{"url": "https://quotes.toscrape.com/", "text": "The world as we...", "author": "Albert Einstein", "tags": ["change", "world"]}
{"url": "https://quotes.toscrape.com/", "text": "It is our choices...", "author": "J.K. Rowling", "tags": ["abilities", "choices"]}
```

### JSON (full array)
```bash
# All items in a single JSON array
qcrawl quotes_css_spider.py --export output.json --export-format json
```

**Output (output.json):**
```json
[
  {
    "url": "https://quotes.toscrape.com/",
    "text": "The world as we...",
    "author": "Albert Einstein",
    "tags": ["change", "world"]
  },
  {
    "url": "https://quotes.toscrape.com/",
    "text": "It is our choices...",
    "author": "J.K. Rowling",
    "tags": ["abilities", "choices"]
  }
]
```

### CSV
```bash
# Comma-separated values (works best with flat data structures)
qcrawl quotes_css_spider.py --export output.csv --export-format csv
```

**Output (output.csv):**
```csv
url,text,author,tags
https://quotes.toscrape.com/,"The world as we...",Albert Einstein,"['change', 'world']"
https://quotes.toscrape.com/,"It is our choices...",J.K. Rowling,"['abilities', 'choices']"
```

### XML
```bash
# XML format
qcrawl quotes_css_spider.py --export output.xml --export-format xml
```

**Output (output.xml):**
```xml
<?xml version="1.0" encoding="utf-8"?>
<items>
  <item>
    <url>https://quotes.toscrape.com/</url>
    <text>The world as we...</text>
    <author>Albert Einstein</author>
    <tags>
      <value>change</value>
      <value>world</value>
    </tags>
  </item>
</items>
```

### Streaming vs Buffered Mode

```bash
# Buffered (default): Collects items in memory, writes at end
qcrawl quotes_css_spider.py --export output.json --export-mode buffered

# Streaming: Writes items immediately as they're scraped (memory efficient)
qcrawl quotes_css_spider.py --export output.jsonl --export-mode stream

# Adjust buffer size (items to collect before writing)
qcrawl quotes_css_spider.py --export output.jsonl --export-buffer-size 1000
```

**Format comparison:**

| Format | Streaming | Best For | File Size |
|--------|-----------|----------|-----------|
| **NDJSON** | ✅ Yes | Large datasets, streaming pipelines | Medium |
| **JSON** | ❌ No | API responses, small datasets | Medium |
| **CSV** | ✅ Yes | Excel, data analysis, flat data | Small |
| **XML** | ❌ No | Legacy systems, SOAP APIs | Large |
