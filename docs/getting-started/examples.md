
!!! quote

    "You don't learn to walk by following rules. You learn by doing, and by falling over."
    — Richard Branson

This page provides practical examples demonstrating different qCrawl features and common use cases.

## CSS selectors with custom settings

This example demonstrates CSS selectors, custom spider settings, and error handling patterns.

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

        # Safely handle CSS selector errors (malformed or unsupported selectors)
        try:
            quotes = rv.doc.cssselect("div.quote")
        except SelectorError:
            # Skip page gracefully without raising an error
            return

        for q in quotes:
            try:
                text = q.cssselect("span.text")[0].text_content().strip()
                author = q.cssselect("small.author")[0].text_content().strip()
                tags = [t.text_content().strip() for t in q.cssselect("div.tags a.tag")]
            except (IndexError, SelectorError):
                # Skip items with missing elements or selector issues
                continue

            yield Item(
                data={"url": response.url, "text": text, "author": author, "tags": tags},
                metadata={"source": "quotes.toscrape.com"},
            )

        # Follow pagination links (rv.follow resolves relative URLs automatically)
        next_a = rv.doc.cssselect("li.next a")
        if next_a:
            href = next_a[0].get("href")
            if href:
                yield rv.follow(href)
```

For settings setup including `custom_settings`, see [Settings documentation](../concepts/settings.md).

## XPath selectors

This example shows how to use XPath selectors for more powerful element selection and text extraction.

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

See [Selectors documentation](../concepts/selectors.md) for more details.


## API and JSON scraping

This example demonstrates scraping JSON APIs, handling content-type validation, and parsing Link headers for pagination.

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

## Running spiders programmatically (without CLI)

Use `SpiderRunner` to run any spider programmatically from Python code:

```python
import asyncio
from qcrawl.runner.run import SpiderRunner

async def main():
    runner = SpiderRunner(
        settings={
            "CONCURRENCY": 4,
            "LOG_LEVEL": "INFO",
        }
    )

    # Run any spider defined above (e.g. Quotes)
    await runner.crawl(Quotes)

if __name__ == "__main__":
    asyncio.run(main())
```

This is useful for embedding qCrawl in applications, scripts, or automated workflows.

See "Advanced Topics" section for more complex patterns like collecting items programmatically, running multiple spiders, or using pipelines and signals.
