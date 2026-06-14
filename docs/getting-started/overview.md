
## Introduction

qCrawl is a fast async web crawling framework for Python that makes it easy to extract structured data from websites.

What qCrawl provides:

1. **Async architecture** - High-performance concurrent crawling based on asyncio
2. **Powerful parsing** - Built-in CSS/XPath selectors with lxml
3. **Middleware system** - Customizable request/response processing with downloader and spider middlewares
4. **Flexible export** - Multiple output formats including JSON, CSV, XML
5. **Flexible queue backends** - Memory, disk, or Redis-based schedulers for different scale requirements
6. **Item pipelines** - Data transformation, validation, and processing before export


## Your first spider

Let's start with the simplest possible spider and build from there.

```python title="quotes_spider.py"
from qcrawl.core.spider import Spider

class QuotesSpider(Spider):
    name = "quotes_spider"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response):

        # ResponseView provides convenient parsing methods:
        # CSS/XPath selectors, link extraction, and URL resolution
        rv = self.response_view(response)

        # rv.doc exposes a lazy-loaded lxml document tree for efficient parsing
        for quote in rv.doc.cssselect('div.quote'):
            yield {
                "text": quote.cssselect('span.text')[0].text_content(),
                "author": quote.cssselect('small.author')[0].text_content(),
            }
```

Run it:
```bash
# Default - prints to console (stdout) in NDJSON format
qcrawl quotes_spider:QuotesSpider
```
The spider visits the URL, extracts quotes, and outputs them to stdout in NDJSON format (one JSON object per line).

## Understanding the response

Let's explore what qCrawl provides in the `response` object:

```python
async def parse(self, response):
    # Access response properties
    print(f"URL: {response.url}")
    print(f"Status: {response.status_code}")
    print(f"Headers: {response.headers}")

    # Get response view for parsing
    rv = self.response_view(response)

    # Access the parsed HTML document
    print(f"Title: {rv.doc.cssselect('title')[0].text_content()}")

    # Extract all links
    links = [a.get('href') for a in rv.doc.cssselect('a')]
    print(f"Found {len(links)} links")
```

**Key response properties:**

- `response.url` - Final URL (after redirects)
- `response.status_code` - HTTP status (200, 404, etc.)
- `response.headers` - Response headers dict
- `response.text()` - Raw HTML content
- `response.json()` - Parse JSON responses

**ResponseView (rv) provides:**

- `rv.doc` - Lazy-loaded lxml document tree for CSS/XPath selectors
- `rv.follow(href, priority=0, meta=None)` - Create new `Request` from relative or absolute `URL`
- `rv.urljoin(url)` - Resolve relative `URL` to absolute `URL` (returns string)
- `rv.response` - Access to the underlying `Response` object
- `rv.spider` - Access to the current `Spider` instance


## Adding configuration

Customize spider behavior with settings:

```python
class QuotesSpider(Spider):
    name = "quotes_spider"
    start_urls = ["https://quotes.toscrape.com/"]

    # Spider-specific settings
    custom_settings = {
        "CONCURRENCY": 5,
        "DELAY_PER_DOMAIN": 0.5,
        "USER_AGENT": "MyBot/1.0",
        "TIMEOUT": 30.0,
    }

    async def parse(self, response):
        rv = self.response_view(response)

        for quote in rv.doc.cssselect('div.quote'):
            text_elem = quote.cssselect('span.text')
            author_elem = quote.cssselect('small.author')

            # Skip if elements are missing
            if not text_elem or not author_elem:
                continue

            yield {
                "text": text_elem[0].text_content().strip(),
                "author": author_elem[0].text_content().strip(),
                "url": response.url,
            }
```

**Common settings:**

- `CONCURRENCY` - Number of concurrent requests
- `DELAY_PER_DOMAIN` - Delay between requests to same domain (seconds)
- `USER_AGENT` - Browser user agent string
- `TIMEOUT` - Request timeout in seconds
- `MAX_RETRIES` - Retry failed requests

For a full list of settings, see the [settings reference](../concepts/settings.md).

## Handling errors

Add error handling for robust spiders:

```python
from cssselect import SelectorError

class QuotesSpider(Spider):
    name = "quotes_spider"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response):
        # Check response status
        if response.status_code != 200:
            self.logger.warning(f"Non-200 status: {response.status_code}")
            return

        rv = self.response_view(response)

        # Safe selector usage
        try:
            quotes = rv.doc.cssselect('div.quote')
        except SelectorError as e:
            self.logger.error(f"Invalid selector: {e}")
            return

        if not quotes:
            self.logger.info("No quotes found on page")
            return

        for quote in quotes:
            try:
                text = quote.cssselect('span.text')[0].text_content().strip()
                author = quote.cssselect('small.author')[0].text_content().strip()

                yield {
                    "text": text,
                    "author": author,
                    "url": response.url,
                }
            except (IndexError, AttributeError) as e:
                # Skip malformed quotes
                self.logger.debug(f"Skipping quote due to: {e}")
                continue
```

**Error handling patterns:**

- Check `response.status_code` before processing
- Use try/except for selector operations
- Validate elements exist before accessing
- Log warnings/errors for debugging
- Use `continue` to skip bad items, `return` to skip page

For advanced error handling, see [error recovery](../advanced-topics/error_recovery.md).


## Following links and pagination

Crawl multiple pages by following links:

```python
class QuotesSpider(Spider):
    name = "quotes_spider"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response):
        rv = self.response_view(response)

        # Extract items from current page
        for quote in rv.doc.cssselect('div.quote'):
            text_elem = quote.cssselect('span.text')
            author_elem = quote.cssselect('small.author')

            if text_elem and author_elem:
                yield {
                    "text": text_elem[0].text_content().strip(),
                    "author": author_elem[0].text_content().strip(),
                    "url": response.url,
                }

        # Follow "next" link for pagination
        next_links = rv.doc.cssselect('li.next a')
        if next_links:
            href = next_links[0].get('href')
            if href:
                # rv.follow() resolves relative URLs and preserves context
                yield rv.follow(href)
```

**Link following patterns:**

- `rv.follow(href)` - Follow relative or absolute URL
- `self.follow(response, href)` - Alternative syntax
- Yields new requests back to the scheduler
- Same `parse()` method handles all pages


For advanced pagination techniques, see [pagination strategies](../advanced-topics/pagination.md).


## Exporting data

Save scraped data to files:

```bash
# Export to JSON
qcrawl --export quotes.json --export-format json quotes_spider:QuotesSpider

# Export to CSV
qcrawl --export quotes.csv --export-format csv quotes_spider:QuotesSpider

# Export to XML
qcrawl --export quotes.xml --export-format xml quotes_spider:QuotesSpider
```

**Export formats:**

- `json` - JSON array of items
- `ndjson` - One JSON object per line (NDJSON format)
- `csv` - CSV with headers
- `xml` - XML format

## Complete example

Putting it all together:

```python
from cssselect import SelectorError
from qcrawl.core.spider import Spider
from qcrawl.core.item import Item

class QuotesSpider(Spider):
    name = "quotes_spider"
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "CONCURRENCY": 5,
        "DELAY_PER_DOMAIN": 0.5,
        "USER_AGENT": "QuoteBot/1.0",
        "MAX_DEPTH": 0,  # Unlimited depth
    }

    async def parse(self, response):
        """Extract quotes and follow pagination."""

        # Validate response
        if response.status_code != 200:
            self.logger.warning(f"Non-200 status: {response.status_code}")
            return

        rv = self.response_view(response)

        # Safe selector usage
        try:
            quotes = rv.doc.cssselect('div.quote')
        except SelectorError as e:
            self.logger.error(f"Selector error: {e}")
            return

        # Extract quotes
        for quote in quotes:
            try:
                text = quote.cssselect('span.text')[0].text_content().strip()
                author = quote.cssselect('small.author')[0].text_content().strip()
                tags = [t.text_content().strip()
                       for t in quote.cssselect('div.tags a.tag')]

                yield Item(
                    data={
                        "text": text,
                        "author": author,
                        "tags": tags,
                        "url": response.url,
                    },
                    metadata={"source": "quotes.toscrape.com"}
                )
            except (IndexError, AttributeError) as e:
                self.logger.debug(f"Skipping malformed quote: {e}")
                continue

        # Follow pagination
        next_links = rv.doc.cssselect('li.next a')
        if next_links:
            href = next_links[0].get('href')
            if href:
                yield rv.follow(href)
```

**Run with export:**
```bash
qcrawl --export quotes.json --export-format json quotes_spider:QuotesSpider
```

## What’s next?
Ready to get started? [Install qCrawl](installation.md), [review examples](examples.md), and [join the community](https://discord.gg/yT54ff6STY) on Discord for support.<br>
Thanks for your interest!
