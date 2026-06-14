
Websites can be unpredictable: pages may fail to load, content might be missing, or selectors might change. Robust spiders implement error recovery strategies to handle these issues gracefully.

## Built-in retry with exponential backoff

qCrawl includes a **RetryMiddleware** (enabled by default) that automatically retries failed requests with exponential backoff.

### Default behavior

The RetryMiddleware automatically retries:

- **Network errors**: `aiohttp.ClientError`, `asyncio.TimeoutError`
- **HTTP errors**: Status codes `429, 500, 502, 503, 504`
- **Max retries**: 3 attempts by default
- **Exponential backoff**: `delay = backoff_base * (2^retry_count)` with jitter
- **Retry-After support**: Respects server `Retry-After` headers

No configuration needed - it works out of the box!

### Configure retry settings

All retry behaviour is configurable via settings:

```python
class MySpider(Spider):
    name = "my_spider"
    start_urls = ["https://example.com"]

    custom_settings = {
        "MAX_RETRIES": 5,                                    # default: 3
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 408],  # default: [429, 500, 502, 503, 504]
        "RETRY_PRIORITY_ADJUST": -1,                         # default: -1 (lower priority for retries)
        "RETRY_BACKOFF_BASE": 2.0,                           # default: 1.0
        "RETRY_BACKOFF_MAX": 120.0,                          # default: 60.0
        "RETRY_BACKOFF_JITTER": 0.3,                         # default: 0.3
    }
```

### Backoff calculation

The delay between retries is calculated as:

```
delay = min(backoff_base * (2^retry_count), backoff_max)
```

With jitter applied: `delay ± (delay * jitter)`

**Example delays** (with `backoff_base=1.0`, `backoff_max=60.0`, `jitter=0.3`):

- Retry 1: 1.0s ± 0.3s (0.7-1.3s)
- Retry 2: 2.0s ± 0.6s (1.4-2.6s)
- Retry 3: 4.0s ± 1.2s (2.8-5.2s)
- Retry 4: 8.0s ± 2.4s (5.6-10.4s)

### Monitor retry stats

Check retry statistics:

```python
stats = crawler.stats.snapshot()

print(f"Total retries: {stats.get('downloader/retry/total', 0)}")
print(f"Network errors: {stats.get('downloader/retry/network_error', 0)}")
print(f"HTTP errors: {stats.get('downloader/retry/http_error', 0)}")
print(f"Max retries reached: {stats.get('downloader/retry/max_reached', 0)}")
```

### Skip retries for specific requests

Control retries per request through `meta`: `dont_retry` skips retrying a
request entirely, and `max_retry_times` caps its retries independently of the
`MAX_RETRIES` setting.

```python
from qcrawl.core.request import Request

# Never retry this request
yield Request(url="https://example.com/optional", meta={"dont_retry": True})

# Allow at most one retry for this request
yield Request(url="https://example.com/flaky", meta={"max_retry_times": 1})
```


## Handle missing data gracefully

Extract data with defaults and error handling:

```python
async def parse(self, response):
    rv = self.response_view(response)

    for product in rv.doc.cssselect(".product"):
        # Extract with fallbacks and defaults
        title_elem = product.cssselect("h2.title")
        price_elem = product.cssselect(".price")

        title = title_elem[0].text_content().strip() if title_elem else "Unknown"
        price_text = price_elem[0].text_content().strip() if price_elem else None

        # Parse price with error handling
        try:
            price = float(price_text.replace("$", "").replace(",", "")) if price_text else None
        except (ValueError, AttributeError):
            price = None

        yield {
            "title": title,
            "price": price,
            "url": response.url
        }
```


## Handle HTTP errors

Respond to different HTTP status codes:

```python
async def parse(self, response):
    # Handle various status codes (5xx errors are auto-retried by RetryMiddleware)
    if response.status_code == 404:
        self.logger.warning(f"Page not found: {response.url}")
        return

    if response.status_code == 403:
        self.logger.error(f"Access forbidden: {response.url}")
        return

    if response.status_code != 200:
        self.logger.warning(f"Unexpected status {response.status_code}: {response.url}")
        return

    # Process successful response
    rv = self.response_view(response)
    for item in rv.doc.cssselect(".item"):
        yield self.extract_item(item)
```


## Validate response content

Check response before processing:

```python
async def parse(self, response):
    rv = self.response_view(response)

    # Validate content type
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        self.logger.warning(f"Unexpected content type {content_type}: {response.url}")
        return

    # Validate page structure
    if not rv.doc.cssselect("body"):
        self.logger.error(f"Invalid HTML structure: {response.url}")
        return

    # Check for error pages
    error_indicators = rv.doc.cssselect(".error-page, .not-found, .access-denied")
    if error_indicators:
        self.logger.warning(f"Error page detected: {response.url}")
        return

    # Process valid response
    for item in rv.doc.cssselect(".item"):
        yield self.extract_item(item)
```


## Circuit breaker pattern

Stop crawling a source after repeated failures:

```python
class ResilientSpider(Spider):
    name = "resilient"
    start_urls = ["https://example.com"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failure_count = {}
        self.circuit_open = set()

    def check_circuit(self, domain):
        """Check if circuit breaker is open for domain."""
        return domain in self.circuit_open

    def record_failure(self, domain):
        """Record failure and open circuit if threshold exceeded."""
        self.failure_count[domain] = self.failure_count.get(domain, 0) + 1

        if self.failure_count[domain] >= 5:
            self.circuit_open.add(domain)
            self.logger.error(f"Circuit breaker opened for {domain} after 5 failures")

    async def parse(self, response):
        from urllib.parse import urlparse
        domain = urlparse(response.url).netloc

        # Check circuit breaker
        if self.check_circuit(domain):
            self.logger.warning(f"Skipping {response.url} - circuit open for {domain}")
            return

        rv = self.response_view(response)

        # Validate response
        if not rv.doc.cssselect(".expected-content"):
            self.record_failure(domain)
            return

        # Reset failure count on success
        self.failure_count[domain] = 0

        # Process response
        for item in rv.doc.cssselect(".item"):
            yield self.extract_item(item)
```


## Logging and debugging

Log errors for debugging:

```python
async def parse(self, response):
    rv = self.response_view(response)

    items = rv.doc.cssselect(".item")

    if not items:
        self.logger.warning(
            f"No items found on {response.url} "
            f"(status: {response.status_code}, "
            f"content-length: {len(response.text())})"
        )

        # Log page structure for debugging
        self.logger.debug(f"Page structure: {[elem.tag for elem in rv.doc[:10]]}")
        return

    self.logger.info(f"Found {len(items)} items on {response.url}")

    for item in items:
        try:
            yield self.extract_item(item)
        except Exception as e:
            self.logger.error(f"Error extracting item: {e}", exc_info=True)
```


## Best practices

- **Use built-in RetryMiddleware**: It's enabled by default and handles network errors and HTTP errors automatically
- **Configure retry settings**: Adjust `MAX_RETRIES`, `RETRY_HTTP_CODES`, `RETRY_PRIORITY_ADJUST`, and `RETRY_BACKOFF_*` as needed
- **Monitor retry stats**: Track retry metrics to detect problematic sources
- **Handle missing data gracefully**: Use default values and defensive checks for optional fields
- **Log errors appropriately**: Use different log levels (warning, error, debug)
- **Validate before processing**: Check response status, content type, page structure
- **Handle edge cases**: Account for missing elements, empty pages, malformed data
- **Use circuit breakers**: Stop hitting failing sources to save resources after repeated failures

See also: [Authentication](authentication.md), [Rate Limiting](rate_limiting.md), [Data Extraction](data_extraction.md), [Middlewares](../concepts/middlewares.md)