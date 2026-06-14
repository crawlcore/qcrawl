
Middlewares are hooks that sit between core components, allowing you to modify request/response processing, filter data, handle errors, and extend qCrawl's behavior without modifying the core codebase.

## Overview

qCrawl has two types of middlewares:

**DownloaderMiddleware** - Hooks around HTTP download:

- Modify requests before they're sent (add headers, authentication)
- Process responses after download (filter, retry, redirect)
- Handle exceptions during download (network errors, timeouts)

**SpiderMiddleware** - Hooks around spider processing:

- Filter initial requests from `start_requests()`
- Process responses before `spider.parse()`
- Filter Items and Requests yielded by spider
- Handle exceptions during parsing

Both types execute in chains with configurable priority ordering.


## Downloader Middleware

Downloader middlewares wrap the HTTP download process.

### Hooks

#### process_request(request, spider)
Called **before** the request is sent to the downloader.

**Use cases**:

- Add authentication headers
- Set cookies
- Add custom headers
- Modify request parameters
- Log outgoing requests

**Returns**: a `MiddlewareResult` from one of its factory methods

- `MiddlewareResult.continue_()` - Pass request to next middleware / proceed to download
- `MiddlewareResult.keep(page)` - Short-circuit: use this `Page` instead of downloading
- `MiddlewareResult.drop()` - Drop request entirely (don't download)

**Example**:
```python
from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult

class AuthMiddleware(DownloaderMiddleware):
    async def process_request(self, request, spider):
        # Add authentication token
        request.headers["Authorization"] = f"Bearer {spider.api_token}"
        return MiddlewareResult.continue_()
```

#### process_response(request, response, spider)
Called **after** the downloader returns a response.

**Use cases**:

- Filter responses by status code
- Detect and handle errors
- Transform response content
- Log responses
- Trigger retries

**Returns**: a `MiddlewareResult` from one of its factory methods

- `MiddlewareResult.continue_()` - Pass response to next middleware
- `MiddlewareResult.keep(page)` - Stop chain, send this `Page` to the spider
- `MiddlewareResult.retry(request)` - Retry the request
- `MiddlewareResult.drop()` - Drop response (don't send to spider)

**Example**:
```python
class StatusCodeMiddleware(DownloaderMiddleware):
    async def process_response(self, request, response, spider):
        # Drop 404 responses
        if response.status_code == 404:
            spider.logger.warning(f"Page not found: {response.url}")
            return MiddlewareResult.drop()

        return MiddlewareResult.continue_()
```

#### process_exception(request, exception, spider)
Called when an exception occurs during download.

**Use cases**:

- Handle network errors
- Log exceptions
- Retry failed requests
- Return fallback responses

**Returns**: a `MiddlewareResult` from one of its factory methods

- `MiddlewareResult.continue_()` - Pass to next middleware
- `MiddlewareResult.retry(request)` - Retry the request
- `MiddlewareResult.drop()` - Drop request

**Example**:
```python
import aiohttp
from qcrawl.middleware.base import DownloaderMiddleware, MiddlewareResult

class NetworkErrorMiddleware(DownloaderMiddleware):
    async def process_exception(self, request, exception, spider):
        if isinstance(exception, aiohttp.ClientError):
            spider.logger.error(f"Network error for {request.url}: {exception}")
            return MiddlewareResult.retry(request)

        return MiddlewareResult.continue_()
```

### Execution order

Downloader middlewares execute in two phases:

**Request phase** (before download):
```
MW1.process_request → MW2.process_request → MW3.process_request → Downloader
```

**Response phase** (after download, **reversed order**):
```
MW3.process_response ← MW2.process_response ← MW1.process_response ← Downloader
```

Lower priority number = executed first in request phase, last in response phase.


## Spider Middleware

Spider middlewares wrap spider processing.

### Hooks

#### process_start_requests(start_requests, spider)
Called with initial requests from `spider.start_requests()`.

**Use cases**:

- Filter initial URLs
- Add metadata to start requests
- Transform URLs
- Limit initial requests

**Parameters**:

- `start_requests` - Async generator of initial Requests

**Returns**: Async generator of Requests

**Example**:
```python
from qcrawl.middleware.base import SpiderMiddleware

class StartRequestsFilterMiddleware(SpiderMiddleware):
    async def process_start_requests(self, start_requests, spider):
        async for request in start_requests:
            # Only crawl .com domains
            if ".com" in request.url:
                yield request
```

#### process_spider_input(response, spider)
Called **before** `spider.parse()` receives the response.

**Use cases**:

- Validate response before parsing
- Add metadata to response
- Filter responses
- Log incoming responses

**Returns**: `Exception | None`

- Return `None` to continue to `spider.parse()`
- Return an `Exception` instance to abort parsing this response (it is routed to `process_spider_exception`)

**Example**:
```python
class ResponseValidationMiddleware(SpiderMiddleware):
    async def process_spider_input(self, response, spider):
        # Abort parsing of empty responses (note: response.text is a method)
        if not response.text():
            spider.logger.warning(f"Empty response from {response.url}")
            return ValueError("empty response")

        return None
```

#### process_spider_output(response, result, spider)
Called with each Item or Request yielded by `spider.parse()`.

**Use cases**:

- Filter Items or Requests
- Transform yielded data
- Add metadata to Items
- Enforce depth limits
- Log scraped items

**Parameters**:

- `response` - The response being parsed
- `result` - Async generator of the Items/Requests/`str` yielded by the spider

**Returns**: an async generator — iterate `result` and `yield` what should pass along (skip an item by not yielding it)

**Example**:
```python
class ItemFilterMiddleware(SpiderMiddleware):
    async def process_spider_output(self, response, result, spider):
        from qcrawl.core.item import Item

        async for obj in result:
            # Filter out items without a title; pass everything else through
            if isinstance(obj, Item) and not obj.data.get("title"):
                spider.logger.debug("Dropping item without title")
                continue
            yield obj
```

#### process_spider_exception(response, exception, spider)
Called when spider.parse() raises an exception.

**Use cases**:

- Log parsing errors
- Handle specific exceptions
- Return fallback Items/Requests
- Skip problematic pages

**Returns**: an async generator of recovery Items/Requests, or `None` to re-raise

**Example**:
```python
class ParsingErrorMiddleware(SpiderMiddleware):
    async def process_spider_exception(self, response, exception, spider):
        spider.logger.error(
            f"Error parsing {response.url}: {exception}",
            exc_info=True,
        )
        # Return None to skip this page (the exception is re-raised/logged).
        # To recover instead, make this an async generator that `yield`s
        # Items/Requests.
        return None
```

### Execution order

Spider middlewares execute in two phases:

**Input phase** (before parse):
```
MW1.process_spider_input → MW2.process_spider_input → MW3.process_spider_input → Spider.parse()
```

**Output phase** (after parse, **reversed order**):
```
MW3.process_spider_output ← MW2.process_spider_output ← MW1.process_spider_output ← Spider.parse()
```


## Middleware Results

Downloader middlewares return a `MiddlewareResult`, built via its factory methods, to control execution flow:

```python
from qcrawl.middleware.base import MiddlewareResult

class MyMiddleware(DownloaderMiddleware):
    async def process_response(self, request, response, spider):
        # Continue to next middleware
        return MiddlewareResult.continue_()

        # Stop chain, use this Page as the response
        return MiddlewareResult.keep(response)

        # Retry the given request
        return MiddlewareResult.retry(request)

        # Drop the request/response
        return MiddlewareResult.drop()
```

**When to use each**:

- `continue_()` - Default, pass to next middleware
- `keep(page)` - Stop chain early with a specific `Page` (optimization / short-circuit)
- `retry(request)` - Request failed, retry with backoff
- `drop()` - Filter out unwanted requests/responses

(Spider middlewares do not use `MiddlewareResult`: `process_spider_input` returns `Exception | None`, while `process_spider_output`/`process_spider_exception` are async generators.)


## Registering Middlewares

### In spider settings

```python
from qcrawl.core.spider import Spider

class MySpider(Spider):
    name = "my_spider"
    start_urls = ["https://example.com"]

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "myproject.middlewares.AuthMiddleware": 100,
            "myproject.middlewares.StatusCodeMiddleware": 200,
        },
        "SPIDER_MIDDLEWARES": {
            "myproject.middlewares.ItemFilterMiddleware": 300,
        }
    }
```

### In global settings

```python
# settings.py or pyproject.toml
DOWNLOADER_MIDDLEWARES = {
    "myproject.middlewares.AuthMiddleware": 100,
    "myproject.middlewares.CustomMiddleware": 500,
}

SPIDER_MIDDLEWARES = {
    "myproject.middlewares.DepthMiddleware": 100,
}
```

### Priority numbers

- Lower number = executed first in request/input phase
- Higher number = executed first in response/output phase
- Built-in middlewares use 100, 200, 300, etc.
- Leave gaps for custom middlewares

**Example ordering**:
```
Priority 100: MW1
Priority 200: MW2
Priority 500: MW3

Request flow:  MW1 → MW2 → MW3 → Downloader
Response flow: MW3 ← MW2 ← MW1 ← Downloader
```


## Middleware with State

### Initialization with from_crawler

Middlewares can access crawler components via `from_crawler()`:

```python
class StatefulMiddleware(DownloaderMiddleware):
    def __init__(self, settings, stats):
        self.max_retries = settings.get("MAX_RETRIES", 3)
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            settings=crawler.settings,
            stats=crawler.stats
        )

    async def process_request(self, request, spider):
        self.stats.inc("custom/requests")
        return MiddlewareResult.continue_()
```

### Spider-specific state

Store state per spider instance:

```python
class RateLimitMiddleware(DownloaderMiddleware):
    def __init__(self):
        self.request_times = {}  # domain → last request time

    async def process_request(self, request, spider):
        from urllib.parse import urlparse
        import time

        domain = urlparse(request.url).netloc
        last_time = self.request_times.get(domain, 0)
        current_time = time.time()

        # Wait if too soon
        delay = getattr(spider, "download_delay", 1.0)
        if current_time - last_time < delay:
            await asyncio.sleep(delay - (current_time - last_time))

        self.request_times[domain] = time.time()
        return MiddlewareResult.continue_()
```

## Best Practices

### Design

- **Single responsibility**: Each middleware should do one thing well
- **Fail gracefully**: Handle errors without breaking the crawl
- **Be defensive**: Validate inputs and handle edge cases
- **Document behavior**: Explain what your middleware does and when to use it

### Performance

- **Minimize overhead**: Middlewares execute on every request/response
- **Use async operations**: Don't block the event loop
- **Cache when possible**: Avoid repeated computations
- **Profile impact**: Measure middleware overhead

### State management

- **Use from_crawler()**: Access settings, stats, signals
- **Thread-safe state**: Multiple workers access middlewares concurrently
- **Clean up resources**: Implement cleanup if needed

### Configuration

- **Use settings**: Make behavior configurable
- **Provide defaults**: Sensible defaults for all options
- **Document settings**: Explain all configuration options
- **Validate settings**: Check required settings exist

### Testing

- **Unit test logic**: Test middleware behavior in isolation
- **Mock dependencies**: Don't require real HTTP requests
- **Test edge cases**: Handle errors, empty responses, etc.
- **Integration test**: Verify middleware works in full crawl


## See also

- [Architecture overview](architecture_overview.md) - How middlewares fit into qCrawl
- [Signals reference](signals.md) - React to crawler events
- [Built-in middlewares](../concepts/middlewares.md) - Reference for built-in middlewares
- [Settings](../concepts/settings.md) - Configure middleware behavior
