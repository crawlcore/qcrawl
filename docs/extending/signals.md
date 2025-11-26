
Signals provide an event-driven extension mechanism for monitoring and extending qCrawl's behavior without modifying core components. Signals allow you to hook into the crawling lifecycle, collect custom metrics, implement logging, and perform side effects.

## Key concepts

- **Signal**: Named event emitted at specific points in the crawl lifecycle (e.g., `response_received`, `item_scraped`)
- **Handler**: Async function connected to a signal that executes when the signal is emitted
- **Sender**: Component that emitted the signal (e.g., Engine, Downloader, Spider)
- **Priority**: Handlers with higher priority execute first (default: 0)
- **Weak references**: Handlers are stored as weak references by default to prevent memory leaks

## Available signals

### Spider lifecycle
| Signal          | Emitted when                  | Handler signature                                     |
|-----------------|-------------------------------|-------------------------------------------------------|
| `spider_opened` | Spider starts crawling        | `async def handler(sender, spider, **kwargs)`         |
| `spider_closed` | Spider finishes crawling      | `async def handler(sender, spider, reason, **kwargs)` |
| `spider_idle`   | Scheduler has no pending URLs | `async def handler(sender, spider, **kwargs)`         |
| `spider_error`  | Uncaught error in spider      | `async def handler(sender, spider, error, **kwargs)`  |

### Request/Response lifecycle
| Signal                       | Emitted when                     | Handler signature                                                 |
|------------------------------|----------------------------------|-------------------------------------------------------------------|
| `request_scheduled`          | Request added to scheduler       | `async def handler(sender, request, spider, **kwargs)`            |
| `request_dropped`            | Request filtered/dropped         | `async def handler(sender, request, spider, **kwargs)`            |
| `request_reached_downloader` | Request sent to downloader       | `async def handler(sender, request, spider, **kwargs)`            |
| `request_failed`             | Download failed                  | `async def handler(sender, request, error, spider, **kwargs)`     |
| `response_received`          | Response successfully downloaded | `async def handler(sender, response, request, spider, **kwargs)`  |

### Item lifecycle
| Signal         | Emitted when              | Handler signature                                             |
|----------------|---------------------------|---------------------------------------------------------------|
| `item_scraped` | Item yielded from spider  | `async def handler(sender, item, response, spider, **kwargs)` |
| `item_dropped` | Item filtered by pipeline | `async def handler(sender, item, spider, **kwargs)`           |
| `item_error`   | Item processing failed    | `async def handler(sender, item, error, spider, **kwargs)`    |

### Tracking signals
| Signal             | Emitted when          | Handler signature                                                   |
|--------------------|-----------------------|---------------------------------------------------------------------|
| `bytes_received`   | Response bytes read   | `async def handler(sender, data_length, request, spider, **kwargs)` |
| `headers_received` | Response headers read | `async def handler(sender, headers, request, spider, **kwargs)`     |


## Connecting handlers

### Basic handler
```python
from qcrawl.core.crawler import Crawler

async def on_response(sender, response, request=None, spider=None, **kwargs):
    print(f"Downloaded: {response.url} (status: {response.status_code})")

# Connect during crawler setup
async with Crawler(spider, settings) as crawler:
    crawler.signals.connect("response_received", on_response)
    await crawler.crawl()
```

### Handler with priority
Higher priority handlers execute first:

```python
async def high_priority_handler(sender, **kwargs):
    print("Runs first")

async def low_priority_handler(sender, **kwargs):
    print("Runs second")

crawler.signals.connect("spider_opened", high_priority_handler, priority=100)
crawler.signals.connect("spider_opened", low_priority_handler, priority=50)
```

### Sender filtering
Receive signals only from specific components:

```python
async def engine_only_handler(sender, **kwargs):
    print("Only called when Engine emits signals")

# Only receive signals from the engine instance
crawler.signals.connect(
    "response_received",
    engine_only_handler,
    sender=crawler.engine
)
```

### Strong references
Prevent handler garbage collection:

```python
async def permanent_handler(sender, **kwargs):
    pass

# Use weak=False to keep a strong reference
crawler.signals.connect("item_scraped", permanent_handler, weak=False)
```


## Common use cases

### Collecting custom metrics
```python
async def track_api_calls(sender, response, request=None, **kwargs):
    if "api" in response.url:
        sender.stats.inc_value("custom/api_calls", count=1)

crawler.signals.connect("response_received", track_api_calls)
```

### Logging specific events
```python
import logging

logger = logging.getLogger(__name__)

async def log_errors(sender, request, error, spider=None, **kwargs):
    logger.error(f"Failed to download {request.url}: {error}")

crawler.signals.connect("request_failed", log_errors)
```

### Custom item validation
```python
async def validate_items(sender, item, response, spider=None, **kwargs):
    data = getattr(item, "data", item)

    if not data.get("title"):
        logger.warning(f"Item missing title from {response.url}")
        sender.stats.inc_value("custom/validation_warnings", count=1)

crawler.signals.connect("item_scraped", validate_items)
```

### Tracking crawl progress
```python
async def track_progress(sender, spider, **kwargs):
    stats = sender.stats.get_stats()
    scheduled = stats.get("scheduler/request_scheduled_count", 0)
    downloaded = stats.get("downloader/request_downloaded_count", 0)

    if scheduled > 0:
        progress = (downloaded / scheduled) * 100
        print(f"Progress: {progress:.1f}% ({downloaded}/{scheduled})")

# Call every time spider becomes idle
crawler.signals.connect("spider_idle", track_progress)
```

### Sending notifications
```python
async def send_completion_email(sender, spider, reason, **kwargs):
    stats = sender.stats.get_stats()
    items = stats.get("pipeline/item_scraped_count", 0)

    # Send email notification (pseudo-code)
    # await send_email(
    #     subject=f"Crawl completed: {spider.name}",
    #     body=f"Scraped {items} items. Reason: {reason}"
    # )
    print(f"Crawl completed: {items} items scraped")

crawler.signals.connect("spider_closed", send_completion_email)
```


## Disconnecting handlers

### Disconnect specific handler
```python
# Disconnect a specific handler
crawler.signals.disconnect("response_received", on_response)

# Disconnect handler for specific sender only
crawler.signals.disconnect("response_received", on_response, sender=crawler.engine)
```

### Disconnect all handlers
```python
# Remove all handlers for a signal
crawler.signals.disconnect_all("item_scraped")

# Remove all handlers for a signal from specific sender
crawler.signals.disconnect_all("item_scraped", sender=spider)
```


## Advanced usage

### Concurrent handler execution
By default, handlers execute sequentially in priority order. Use concurrent execution for independent handlers:

```python
# Emit signal with concurrent execution
await crawler.signals.send_async(
    "custom_signal",
    concurrent=True,
    max_concurrency=10,  # Limit concurrent handlers
    sender=spider,
    custom_data="value"
)
```

### Collecting handler results
```python
async def handler_with_result(sender, **kwargs):
    return {"processed": True, "count": 42}

crawler.signals.connect("custom_signal", handler_with_result)

# Emit and collect results
results = await crawler.signals.send_async("custom_signal", sender=spider)
print(results)  # [{"processed": True, "count": 42}]
```

### Handler exceptions
```python
async def failing_handler(sender, **kwargs):
    raise ValueError("Handler error")

# By default, exceptions are logged and swallowed
crawler.signals.connect("spider_opened", failing_handler)

# To propagate exceptions:
await crawler.signals.send_async(
    "spider_opened",
    sender=spider,
    raise_exceptions=True  # Will raise ValueError
)
```


## Best practices

- **Use weak references (default)**: Prevents memory leaks when handlers are class methods
- **Keep handlers fast**: Handlers block signal delivery; use `concurrent=True` for slow operations
- **Handle exceptions gracefully**: Exceptions in handlers are logged but don't stop the crawl by default
- **Use sender filtering**: Reduce unnecessary handler calls by filtering by sender
- **Use priority wisely**: Higher priority = earlier execution (stats collectors typically use high priority)
- **Avoid modifying shared state**: Handlers run concurrently when `concurrent=True`; use thread-safe operations


## Signal handler signature

All handlers must be async functions with this signature:

```python
async def handler(sender, *args, **kwargs) -> object | None:
    """
    Args:
        sender: Component that emitted the signal (Engine, Downloader, etc.)
        *args: Positional arguments specific to the signal
        **kwargs: Keyword arguments specific to the signal (spider, request, response, etc.)

    Returns:
        Optional value collected by send_async()
    """
    pass
```

**Important**: Always accept `**kwargs` for forward compatibility as new signal arguments may be added.


## Implementation notes

- Signals are managed by `SignalRegistry` in `qcrawl/signals.py`
- The global `signals_registry` instance is used throughout qCrawl
- Handlers are stored with weak references by default to prevent memory leaks
- Dead (garbage-collected) handlers are cleaned up lazily during signal emission
- Priority-based execution: handlers with higher priority values execute first
- Sequential execution by default; use `concurrent=True` for parallel execution
