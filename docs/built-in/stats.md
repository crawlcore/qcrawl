
qCrawl provides a comprehensive, thread-safe, and extensible statistics system via the `StatsCollector` class.<br>
It allows monitoring and recording various metrics during crawling sessions.

## Key Features

* **Thread-Safe**: Designed to work safely with synchronous counters in an async runtime.
* **Custom Metrics**: Easily define and track custom statistics relevant to your crawl.
* **Built-in Metrics**: The runtime emits common metrics (request/response counts, bytes, errors).
* **Exportable**: Collected statistics can be retrieved programmatically for export or display.

## Default Metrics

| Metric key                            | Description                                                              |
|---------------------------------------|--------------------------------------------------------------------------|
| `spider_name`                         | Spider name                                                              |
| `start_time`                          | Time when spider opened (ISO 8601 timestamp)                             |
| `finish_time`                         | Time when spider closed (ISO 8601 timestamp)                             |
| `finish_reason`                       | Reason the spider stopped (`finished`, `error`, etc.)                    |
| `elapsed_time_seconds`                | Total runtime in seconds                                                 |
| `scheduler/request_scheduled_count`   | Total URLs added to the scheduler (deduplicated adds)                    |
| `scheduler/dequeued`                  | Counter incremented when a request is dropped/removed                    |
| `downloader/request_downloaded_count` | Number of requests that reached the downloader (attempted fetch)         |
| `downloader/response_status_count`    | Total responses received                                                 |
| `downloader/response_status_{CODE}`   | Responses grouped by HTTP status (e.g. `downloader/response_status_200`) |
| `downloader/bytes_downloaded`         | Total bytes received                                                     |
| `pipeline/item_scraped_count`         | Items that passed the item pipeline (emitted post-pipeline)              |
| `pipeline/item_dropped_count`         | Items dropped by a pipeline (via `DropItem`)                             |
| `pipeline/item_error_count`           | Items whose pipeline processing raised an unexpected error               |
| `engine/error_count`                  | Total exceptions/errors signalled as engine errors                       |


## Accessing Stats

### During crawl
```python
async with Crawler(spider, settings) as crawler:
    await crawler.crawl()

    # Get single value (inside context manager)
    downloaded = crawler.stats.get("downloader/request_downloaded_count", 0)

    # Get all stats snapshot
    all_stats = crawler.stats.snapshot()
    print(f"Downloaded {downloaded} pages")
```

### After crawl
```python
# Store stats before crawler closes
async with Crawler(spider, settings) as crawler:
    await crawler.crawl()
    stats = crawler.stats.snapshot()  # Get stats before exiting

# Use stats after crawler is closed
downloaded_count = stats.get('downloader/request_downloaded_count', 0)
print(f"Downloaded {downloaded_count} pages")
```

## Adding Custom Metrics

`StatsCollector` exposes intent-revealing operations over a single flat,
slash-namespaced key store — the verb says what kind of metric it is:

```python
crawler.stats.inc("custom/my_metric")                 # counter: increment by 1 (or count=N)
crawler.stats.set("custom/queue_depth", 42)            # gauge: set a number
crawler.stats.max("custom/peak_depth", 128)            # gauge: keep the high-water mark
crawler.stats.min("custom/min_latency", 0.12)          # gauge: keep the low-water mark
crawler.stats.label("custom/last_run", "2025-04-05")   # string metadata
```

Read them back with `get(key, default)` for one value or `snapshot()` for a copy
of everything.

The preferred way to record custom metrics is from a signal handler:

```python
async def on_response(sender, response, request=None, **kwargs):
    # `response_received` does not pass `spider`; close over `crawler` for stats.
    if "api" in getattr(response, "url", ""):
        crawler.stats.inc("custom/api_calls")

# Connect the handler to the crawler-bound dispatcher
crawler.signals.connect("response_received", on_response)
```
