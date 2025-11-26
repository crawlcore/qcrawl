
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
| `pipeline/item_scraped_count`         | Total items yielded to pipelines                                         |
| `engine/error_count`                  | Total exceptions/errors signalled as engine errors                       |


## Accessing Stats

### During crawl
```python
async with Crawler(spider, settings) as crawler:
    await crawler.crawl()

    # Get single value (inside context manager)
    downloaded = crawler.stats.get_value("downloader/request_downloaded_count", 0)

    # Get all stats snapshot
    all_stats = crawler.stats.get_stats()
    print(f"Downloaded {downloaded} pages")
```

### After crawl
```python
# Store stats before crawler closes
async with Crawler(spider, settings) as crawler:
    await crawler.crawl()
    stats = crawler.stats.get_stats()  # Get stats before exiting

# Use stats after crawler is closed
downloaded_count = stats.get('downloader/request_downloaded_count', 0)
print(f"Downloaded {downloaded_count} pages")
```

## Adding Custom Metrics

Increment Counter

```python
crawler.stats.inc_value("custom/my_metric", count=1)
```

Set Value

```python
crawler.stats.set_counter("custom/processed_items", 42)
crawler.stats.set_meta("custom/last_run", "2025-04-05")
```

Preferred way to add custom metrics (using Signals):

```python
async def on_response(sender, response, request=None, **kwargs):
    if "api" in getattr(response, "url", ""):
        sender.stats.inc_value("api_calls", count=1)

# Connect the handler to the crawler-bound dispatcher
crawler.signals.connect("response_received", on_response)
```
