
After a spider extracts data from web-page it's often necessary to store that data in a structured format
for further analysis or processing.

qCrawl provides exporters that serialize scraped items and write them (streamed or buffered) to a destination.
They can be used via the CLI or programmatically.

## Serialization formats

- `ndjson` — newline-delimited JSON (one JSON object per line)
- `json` — full JSON array (can be buffered or streamed)
- `csv` — CSV file with detected columns
- `xml` — simple XML wrapper around items


## CLI usage

The CLI wires exporters automatically.

Example:

write JSON (buffered):
```bash
qcrawl my.module:MySpider --export results.json --export-format json --export-mode buffered
```

write newline-delimited JSON to stdout:
```bash
qcrawl my.module:MySpider --export - --export-format ndjson
```

See [CLI documentation](cli.md) for details on available options.


## Programmatic usage

```python hl_lines="3"
# Async usage (recommended)
import asyncio
from quotes_spider import QuotesSpider # replace with your spider
from qcrawl.runner import SpiderRunner

runner = SpiderRunner(
    settings={
        "export": "exports/quotes.ndjson",
        "export_format": "ndjson",
        "export_mode": "buffered",
        "export_buffer_size": 500,
        "log_level": "INFO",
        "concurrency": 50,
    }
)

async def main() -> None:
    # Await the async entrypoint from your event loop
    await runner.crawl(QuotesSpider)

if __name__ == "__main__":
    asyncio.run(main())


# Synchronous convenience (for simple scripts)
from examples.quotes_spider import QuotesSpider
from qcrawl.runner import SpiderRunner

runner = SpiderRunner(
    settings={
        "export": "exports/quotes.ndjson",
        "export_format": "ndjson",
        "log_level": "INFO",
        "concurrency": 50,
    }
)

# Blocks and is implemented via asyncio.run(); raises if an event loop is already running.
runner.crawl_sync(QuotesSpider)
```
