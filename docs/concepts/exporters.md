
After a spider extracts data from web-page it's often necessary to store that data in a structured format
for further analysis or processing.

qCrawl provides exporters that serialize scraped items and write them (streamed or buffered) to a destination.
They can be used via the CLI or programmatically.

**Format comparison:**

| Format     | Streaming  | Best For                            | File Size  |
|------------|------------|-------------------------------------|------------|
| **NDJSON** | ✅ Yes      | Large datasets, streaming pipelines | Medium     |
| **JSON**   | ❌ No       | API responses, small datasets       | Medium     |
| **CSV**    | ✅ Yes      | Excel, data analysis, flat data     | Small      |
| **XML**    | ❌ No       | Legacy systems, SOAP APIs           | Large      |


## Examples - CLI usage

### JSON Lines (default)
```bash
# One JSON object per line (streaming-friendly, memory efficient)
qcrawl quotes_css_spider:Quotes --export output.jsonl

# Or explicitly specify format
qcrawl quotes_css_spider:Quotes --export output.jsonl --export-format ndjson
```

**Output (output.jsonl):**
```json
{"url": "https://quotes.toscrape.com/", "text": "The world as we...", "author": "Albert Einstein", "tags": ["change", "world"]}
{"url": "https://quotes.toscrape.com/", "text": "It is our choices...", "author": "J.K. Rowling", "tags": ["abilities", "choices"]}
```

### JSON (full array)
```bash
# All items in a single JSON array
qcrawl quotes_css_spider:Quotes --export output.json --export-format json
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
qcrawl quotes_css_spider:Quotes --export output.csv --export-format csv
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
qcrawl quotes_css_spider:Quotes --export output.xml --export-format xml
```

**Output (output.xml):**
```xml
<?xml version="1.0" encoding="utf-8"?>
<items>
  <item>
    <url>https://quotes.toscrape.com/</url>
    <text>The world as we...</text>
    <author>Albert Einstein</author>
    <tags>['change', 'world']</tags>
  </item>
</items>
```

The XML exporter writes each field value via `str(value)`, so non-scalar
values (lists, dicts) are stringified rather than expanded into child elements.
Flatten or pre-format such fields in a pipeline if you need structured XML.

### Streaming vs Buffered Mode

```bash
# Buffered (default): Collects items in memory, writes at end
qcrawl quotes_css_spider:Quotes --export output.json --export-mode buffered

# Streaming: Writes items immediately as they're scraped (memory efficient)
qcrawl quotes_css_spider:Quotes --export output.jsonl --export-mode stream

# Adjust buffer size (items to collect before writing)
qcrawl quotes_css_spider:Quotes --export output.jsonl --export-buffer-size 1000
```



## Examples - Programmatic usage

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
from quotes_spider import QuotesSpider  # replace with your spider
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
