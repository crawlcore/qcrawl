
## Walk-through of a simple spider

In order to show you how qCrawl works I’ll walk you through an example of a simple Spider using the simplest way to run a spider.

```py title="quotes_spider.py"
from qcrawl.core.spider import Spider
from qcrawl.core.response import Page

class QuotesSpider(Spider):

    name = "quotes"
    start_urls = ["https://quotes.toscrape.com/"]

    async def parse(self, response: Page):
        rv = self.response_view(response)

        for q in rv.doc.cssselect('div.quote'):
            text_nodes = q.cssselect('span.text')
            author_nodes = q.cssselect('small.author')

            if not text_nodes or not author_nodes:
                continue

            text = text_nodes[0].text_content().strip()
            author = author_nodes[0].text_content().strip()

            yield {
                "text": text,
                "author": author,
                "url": response.url,
            }

        next_nodes = rv.doc.cssselect('li.next a')
        if next_nodes:
            href = next_nodes[0].get('href')
            if href:
                yield self.follow(response, href)
```

Put this code in a text file, name it something like `quotes_spider.py`, and run the spider using the
CLI (Command-Line Interface) command:

```shell
qcrawl --export quotes.json --export-format json quotes_spider:QuotesSpider
```

Upon completion, the `quotes.json` file will include the extracted items.

Example item (JSON):

```json
[
  {
    "text": "I have not failed. I've just found 10,000 ways that won't work.",
    "author": "Thomas A. Edison",
    "url": "https://quotes.toscrape.com/..."
  }
]
```

## How does it work?
When you run `qcrawl quotes_spider:QuotesSpider` qCrawl does the following:

1. qCrawl locates the `QuotesSpider` class, instantiate it, and passed to the crawling engine.
2. The crawler schedules HTTP requests for the URLs in the spider's `start_urls` attribute.
3. Upon receiving a successful response (HTTP 200), it is routed to the default callback method: `parse()`.
4. Inside the `parse()` method:

    A CSS selector `('div.quote')` is used to locate all quote containers on the page.
    For each match, the spider extracts:

    - Quote text → `q.cssselect('span.text')[0].text_content().strip()`
    - Author name → `q.cssselect('small.author')[0].text_content().strip()`

    Extracted data is yielded as a structured Python dictionary:

    ```python
    {
        'text': '“Be yourself; everyone else is already taken.”',
        'author': 'Oscar Wilde'
    }
    ```

    These items are collected and can be exported to files (JSON, CSV, etc.) or processed further via [item pipelines](../concepts/item_pipeline.md).


5. The spider searches for a “Next” link using a CSS selector `rv.doc.cssselect('li.next a')`.

    If found:

    * A new request is scheduled to the next page URL using `yield self.follow(response, href)`.
    * The same `parse()` method is reused as the callback until no “Next” link is present.


6. An item processed according to the [CLI options](../concepts/cli.md):


``` mermaid
flowchart LR
  A[Spider] -->|yield Item / dict| C
  C["Item Pipeline<br>(drop / transform)"]
  C --> E["Exporter<br>(format data)"]
  E --> F["Storage (save data)"]
  E --> G["Stdout (print data)"]

  D["CLI options"]:::CLI

  %% Styles
  classDef CLI fill:#333,color:#fff,stroke:#777,stroke-width:2px
```

!!! note

    This example is using data [exporter](../concepts/exporters.md) to generate the JSON file, you can easily change the export format (XML, CSV, ...)
    or the storage backend. You can also use [item pipeline](../concepts/item_pipeline.md) to drop / transform data.

## What’s next?
The next steps are to [install qCrawl](installation.md), follow through the tutorial and [join the community](https://discord.gg/yT54ff6STY) on Discord.<br>
Thanks for your interest!
