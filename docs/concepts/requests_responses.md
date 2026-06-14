
qCrawl uses `Request` and `Page` (response) objects to handle HTTP interactions during the crawling process.

## Request

`Request` is the canonical object for instructing the crawler what to fetch. It contains `url`, `headers`, `priority`,
immutable `body` (bytes), and a `meta` dict for crawler, spider, middleware, and metadata.


### Serialization

- `Request.to_bytes()` serializes to a compact binary format using [Msgspec](https://jcristharif.com/msgspec/).
- `Request.from_bytes()` or `Request.from_dict()` deserialize from bytes/dict; both methods validate types and raise `TypeError` for malformed input.


### Creating and yielding requests

A `Request` with custom headers is yielded from a spider.

```python
from qcrawl.core.request import Request

req = Request(
    url="https://example.com/page",
    headers={"X-Custom-Header": "value"},
    priority=5,
    meta={"depth": 2}
)
yield req  # from spider's parse(), start_requests(), or middleware
```

`body` must be `bytes`. For a JSON request, pass `json=` instead — it serializes
the value and sets `Content-Type: application/json` (pass `body` *or* `json`, not
both):

```python
yield Request(url="https://api.example.com/items", method="POST", json={"q": "term"})
```

Use `callback` (a spider method or its name) and `cb_kwargs` to route this
request's response to a method other than `parse` — see
[Per-request callbacks](spiders.md#per-request-callbacks).

### Accessing response data

Inside `Spider.parse()`, you receive a `Page` object representing the HTTP response:

```python
async def parse(self, response: Page):
    url = response.url
    status = response.status_code
    content = response.content  # bytes
    headers = response.headers
    request = response.request  # the original Request object
    # Process the response content...
```

## Page (response)

`Page` is the response object produced by the `Downloader` and passed to `Spider.parse()`.
It provides attributes (`content`, `status_code`, `headers`, `url`, `request`) and methods (`text()`, `json()`).


### Creating Page objects (manually)

You typically do not create `Page` objects yourself; they are produced by the downloader. For unit tests or middleware
mocks you can construct one explicitly:

```python
from qcrawl.core.response import Page
fake_response = Page(
    url="https://example.com/fake",
    status_code=200,
    content=b"<html>...</html>",
    headers={"Content-Type": "text/html"},
    request=original_request  # optional, link to the originating Request
)
```

### Best practices

- Use `Request.copy()` before mutating `meta` or `headers` in middleware or spider code to avoid accidental shared-mutation bugs.
- Use `meta` only for crawler-internal flags (depth tracking, retry counters, etc.). Never store scraped data here — use `Item` instead.
