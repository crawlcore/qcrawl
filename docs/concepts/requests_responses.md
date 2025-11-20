
qCrawl uses `Request` and `Page` (response) objects to handle HTTP interactions during the crawling process.

## Request

`Request` is the canonical object used to schedule HTTP work. It carries URL, headers, priority,
immutable `body` (bytes), and a `meta` dict for crawler, spider, middleware, and metadata.


### Serialization

!!! warning "Not implemented yet"

    Serialization is not implemented yet. It will be used for persisting requests into queues. I will implement it together with support of Redis backend.

- Use `Request.to_bytes()` to persist requests into queues in binary format using MessagePack.
- Reconstruct with `Request.from_bytes()` or `Request.from_dict()`; these methods validate types and raise `TypeError` for malformed input.


### Best practices

- Always use `Request.copy()` before mutating `meta` or `headers` in middleware or spider code to avoid accidental shared-mutation bugs.
- `meta` is for internal flags (depth, retry accounting). Avoid putting exportable scraped fields into `meta` (use `Item.data` instead).


### Creating and scheduling requests

A `Request` with custom headers is scheduled via scheduler (or yielded from a spider).

```python
from qcrawl.core.request import Request
req = Request(
    url="https://example.com/page",
    headers={"X-Custom-Header": "value"},
    priority=5,
    meta={"depth": 2}
)
yield req  # from spider parse() or middleware
```

### Accessing response data

In your spider's `parse()` method, you receive a `Page` object representing the HTTP response. You can access its properties as follows:

```python
async def parse(self, response: Page):
    url = response.url
    status = response.status_code
    content = response.content  # bytes
    headers = response.headers  # dict-like
    # Process the response content...
```

## Page (response)

`Page` is the downloader representation returned by the `Downloader` and received in `Spider.parse()`; it exposes
`content`, `status_code`, `headers`, `url`, and `request`.


### Creating Page objects

You typically do not construct Page objects manually; they are produced by the downloader. For tests or middleware
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
