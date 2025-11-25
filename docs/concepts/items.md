
The goal of scraping is to extract structured data from unstructured sources, typically, web pages.
qCrawl supports extracting structured data as `Item` objects and plain Python `dict`'s.

## Python's dict as container
In the simplest case, you can yield a plain Python `dict` from your spider's `parse()` method.
The engine will automatically wrap it into an `Item` before emitting `item_scraped` signals and passing to exporters.
Example:

```python
    async def parse(self, response):
        rv = self.response_view(response)

        # Extract data using CSS selectors
        title_nodes = rv.doc.cssselect(".title")
        title = title_nodes[0].text_content().strip() if title_nodes else None
        yield {"title": title, "url": response.url}
```

However, using `dict` has several limitations:

  - You cannot attach internal metadata (depth, timestamps, flags, etc).
  - You cannot reuse the same container to build an item step-by-step.
  - You miss out on convenient `Item` helpers and readable representation.

``` python title="Bad practice"
# depth ends up in exported output
yield {
    "title": title,
    "url": response.url,
    "depth": response.request.meta.get("depth")
}
```

``` python title="Good practice"
# keep exportable fields in .data and internal state in .metadata
from qcrawl.core.item import Item

it = Item(
    data={"title": title, "url": response.url},
    metadata={"depth": response.request.meta.get("depth")})
yield it
```

## qCrawl Item as container
An `Item` is a simple mutable container defined in `qcrawl/core/item.py`.

It has two parts:

  - `data` (dict) — scraped fields intended for export.
  - `metadata` (dict) — internal metadata (depth, timestamps, internal flags).

`Item` implements convenient dict-like helpers: indexing, `get()`, `keys()`, `values()`, `items()` and a readable `__repr__`.


### How to produce items in `parse()`

You may yield either a plain `dict` or an `Item` instance:

  - When a `dict` is yielded, the engine wraps it into an `Item` before emitting `item_scraped` signals.
  - When you yield an `Item`, it is passed through as-is.

Example (inside a spider `parse` method)

```python
    async def parse(self, response):
        rv = self.response_view(response)

        # Yield a plain dict (engine wraps to Item)
        title_nodes = rv.doc.cssselect(".title")
        yield {"title": title_nodes[0].text_content().strip() if title_nodes else None}

        # Yield an Item explicitly
        from qcrawl.core.item import Item
        it = Item(
            data={"title": "Explicit title"},
            metadata={"depth": 1})
        it["price"] = 9.99
        yield it
```

### Best practices

- Use string keys for scraped fields.
- Keep `item.data` serializable (primitives, lists, dicts). Avoid complex objects that JSON/pickle exporters cannot serialize.
- Keep metadata small and internal-only.
- Reuse `Item` when building a result step-by-step (it's mutable), but be careful with shared references in async code.
- Prefer yielding `dict` for simple cases and `Item` when you need to attach metadata explicitly.

### Common pitfalls

- Exporter ignores items without `.data` or if `.data` is not a `dict`.
- Serializing non-JSON-friendly values in `item.data` will fail exporters (JSON/XML/CSV).
- Do not rely on `Item.metadata` being exported — exporters use `.data` only.
