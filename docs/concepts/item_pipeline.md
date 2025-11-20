
Once an [item](items.md) is scraped by a spider, it is passed to the *Item Pipeline*, where it moves through
a series of components executed in sequence.

Each pipeline component is a Python class that defines a method to process incoming items.
The component can modify the item, validate it, store it, or decide to drop it entirely—controlling whether it
continues through the pipeline.

Common uses of item pipelines include:

* Validating items to ensure required fields are present
* Detecting and removing duplicate items
* Cleaning or normalizing HTML data
* Route items to external sinks (DB, message queue) independent from file exporters.


## Pipeline API

Item pipelines are async handlers that run when `item_scraped` is emitted by the engine (after spider parsing).

To create a pipeline component, define a class with an `async def process_item(self, item: Item, spider) -> Item | None:` method.
The method receives the scraped `item` and the `spider` that produced it.

Example pipeline that validates required fields and marks items to be dropped via metadata:

```python
import logging

from qcrawl.pipelines.base import ItemPipeline, DropItem
from qcrawl.core.item import Item

logger = logging.getLogger(__name__)

class ValidationPipeline(ItemPipeline):
    """Validate required fields and raise DropItem for invalid items."""

    def __init__(self, required=None):
        super().__init__()
        self.required = set(required or ["title"])

    async def process_item(self, item: Item, spider) -> Item | None:
        data = getattr(item, "data", None)
        if not isinstance(data, dict):
            logger.error("ValidationPipeline: item.data is not a dict; dropping item")
            raise DropItem("invalid item.data type")

        missing = [k for k in self.required if not data.get(k)]
        if missing:
            logger.debug("ValidationPipeline: missing keys %s; dropping item", missing)
            raise DropItem(f"Missing required fields: {missing}")

        # normalization example
        if "price" in data:
            try:
                data["price"] = float(data["price"])
            except Exception:
                data.pop("price", None)

        return item
```


## Best practices

- If you must prevent export, prefer raising DropItem from a pipeline so PipelineManager can handle logging and metrics.
- Keep `item.data` serializable; exporters operate on `item.data` only.
- Prefer in-spider transforms for deterministic ordering. Use signal-based/global pipelines for concerns like dedupe, DB writes, and metrics.
- Register pipelines early (in spider open hooks or via `PipelineManager.from_settings`) so pipeline handlers are connected before CLI-installed exporters.
- Be defensive: validate item structure and types to avoid runtime errors.
