
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

Item pipelines process each scraped item on the engine's data path, before it is exported. The engine passes each item through the pipeline chain; items that pass are emitted as `item_scraped`, while items a pipeline drops (by raising `DropItem`) are emitted as `item_dropped`.

To create a pipeline component, define a class with an `async def process_item(self, item: Item, spider) -> Item:` method.
The method receives the scraped `item` and the `spider` that produced it.

Example pipeline that validates required fields and drops invalid items by raising `DropItem`:

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

    async def process_item(self, item: Item, spider) -> Item:
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


## Data cleaning pipelines

Pipelines are ideal for cleaning and normalizing extracted data:

### Clean text data
```python
import re
from qcrawl.pipelines.base import ItemPipeline
from qcrawl.core.item import Item

class TextCleaningPipeline(ItemPipeline):
    """Clean and normalize text fields."""

    def clean_text(self, text):
        """Remove extra whitespace and normalize text."""
        if not text:
            return None

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)

        # Strip leading/trailing whitespace
        text = text.strip()

        return text if text else None

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        # Clean text fields
        for field in ["title", "description", "author"]:
            if field in data and isinstance(data[field], str):
                data[field] = self.clean_text(data[field])

        return item
```

### Parse and normalize prices
```python
import re
from qcrawl.pipelines.base import ItemPipeline
from qcrawl.core.item import Item

class PriceNormalizationPipeline(ItemPipeline):
    """Parse and normalize price fields."""

    def parse_price(self, price_text):
        """Extract numeric price from text."""
        if not price_text:
            return None

        # Remove currency symbols and commas
        cleaned = re.sub(r'[$€£¥,]', '', str(price_text))

        # Extract first number
        match = re.search(r'\d+\.?\d*', cleaned)
        if match:
            try:
                return float(match.group())
            except ValueError:
                return None

        return None

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        # Parse price field
        if "price" in data:
            data["price"] = self.parse_price(data["price"])

        return item
```

### Parse dates
```python
from datetime import datetime
from qcrawl.pipelines.base import ItemPipeline
from qcrawl.core.item import Item

class DateParsingPipeline(ItemPipeline):
    """Parse various date formats to ISO format."""

    def __init__(self):
        super().__init__()
        self.date_formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%Y-%m-%dT%H:%M:%S",
        ]

    def parse_date(self, date_text):
        """Parse various date formats."""
        if not date_text:
            return None

        # Try each format
        for fmt in self.date_formats:
            try:
                return datetime.strptime(str(date_text).strip(), fmt).isoformat()
            except ValueError:
                continue

        return None

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        # Parse date fields
        for field in ["published", "updated", "created_at"]:
            if field in data:
                data[field] = self.parse_date(data[field])

        return item
```


## Data transformation pipelines

Transform and enrich item data:

### Type conversion
```python
from qcrawl.pipelines.base import ItemPipeline
from qcrawl.core.item import Item

class TypeConversionPipeline(ItemPipeline):
    """Convert field types."""

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        # Convert numeric fields
        for field in ["quantity", "stock", "views"]:
            if field in data:
                try:
                    data[field] = int(data[field])
                except (ValueError, TypeError):
                    data[field] = None

        # Convert boolean fields
        for field in ["in_stock", "featured"]:
            if field in data:
                data[field] = str(data[field]).lower() in {"true", "1", "yes"}

        return item
```

### Data enrichment
```python
from qcrawl.pipelines.base import ItemPipeline
from qcrawl.core.item import Item
from datetime import datetime, timezone

class EnrichmentPipeline(ItemPipeline):
    """Add computed or metadata fields."""

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})
        metadata = getattr(item, "metadata", {})

        # Add scrape timestamp
        metadata["scraped_at"] = datetime.now(timezone.utc).isoformat()

        # Add spider name
        metadata["spider_name"] = spider.name

        # Compute derived fields
        if "price" in data and "quantity" in data:
            data["total_value"] = data["price"] * data["quantity"]

        return item
```


## Advanced validation pipelines

Complex validation beyond simple field checks:

### Schema validation
```python
from qcrawl.pipelines.base import ItemPipeline, DropItem
from qcrawl.core.item import Item

class SchemaValidationPipeline(ItemPipeline):
    """Validate item structure and types."""

    def __init__(self):
        super().__init__()
        self.schema = {
            "title": str,
            "price": (int, float),
            "url": str,
        }

    def validate_item(self, data):
        """Validate data against schema."""
        for field, expected_type in self.schema.items():
            if field not in data:
                return False, f"Missing required field: {field}"

            if data[field] is not None and not isinstance(data[field], expected_type):
                return False, f"Invalid type for {field}: expected {expected_type}"

        return True, None

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        valid, error = self.validate_item(data)
        if not valid:
            raise DropItem(error)

        return item
```

### Business logic validation
```python
from qcrawl.pipelines.base import ItemPipeline, DropItem
from qcrawl.core.item import Item

class BusinessValidationPipeline(ItemPipeline):
    """Validate business rules."""

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        # Price must be positive
        if "price" in data and data["price"] is not None:
            if data["price"] <= 0:
                raise DropItem("Invalid price: must be positive")

        # Quantity must be non-negative
        if "quantity" in data and data["quantity"] is not None:
            if data["quantity"] < 0:
                raise DropItem("Invalid quantity: cannot be negative")

        # URL must be valid
        if "url" in data:
            if not data["url"].startswith(("http://", "https://")):
                raise DropItem("Invalid URL format")

        return item
```


## Deduplication pipeline

Remove duplicate items:

```python
from qcrawl.pipelines.base import ItemPipeline, DropItem
from qcrawl.core.item import Item

class DeduplicationPipeline(ItemPipeline):
    """Drop duplicate items based on unique key."""

    def __init__(self):
        super().__init__()
        self.seen_ids = set()

    async def process_item(self, item: Item, spider) -> Item:
        data = getattr(item, "data", {})

        # Use URL as unique identifier
        item_id = data.get("url")
        if not item_id:
            return item

        if item_id in self.seen_ids:
            raise DropItem(f"Duplicate item: {item_id}")

        self.seen_ids.add(item_id)
        return item
```


## Registering pipelines

Configure pipelines in spider settings:

```python
class MySpider(Spider):
    name = "my_spider"
    start_urls = ["https://example.com"]

    custom_settings = {
        "PIPELINES": {
            "myproject.pipelines.TextCleaningPipeline": 100,
            "myproject.pipelines.PriceNormalizationPipeline": 200,
            "myproject.pipelines.DateParsingPipeline": 300,
            "myproject.pipelines.ValidationPipeline": 400,
            "myproject.pipelines.DeduplicationPipeline": 500,
        }
    }
```

**Pipeline order:**
- Lower numbers run first (100, 200, 300...)
- Clean → Transform → Validate → Deduplicate


## Best practices

- **Separation of concerns**: Extract in spiders, clean/transform in pipelines
- **Keep spiders simple**: Only basic `.strip()` in spiders, complex cleaning in pipelines
- **Pipeline order matters**: Clean before validate, validate before dedupe
- **Raise DropItem**: Use `DropItem` exception to prevent export
- **Keep data serializable**: Exporters operate on `item.data` only
- **Be defensive**: Validate item structure and types to avoid runtime errors
- **Log appropriately**: Use logger for debugging pipeline issues
- **Test pipelines**: Unit test pipeline logic independently from spiders
- **Reuse pipelines**: Share common pipelines across multiple spiders

See also: [Items](items.md), [Data Extraction](../advanced-topics/data_extraction.md), [Exporters](exporters.md)
