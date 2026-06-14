
## Extraction vs Pipeline Processing

**Best practice:** Separate extraction from transformation.

**In the Spider (extraction):**

- Extract raw or lightly cleaned data
- Use basic `.strip()` for whitespace
- Check element existence
- Provide fallbacks for optional fields

**In Item Pipelines (transformation):**

- Complex text cleaning (regex, normalization)
- Price/date parsing and formatting
- Type conversions and validation
- Business logic and data enrichment

**Example pattern:**

```python
# Spider: Extract raw data with minimal cleaning
async def parse(self, response):
    rv = self.response_view(response)

    for product in rv.doc.cssselect(".product"):
        title = product.cssselect("h2")
        price = product.cssselect(".price")

        yield {
            "title": title[0].text_content().strip() if title else None,
            "price": price[0].text_content().strip() if price else None,  # Raw: "$1,234.56"
            "url": response.url,
        }

# Pipeline: Clean and transform (in pipelines, not spider)
class DataCleaningPipeline(ItemPipeline):
    async def process_item(self, item, spider):
        # Transform "$1,234.56" → 1234.56
        if "price" in item.data:
            item.data["price"] = self.parse_price(item.data["price"])
        return item
```

For data cleaning, transformation, and validation examples, see [Item Pipeline](../concepts/item_pipeline.md).

## Structured data extraction

### Extract with Item class (recommended)
```python
from qcrawl.core.item import Item

async def parse(self, response):
    rv = self.response_view(response)

    for product in rv.doc.cssselect(".product"):
        # Extract with fallbacks
        title = product.cssselect("h2.title")
        price = product.cssselect(".price")
        rating = product.cssselect(".rating")

        yield Item(data={
            "title": title[0].text_content().strip() if title else None,
            "price": price[0].text_content().strip() if price else None,
            "rating": float(rating[0].get("data-rating")) if rating else None,
            "url": response.url,
        })
```
For full info on using Items, see the [Item documentation](../concepts/items.md).


### Extract with plain dict
```python
async def parse(self, response):
    rv = self.response_view(response)

    for article in rv.doc.cssselect("article"):
        title_elem = article.cssselect("h2")
        author_elem = article.cssselect(".author")
        date_elem = article.cssselect("time")

        yield {
            "title": title_elem[0].text_content().strip() if title_elem else None,
            "author": author_elem[0].text_content().strip() if author_elem else None,
            "published": date_elem[0].get("datetime") if date_elem else None,
            "url": response.url,
        }
```


## Nested data extraction

### Extract nested attributes
```python
async def parse(self, response):
    rv = self.response_view(response)

    for product in rv.doc.cssselect(".product"):
        # Extract nested key-value pairs
        attributes = {}
        for attr in product.cssselect(".attribute"):
            key_elem = attr.cssselect(".key")
            value_elem = attr.cssselect(".value")

            if key_elem and value_elem:
                key = key_elem[0].text_content().strip()
                value = value_elem[0].text_content().strip()
                attributes[key] = value

        # Extract list of images
        images = [
            img.get("src")
            for img in product.cssselect("img.gallery")
            if img.get("src")
        ]

        yield {
            "title": product.cssselect("h2")[0].text_content(),
            "attributes": attributes,
            "images": images,
        }
```

### Extract hierarchical data
```python
async def parse(self, response):
    rv = self.response_view(response)

    for category in rv.doc.cssselect(".category"):
        category_name = category.cssselect("h2")[0].text_content()

        # Extract nested subcategories
        subcategories = []
        for subcat in category.cssselect(".subcategory"):
            subcat_name = subcat.cssselect("h3")[0].text_content()

            # Extract items within subcategory
            items = [
                item.text_content().strip()
                for item in subcat.cssselect("li")
            ]

            subcategories.append({
                "name": subcat_name,
                "items": items
            })

        yield {
            "category": category_name,
            "subcategories": subcategories
        }
```


## API and JSON data

### Parse JSON responses
```python
from qcrawl.core.request import Request

async def parse(self, response):
    # Parse JSON response
    data = response.json()

    # Extract items
    for item in data.get("results", []):
        yield {
            "id": item.get("id"),
            "name": item.get("name"),
            "price": item.get("price"),
            "stock": item.get("stock_count", 0)
        }

    # Handle pagination
    next_page = data.get("next_page")
    if next_page:
        yield Request(url=next_page)
```

### Parse JSON within HTML
```python
import json

async def parse(self, response):
    rv = self.response_view(response)

    # Extract JSON from script tag
    script_tags = rv.doc.cssselect("script[type='application/ld+json']")

    for script in script_tags:
        try:
            data = json.loads(script.text_content())

            if data.get("@type") == "Product":
                yield {
                    "name": data.get("name"),
                    "price": data.get("offers", {}).get("price"),
                    "currency": data.get("offers", {}).get("priceCurrency"),
                    "rating": data.get("aggregateRating", {}).get("ratingValue"),
                }
        except (json.JSONDecodeError, AttributeError):
            continue
```

### Handle GraphQL APIs
```python
async def start_requests(self):
    query = """
    query GetProducts($page: Int!) {
        products(page: $page) {
            id
            name
            price
        }
    }
    """

    yield Request(
        url="https://api.example.com/graphql",
        method="POST",
        json={"query": query, "variables": {"page": 1}},
    )

async def parse(self, response):
    data = response.json()

    products = data.get("data", {}).get("products", [])

    for product in products:
        yield {
            "id": product["id"],
            "name": product["name"],
            "price": product["price"]
        }
```


## Sitemap crawling

### Parse XML sitemaps
```python
from qcrawl.core.spider import Spider
from qcrawl.core.request import Request
import xml.etree.ElementTree as ET

class SitemapSpider(Spider):
    name = "sitemap_spider"
    start_urls = ["https://example.com/sitemap.xml"]

    async def parse(self, response):
        # Parse sitemap XML
        root = ET.fromstring(response.text())
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        # Extract URLs
        for url_elem in root.findall(".//ns:url/ns:loc", namespace):
            page_url = url_elem.text

            # Optionally filter URLs
            if "/products/" in page_url:
                yield Request(url=page_url)

        # Handle sitemap index (sitemap of sitemaps)
        for sitemap in root.findall(".//ns:sitemap/ns:loc", namespace):
            yield Request(url=sitemap.text)
```

### Extract sitemap metadata
```python
async def parse(self, response):
    root = ET.fromstring(response.text())
    namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    for url_elem in root.findall(".//ns:url", namespace):
        loc = url_elem.find("ns:loc", namespace)
        lastmod = url_elem.find("ns:lastmod", namespace)
        priority = url_elem.find("ns:priority", namespace)

        if loc is not None:
            yield Request(
                url=loc.text,
                meta={
                    "lastmod": lastmod.text if lastmod is not None else None,
                    "priority": priority.text if priority is not None else None
                }
            )
```


## Best practices

- **Separate extraction from transformation**: Extract raw data in spiders, clean/transform/validate in pipelines
- **Use Item for structured data**: Wrap data in `Item` class for pipeline processing
- **Extract data defensively**: Always check if elements exist before accessing
- **Document data schema**: Comment expected data structure and field types
- **Store raw URLs**: Include source URL in extracted data for debugging (use Items metadata)

See also: [Item Pipeline](../concepts/item_pipeline.md), [Items](../concepts/items.md)
