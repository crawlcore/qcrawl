"""Tests for qcrawl.pipelines.validation.ValidationPipeline"""

import pytest

from qcrawl.core.item import Item
from qcrawl.core.spider import Spider
from qcrawl.pipelines.base import DropItem
from qcrawl.pipelines.validation import ValidationPipeline


class SpiderWithRequiredFields(Spider):
    """Spider with REQUIRED_FIELDS defined."""

    name = "with_required"
    start_urls = ["http://example.com"]
    REQUIRED_FIELDS = ["title", "url"]

    async def parse(self, response):
        yield {}


class SpiderWithRequiredFieldsTuple(Spider):
    """Spider with REQUIRED_FIELDS as tuple."""

    name = "with_required_tuple"
    start_urls = ["http://example.com"]
    REQUIRED_FIELDS = ("title", "price")

    async def parse(self, response):
        yield {}


class SpiderWithRequiredFieldsSet(Spider):
    """Spider with REQUIRED_FIELDS as set."""

    name = "with_required_set"
    start_urls = ["http://example.com"]
    REQUIRED_FIELDS = {"url"}

    async def parse(self, response):
        yield {}


# Note: Uses dummy_spider fixture from tests/conftest.py


@pytest.fixture
def spider_with_required():
    """Provide a spider with required fields."""
    return SpiderWithRequiredFields()


# No Required Fields Tests


@pytest.mark.asyncio
async def test_allows_all_items_when_no_required_fields(dummy_spider):
    """Pipeline allows all items when spider has no REQUIRED_FIELDS."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "Page"})

    result = await pipeline.process_item(item, dummy_spider)

    assert result is item


@pytest.mark.asyncio
async def test_allows_empty_item_when_no_required_fields(dummy_spider):
    """Pipeline allows empty items when spider has no REQUIRED_FIELDS."""
    pipeline = ValidationPipeline()
    item = Item(data={})

    result = await pipeline.process_item(item, dummy_spider)

    assert result is item


# Required Fields Present Tests


@pytest.mark.asyncio
async def test_allows_item_with_all_required_fields(spider_with_required):
    """Pipeline allows items with all required fields present and non-empty."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "Article", "url": "https://example.com", "extra": "data"})

    result = await pipeline.process_item(item, spider_with_required)

    assert result is item


@pytest.mark.asyncio
async def test_allows_item_with_only_required_fields(spider_with_required):
    """Pipeline allows items with exactly the required fields."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "Article", "url": "https://example.com"})

    result = await pipeline.process_item(item, spider_with_required)

    assert result is item


# Missing Required Fields Tests


@pytest.mark.asyncio
async def test_drops_item_missing_required_field(spider_with_required):
    """Pipeline drops items missing a required field."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "Article"})  # Missing 'url'

    with pytest.raises(DropItem, match="Missing required field: url"):
        await pipeline.process_item(item, spider_with_required)


@pytest.mark.asyncio
async def test_drops_item_missing_all_required_fields(spider_with_required):
    """Pipeline drops items missing all required fields."""
    pipeline = ValidationPipeline()
    item = Item(data={"extra": "data"})  # Missing both 'title' and 'url'

    # Should fail on first missing field encountered
    with pytest.raises(DropItem, match="Missing required field: (title|url)"):
        await pipeline.process_item(item, spider_with_required)


# Falsy/Empty Required Fields Tests


@pytest.mark.asyncio
async def test_drops_item_with_empty_string_required_field(spider_with_required):
    """Pipeline drops items with empty string in required field."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "", "url": "https://example.com"})

    with pytest.raises(DropItem, match="Required field empty: title"):
        await pipeline.process_item(item, spider_with_required)


@pytest.mark.asyncio
async def test_drops_item_with_none_required_field(spider_with_required):
    """Pipeline drops items with None in required field."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "Article", "url": None})

    with pytest.raises(DropItem, match="Required field empty: url"):
        await pipeline.process_item(item, spider_with_required)


@pytest.mark.asyncio
async def test_drops_item_with_zero_required_field(spider_with_required):
    """Pipeline drops items with 0 in required field (falsy)."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": 0, "url": "https://example.com"})

    with pytest.raises(DropItem, match="Required field empty: title"):
        await pipeline.process_item(item, spider_with_required)


@pytest.mark.asyncio
async def test_drops_item_with_false_required_field(spider_with_required):
    """Pipeline drops items with False in required field (falsy)."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": "Article", "url": False})

    with pytest.raises(DropItem, match="Required field empty: url"):
        await pipeline.process_item(item, spider_with_required)


@pytest.mark.asyncio
async def test_drops_item_with_empty_list_required_field(spider_with_required):
    """Pipeline drops items with empty list in required field (falsy)."""
    pipeline = ValidationPipeline()
    item = Item(data={"title": [], "url": "https://example.com"})

    with pytest.raises(DropItem, match="Required field empty: title"):
        await pipeline.process_item(item, spider_with_required)


# Different REQUIRED_FIELDS Types


@pytest.mark.asyncio
async def test_handles_required_fields_as_tuple():
    """Pipeline handles REQUIRED_FIELDS defined as tuple."""
    pipeline = ValidationPipeline()
    spider = SpiderWithRequiredFieldsTuple()

    valid_item = Item(data={"title": "Product", "price": "9.99"})
    result = await pipeline.process_item(valid_item, spider)
    assert result is valid_item

    invalid_item = Item(data={"title": "Product"})  # Missing 'price'
    with pytest.raises(DropItem, match="Missing required field: price"):
        await pipeline.process_item(invalid_item, spider)


@pytest.mark.asyncio
async def test_handles_required_fields_as_set():
    """Pipeline handles REQUIRED_FIELDS defined as set."""
    pipeline = ValidationPipeline()
    spider = SpiderWithRequiredFieldsSet()

    valid_item = Item(data={"url": "https://example.com"})
    result = await pipeline.process_item(valid_item, spider)
    assert result is valid_item

    invalid_item = Item(data={"title": "Page"})  # Missing 'url'
    with pytest.raises(DropItem, match="Missing required field: url"):
        await pipeline.process_item(invalid_item, spider)


@pytest.mark.asyncio
async def test_handles_none_required_fields_attribute():
    """Pipeline handles spider with REQUIRED_FIELDS = None."""

    class SpiderWithNoneRequired(Spider):
        name = "none_required"
        start_urls = ["http://example.com"]
        REQUIRED_FIELDS = None

        async def parse(self, response):
            yield {}

    pipeline = ValidationPipeline()
    spider_none = SpiderWithNoneRequired()
    item = Item(data={"anything": "goes"})

    result = await pipeline.process_item(item, spider_none)
    assert result is item


# Invalid Item Validation


@pytest.mark.asyncio
async def test_rejects_item_without_data_attribute(dummy_spider):
    """Pipeline rejects items without .data attribute."""
    pipeline = ValidationPipeline()
    invalid_item = {"url": "https://example.com"}  # Plain dict, not Item

    with pytest.raises(DropItem, match="missing .data attribute"):
        await pipeline.process_item(invalid_item, dummy_spider)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rejects_item_with_non_dict_data(dummy_spider):
    """Pipeline rejects items where .data is not a dict."""
    pipeline = ValidationPipeline()

    class BadItem:
        data = "not a dict"

    with pytest.raises(DropItem, match="invalid item.data type"):
        await pipeline.process_item(BadItem(), dummy_spider)  # type: ignore[arg-type]
