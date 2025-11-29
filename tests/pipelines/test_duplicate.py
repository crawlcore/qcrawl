"""Tests for qcrawl.pipelines.duplicate.DuplicateFilterPipeline"""

import pytest

from qcrawl.core.item import Item
from qcrawl.pipelines.base import DropItem
from qcrawl.pipelines.duplicate import DuplicateFilterPipeline

# Initialization Tests


def test_pipeline_initializes_with_defaults():
    """Pipeline initializes with default key_fields."""
    pipeline = DuplicateFilterPipeline()

    assert pipeline.key_fields == ["url"]
    assert pipeline.seen == set()


def test_pipeline_initializes_with_custom_fields():
    """Pipeline initializes with custom key_fields."""
    pipeline = DuplicateFilterPipeline(key_fields=["title", "id"])

    assert pipeline.key_fields == ["title", "id"]
    assert pipeline.seen == set()


# Single Field Deduplication Tests


@pytest.mark.asyncio
async def test_allows_first_item_with_unique_url(dummy_spider):
    """Pipeline allows first item with unique URL."""
    pipeline = DuplicateFilterPipeline()
    item = Item(data={"url": "https://example.com/1", "title": "Page 1"})

    result = await pipeline.process_item(item, dummy_spider)

    assert result is item
    assert "https://example.com/1" in pipeline.seen


@pytest.mark.asyncio
async def test_drops_duplicate_item_same_url(dummy_spider):
    """Pipeline drops second item with same URL."""
    pipeline = DuplicateFilterPipeline()
    item1 = Item(data={"url": "https://example.com/1", "title": "Page 1"})
    item2 = Item(data={"url": "https://example.com/1", "title": "Page 1 Updated"})

    # First item passes
    await pipeline.process_item(item1, dummy_spider)

    # Second item with same URL is dropped
    with pytest.raises(DropItem, match="Duplicate item: https://example.com/1"):
        await pipeline.process_item(item2, dummy_spider)


@pytest.mark.asyncio
async def test_allows_items_with_different_urls(dummy_spider):
    """Pipeline allows items with different URLs."""
    pipeline = DuplicateFilterPipeline()
    item1 = Item(data={"url": "https://example.com/1"})
    item2 = Item(data={"url": "https://example.com/2"})
    item3 = Item(data={"url": "https://example.com/3"})

    result1 = await pipeline.process_item(item1, dummy_spider)
    result2 = await pipeline.process_item(item2, dummy_spider)
    result3 = await pipeline.process_item(item3, dummy_spider)

    assert result1 is item1
    assert result2 is item2
    assert result3 is item3
    assert len(pipeline.seen) == 3


# Multi-Field Composite Key Tests


@pytest.mark.asyncio
async def test_composite_key_with_multiple_fields(dummy_spider):
    """Pipeline uses composite key from multiple fields."""
    pipeline = DuplicateFilterPipeline(key_fields=["title", "author"])
    item1 = Item(data={"title": "Article", "author": "Alice", "url": "url1"})
    item2 = Item(data={"title": "Article", "author": "Bob", "url": "url2"})
    item3 = Item(data={"title": "Article", "author": "Alice", "url": "url3"})

    # First item passes
    result1 = await pipeline.process_item(item1, dummy_spider)
    assert result1 is item1

    # Different composite key (same title, different author) - passes
    result2 = await pipeline.process_item(item2, dummy_spider)
    assert result2 is item2

    # Same composite key as item1 - dropped
    with pytest.raises(DropItem, match="Duplicate item: Article\\|Alice"):
        await pipeline.process_item(item3, dummy_spider)


@pytest.mark.asyncio
async def test_composite_key_handles_missing_fields(dummy_spider):
    """Pipeline handles missing fields in composite key (treats as empty)."""
    pipeline = DuplicateFilterPipeline(key_fields=["title", "author"])
    item1 = Item(data={"title": "Article"})  # Missing 'author'
    item2 = Item(data={"title": "Article"})  # Missing 'author'

    # First item with missing field passes
    result1 = await pipeline.process_item(item1, dummy_spider)
    assert result1 is item1

    # Second item with same missing field - dropped as duplicate
    with pytest.raises(DropItem, match="Duplicate item: Article\\|"):
        await pipeline.process_item(item2, dummy_spider)


# Empty/None Key Handling


@pytest.mark.asyncio
async def test_allows_item_with_no_unique_key(dummy_spider):
    """Pipeline allows items with no identifying data (no key fields present)."""
    pipeline = DuplicateFilterPipeline(key_fields=["url"])
    item = Item(data={"title": "No URL"})  # Missing 'url' field

    # Item without unique key passes through
    result = await pipeline.process_item(item, dummy_spider)
    assert result is item
    # Nothing added to seen set (empty key not tracked)


@pytest.mark.asyncio
async def test_allows_multiple_items_with_empty_keys(dummy_spider):
    """Pipeline allows multiple items when all have empty/missing keys."""
    pipeline = DuplicateFilterPipeline(key_fields=["url"])
    item1 = Item(data={"title": "No URL 1"})
    item2 = Item(data={"title": "No URL 2"})

    result1 = await pipeline.process_item(item1, dummy_spider)
    result2 = await pipeline.process_item(item2, dummy_spider)

    assert result1 is item1
    assert result2 is item2


@pytest.mark.asyncio
async def test_handles_none_values_in_key_fields(dummy_spider):
    """Pipeline handles None values in key fields."""
    pipeline = DuplicateFilterPipeline(key_fields=["url"])
    item1 = Item(data={"url": None, "title": "Page"})
    item2 = Item(data={"url": None, "title": "Page 2"})

    # None is treated as empty string, resulting in empty unique_id
    result1 = await pipeline.process_item(item1, dummy_spider)
    result2 = await pipeline.process_item(item2, dummy_spider)

    # Both should pass (empty keys not tracked)
    assert result1 is item1
    assert result2 is item2


# Invalid Item Validation


@pytest.mark.asyncio
async def test_rejects_item_without_data_attribute(dummy_spider):
    """Pipeline rejects items without .data attribute."""
    pipeline = DuplicateFilterPipeline()
    invalid_item = {"url": "https://example.com"}  # Plain dict, not Item

    with pytest.raises(DropItem, match="missing .data attribute"):
        await pipeline.process_item(invalid_item, dummy_spider)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rejects_item_with_non_dict_data(dummy_spider):
    """Pipeline rejects items where .data is not a dict."""
    pipeline = DuplicateFilterPipeline()

    class BadItem:
        data = "not a dict"

    with pytest.raises(DropItem, match="invalid item.data type"):
        await pipeline.process_item(BadItem(), dummy_spider)  # type: ignore[arg-type]


# Spider Lifecycle Tests


@pytest.mark.asyncio
async def test_close_spider_clears_seen_cache(dummy_spider):
    """Pipeline clears seen set when spider closes."""
    pipeline = DuplicateFilterPipeline()
    item = Item(data={"url": "https://example.com/1"})

    await pipeline.process_item(item, dummy_spider)
    assert len(pipeline.seen) == 1

    await pipeline.close_spider(dummy_spider)
    assert len(pipeline.seen) == 0


@pytest.mark.asyncio
async def test_can_process_same_url_after_spider_close(dummy_spider):
    """Pipeline allows same URL after spider close (cache cleared)."""
    pipeline = DuplicateFilterPipeline()
    item1 = Item(data={"url": "https://example.com/1"})
    item2 = Item(data={"url": "https://example.com/1"})

    # First crawl
    await pipeline.process_item(item1, dummy_spider)
    await pipeline.close_spider(dummy_spider)

    # Second crawl - same URL should be allowed
    result = await pipeline.process_item(item2, dummy_spider)
    assert result is item2
