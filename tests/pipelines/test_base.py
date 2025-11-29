"""Tests for qcrawl.pipelines.base"""

import pytest

from qcrawl.core.item import Item
from qcrawl.pipelines.base import DropItem, ItemPipeline

# DropItem Exception Tests


def test_dropitem_with_reason():
    """DropItem stores reason and displays it correctly."""
    exc = DropItem("Item is invalid")

    assert exc.reason == "Item is invalid"
    assert str(exc) == "DropItem: Item is invalid"
    assert repr(exc) == "DropItem(reason='Item is invalid')"


def test_dropitem_without_reason():
    """DropItem works without a reason."""
    exc = DropItem()

    assert exc.reason is None
    assert str(exc) == "DropItem: no reason"
    assert repr(exc) == "DropItem(reason=None)"


def test_dropitem_with_none_reason():
    """DropItem handles explicit None reason."""
    exc = DropItem(None)

    assert exc.reason is None
    assert str(exc) == "DropItem: no reason"


def test_dropitem_is_exception():
    """DropItem is an Exception subclass."""
    exc = DropItem("test")

    assert isinstance(exc, Exception)


def test_dropitem_can_be_raised_and_caught():
    """DropItem can be raised and caught as exception."""
    with pytest.raises(DropItem) as exc_info:
        raise DropItem("Test error")

    assert exc_info.value.reason == "Test error"


# ItemPipeline Base Class Tests


@pytest.mark.asyncio
async def test_base_pipeline_allows_valid_item(dummy_spider):
    """Base ItemPipeline allows items with .data attribute."""
    pipeline = ItemPipeline()
    item = Item(data={"url": "https://example.com"})

    result = await pipeline.process_item(item, dummy_spider)

    assert result is item


@pytest.mark.asyncio
async def test_base_pipeline_rejects_item_without_data(dummy_spider):
    """Base ItemPipeline rejects items without .data attribute."""
    pipeline = ItemPipeline()
    invalid_item = {"url": "https://example.com"}  # Plain dict

    with pytest.raises(DropItem, match="missing .data attribute"):
        await pipeline.process_item(invalid_item, dummy_spider)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_base_pipeline_allows_item_with_empty_data(dummy_spider):
    """Base ItemPipeline allows items with empty data dict."""
    pipeline = ItemPipeline()
    item = Item(data={})

    result = await pipeline.process_item(item, dummy_spider)

    assert result is item


# Lifecycle Hooks Tests


@pytest.mark.asyncio
async def test_open_spider_returns_none(dummy_spider):
    """Base open_spider hook returns None by default."""
    pipeline = ItemPipeline()

    # Base implementation returns None
    await pipeline.open_spider(dummy_spider)


@pytest.mark.asyncio
async def test_close_spider_returns_none(dummy_spider):
    """Base close_spider hook returns None by default."""
    pipeline = ItemPipeline()

    # Base implementation returns None
    await pipeline.close_spider(dummy_spider)


@pytest.mark.asyncio
async def test_lifecycle_hooks_can_be_called_multiple_times(dummy_spider):
    """Lifecycle hooks can be called multiple times without error."""
    pipeline = ItemPipeline()

    await pipeline.open_spider(dummy_spider)
    await pipeline.open_spider(dummy_spider)
    await pipeline.close_spider(dummy_spider)
    await pipeline.close_spider(dummy_spider)

    # Should not raise any errors


# Custom Pipeline Subclass Tests


@pytest.mark.asyncio
async def test_custom_pipeline_can_override_process_item(dummy_spider):
    """Custom pipelines can override process_item."""

    class UppercaseTitlePipeline(ItemPipeline):
        async def process_item(self, item_arg, spider):
            if "title" in item_arg.data:
                title = item_arg.data["title"]
                if isinstance(title, str):
                    item_arg.data["title"] = title.upper()
            return item_arg

    pipeline = UppercaseTitlePipeline()
    item = Item(data={"title": "hello world"})

    result = await pipeline.process_item(item, dummy_spider)

    assert result.data["title"] == "HELLO WORLD"


@pytest.mark.asyncio
async def test_custom_pipeline_can_override_lifecycle_hooks(dummy_spider):
    """Custom pipelines can override lifecycle hooks."""

    class StatefulPipeline(ItemPipeline):
        def __init__(self):
            self.opened = False
            self.closed = False

        async def open_spider(self, spider):
            self.opened = True

        async def close_spider(self, spider):
            self.closed = True

    pipeline = StatefulPipeline()

    assert not pipeline.opened
    assert not pipeline.closed

    await pipeline.open_spider(dummy_spider)
    assert pipeline.opened
    assert not pipeline.closed

    await pipeline.close_spider(dummy_spider)
    assert pipeline.opened
    assert pipeline.closed
