"""Tests for qcrawl.pipelines.manager.PipelineManager"""

import pytest

from qcrawl.core.item import Item
from qcrawl.core.spider import Spider
from qcrawl.pipelines.base import DropItem, ItemPipeline
from qcrawl.pipelines.duplicate import DuplicateFilterPipeline
from qcrawl.pipelines.manager import PipelineManager
from qcrawl.pipelines.validation import ValidationPipeline


class TransformPipeline(ItemPipeline):
    """Pipeline that transforms items."""

    async def process_item(self, item, spider):
        if "title" in item.data:
            title = item.data["title"]
            if isinstance(title, str):
                item.data["title"] = title.upper()
        return item


class CountingPipeline(ItemPipeline):
    """Pipeline that counts items processed."""

    def __init__(self):
        self.count = 0

    async def process_item(self, item, spider):
        self.count += 1
        return item


class DropAllPipeline(ItemPipeline):
    """Pipeline that drops all items."""

    async def process_item(self, item, spider):
        raise DropItem("Dropped by test pipeline")


class ErrorPipeline(ItemPipeline):
    """Pipeline that raises errors."""

    async def process_item(self, item, spider):
        raise ValueError("Test error")


# Initialization Tests


def test_manager_initializes_empty():
    """PipelineManager initializes with empty pipeline list."""
    manager = PipelineManager()

    assert manager.pipelines == []


def test_manager_initializes_with_pipelines():
    """PipelineManager initializes with provided pipelines."""
    pipeline1 = TransformPipeline()
    pipeline2 = CountingPipeline()

    manager = PipelineManager(pipelines=[pipeline1, pipeline2])

    assert len(manager.pipelines) == 2
    assert manager.pipelines[0] is pipeline1
    assert manager.pipelines[1] is pipeline2


# Add Pipeline Tests


def test_add_pipeline_appends_to_list():
    """add_pipeline appends pipeline to the list."""
    manager = PipelineManager()
    pipeline = TransformPipeline()

    manager.add_pipeline(pipeline)

    assert pipeline in manager.pipelines
    assert len(manager.pipelines) == 1


def test_add_pipeline_rejects_non_pipeline():
    """add_pipeline rejects non-ItemPipeline instances."""
    manager = PipelineManager()

    with pytest.raises(TypeError, match="Pipeline must be ItemPipeline instance"):
        manager.add_pipeline("not a pipeline")  # type: ignore[arg-type]


def test_add_pipeline_rejects_sync_process_item():
    """add_pipeline rejects pipelines with sync process_item."""

    class SyncPipeline(ItemPipeline):
        def process_item(self, item, spider):
            return item

    manager = PipelineManager()
    pipeline = SyncPipeline()

    with pytest.raises(TypeError, match="process_item must be `async def` coroutine function"):
        manager.add_pipeline(pipeline)


# Process Item Chain Tests


@pytest.mark.asyncio
async def test_process_item_through_empty_chain(dummy_spider):
    """Empty pipeline chain returns item unchanged."""
    manager = PipelineManager()
    item = Item(data={"title": "test"})

    result = await manager.process_item(item, dummy_spider)

    assert result is item


@pytest.mark.asyncio
async def test_process_item_through_single_pipeline(dummy_spider):
    """Item is processed through single pipeline."""
    manager = PipelineManager()
    pipeline = TransformPipeline()
    manager.add_pipeline(pipeline)

    item = Item(data={"title": "test"})
    result = await manager.process_item(item, dummy_spider)

    assert result is not None
    assert result.data["title"] == "TEST"


@pytest.mark.asyncio
async def test_process_item_chains_transformations(dummy_spider):
    """Multiple transformations are chained correctly."""

    class AppendPipeline(ItemPipeline):
        def __init__(self, suffix):
            self.suffix = suffix

        async def process_item(self, item, spider):
            item.data["title"] = item.data.get("title", "") + self.suffix
            return item

    manager = PipelineManager()
    manager.add_pipeline(AppendPipeline("_1"))
    manager.add_pipeline(AppendPipeline("_2"))
    manager.add_pipeline(AppendPipeline("_3"))

    item = Item(data={"title": "test"})
    result = await manager.process_item(item, dummy_spider)

    assert result is not None
    assert result.data["title"] == "test_1_2_3"


# DropItem Handling Tests


@pytest.mark.asyncio
async def test_process_item_stops_on_drop(dummy_spider):
    """Pipeline chain stops and returns None when item is dropped."""
    manager = PipelineManager()

    counter1 = CountingPipeline()
    drop = DropAllPipeline()
    counter2 = CountingPipeline()

    manager.add_pipeline(counter1)
    manager.add_pipeline(drop)
    manager.add_pipeline(counter2)

    item = Item(data={"title": "test"})
    result = await manager.process_item(item, dummy_spider)

    assert result is None
    assert counter1.count == 1
    assert counter2.count == 0  # Never reached


# Error Handling Tests


@pytest.mark.asyncio
async def test_process_item_stops_chain_on_error(dummy_spider):
    """Pipeline chain stops on error and doesn't process remaining pipelines."""
    manager = PipelineManager()

    counter1 = CountingPipeline()
    error = ErrorPipeline()
    counter2 = CountingPipeline()

    manager.add_pipeline(counter1)
    manager.add_pipeline(error)
    manager.add_pipeline(counter2)

    item = Item(data={"title": "test"})
    result = await manager.process_item(item, dummy_spider)

    assert result is None
    assert counter1.count == 1
    assert counter2.count == 0


@pytest.mark.asyncio
async def test_process_item_handles_none_return(dummy_spider):
    """Pipeline treats explicit None return as dropped item."""

    class ReturnNonePipeline(ItemPipeline):
        async def process_item(self, item, spider):
            return None

    manager = PipelineManager()
    counter = CountingPipeline()
    manager.add_pipeline(ReturnNonePipeline())
    manager.add_pipeline(counter)

    item = Item(data={"title": "test"})
    result = await manager.process_item(item, dummy_spider)

    assert result is None
    assert counter.count == 0


# Lifecycle Hook Tests


@pytest.mark.asyncio
async def test_open_spider_calls_all_pipelines(dummy_spider):
    """open_spider calls open_spider on all pipelines."""

    class TrackingPipeline(ItemPipeline):
        def __init__(self):
            self.opened = False

        async def open_spider(self, spider):
            self.opened = True

    manager = PipelineManager()
    p1 = TrackingPipeline()
    p2 = TrackingPipeline()

    manager.add_pipeline(p1)
    manager.add_pipeline(p2)

    await manager.open_spider(dummy_spider)

    assert p1.opened
    assert p2.opened


@pytest.mark.asyncio
async def test_close_spider_calls_all_pipelines(dummy_spider):
    """close_spider calls close_spider on all pipelines."""

    class TrackingPipeline(ItemPipeline):
        def __init__(self):
            self.closed = False

        async def close_spider(self, spider):
            self.closed = True

    manager = PipelineManager()
    p1 = TrackingPipeline()
    p2 = TrackingPipeline()

    manager.add_pipeline(p1)
    manager.add_pipeline(p2)

    await manager.close_spider(dummy_spider)

    assert p1.closed
    assert p2.closed


@pytest.mark.asyncio
async def test_open_spider_continues_on_error(dummy_spider):
    """open_spider continues even if one pipeline raises error."""

    class ErrorOpenPipeline(ItemPipeline):
        async def open_spider(self, spider):
            raise ValueError("Test error")

    class TrackingPipeline(ItemPipeline):
        def __init__(self):
            self.opened = False

        async def open_spider(self, spider):
            self.opened = True

    manager = PipelineManager()
    error_pipe = ErrorOpenPipeline()
    tracking_pipe = TrackingPipeline()

    manager.add_pipeline(error_pipe)
    manager.add_pipeline(tracking_pipe)

    await manager.open_spider(dummy_spider)

    assert tracking_pipe.opened


@pytest.mark.asyncio
async def test_close_spider_continues_on_error(dummy_spider):
    """close_spider continues even if one pipeline raises error."""

    class ErrorClosePipeline(ItemPipeline):
        async def close_spider(self, spider):
            raise ValueError("Test error")

    class TrackingPipeline(ItemPipeline):
        def __init__(self):
            self.closed = False

        async def close_spider(self, spider):
            self.closed = True

    manager = PipelineManager()
    error_pipe = ErrorClosePipeline()
    tracking_pipe = TrackingPipeline()

    manager.add_pipeline(error_pipe)
    manager.add_pipeline(tracking_pipe)

    await manager.close_spider(dummy_spider)

    assert tracking_pipe.closed


# Integration Tests


@pytest.mark.asyncio
async def test_integration_validation_then_duplicate(dummy_spider):
    """Integration: ValidationPipeline before DuplicateFilterPipeline."""

    class SpiderWithRequired(Spider):
        name = "required"
        start_urls = ["http://example.com"]
        REQUIRED_FIELDS = ["url", "title"]

        async def parse(self, response):
            yield {}

    spider_req = SpiderWithRequired()
    manager = PipelineManager()
    manager.add_pipeline(ValidationPipeline())
    manager.add_pipeline(DuplicateFilterPipeline())

    # Valid item passes
    item1 = Item(data={"url": "https://example.com", "title": "Page"})
    result1 = await manager.process_item(item1, spider_req)
    assert result1 is item1

    # Duplicate dropped
    item2 = Item(data={"url": "https://example.com", "title": "Updated"})
    result2 = await manager.process_item(item2, spider_req)
    assert result2 is None

    # Invalid item dropped by validation
    item3 = Item(data={"url": "https://example.com/2"})  # Missing title
    result3 = await manager.process_item(item3, spider_req)
    assert result3 is None


# from_settings Tests


def test_from_settings_with_none():
    """from_settings returns empty manager when settings is None."""
    manager = PipelineManager.from_settings(None)

    assert len(manager.pipelines) == 0


def test_from_settings_with_no_pipelines_key():
    """from_settings returns empty manager when pipelines key missing."""
    manager = PipelineManager.from_settings({"other_setting": "value"})

    assert len(manager.pipelines) == 0


def test_from_settings_with_valid_pipeline():
    """from_settings loads pipeline from dotted path."""
    settings = {"pipelines": {"qcrawl.pipelines.duplicate.DuplicateFilterPipeline": 100}}

    manager = PipelineManager.from_settings(settings)

    assert len(manager.pipelines) == 1
    assert isinstance(manager.pipelines[0], DuplicateFilterPipeline)


def test_from_settings_with_multiple_pipelines_ordered():
    """from_settings loads multiple pipelines in priority order."""
    settings = {
        "pipelines": {
            "qcrawl.pipelines.duplicate.DuplicateFilterPipeline": 200,
            "qcrawl.pipelines.validation.ValidationPipeline": 100,
        }
    }

    manager = PipelineManager.from_settings(settings)

    assert len(manager.pipelines) == 2
    # Lower priority number comes first
    assert isinstance(manager.pipelines[0], ValidationPipeline)
    assert isinstance(manager.pipelines[1], DuplicateFilterPipeline)


def test_from_settings_ignores_invalid_paths():
    """from_settings ignores invalid dotted paths."""
    settings = {
        "pipelines": {
            "invalid.module.Pipeline": 100,
            "qcrawl.pipelines.validation.ValidationPipeline": 200,
        }
    }

    manager = PipelineManager.from_settings(settings)

    # Only valid pipeline loaded
    assert len(manager.pipelines) == 1
    assert isinstance(manager.pipelines[0], ValidationPipeline)


# Representation Tests


def test_repr_shows_pipeline_count():
    """__repr__ shows number of pipelines."""
    manager = PipelineManager()
    assert repr(manager) == "PipelineManager(pipelines=0)"

    manager.add_pipeline(TransformPipeline())
    manager.add_pipeline(CountingPipeline())
    assert repr(manager) == "PipelineManager(pipelines=2)"
