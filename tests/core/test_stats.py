"""Tests for qcrawl.core.stats.StatsCollector"""

import time
from unittest.mock import Mock

import pytest

from qcrawl.core.stats import StatsCollector

# Counter Tests


def test_inc():
    """StatsCollector.inc increments counters."""
    stats = StatsCollector()

    stats.inc("requests")
    stats.inc("requests")
    stats.inc("responses", count=5)

    assert stats.get("requests") == 2
    assert stats.get("responses") == 5


def test_inc_coerces_non_numeric():
    """StatsCollector.inc coerces non-numeric values to 0 before incrementing."""
    stats = StatsCollector()

    stats.label("key", "string_value")
    stats.inc("key")  # coerces to 0 then increments

    assert stats.get("key") == 1


# Gauge Tests


def test_set():
    """StatsCollector.set sets numeric gauges."""
    stats = StatsCollector()

    stats.set("total", 100)
    stats.set("average", 42.5)

    assert stats.get("total") == 100
    assert stats.get("average") == 42.5


def test_set_rejects_non_numeric():
    """StatsCollector.set raises TypeError for non-numeric values."""
    stats = StatsCollector()

    with pytest.raises(TypeError, match="set accepts only int or float"):
        stats.set("key", "string")  # type: ignore[arg-type]


def test_max_keeps_high_water_mark():
    """StatsCollector.max keeps the larger of the stored value and the new value."""
    stats = StatsCollector()

    stats.max("peak", 5)
    stats.max("peak", 12)
    stats.max("peak", 9)

    assert stats.get("peak") == 12


def test_min_keeps_low_water_mark():
    """StatsCollector.min keeps the smaller of the stored value and the new value."""
    stats = StatsCollector()

    stats.min("low", 5)
    stats.min("low", 2)
    stats.min("low", 9)

    assert stats.get("low") == 2


# Label Tests


def test_label():
    """StatsCollector.label sets string metadata."""
    stats = StatsCollector()

    stats.label("spider_name", "test_spider")
    stats.label("reason", "finished")

    assert stats.get("spider_name") == "test_spider"
    assert stats.get("reason") == "finished"


def test_label_rejects_non_string():
    """StatsCollector.label raises TypeError for non-string values."""
    stats = StatsCollector()

    with pytest.raises(TypeError, match="label accepts only str"):
        stats.label("key", 123)  # type: ignore[arg-type]


# Read Tests


def test_get_with_default():
    """StatsCollector.get returns the default for missing keys."""
    stats = StatsCollector()

    assert stats.get("missing") is None
    assert stats.get("missing", default=0) == 0


def test_snapshot():
    """StatsCollector.snapshot returns a copy of all stats."""
    stats = StatsCollector()

    stats.inc("requests", 10)
    stats.label("spider", "test")

    snapshot = stats.snapshot()
    assert snapshot["requests"] == 10
    assert snapshot["spider"] == "test"

    # Snapshot is a copy
    stats.inc("requests", 5)
    assert snapshot["requests"] == 10  # Unchanged


# Lifecycle Tests


def test_open_spider():
    """StatsCollector open_spider records start time and spider name."""
    stats = StatsCollector()
    spider = Mock()
    spider.name = "test_spider"

    stats.open_spider(spider)

    assert stats.get("start_time") is not None
    assert stats.get("spider_name") == "test_spider"
    assert stats._start_time is not None


def test_close_spider():
    """StatsCollector close_spider records finish time and elapsed time."""
    stats = StatsCollector()
    spider = Mock()
    spider.name = "test_spider"

    stats.open_spider(spider)
    time.sleep(0.01)
    stats.close_spider(spider, reason="finished")

    assert stats.get("finish_time") is not None
    assert stats.get("finish_reason") == "finished"
    elapsed = stats.get("elapsed_time_seconds")
    assert isinstance(elapsed, (int, float)) and elapsed > 0


def test_log_stats():
    """StatsCollector log_stats formats stats for logging."""
    stats = StatsCollector()

    stats.inc("requests", 1000)
    stats.set("average", 3.14159)
    stats.label("spider", "test")

    log_output = stats.log_stats()

    assert "requests: 1,000" in log_output
    assert "average: 3.14159" in log_output
    assert "spider: test" in log_output
