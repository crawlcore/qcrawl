"""Unit tests for RedisQueue connection wiring (no live Redis)."""

from unittest.mock import patch

import pytest

from qcrawl.core.queues.factory import create_queue
from qcrawl.core.queues.redis import RedisQueue
from qcrawl.settings import Settings

# Connection URL Tests


def test_builds_url_from_parts_without_credentials():
    """With no url, RedisQueue builds redis://host:port/db and forwards no creds."""
    with patch("qcrawl.core.queues.redis.Redis") as redis_cls:
        RedisQueue(host="db.internal", port=6390, db=2)

    redis_cls.from_url.assert_called_once()
    args, kwargs = redis_cls.from_url.call_args
    assert args[0] == "redis://db.internal:6390/2"
    assert kwargs["username"] is None
    assert kwargs["password"] is None
    assert kwargs["decode_responses"] is False


def test_builds_rediss_url_and_forwards_credentials():
    """ssl selects the rediss:// scheme; user/password forward as kwargs."""
    with patch("qcrawl.core.queues.redis.Redis") as redis_cls:
        RedisQueue(host="h", port="6379", user="alice", password="secret", ssl=True)

    args, kwargs = redis_cls.from_url.call_args
    assert args[0] == "rediss://h:6379/0"
    assert kwargs["username"] == "alice"
    assert kwargs["password"] == "secret"


def test_explicit_url_takes_precedence_over_parts():
    """An explicit url is used as-is and credentials are not injected."""
    with patch("qcrawl.core.queues.redis.Redis") as redis_cls:
        RedisQueue(url="redis://example:7000/3", host="ignored", port=1)

    args, kwargs = redis_cls.from_url.call_args
    assert args[0] == "redis://example:7000/3"
    assert "username" not in kwargs
    assert "password" not in kwargs


def test_decode_responses_invariant_not_overridable():
    """A user-supplied decode_responses is ignored; the bytes-only invariant holds."""
    with patch("qcrawl.core.queues.redis.Redis") as redis_cls:
        RedisQueue(decode_responses=True)

    _, kwargs = redis_cls.from_url.call_args
    assert kwargs["decode_responses"] is False


# Default Config Tests


@pytest.mark.asyncio
async def test_shipped_redis_backend_config_constructs():
    """The shipped redis backend config builds through the real queue factory."""
    cfg = Settings().QUEUE_BACKENDS["redis"]
    init_kwargs = {k: v for k, v in cfg.items() if k != "class"}

    with patch("qcrawl.core.queues.redis.Redis") as redis_cls:
        queue = await create_queue(str(cfg["class"]), **init_kwargs)

    redis_cls.from_url.assert_called_once()
    args, _ = redis_cls.from_url.call_args
    assert args[0] == "redis://localhost:6379/0"
    assert queue._maxsize == 0
