import asyncio
import importlib
import logging
import uuid
from asyncio import QueueEmpty

from qcrawl.core._msgspec import decode_request, encode_request
from qcrawl.core.queue import RequestQueue
from qcrawl.core.request import Request
from qcrawl.utils.fingerprint import RequestFingerprinter

redis_exceptions: object | None = None
Redis: type[object] = object

try:
    _redis_mod = importlib.import_module("redis")
    redis_exceptions = getattr(_redis_mod, "exceptions", None)
    _redis_async = importlib.import_module("redis.asyncio")
    Redis = getattr(_redis_async, "Redis", object)
except Exception:
    # Redis extras not installed — keep fallbacks (behaviour unchanged)
    pass


logger = logging.getLogger(__name__)


# Lua: add only if not present (uses separate fingerprint set)
# When used:
#   - dedupe=True, maxsize=0, update_priority=False
# Behavior:
#   - Insert-only (no priority updates). Returns 1 on insert, 0 if duplicate.
# Supports optional TTL (ARGV[4]) - when provided, the script will:
#  - ZADD with 'EX' ttl
#  - HEXPIRE the specific hash field
#  - EXPIRE the fingerprint set key
_DEDUP_LUA = """
local set_key = KEYS[1]
local hash_key = KEYS[2]
local zset_key = KEYS[3]
local id = ARGV[1]
local payload = ARGV[2]
local score = ARGV[3]
local ttl = tonumber(ARGV[4])
if redis.call('SISMEMBER', set_key, id) == 1 then
  return 0
end
redis.call('SADD', set_key, id)
redis.call('HSET', hash_key, id, payload)
if ttl then
  -- set zset entry with TTL, set per-field TTL on hash and TTL on the fp set
  redis.call('ZADD', zset_key, 'EX', ttl, score, id)
  redis.call('HEXPIRE', hash_key, id, ttl)
else
  redis.call('ZADD', zset_key, score, id)
end
return 1
"""

# Lua: add if missing, else update score only if new score is better
# When used:
#   - dedupe=True, maxsize=0, update_priority=True
# Behavior:
#   - Insert if missing; if present, only lower the score (higher priority) when applicable.
# Supports optional TTL (ARGV[4]) and updates TTL when inserting/updating score.
_UPDATE_PRIORITY_LUA = """
local set_key = KEYS[1]
local hash_key = KEYS[2]
local zset_key = KEYS[3]
local id = ARGV[1]
local payload = ARGV[2]
local score = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
if redis.call('SISMEMBER', set_key, id) == 0 then
  if ttl then
    redis.call('SADD', set_key, id)
    redis.call('HSET', hash_key, id, payload)
    redis.call('ZADD', zset_key, 'EX', ttl, score, id)
    redis.call('HEXPIRE', hash_key, id, ttl)
    redis.call('EXPIRE', set_key, ttl)
  else
    redis.call('SADD', set_key, id)
    redis.call('HSET', hash_key, id, payload)
    redis.call('ZADD', zset_key, score, id)
  end
  return 1
end
local cur = redis.call('ZSCORE', zset_key, id)
if cur then
  cur = tonumber(cur)
  if score < cur then
    if ttl then
      -- update existing score and refresh TTL atomically
      redis.call('ZADD', zset_key, 'XX', 'EX', ttl, score, id)
      redis.call('HEXPIRE', hash_key, id, ttl)
    else
      redis.call('ZADD', zset_key, 'XX', score, id)
    end
  end
end
return 0
"""

# Limit-aware variants: check zset cardinality before inserting to enforce maxsize.
# When used:
#   - dedupe=True, maxsize>0, update_priority=False
# Behavior:
#   - Same as _DEDUP_LUA but returns -1 when zset cardinality >= max (ARGV[5]).
_DEDUP_LUA_LIMIT = """
local set_key = KEYS[1]
local hash_key = KEYS[2]
local zset_key = KEYS[3]
local id = ARGV[1]
local payload = ARGV[2]
local score = ARGV[3]
local ttl = tonumber(ARGV[4])
local max = tonumber(ARGV[5]) or 0
if max > 0 and redis.call('ZCARD', zset_key) >= max then
  return -1
end
if redis.call('SISMEMBER', set_key, id) == 1 then
  return 0
end
redis.call('SADD', set_key, id)
redis.call('HSET', hash_key, id, payload)
if ttl then
  redis.call('ZADD', zset_key, 'EX', ttl, score, id)
  redis.call('HEXPIRE', hash_key, id, ttl)
else
  redis.call('ZADD', zset_key, score, id)
end
return 1
"""

# Limit-aware update variant: check zset cardinality before inserting to enforce maxsize.
# When used:
#   - dedupe=True, maxsize>0, update_priority=True
# Behavior:
#   - Same as _UPDATE_PRIORITY_LUA but returns -1 when zset cardinality >= max and the id is not present.
_UPDATE_PRIORITY_LUA_LIMIT = """
local set_key = KEYS[1]
local hash_key = KEYS[2]
local zset_key = KEYS[3]
local id = ARGV[1]
local payload = ARGV[2]
local score = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local max = tonumber(ARGV[5]) or 0
if max > 0 and redis.call('ZCARD', zset_key) >= max and redis.call('SISMEMBER', set_key, id) == 0 then
  return -1
end
if redis.call('SISMEMBER', set_key, id) == 0 then
  if ttl then
    redis.call('SADD', set_key, id)
    redis.call('HSET', hash_key, id, payload)
    redis.call('ZADD', zset_key, 'EX', ttl, score, id)
    redis.call('HEXPIRE', hash_key, id, ttl)
    redis.call('EXPIRE', set_key, ttl)
  else
    redis.call('SADD', set_key, id)
    redis.call('HSET', hash_key, id, payload)
    redis.call('ZADD', zset_key, score, id)
  end
  return 1
end
local cur = redis.call('ZSCORE', zset_key, id)
if cur then
  cur = tonumber(cur)
  if score < cur then
    if ttl then
      redis.call('ZADD', zset_key, 'XX', 'EX', ttl, score, id)
      redis.call('HEXPIRE', hash_key, id, ttl)
    else
      redis.call('ZADD', zset_key, 'XX', score, id)
    end
  end
end
return 0
"""

# Non-dedupe limit-aware variant.
# When used:
#   - dedupe=False, maxsize>0
# Behavior:
#   - Insert a new UUID id into zset + hash with optional TTL. Returns 1 on insert, 0 when full.
_NON_DEDUPE_LUA_LIMIT = """
local hash_key = KEYS[2]
local zset_key = KEYS[3]
local id = ARGV[1]
local payload = ARGV[2]
local score = ARGV[3]
local ttl = tonumber(ARGV[4])
local max = tonumber(ARGV[5]) or 0
if max > 0 and redis.call('ZCARD', zset_key) >= max then
  return 0
end
if ttl then
  redis.call('ZADD', zset_key, 'EX', ttl, score, id)
else
  redis.call('ZADD', zset_key, score, id)
end
redis.call('HSET', hash_key, id, payload)
if ttl then
  redis.call('HEXPIRE', hash_key, id, ttl)
end
return 1
"""


class RedisQueue(RequestQueue):
    """Redis-backed priority queue with per-item TTL support.

    Requires Redis server 7.4+ and a client that supports per-member TTL via
    `ZADD ... EX` and per-hash-field TTL via `HEXPIRE`. This implementation
    relies on those features and does not fall back to key-level expiry.

    This implementation enforces bytes-only semantics for script SHAs and
    item ids: the Redis client must be configured with `decode_responses=False`.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        namespace: str = "qcrawl",
        *,
        ssl: bool = False,
        dedupe: bool = False,
        update_priority: bool = False,
        fingerprint_size: int = 16,
        item_ttl: int | None = None,
        dedupe_ttl: int | None = None,
        max_orphan_retries: int = 10,
        maxsize: int | None = None,
        **redis_kwargs: object,
    ) -> None:
        self.client = Redis.from_url(
            url,
            decode_responses=False,
            ssl=ssl,
            retry_on_timeout=True,
            health_check_interval=30,
            **redis_kwargs,
        )
        self.zset_key = f"{namespace}:queue:zset"
        self.hash_key = f"{namespace}:queue:items"
        self.fp_set_key = f"{namespace}:queue:fpset"

        self.dedupe = dedupe
        self.update_priority = update_priority
        self.fingerprint_size = fingerprint_size
        self.fingerprinter = RequestFingerprinter()

        # per-item TTL applied to zset entry and hash field when provided
        self.item_ttl = item_ttl
        # TTL applied to dedupe/fingerprint state when provided
        self.dedupe_ttl = dedupe_ttl
        self.max_orphan_retries = max_orphan_retries

        # capacity
        if maxsize is None:
            self._maxsize = 0
        else:
            if maxsize < 0:
                raise ValueError("maxsize must be >= 0")
            self._maxsize = int(maxsize)

        # script SHAs (lazy-loaded) — expect bytes
        self._dedup_sha: bytes | None = None
        self._update_sha: bytes | None = None
        self._dedup_limit_sha: bytes | None = None
        self._update_limit_sha: bytes | None = None
        self._non_dedupe_limit_sha: bytes | None = None

    async def _ensure_scripts_loaded(self) -> None:
        """Ensure Lua scripts are loaded into Redis script cache.

        Loads normal and limit-aware variants into the Redis script cache if
        their SHAs have not yet been cached locally. Expects the Redis client
        to return binary SHAs (bytes). If a non-bytes value is returned the
        method raises a RuntimeError to fail fast and require correct client
        configuration (`decode_responses=False`).
        """
        if self._dedup_sha is None:
            sha = await self.client.script_load(_DEDUP_LUA)
            if not isinstance(sha, (bytes, bytearray)):
                raise RuntimeError(
                    "Redis client returned non-bytes SHA from script_load; expected bytes. "
                    "Configure the client with decode_responses=False."
                )
            self._dedup_sha = bytes(sha)

        if self._update_sha is None:
            sha = await self.client.script_load(_UPDATE_PRIORITY_LUA)
            if not isinstance(sha, (bytes, bytearray)):
                raise RuntimeError(
                    "Redis client returned non-bytes SHA from script_load; expected bytes. "
                    "Configure the client with decode_responses=False."
                )
            self._update_sha = bytes(sha)

        if self._dedup_limit_sha is None:
            sha = await self.client.script_load(_DEDUP_LUA_LIMIT)
            if not isinstance(sha, (bytes, bytearray)):
                raise RuntimeError(
                    "Redis client returned non-bytes SHA from script_load; expected bytes. "
                    "Configure the client with decode_responses=False."
                )
            self._dedup_limit_sha = bytes(sha)

        if self._update_limit_sha is None:
            sha = await self.client.script_load(_UPDATE_PRIORITY_LUA_LIMIT)
            if not isinstance(sha, (bytes, bytearray)):
                raise RuntimeError(
                    "Redis client returned non-bytes SHA from script_load; expected bytes. "
                    "Configure the client with decode_responses=False."
                )
            self._update_limit_sha = bytes(sha)

        if self._non_dedupe_limit_sha is None:
            sha = await self.client.script_load(_NON_DEDUPE_LUA_LIMIT)
            if not isinstance(sha, (bytes, bytearray)):
                raise RuntimeError(
                    "Redis client returned non-bytes SHA from script_load; expected bytes. "
                    "Configure the client with decode_responses=False."
                )
            self._non_dedupe_limit_sha = bytes(sha)

    async def _evalsha_with_reload(self, sha: bytes | None, num_keys: int, *args: object) -> int:
        """Call `EVALSHA` and reload scripts on `NOSCRIPT` errors, then retry once.

        Accepts SHA as `bytes` (or `None`), retries script load on NOSCRIPT.
        """
        if sha is None:
            await self._ensure_scripts_loaded()
            # if caller passed None, try to continue but require a sha
            raise RuntimeError("Missing script SHA for EVALSHA call")

        try:
            res = await self.client.evalsha(sha, num_keys, *args)
            return int(res)
        except Exception as exc:
            # Prefer driver-specific NoScriptError if present (combined check)
            if (
                redis_exceptions is not None
                and hasattr(redis_exceptions, "NoScriptError")
                and isinstance(exc, redis_exceptions.NoScriptError)
            ):
                logger.debug("NOSCRIPT detected (driver exception); reloading scripts and retrying")
                await self._ensure_scripts_loaded()
                res = await self.client.evalsha(sha, num_keys, *args)
                return int(res)

            # Fallback: inspect exception message
            if "NOSCRIPT" in str(exc).upper():
                logger.debug("NOSCRIPT detected (message); reloading scripts and retrying")
                await self._ensure_scripts_loaded()
                res = await self.client.evalsha(sha, num_keys, *args)
                return int(res)
            raise

    async def put(self, request: Request, priority: int = 0) -> bool | None:  # type: ignore[override]
        """Enqueue a request as binary with an associated integer priority.

        In non-dedupe mode this creates a UUID for the item and stores the payload
        in a pipeline (zadd + hset). Returns `None` in this mode.

        In dedupe mode a deterministic fingerprint is computed and a Lua script is
        executed atomically to insert only if missing (or update priority if
        `update_priority` is enabled). The method returns `True` if the item was
        newly added (script returned a truthy value) or `False` if it was a
        duplicate. When `dedupe_ttl` is configured, TTLs are applied to dedupe
        related keys per-item (via `ZADD ... EX` and `HEXPIRE`) as described above.

        When `maxsize` is configured (>0), limit-aware scripts are used and a
        server-side capacity check is performed atomically. On full result the
        method raises `asyncio.QueueFull`.
        """
        payload = encode_request(request)
        score = -priority

        max_arg = str(self._maxsize) if self._maxsize else "0"

        if self.dedupe:
            # deterministic fingerprint \- use raw bytes (no hex)
            item_id = self.fingerprinter.fingerprint_bytes(
                request, digest_size=self.fingerprint_size
            )
            # choose sha (limit-aware if maxsize configured)
            if self._maxsize:
                sha = self._update_limit_sha if self.update_priority else self._dedup_limit_sha
                # ARGV positions: id, payload, score, ttl, max
                args_list: list[object] = [
                    self.fp_set_key,
                    self.hash_key,
                    self.zset_key,
                    item_id,
                    payload,
                    str(score),
                    "" if not self.item_ttl else str(self.item_ttl),
                    max_arg,
                ]
                # ensure num_keys is 3 (KEYS: fp_set, hash, zset)
                num_keys = 3
            else:
                sha = self._update_sha if self.update_priority else self._dedup_sha
                args_list = [
                    self.fp_set_key,
                    self.hash_key,
                    self.zset_key,
                    item_id,
                    payload,
                    str(score),
                ]
                if self.item_ttl:
                    args_list.append(str(self.item_ttl))
                num_keys = 3

            # Call script (may raise and will reload on NOSCRIPT)
            res = await self._evalsha_with_reload(sha, num_keys, *args_list)

            # For limit variants script returns -1 on full, 0 duplicate, 1 added
            if res < 0:
                raise asyncio.QueueFull
            if self.dedupe_ttl and res:
                await self.client.expire(self.fp_set_key, self.dedupe_ttl)
            return bool(res)

        else:
            # non-dedupe path: use binary UUID (16 bytes)
            item_id = uuid.uuid4().bytes
            if self._maxsize:
                sha = self._non_dedupe_limit_sha
                # ARGV: id, payload, score, ttl, max
                args_list = [
                    item_id,
                    payload,
                    str(score),
                    str(self.item_ttl) if self.item_ttl else "",
                    max_arg,
                ]
                res = await self._evalsha_with_reload(sha, 3, *args_list)
                if int(res) == 0:
                    raise asyncio.QueueFull
                return None
            else:
                # fallback: pipeline (existing behavior)
                item_ttl = self.item_ttl

                async with self.client.pipeline(transaction=True) as pipe:
                    if item_ttl:
                        pipe.zadd(self.zset_key, {item_id: score}, ex=item_ttl)
                    else:
                        pipe.zadd(self.zset_key, {item_id: score})

                    pipe.hset(self.hash_key, item_id, payload)

                    if item_ttl:
                        try:
                            pipe.hexpire(self.hash_key, item_id, item_ttl)
                        except AttributeError:
                            logger.debug("HEXPIRE not supported; relying on ZADD EX only")

                    await pipe.execute()
                return None

    async def get(self, timeout: float = 0.0) -> Request:
        """Pop the highest-priority request, blocking up to *timeout* seconds.

        This method uses `BZPOPMIN` to retrieve the smallest-score element (highest
        priority due to negative scoring) and then fetches the payload from the
        hash. If the hash payload is missing (an orphaned zset member), the zset
        entry is removed and the operation retries up to `max_orphan_retries`.

        Args:
            timeout: Blocking timeout in seconds for the blocking pop. A value
                of `0.0` behaves like an immediate (non-blocking) check for some
                Redis client configurations; consult your client's semantics.

        Returns:
            A deserialized `Request` instance.

        Raises:
            asyncio.QueueEmpty: If the pop timed out with no result.
            RuntimeError: If deserialization fails or repeated orphaned entries
                exceed `max_orphan_retries`.
        """
        retries: int = 0
        while True:
            result = await self.client.bzpopmin(self.zset_key, timeout=timeout)
            if not result:
                raise QueueEmpty

            _, item_id, _ = result

            data = await self.client.hget(self.hash_key, item_id)
            if data is None:
                logger.warning("Orphaned item %s: in zset but missing in hash. Removing.", item_id)
                await self.client.zrem(self.zset_key, item_id)
                retries += 1
                if retries >= self.max_orphan_retries:
                    raise RuntimeError("Exceeded max orphan retries while fetching queue item")
                continue

            try:
                # decode_request returns a qcrawl.core.request.Request instance
                req = decode_request(data)
                await self.client.hdel(self.hash_key, item_id)
                return req
            except Exception as exc:
                logger.error("Failed to deserialize item %s: %s", item_id, exc, exc_info=True)
                raise RuntimeError("Failed to deserialize request") from exc

    async def size(self) -> int:
        """Return the approximate number of queued items.

        This returns the cardinality of the zset backing the queue.

        Returns:
            Approximate queue size as an integer.
        """
        return int(await self.client.zcard(self.zset_key))

    async def clear(self) -> None:
        """Remove all items and dedupe state from Redis."""
        async with self.client.pipeline() as pipe:
            pipe.delete(self.zset_key)
            pipe.delete(self.hash_key)
            pipe.delete(self.fp_set_key)
            await pipe.execute()

    async def close(self) -> None:
        """Close the underlying Redis client connection."""
        await self.client.close()

    def maxsize(self) -> int:
        """Return configured maximum capacity (0 = unlimited)."""
        return int(self._maxsize or 0)
