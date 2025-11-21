from __future__ import annotations

import importlib

from qcrawl.core.queue import RequestQueue
from qcrawl.core.queues.memory import MemoryPriorityQueue
from qcrawl.settings import Settings
from qcrawl.utils.settings import ensure_bool, ensure_int, ensure_str


async def create_queue(backend: str = "memory") -> RequestQueue:
    """Async factory for queue backends. Await the result to obtain a RequestQueue."""
    normalized = (backend or "memory").lower().strip()

    settings = Settings()
    backends = getattr(settings, "QUEUE_BACKENDS", {}) or {}
    cfg = backends.get(normalized, {}) or {}
    allowed_template_keys = set(cfg.keys())
    allowed = allowed_template_keys | {"class", "redis_kwargs"}

    if normalized == "memory":
        mem_maxsize_val = cfg.get("maxsize", 0)
        mem_maxsize_any = ensure_int(mem_maxsize_val, "memory.maxsize", allow_none=False)
        assert isinstance(mem_maxsize_any, int), "memory.maxsize should be int after validation"
        memory_maxsize: int = mem_maxsize_any

        unexpected = set(cfg.keys()) - allowed
        if unexpected:
            raise TypeError(f"Unexpected memory backend keys: {', '.join(sorted(unexpected))}")
        return MemoryPriorityQueue(maxsize=memory_maxsize)

    if normalized == "redis":
        url = ensure_str(cfg.get("url", None), "redis.url", allow_none=True)

        if not url:
            host = cfg.get("host", None)
            if host is not None and not isinstance(host, str):
                raise TypeError("redis.host must be str")
            port_cfg = cfg.get("port", None)
            port_str = None
            if port_cfg is not None:
                if isinstance(port_cfg, int) and not isinstance(port_cfg, bool):
                    port_str = str(port_cfg)
                elif isinstance(port_cfg, str) and port_cfg.isdigit():
                    port_str = port_cfg
                else:
                    raise TypeError("redis.port must be int or numeric str")
            user = cfg.get("user", cfg.get("username", None))
            if user is not None and not isinstance(user, str):
                raise TypeError("redis.user must be str")
            password = cfg.get("password", cfg.get("pass", None))
            if password is not None and not isinstance(password, str):
                raise TypeError("redis.password must be str")

            if host:
                auth = ""
                if user:
                    auth = f"{user}:{password or ''}@"
                port_part = f":{port_str}" if port_str else ""
                url = f"redis://{auth}{host}{port_part}/0"

        namespace_any = ensure_str(cfg.get("namespace", "qcrawl"), "redis.namespace")
        assert isinstance(namespace_any, str), "redis.namespace must be str"
        namespace: str = namespace_any

        ssl = (
            bool(cfg.get("ssl", False))
            if isinstance(cfg.get("ssl", False), bool)
            else ensure_bool(cfg.get("ssl", False), "redis.ssl")
        )
        dedupe = (
            bool(cfg.get("dedupe", False))
            if isinstance(cfg.get("dedupe", False), bool)
            else ensure_bool(cfg.get("dedupe", False), "redis.dedupe")
        )
        update_priority = (
            bool(cfg.get("update_priority", False))
            if isinstance(cfg.get("update_priority", False), bool)
            else ensure_bool(cfg.get("update_priority", False), "redis.update_priority")
        )

        fingerprint_size_any = ensure_int(cfg.get("fingerprint_size", 16), "redis.fingerprint_size")
        assert isinstance(fingerprint_size_any, int), "redis.fingerprint_size must be int"
        fingerprint_size: int = fingerprint_size_any

        item_ttl: int | None = None
        if "item_ttl" in cfg:
            item_ttl = ensure_int(cfg.get("item_ttl"), "redis.item_ttl", allow_none=True)
        dedupe_ttl: int | None = None
        if "dedupe_ttl" in cfg:
            dedupe_ttl = ensure_int(cfg.get("dedupe_ttl"), "redis.dedupe_ttl", allow_none=True)

        max_orphan_retries_any = ensure_int(
            cfg.get("max_orphan_retries", 10), "redis.max_orphan_retries"
        )
        assert isinstance(max_orphan_retries_any, int), "redis.max_orphan_retries must be int"
        max_orphan_retries: int = max_orphan_retries_any

        redis_maxsize: int | None = None
        if "maxsize" in cfg:
            redis_maxsize = ensure_int(cfg.get("maxsize"), "redis.maxsize", allow_none=True)

        redis_kwargs_val = cfg.get("redis_kwargs", {}) or {}
        if not isinstance(redis_kwargs_val, dict):
            raise TypeError("redis.redis_kwargs must be a dict if provided")
        redis_kwargs: dict[str, object] = dict(redis_kwargs_val)

        unexpected = set(cfg.keys()) - allowed
        if unexpected:
            raise TypeError(
                f"Unexpected top-level redis backend keys: {', '.join(sorted(unexpected))}. "
                "Provide driver-specific options under 'redis_kwargs'."
            )

        try:
            from qcrawl.core.queues.redis import RedisQueue
        except ImportError as exc:
            raise ImportError(
                "Redis queue requested but redis extras not installed. Run: pip install 'qcrawl[redis]'"
            ) from exc

        return RedisQueue(
            url=url or "redis://localhost:6379/0",
            namespace=namespace,
            ssl=ssl,
            dedupe=dedupe,
            update_priority=update_priority,
            fingerprint_size=fingerprint_size,
            item_ttl=item_ttl,
            dedupe_ttl=dedupe_ttl,
            max_orphan_retries=max_orphan_retries,
            maxsize=redis_maxsize,
            **redis_kwargs,
        )

    cls_path = cfg.get("class")
    if cls_path:
        module_name, sep, class_name = str(cls_path).rpartition(".")
        if not sep:
            raise ImportError(f"Invalid class path: {cls_path!r}")
        module = importlib.import_module(module_name)
        try:
            BackendCls_obj = getattr(module, class_name)
        except AttributeError as exc:
            raise ImportError(f"Module {module_name!r} has no attribute {class_name!r}") from exc

        if not isinstance(BackendCls_obj, type):
            raise ImportError(f"Configured backend {cls_path!r} is not a class")

        if not issubclass(BackendCls_obj, RequestQueue):
            raise TypeError(f"Custom queue backend {cls_path!r} must subclass RequestQueue")

        BackendClass: type[RequestQueue] = BackendCls_obj

        init_kwargs = {k: v for k, v in cfg.items() if k != "class"}
        try:
            instance = BackendClass(**init_kwargs)
        except TypeError as exc:
            raise TypeError(
                f"Failed to instantiate custom queue backend {cls_path!r} with args {init_kwargs!r}: {exc}"
            ) from exc

        return instance

    supported = ", ".join(sorted(backends.keys() or {"memory", "redis"}))
    raise ValueError(f"Unsupported queue backend: {backend!r}. Supported: {supported}")
