from __future__ import annotations

import importlib

from qcrawl.core.queue import RequestQueue


async def create_queue(backend: str, **init_kwargs: object) -> RequestQueue:
    """Create a queue backend from a dotted class path.

    - `backend` must be a dotted path like `module.Class`.
    - `init_kwargs` are forwarded to the backend class constructor.
    - Ensures the resolved object is a class and a subclass of RequestQueue,
      then instantiates it with `**init_kwargs`.
    """
    if not backend or "." not in backend:
        raise ValueError("backend must be a dotted class path like 'module.Class'")

    module_name, _, class_name = backend.rpartition(".")
    if not module_name or not class_name:
        raise ImportError(f"Invalid backend class path: {backend!r}")

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ImportError(f"Could not import module {module_name!r}") from exc

    try:
        BackendCls = getattr(module, class_name)
    except AttributeError as exc:
        raise ImportError(f"Module {module_name!r} has no attribute {class_name!r}") from exc

    if not isinstance(BackendCls, type):
        raise TypeError(f"Resolved object {backend!r} is not a class")

    if not issubclass(BackendCls, RequestQueue):
        raise TypeError(f"Backend class {backend!r} must subclass RequestQueue")

    try:
        instance = BackendCls(**init_kwargs)
    except TypeError as exc:
        raise TypeError(
            f"Failed to instantiate backend {backend!r} with args {init_kwargs!r}: {exc}"
        ) from exc

    return instance
