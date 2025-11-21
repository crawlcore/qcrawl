from __future__ import annotations

import contextlib
import os
from pathlib import Path

import orjson
import yaml


def ensure_int(val: object, name: str, allow_none: bool = False) -> int | None:
    if val is None and allow_none:
        return None
    if isinstance(val, int) and not isinstance(val, bool):
        return int(val)
    raise TypeError(f"{name} must be int{', or None' if allow_none else ''}")


def ensure_bool(val: object, name: str) -> bool:
    if isinstance(val, bool):
        return val
    raise TypeError(f"{name} must be bool")


def ensure_str(val: object, name: str, allow_none: bool = False) -> str | None:
    if val is None and allow_none:
        return None
    if isinstance(val, str):
        return val
    raise TypeError(f"{name} must be str{', or None' if allow_none else ''}")


def load_config_file(path: str) -> dict[str, object]:
    """Load config from YAML or JSON file and return dict."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".yaml", ".yml"}:
        data = yaml.safe_load(text) or {}
    else:
        data = orjson.loads(text or "{}")
    if not isinstance(data, dict):
        raise TypeError(f"Config file must contain a dict, got {type(data)}")
    return data


def load_env(prefix: str = "QCRAWL_") -> dict[str, object]:
    """Load namespaced env vars (prefix) into a settings dict."""
    config: dict[str, object] = {}
    get = os.getenv

    if val := get(f"{prefix}CONCURRENCY"):
        with contextlib.suppress(ValueError):
            config["CONCURRENCY"] = int(val)

    if val := get(f"{prefix}CONCURRENCY_PER_DOMAIN"):
        with contextlib.suppress(ValueError):
            config["CONCURRENCY_PER_DOMAIN"] = int(val)

    if val := get(f"{prefix}DELAY_PER_DOMAIN"):
        with contextlib.suppress(ValueError):
            config["DELAY_PER_DOMAIN"] = float(val)

    if val := get(f"{prefix}MAX_DEPTH"):
        with contextlib.suppress(ValueError):
            config["MAX_DEPTH"] = int(val)

    if val := get(f"{prefix}TIMEOUT"):
        with contextlib.suppress(ValueError):
            config["TIMEOUT"] = float(val)

    if val := get(f"{prefix}MAX_RETRIES"):
        with contextlib.suppress(ValueError):
            config["MAX_RETRIES"] = int(val)

    if val := get(f"{prefix}LOG_LEVEL"):
        config["LOG_LEVEL"] = val

    if val := get(f"{prefix}LOG_FILE"):
        config["LOG_FILE"] = val

    return config


def map_keys_to_canonical(
    overrides: dict[str, object], canonical_keys: set[str]
) -> dict[str, object]:
    """Map override keys to canonical UPPERCASE names case-insensitively."""
    mapped: dict[str, object] = {}
    for k, v in overrides.items():
        upper_k = k.upper()
        if upper_k in canonical_keys:
            mapped[upper_k] = v
        else:
            mapped[k] = v
    return mapped


def shallow_merge_dicts(base: dict[str, object], overrides: dict[str, object]) -> dict[str, object]:
    """Return a shallow-merged copy of base with overrides applied (dict values merged shallowly)."""
    merged = dict(base)
    for k, v in overrides.items():
        cur = merged.get(k)
        if isinstance(cur, dict) and isinstance(v, dict):
            new = dict(cur)
            new.update(v)
            merged[k] = new
        else:
            merged[k] = v
    return merged
