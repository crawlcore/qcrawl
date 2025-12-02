from __future__ import annotations

import importlib
import os
import tomllib
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import orjson

if TYPE_CHECKING:
    from qcrawl.settings import Settings


def ensure_int(value: object, name: str, *, allow_none: bool = False) -> int | None:
    """Validate/coerce an integer; raise TypeError on invalid input."""
    if value is None:
        if allow_none:
            return None
        raise TypeError(f"{name} must be int, got None")
    if isinstance(value, bool):
        raise TypeError(f"{name} must be int, bool is not allowed")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit() or (s.startswith(("+", "-")) and s[1:].isdigit()):
            return int(s)
    raise TypeError(f"{name} must be int-like, got {type(value)!r}")


def ensure_float(value: object, name: str, *, allow_none: bool = False) -> float | None:
    """Validate/coerce a float; raise TypeError on invalid input."""
    if value is None:
        if allow_none:
            return None
        raise TypeError(f"{name} must be float, got None")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        try:
            return float(s)
        except Exception:
            pass
    raise TypeError(f"{name} must be float-like, got {type(value)!r}")


def ensure_bool(value: object, name: str, *, allow_none: bool = False) -> bool | None:
    """Validate/coerce boolean-like values; raise TypeError on invalid input."""
    if value is None:
        if allow_none:
            return None
        raise TypeError(f"{name} must be bool, got None")
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"1", "true", "yes", "on"}:
            return True
        if low in {"0", "false", "no", "off"}:
            return False
    raise TypeError(f"{name} must be bool-like, got {type(value)!r}")


def ensure_str(value: object, name: str, *, allow_none: bool = False) -> str | None:
    """Validate/coerce string-like values; raise TypeError on invalid input."""
    if value is None:
        if allow_none:
            return None
        raise TypeError(f"{name} must be str, got None")
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8")
        except Exception:
            pass
    # Fallback to str for other types
    return str(value)


def parse_literal(s: str | None) -> bool | int | float | str | None:
    """Parse a simple literal string into bool / int / float / str / None.

    - Handles None → None
    - Empty/whitespace → empty string
    - Boolean: only 'true' / 'false' (case-insensitive) → True / False
    - Tries int → float → returns stripped string
    """
    if s is None:
        return None

    val = s.strip()
    if not val:
        return val  # preserve "" or whitespace-only

    low = val.lower()
    if low == "true":
        return True
    if low == "false":
        return False

    # Try int
    try:
        return int(val)
    except ValueError:
        pass

    # Try float (only if it looks numeric)
    try:
        cleaned = val.lstrip("-+")
        if cleaned.replace(".", "", 1).replace("e", "", 1).replace("E", "", 1).isdigit():
            return float(val)
    except Exception:  # pragma: no cover
        pass

    return val


def parse_json_like(s: str) -> object:
    """Parse a JSON-like string using orjson; raises on parse error."""
    return orjson.loads(s)


_SECRET_KEYS = {"password", "pass", "pwd", "token", "secret"}


def mask_secrets(
    cfg: Mapping[str, object], *, secret_keys: set[str] | None = None
) -> dict[str, object]:
    """Return a shallow copy of mapping with common secret keys masked."""
    sk = {k.lower() for k in (secret_keys or _SECRET_KEYS)}
    out: dict[str, object] = {}
    for k, v in dict(cfg).items():
        if isinstance(k, str) and k.lower() in sk:
            out[k] = "*****" if v else None
        else:
            out[k] = v
    return out


def load_config_file(path: str) -> dict[str, object]:
    """Load config file from TOML format only.

    Args:
        path: Path to the TOML config file (must have .toml extension).

    Returns:
        Dictionary of configuration values.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file does not have a .toml extension.
        TypeError: If the file content is not a dict.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    if p.suffix.lower() != ".toml":
        raise ValueError("Config file must be a TOML file with a .toml extension")
    text = p.read_text(encoding="utf-8")
    data = tomllib.loads(text) or {}
    if not isinstance(data, dict):
        raise TypeError("Config file must yield a dict")
    return data


def load_env(prefix: str = "QCRAWL_") -> dict[str, object]:
    """Load environment overrides using QCRAWL_* variables.

    - Keys are returned uppercased with the prefix stripped.
    - JSON-like values (start with { or [) are parsed via orjson.
    - Otherwise uses `parse_literal` to coerce simple types.
    - Double-underscore keys (e.g., QCRAWL_CAMOUFOX__MAX_CONTEXTS) are expanded
      into nested dicts (e.g., {"CAMOUFOX": {"max_contexts": value}}).
    """
    out: dict[str, object] = {}
    for k, v in os.environ.items():
        if not k.startswith(prefix):
            continue
        key = k[len(prefix) :].strip()
        if not key:
            continue
        raw = v
        if raw is None:
            continue
        s = raw.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                val = parse_json_like(s)
            except Exception:
                val = parse_literal(raw)
        else:
            val = parse_literal(raw)

        # Handle double-underscore for nested keys
        if "__" in key:
            _set_nested_value(out, key, val)
        else:
            out[key.upper()] = val
    return out


def _set_nested_value(target: dict[str, object], key: str, value: object) -> None:
    """Set a nested value using double-underscore key notation.

    Example: key="CAMOUFOX__MAX_CONTEXTS", value=20
    Result: target["CAMOUFOX"]["max_contexts"] = 20

    The first segment is uppercased (top-level setting name).
    Subsequent segments are lowercased (nested keys).
    """
    parts = key.split("__")
    if len(parts) < 2:
        # No nesting, just set directly
        target[key.upper()] = value
        return

    # First part is the top-level setting (UPPERCASE)
    top_key = parts[0].upper()

    # Navigate/create nested structure
    current = target
    for i, part in enumerate(parts[:-1]):
        # Top-level key (UPPERCASE), nested keys (lowercase)
        segment = top_key if i == 0 else part.lower()

        if segment not in current:
            current[segment] = {}
        nested = current[segment]
        if not isinstance(nested, dict):
            # Can't nest into a non-dict value; skip this key
            return
        current = nested

    # Set the final value (nested key in lowercase)
    final_key = parts[-1].lower()
    current[final_key] = value


def map_keys_to_canonical(
    overrides: dict[str, object] | None, valid_keys: Iterable[str]
) -> dict[str, object]:
    """Map override keys case-insensitively to canonical UPPERCASE keys present in valid_keys.
    Unknown keys are preserved as uppercased strings.
    """
    if not overrides:
        return {}
    canon_map = {k.lower(): k for k in map(str, valid_keys)}
    mapped: dict[str, object] = {}
    for k, v in overrides.items():
        if not isinstance(k, str):
            continue
        lower = k.lower()
        if lower in canon_map:
            mapped[canon_map[lower]] = v
        else:
            mapped[k.upper()] = v
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


def get_setting(snapshot: Settings | dict[str, object] | None, name: str) -> object:
    """Case-insensitive lookup for a Settings-like snapshot or plain dict.

    - For dict snapshots: compare keys by uppercasing (return first matching value).
    - For objects: lookup attribute using the UPPERCASE name only (no lowercase fallback).
    """
    if snapshot is None:
        return None
    if not isinstance(name, str):
        name = str(name)
    key_up = name.upper()

    if isinstance(snapshot, dict):
        for k, v in snapshot.items():
            if isinstance(k, str) and k.upper() == key_up:
                return v
        return None

    if hasattr(snapshot, key_up):
        return getattr(snapshot, key_up)
    return None


def resolve_dotted_path(token: object, *, token_name: str = "token") -> object:
    """Resolve a dotted-path string `module.Class` to the attribute, or return token unchanged.

    - If `token` is not a string it is returned unchanged (supports programmatic objects).
    - If `token` is a string it must contain a module part ('.'); non-dotted strings raise
      ValueError, import failures raise ImportError.
    """
    if not isinstance(token, str):
        return token
    tok = ensure_str(token, token_name)
    assert tok is not None  # ensure_str with allow_none=False never returns None
    if "." not in tok:
        raise ValueError(f"Invalid dotted path (no module part): {tok!r}")
    module_name, cls_name = tok.rsplit(".", 1)
    try:
        mod = importlib.import_module(module_name)
    except Exception as exc:
        raise ImportError(f"Could not import module {module_name!r}") from exc
    try:
        return getattr(mod, cls_name)
    except AttributeError as exc:
        raise ImportError(f"Module {module_name!r} has no attribute {cls_name!r}") from exc
