from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Mapping
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def env_str(name: str, default: str | None = None) -> str | None:
    """Return the raw environment value for `name`, or `default` if the variable is
    not set. Note: this distinguishes unset (`None`) from an empty string.

    Args:
        name: environment variable name
        default: returned when the variable is not set

    Returns:
        str or None
    """
    v = os.getenv(name)
    return v if v is not None else default


def env_bool(name: str, default: bool) -> bool:
    """Parse a boolean-like environment variable.

    Behaviour:
    - If the environment variable `name` is not set or is empty/whitespace-only,
      returns `default`.
    - Recognizes truthy tokens (case-insensitive): "1", "true", "yes", "on".
    - Recognizes falsy tokens (case-insensitive): "0", "false", "no", "off".
    - For any other non-empty value raises ValueError to fail fast on misconfiguration.

    Args:
        name: Environment variable name.
        default: Fallback boolean when env var is unset/empty.

    Returns:
        bool
    """
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default

    low = v.strip().lower()
    if low in {"1", "true", "yes", "on"}:
        return True
    if low in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"Invalid boolean value for env {name!r}: {v!r}")


def env_int(name: str, default: int) -> int:
    """Parse an integer environment variable.

    Empty string, whitespace-only, or unset returns `default`. On parse error a debug message is
    emitted and `default` is returned.

    Args:
        name: environment variable name
        default: fallback integer when env var missing or invalid

    Returns:
        int
    """
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v.strip())
    except Exception:
        logger.debug("Invalid int for env %s: %r", name, v)
        return default


def env_float(name: str, default: float) -> float:
    """Parse a float environment variable.

    Empty string, whitespace-only, or unset returns `default`. On parse error a debug message is
    emitted and `default` is returned.

    Args:
        name: environment variable name
        default: fallback float when env var missing or invalid

    Returns:
        float
    """
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v.strip())
    except Exception:
        logger.debug("Invalid float for env %s: %r", name, v)
        return default


def env_csv_ints(name: str, default: Iterable[int]) -> set[int]:
    """Parse a comma-separated list of integers from an environment variable.

    Behaviour:
    - If the environment variable `name` is not set or is an empty/whitespace-only
      string, returns `set(default)`.
    - Splits on commas, strips whitespace from each token, ignores empty tokens.
    - Converts tokens to `int` and returns them as a `set[int]` (order not preserved).
    - On any parse error logs a debug message and returns `set(default)`.

    Args:
        name: Environment variable name to read.
        default: Iterable of ints used when the variable is unset, empty, or invalid.

    Returns:
        A `set[int]` containing the parsed integers, or `set(default)` on missing/invalid input.
    """
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return set(default)
    try:
        return {int(x.strip()) for x in v.split(",") if x.strip()}
    except Exception:
        logger.debug("Invalid CSV ints for env %s: %r", name, v)
        return set(default)


def apply_env_overrides(
    target: object,
    overrides: Mapping[
        str,
        tuple[
            str,
            object,  # parser function
            object,  # default provider
        ],
    ],
) -> None:
    """
    Apply environment overrides to attributes on `target`.

    - `overrides` maps env var name -> (attr_name, parser_fn, default_provider)
    - Only applies when the env var is explicitly set (distinguish unset vs empty).
    - Uses `parser_fn(env_name, default)` to parse and then assigns to `target.attr_name`.
    - Keeps the existing attribute value on parse errors and logs a debug message.
    """
    for env_name, (attr_name, parser, default_fn) in overrides.items():
        if os.getenv(env_name) is None:
            continue
        if not callable(parser) or not callable(default_fn):
            logger.warning("Invalid override for %s: not callable", env_name)
            continue
        try:
            value = parser(env_name, default_fn())
            setattr(target, attr_name, value)
        except Exception:
            logger.debug("Failed to apply env override %s for %s", env_name, attr_name)
