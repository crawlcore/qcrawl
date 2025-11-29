"""Tests for qcrawl.utils.env"""

import pytest

from qcrawl.utils.env import (
    apply_env_overrides,
    env_bool,
    env_csv_ints,
    env_float,
    env_int,
    env_str,
)

# env_str Tests


def test_env_str_returns_value_when_set(monkeypatch):
    """env_str returns environment value when set."""
    monkeypatch.setenv("TEST_VAR", "hello")

    result = env_str("TEST_VAR")

    assert result == "hello"


def test_env_str_returns_default_when_unset(monkeypatch):
    """env_str returns default when environment variable is unset."""
    monkeypatch.delenv("TEST_VAR", raising=False)

    result = env_str("TEST_VAR", default="default_value")

    assert result == "default_value"


def test_env_str_returns_none_when_unset_no_default(monkeypatch):
    """env_str returns None when unset and no default provided."""
    monkeypatch.delenv("TEST_VAR", raising=False)

    result = env_str("TEST_VAR")

    assert result is None


def test_env_str_distinguishes_empty_from_unset(monkeypatch):
    """env_str returns empty string when set to empty (not default)."""
    monkeypatch.setenv("TEST_VAR", "")

    result = env_str("TEST_VAR", default="default_value")

    assert result == ""


# env_bool Tests


def test_env_bool_returns_default_when_unset(monkeypatch):
    """env_bool returns default when environment variable is unset."""
    monkeypatch.delenv("TEST_BOOL", raising=False)

    result = env_bool("TEST_BOOL", default=True)

    assert result is True


def test_env_bool_returns_default_when_empty(monkeypatch):
    """env_bool returns default when environment variable is empty."""
    monkeypatch.setenv("TEST_BOOL", "")

    result = env_bool("TEST_BOOL", default=False)

    assert result is False


def test_env_bool_returns_default_when_whitespace(monkeypatch):
    """env_bool returns default when environment variable is whitespace only."""
    monkeypatch.setenv("TEST_BOOL", "   ")

    result = env_bool("TEST_BOOL", default=True)

    assert result is True


def test_env_bool_parses_truthy_values(monkeypatch):
    """env_bool parses truthy values (1, true, yes, on)."""
    truthy_values = ["1", "true", "yes", "on", "TRUE", "YES", "ON", "True"]

    for value in truthy_values:
        monkeypatch.setenv("TEST_BOOL", value)
        result = env_bool("TEST_BOOL", default=False)
        assert result is True, f"Failed to parse {value!r} as True"


def test_env_bool_parses_falsy_values(monkeypatch):
    """env_bool parses falsy values (0, false, no, off)."""
    falsy_values = ["0", "false", "no", "off", "FALSE", "NO", "OFF", "False"]

    for value in falsy_values:
        monkeypatch.setenv("TEST_BOOL", value)
        result = env_bool("TEST_BOOL", default=True)
        assert result is False, f"Failed to parse {value!r} as False"


def test_env_bool_raises_on_invalid_value(monkeypatch):
    """env_bool raises ValueError for invalid boolean values."""
    monkeypatch.setenv("TEST_BOOL", "invalid")

    with pytest.raises(ValueError, match="Invalid boolean value"):
        env_bool("TEST_BOOL", default=False)


# env_int Tests


def test_env_int_returns_default_when_unset(monkeypatch):
    """env_int returns default when environment variable is unset."""
    monkeypatch.delenv("TEST_INT", raising=False)

    result = env_int("TEST_INT", default=42)

    assert result == 42


def test_env_int_returns_default_when_empty(monkeypatch):
    """env_int returns default when environment variable is empty."""
    monkeypatch.setenv("TEST_INT", "")

    result = env_int("TEST_INT", default=100)

    assert result == 100


def test_env_int_parses_valid_integer(monkeypatch):
    """env_int parses valid integer values."""
    monkeypatch.setenv("TEST_INT", "123")

    result = env_int("TEST_INT", default=0)

    assert result == 123


def test_env_int_parses_negative_integer(monkeypatch):
    """env_int parses negative integer values."""
    monkeypatch.setenv("TEST_INT", "-456")

    result = env_int("TEST_INT", default=0)

    assert result == -456


def test_env_int_strips_whitespace(monkeypatch):
    """env_int strips whitespace from value."""
    monkeypatch.setenv("TEST_INT", "  789  ")

    result = env_int("TEST_INT", default=0)

    assert result == 789


def test_env_int_returns_default_on_invalid_value(monkeypatch):
    """env_int returns default when value is not a valid integer."""
    monkeypatch.setenv("TEST_INT", "not_an_int")

    result = env_int("TEST_INT", default=99)

    assert result == 99


# env_float Tests


def test_env_float_returns_default_when_unset(monkeypatch):
    """env_float returns default when environment variable is unset."""
    monkeypatch.delenv("TEST_FLOAT", raising=False)

    result = env_float("TEST_FLOAT", default=3.14)

    assert result == 3.14


def test_env_float_returns_default_when_empty(monkeypatch):
    """env_float returns default when environment variable is empty."""
    monkeypatch.setenv("TEST_FLOAT", "")

    result = env_float("TEST_FLOAT", default=2.71)

    assert result == 2.71


def test_env_float_parses_valid_float(monkeypatch):
    """env_float parses valid float values."""
    monkeypatch.setenv("TEST_FLOAT", "1.23")

    result = env_float("TEST_FLOAT", default=0.0)

    assert result == 1.23


def test_env_float_parses_integer_as_float(monkeypatch):
    """env_float parses integer string as float."""
    monkeypatch.setenv("TEST_FLOAT", "42")

    result = env_float("TEST_FLOAT", default=0.0)

    assert result == 42.0


def test_env_float_strips_whitespace(monkeypatch):
    """env_float strips whitespace from value."""
    monkeypatch.setenv("TEST_FLOAT", "  9.87  ")

    result = env_float("TEST_FLOAT", default=0.0)

    assert result == 9.87


def test_env_float_returns_default_on_invalid_value(monkeypatch):
    """env_float returns default when value is not a valid float."""
    monkeypatch.setenv("TEST_FLOAT", "not_a_float")

    result = env_float("TEST_FLOAT", default=1.5)

    assert result == 1.5


# env_csv_ints Tests


def test_env_csv_ints_returns_default_when_unset(monkeypatch):
    """env_csv_ints returns default set when environment variable is unset."""
    monkeypatch.delenv("TEST_CSV", raising=False)

    result = env_csv_ints("TEST_CSV", default=[1, 2, 3])

    assert result == {1, 2, 3}


def test_env_csv_ints_returns_default_when_empty(monkeypatch):
    """env_csv_ints returns default set when environment variable is empty."""
    monkeypatch.setenv("TEST_CSV", "")

    result = env_csv_ints("TEST_CSV", default=[4, 5])

    assert result == {4, 5}


def test_env_csv_ints_parses_comma_separated_integers(monkeypatch):
    """env_csv_ints parses comma-separated integers."""
    monkeypatch.setenv("TEST_CSV", "10,20,30")

    result = env_csv_ints("TEST_CSV", default=[])

    assert result == {10, 20, 30}


def test_env_csv_ints_strips_whitespace(monkeypatch):
    """env_csv_ints strips whitespace from tokens."""
    monkeypatch.setenv("TEST_CSV", " 1 , 2 , 3 ")

    result = env_csv_ints("TEST_CSV", default=[])

    assert result == {1, 2, 3}


def test_env_csv_ints_ignores_empty_tokens(monkeypatch):
    """env_csv_ints ignores empty tokens."""
    monkeypatch.setenv("TEST_CSV", "1,,2,,,3")

    result = env_csv_ints("TEST_CSV", default=[])

    assert result == {1, 2, 3}


def test_env_csv_ints_returns_set(monkeypatch):
    """env_csv_ints returns a set (duplicates removed)."""
    monkeypatch.setenv("TEST_CSV", "1,2,2,3,3,3")

    result = env_csv_ints("TEST_CSV", default=[])

    assert result == {1, 2, 3}


def test_env_csv_ints_returns_default_on_invalid_value(monkeypatch):
    """env_csv_ints returns default when value contains non-integers."""
    monkeypatch.setenv("TEST_CSV", "1,not_int,3")

    result = env_csv_ints("TEST_CSV", default=[99])

    assert result == {99}


# apply_env_overrides Tests


def test_apply_env_overrides_applies_when_set(monkeypatch):
    """apply_env_overrides applies env var to object attribute."""
    monkeypatch.setenv("TEST_VAR", "123")

    class Target:
        value = 0

    target = Target()
    overrides = {"TEST_VAR": ("value", env_int, lambda: 0)}

    apply_env_overrides(target, overrides)

    assert target.value == 123


def test_apply_env_overrides_skips_when_unset(monkeypatch):
    """apply_env_overrides skips when env var is not set."""
    monkeypatch.delenv("TEST_VAR", raising=False)

    class Target:
        value = 42

    target = Target()
    overrides = {"TEST_VAR": ("value", env_int, lambda: 0)}

    apply_env_overrides(target, overrides)

    assert target.value == 42  # Unchanged


def test_apply_env_overrides_applies_multiple(monkeypatch):
    """apply_env_overrides applies multiple env var overrides."""
    monkeypatch.setenv("VAR1", "100")
    monkeypatch.setenv("VAR2", "true")

    class Target:
        num = 0
        flag = False

    target = Target()
    overrides = {
        "VAR1": ("num", env_int, lambda: 0),
        "VAR2": ("flag", env_bool, lambda: False),
    }

    apply_env_overrides(target, overrides)

    assert target.num == 100
    assert target.flag is True


def test_apply_env_overrides_handles_parse_error(monkeypatch):
    """apply_env_overrides applies parser default on parse error."""
    monkeypatch.setenv("TEST_VAR", "invalid")

    class Target:
        value = 42

    target = Target()
    overrides = {"TEST_VAR": ("value", env_int, lambda: 0)}

    # env_int returns default (0) on parse error
    apply_env_overrides(target, overrides)

    assert target.value == 0


# Integration Tests


def test_env_parsing_integration(monkeypatch):
    """Integration: all env parsers work correctly together."""
    monkeypatch.setenv("STR_VAR", "hello")
    monkeypatch.setenv("BOOL_VAR", "true")
    monkeypatch.setenv("INT_VAR", "42")
    monkeypatch.setenv("FLOAT_VAR", "3.14")
    monkeypatch.setenv("CSV_VAR", "1,2,3")

    assert env_str("STR_VAR") == "hello"
    assert env_bool("BOOL_VAR", default=False) is True
    assert env_int("INT_VAR", default=0) == 42
    assert env_float("FLOAT_VAR", default=0.0) == 3.14
    assert env_csv_ints("CSV_VAR", default=[]) == {1, 2, 3}


def test_apply_env_overrides_integration(monkeypatch):
    """Integration: apply_env_overrides with all parser types."""
    monkeypatch.setenv("APP_NAME", "qcrawl")
    monkeypatch.setenv("APP_DEBUG", "yes")
    monkeypatch.setenv("APP_WORKERS", "8")
    monkeypatch.setenv("APP_TIMEOUT", "30.5")

    class Config:
        name = "default"
        debug = False
        workers = 1
        timeout = 10.0

    config = Config()
    overrides = {
        "APP_NAME": ("name", env_str, lambda: "default"),
        "APP_DEBUG": ("debug", env_bool, lambda: False),
        "APP_WORKERS": ("workers", env_int, lambda: 1),
        "APP_TIMEOUT": ("timeout", env_float, lambda: 10.0),
    }

    apply_env_overrides(config, overrides)

    assert config.name == "qcrawl"
    assert config.debug is True
    assert config.workers == 8
    assert config.timeout == 30.5
