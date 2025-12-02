"""Tests for qcrawl.utils.settings - CAMOUFOX configuration and nested env expansion."""

import pytest

from qcrawl.settings import Settings
from qcrawl.utils.settings import _set_nested_value, load_env

# === CAMOUFOX Default Configuration Tests ===


def test_camoufox_default_config():
    """Settings includes CAMOUFOX with sensible defaults."""
    s = Settings()
    assert s.CAMOUFOX is not None
    assert isinstance(s.CAMOUFOX, dict)

    # Check default values
    assert s.CAMOUFOX["enabled"] is False
    assert s.CAMOUFOX["max_contexts"] == 10
    assert s.CAMOUFOX["max_pages_per_context"] == 5
    assert s.CAMOUFOX["default_navigation_timeout"] == 30000.0
    assert s.CAMOUFOX["launch_options"] == {}
    assert s.CAMOUFOX["abort_request"] is None
    assert s.CAMOUFOX["process_request_headers"] == "use_scrapy_headers"
    assert s.CAMOUFOX["cdp_url"] is None
    assert s.CAMOUFOX["contexts"] == {"default": {}}


def test_camoufox_in_to_dict():
    """to_dict includes CAMOUFOX settings."""
    s = Settings()
    d = s.to_dict()
    assert "CAMOUFOX" in d
    assert d["CAMOUFOX"] == s.CAMOUFOX


# === CAMOUFOX Validation Tests ===


def test_camoufox_rejects_invalid_keys():
    """CAMOUFOX validation rejects unknown keys."""
    with pytest.raises(ValueError, match="Invalid CAMOUFOX keys"):
        Settings(CAMOUFOX={"unknown_option": 123})


def test_camoufox_validates_enabled_type():
    """CAMOUFOX.enabled must be bool."""
    with pytest.raises(TypeError, match="CAMOUFOX.enabled must be bool"):
        Settings(CAMOUFOX={"enabled": "yes"})


def test_camoufox_validates_max_contexts_type():
    """CAMOUFOX.max_contexts must be int."""
    with pytest.raises(TypeError, match="CAMOUFOX.max_contexts must be int"):
        Settings(CAMOUFOX={"max_contexts": "not_an_int"})


def test_camoufox_validates_max_contexts_range():
    """CAMOUFOX.max_contexts must be >= 1."""
    with pytest.raises(ValueError, match="CAMOUFOX.max_contexts must be >= 1"):
        Settings(CAMOUFOX={"max_contexts": 0})


def test_camoufox_validates_max_pages_per_context_range():
    """CAMOUFOX.max_pages_per_context must be >= 1."""
    with pytest.raises(ValueError, match="CAMOUFOX.max_pages_per_context must be >= 1"):
        Settings(CAMOUFOX={"max_pages_per_context": 0})


def test_camoufox_validates_navigation_timeout_type():
    """CAMOUFOX.default_navigation_timeout must be numeric."""
    with pytest.raises(TypeError, match="CAMOUFOX.default_navigation_timeout must be numeric"):
        Settings(CAMOUFOX={"default_navigation_timeout": "fast"})


def test_camoufox_validates_navigation_timeout_range():
    """CAMOUFOX.default_navigation_timeout must be > 0."""
    with pytest.raises(ValueError, match="CAMOUFOX.default_navigation_timeout must be > 0"):
        Settings(CAMOUFOX={"default_navigation_timeout": -1})


def test_camoufox_validates_launch_options_type():
    """CAMOUFOX.launch_options must be dict or None."""
    with pytest.raises(TypeError, match="CAMOUFOX.launch_options must be dict"):
        Settings(CAMOUFOX={"launch_options": "invalid"})


def test_camoufox_validates_abort_request_type():
    """CAMOUFOX.abort_request must be str or None."""
    with pytest.raises(TypeError, match="CAMOUFOX.abort_request must be str"):
        Settings(CAMOUFOX={"abort_request": 123})


def test_camoufox_validates_process_request_headers_type():
    """CAMOUFOX.process_request_headers must be str or None."""
    with pytest.raises(TypeError, match="CAMOUFOX.process_request_headers must be str"):
        Settings(CAMOUFOX={"process_request_headers": 123})


def test_camoufox_validates_cdp_url_type():
    """CAMOUFOX.cdp_url must be str or None."""
    with pytest.raises(TypeError, match="CAMOUFOX.cdp_url must be str"):
        Settings(CAMOUFOX={"cdp_url": 12345})


def test_camoufox_validates_contexts_type():
    """CAMOUFOX.contexts must be dict or None."""
    with pytest.raises(TypeError, match="CAMOUFOX.contexts must be dict"):
        Settings(CAMOUFOX={"contexts": "not_a_dict"})


def test_camoufox_validates_context_keys_are_strings():
    """CAMOUFOX.contexts keys must be strings."""
    with pytest.raises(TypeError, match="CAMOUFOX.contexts keys must be str"):
        Settings(CAMOUFOX={"contexts": {123: {}}})


def test_camoufox_validates_context_values_are_dicts():
    """CAMOUFOX.contexts values must be dicts or None."""
    with pytest.raises(TypeError, match="CAMOUFOX.contexts.*must be dict"):
        Settings(CAMOUFOX={"contexts": {"mobile": "invalid"}})


def test_camoufox_accepts_valid_custom_config():
    """CAMOUFOX accepts valid custom configuration."""
    custom_config = {
        "enabled": True,
        "max_contexts": 20,
        "max_pages_per_context": 10,
        "default_navigation_timeout": 60000.0,
        "launch_options": {"headless": True},
        "abort_request": "myproject.utils.should_abort",
        "process_request_headers": "use_scrapy_headers",
        "cdp_url": "ws://localhost:9222",
        "contexts": {
            "default": {},
            "mobile": {"viewport": {"width": 375, "height": 667}},
        },
    }
    s = Settings(CAMOUFOX=custom_config)
    assert custom_config == s.CAMOUFOX


def test_camoufox_accepts_none_values():
    """CAMOUFOX accepts None for optional fields."""
    config = {
        "enabled": False,
        "max_contexts": 5,
        "max_pages_per_context": 3,
        "default_navigation_timeout": 15000.0,
        "launch_options": None,
        "abort_request": None,
        "process_request_headers": None,
        "cdp_url": None,
        "contexts": None,
    }
    s = Settings(CAMOUFOX=config)
    assert s.CAMOUFOX["launch_options"] is None
    assert s.CAMOUFOX["contexts"] is None


# === Double-Underscore Nested Env Expansion Tests ===


def test_set_nested_value_single_level():
    """_set_nested_value sets top-level keys correctly."""
    target: dict[str, object] = {}
    _set_nested_value(target, "CONCURRENCY", 50)
    assert target == {"CONCURRENCY": 50}


def test_set_nested_value_two_levels():
    """_set_nested_value expands two-level keys."""
    target: dict[str, object] = {}
    _set_nested_value(target, "CAMOUFOX__MAX_CONTEXTS", 20)
    assert target == {"CAMOUFOX": {"max_contexts": 20}}


def test_set_nested_value_three_levels():
    """_set_nested_value expands three-level keys."""
    target: dict[str, object] = {}
    _set_nested_value(target, "CAMOUFOX__CONTEXTS__MOBILE", {"viewport": {"width": 375}})
    assert target == {"CAMOUFOX": {"contexts": {"mobile": {"viewport": {"width": 375}}}}}


def test_set_nested_value_four_levels():
    """_set_nested_value expands deeply nested keys."""
    target: dict[str, object] = {}
    _set_nested_value(target, "CAMOUFOX__CONTEXTS__MOBILE__VIEWPORT__WIDTH", 375)
    assert target == {"CAMOUFOX": {"contexts": {"mobile": {"viewport": {"width": 375}}}}}


def test_set_nested_value_merges_existing():
    """_set_nested_value merges with existing nested structure."""
    target: dict[str, object] = {"CAMOUFOX": {"enabled": True}}
    _set_nested_value(target, "CAMOUFOX__MAX_CONTEXTS", 20)
    assert target == {"CAMOUFOX": {"enabled": True, "max_contexts": 20}}


def test_set_nested_value_deep_merge():
    """_set_nested_value performs deep merge on nested structures."""
    target: dict[str, object] = {"CAMOUFOX": {"contexts": {"default": {}}}}
    _set_nested_value(target, "CAMOUFOX__CONTEXTS__MOBILE__VIEWPORT__WIDTH", 375)
    assert target == {
        "CAMOUFOX": {
            "contexts": {"default": {}, "mobile": {"viewport": {"width": 375}}}
        }
    }


def test_set_nested_value_lowercase_nested_keys():
    """_set_nested_value lowercases nested keys but uppercases top-level."""
    target: dict[str, object] = {}
    _set_nested_value(target, "CAMOUFOX__LaunchOptions__Headless", True)
    assert target == {"CAMOUFOX": {"launchoptions": {"headless": True}}}


# === load_env with Double-Underscore Support Tests ===


def test_load_env_simple_keys(monkeypatch):
    """load_env handles simple (non-nested) keys."""
    monkeypatch.setenv("QCRAWL_CONCURRENCY", "50")
    result = load_env()
    assert result["CONCURRENCY"] == 50


def test_load_env_nested_keys(monkeypatch):
    """load_env expands double-underscore nested keys."""
    monkeypatch.setenv("QCRAWL_CAMOUFOX__ENABLED", "true")
    monkeypatch.setenv("QCRAWL_CAMOUFOX__MAX_CONTEXTS", "20")
    result = load_env()
    assert "CAMOUFOX" in result
    assert result["CAMOUFOX"]["enabled"] is True
    assert result["CAMOUFOX"]["max_contexts"] == 20


def test_load_env_deeply_nested_keys(monkeypatch):
    """load_env expands deeply nested keys."""
    monkeypatch.setenv("QCRAWL_CAMOUFOX__CONTEXTS__MOBILE__VIEWPORT__WIDTH", "375")
    result = load_env()
    assert result == {"CAMOUFOX": {"contexts": {"mobile": {"viewport": {"width": 375}}}}}


def test_load_env_mixed_keys(monkeypatch):
    """load_env handles both simple and nested keys."""
    monkeypatch.setenv("QCRAWL_CONCURRENCY", "100")
    monkeypatch.setenv("QCRAWL_CAMOUFOX__ENABLED", "true")
    result = load_env()
    assert result["CONCURRENCY"] == 100
    assert result["CAMOUFOX"]["enabled"] is True


def test_load_env_json_value(monkeypatch):
    """load_env parses JSON values for nested configs."""
    monkeypatch.setenv("QCRAWL_CAMOUFOX", '{"enabled": true, "max_contexts": 15}')
    result = load_env()
    assert result["CAMOUFOX"]["enabled"] is True
    assert result["CAMOUFOX"]["max_contexts"] == 15


# === Settings.load Integration Tests ===


def test_settings_load_with_camoufox_env_override(monkeypatch):
    """Settings.load merges CAMOUFOX env overrides correctly."""
    monkeypatch.setenv("QCRAWL_CAMOUFOX__ENABLED", "true")
    monkeypatch.setenv("QCRAWL_CAMOUFOX__MAX_CONTEXTS", "25")

    s = Settings.load()

    # Overridden values
    assert s.CAMOUFOX["enabled"] is True
    assert s.CAMOUFOX["max_contexts"] == 25

    # Default values preserved
    assert s.CAMOUFOX["max_pages_per_context"] == 5
    assert s.CAMOUFOX["default_navigation_timeout"] == 30000.0


def test_settings_load_with_camoufox_json_env(monkeypatch):
    """Settings.load handles JSON CAMOUFOX env var."""
    monkeypatch.setenv(
        "QCRAWL_CAMOUFOX",
        '{"enabled": true, "max_contexts": 30, "cdp_url": "ws://browser:9222"}',
    )

    s = Settings.load()

    assert s.CAMOUFOX["enabled"] is True
    assert s.CAMOUFOX["max_contexts"] == 30
    assert s.CAMOUFOX["cdp_url"] == "ws://browser:9222"
    # Defaults still present
    assert s.CAMOUFOX["max_pages_per_context"] == 5


def test_settings_with_overrides_merges_camoufox():
    """Settings.with_overrides merges CAMOUFOX dicts shallowly."""
    s = Settings()
    s2 = s.with_overrides({"CAMOUFOX": {"enabled": True, "max_contexts": 50}})

    assert s2.CAMOUFOX["enabled"] is True
    assert s2.CAMOUFOX["max_contexts"] == 50
    # Original defaults are merged in
    assert s2.CAMOUFOX["max_pages_per_context"] == 5
    assert s2.CAMOUFOX["contexts"] == {"default": {}}


def test_settings_load_programmatic_override():
    """Settings.load accepts programmatic CAMOUFOX overrides."""
    s = Settings.load(CAMOUFOX={"enabled": True, "max_contexts": 100})

    assert s.CAMOUFOX["enabled"] is True
    assert s.CAMOUFOX["max_contexts"] == 100
