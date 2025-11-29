"""Tests for qcrawl.utils.url"""

from qcrawl.utils.url import get_domain, get_domain_base, join_and_normalize, normalize_url

# get_domain Tests


def test_get_domain_returns_host():
    """get_domain returns lowercase host for simple URL."""
    url = "https://Example.COM/path"

    result = get_domain(url)

    assert result == "example.com"


def test_get_domain_includes_port_when_non_default():
    """get_domain includes port for non-default ports."""
    url = "https://example.com:8080/path"

    result = get_domain(url)

    assert result == "example.com:8080"


def test_get_domain_omits_default_http_port():
    """get_domain omits port 80 for http URLs."""
    url = "http://example.com:80/path"

    result = get_domain(url)

    assert result == "example.com"


def test_get_domain_omits_default_https_port():
    """get_domain omits port 443 for https URLs."""
    url = "https://example.com:443/path"

    result = get_domain(url)

    assert result == "example.com"


def test_get_domain_returns_empty_for_no_host():
    """get_domain returns empty string when URL has no host."""
    url = "file:///path/to/file"

    result = get_domain(url)

    assert result == ""


def test_get_domain_returns_empty_for_invalid_url():
    """get_domain returns empty string for unparseable URL."""
    url = "not a valid url at all"

    result = get_domain(url)

    assert result == ""


def test_get_domain_strips_userinfo():
    """get_domain strips username/password from URL."""
    url = "https://user:pass@example.com/path"

    result = get_domain(url)

    assert result == "example.com"


# get_domain_base Tests


def test_get_domain_base_returns_scheme_and_host():
    """get_domain_base returns scheme://host."""
    url = "https://example.com/path?query=1"

    result = get_domain_base(url)

    assert result == "https://example.com"


def test_get_domain_base_lowercases_scheme_and_host():
    """get_domain_base lowercases scheme and host."""
    url = "HTTPS://EXAMPLE.COM/path"

    result = get_domain_base(url)

    assert result == "https://example.com"


def test_get_domain_base_strips_userinfo():
    """get_domain_base strips userinfo."""
    url = "https://user:pass@example.com/path"

    result = get_domain_base(url)

    assert result == "https://example.com"


def test_get_domain_base_omits_port():
    """get_domain_base omits port from result."""
    url = "https://example.com:8080/path"

    result = get_domain_base(url)

    assert result == "https://example.com"


def test_get_domain_base_defaults_to_https_when_no_scheme():
    """get_domain_base uses https when scheme missing."""
    url = "//example.com/path"

    result = get_domain_base(url)

    assert result == "https://example.com"


def test_get_domain_base_returns_default_for_no_host():
    """get_domain_base returns https:// when URL has no host."""
    url = "file:///path/to/file"

    result = get_domain_base(url)

    assert result == "https://"


def test_get_domain_base_returns_default_for_invalid_url():
    """get_domain_base returns https:// for unparseable URL."""
    url = "invalid url"

    result = get_domain_base(url)

    assert result == "https://"


# normalize_url Tests


def test_normalize_url_lowercases_scheme_and_host():
    """normalize_url lowercases scheme and host."""
    url = "HTTPS://EXAMPLE.COM/Path"

    result = normalize_url(url)

    assert result == "https://example.com/Path"


def test_normalize_url_strips_userinfo():
    """normalize_url removes username and password."""
    url = "https://user:pass@example.com/path"

    result = normalize_url(url)

    assert result == "https://example.com/path"


def test_normalize_url_removes_default_http_port():
    """normalize_url removes port 80 for http."""
    url = "http://example.com:80/path"

    result = normalize_url(url)

    assert result == "http://example.com/path"


def test_normalize_url_removes_default_https_port():
    """normalize_url removes port 443 for https."""
    url = "https://example.com:443/path"

    result = normalize_url(url)

    assert result == "https://example.com/path"


def test_normalize_url_keeps_non_default_port():
    """normalize_url keeps non-default ports."""
    url = "https://example.com:8080/path"

    result = normalize_url(url)

    assert result == "https://example.com:8080/path"


def test_normalize_url_removes_fragment():
    """normalize_url removes fragment identifier."""
    url = "https://example.com/path#section"

    result = normalize_url(url)

    assert result == "https://example.com/path"


def test_normalize_url_preserves_query():
    """normalize_url preserves query string."""
    url = "https://example.com/path?foo=bar&baz=qux"

    result = normalize_url(url)

    assert result == "https://example.com/path?foo=bar&baz=qux"


def test_normalize_url_collapses_duplicate_slashes():
    """normalize_url collapses duplicate slashes in path."""
    url = "https://example.com/path//to///resource"

    result = normalize_url(url)

    assert result == "https://example.com/path/to/resource"


def test_normalize_url_resolves_dot_segments():
    """normalize_url resolves . and .. in path."""
    url = "https://example.com/path/./to/../resource"

    result = normalize_url(url)

    assert result == "https://example.com/path/resource"


def test_normalize_url_removes_trailing_slash():
    """normalize_url removes trailing slash from non-root paths."""
    url = "https://example.com/path/"

    result = normalize_url(url)

    assert result == "https://example.com/path"


def test_normalize_url_keeps_root_trailing_slash():
    """normalize_url keeps trailing slash for root path."""
    url = "https://example.com/"

    result = normalize_url(url)

    assert result == "https://example.com/"


def test_normalize_url_adds_root_slash_when_missing():
    """normalize_url adds root slash for host-only URLs."""
    url = "https://example.com"

    result = normalize_url(url)

    assert result == "https://example.com/"


def test_normalize_url_handles_relative_path():
    """normalize_url handles path-only URLs."""
    url = "/path/to/resource"

    result = normalize_url(url)

    assert result == "/path/to/resource"


def test_normalize_url_handles_relative_path_with_query():
    """normalize_url handles path-only URLs with query."""
    url = "/path?foo=bar"

    result = normalize_url(url)

    assert result == "/path?foo=bar"


# join_and_normalize Tests


def test_join_and_normalize_joins_relative_path():
    """join_and_normalize resolves relative path against base."""
    base = "https://example.com/path/page.html"
    href = "other.html"

    result = join_and_normalize(base, href)

    assert result == "https://example.com/path/other.html"


def test_join_and_normalize_joins_absolute_path():
    """join_and_normalize resolves absolute path against base."""
    base = "https://example.com/path/page.html"
    href = "/other/page.html"

    result = join_and_normalize(base, href)

    assert result == "https://example.com/other/page.html"


def test_join_and_normalize_absolute_url_replaces_base():
    """join_and_normalize uses absolute URL, ignoring base."""
    base = "https://example.com/path"
    href = "https://other.com/resource"

    result = join_and_normalize(base, href)

    assert result == "https://other.com/resource"


def test_join_and_normalize_handles_parent_directory():
    """join_and_normalize resolves .. in href."""
    base = "https://example.com/path/to/page.html"
    href = "../other.html"

    result = join_and_normalize(base, href)

    assert result == "https://example.com/path/other.html"


def test_join_and_normalize_normalizes_result():
    """join_and_normalize normalizes the joined URL."""
    base = "HTTPS://EXAMPLE.COM/path/"
    href = "other//page.html#fragment"

    result = join_and_normalize(base, href)

    # Should be normalized: lowercase, no //, no fragment
    assert result == "https://example.com/path/other/page.html"


def test_join_and_normalize_handles_protocol_relative():
    """join_and_normalize handles protocol-relative URLs."""
    base = "https://example.com/path"
    href = "//other.com/resource"

    result = join_and_normalize(base, href)

    assert result == "https://other.com/resource"


def test_join_and_normalize_handles_query():
    """join_and_normalize preserves query string."""
    base = "https://example.com/path/"
    href = "page.html?foo=bar"

    result = join_and_normalize(base, href)

    assert result == "https://example.com/path/page.html?foo=bar"


def test_join_and_normalize_fallback_on_invalid_base():
    """join_and_normalize falls back to simple concatenation on parse error."""
    base = "invalid://base"
    href = "path"

    result = join_and_normalize(base, href)

    # Should attempt some result rather than raising
    assert isinstance(result, str)


# Integration Tests


def test_url_normalization_consistency():
    """Integration: normalize_url produces consistent results."""
    urls = [
        "HTTPS://EXAMPLE.COM/Path",
        "https://EXAMPLE.com/Path",
        "https://example.com:443/Path",
        "https://user:pass@example.com/Path#fragment",
    ]

    results = [normalize_url(url) for url in urls]

    # All should normalize to the same canonical form
    assert len(set(results)) == 1
    assert results[0] == "https://example.com/Path"


def test_join_and_domain_extraction():
    """Integration: join and extract domain from result."""
    base = "https://example.com/path/"
    href = "../other/resource.html"

    joined = join_and_normalize(base, href)
    domain = get_domain(joined)

    assert domain == "example.com"


def test_domain_base_for_robots_txt():
    """Integration: get_domain_base suitable for robots.txt URL construction."""
    url = "https://example.com:8080/path/to/page.html?query=1#section"

    base = get_domain_base(url)

    # robots.txt should be at {base}/robots.txt
    assert base == "https://example.com"
    robots_url = f"{base}/robots.txt"
    assert robots_url == "https://example.com/robots.txt"
