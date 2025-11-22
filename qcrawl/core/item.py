from __future__ import annotations

from collections.abc import ItemsView, KeysView, ValuesView


class Item:
    """Container for scraped fields and internal metadata.

    Purpose:
        Provide predictable data container for scraped items. Spiders
        and pipelines may read and mutate `.data` and `.metadata` directly.
        The object is not frozen — pipelines may transform the item in-place.

    Attributes:
        _data (dict[str, object]): Primary scraped fields (e.g., title, price).
        _metadata (dict[str, object]): Internal metadata (e.g., crawl depth, timestamps).

    Behavior:
        - Access fields via the `.data` property or mapping-style access
          (`item["field"]`).
        - Access internal metadata via the `.metadata` property.
        - Mapping-style reads raise `KeyError` for missing keys (consistent with `dict`).
        - `.get()` provides a safe accessor with a default fallback.

    Example:
        >>> item = Item({"title": "Hello"}, {"depth": 2})
        >>> item.data["title"]
        'Hello'
        >>> item["source"] = "quotes.toscrape.com"
        >>> "source" in item
        True
        >>> for k in item.keys():
        ...     print(k)
        title
        source
    """

    __slots__ = ("_data", "_metadata")

    def __init__(
        self, data: dict[str, str] | None = None, metadata: dict[str, str] | None = None
    ) -> None:
        """Create a new Item (scraped fields and internal metadata).

        Args:
            data: Optional mapping of scraped fields.
            metadata: Optional mapping of internal metadata.

        Raises:
            None. Defensive callers should validate their inputs before constructing.
        """
        self._data: dict[str, str] = data or {}
        self._metadata: dict[str, str] = metadata or {}

    @property
    def data(self) -> dict[str, str]:
        """Return the main data mapping (scraped fields e.g., title, price).

        Mutating the returned dict changes the Item in-place.
        """
        return self._data

    @property
    def metadata(self) -> dict[str, str]:
        """Return the internal metadata mapping (e.g., crawler depth, timestamp)."""
        return self._metadata

    def __repr__(self) -> str:
        return f"Item(data={self._data!r}, metadata={self._metadata!r})"

    def __getitem__(self, key: str) -> str:
        return self._data[key]

    def __setitem__(self, key: str, value: str) -> None:
        self._data[key] = value

    def get(self, key: str, default: str) -> str:
        """Return `.data.get(key, default)`.

        Convenience wrapper matching the dict API.

        Args:
            key: Field name to look up.
            default: Value returned when key is missing.

        Returns:
            The stored value or *default*.
        """
        return self._data.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def keys(self) -> KeysView[str]:
        """Return a view over `.data` keys."""
        return self._data.keys()

    def values(self) -> ValuesView[str]:
        """Return a view over `.data` values."""
        return self._data.values()

    def items(self) -> ItemsView[str, str]:
        """Iterate over field names (keys) in `.data` to support `for k in item`."""
        return self._data.items()
