"""Registry test fixtures and utilities."""


class MockRecord:
    """Mock asyncpg record that supports both dictionary and attribute access."""

    def __init__(self, **kwargs):
        self._data = kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __contains__(self, key):
        return key in self._data

    def keys(self):
        """Support dict() conversion."""
        return self._data.keys()

    def __iter__(self):
        """Support dict() conversion."""
        return iter(self._data)

    def items(self):
        """Support dict() conversion."""
        return self._data.items()

    def values(self):
        """Support dict() conversion."""
        return self._data.values()
