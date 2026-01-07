from importlib.metadata import entry_points
from typing import Dict, Type

from .core import (
    DataProvider,
    HistoricalDataProvider,
    LiveDataProvider,
    Req,
    Bar,
    Interval,
    ProviderType,
    SymbolInfo,
)  # noqa: F401

# Centralized Provider Registry
_registry: Dict[str, Type[HistoricalDataProvider]] = {}

def register_provider(cls):
    """Decorater for buit-in providers."""
    _registry[cls.name] = cls
    return cls

def load_provider(name: str):
    """Returns the provider class by its .name (raise KeyError if missing)"""
    if not _registry:
        _autodiscover()
    return _registry[name]

def _autodiscover():
    """Import built-ins and external entry-points"""
    # Built-Ins
    from .examples.eodhd import EODHDProvider
    from .examples.kraken import KrakenProvider

    # External Entry Points
    for ep in entry_points(group="quasar.providers"):
        cls = ep.load()
        _registry.setdefault(cls.name, cls)

# Re-Export Built-Ins
from .examples.eodhd import EODHDProvider
from .examples.kraken import KrakenProvider
__all__ = [
    'DataProvider',
    'HistoricalDataProvider',
    'LiveDataProvider',
    'EODHDProvider',
    'KrakenProvider',
    'Bar',
    'Req',
    'Interval',
    'ProviderType',
    'SymbolInfo',
]