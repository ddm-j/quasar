from importlib.metadata import entry_points
from typing import Dict, Type

from .core import HistoricalDataProvider, Req, Bar, Interval, ProviderType               # noqa: F401

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
    from .eodhd import EODHDProvider
    from .kraken import KrakenProvider

    # External Entry Points
    for ep in entry_points(group="quasar.providers"):
        cls = ep.load()
        _registry.setdefault(cls.name, cls)

# Re-Export Built-Ins
from .eodhd import EODHDProvider
from .kraken import KrakenProvider
__all__ = ['HistoricalDataProvider', 'EODHDProvider', 'KrakenProvider', 'Bar', 'Req', 'Interval', 'ProviderType']