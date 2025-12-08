"""Quasar trading platform core."""
from __future__ import annotations

from types import ModuleType
from typing import Any

from . import lib

__all__ = ["lib", "services"]


def __getattr__(name: str) -> Any:
    """Lazily import services to avoid side effects during lightweight usage."""
    if name == "services":
        import importlib

        module: ModuleType = importlib.import_module("quasar.services")
        globals()["services"] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
