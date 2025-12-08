"""Shared helpers for provider dev harnesses.

These utilities are intentionally lightweight so users can load provider
classes, read simple config files, and build an in‑memory DerivedContext
without needing the full platform running.
"""

from __future__ import annotations

import json
import logging
import os
from importlib import import_module
from pathlib import Path
from typing import Any, Type

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from quasar.lib.common.context import DerivedContext
from quasar.lib.providers import (
    HistoricalDataProvider,
    LiveDataProvider,
    ProviderType,
    load_provider,
)

LOGGER = logging.getLogger(__name__)


def configure_dev_logging(level: int = logging.INFO) -> None:
    """Enable basic logging when running harnesses directly.

    Avoids changing application logging; only configure if nothing is set.
    """
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )


def load_provider_class(identifier: str) -> Type[HistoricalDataProvider] | Type[LiveDataProvider]:
    """Load a provider class from a registry name or dotted path.

    Args:
        identifier: Either a registered provider name (e.g. ``EODHD``) or a
            dotted path like ``package.module:ClassName`` / ``package.module.ClassName``.

    Returns:
        Provider class.

    Raises:
        ImportError: If the module or class cannot be imported.
        KeyError: If the provider name is not registered.
        ValueError: If the resolved object is not a provider subclass.
    """
    if ":" in identifier:
        module_path, class_name = identifier.split(":", maxsplit=1)
    elif identifier.count(".") >= 1:
        module_path, _, class_name = identifier.rpartition(".")
    else:
        cls = load_provider(identifier)
        return cls

    module = import_module(module_path)
    cls = getattr(module, class_name)
    if not issubclass(cls, (HistoricalDataProvider, LiveDataProvider)):
        raise ValueError(f"{identifier} is not a DataProvider subclass")
    return cls


def ensure_provider_type(provider_cls: Type[Any], provider_type: ProviderType) -> None:
    """Assert provider_cls is of the expected ProviderType."""
    if not hasattr(provider_cls, "provider_type"):
        raise ValueError("Provider class is missing provider_type attribute")
    if provider_cls.provider_type != provider_type:
        raise ValueError(
            f"Expected provider_type {provider_type.name}, got {provider_cls.provider_type}"
        )


def parse_provider_type(raw: Any) -> ProviderType:
    """Parse provider_type from config value."""
    if isinstance(raw, ProviderType):
        return raw
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"historical", "history", "hist"}:
            return ProviderType.HISTORICAL
        if normalized in {"live", "realtime", "real-time", "rt"}:
            return ProviderType.REALTIME
    raise ValueError("provider_type must be 'historical' or 'live'")


def load_config(config: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Load a configuration dict from path or return existing dict."""
    if isinstance(config, dict):
        return config
    path = Path(config)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yml", ".yaml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError("pyyaml is required for YAML configs") from exc
        return yaml.safe_load(text)
    return json.loads(text)


def build_plain_context(secrets: dict[str, Any]) -> DerivedContext:
    """Create a DerivedContext from a plain dictionary for dev use.

    This avoids the full SystemContext flow while still exercising the
    same interface as production providers expect.
    """
    payload = json.dumps(secrets or {}, default=str).encode("utf-8")
    key = AESGCM.generate_key(bit_length=128)
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ciphertext = aesgcm.encrypt(nonce, payload, None)
    return DerivedContext(aesgcm, nonce, ciphertext)

