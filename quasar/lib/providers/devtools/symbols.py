"""Dev harness for provider symbol discovery."""

from __future__ import annotations

import asyncio
from typing import Type

from quasar.lib.providers import HistoricalDataProvider, LiveDataProvider, ProviderType

from . import validation
from .utils import (
    build_plain_context,
    configure_dev_logging,
    ensure_provider_type,
    load_config,
    load_provider_class,
    parse_provider_type,
)


async def _run_symbols_async(
    provider_cls: Type[HistoricalDataProvider] | Type[LiveDataProvider],
    config: dict,
    strict: bool,
) -> list[dict]:
    secrets = config.get("secrets", {})
    context = build_plain_context(secrets)

    async with provider_cls(context) as provider:
        symbols = await provider.get_available_symbols()

    validation.validate_symbols(symbols, strict=strict)
    return symbols


def run_symbols(
    config: dict | str,
    strict: bool = True,
) -> list[dict]:
    """Fetch available symbols and validate schema."""
    configure_dev_logging()
    cfg = load_config(config)
    provider_type = parse_provider_type(cfg.get("provider_type"))
    provider_cls = load_provider_class(cfg["provider"])
    ensure_provider_type(provider_cls, provider_type)
    return asyncio.run(_run_symbols_async(provider_cls, cfg, strict=strict))
