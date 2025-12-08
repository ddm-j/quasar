"""Dev harness for live providers."""

from __future__ import annotations

import asyncio
from typing import Iterable

from quasar.lib.providers import Interval, LiveDataProvider, ProviderType

from . import validation
from .utils import (
    build_plain_context,
    configure_dev_logging,
    ensure_provider_type,
    load_config,
    load_provider_class,
    parse_provider_type,
)


def _coerce_symbols(symbols: Iterable[str]) -> list[str]:
    symbols_list = list(symbols)
    if not symbols_list:
        raise ValueError("At least one symbol is required")
    return symbols_list


async def _run_live_async(
    provider_cls: type[LiveDataProvider],
    config: dict,
    strict: bool,
    limit: int | None,
) -> list:
    ensure_provider_type(provider_cls, ProviderType.REALTIME)

    secrets = config.get("secrets", {})
    context = build_plain_context(secrets)
    interval: Interval = config["interval"]
    symbols = _coerce_symbols(config.get("symbols", []))

    async with provider_cls(context) as provider:
        bars = await provider.get_live(interval=interval, symbols=symbols)

    if limit:
        bars = bars[:limit]

    validation.validate_bar_sequence(bars, require_sorted=False, strict=strict)
    return bars


def run_live(
    config: dict | str,
    strict: bool = True,
    limit: int | None = 50,
) -> list:
    """Execute a live provider and return collected bars with validation."""
    configure_dev_logging()
    cfg = load_config(config)
    provider_type = parse_provider_type(cfg.get("provider_type"))
    if provider_type != ProviderType.REALTIME:
        raise ValueError("Config provider_type must be 'live' for bars")
    provider_cls = load_provider_class(cfg["provider"])
    return asyncio.run(_run_live_async(provider_cls, cfg, strict=strict, limit=limit))

