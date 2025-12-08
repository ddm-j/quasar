"""Dev harness for historical providers."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Iterable

from quasar.lib.providers import HistoricalDataProvider, Req, ProviderType

from . import validation
from .utils import (
    build_plain_context,
    configure_dev_logging,
    ensure_provider_type,
    load_config,
    load_provider_class,
    parse_provider_type,
)


def _coerce_date(value: object) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"Unsupported date value: {value!r}")


def _make_requests(reqs_cfg: Iterable[dict]) -> list[Req]:
    requests: list[Req] = []
    for req in reqs_cfg:
        requests.append(
            Req(
                sym=req["sym"],
                start=_coerce_date(req["start"]),
                end=_coerce_date(req["end"]),
                interval=req["interval"],
            )
        )
    return requests


async def _run_historical_async(
    provider_cls: type[HistoricalDataProvider],
    config: dict,
    strict: bool,
    limit: int | None,
) -> list:
    ensure_provider_type(provider_cls, ProviderType.HISTORICAL)

    secrets = config.get("secrets", {})
    context = build_plain_context(secrets)
    requests = _make_requests(config.get("requests", []))

    if not requests:
        raise ValueError("No historical requests provided")

    async with provider_cls(context) as provider:
        bars = await validation.drain_async_iterable(
            provider.get_history_many(requests),
            limit=limit,
        )

    validation.validate_bar_sequence(bars, require_sorted=True, strict=strict)
    return bars


def run_historical(
    config: dict | str,
    strict: bool = True,
    limit: int | None = 500,
) -> list:
    """Execute a historical provider and return collected bars with validation."""
    configure_dev_logging()
    cfg = load_config(config)
    provider_type = parse_provider_type(cfg.get("provider_type"))
    if provider_type != ProviderType.HISTORICAL:
        raise ValueError("Config provider_type must be 'historical' for bars")
    provider_cls = load_provider_class(cfg["provider"])
    return asyncio.run(_run_historical_async(provider_cls, cfg, strict=strict, limit=limit))

