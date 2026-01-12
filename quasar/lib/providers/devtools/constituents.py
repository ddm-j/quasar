"""Harness for testing IndexProvider implementations."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

from quasar.lib.providers import ProviderType

from .utils import (
    load_provider_class,
    build_plain_context,
    ensure_provider_type,
)
from .validation import validate_constituents


def run_constituents(
    config: dict[str, Any],
    strict: bool = True,
) -> list[dict]:
    """Run an IndexProvider and return constituents.

    Args:
        config: Configuration dict with provider, secrets, and optional as_of_date.
        strict: Whether to run strict validation on returned constituents.

    Returns:
        List of constituent dicts from the provider.
    """
    return asyncio.run(_run_constituents_async(config, strict))


async def _run_constituents_async(
    config: dict[str, Any],
    strict: bool,
) -> list[dict]:
    """Async implementation of run_constituents."""
    provider_id = config["provider"]
    secrets = config.get("secrets", {})
    as_of_date_str = config.get("as_of_date")

    as_of_date = None
    if as_of_date_str:
        as_of_date = date.fromisoformat(as_of_date_str)

    cls = load_provider_class(provider_id)
    ensure_provider_type(cls, ProviderType.INDEX)

    ctx = build_plain_context(secrets)
    provider = cls(ctx)

    async with provider:
        constituents = await provider.get_constituents(as_of_date)

    if strict:
        validate_constituents(constituents)

    return constituents
