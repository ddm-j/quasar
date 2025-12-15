"""Lightweight schema checks for provider outputs."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Iterable, Sequence
import logging

from quasar.lib.enums import (
    ASSET_CLASS_ALIAS_MAP,
    ASSET_CLASSES,
    INTERVAL_ALIAS_MAP,
    INTERVALS,
    normalize_asset_class,
    normalize_interval,
)
from quasar.lib.providers import Bar

logger = logging.getLogger(__name__)


class ValidationError(ValueError):
    """Raised when provider output fails a dev harness check."""


REQUIRED_BAR_FIELDS = ("ts", "sym", "o", "h", "l", "c", "v")


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _ts_key(ts: object) -> float:
    if isinstance(ts, datetime):
        return ts.timestamp()
    if isinstance(ts, date):
        return datetime(ts.year, ts.month, ts.day).timestamp()
    if isinstance(ts, (int, float)) and not math.isnan(ts):
        return float(ts)
    raise ValidationError("Unsupported ts type; expected datetime/date/int/float")


def validate_bar_schema(bar: Bar, strict: bool = True) -> None:
    """Validate a single OHLCV bar."""
    missing = [field for field in REQUIRED_BAR_FIELDS if field not in bar]
    if missing:
        raise ValidationError(f"Missing fields: {missing}")

    if not isinstance(bar["sym"], str) or not bar["sym"]:
        raise ValidationError("sym must be a non-empty string")

    try:
        _ts_key(bar["ts"])
    except ValidationError as exc:
        raise ValidationError(f"Invalid ts: {exc}") from exc

    for price_field in ("o", "h", "l", "c"):
        value = bar[price_field]
        if not _is_number(value) or math.isnan(value) or math.isinf(float(value)):
            raise ValidationError(f"{price_field} must be a finite number")

    volume = bar["v"]
    if not _is_number(volume) or math.isnan(volume) or math.isinf(float(volume)):
        raise ValidationError("v must be a finite number")
    if strict and volume < 0:
        raise ValidationError("v must be non-negative in strict mode")

    if strict:
        high = float(bar["h"])
        low = float(bar["l"])
        if low > high:
            raise ValidationError("l must be <= h")
        for field in ("o", "c"):
            value = float(bar[field])
            if value < low or value > high:
                raise ValidationError(f"{field} out of [l, h] range")


def validate_bar_sequence(bars: Sequence[Bar], require_sorted: bool = False, strict: bool = True) -> None:
    """Validate a sequence of bars for basic correctness."""
    prev_ts = None
    for bar in bars:
        validate_bar_schema(bar, strict=strict)
        if require_sorted:
            ts_key = _ts_key(bar["ts"])
            if prev_ts is not None and ts_key < prev_ts:
                raise ValidationError("Bars must be sorted by ts ascending")
            prev_ts = ts_key


def validate_symbols(symbols: Sequence[dict], strict: bool = True) -> None:
    """Validate a list of symbol dicts with required keys."""
    required = [
        "provider",
        "provider_id",
        "symbol",
        "name",
        "exchange",
        "asset_class",
        "base_currency",
        "quote_currency",
    ]
    for sym in symbols:
        if not isinstance(sym, dict):
            raise ValidationError("Symbol entries must be dicts")
        missing = [k for k in required if k not in sym]
        if missing:
            raise ValidationError(f"Symbol missing fields: {missing}")
        for key in required:
            val = sym.get(key)
            if val is None:
                continue
            if not isinstance(val, str):
                raise ValidationError(f"Symbol field {key} must be a string or None")
            if strict and val.strip() == "":
                raise ValidationError(f"Symbol field {key} cannot be empty in strict mode")

        # Normalize and validate asset_class
        raw_ac = sym.get("asset_class")
        norm_ac = normalize_asset_class(raw_ac)
        if norm_ac is not None and norm_ac in ASSET_CLASS_ALIAS_MAP:
            norm_ac = ASSET_CLASS_ALIAS_MAP[norm_ac]
        if strict and norm_ac not in ASSET_CLASSES:
            raise ValidationError(f"Invalid asset_class: {raw_ac!r}")
        # Update symbol in-place for downstream consumers
        if norm_ac:
            sym["asset_class"] = norm_ac

        # Normalize and validate interval if present (optional field in some flows)
        if "interval" in sym:
            raw_iv = sym.get("interval")
            norm_iv = normalize_interval(raw_iv)
            if norm_iv is not None and norm_iv in INTERVAL_ALIAS_MAP:
                norm_iv = INTERVAL_ALIAS_MAP[norm_iv]
            if strict and norm_iv not in INTERVALS:
                raise ValidationError(f"Invalid interval: {raw_iv!r}")
            if norm_iv:
                sym["interval"] = norm_iv


async def drain_async_iterable(stream: Iterable[Bar], limit: int | None = None) -> list[Bar]:
    """Collect at most ``limit`` items from an async iterator."""
    items: list[Bar] = []
    count = 0
    async for item in stream:
        items.append(item)
        count += 1
        if limit and count >= limit:
            break
    return items

