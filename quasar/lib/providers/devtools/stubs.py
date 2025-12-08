"""Minimal stub providers for dev harness usage and tests."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import AsyncIterator

from quasar.lib.providers import HistoricalDataProvider, Interval, LiveDataProvider, SymbolInfo


class HistoricalStub(HistoricalDataProvider):
    """Deterministic historical provider returning synthetic bars."""

    name = "DEV_HIST_STUB"

    async def get_available_symbols(self) -> list[SymbolInfo]:  # pragma: no cover - unused in tests
        return []

    async def get_history(
        self, sym: str, start: date, end: date, interval: Interval
    ) -> AsyncIterator[dict]:
        current = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc)
        delta = timedelta(days=1)
        price = 100.0
        while current <= end_dt:
            bar = {
                "ts": current,
                "sym": sym,
                "o": price,
                "h": price + 1,
                "l": price - 1,
                "c": price + 0.5,
                "v": 1000,
            }
            yield bar
            current += delta
            price += 1.0


class LiveStub(LiveDataProvider):
    """Deterministic live provider returning a single bar per symbol."""

    name = "DEV_LIVE_STUB"

    @property
    def close_buffer_seconds(self) -> int:  # pragma: no cover - unused
        return 0

    async def get_available_symbols(self) -> list[SymbolInfo]:  # pragma: no cover - unused
        return []

    async def get_live(self, interval: Interval, symbols: list[str], timeout: int | None = None):
        now = datetime.now(timezone.utc)
        bars = []
        for idx, sym in enumerate(symbols):
            price = 50.0 + idx
            bars.append(
                {
                    "ts": now,
                    "sym": sym,
                    "o": price,
                    "h": price + 1,
                    "l": price - 1,
                    "c": price + 0.5,
                    "v": 10_000,
                }
            )
        await asyncio.sleep(0)
        return bars

    # Abstract hooks not used for the stub harness path
    async def _connect(self):
        raise RuntimeError("LiveStub._connect not used")

    async def _subscribe(self, symbols: list[str]):
        return {}

    async def _unsubscribe(self, symbols: list[str]):
        return {}

    async def _parse_message(self, message: str):
        return []


__all__ = ["HistoricalStub", "LiveStub"]

