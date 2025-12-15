import pytest
import logging
from asyncpg.exceptions import UndefinedTableError

from quasar.lib.common.enum_guard import validate_enums


class FakeConn:
    def __init__(self, rows, exc: Exception | None = None):
        self._rows = rows
        self._exc = exc

    async def fetch(self, _query):
        if self._exc:
            raise self._exc
        return self._rows


class FakePool:
    def __init__(self, rows_assets, rows_intervals, exc_assets=None, exc_intervals=None):
        self._rows_assets = rows_assets
        self._rows_intervals = rows_intervals
        self._exc_assets = exc_assets
        self._exc_intervals = exc_intervals
        self._call = 0

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self_inner):
                pool._call += 1
                if pool._call == 1:
                    return FakeConn(pool._rows_assets, pool._exc_assets)
                return FakeConn(pool._rows_intervals, pool._exc_intervals)

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()


@pytest.mark.asyncio
async def test_validate_enums_passes_when_matching(caplog):
    caplog.set_level(logging.INFO, logger="quasar.lib.common.enum_guard")
    pool = FakePool(
        rows_assets=[{"code": c} for c in ["equity", "fund", "etf", "bond", "crypto", "currency", "future", "option", "index", "commodity", "derivative", "cfd", "warrant", "adr", "preferred", "mutual_fund", "money_market", "rates", "mbs", "muni", "structured_product"]],
        rows_intervals=[{"interval": i} for i in ["1min", "5min", "15min", "30min", "1h", "4h", "1d", "1w", "1M"]],
    )
    await validate_enums(pool, strict=False)
    assert any("match generated enums" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_validate_enums_warns_on_mismatch(caplog):
    pool = FakePool(
        rows_assets=[{"code": "equity"}],  # missing crypto; extra bond triggers
        rows_intervals=[{"interval": "1d"}, {"interval": "weird"}],
    )
    await validate_enums(pool, strict=False)
    assert any("Enum guard warning" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_validate_enums_strict_raises():
    pool = FakePool(
        rows_assets=[{"code": "equity"}],
        rows_intervals=[{"interval": "1d"}, {"interval": "weird"}],
    )
    with pytest.raises(RuntimeError):
        await validate_enums(pool, strict=True)


@pytest.mark.asyncio
async def test_validate_enums_skips_when_table_missing(caplog):
    pool = FakePool(
        rows_assets=[{"code": "equity"}],
        rows_intervals=[],
        exc_intervals=UndefinedTableError("missing"),
    )
    await validate_enums(pool, strict=False)
    assert any("table accepted_intervals missing" in rec.message for rec in caplog.records)
