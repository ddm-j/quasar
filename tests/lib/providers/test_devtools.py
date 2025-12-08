from datetime import datetime, timezone

import pytest

from quasar.lib.providers.devtools import run_symbols, validation
from quasar.lib.providers.devtools.historical import run_historical
from quasar.lib.providers.devtools.live import run_live
from quasar.lib.providers.devtools.stubs import HistoricalStub, LiveStub
from quasar.lib.providers.devtools.utils import load_provider_class


def test_load_provider_class_dotted_path():
    cls = load_provider_class("quasar.lib.providers.devtools.stubs:HistoricalStub")
    assert cls is HistoricalStub


def test_run_historical_stub_returns_bars():
    config = {
        "provider_type": "historical",
        "provider": "quasar.lib.providers.devtools.stubs:HistoricalStub",
        "requests": [
            {"sym": "TEST", "start": "2024-01-01", "end": "2024-01-02", "interval": "1d"}
        ],
        "secrets": {},
    }
    bars = run_historical(config, strict=True, limit=10)
    assert len(bars) == 2
    assert bars[0]["sym"] == "TEST"
    assert bars[0]["o"] < bars[-1]["o"]


def test_run_live_stub_returns_per_symbol():
    config = {
        "provider_type": "live",
        "provider": "quasar.lib.providers.devtools.stubs:LiveStub",
        "interval": "1min",
        "symbols": ["AAA", "BBB"],
        "secrets": {},
    }
    bars = run_live(config, strict=True, limit=10)
    assert len(bars) == 2
    assert {b["sym"] for b in bars} == {"AAA", "BBB"}


def test_validation_rejects_negative_volume():
    bars = [
        {
            "ts": datetime.now(timezone.utc),
            "sym": "BAD",
            "o": 1,
            "h": 1,
            "l": 1,
            "c": 1,
            "v": -1,
        }
    ]
    with pytest.raises(validation.ValidationError):
        validation.validate_bar_sequence(bars, strict=True)


def test_symbols_validation_and_run():
    cfg = {
        "provider_type": "historical",
        "provider": "quasar.lib.providers.devtools.stubs:HistoricalStub",
        "secrets": {},
    }
    symbols = run_symbols(cfg, strict=True)
    assert isinstance(symbols, list)

