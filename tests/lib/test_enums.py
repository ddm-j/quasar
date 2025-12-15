import pytest

from quasar.lib.enums import (
    normalize_asset_class,
    normalize_interval,
    ASSET_CLASSES,
    ASSET_CLASS_ALIAS_MAP,
    INTERVALS,
    INTERVAL_ALIAS_MAP,
)
from quasar.lib.providers.devtools import validation


def test_normalize_asset_class_alias_and_trim():
    assert normalize_asset_class(" Fx ") == "currency"
    assert normalize_asset_class("equity") == "equity"
    assert normalize_asset_class(None) is None
    assert normalize_asset_class("   ") is None


def test_normalize_interval_alias_and_unknown_passthrough():
    assert normalize_interval("daily") == "1d"
    assert normalize_interval("1w") == "1w"
    assert normalize_interval("strange") == "strange"

def test_normalize_interval_preserves_canonical_case():
    assert normalize_interval("1M") == "1M"
    assert normalize_interval("1m") == "1M"  # case-insensitive match to canonical


def test_devtools_validation_normalizes_alias_and_rejects_invalid():
    symbols = [
        {
            "provider": "P",
            "provider_id": "ID",
            "symbol": "SYM",
            "name": "Name",
            "exchange": "X",
            "asset_class": "stock",  # alias -> equity
            "base_currency": "USD",
            "quote_currency": "USD",
            "interval": "1m",  # should normalize to canonical 1M and be accepted
        }
    ]
    validation.validate_symbols(symbols, strict=True)
    assert symbols[0]["asset_class"] == "equity"
    assert symbols[0]["interval"] == "1M"

    bad = [
        {
            "provider": "P",
            "provider_id": "ID",
            "symbol": "SYM",
            "name": "Name",
            "exchange": "X",
            "asset_class": "not_real",
            "base_currency": "USD",
            "quote_currency": "USD",
        }
    ]
    with pytest.raises(validation.ValidationError):
        validation.validate_symbols(bad, strict=True)
