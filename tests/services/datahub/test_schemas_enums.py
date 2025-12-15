import pytest
from pydantic import ValidationError

from quasar.services.datahub import schemas as dh_schemas


def test_symbol_search_item_accepts_canonical_interval():
    item = dh_schemas.SymbolSearchItem(
        common_symbol="AAPL",
        provider="P",
        provider_symbol="AAPL.X",
        has_historical=True,
        has_live=False,
        available_intervals=["1d"],
        last_updated=None,
        asset_info=None,
    )
    assert item.available_intervals == ["1d"]


def test_symbol_search_item_rejects_invalid_interval():
    with pytest.raises(ValidationError):
        dh_schemas.SymbolSearchItem(
            common_symbol="AAPL",
            provider="P",
            provider_symbol="AAPL.X",
            has_historical=True,
            has_live=False,
            available_intervals=["weird"],
        )


def test_asset_info_rejects_invalid_asset_class():
    with pytest.raises(ValidationError):
        dh_schemas.AssetInfo(asset_class="not_real")


def test_asset_info_accepts_canonical_asset_class():
    ai = dh_schemas.AssetInfo(asset_class="equity")
    assert ai.asset_class == "equity"
