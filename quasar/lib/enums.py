"""Generated enum definitions. Do not edit by hand."""
from __future__ import annotations
from enum import Enum
from typing import Iterable

class AssetClass(str, Enum):
    EQUITY = 'equity'
    FUND = 'fund'
    ETF = 'etf'
    BOND = 'bond'
    CRYPTO = 'crypto'
    CURRENCY = 'currency'
    FUTURE = 'future'
    OPTION = 'option'
    INDEX = 'index'
    COMMODITY = 'commodity'
    DERIVATIVE = 'derivative'
    CFD = 'cfd'
    WARRANT = 'warrant'
    ADR = 'adr'
    PREFERRED = 'preferred'
    MUTUAL_FUND = 'mutual_fund'
    MONEY_MARKET = 'money_market'
    RATES = 'rates'
    MBS = 'mbs'
    MUNI = 'muni'
    STRUCTURED_PRODUCT = 'structured_product'

class Interval(str, Enum):
    I_1MIN = '1min'
    I_5MIN = '5min'
    I_15MIN = '15min'
    I_30MIN = '30min'
    I_1H = '1h'
    I_4H = '4h'
    I_1D = '1d'
    I_1W = '1w'
    I_1M = '1M'

ASSET_CLASSES = tuple(e.value for e in AssetClass)
INTERVALS = tuple(e.value for e in Interval)

ASSET_CLASS_ALIAS_MAP = {
    "adr_pref": "preferred",
    "bond_etf": "etf",
    "futures": "future",
    "fx": "currency",
    "index_option": "option",
    "mmf": "money_market",
    "perp": "future",
    "perps": "future",
    "stock": "equity"
}
INTERVAL_ALIAS_MAP = {
    "daily": "1d",
    "one_minute": "1min"
}
ASSET_CLASS_CANONICAL_MAP = {k.lower(): k for k in ASSET_CLASSES}
INTERVAL_CANONICAL_MAP = {k.lower(): k for k in INTERVALS}

def _normalize(value: str | None, aliases: dict[str, str], canonical_map: dict[str, str]) -> str | None:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    v_lower = v.lower()
    if v_lower in aliases:
        return aliases[v_lower]
    if v_lower in canonical_map:
        return canonical_map[v_lower]
    return v_lower  # leave as-is; caller may decide to reject

def normalize_asset_class(value: str | None) -> str | None:
    return _normalize(value, ASSET_CLASS_ALIAS_MAP, ASSET_CLASS_CANONICAL_MAP)

def normalize_interval(value: str | None) -> str | None:
    return _normalize(value, INTERVAL_ALIAS_MAP, INTERVAL_CANONICAL_MAP)
