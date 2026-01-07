"""Built-in historical data provider for EODHD."""

from quasar.lib.enums import AssetClass, Interval
from quasar.lib.providers.core import HistoricalDataProvider, Bar, SymbolInfo
from quasar.lib.providers import register_provider
from datetime import date, datetime, timezone
from typing import Iterable
import requests
import aiohttp

BASE = 'https://eodhd.com/api/'

@register_provider
class EODHDProvider(HistoricalDataProvider):
    name = 'EODHD'
    RATE_LIMIT = (1000, 60)

    EXCHANGE_MAP = {
        "NASDAQ": "XNAS",
        "NYSE": "XNYS",
        "NYSE ARCA": "ARCX",
        "NYSE MKT": "XASE",
        "CC": None,
        "cc": None,
        "FOREX": "XFX"
    }

    async def fetch_available_symbols(self) -> list[SymbolInfo]:
        """Return available symbols from EODHD filtered to supported exchanges."""
        symbols = []

        # Pull Data from Exchanges of Interest
        exchanges = ['NASDAQ', 'NYSE', 'CC', 'FOREX']
        for exchange in exchanges:
            url = f"{BASE}/exchange-symbol-list/{exchange}?" \
                f"api_token={self.context.get('api_token')}&fmt=json"
            data = await self._api_get(url)
            symbols.extend(data)
        

        class_map = {
            'common stock': AssetClass.EQUITY.value,
            'fund': AssetClass.FUND.value,
            'etf': AssetClass.ETF.value,
            'bond': AssetClass.BOND.value,
            'currency': AssetClass.CURRENCY.value,
        }

        symbol_info = []
        for e in symbols:
            # Normalize Exchange Name to MIC (with graceful fallback)
            eodhd_exchange = e.get('Exchange', '')
            exchange = self.EXCHANGE_MAP.get(eodhd_exchange, eodhd_exchange)

            # Asset Class / API Suffix Information
            if eodhd_exchange.lower() == 'cc':
                asset_class = AssetClass.CRYPTO.value
                # EODHD API uses .CC for crypto symbols
                api_symbol_suffix = 'CC'
            elif eodhd_exchange.upper() == 'FOREX':
                asset_class = AssetClass.CURRENCY.value
                # EODHD API uses .FOREX for forex symbols
                api_symbol_suffix = 'FOREX'
            else:
                asset_class = class_map.get(e['Type'].lower())
                # EODHD API uses .US for all U.S. exchanges (NASDAQ, NYSE, etc.)
                if eodhd_exchange in ['NASDAQ', 'NYSE']:
                    api_symbol_suffix = 'US'
                else:
                    # For other exchanges, use the exchange name as-is
                    api_symbol_suffix = eodhd_exchange

            if asset_class is None:
                continue

            # Currency Information
            matcher_symbol = e['Code'].split('.')[0]
            base_currency = 'USD'
            quote_currency = None
            currs = None
            if asset_class == AssetClass.CRYPTO.value:
                currs = e['Code'].split('-')
                matcher_symbol = currs[0]
            elif asset_class == AssetClass.CURRENCY.value:
                try:
                    currs = [e['Code'][:3], e['Code'][3:]]
                    assert len(currs[0]) == 3
                    assert len(currs[1]) == 3
                except:
                    continue
            if currs:
                if len(currs) == 2:
                    base_currency = currs[0]
                    quote_currency = currs[1]
                    if quote_currency != 'USD':
                        # Skip non-USD pairs
                        continue
                else:
                    continue

            syminfo = SymbolInfo(
                provider=self.name,
                provider_id=None,
                primary_id=None,  # Provider does not supply FIGI
                # Use API-compatible symbol format (e.g., AAPL.US for U.S. stocks)
                symbol=f"{e['Code']}.{api_symbol_suffix}",
                matcher_symbol=matcher_symbol,
                name=e['Name'],
                exchange=exchange,
                asset_class=asset_class,
                base_currency=base_currency,
                quote_currency=quote_currency,
                country=e['Country']
            )
            symbol_info.append(syminfo)

        return symbol_info

    async def get_history(
        self,
        sym: str,
        start: date,
        end: date,
        interval: Interval,
    ):
        """Yield historical bars from EODHD for the given symbol and range."""
        # Map Interval to EODHD API (supported subset: 1d,1w,1M)
        eodhd_interval_map = {
            Interval.I_1D: '1d',
            Interval.I_1W: '1w',
            Interval.I_1M: '1M'
        }
        eodhd_interval = eodhd_interval_map.get(interval)
        if eodhd_interval is None:
            raise ValueError(f"Unsupported interval: {interval}")

        # Create Request
        url = (
            f"{BASE}/eod/{sym}"
            f"?from={str(start)}"
            f"&to={str(end)}"
            f"&period={eodhd_interval}"
            f"&api_token={self.context.get('api_token')}&fmt=json"
        )
        
        # Make Request (Uses Rate Builtin Rate Limiter)
        data = await self._api_get(url)

        for e in data:
            # Format Date to UTC
            ts_date = datetime.strptime(e["date"], "%Y-%m-%d")
            ts = datetime(ts_date.year, ts_date.month, ts_date.day, tzinfo=timezone.utc)
            yield Bar(
                ts=ts,
                sym=sym,
                o=e["open"],
                h=e["high"],
                l=e["low"],
                c=e["close"],
                v=e["volume"],
            )
