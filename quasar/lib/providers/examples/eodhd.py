"""Built-in historical data provider for EODHD."""

from quasar.lib.providers.core import HistoricalDataProvider, Interval, Bar, SymbolInfo
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

    async def get_available_symbols(self) -> list[SymbolInfo]:
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
            'common stock': 'equity',
            'fund': 'fund',
            'etf': 'etf',
            'bond': 'bond',
            'currency': 'currency',
        }

        symbol_info = []
        for e in symbols:
            # Asset Class / Exchange Information
            # Crypto and FOREX from EODHD are not exchange specific
            if e['Exchange'] == 'CC':
                exchange = None
                asset_class = 'crypto'
            elif e['Exchange'] == 'FOREX':
                exchange = None
                asset_class = 'currency'
            else:
                exchange = e['Exchange']
                asset_class = class_map.get(e['Type'].lower())
            if asset_class is None:
                continue

            # Currency Information
            base_currency = 'USD'
            quote_currency = None
            currs = None
            if asset_class == 'crypto':
                currs = e['Code'].split('-')
            elif asset_class == 'currency':
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
                isin=e['Isin'],
                symbol=f"{e['Code']}.{e['Exchange']}",
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
        # Map Interval to EODHD API
        eodhd_interval_map = {
            '1d': '1d',
            '1w': '1w',
            '1M': '1M'
        }
        eodhd_interval = eodhd_interval_map.get(interval, '1d')
        if eodhd_interval is None:
            raise ValueError(f"Unsupported interval: {interval}")

        # Create Request
        url = (
            f"{BASE}/eod/{sym}"
            f"?from={str(start)}"
            f"&to={str(end)}"
            f"&period={interval}"
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
