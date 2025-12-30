"""Built-in live data provider for Kraken WebSocket OHLC data."""

from quasar.lib.enums import AssetClass, Interval
from quasar.lib.providers.core import LiveDataProvider, Bar, SymbolInfo
from quasar.lib.providers import register_provider
from datetime import date, datetime, timezone
from typing import Iterable
import requests
import aiohttp
import websockets
import json
import re
import urllib.parse

from quasar.lib.common.context import DerivedContext

@register_provider
class KrakenProvider(LiveDataProvider):
    name = 'KRAKEN'
    RATE_LIMIT = (1000, 60)
    close_buffer_seconds = 5

    def __init__(self, context: DerivedContext):
        super().__init__(context)
        self._url = "wss://ws.kraken.com/v2"

    async def get_available_symbols(self) -> list[SymbolInfo]:
        """Return supported Kraken trading pairs denominated in USD or USDC."""
        base_url = f"https://api.kraken.com/0/public/AssetPairs"
        params = {
            'country_code': 'US:TX'
        }
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}?{query_string}"

        data = await self._api_get(url)
        result = data.get('result')
        if not result:
            raise ValueError("Error fetching asset pairs from Kraken")
        
        # These Values are the same for all symbols from Kraken
        asset_class = AssetClass.CRYPTO.value
        country = None
        exchange = None
        symbols = []
        for sym, e in result.items():
            if e['quote'] not in ['ZUSD', 'USDC']:
                continue
            quote_currency = 'USD' if e['quote'] == 'ZUSD' else 'USDC'
            base_currency = e['base']
            if not e.get('wsname') or not e.get('altname'):
                continue

            # print(f"{e['wsname']} - {e['altname']} -> {e['wsname'].split('/')[0]}")
            symbol = SymbolInfo(
                provider=self.name,
                provider_id=e['altname'],
                primary_id=None,  # Provider does not supply FIGI
                symbol=e['wsname'],
                matcher_symbol=e['wsname'].split('/')[0],
                name=sym,
                exchange=exchange,
                asset_class=asset_class,
                base_currency=base_currency,
                quote_currency=quote_currency,
                country=country
            )
            symbols.append(symbol)

        return symbols



    async def _connect(self):
        """Connect to the Kraken WebSocket API."""
        print("Connected to Kraken WebSocket API")
        return await websockets.connect(self._url)

    async def _subscribe(self, interval: Interval, symbols: list[str]) -> None:
        """Return subscription payload for Kraken OHLC channel."""
        interval_map = {
            Interval.I_1MIN: 1,
            Interval.I_5MIN: 5,
            Interval.I_15MIN: 15,
            Interval.I_30MIN: 30,
            Interval.I_1H: 60,
            Interval.I_4H: 240,
            Interval.I_1D: 1440,
            Interval.I_1W: 10080,
            Interval.I_1M: 43200,
        }
        kraken_interval = interval_map.get(interval, 1440)
        if kraken_interval is None:
            raise ValueError(f"Unsupported interval: {interval}")

        subscribe_message = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": symbols,
                "interval": kraken_interval,  # 1-minute candles
                "snapshot": False  # Get the latest snapshot of the candles
            }
        }
        
        print(f"Subscribed to {symbols} on Kraken WebSocket API")
        return subscribe_message

    async def _unsubscribe(self, symbols: list[str]) -> None:
        """Return unsubscribe payload for Kraken OHLC channel."""
        unsubscribe_message = {
            "method": "unsubscribe",
            "params": {
                "channel": "ohlc",
                "symbol": symbols,
            }
        }
        
        print(f"Unsubscribed from {symbols} on Kraken WebSocket API")
        return unsubscribe_message

    async def _parse_message(self, message: str) -> list[Bar]:
        """Parse Kraken OHLC websocket message into ``Bar`` objects."""
        data = json.loads(message)

        # Check Message Format
        if not isinstance(data, dict) or 'data' not in data.keys():
            return None
        
        # Extract Candle Data
        candle_data = data['data']
        if not isinstance(candle_data, list):
            return None

        # Check that we got symbols
        for e in candle_data:
            if 'symbol' not in e.keys():
                return None
        
        # Kraken returns 9 point precision floats, we need to convert them to 6 point precision
        def format_timestamp(ts_str):
            return re.sub(r'(\.\d{6})\d+Z', r'\1Z', ts_str)

        bars = []
        for dat in candle_data:
            sym = dat['symbol']
            o = dat['open']
            h = dat['high']
            l = dat['low']
            c = dat['close']
            v = dat['volume']
            ts = format_timestamp(dat['timestamp'])
            ts = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            
            # Create and append Bar object
            bar = Bar(
                ts=ts,
                sym=sym,
                o=o,
                h=h,
                l=l,
                c=c,
                v=v
            )
            bars.append(bar)

        return bars