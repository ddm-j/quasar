from quasar.providers.core import LiveDataProvider, Interval, Bar, SymbolInfo
from quasar.providers import register_provider
from datetime import date, datetime, timezone
from typing import Iterable
import requests
import aiohttp
import websockets
import json
import re
import urllib.parse

from quasar.common.context import DerivedContext

# class SymbolInfo(TypedDict):
#     provider: str
#     provider_id: str | None
#     isin: str | None
#     symbol: str
#     name: str
#     exchange: str
#     asset_class: str
#     base_currency: str
#     quote_currency: str
#     country: str | None

@register_provider
class KrakenProvider(LiveDataProvider):
    name = 'KRAKEN'
    RATE_LIMIT = (1000, 60)
    close_buffer_seconds = 5

    def __init__(self, context: DerivedContext):
        super().__init__(context)
        self._url = "wss://ws.kraken.com/v2"

    async def get_available_symbols(self) -> list[SymbolInfo]:
        # Get Available Trading Pairs from Kraken
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
        asset_class = 'crypto'
        country = None
        isin = None
        exchange = 'Kraken'
        symbols = []
        for sym, e in result.items():
            if e['quote'] not in ['ZUSD', 'USDC']:
                continue
            quote_currency = 'USD' if e['quote'] == 'ZUSD' else 'USDC'
            base_currency = e['base']
            if not e.get('wsname') or not e.get('altname'):
                continue
            symbol = SymbolInfo(
                provider=self.name,
                provider_id=e['altname'],
                isin=isin,
                symbol=e['wsname'],
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
        """
        Connect to Kraken WebSocket API
        """
        print("Connected to Kraken WebSocket API")
        return await websockets.connect(self._url)

    async def _subscribe(self, interval: Interval, symbols: list[str]) -> None:
        """
        Subscribe to the Kraken WebSocket API for the given symbols
        """
        interval_map = {
            '1min': 1,
            '5min': 5,
            '15min': 15,
            '30min': 30,
            '1h': 60,
            '4h': 240,
            '1d': 1440,
            '1w': 10080,
            '1M': 43200,
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
        """
        Unsubscribe from the Kraken WebSocket API for the given symbols
        """
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
        """
        Parse the message received from the Kraken WebSocket API, Return a Bar
        """
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