from quasar.providers.core import LiveDataProvider, Interval, Bar
from quasar.providers import register_provider
from datetime import date, datetime, timezone
from typing import Iterable
import requests
import aiohttp
import websockets
import json
import re

@register_provider
class KrakenProvider(LiveDataProvider):
    name = 'KRAKEN'
    RATE_LIMIT = None
    close_buffer_seconds = 5

    def __init__(self, **config):
        super().__init__(**config)
        self._url = "wss://ws.kraken.com/v2"

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