from quasar.providers.core import HistoricalDataProvider, Interval, Bar
from quasar.providers import register_provider
from datetime import date, datetime, timezone
from typing import Iterable
import requests
import aiohttp

BASE = 'https://eodhd.com/api/eod'

@register_provider
class EODHDProvider(HistoricalDataProvider):
    name = 'EODHD'
    RATE_LIMIT = (1000, 60)

    async def get_history(
        self,
        sym: str,
        start: date,
        end: date,
        interval: Interval,
    ):
        """
        Symbol Pull Implementation for EODHD
        """
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
            f"{BASE}/{sym}"
            f"?from={str(start)}"
            f"&to={str(end)}"
            f"&period={interval}"
            f"&api_token={self.cfg['api_token']}&fmt=json"
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