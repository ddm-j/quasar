# quasar/provider_base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone, timedelta
from enum import Enum
from aiolimiter import AsyncLimiter
import json
import aiohttp
import websockets
import asyncio
import functools
from typing import (
    Any,
    Iterable,
    Callable,
    Awaitable,
    TypeVar,
    TypedDict,
    Literal,
    AsyncIterator,
    NamedTuple,
    overload
)

import logging

import websockets.asyncio
logger = logging.getLogger(__name__)

# Provider Primitives
# Data bar inteverval
Interval = Literal[
    '1min', '5min', '15min', '30min', '1h', '4h', '1d', '1w', '1M'
] 

# Data bar type
class Bar(TypedDict):
    ts: datetime | int                         # end‑time of bar
    sym: str                                   # canonical_id
    o: float; h: float; l: float; c: float
    v: int | float

# Provider request type
class Req(NamedTuple):
    sym: str; start: date; end: date; interval: Interval

# Provider Class Type Enum
class ProviderType(Enum):
    HISTORICAL = 0                 # historical data provider
    REALTIME = 1                     # real-time data provider

# Asynchronous timeout decorator
T = TypeVar('T')
def async_timeout(seconds: int = 60):
    """
    Asynchronous timeout decorator.
    """
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        nonlocal seconds
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            # Extract Timeout, if Provided
            nonlocal seconds
            seconds = kwargs.pop('timeout', seconds)
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            except Exception as e:
                class_name = args[0].__class__.__name__ if args else ""
                func_name = f"{class_name}.{func.__name__}" if class_name else "Unknown Function"
                logger.error(f"{func_name} timed out after {seconds} seconds. This may be due to a hung/orphaned APScheduler job.")
                raise e
        return wrapper
    return decorator

class DataProvider(ABC):
    """
    Base class for all data provider types
    """
    
    def __init__(self, **config):
        self.cfg = config

    @property
    @abstractmethod
    def name(self) -> str:                   # unique provider id, e.g. "EODHD"
        ...
    @property
    @abstractmethod
    def provider_type(self) -> ProviderType:          # ProviderType enum option
        ...

    # Overload for get_history_many method (HISTORICAL)
    @overload
    async def get_data(self, reqs: Iterable[Req]) -> AsyncIterator[Bar]:
        """
        Get historical data for the given requests.
        """
    
    # Overload for get_live method (REALTIME)
    @overload
    async def get_data(self, interval: Interval, symbols: list[str]) -> list[Bar]:
        """
        Get live data for the given request.
        """

    async def get_data(self, *args, **kwargs) -> AsyncIterator[Bar]:
        """
        Universal method to get data from the provider. Type hints through overloads
        """
        if self.provider_type == ProviderType.HISTORICAL:
            async for bar in self.get_history_many(*args, **kwargs):
                yield bar
        elif self.provider_type == ProviderType.REALTIME:
            bars = await self.get_live(*args, **kwargs)
            for bar in bars:
                yield bar
        else:
            raise ValueError(f"Unsupported provider type: {self.provider_type}")

class HistoricalDataProvider(DataProvider):
    """
    Base class for all historical data provider types
    """
    provider_type = ProviderType.HISTORICAL
    RATE_LIMIT = None      # (calls, seconds), e.g. (1000, 60)
    CONCURRENCY = 5        # open sockets
    
    def __init__(self, **config):
        calls, seconds = self.RATE_LIMIT or (float("inf"), 1)
        self._limiter = AsyncLimiter(calls, seconds)
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.CONCURRENCY)
        )
        self.cfg = config

    async def _api_get(
            self,
            url: str
    ) -> dict:
        async with self._limiter:
            async with self._session.get(url) as r:
                return await r.json()

    async def get_history_many(          # OPTIONAL override
        self,
        reqs: Iterable[Req],
    ) -> AsyncIterator[Bar]:
        # default: loop over single‑symbol method
        for r in reqs:
            async for bar in self.get_history(
                r.sym, r.start, r.end, r.interval
            ):
                yield bar

    @abstractmethod
    async def get_history(
        self,
        sym: str,
        start: date,
        end: date,
        interval: Interval,
    ) -> AsyncIterator[Bar]:
        """
        Return **inclusive** [start, end] bars ordered oldest→newest.
        Raise ValueError if interval unsupported.
        """


    async def aclose(self):
        await self._session.close()

    async def __aenter__(self):   
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

class LiveDataProvider(DataProvider):
    """
    Base class for all live data provider types
    """
    provider_type = ProviderType.REALTIME

    def __init__(self, **config):
        super().__init__(**config)

    @property
    @abstractmethod
    def close_buffer_seconds(self) -> int:
        """
        Number of seconds to keep listening for message after bar close
        """
        ...

    @abstractmethod
    async def _connect(self) -> websockets.asyncio.client.WebSocketClientConnection:
        """
        Establish the websocket connection.
        Implementers should connect to their websocket API here.
        """
        pass
        
    @abstractmethod
    async def _subscribe(self, symbols: list[str]) -> dict:
        """
        Subscribe to updates for the given symbols.
        Send the appropriate subscription messages over the websocket.
        """
        pass
        
    @abstractmethod
    async def _unsubscribe(self, symbols: list[str]) -> dict:
        """
        Unsubscribe from updates for the given symbols.
        """
        pass
        
    @abstractmethod
    async def _parse_message(self, message: str) -> list[Bar]:
        """
        Parse an incoming websocket message and extract OHLCV + timestamp data.
        
        Returns:
            tuple: (symbol, open, high, low, close, volume, timestamp)
        
        Raise ValueError if the message isn't a price update we care about.
        """
        pass

    async def subscribe(
            self,
            connection: websockets.asyncio.client.WebSocketClientConnection,
            interval: Interval,
            symbols: list[str]):
        """
        Subscribe to the given symbols for the specified interval.
        """
        logger.info(f"Subscribing to {symbols.__len__()} on {self.name} WebSocket API")
        try:
            sus_msg = await self._subscribe(interval, symbols)
            await connection.send(json.dumps(sus_msg))
        except Exception as e:
            logger.error(f"Error subscribing to {symbols}: {e}")
            raise e 

    async def unsubscribe(
            self,
            conn: websockets.asyncio.client.WebSocketClientConnection,
            symbols: list[str]):
        """
        Unsubscribe from the given symbols.
        """
        logger.info(f"Unsubscribing from {symbols.__len__()} on {self.name} WebSocket API")
        try:
            unsub_msg = await self._unsubscribe(symbols)
            await conn.send(json.dumps(unsub_msg))
        except Exception as e:
            logger.error(f"Error unsubscribing from {symbols}: {e}")

    @async_timeout()
    async def get_live(self, interval: Interval, symbols: list[str], timeout: int = None) -> list[Bar]:
        """
        Get live data for the given symbols.
        """
        # Open Asynchronous WebSocket connection
        connection = await self._connect()
        async with connection as conn:
            # Subscribe to the Feed
            await self.subscribe(conn, interval, symbols)

            # Determine Cutoff Time
            bar_end = get_next_interval_timestamp(interval)
            cutoff = bar_end + timedelta(seconds=self.close_buffer_seconds)

            # Listen for messages
            symbol_bars: dict[str, Bar] = {}
            async for message in conn:
                # Check current time against cutoff
                now = datetime.now(timezone.utc)
                if now >= cutoff:
                    break

                # Parse the message
                ebars = await self._parse_message(message)
                if (ebars is None) or ebars.__len__() == 0:
                    continue

                for bar in ebars:
                    if bar['ts'] > bar_end:
                        # If bar is older than cutoff, skip it
                        continue
                    symbol_bars[bar['sym']] = bar


            # Unsubscribe from the feed
            await self.unsubscribe(conn, symbols)

        # Check if we got a bar for each symbol
        if set(symbol_bars.keys()) != set(symbols):
            missing_symbols = set(symbols) - set(symbol_bars.keys())
            logger.warning(f"Did not recieve bars for {missing_symbols.__len__()}: {missing_symbols}")

        # Return raw bars
        bars = list(symbol_bars.values())
        return bars


def get_next_interval_timestamp(interval: Interval) -> datetime:
    """Calculate the next even interval timestamp from current time"""
    now = datetime.now(timezone.utc)
    
    if interval == '1min':
        # Next minute
        return datetime(now.year, now.month, now.day, now.hour, 
                      now.minute + (0 if now.second == 0 else 1), 
                      0, 0, tzinfo=timezone.utc)
    
    elif interval == '5min':
        # Next 5 minute mark (00, 05, 10, ...)
        current_minute = now.minute
        next_5min = ((current_minute // 5) + 1) * 5
        if next_5min == 60:
            return (datetime(now.year, now.month, now.day, now.hour + 1, 
                           0, 0, 0, tzinfo=timezone.utc))
        return datetime(now.year, now.month, now.day, now.hour, 
                      next_5min, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '15min':
        # Next 15 minute mark (00, 15, 30, 45)
        current_minute = now.minute
        next_15min = ((current_minute // 15) + 1) * 15
        if next_15min == 60:
            return (datetime(now.year, now.month, now.day, now.hour + 1, 
                           0, 0, 0, tzinfo=timezone.utc))
        return datetime(now.year, now.month, now.day, now.hour, 
                      next_15min, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '30min':
        # Next 30 minute mark (00, 30)
        next_30min = 0 if now.minute >= 30 else 30
        hour_adjust = 1 if now.minute >= 30 else 0
        return datetime(now.year, now.month, now.day, now.hour + hour_adjust, 
                      next_30min, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '1h':
        # Next hour
        return datetime(now.year, now.month, now.day, now.hour + 1, 
                      0, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '4h':
        # Next 4 hour mark (00, 04, 08, 12, 16, 20)
        current_hour = now.hour
        next_4h = ((current_hour // 4) + 1) * 4
        day_adjust = 0
        if next_4h == 24:
            next_4h = 0
            day_adjust = 1
        return datetime(now.year, now.month, now.day + day_adjust, 
                      next_4h, 0, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '1d':
        # Next day, midnight
        return datetime(now.year, now.month, now.day + 1, 
                      0, 0, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '1w':
        # Next Monday
        days_to_monday = (7 - now.weekday()) % 7
        if days_to_monday == 0:
            days_to_monday = 7
        return datetime(now.year, now.month, now.day + days_to_monday, 
                      0, 0, 0, 0, tzinfo=timezone.utc)
    
    elif interval == '1M':
        # First day of next month
        if now.month == 12:
            return datetime(now.year + 1, 1, 1, 
                          0, 0, 0, 0, tzinfo=timezone.utc)
        else:
            return datetime(now.year, now.month + 1, 1, 
                          0, 0, 0, 0, tzinfo=timezone.utc)
    
    raise ValueError(f"Unsupported interval: {interval}")

