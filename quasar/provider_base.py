# quasar/provider_base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Iterable, Protocol, TypedDict, Literal, AsyncIterator, NamedTuple
from aiolimiter import AsyncLimiter
import aiohttp


Interval = Literal["d", "w", "m"]           # extend as needed


class Bar(TypedDict):
    ts: datetime | int                         # end‑time of bar
    sym: str                                   # canonical_id
    o: float; h: float; l: float; c: float
    v: int | float

class Req(NamedTuple):
    sym: str; start: date; end: date; interval: Interval


class HistoricalDataProvider(ABC):
    """
    Pull‑only provider.
    DataHub schedules it; implement *one* method.
    """
    # Rate Limiting
    RATE_LIMIT = None      # (calls, seconds), e.g. (1000, 60)
    CONCURRENCY = 5        # open sockets
    
    def __init__(self, **config):
        calls, seconds = self.RATE_LIMIT or (float("inf"), 1)
        self._limiter = AsyncLimiter(calls, seconds)
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.CONCURRENCY)
        )
        self.cfg = config

    @property
    @abstractmethod
    def name(self) -> str:                      # unique id, e.g. "EOD"
        ...

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