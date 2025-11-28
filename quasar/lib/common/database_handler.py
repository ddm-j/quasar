import asyncpg
import asyncio
from typing import Optional
from abc import ABC, abstractmethod

import logging
logger = logging.getLogger(__name__)

class DatabaseHandler(ABC):
    """A class to handle database connections and queries."""

    @property
    @abstractmethod
    def name(self) -> str:                   # Name of the database handler
        ...

    def __init__(
            self,
            dsn: str | None = None,
            pool: Optional[asyncpg.Pool] = None) -> None:

        # Pool Setup
        if not dsn and not pool:
            logger.error(f"{self.name} was intialized without DSN or Pool for database connection.")
            raise ValueError("Provide either dsn or pool")
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = pool

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError(f"{self.name} pool not started yet")
        return self._pool
    
    async def init_pool(self) -> None:
        """Create pool (if we own it) – call once at boot."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)

    async def close_pool(self) -> None:
        """Close pool (if we own it) – call once at shutdown."""
        if self._pool is not None and not self._pool._closed:   # type: ignore
            await self._pool.close()

    # async def fetch_from_db(self, query: str, *args) -> list[asyncpg.Record]:
    #     """Fetch a list of records from the database."""
    #     if not self._pool:
    #         raise ValueError("Database pool is not initialized.")
    #     async with self._pool.acquire() as connection:
    #         return await connection.fetch(query, *args)

    # async def copy_to_db(self, table: str, records: list[tuple]) -> None:
    #     """Copy records to the database."""
    #     if not self._pool:
    #         raise ValueError("Database pool is not initialized.")
    #     async with self._pool.acquire() as connection:
    #         await connection.copy_records_to_table(table, records=records)