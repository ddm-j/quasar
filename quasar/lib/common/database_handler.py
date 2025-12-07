"""Shared database handler with pooled connection lifecycle helpers."""

import asyncpg
import asyncio
from typing import Optional
from abc import ABC, abstractmethod

import logging
logger = logging.getLogger(__name__)


class DatabaseHandler(ABC):
    """Manage asyncpg pools and expose a unified interface."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-friendly identifier used for logging."""
        ...

    def __init__(
            self,
            dsn: str | None = None,
            pool: Optional[asyncpg.Pool] = None) -> None:
        """Configure the handler with either a DSN or an existing pool.

        Args:
            dsn (str | None): Database connection string used to create a pool.
            pool (asyncpg.Pool | None): Pre-existing pool to reuse.

        Raises:
            ValueError: If neither ``dsn`` nor ``pool`` is provided.
        """

        # Pool Setup
        if not dsn and not pool:
            logger.error(f"{self.name} was intialized without DSN or Pool for database connection.")
            raise ValueError("Provide either dsn or pool")
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = pool

    @property
    def pool(self) -> asyncpg.Pool:
        """Return the active pool or raise if it is not initialized."""
        if self._pool is None:
            raise RuntimeError(f"{self.name} pool not started yet")
        return self._pool
    
    async def init_pool(self) -> None:
        """Create the asyncpg pool if this handler owns it."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)

    async def close_pool(self) -> None:
        """Close the owned pool."""
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