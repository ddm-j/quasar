"""Tests for DatabaseHandler."""
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
import asyncpg

from quasar.common.database_handler import DatabaseHandler


class ConcreteDatabaseHandler(DatabaseHandler):
    """Concrete implementation of DatabaseHandler for testing."""
    name = "TestHandler"


class TestDatabaseHandler:
    """Tests for DatabaseHandler."""
    
    @pytest.mark.asyncio
    async def test_init_pool_creates_pool_with_dsn(self, monkeypatch: pytest.MonkeyPatch):
        """Test that init_pool creates asyncpg pool with DSN."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_pool._closed = False
        
        create_pool_mock = AsyncMock(return_value=mock_pool)
        monkeypatch.setattr("asyncpg.create_pool", create_pool_mock)
        
        handler = ConcreteDatabaseHandler(dsn="postgresql://user:pass@localhost/db")
        await handler.init_pool()
        
        assert handler._pool == mock_pool
        create_pool_mock.assert_called_once_with("postgresql://user:pass@localhost/db")
    
    @pytest.mark.asyncio
    async def test_init_pool_noop_with_existing_pool(self):
        """Test that init_pool does nothing if pool already exists."""
        existing_pool = AsyncMock(spec=asyncpg.Pool)
        existing_pool._closed = False
        
        handler = ConcreteDatabaseHandler(pool=existing_pool)
        await handler.init_pool()
        
        assert handler._pool == existing_pool
    
    @pytest.mark.asyncio
    async def test_close_pool_closes_owned_pool(self):
        """Test that close_pool closes the pool if it exists and is open."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_pool._closed = False
        mock_pool.close = AsyncMock()
        
        handler = ConcreteDatabaseHandler(pool=mock_pool)
        await handler.close_pool()
        
        mock_pool.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_close_pool_noop_if_already_closed(self):
        """Test that close_pool does nothing if pool is already closed."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_pool._closed = True
        mock_pool.close = AsyncMock()
        
        handler = ConcreteDatabaseHandler(pool=mock_pool)
        await handler.close_pool()
        
        mock_pool.close.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_close_pool_noop_if_no_pool(self):
        """Test that close_pool does nothing if no pool exists."""
        handler = ConcreteDatabaseHandler(dsn="postgresql://user:pass@localhost/db")
        # Don't initialize pool
        await handler.close_pool()
        
        # Should not raise error
        assert handler._pool is None
    
    def test_pool_property_returns_pool_when_initialized(self):
        """Test that pool property returns pool when initialized."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        handler = ConcreteDatabaseHandler(pool=mock_pool)
        
        assert handler.pool == mock_pool
    
    def test_pool_property_raises_runtimeerror_when_not_initialized(self):
        """Test that pool property raises RuntimeError when not initialized."""
        handler = ConcreteDatabaseHandler(dsn="postgresql://user:pass@localhost/db")
        
        with pytest.raises(RuntimeError, match="pool not started yet"):
            _ = handler.pool
    
    def test_init_raises_valueerror_without_dsn_or_pool(self):
        """Test that __init__ raises ValueError when neither dsn nor pool provided."""
        with pytest.raises(ValueError, match="Provide either dsn or pool"):
            ConcreteDatabaseHandler()

