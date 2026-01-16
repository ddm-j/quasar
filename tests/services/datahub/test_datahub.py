"""Tests for Data Explorer API handlers."""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from datetime import datetime, timezone, date, timedelta

from fastapi import HTTPException


# =============================================================================
# Tests for Data Explorer API Endpoints
# =============================================================================

class TestDataExplorerAPIEndpoints:
    """Tests for Data Explorer API endpoints."""
    
    @pytest.mark.asyncio
    async def test_handle_search_symbols_by_common_symbol(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_search_symbols finds symbols by common symbol."""
        hub = datahub_with_mocks
        
        # Mock search query results
        search_records = [
            {
                'common_symbol': 'BTCUSD',
                'provider': 'Kraken',
                'provider_symbol': 'XBT/USD',
                'is_active': True,
                'asset_name': 'Bitcoin / US Dollar',
                'base_currency': 'BTC',
                'quote_currency': 'USD',
                'exchange': 'Kraken',
                'asset_class': 'crypto'
            }
        ]
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            search_records,  # Search query
            [],  # Historical data check
            [{'interval': '1h', 'max_ts': datetime.now(timezone.utc)}]  # Live data check
        ])
        
        response = await hub.handle_search_symbols(q="BTCUSD")
        
        assert response.total == 1
        assert len(response.items) == 1
        assert response.items[0].common_symbol == "BTCUSD"
        assert response.items[0].provider == "Kraken"
        assert response.items[0].has_live is True
        assert response.items[0].has_historical is False
    
    @pytest.mark.asyncio
    async def test_handle_search_symbols_by_provider_symbol(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_search_symbols finds symbols by provider symbol."""
        hub = datahub_with_mocks
        
        search_records = [
            {
                'common_symbol': 'BTCUSD',
                'provider': 'Kraken',
                'provider_symbol': 'XBT/USD',
                'is_active': True,
                'asset_name': None,
                'base_currency': None,
                'quote_currency': None,
                'exchange': None,
                'asset_class': None
            }
        ]
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            search_records,
            [{'interval': '1d', 'max_ts': datetime.now(timezone.utc)}],  # Historical
            []  # Live
        ])
        
        response = await hub.handle_search_symbols(q="XBT/USD")
        
        assert response.total == 1
        assert response.items[0].provider_symbol == "XBT/USD"
        assert response.items[0].has_historical is True
        assert response.items[0].has_live is False
    
    @pytest.mark.asyncio
    async def test_handle_search_symbols_filter_by_data_type(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_search_symbols filters by data_type."""
        hub = datahub_with_mocks
        
        search_records = [
            {
                'common_symbol': 'BTCUSD',
                'provider': 'Kraken',
                'provider_symbol': 'XBT/USD',
                'is_active': True,
                'asset_name': None,
                'base_currency': None,
                'quote_currency': None,
                'exchange': None,
                'asset_class': None
            }
        ]
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            search_records,
            [],  # Historical (no data)
            []  # Live (no data)
        ])
        
        # Should return empty because no historical data
        response = await hub.handle_search_symbols(q="BTCUSD", data_type="historical")
        
        assert response.total == 0
        assert len(response.items) == 0
    
    @pytest.mark.asyncio
    async def test_handle_search_symbols_filter_by_provider(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_search_symbols filters by provider."""
        hub = datahub_with_mocks
        
        search_records = [
            {
                'common_symbol': 'BTCUSD',
                'provider': 'Kraken',
                'provider_symbol': 'XBT/USD',
                'is_active': True,
                'asset_name': None,
                'base_currency': None,
                'quote_currency': None,
                'exchange': None,
                'asset_class': None
            }
        ]
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            search_records,
            [{'interval': '1h', 'max_ts': datetime.now(timezone.utc)}],
            []
        ])
        
        response = await hub.handle_search_symbols(q="BTCUSD", provider="Kraken")
        
        assert response.total == 1
        assert response.items[0].provider == "Kraken"
    
    @pytest.mark.asyncio
    async def test_handle_search_symbols_empty_results(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_search_symbols returns empty results when no matches."""
        hub = datahub_with_mocks
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        response = await hub.handle_search_symbols(q="NONEXISTENT")
        
        assert response.total == 0
        assert len(response.items) == 0
        assert response.limit == 50
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_historical(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data retrieves historical data."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        bars = [
            {
                'ts': now - timedelta(hours=2),
                'o': 100.0,
                'h': 110.0,
                'l': 95.0,
                'c': 105.0,
                'v': 1000.0
            },
            {
                'ts': now - timedelta(hours=1),
                'o': 105.0,
                'h': 115.0,
                'l': 100.0,
                'c': 110.0,
                'v': 1200.0
            }
        ]
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=bars)
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            {'count': 2},  # Count query
            {'common_symbol': 'BTCUSD'}  # Mapping query
        ])
        
        response = await hub.handle_get_ohlc_data(
            provider="Kraken",
            symbol="XBT/USD",
            data_type="historical",
            interval="1h"
        )
        
        assert response.provider == "Kraken"
        assert response.symbol == "XBT/USD"
        assert response.data_type == "historical"
        assert response.interval == "1h"
        assert response.count == 2
        assert len(response.bars) == 2
        assert response.bars[0].open == 100.0
        assert response.has_more is False
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_live(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data retrieves live data."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        bars = [
            {
                'ts': now - timedelta(minutes=5),
                'o': 100.0,
                'h': 110.0,
                'l': 95.0,
                'c': 105.0,
                'v': 1000.0
            }
        ]
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=bars)
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            {'count': 1},
            {'common_symbol': 'BTCUSD'}
        ])
        
        response = await hub.handle_get_ohlc_data(
            provider="Kraken",
            symbol="XBT/USD",
            data_type="live",
            interval="1min"
        )
        
        assert response.data_type == "live"
        assert response.count == 1
        assert response.has_more is False
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_with_time_range(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data filters by time range."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        bars = [
            {
                'ts': now - timedelta(hours=1),
                'o': 100.0,
                'h': 110.0,
                'l': 95.0,
                'c': 105.0,
                'v': 1000.0
            }
        ]
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=bars)
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            {'count': 1},
            {'common_symbol': 'BTCUSD'}
        ])
        
        from_time = (now - timedelta(hours=2)).isoformat()
        to_time = now.isoformat()
        
        response = await hub.handle_get_ohlc_data(
            provider="Kraken",
            symbol="XBT/USD",
            data_type="historical",
            interval="1h",
            from_time=from_time,
            to_time=to_time
        )
        
        assert response.count == 1
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_with_unix_timestamp(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data accepts Unix timestamps."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        bars = [
            {
                'ts': now - timedelta(hours=1),
                'o': 100.0,
                'h': 110.0,
                'l': 95.0,
                'c': 105.0,
                'v': 1000.0
            }
        ]
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=bars)
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            {'count': 1},
            {'common_symbol': 'BTCUSD'}
        ])
        
        from_time = int((now - timedelta(hours=2)).timestamp())
        to_time = int(now.timestamp())
        
        response = await hub.handle_get_ohlc_data(
            provider="Kraken",
            symbol="XBT/USD",
            data_type="historical",
            interval="1h",
            from_time=str(from_time),
            to_time=str(to_time)
        )
        
        assert response.count == 1
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_has_more(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data correctly indicates has_more."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        bars = [{'ts': now, 'o': 100.0, 'h': 110.0, 'l': 95.0, 'c': 105.0, 'v': 1000.0}] * 500
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=bars)
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            {'count': 1000},  # More than limit
            {'common_symbol': 'BTCUSD'}
        ])
        
        response = await hub.handle_get_ohlc_data(
            provider="Kraken",
            symbol="XBT/USD",
            data_type="historical",
            interval="1h",
            limit=500
        )
        
        assert response.count == 500
        assert response.has_more is True
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_order_asc(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data respects order parameter."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        bars = [
            {'ts': now - timedelta(hours=2), 'o': 100.0, 'h': 110.0, 'l': 95.0, 'c': 105.0, 'v': 1000.0},
            {'ts': now - timedelta(hours=1), 'o': 105.0, 'h': 115.0, 'l': 100.0, 'c': 110.0, 'v': 1200.0}
        ]
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=bars)
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            {'count': 2},
            {'common_symbol': 'BTCUSD'}
        ])
        
        response = await hub.handle_get_ohlc_data(
            provider="Kraken",
            symbol="XBT/USD",
            data_type="historical",
            interval="1h",
            order="asc"
        )
        
        assert response.count == 2
        # First bar should be older
        assert response.bars[0].time < response.bars[1].time
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_invalid_data_type(self, datahub_with_mocks):
        """Test that handle_get_ohlc_data returns 400 for invalid data_type."""
        hub = datahub_with_mocks
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_ohlc_data(
                provider="Kraken",
                symbol="XBT/USD",
                data_type="invalid",
                interval="1h"
            )
        
        assert exc_info.value.status_code == 400
        assert "data_type must be" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_invalid_order(self, datahub_with_mocks):
        """Test that handle_get_ohlc_data returns 400 for invalid order."""
        hub = datahub_with_mocks
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_ohlc_data(
                provider="Kraken",
                symbol="XBT/USD",
                data_type="historical",
                interval="1h",
                order="invalid"
            )
        
        assert exc_info.value.status_code == 400
        assert "order must be" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_handle_get_ohlc_data_not_found(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_ohlc_data returns 404 when no data found."""
        hub = datahub_with_mocks
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_ohlc_data(
                provider="Kraken",
                symbol="XBT/USD",
                data_type="historical",
                interval="1h"
            )
        
        assert exc_info.value.status_code == 404
        assert "No data found" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_handle_get_symbol_metadata_success(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_symbol_metadata returns complete metadata."""
        hub = datahub_with_mocks
        
        now = datetime.now(timezone.utc)
        
        mapping_record = {
            'common_symbol': 'BTCUSD',
            'provider': 'Kraken',
            'provider_symbol': 'XBT/USD',
            'asset_name': 'Bitcoin / US Dollar',
            'base_currency': 'BTC',
            'quote_currency': 'USD',
            'exchange': 'Kraken',
            'asset_class': 'crypto'
        }
        
        hist_record = {
            'has_data': True,
            'intervals': ['1h', '1d'],
            'earliest': now - timedelta(days=30),
            'latest': now
        }
        
        live_record = {
            'has_data': True,
            'intervals': ['1min', '5min'],
            'earliest': now - timedelta(hours=1),
            'latest': now
        }
        
        other_providers_records = [
            {
                'provider': 'Binance',
                'provider_symbol': 'BTCUSDT',
                'has_historical': True,
                'has_live': True
            }
        ]
        
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            mapping_record,
            hist_record,
            live_record
        ])
        mock_asyncpg_conn.fetch = AsyncMock(return_value=other_providers_records)
        
        response = await hub.handle_get_symbol_metadata("Kraken", "XBT/USD")
        
        assert response.common_symbol == "BTCUSD"
        assert response.provider == "Kraken"
        assert response.provider_symbol == "XBT/USD"
        assert "historical" in response.data_types
        assert "live" in response.data_types
        assert response.data_types["historical"].available is True
        assert response.data_types["live"].available is True
        assert len(response.data_types["historical"].intervals) == 2
        assert len(response.other_providers) == 1
        assert response.other_providers[0].provider == "Binance"
        assert response.asset_info is not None
        assert response.asset_info.base_currency == "BTC"
    
    @pytest.mark.asyncio
    async def test_handle_get_symbol_metadata_no_data(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_symbol_metadata handles symbols with no data."""
        hub = datahub_with_mocks
        
        mapping_record = {
            'common_symbol': 'BTCUSD',
            'provider': 'Kraken',
            'provider_symbol': 'XBT/USD',
            'asset_name': None,
            'base_currency': None,
            'quote_currency': None,
            'exchange': None,
            'asset_class': None
        }
        
        hist_record = {
            'has_data': False,
            'intervals': [],
            'earliest': None,
            'latest': None
        }
        
        live_record = {
            'has_data': False,
            'intervals': [],
            'earliest': None,
            'latest': None
        }
        
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            mapping_record,
            hist_record,
            live_record
        ])
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        response = await hub.handle_get_symbol_metadata("Kraken", "XBT/USD")
        
        assert response.data_types["historical"].available is False
        assert response.data_types["live"].available is False
        assert len(response.data_types["historical"].intervals) == 0
    
    @pytest.mark.asyncio
    async def test_handle_get_symbol_metadata_not_found(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_symbol_metadata returns 404 when symbol not found."""
        hub = datahub_with_mocks
        
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_symbol_metadata("Kraken", "NONEXISTENT")
        
        assert exc_info.value.status_code == 404
        assert "Symbol not found" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_handle_get_symbol_metadata_filter_by_data_type(self, datahub_with_mocks, mock_asyncpg_conn):
        """Test that handle_get_symbol_metadata filters by data_type."""
        hub = datahub_with_mocks
        
        mapping_record = {
            'common_symbol': 'BTCUSD',
            'provider': 'Kraken',
            'provider_symbol': 'XBT/USD',
            'asset_name': None,
            'base_currency': None,
            'quote_currency': None,
            'exchange': None,
            'asset_class': None
        }
        
        hist_record = {
            'has_data': True,
            'intervals': ['1h'],
            'earliest': datetime.now(timezone.utc) - timedelta(days=30),
            'latest': datetime.now(timezone.utc)
        }
        
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            mapping_record,
            hist_record
        ])
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        response = await hub.handle_get_symbol_metadata("Kraken", "XBT/USD", data_type="historical")

        assert "historical" in response.data_types
        assert "live" not in response.data_types

    # Usage tracking and race condition tests
    @pytest.mark.asyncio
    async def test_provider_in_use_property_false_when_not_active(self, datahub_with_mocks, mock_asyncpg_conn, valid_provider_file, mock_system_context):
        """Test that in_use property returns False when no operations are active."""
        hub = datahub_with_mocks
        hub.system_context = mock_system_context

        # Mock database to return provider data
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': valid_provider_file['file_path'],
            'file_hash': valid_provider_file['file_hash'],
            'nonce': b'test_nonce',
            'ciphertext': b'test_ciphertext'
        })

        # Load a provider
        await hub.load_provider_cls("TEST_LIVE_PROVIDER")
        prov = hub._providers["TEST_LIVE_PROVIDER"]

        # Should not be in use initially
        assert prov.in_use is False

    @pytest.mark.asyncio
    async def test_provider_in_use_property_true_during_operation(self, datahub_with_mocks, mock_asyncpg_conn, valid_provider_file, mock_system_context):
        """Test that in_use property returns True during active operations."""
        hub = datahub_with_mocks
        hub.system_context = mock_system_context

        # Mock database to return provider data
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': valid_provider_file['file_path'],
            'file_hash': valid_provider_file['file_hash'],
            'nonce': b'test_nonce',
            'ciphertext': b'test_ciphertext'
        })

        # Load a provider
        await hub.load_provider_cls("TEST_LIVE_PROVIDER")
        prov = hub._providers["TEST_LIVE_PROVIDER"]

        # Start an operation
        async def check_in_use_during_operation():
            # Should be in use during operation
            assert prov.in_use is True
            return []

        # Mock the fetch method to check in_use status
        prov.fetch_available_symbols = check_in_use_during_operation

        # Call the public method which should set in_use to True
        symbols = await prov.get_available_symbols()

        # Should not be in use after operation completes
        assert prov.in_use is False
        assert symbols == []

    @pytest.mark.asyncio
    async def test_refresh_subscriptions_skips_unload_when_provider_in_use(self, datahub_with_mocks, mock_asyncpg_conn, valid_provider_file, mock_system_context):
        """Test that refresh_subscriptions skips unloading providers that are in use."""
        hub = datahub_with_mocks
        hub.system_context = mock_system_context

        # Mock database to return provider data for loading
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': valid_provider_file['file_path'],
            'file_hash': valid_provider_file['file_hash'],
            'nonce': b'test_nonce',
            'ciphertext': b'test_ciphertext'
        })

        # Load a provider
        await hub.load_provider_cls("TEST_LIVE_PROVIDER")
        prov = hub._providers["TEST_LIVE_PROVIDER"]

        # Mock the database to return no subscriptions (provider should be removed)
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        # Start an operation to make provider "in use"
        async def slow_operation():
            await asyncio.sleep(0.1)  # Small delay to ensure test timing
            return []

        # Store original method and replace with slow version
        original_method = prov.fetch_available_symbols
        prov.fetch_available_symbols = slow_operation

        # Start the operation asynchronously
        task = asyncio.create_task(prov.get_available_symbols())

        # Give the task a moment to start and acquire the semaphore
        await asyncio.sleep(0.01)

        # Verify provider is now in use
        assert prov.in_use is True

        # Run refresh_subscriptions - should skip unloading because provider is in use
        await hub.refresh_subscriptions()

        # Provider should still be loaded
        assert "TEST_LIVE_PROVIDER" in hub._providers

        # Wait for operation to complete
        await task

        # After completion, provider should not be in use
        assert prov.in_use is False

        # Now run refresh again - should unload the provider
        await hub.refresh_subscriptions()
        assert "TEST_LIVE_PROVIDER" not in hub._providers