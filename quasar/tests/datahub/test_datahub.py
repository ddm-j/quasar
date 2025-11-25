"""Tests for DataHub API endpoints and public methods."""
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch, mock_open
from fastapi import HTTPException
from pathlib import Path
import importlib.util
import inspect
from datetime import datetime, timezone, date, timedelta
import hashlib

from quasar.datahub.core import DataHub, load_provider_from_file_path
from quasar.datahub.schemas import ProviderValidateRequest, ProviderValidateResponse
from quasar.providers.core import HistoricalDataProvider, LiveDataProvider, ProviderType, Req, Bar
from quasar.common.secret_store import SecretStore


class TestDataHubAPIEndpoints:
    """Tests for DataHub API endpoints."""
    
    @pytest.mark.asyncio
    async def test_validate_provider_valid_provider_file(
        self, datahub_with_mocks, mock_file_system, monkeypatch
    ):
        """Test that validate_provider succeeds with valid provider file."""
        from httpx import AsyncClient, ASGITransport
        
        file_path = "/app/dynamic_providers/test_provider.py"
        mock_file_system["files"][file_path] = b"class TestProvider: pass"
        
        # Create a mock provider class
        class TestProvider(HistoricalDataProvider):
            name = "TestProvider"
            
            async def get_history(self, sym, start, end, interval):
                yield None
                
            async def get_available_symbols():
                return []
        
        # Set the __module__ to match what the mock module will have
        TestProvider.__module__ = 'test_provider'
        
        # Mock module loading
        def mock_spec_from_file(module_name, file_path, loader=None, **kwargs):
            spec = MagicMock()
            spec.loader = MagicMock()
            return spec
        
        def mock_exec_module(module):
            # This is called after module_from_spec, so we can set attributes here
            pass
        
        def mock_module_from_spec(spec):
            # Create module with __spec__ already set and TestProvider in it
            mock_module = type('MockModule', (), {
                '__name__': 'test_provider',
                '__spec__': spec,
                'TestProvider': TestProvider,
                '__dict__': {'TestProvider': TestProvider}
            })()
            spec.loader.exec_module = mock_exec_module
            return mock_module
        
        # Create a custom getmembers that returns our TestProvider with correct module check
        def mock_getmembers(module, predicate=None):
            if predicate == inspect.isclass:
                # Return classes where __module__ matches module.__name__
                if hasattr(module, 'TestProvider'):
                    test_provider = module.TestProvider
                    if test_provider.__module__ == module.__name__:
                        return [('TestProvider', test_provider)]
            return []
        
        with patch('importlib.util.spec_from_file_location', side_effect=mock_spec_from_file):
            with patch('importlib.util.module_from_spec', side_effect=mock_module_from_spec):
                with patch('inspect.getmembers', side_effect=mock_getmembers):
                    with patch('pathlib.Path.is_file', return_value=True):
                        request = ProviderValidateRequest(file_path=file_path)
                        
                        transport = ASGITransport(app=datahub_with_mocks._api_app)
                        async with AsyncClient(transport=transport, base_url="http://test") as client:
                            response = await client.post(
                                "/internal/provider/validate",
                                json=request.model_dump()
                            )
                            
                            assert response.status_code == 200
                            data = response.json()
                            assert data["status"] == "success"
                            assert data["class_name"] == "TestProvider"
    
    def test_validate_provider_invalid_file_path(self, datahub_client):
        """Test that validate_provider returns 403 for invalid file path (not in allowed path)."""
        request = ProviderValidateRequest(file_path="/invalid/path.py")
        
        response = datahub_client.post(
            "/internal/provider/validate",
            json=request.dict()
        )
        
        # The endpoint checks allowed path first, so it returns 403, not 404
        assert response.status_code == 403
    
    def test_validate_provider_file_not_in_allowed_path(self, datahub_client):
        """Test that validate_provider returns 403 for file outside allowed path."""
        request = ProviderValidateRequest(file_path="/unauthorized/path.py")
        
        with patch('pathlib.Path.is_file', return_value=True):
            response = datahub_client.post(
                "/internal/provider/validate",
                json=request.dict()
            )
            
            assert response.status_code == 403
    
    def test_validate_provider_multiple_classes_error(self, datahub_client):
        """Test that validate_provider returns 500 for multiple classes."""
        file_path = "/app/dynamic_providers/test.py"
        
        class Class1:
            pass
        class Class2:
            pass
        
        with patch('pathlib.Path.is_file', return_value=True):
            with patch('importlib.util.spec_from_file_location'):
                with patch('importlib.util.module_from_spec'):
                    mock_module = MagicMock()
                    mock_module.__name__ = "test"
                    with patch('inspect.getmembers', return_value=[
                        ('Class1', Class1), ('Class2', Class2)
                    ]):
                        with patch.object(inspect, 'isclass', return_value=True):
                            request = ProviderValidateRequest(file_path=file_path)
                            
                            response = datahub_client.post(
                                "/internal/provider/validate",
                                json=request.dict()
                            )
                            
                            assert response.status_code == 500
    
    def test_validate_provider_no_classes_error(self, datahub_client):
        """Test that validate_provider returns 500 for file with no classes."""
        file_path = "/app/dynamic_providers/test.py"
        
        with patch('pathlib.Path.is_file', return_value=True):
            with patch('importlib.util.spec_from_file_location'):
                with patch('importlib.util.module_from_spec'):
                    mock_module = MagicMock()
                    mock_module.__name__ = "test"
                    with patch('inspect.getmembers', return_value=[]):
                        request = ProviderValidateRequest(file_path=file_path)
                        
                        response = datahub_client.post(
                            "/internal/provider/validate",
                            json=request.model_dump()
                        )
                        
                        assert response.status_code == 500
    
    def test_validate_provider_invalid_subclass_error(self, datahub_client):
        """Test that validate_provider returns 500 for invalid subclass."""
        file_path = "/app/dynamic_providers/test.py"
        
        class InvalidClass:
            pass
        
        with patch('pathlib.Path.is_file', return_value=True):
            with patch('importlib.util.spec_from_file_location'):
                with patch('importlib.util.module_from_spec'):
                    mock_module = MagicMock()
                    mock_module.__name__ = "test"
                    with patch('inspect.getmembers', return_value=[('InvalidClass', InvalidClass)]):
                        with patch.object(inspect, 'isclass', return_value=True):
                            request = ProviderValidateRequest(file_path=file_path)
                            
                            response = datahub_client.post(
                                "/internal/provider/validate",
                                json=request.dict()
                            )
                            
                            assert response.status_code == 500
    
    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_provider_exists(
        self, datahub_with_mocks, mock_provider_historical
    ):
        """Test that handle_get_available_symbols returns symbols for loaded provider."""
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        symbols = await hub.handle_get_available_symbols("TestProvider")
        
        assert len(symbols) == 1
        assert symbols[0]["symbol"] == "TEST"
    
    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_provider_not_loaded(
        self, datahub_with_mocks, mock_asyncpg_conn, monkeypatch
    ):
        """Test that handle_get_available_symbols auto-loads provider if not loaded."""
        hub = datahub_with_mocks
        
        # Mock provider loading
        mock_provider = Mock()
        mock_provider.get_available_symbols = AsyncMock(return_value=[{"symbol": "TEST"}])
        
        async def mock_load_provider(name):
            hub._providers[name] = mock_provider
            return True
        
        with patch.object(hub, '_load_provider_cls', side_effect=mock_load_provider):
            symbols = await hub.handle_get_available_symbols("TestProvider")
            
            assert len(symbols) == 1
    
    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_provider_not_found(
        self, datahub_with_mocks
    ):
        """Test that handle_get_available_symbols returns 404 for non-existent provider."""
        hub = datahub_with_mocks
        
        with patch.object(hub, '_load_provider_cls', return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                await hub.handle_get_available_symbols("NonExistentProvider")
            
            assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_not_implemented(
        self, datahub_with_mocks
    ):
        """Test that handle_get_available_symbols returns 501 if method not implemented."""
        hub = datahub_with_mocks
        
        mock_provider = Mock()
        del mock_provider.get_available_symbols  # Remove the method
        hub._providers["TestProvider"] = mock_provider
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_available_symbols("TestProvider")
        
        assert exc_info.value.status_code == 501


class TestDataHubLifecycleMethods:
    """Tests for DataHub lifecycle methods."""
    
    @pytest.mark.asyncio
    async def test_start_initializes_pool_and_scheduler(
        self, datahub_with_mocks, mock_asyncpg_conn
    ):
        """Test that start() initializes pool, scheduler, and starts API server."""
        hub = datahub_with_mocks
        
        # Mock pool initialization
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        with patch.object(hub, 'start_api_server', new_callable=AsyncMock):
            await hub.start()
            
            # Verify pool is initialized
            assert hub._pool is not None
            # Verify scheduler is started
            assert hub._sched.state == 1  # STATE_RUNNING = 1
    
    @pytest.mark.asyncio
    async def test_stop_closes_pool_and_stops_scheduler(
        self, datahub_with_mocks
    ):
        """Test that stop() stops API server, scheduler, and closes pool."""
        hub = datahub_with_mocks
        hub._sched.state = 1  # STATE_RUNNING
        
        # Mock the scheduler's shutdown method and event loop
        hub._sched._eventloop = Mock()
        hub._sched._eventloop.call_soon_threadsafe = Mock()
        hub._sched.shutdown = Mock()
        
        with patch.object(hub, 'stop_api_server', new_callable=AsyncMock):
            with patch.object(hub, 'close_pool', new_callable=AsyncMock):
                await hub.stop()
                
                hub.stop_api_server.assert_called_once()
                hub.close_pool.assert_called_once()


class TestDataHubGetData:
    """Tests for DataHub get_data() method."""
    
    @pytest.mark.asyncio
    async def test_get_data_historical_provider(
        self, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data() works with historical provider."""
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Mock last_updated query
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        # Mock insert_bars
        with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
            await hub.get_data("TestProvider", "1d", ["TEST"])
            
            # Verify insert was called (even if with empty batches)
            hub._insert_bars.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_data_live_provider(
        self, datahub_with_mocks, mock_provider_live
    ):
        """Test that get_data() works with live provider."""
        hub = datahub_with_mocks
        hub._providers["TestLiveProvider"] = mock_provider_live
        
        with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
            await hub.get_data("TestLiveProvider", "1h", ["TEST"])
            
            hub._insert_bars.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_data_provider_not_found(
        self, datahub_with_mocks
    ):
        """Test that get_data() returns None for non-existent provider (error caught by safe_job)."""
        hub = datahub_with_mocks
        
        # get_data is decorated with @safe_job which catches exceptions
        result = await hub.get_data("NonExistentProvider", "1d", ["TEST"])
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_data_batch_insertion(
        self, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data() batches insertion at 500 bars."""
        hub = datahub_with_mocks
        
        # Create a provider that yields many bars
        async def mock_get_data(reqs):
            for i in range(750):  # More than one batch
                yield Bar(
                    ts=datetime.now(timezone.utc),
                    sym="TEST",
                    o=100.0,
                    h=110.0,
                    l=95.0,
                    c=105.0,
                    v=1000
                )
        
        mock_provider = Mock()
        mock_provider.provider_type = ProviderType.HISTORICAL
        mock_provider.get_data = mock_get_data
        hub._providers["TestProvider"] = mock_provider
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        insert_calls = []
        async def track_insert(*args, **kwargs):
            insert_calls.append((args, kwargs))
        
        with patch.object(hub, '_insert_bars', side_effect=track_insert):
            await hub.get_data("TestProvider", "1d", ["TEST"])
            
            # Should have been called twice: once for 500 bars, once for 250
            assert len(insert_calls) == 2

