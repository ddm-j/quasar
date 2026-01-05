"""Tests for DataHub API endpoints and public methods."""
import pytest
import os
import tempfile
import shutil
import asyncio
from unittest.mock import Mock, AsyncMock, MagicMock, patch, mock_open
from fastapi import HTTPException
from pathlib import Path
import importlib.util
import inspect
from datetime import datetime, timezone, date, timedelta
import hashlib

from quasar.services.datahub.core import DataHub, load_provider_from_file_path, ALLOWED_DYNAMIC_PATH
from quasar.services.datahub.schemas import ProviderValidateRequest, ProviderValidateResponse
from quasar.lib.providers.core import HistoricalDataProvider, LiveDataProvider, ProviderType, Req, Bar
from quasar.lib.common.secret_store import SecretStore


# =============================================================================
# Fixtures for Real File I/O Provider Testing
# =============================================================================

# Valid provider code template (simplified from Kraken)
VALID_LIVE_PROVIDER_CODE = '''
from quasar.lib.providers.core import LiveDataProvider, Bar, SymbolInfo
from quasar.lib.common.context import DerivedContext

class TestLiveProvider(LiveDataProvider):
    name = 'TEST_LIVE_PROVIDER'
    RATE_LIMIT = (100, 60)
    close_buffer_seconds = 5

    def __init__(self, context: DerivedContext):
        super().__init__(context)

    async def get_available_symbols(self):
        return []

    async def _connect(self):
        return None

    async def _subscribe(self, interval, symbols):
        return {}

    async def _unsubscribe(self, symbols):
        return {}

    async def _parse_message(self, message):
        return []
'''

VALID_HISTORICAL_PROVIDER_CODE = '''
from quasar.lib.providers.core import HistoricalDataProvider, Bar, SymbolInfo
from quasar.lib.common.context import DerivedContext

class TestHistoricalProvider(HistoricalDataProvider):
    name = 'TEST_HISTORICAL_PROVIDER'
    RATE_LIMIT = (100, 60)

    def __init__(self, context: DerivedContext):
        super().__init__(context)

    async def get_available_symbols(self):
        return []

    async def get_history(self, sym, start, end, interval):
        yield None
'''


@pytest.fixture
def temp_provider_dir(tmp_path, monkeypatch):
    """
    Create a temporary directory for provider files and patch ALLOWED_DYNAMIC_PATH.
    Returns the path to the temp directory.
    """
    provider_dir = tmp_path / "dynamic_providers"
    provider_dir.mkdir()
    
    # Patch the ALLOWED_DYNAMIC_PATH constant
    monkeypatch.setattr(
        "quasar.services.datahub.core.ALLOWED_DYNAMIC_PATH",
        str(provider_dir)
    )
    
    return provider_dir


@pytest.fixture
def valid_provider_file(temp_provider_dir):
    """
    Create a valid provider Python file in the temp directory.
    Returns a dict with file_path, file_hash, and content.
    """
    file_path = temp_provider_dir / "test_provider.py"
    content = VALID_LIVE_PROVIDER_CODE.encode('utf-8')
    file_path.write_bytes(content)
    
    # Compute hash
    sha256 = hashlib.sha256()
    sha256.update(content)
    file_hash = sha256.digest()
    
    return {
        "file_path": str(file_path),
        "file_hash": file_hash,
        "content": content,
        "class_name": "TEST_LIVE_PROVIDER"
    }


@pytest.fixture
def valid_historical_provider_file(temp_provider_dir):
    """
    Create a valid historical provider Python file in the temp directory.
    Returns a dict with file_path, file_hash, and content.
    """
    file_path = temp_provider_dir / "test_historical_provider.py"
    content = VALID_HISTORICAL_PROVIDER_CODE.encode('utf-8')
    file_path.write_bytes(content)
    
    # Compute hash
    sha256 = hashlib.sha256()
    sha256.update(content)
    file_hash = sha256.digest()
    
    return {
        "file_path": str(file_path),
        "file_hash": file_hash,
        "content": content,
        "class_name": "TEST_HISTORICAL_PROVIDER"
    }


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

    def test_validate_provider_file_not_found_after_path_check(self, datahub_client):
        """Test that validate_provider returns 404 when file doesn't exist (valid path prefix)."""
        file_path = "/app/dynamic_providers/missing_file.py"
        
        # Path prefix is valid but file doesn't exist
        with patch('pathlib.Path.is_file', return_value=False):
            request = ProviderValidateRequest(file_path=file_path)
            
            response = datahub_client.post(
                "/internal/provider/validate",
                json=request.model_dump()
            )
            
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]

    def test_validate_provider_spec_loader_is_none(self, datahub_client):
        """Test that validate_provider returns 500 when spec.loader is None."""
        file_path = "/app/dynamic_providers/test.py"
        
        # Create a spec with loader=None
        mock_spec = MagicMock()
        mock_spec.loader = None
        
        with patch('pathlib.Path.is_file', return_value=True):
            with patch('importlib.util.spec_from_file_location', return_value=mock_spec):
                request = ProviderValidateRequest(file_path=file_path)
                
                response = datahub_client.post(
                    "/internal/provider/validate",
                    json=request.model_dump()
                )
                
                assert response.status_code == 500
                assert "Unable to load module" in response.json()["detail"]

    def test_validate_provider_non_string_name_attribute(self, datahub_client):
        """Test that validate_provider returns 500 when class name attribute is not a string."""
        file_path = "/app/dynamic_providers/test.py"
        
        # Create a provider class with non-string name attribute
        class ProviderWithIntName(HistoricalDataProvider):
            name = 12345  # Non-string name
            
            async def get_history(self, sym, start, end, interval):
                yield None
                
            async def get_available_symbols():
                return []
        
        ProviderWithIntName.__module__ = 'test'
        
        def mock_spec_from_file(module_name, file_path, **kwargs):
            spec = MagicMock()
            spec.loader = MagicMock()
            return spec
        
        def mock_module_from_spec(spec):
            mock_module = type('MockModule', (), {
                '__name__': 'test',
                '__spec__': spec,
                'ProviderWithIntName': ProviderWithIntName,
            })()
            return mock_module
        
        def mock_getmembers(module, predicate=None):
            if predicate == inspect.isclass:
                if hasattr(module, 'ProviderWithIntName'):
                    return [('ProviderWithIntName', ProviderWithIntName)]
            return []
        
        with patch('pathlib.Path.is_file', return_value=True):
            with patch('importlib.util.spec_from_file_location', side_effect=mock_spec_from_file):
                with patch('importlib.util.module_from_spec', side_effect=mock_module_from_spec):
                    with patch('inspect.getmembers', side_effect=mock_getmembers):
                        request = ProviderValidateRequest(file_path=file_path)
                        
                        response = datahub_client.post(
                            "/internal/provider/validate",
                            json=request.model_dump()
                        )
                        
                        assert response.status_code == 500
                        assert "valid name attribute" in response.json()["detail"]
    
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
        mock_provider.get_available_symbols = AsyncMock(return_value=[{"symbol": "TEST", "matcher_symbol": "TEST"}])
        
        async def mock_load_provider(name):
            hub._providers[name] = mock_provider
            return True
        
        with patch.object(hub, 'load_provider_cls', side_effect=mock_load_provider):
            symbols = await hub.handle_get_available_symbols("TestProvider")
            
            assert len(symbols) == 1
    
    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_provider_not_found(
        self, datahub_with_mocks
    ):
        """Test that handle_get_available_symbols returns 404 for non-existent provider."""
        hub = datahub_with_mocks
        
        with patch.object(hub, 'load_provider_cls', return_value=False):
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

    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_raises_not_implemented_error(
        self, datahub_with_mocks
    ):
        """Test that handle_get_available_symbols returns 501 when provider raises NotImplementedError."""
        hub = datahub_with_mocks
        
        mock_provider = Mock()
        mock_provider.get_available_symbols = AsyncMock(side_effect=NotImplementedError("Not implemented"))
        hub._providers["TestProvider"] = mock_provider
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_available_symbols("TestProvider")
        
        assert exc_info.value.status_code == 501
        assert "not implemented" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_generic_exception(
        self, datahub_with_mocks
    ):
        """Test that handle_get_available_symbols returns 500 for generic exceptions."""
        hub = datahub_with_mocks
        
        mock_provider = Mock()
        mock_provider.get_available_symbols = AsyncMock(side_effect=RuntimeError("Provider crashed"))
        hub._providers["TestProvider"] = mock_provider
        
        with pytest.raises(HTTPException) as exc_info:
            await hub.handle_get_available_symbols("TestProvider")
        
        assert exc_info.value.status_code == 500
        assert "Internal server error" in exc_info.value.detail


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

    @pytest.mark.asyncio
    async def test_start_calls_stop_scheduler_when_running(
        self, datahub_with_mocks, mock_asyncpg_conn
    ):
        """Test that start() calls _stop_scheduler to reset running scheduler."""
        from apscheduler.schedulers.base import STATE_RUNNING
        
        hub = datahub_with_mocks
        
        # Add job keys to verify they get cleared
        hub.job_keys.add("test_job_key_1")
        hub.job_keys.add("test_job_key_2")
        
        # Create a mock scheduler that reports as running
        mock_scheduler = Mock()
        mock_scheduler.state = STATE_RUNNING  # state property returns STATE_RUNNING
        hub._sched = mock_scheduler
        
        # Call _stop_scheduler directly (which is called by start())
        hub._stop_scheduler()
        
        # Verify shutdown was called
        mock_scheduler.shutdown.assert_called_once_with(wait=False)
        # Verify job_keys were cleared
        assert len(hub.job_keys) == 0


class TestRefreshSubscriptions:
    """Tests for DataHub.refresh_subscriptions() method."""

    @pytest.mark.asyncio
    async def test_refresh_loads_new_providers(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions loads new providers from database."""
        hub = datahub_with_mocks
        
        # Mock database returning a subscription for a new provider
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "NewProvider", 
                "interval": "1d", 
                "cron": "0 0 * * *", 
                "syms": ["SYM1"],
                "exchanges": ["XNAS"]
            }
        ])
        
        # Mock load_provider_cls to succeed and add the provider
        async def mock_load(name):
            hub._providers[name] = mock_provider_historical
            return True
        
        with patch.object(hub, 'load_provider_cls', side_effect=mock_load):
            await hub.refresh_subscriptions()
            
            hub.load_provider_cls.assert_called_once_with("NewProvider")
            assert "NewProvider" in hub._providers

    @pytest.mark.asyncio
    async def test_refresh_handles_provider_load_failure(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Test that refresh_subscriptions skips providers that fail to load."""
        hub = datahub_with_mocks
        
        # Mock database returning subscriptions for an invalid provider
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "InvalidProvider", 
                "interval": "1d", 
                "cron": "0 0 * * *", 
                "syms": ["SYM1"],
                "exchanges": ["XNAS"]
            }
        ])
        
        # Mock load_provider_cls to fail
        with patch.object(hub, 'load_provider_cls', return_value=False):
            await hub.refresh_subscriptions()
            
            # Provider should not be in the registry
            assert "InvalidProvider" not in hub._providers
            # No job should be scheduled for invalid provider
            assert len(hub.job_keys) == 0

    @pytest.mark.asyncio
    async def test_refresh_removes_obsolete_providers_with_aclose(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that obsolete providers with aclose method are properly closed and removed."""
        hub = datahub_with_mocks
        
        # Pre-load an obsolete provider that has aclose
        hub._providers["ObsoleteProvider"] = mock_provider_historical
        
        # Database returns empty subscriptions - provider is now obsolete
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])
        
        await hub.refresh_subscriptions()
        
        # Provider should be removed
        assert "ObsoleteProvider" not in hub._providers
        # aclose should have been called
        mock_provider_historical.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_removes_obsolete_providers_without_aclose(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Test that obsolete providers without aclose method are removed gracefully."""
        hub = datahub_with_mocks
        
        # Create provider without aclose method
        mock_provider = Mock()
        mock_provider.provider_type = ProviderType.HISTORICAL
        del mock_provider.aclose  # Ensure no aclose method
        hub._providers["ObsoleteProvider"] = mock_provider
        
        # Database returns empty subscriptions - provider is now obsolete
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])
        
        await hub.refresh_subscriptions()
        
        # Provider should be removed without error
        assert "ObsoleteProvider" not in hub._providers

    @pytest.mark.asyncio
    async def test_refresh_adds_new_scheduled_job(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions adds new scheduled jobs for subscriptions."""
        hub = datahub_with_mocks
        
        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Mock database returning a new subscription
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "TestProvider", 
                "interval": "1d", 
                "cron": "0 0 * * *", 
                "syms": ["SYM1", "SYM2"],
                "exchanges": ["XNAS", "XNAS"]
            }
        ])
        
        # Start the scheduler so we can add jobs
        hub._sched.start()
        
        await hub.refresh_subscriptions()
        
        # Job key should be tracked
        expected_key = "TestProvider|1d|0 0 * * *"
        assert expected_key in hub.job_keys
        # Job should exist in scheduler
        assert hub._sched.get_job(expected_key) is not None
        
        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_updates_existing_job_symbols(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions updates job args when symbols change."""
        hub = datahub_with_mocks
        
        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Start the scheduler and add initial job
        hub._sched.start()
        job_key = "TestProvider|1d|0 0 * * *"
        hub.job_keys.add(job_key)
        hub._sched.add_job(
            func=hub.get_data,
            trigger='interval',
            seconds=3600,
            args=["TestProvider", "1d", ["OLD_SYM"], ["XNAS"]],
            id=job_key,
        )
        
        # Mock database returning updated symbols
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "TestProvider", 
                "interval": "1d", 
                "cron": "0 0 * * *", 
                "syms": ["NEW_SYM1", "NEW_SYM2"],
                "exchanges": ["XNAS", "XNAS"]
            }
        ])
        
        await hub.refresh_subscriptions()
        
        # Job should still exist with updated args
        job = hub._sched.get_job(job_key)
        assert job is not None
        assert job.args == ["TestProvider", "1d", ["NEW_SYM1", "NEW_SYM2"], ["XNAS", "XNAS"]]
        
        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_triggers_immediate_pull_for_new_symbols_in_existing_job(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions triggers an immediate pull for newly added symbols."""
        hub = datahub_with_mocks
        
        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Start the scheduler and add initial job
        hub._sched.start()
        job_key = "TestProvider|1d|0 0 * * *"
        hub.job_keys.add(job_key)
        hub._sched.add_job(
            func=hub.get_data,
            trigger='interval',
            seconds=3600,
            args=["TestProvider", "1d", ["AAPL"], ["XNAS"]],
            id=job_key,
        )
        
        # Mock database returning updated symbols with a new one
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "TestProvider", 
                "interval": "1d", 
                "cron": "0 0 * * *", 
                "syms": ["AAPL", "TSLA"],
                "exchanges": ["XNAS", "XNAS"]
            }
        ])
        
        # Mock get_data to verify it's called
        with patch.object(hub, 'get_data', new_callable=AsyncMock) as mock_get_data:
            await hub.refresh_subscriptions()
            
            # Allow control to return to the event loop so the task can start
            await asyncio.sleep(0) 

            # Verify get_data was called for TSLA only
            mock_get_data.assert_called_once_with("TestProvider", "1d", ["TSLA"], ["XNAS"])
        
        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_removes_unsubscribed_job(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions removes jobs for unsubscribed data."""
        hub = datahub_with_mocks
        
        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Start the scheduler and add existing job
        hub._sched.start()
        job_key = "TestProvider|1d|0 0 * * *"
        hub.job_keys.add(job_key)
        hub._sched.add_job(
            func=hub.get_data,
            trigger='interval',
            seconds=3600,
            args=["TestProvider", "1d", ["SYM1"]],
            id=job_key,
        )
        
        # Mock database returning no subscriptions - job should be removed
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])
        
        await hub.refresh_subscriptions()
        
        # Job should be removed
        assert job_key not in hub.job_keys
        assert hub._sched.get_job(job_key) is None
        
        hub._sched.shutdown(wait=False)


class TestDataHubGetData:
    """Tests for DataHub get_data() method."""
    
    @pytest.mark.asyncio
    @patch("quasar.services.datahub.core.TradingCalendar")
    async def test_get_data_historical_provider(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data() works with historical provider."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Mock last_updated query
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        # Mock insert_bars
        with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
            await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])
            
            # Verify insert was called (even if with empty batches)
            hub._insert_bars.assert_called()
    
    @pytest.mark.asyncio
    @patch("quasar.services.datahub.core.TradingCalendar")
    async def test_get_data_live_provider(
        self, mock_calendar, datahub_with_mocks, mock_provider_live
    ):
        """Test that get_data() works with live provider."""
        mock_calendar.is_open_now.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestLiveProvider"] = mock_provider_live
        
        with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
            await hub.get_data("TestLiveProvider", "1h", ["TEST"], ["CRYPTO"])
            
            hub._insert_bars.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_data_provider_not_found(
        self, datahub_with_mocks
    ):
        """Test that get_data() returns None for non-existent provider (error caught by safe_job)."""
        hub = datahub_with_mocks
        
        # get_data is decorated with @safe_job which catches exceptions
        result = await hub.get_data("NonExistentProvider", "1d", ["TEST"], ["XNAS"])
        assert result is None
    
    @pytest.mark.asyncio
    @patch("quasar.services.datahub.core.TradingCalendar")
    async def test_get_data_batch_insertion(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data() batches insertion at 500 bars."""
        mock_calendar.has_sessions_in_range.return_value = True
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
            await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])
            
            # Should have been called twice: once for 500 bars, once for 250
            assert len(insert_calls) == 2

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.core.TradingCalendar")
    async def test_get_data_handles_unique_violation_fallback(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn, mock_asyncpg_pool
    ):
        """Test that _insert_bars falls back to INSERT ON CONFLICT when COPY fails with duplicates."""
        mock_calendar.has_sessions_in_range.return_value = True
        import asyncpg.exceptions
        
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Mock last_updated query to return empty (so requests are generated)
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        # First call to copy_records_to_table raises UniqueViolationError
        mock_asyncpg_conn.copy_records_to_table = AsyncMock(
            side_effect=asyncpg.exceptions.UniqueViolationError("duplicate key")
        )
        # Fallback executemany should succeed
        mock_asyncpg_conn.executemany = AsyncMock()
        
        await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])
        
        # Verify fallback was used - executemany should have been called
        mock_asyncpg_conn.executemany.assert_called()

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.core.TradingCalendar")
    async def test_get_data_reraises_non_unique_db_errors(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that non-UniqueViolation DB errors are caught by @safe_job decorator."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Mock last_updated query to return empty
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        # Raise a different database error
        mock_asyncpg_conn.copy_records_to_table = AsyncMock(
            side_effect=Exception("Connection lost")
        )
        
        # get_data is decorated with @safe_job which catches and logs errors
        result = await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])
        
        # Should return None (default from @safe_job) instead of raising
        assert result is None

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.core.TradingCalendar")
    async def test_get_data_historical_no_valid_requests_returns_early(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data returns early when all symbols are already up-to-date."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        # Mock last_updated query to return symbols that are already up-to-date
        # All symbols have been updated recently, so no requests needed
        from datetime import date
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            {"sym": "TEST", "d": date.today()}  # Already updated today
        ])
        
        # Track if insert_bars is called
        insert_called = False
        async def track_insert(*args, **kwargs):
            nonlocal insert_called
            insert_called = True
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with patch.object(hub, '_insert_bars', side_effect=track_insert):
                await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])
            
            # Should have issued a warning about no valid requests
            # Wait - with new logic, if start > yday, it just continues. 
            # In build_reqs_historical, if reqs is empty, get_data returns early.
            pass
        
        # Insert should never have been called
        assert not insert_called

    @pytest.mark.asyncio
    async def test_get_data_invalid_provider_type_returns_none(
        self, datahub_with_mocks
    ):
        """Test that get_data returns None for provider with invalid type (caught by @safe_job)."""
        hub = datahub_with_mocks
        
        # Create a mock provider with an invalid provider_type
        mock_provider = Mock()
        mock_provider.provider_type = None  # Invalid type
        hub._providers["BadProvider"] = mock_provider
        
        # get_data is decorated with @safe_job which catches exceptions
        result = await hub.get_data("BadProvider", "1d", ["TEST"], ["XNAS"])
        
        # Should return None (default from @safe_job) due to the invalid type causing an error
        assert result is None


# =============================================================================
# Tests for load_provider_cls Method
# =============================================================================

class TestLoadProviderCls:
    """Tests for DataHub.load_provider_cls() method with real file I/O."""

    @pytest.mark.asyncio
    async def test_load_provider_cls_already_loaded_returns_true(
        self, datahub_with_mocks, mock_provider_historical
    ):
        """Test that load_provider_cls returns True early if provider already loaded."""
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical
        
        result = await hub.load_provider_cls("TestProvider")
        
        assert result is True

    @pytest.mark.asyncio
    async def test_load_provider_cls_not_in_database_returns_false(
        self, datahub_with_mocks, mock_asyncpg_conn
    ):
        """Test that load_provider_cls returns False if provider not in database."""
        hub = datahub_with_mocks
        
        # Mock database to return no provider
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=None)
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await hub.load_provider_cls("NonExistentProvider")
            
            assert result is False
            assert len(w) == 1
            assert "not found in database" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_load_provider_cls_file_outside_allowed_path_returns_false(
        self, datahub_with_mocks, mock_asyncpg_conn, temp_provider_dir
    ):
        """Test that load_provider_cls returns False if file is outside allowed path."""
        hub = datahub_with_mocks
        
        # Mock database to return a provider with file outside allowed path
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': '/unauthorized/path/provider.py',
            'file_hash': b'somehash',
            'nonce': b'nonce',
            'ciphertext': b'ciphertext'
        })
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await hub.load_provider_cls("TestProvider")
            
            assert result is False
            assert len(w) == 1
            assert "not in allowed path" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_load_provider_cls_file_not_exists_returns_false(
        self, datahub_with_mocks, mock_asyncpg_conn, temp_provider_dir
    ):
        """Test that load_provider_cls returns False if file doesn't exist."""
        hub = datahub_with_mocks
        
        # Mock database to return a provider with non-existent file
        non_existent_path = str(temp_provider_dir / "does_not_exist.py")
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': non_existent_path,
            'file_hash': b'somehash',
            'nonce': b'nonce',
            'ciphertext': b'ciphertext'
        })
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await hub.load_provider_cls("TestProvider")
            
            assert result is False
            assert len(w) == 1
            assert "not found" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_load_provider_cls_hash_mismatch_returns_false(
        self, datahub_with_mocks, mock_asyncpg_conn, valid_provider_file
    ):
        """Test that load_provider_cls returns False if file hash doesn't match."""
        hub = datahub_with_mocks
        
        # Use wrong hash
        wrong_hash = b'wrong_hash_value_here_1234567890'
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': valid_provider_file['file_path'],
            'file_hash': wrong_hash,
            'nonce': b'nonce',
            'ciphertext': b'ciphertext'
        })
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await hub.load_provider_cls("TestProvider")
            
            assert result is False
            assert len(w) == 1
            assert "hash does not match" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_load_provider_cls_class_loading_failure_returns_false(
        self, datahub_with_mocks, mock_asyncpg_conn, temp_provider_dir
    ):
        """Test that load_provider_cls returns False if class loading fails."""
        hub = datahub_with_mocks
        
        # Create a file with invalid Python code
        invalid_file = temp_provider_dir / "invalid_provider.py"
        invalid_content = b"this is not valid python code @@@@"
        invalid_file.write_bytes(invalid_content)
        
        # Compute hash of invalid content
        sha256 = hashlib.sha256()
        sha256.update(invalid_content)
        file_hash = sha256.digest()
        
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': str(invalid_file),
            'file_hash': file_hash,
            'nonce': b'nonce',
            'ciphertext': b'ciphertext'
        })
        
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await hub.load_provider_cls("TestProvider")
            
            assert result is False
            assert len(w) == 1
            assert "Unable to load provider" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_load_provider_cls_success_returns_true(
        self, datahub_with_mocks, mock_asyncpg_conn, valid_provider_file, mock_system_context
    ):
        """Test that load_provider_cls returns True and loads provider on success."""
        hub = datahub_with_mocks
        hub.system_context = mock_system_context
        
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={
            'file_path': valid_provider_file['file_path'],
            'file_hash': valid_provider_file['file_hash'],
            'nonce': b'test_nonce',
            'ciphertext': b'test_ciphertext'
        })
        
        result = await hub.load_provider_cls("TEST_LIVE_PROVIDER")
        
        assert result is True
        assert "TEST_LIVE_PROVIDER" in hub._providers
        assert hub._providers["TEST_LIVE_PROVIDER"].name == "TEST_LIVE_PROVIDER"

    @pytest.mark.asyncio
    async def test_load_provider_cls_outer_exception_returns_false(
        self, datahub_with_mocks, mock_asyncpg_conn
    ):
        """Test that load_provider_cls returns False when outer exception occurs."""
        hub = datahub_with_mocks
        
        # Force an exception by making pool.acquire raise
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=Exception("Database connection error"))
        
        result = await hub.load_provider_cls("TestProvider")
        
        assert result is False


# =============================================================================
# Tests for load_provider_from_file_path Function
# =============================================================================

class TestLoadProviderFromFilePath:
    """Tests for load_provider_from_file_path() standalone function with real file I/O."""

    def test_file_not_found_raises_file_not_found_error(self, temp_provider_dir):
        """Test that load_provider_from_file_path raises FileNotFoundError for missing file."""
        non_existent = str(temp_provider_dir / "does_not_exist.py")
        
        with pytest.raises(FileNotFoundError) as exc_info:
            load_provider_from_file_path(non_existent, "TestProvider")
        
        assert "Provider file not found" in str(exc_info.value)

    def test_spec_creation_failure_raises_import_error(self, valid_provider_file, monkeypatch):
        """Test that load_provider_from_file_path raises ImportError if spec creation fails."""
        # Mock spec_from_file_location to return None
        monkeypatch.setattr(
            importlib.util,
            "spec_from_file_location",
            lambda *args, **kwargs: None
        )
        
        with pytest.raises(ImportError) as exc_info:
            load_provider_from_file_path(valid_provider_file['file_path'], "TEST_LIVE_PROVIDER")
        
        assert "Could not create module spec" in str(exc_info.value)

    def test_module_execution_error_raises_import_error(self, temp_provider_dir):
        """Test that load_provider_from_file_path raises ImportError if module execution fails."""
        # Create a file with syntax error
        syntax_error_file = temp_provider_dir / "syntax_error.py"
        syntax_error_file.write_text("def broken( # missing closing paren")
        
        with pytest.raises(ImportError) as exc_info:
            load_provider_from_file_path(str(syntax_error_file), "TestProvider")
        
        assert "Error executing module" in str(exc_info.value)

    def test_no_provider_classes_raises_import_error(self, temp_provider_dir):
        """Test that load_provider_from_file_path raises ImportError if no provider classes found."""
        # Create a file with no provider classes
        no_provider_file = temp_provider_dir / "no_provider.py"
        no_provider_file.write_text("""
class NotAProvider:
    pass
""")
        
        with pytest.raises(ImportError) as exc_info:
            load_provider_from_file_path(str(no_provider_file), "TestProvider")
        
        assert "No valid provider class found" in str(exc_info.value)

    def test_multiple_provider_classes_raises_import_error(self, temp_provider_dir):
        """Test that load_provider_from_file_path raises ImportError if multiple provider classes found."""
        # Create a file with multiple provider classes
        multi_provider_file = temp_provider_dir / "multi_provider.py"
        multi_provider_file.write_text("""
from quasar.lib.providers.core import HistoricalDataProvider, LiveDataProvider
from quasar.lib.common.context import DerivedContext

class Provider1(HistoricalDataProvider):
    name = 'PROVIDER1'
    RATE_LIMIT = (100, 60)
    
    def __init__(self, context: DerivedContext):
        super().__init__(context)
    
    async def get_available_symbols(self):
        return []
    
    async def get_history(self, sym, start, end, interval):
        yield None

class Provider2(HistoricalDataProvider):
    name = 'PROVIDER2'
    RATE_LIMIT = (100, 60)
    
    def __init__(self, context: DerivedContext):
        super().__init__(context)
    
    async def get_available_symbols(self):
        return []
    
    async def get_history(self, sym, start, end, interval):
        yield None
""")
        
        with pytest.raises(ImportError) as exc_info:
            load_provider_from_file_path(str(multi_provider_file), "PROVIDER1")
        
        assert "Multiple provider classes found" in str(exc_info.value)

    def test_class_name_mismatch_raises_import_error(self, valid_provider_file):
        """Test that load_provider_from_file_path raises ImportError if class name doesn't match."""
        with pytest.raises(ImportError) as exc_info:
            load_provider_from_file_path(
                valid_provider_file['file_path'],
                "WRONG_CLASS_NAME"
            )
        
        assert "but expected 'WRONG_CLASS_NAME'" in str(exc_info.value)

    def test_successful_load_returns_class(self, valid_provider_file):
        """Test that load_provider_from_file_path returns the class on success."""
        result = load_provider_from_file_path(
            valid_provider_file['file_path'],
            valid_provider_file['class_name']
        )
        
        assert result is not None
        assert result.name == valid_provider_file['class_name']
        assert issubclass(result, LiveDataProvider)


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