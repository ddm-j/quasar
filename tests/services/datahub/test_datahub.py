"""Tests for DataHub API endpoints and public methods."""
import pytest
import os
import tempfile
import shutil
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

