"""Shared pytest fixtures for Quasar backend tests."""
import os
import tempfile
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from typing import AsyncGenerator, Generator
try:
    import asyncpg
except ImportError:
    asyncpg = None  # Will be mocked anyway
from pathlib import Path
from fastapi.testclient import TestClient
from datetime import datetime, timezone

# Set up system context file before importing modules that use SystemContext
# SystemContext is instantiated at class definition time, so we need to set
# the environment variable before any imports
if "QUASAR_SYSTEM_CONTEXT" not in os.environ:
    # Create a temporary system context file for testing
    _temp_context_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.key')
    _temp_context_file.write(b'test_system_context_key_for_testing_only_32_bytes!!')
    _temp_context_file.close()
    os.environ["QUASAR_SYSTEM_CONTEXT"] = _temp_context_file.name

from quasar.datahub.core import DataHub
from quasar.registry.core import Registry
from quasar.common.secret_store import SecretStore
from quasar.common.context import SystemContext, DerivedContext
from quasar.providers.core import ProviderType


# pytest-asyncio is configured in pyproject.toml to auto-detect async tests


@pytest.fixture(scope="session", autouse=True)
def cleanup_temp_context_file():
    """Clean up temporary system context file after all tests."""
    yield
    # Clean up the temporary file if it exists
    context_path = os.environ.get("QUASAR_SYSTEM_CONTEXT")
    if context_path and os.path.exists(context_path) and "temp" in context_path.lower():
        try:
            os.unlink(context_path)
        except Exception:
            pass  # Ignore cleanup errors


@pytest.fixture
def mock_asyncpg_pool() -> AsyncMock:
    """Mock asyncpg pool."""
    if asyncpg:
        pool = AsyncMock(spec=asyncpg.Pool)
    else:
        pool = AsyncMock()
    pool._closed = False
    # Mock pool methods that are called directly (not through connection)
    pool.fetchval = AsyncMock()
    pool.fetch = AsyncMock()
    pool.fetchrow = AsyncMock()
    return pool


@pytest.fixture
def mock_asyncpg_conn(mock_asyncpg_pool: AsyncMock) -> AsyncMock:
    """Mock asyncpg connection."""
    if asyncpg:
        conn = AsyncMock(spec=asyncpg.Connection)
    else:
        conn = AsyncMock()
    
    # Setup context manager for pool.acquire()
    async def acquire():
        return conn
    
    mock_asyncpg_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    mock_asyncpg_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    
    return conn


@pytest.fixture
def mock_secret_store() -> Mock:
    """Mock SecretStore."""
    store = Mock(spec=SecretStore)
    store.get = AsyncMock(return_value={"api_key": "test_key"})
    return store


@pytest.fixture
def mock_system_context() -> Mock:
    """Mock SystemContext."""
    context = Mock(spec=SystemContext)
    
    # Mock AESGCM object
    mock_aesgcm = Mock()
    context.get_derived_context = Mock(return_value=mock_aesgcm)
    context.create_context_data = Mock(return_value=(b'test_nonce', b'test_ciphertext'))
    
    return context


@pytest.fixture
def mock_derived_context() -> Mock:
    """Mock DerivedContext."""
    context = Mock(spec=DerivedContext)
    context.get = Mock(return_value="test_value")
    return context


@pytest.fixture
def mock_provider_historical() -> Mock:
    """Mock historical data provider."""
    provider = Mock()
    provider.name = "TestProvider"
    provider.provider_type = ProviderType.HISTORICAL
    
    async def mock_get_data(reqs):
        from quasar.providers.core import Bar
        from datetime import datetime, timezone
        # Yield a few mock bars
        for i in range(3):
            yield Bar(
                ts=datetime.now(timezone.utc),
                sym="TEST",
                o=100.0,
                h=110.0,
                l=95.0,
                c=105.0,
                v=1000
            )
    
    provider.get_data = mock_get_data
    provider.get_available_symbols = AsyncMock(return_value=[
        {
            "provider": "TestProvider",
            "provider_id": "TEST",
            "symbol": "TEST",
            "name": "Test Asset",
            "exchange": "TEST",
            "asset_class": "equity",
            "base_currency": "USD",
            "quote_currency": "USD",
            "country": "US"
        }
    ])
    provider.aclose = AsyncMock()
    
    return provider


@pytest.fixture
def mock_provider_live() -> Mock:
    """Mock live data provider."""
    provider = Mock()
    provider.name = "TestLiveProvider"
    provider.provider_type = ProviderType.REALTIME
    provider.close_buffer_seconds = 10
    
    async def mock_get_data(interval, symbols, timeout=None):
        from quasar.providers.core import Bar
        from datetime import datetime, timezone
        # Return mock bars as async generator (like the real implementation)
        bars = [
            Bar(
                ts=datetime.now(timezone.utc),
                sym="TEST",
                o=100.0,
                h=110.0,
                l=95.0,
                c=105.0,
                v=1000
            )
        ]
        for bar in bars:
            yield bar
    
    provider.get_data = mock_get_data
    provider.get_available_symbols = AsyncMock(return_value=[
        {
            "provider": "TestLiveProvider",
            "provider_id": "TEST",
            "symbol": "TEST",
            "name": "Test Asset",
            "exchange": "TEST",
            "asset_class": "equity",
            "base_currency": "USD",
            "quote_currency": "USD",
            "country": "US"
        }
    ])
    provider.aclose = AsyncMock()
    
    return provider


@pytest.fixture
def mock_file_system(monkeypatch: pytest.MonkeyPatch) -> Generator[dict, None, None]:
    """Mock filesystem operations."""
    files = {}
    
    def mock_path_is_file(self):
        return str(self) in files
    
    def mock_path_read_bytes(self):
        return files.get(str(self), b'')
    
    def mock_open(path, mode='r', **kwargs):
        if 'w' in mode or 'a' in mode:
            file_mock = MagicMock()
            file_mock.write = Mock()
            file_mock.read = Mock(return_value=b'')
            return file_mock
        else:
            content = files.get(str(path), b'')
            file_mock = MagicMock()
            file_mock.read = Mock(return_value=content)
            file_mock.__enter__ = Mock(return_value=file_mock)
            file_mock.__exit__ = Mock(return_value=None)
            return file_mock
    
    monkeypatch.setattr(Path, "is_file", mock_path_is_file)
    monkeypatch.setattr(Path, "read_bytes", mock_path_read_bytes)
    monkeypatch.setattr(Path, "read_text", lambda self: files.get(str(self), ''))
    
    # Store files dict for test manipulation
    yield {"files": files}
    
    files.clear()


@pytest.fixture
def mock_aiohttp_session(monkeypatch: pytest.MonkeyPatch) -> Generator[Mock, None, None]:
    """Mock aiohttp ClientSession."""
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={})
    mock_response.text = AsyncMock(return_value="")
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)
    
    # Create async context manager for response
    class MockResponseContext:
        def __init__(self, response):
            self.response = response
        async def __aenter__(self):
            return self.response
        async def __aexit__(self, *args):
            return None
    
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    # Make post() and get() return async context managers
    mock_session.post = Mock(return_value=MockResponseContext(mock_response))
    mock_session.get = Mock(return_value=MockResponseContext(mock_response))
    
    # Create a proper async context manager class that returns the session
    class MockClientSession:
        def __init__(self, *args, **kwargs):
            pass
        async def __aenter__(self):
            return mock_session
        async def __aexit__(self, *args):
            return None
    
    with patch('aiohttp.ClientSession', MockClientSession):
        yield {"session": mock_session, "response": mock_response}


@pytest.fixture
def datahub_with_mocks(
    mock_asyncpg_pool: AsyncMock,
    mock_secret_store: Mock,
    mock_system_context: Mock,
    monkeypatch: pytest.MonkeyPatch
) -> DataHub:
    """Create DataHub instance with mocked dependencies."""
    # Patch SystemContext singleton
    monkeypatch.setattr("quasar.datahub.core.SystemContext", lambda: mock_system_context)
    
    # Create DataHub with mocked pool
    hub = DataHub(
        secret_store=mock_secret_store,
        pool=mock_asyncpg_pool,
        api_port=0  # Use random port for testing
    )
    
    return hub


@pytest.fixture
def registry_with_mocks(
    mock_asyncpg_pool: AsyncMock,
    monkeypatch: pytest.MonkeyPatch
) -> Registry:
    """Create Registry instance with mocked dependencies."""
    # Patch SystemContext singleton
    mock_system_context = Mock(spec=SystemContext)
    mock_aesgcm = Mock()
    mock_system_context.get_derived_context = Mock(return_value=mock_aesgcm)
    mock_system_context.create_context_data = Mock(return_value=(b'test_nonce', b'test_ciphertext'))
    monkeypatch.setattr("quasar.registry.core.SystemContext", lambda: mock_system_context)
    
    # Create Registry with mocked pool
    registry = Registry(
        pool=mock_asyncpg_pool,
        api_port=0  # Use random port for testing
    )
    
    return registry


@pytest.fixture
def datahub_client(datahub_with_mocks: DataHub) -> TestClient:
    """Create FastAPI TestClient for DataHub."""
    return TestClient(datahub_with_mocks._api_app)


@pytest.fixture
def registry_client(registry_with_mocks: Registry) -> TestClient:
    """Create FastAPI TestClient for Registry."""
    return TestClient(registry_with_mocks._api_app)

