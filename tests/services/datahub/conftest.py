"""DataHub-specific test fixtures."""
import pytest
import hashlib


# =============================================================================
# Provider Code Templates for Real File I/O Testing
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

    async def fetch_available_symbols(self):
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

    async def fetch_available_symbols(self):
        return []

    async def get_history(self, sym, start, end, interval):
        yield None
'''


# =============================================================================
# Fixtures for Real File I/O Provider Testing
# =============================================================================

@pytest.fixture
def temp_provider_dir(tmp_path, monkeypatch):
    """
    Create a temporary directory for provider files and patch ALLOWED_DYNAMIC_PATH.
    Returns the path to the temp directory.
    """
    provider_dir = tmp_path / "dynamic_providers"
    provider_dir.mkdir()

    # Patch the ALLOWED_DYNAMIC_PATH constant in all locations where it's used
    monkeypatch.setattr(
        "quasar.services.datahub.utils.constants.ALLOWED_DYNAMIC_PATH",
        str(provider_dir)
    )
    monkeypatch.setattr(
        "quasar.services.datahub.handlers.providers.ALLOWED_DYNAMIC_PATH",
        str(provider_dir)
    )
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
