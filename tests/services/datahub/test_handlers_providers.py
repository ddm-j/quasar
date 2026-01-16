"""Tests for provider management handlers."""
import pytest
import hashlib
import inspect
import importlib.util
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from fastapi import HTTPException

from quasar.services.datahub.core import DataHub, load_provider_from_file_path
from quasar.services.datahub.schemas import ProviderValidateRequest
from quasar.lib.providers.core import HistoricalDataProvider, LiveDataProvider


class TestValidateProvider:
    """Tests for validate_provider endpoint."""

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


class TestGetAvailableSymbols:
    """Tests for handle_get_available_symbols endpoint."""

    @pytest.mark.asyncio
    async def test_handle_get_available_symbols_provider_exists(
        self, datahub_with_mocks, mock_provider_historical
    ):
        """Test that handle_get_available_symbols returns symbols for loaded provider."""
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical

        response = await hub.handle_get_available_symbols("TestProvider")

        assert len(response.items) == 1
        assert response.items[0]["symbol"] == "TEST"

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
            response = await hub.handle_get_available_symbols("TestProvider")

            assert len(response.items) == 1

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
        del mock_provider.fetch_available_symbols  # Remove the method
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
