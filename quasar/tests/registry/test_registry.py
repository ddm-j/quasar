"""Tests for Registry API endpoints and public methods."""
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch, mock_open
from fastapi import HTTPException, UploadFile
from pathlib import Path
import base64
import json
import os
from io import BytesIO

from quasar.registry.core import Registry
from quasar.registry.schemas import (
    AssetMappingCreate, AssetMappingUpdate, AssetQueryParams,
    ClassType
)


# Helper class for mock asyncpg records that support both dict and attr access
class MockRecord:
    """Mock asyncpg record that supports both dictionary and attribute access."""
    def __init__(self, **kwargs):
        self._data = kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def __getitem__(self, key):
        return self._data[key]
    
    def get(self, key, default=None):
        return self._data.get(key, default)
    
    def __contains__(self, key):
        return key in self._data
    
    def keys(self):
        """Support dict() conversion."""
        return self._data.keys()
    
    def __iter__(self):
        """Support dict() conversion."""
        return iter(self._data)
    
    def items(self):
        """Support dict() conversion."""
        return self._data.items()
    
    def values(self):
        """Support dict() conversion."""
        return self._data.values()


class TestRegistryFileUpload:
    """Tests for file upload endpoint."""
    
    @pytest.mark.asyncio
    async def test_handle_upload_file_valid_provider(
        self, registry_with_mocks, mock_aiohttp_session
    ):
        """Test that handle_upload_file succeeds with valid provider file."""
        reg = registry_with_mocks
        
        # Mock validation response
        mock_aiohttp_session["response"].status = 200
        mock_aiohttp_session["response"].json = AsyncMock(return_value={
            "class_name": "TestProvider",
            "subclass_type": "Historical"
        })
        
        # Mock file
        file_content = b"class TestProvider: pass"
        file = UploadFile(
            filename="test_provider.py",
            file=BytesIO(file_content)
        )
        
        # Mock file operations
        with patch('os.path.exists', return_value=False):
            with patch('builtins.open', mock_open()):
                with patch.object(reg, '_register_code', new_callable=AsyncMock) as mock_register:
                    mock_register.return_value = 1
                    
                    response = await reg.handle_upload_file(
                        class_type="provider",
                        file=file,
                        secrets='{"api_key": "test"}'
                    )
                    
                    assert response.status.startswith("File")
                    assert "uploaded successfully" in response.status
    
    @pytest.mark.asyncio
    async def test_handle_upload_file_invalid_class_type(self, registry_with_mocks):
        """Test that handle_upload_file returns 400 for invalid class type."""
        reg = registry_with_mocks
        
        file = UploadFile(filename="test.py", file=BytesIO(b"content"))
        
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_upload_file(
                class_type="invalid",
                file=file,
                secrets='{}'
            )
        
        assert exc_info.value.status_code == 400
    
    @pytest.mark.asyncio
    async def test_handle_upload_file_invalid_file_type(self, registry_with_mocks):
        """Test that handle_upload_file returns 415 for non-Python file."""
        reg = registry_with_mocks
        
        file = UploadFile(filename="test.txt", file=BytesIO(b"content"))
        
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_upload_file(
                class_type="provider",
                file=file,
                secrets='{}'
            )
        
        assert exc_info.value.status_code == 415
    
    @pytest.mark.asyncio
    async def test_handle_upload_file_empty_file(self, registry_with_mocks):
        """Test that handle_upload_file returns 400 for empty file."""
        reg = registry_with_mocks
        
        file = UploadFile(filename="test.py", file=BytesIO(b""))
        
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_upload_file(
                class_type="provider",
                file=file,
                secrets='{}'
            )
        
        assert exc_info.value.status_code == 400
    
    @pytest.mark.asyncio
    async def test_handle_upload_file_validation_failure(
        self, registry_with_mocks, mock_aiohttp_session
    ):
        """Test that handle_upload_file handles validation failure."""
        reg = registry_with_mocks
        
        mock_aiohttp_session["response"].status = 500
        mock_aiohttp_session["response"].json = AsyncMock(return_value={
            "detail": "Validation failed"
        })
        
        file = UploadFile(filename="test.py", file=BytesIO(b"content"))
        
        with patch('os.path.exists', return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                await reg.handle_upload_file(
                    class_type="provider",
                    file=file,
                    secrets='{}'
                )
            
            assert exc_info.value.status_code == 500


class TestRegistryDeleteClass:
    """Tests for delete class endpoint."""
    
    @pytest.mark.asyncio
    async def test_handle_delete_class_success(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_delete_class successfully deletes class."""
        reg = registry_with_mocks
        
        # handle_delete_class uses pool.fetchval() directly
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            "/app/dynamic_providers/test.py",  # File path query
            1  # Delete query returns ID
        ])
        
        with patch('os.path.exists', return_value=True):
            with patch('os.remove'):
                response = await reg.handle_delete_class("provider", "TestProvider")
                
                assert response.class_name == "TestProvider"
                assert response.class_type == "provider"
                assert response.file_deleted is True
    
    @pytest.mark.asyncio
    async def test_handle_delete_class_not_found(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_delete_class returns 404 for non-existent class."""
        reg = registry_with_mocks

        # Mock pool.fetchval() directly (the method uses pool.fetchval, not conn.fetchval)
        # First call gets file_path, returns None to indicate class not found
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_delete_class("provider", "NonExistent")
        
        assert exc_info.value.status_code == 404


class TestRegistryUpdateAssets:
    """Tests for update assets endpoints."""
    
    @pytest.mark.asyncio
    async def test_handle_update_assets_success(
        self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session
    ):
        """Test that handle_update_assets successfully updates assets."""
        reg = registry_with_mocks
        
        # Mock class exists check
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)
        
        # Mock DataHub response
        mock_aiohttp_session["response"].status = 200
        mock_aiohttp_session["response"].json = AsyncMock(return_value=[
            {
                "symbol": "TEST",
                "name": "Test Asset",
                "provider_id": "TEST_ID"
            }
        ])
        
        # Mock upsert
        mock_asyncpg_conn.prepare = AsyncMock(return_value=mock_asyncpg_conn)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"xmax": 0})
        
        with patch.object(reg, '_update_assets_for_provider', new_callable=AsyncMock) as mock_update:
            mock_update.return_value = {
                "class_name": "TestProvider",
                "class_type": "provider",
                "status": 200,
                "added_symbols": 1,
                "updated_symbols": 0,
                "failed_symbols": 0
            }
            
            response = await reg.handle_update_assets("provider", "TestProvider")
            
            assert response.class_name == "TestProvider"
            assert response.status == 200
    
    @pytest.mark.asyncio
    async def test_handle_update_assets_class_not_registered(
        self, registry_with_mocks, mock_asyncpg_pool, mock_aiohttp_session
    ):
        """Test that handle_update_assets returns 404 for non-registered class."""
        reg = registry_with_mocks

        # Mock pool.fetchval() directly - returns None to indicate class not registered
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_assets("provider", "NonExistent")
        
        assert exc_info.value.status_code == 404
    
    @pytest.mark.asyncio
    async def test_handle_update_all_assets_multiple_providers(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_update_all_assets updates all providers."""
        reg = registry_with_mocks
        
        mock_record1 = MockRecord(class_name="Provider1", class_type="provider")
        mock_record2 = MockRecord(class_name="Provider2", class_type="provider")
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record1, mock_record2])
        
        with patch.object(reg, '_update_assets_for_provider', new_callable=AsyncMock) as mock_update:
            mock_update.return_value = {
                "class_name": "Provider1",
                "class_type": "provider",
                "status": 200
            }
            
            responses = await reg.handle_update_all_assets()
            
            assert len(responses) == 2
    
    @pytest.mark.asyncio
    async def test_handle_update_all_assets_empty_registry(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_update_all_assets returns empty list for empty registry."""
        reg = registry_with_mocks
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        responses = await reg.handle_update_all_assets()
        
        assert responses == []


class TestRegistryGetAssets:
    """Tests for get assets endpoint."""
    
    @pytest.mark.asyncio
    async def test_handle_get_assets_with_pagination(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_assets handles pagination."""
        reg = registry_with_mocks

        # AssetItem requires: id, class_name, class_type, symbol (and optional fields)
        mock_record = MockRecord(
            id=1, 
            class_name="TestProvider", 
            class_type="provider",
            symbol="TEST"
        )
        
        # handle_get_assets uses pool.acquire() then conn.fetch/fetchrow
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=1))
        
        params = AssetQueryParams(limit=10, offset=0)
        response = await reg.handle_get_assets(params)
        
        assert len(response.items) == 1
        assert response.total_items == 1
        assert response.limit == 10
    
    @pytest.mark.asyncio
    async def test_handle_get_assets_with_filtering(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_assets handles filtering."""
        reg = registry_with_mocks
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"total_items": 0})
        
        params = AssetQueryParams(
            limit=25,
            offset=0,
            class_name_like="Test",
            asset_class="equity"
        )
        response = await reg.handle_get_assets(params)
        
        assert response.total_items == 0


class TestRegistryGetClassesSummary:
    """Tests for get classes summary endpoint."""
    
    @pytest.mark.asyncio
    async def test_handle_get_classes_summary_with_classes(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_classes_summary returns classes with asset counts."""
        reg = registry_with_mocks
        
        mock_record = MockRecord(
            id=1, class_name="TestProvider", class_type="provider",
            class_subtype="Historical", uploaded_at="2024-01-01", asset_count=5
        )
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        
        summary = await reg.handle_get_classes_summary()
        
        assert len(summary) == 1
        assert summary[0].class_name == "TestProvider"
        assert summary[0].asset_count == 5
    
    @pytest.mark.asyncio
    async def test_handle_get_classes_summary_empty_registry(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_classes_summary returns empty list for empty registry."""
        reg = registry_with_mocks
        
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        
        summary = await reg.handle_get_classes_summary()
        
        assert summary == []


class TestRegistryAssetMappings:
    """Tests for asset mapping endpoints."""
    
    @pytest.mark.asyncio
    async def test_handle_create_asset_mapping_success(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_create_asset_mapping successfully creates mapping."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )
        
        # handle_create_asset_mapping uses pool.fetchrow() directly
        mock_asyncpg_pool.fetchrow = AsyncMock(return_value=mock_record)
        
        mapping = AssetMappingCreate(
            common_symbol="BTCUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="BTC-USD"
        )
        
        response = await reg.handle_create_asset_mapping(mapping)
        
        assert response.common_symbol == "BTCUSD"
        assert response.class_name == "TestProvider"
    
    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_all(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings returns all mappings."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )
        
        # handle_get_asset_mappings uses pool.fetch() directly
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[mock_record])
        
        mappings = await reg.handle_get_asset_mappings()
        
        assert len(mappings) == 1
        assert mappings[0].common_symbol == "BTCUSD"
    
    @pytest.mark.asyncio
    async def test_handle_update_asset_mapping_success(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_update_asset_mapping successfully updates mapping."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            common_symbol="BTCUSD_UPDATED", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=False
        )
        
        # handle_update_asset_mapping uses pool.fetchrow() directly
        mock_asyncpg_pool.fetchrow = AsyncMock(return_value=mock_record)
        
        update = AssetMappingUpdate(common_symbol="BTCUSD_UPDATED", is_active=False)
        
        response = await reg.handle_update_asset_mapping(
            "TestProvider",
            "provider",
            "BTC-USD",
            update
        )
        
        assert response.common_symbol == "BTCUSD_UPDATED"
        assert response.is_active is False
    
    @pytest.mark.asyncio
    async def test_handle_delete_asset_mapping_success(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_delete_asset_mapping successfully deletes mapping."""
        reg = registry_with_mocks
        
        mock_asyncpg_conn.fetchval = AsyncMock(return_value="BTCUSD")
        
        from fastapi.responses import Response
        response = await reg.handle_delete_asset_mapping(
            "TestProvider",
            "provider",
            "BTC-USD"
        )
        
        assert isinstance(response, Response)
        assert response.status_code == 204
    
    @pytest.mark.asyncio
    async def test_handle_delete_asset_mapping_not_found(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_delete_asset_mapping returns 404 for non-existent mapping."""
        reg = registry_with_mocks

        # handle_delete_asset_mapping uses pool.fetchval() directly
        # fetchval returns None when no rows are deleted
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)
        
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_delete_asset_mapping(
                "TestProvider",
                "provider",
                "NONEXISTENT"
            )
        
        assert exc_info.value.status_code == 404


class TestRegistryLifecycleMethods:
    """Tests for Registry lifecycle methods."""
    
    @pytest.mark.asyncio
    async def test_start_initializes_pool_and_starts_api(
        self, registry_with_mocks
    ):
        """Test that start() initializes pool and starts API server."""
        reg = registry_with_mocks
        
        with patch.object(reg, 'start_api_server', new_callable=AsyncMock):
            await reg.start()
            
            reg.start_api_server.assert_called_once()
            # Pool should be initialized (via init_pool)
    
    @pytest.mark.asyncio
    async def test_stop_closes_pool_and_stops_api(
        self, registry_with_mocks
    ):
        """Test that stop() closes pool and stops API server."""
        reg = registry_with_mocks
        
        with patch.object(reg, 'stop_api_server', new_callable=AsyncMock):
            with patch.object(reg, 'close_pool', new_callable=AsyncMock):
                await reg.stop()
                
                reg.stop_api_server.assert_called_once()
                reg.close_pool.assert_called_once()

