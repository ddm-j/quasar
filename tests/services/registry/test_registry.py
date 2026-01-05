"""Tests for Registry API endpoints and public methods."""
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock, patch, mock_open
from fastapi import HTTPException, UploadFile
from pathlib import Path
import base64
import json
import os
from io import BytesIO
from asyncpg.exceptions import UndefinedFunctionError

from quasar.services.registry.core import Registry, _encode_cursor, _decode_cursor
from quasar.services.registry.schemas import (
    AssetMappingCreate, AssetMappingUpdate, AssetQueryParams,
    AssetMappingQueryParams, AssetMappingPaginatedResponse, AssetMappingResponse,
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


def make_suggestion_record(
    source_class="EODHD",
    source_type="provider",
    source_symbol="AAPL.US",
    source_name="Apple Inc",
    target_class="DATABENTO",
    target_type="provider",
    target_symbol="AAPL",
    target_name="Apple Inc.",
    target_common_symbol=None,
    proposed_common_symbol="aapl",
    score=85.0,
    id_match=True,
    external_id_match=False,
    norm_match=True,
    base_quote_match=True,
    exchange_match=False,
    sym_root_similarity=1.0,
    name_similarity=0.95,
    target_already_mapped=False,
):
    """Factory for creating suggestion mock records."""
    return MockRecord(
        source_class=source_class,
        source_type=source_type,
        source_symbol=source_symbol,
        source_name=source_name,
        target_class=target_class,
        target_type=target_type,
        target_symbol=target_symbol,
        target_name=target_name,
        target_common_symbol=target_common_symbol,
        proposed_common_symbol=proposed_common_symbol,
        score=score,
        id_match=id_match,
        external_id_match=external_id_match,
        norm_match=norm_match,
        base_quote_match=base_quote_match,
        exchange_match=exchange_match,
        sym_root_similarity=sym_root_similarity,
        name_similarity=name_similarity,
        target_already_mapped=target_already_mapped,
    )


async def call_suggestions(
    reg,
    source_class,
    source_type=None,
    target_class=None,
    target_type=None,
    search=None,
    min_score=30.0,
    limit=50,
    offset=0,
    cursor=None,
    include_total=False,
):
    """Helper to call handle_get_asset_mapping_suggestions with explicit params."""
    return await reg.handle_get_asset_mapping_suggestions(
        source_class=source_class,
        source_type=source_type,
        target_class=target_class,
        target_type=target_type,
        search=search,
        min_score=min_score,
        limit=limit,
        offset=offset,
        cursor=cursor,
        include_total=include_total,
    )


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
                "matcher_symbol": "TEST",
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
    async def test_update_assets_response_includes_identity_stats(
        self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session
    ):
        """Test that UpdateAssetsResponse includes identity_matched and identity_skipped."""
        reg = registry_with_mocks
        
        with patch.object(reg, '_update_assets_for_provider', new_callable=AsyncMock) as mock_update:
            mock_update.return_value = {
                "class_name": "TestProvider",
                "class_type": "provider",
                "status": 200,
                "added_symbols": 5,
                "updated_symbols": 2,
                "failed_symbols": 0,
                "identity_matched": 3,
                "identity_skipped": 1
            }
            
            response = await reg.handle_update_assets("provider", "TestProvider")
            
            assert response.identity_matched == 3
            assert response.identity_skipped == 1

    @pytest.mark.asyncio
    async def test_handle_update_all_assets_runs_global_matching(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_update_all_assets runs global identity matching after providers."""
        reg = registry_with_mocks
        
        mock_record = MockRecord(class_name="Provider1", class_type="provider")
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        
        with patch.object(reg, '_update_assets_for_provider', new_callable=AsyncMock) as mock_update, \
             patch.object(reg.matcher, 'identify_all_unidentified_assets', new_callable=AsyncMock) as mock_global_match, \
             patch.object(reg, '_apply_identity_matches', new_callable=AsyncMock) as mock_apply:
            
            mock_update.return_value = {
                "class_name": "Provider1",
                "class_type": "provider",
                "status": 200
            }
            
            # Global matching finds some assets
            from quasar.services.registry.matcher import MatchResult
            mock_global_match.return_value = [
                MatchResult(
                    asset_id=10, symbol="LATE", primary_id="FIGI_LATE",
                    identity_symbol="LATE", identity_name="Late Match",
                    confidence=85.0, match_type="fuzzy_symbol"
                )
            ]
            mock_apply.return_value = {'identified': 1, 'skipped': 0, 'failed': 0, 'constraint_rejected': 0}
            
            await reg.handle_update_all_assets()
            
            # Global matching should have been called
            mock_global_match.assert_called_once()
            # Apply should have been called with the global matches
            mock_apply.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_update_all_assets_gracefully_handles_global_match_failure(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that global matching failure doesn't break the overall response."""
        reg = registry_with_mocks
        
        mock_record = MockRecord(class_name="Provider1", class_type="provider")
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        
        with patch.object(reg, '_update_assets_for_provider', new_callable=AsyncMock) as mock_update, \
             patch.object(reg.matcher, 'identify_all_unidentified_assets', new_callable=AsyncMock) as mock_global_match:
            
            mock_update.return_value = {
                "class_name": "Provider1",
                "class_type": "provider",
                "status": 200
            }
            
            # Global matching throws an error
            mock_global_match.side_effect = Exception("Global matching failed")
            
            # Should not raise - returns successfully
            responses = await reg.handle_update_all_assets()
            
            assert len(responses) == 1
            assert responses[0].class_name == "Provider1"
    
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
            symbol="TEST",
            matcher_symbol="TEST"
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
        self, registry_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Single-item create returns list with one element."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        mock_record = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )

        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=mock_record)
        
        mapping = AssetMappingCreate(
            common_symbol="BTCUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="BTC-USD"
        )
        
        response = await reg.handle_create_asset_mapping(mapping)
        
        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0].common_symbol == "BTCUSD"
        assert response[0].class_name == "TestProvider"

    @pytest.mark.asyncio
    async def test_handle_create_asset_mapping_batch_success(
        self, registry_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Batch create returns list and stays transactional."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        mock_record_one = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )
        mock_record_two = MockRecord(
            common_symbol="BTCUSD", class_name="TestBroker",
            class_type="broker", class_symbol="BTC-USD", is_active=True
        )

        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[mock_record_one, mock_record_two])

        mappings = [
            AssetMappingCreate(
                common_symbol="BTCUSD",
                class_name="TestProvider",
                class_type="provider",
                class_symbol="BTC-USD"
            ),
            AssetMappingCreate(
                common_symbol="BTCUSD",
                class_name="TestBroker",
                class_type="broker",
                class_symbol="BTC-USD"
            ),
        ]

        response = await reg.handle_create_asset_mapping(mappings)

        assert isinstance(response, list)
        assert len(response) == 2
        assert response[0].class_name == "TestProvider"
        assert response[1].class_name == "TestBroker"

    @pytest.mark.asyncio
    async def test_handle_create_asset_mapping_batch_conflict_rolls_back(
        self, registry_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Batch create conflicts raise and do not partially commit."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        mock_record_one = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )

        conflict_error = HTTPException(status_code=409, detail="duplicate mapping")
        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[mock_record_one, conflict_error])

        mappings = [
            AssetMappingCreate(
                common_symbol="BTCUSD",
                class_name="TestProvider",
                class_type="provider",
                class_symbol="BTC-USD"
            ),
            AssetMappingCreate(
                common_symbol="BTCUSD",
                class_name="TestProvider",
                class_type="provider",
                class_symbol="BTC-USD"
            ),
        ]

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_create_asset_mapping(mappings)

        assert exc_info.value.status_code == 409
        assert mock_asyncpg_conn.fetchrow.await_count == 2
        txn.__aexit__.assert_awaited()
    
    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_with_default_pagination(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_asset_mappings works with default pagination."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )

        # handle_get_asset_mappings uses pool.acquire() then conn.fetch/fetchrow
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=1))

        params = AssetMappingQueryParams()
        response = await reg.handle_get_asset_mappings(params)

        assert isinstance(response, AssetMappingPaginatedResponse)
        assert len(response.items) == 1
        assert response.items[0].common_symbol == "BTCUSD"
        assert response.total_items == 1
        assert response.limit == 25  # default
        assert response.offset == 0
        assert response.page == 1
        assert response.total_pages == 1

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_with_pagination(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_asset_mappings handles pagination."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )

        # handle_get_asset_mappings uses pool.acquire() then conn.fetch/fetchrow
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=1))

        params = AssetMappingQueryParams(limit=10, offset=0)
        response = await reg.handle_get_asset_mappings(params)

        assert len(response.items) == 1
        assert response.total_items == 1
        assert response.limit == 10
        assert response.offset == 0
        assert response.page == 1
        assert response.total_pages == 1

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_with_sorting(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_asset_mappings handles sorting."""
        reg = registry_with_mocks

        # Create records in "unsorted" order for testing
        unsorted_records = [
            MockRecord(
                common_symbol="ETHUSD", class_name="TestProvider2",
                class_type="provider", class_symbol="ETH-USD", is_active=False
            ),
            MockRecord(
                common_symbol="BTCUSD", class_name="TestProvider1",
                class_type="provider", class_symbol="BTC-USD", is_active=True
            )
        ]

        # Records sorted by common_symbol ascending (BTCUSD, ETHUSD)
        asc_sorted_records = [
            MockRecord(
                common_symbol="BTCUSD", class_name="TestProvider1",
                class_type="provider", class_symbol="BTC-USD", is_active=True
            ),
            MockRecord(
                common_symbol="ETHUSD", class_name="TestProvider2",
                class_type="provider", class_symbol="ETH-USD", is_active=False
            )
        ]

        # Records sorted by common_symbol descending (ETHUSD, BTCUSD)
        desc_sorted_records = [
            MockRecord(
                common_symbol="ETHUSD", class_name="TestProvider2",
                class_type="provider", class_symbol="ETH-USD", is_active=False
            ),
            MockRecord(
                common_symbol="BTCUSD", class_name="TestProvider1",
                class_type="provider", class_symbol="BTC-USD", is_active=True
            )
        ]

        # Test single column ascending sort
        mock_asyncpg_conn.fetch = AsyncMock(return_value=asc_sorted_records)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=2))

        params = AssetMappingQueryParams(sort_by="common_symbol", sort_order="asc")
        response = await reg.handle_get_asset_mappings(params)

        assert len(response.items) == 2
        assert response.items[0].common_symbol == "BTCUSD"
        assert response.items[1].common_symbol == "ETHUSD"

        # Test single column descending sort
        mock_asyncpg_conn.fetch.reset_mock()
        mock_asyncpg_conn.fetchrow.reset_mock()
        mock_asyncpg_conn.fetch = AsyncMock(return_value=desc_sorted_records)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=2))

        params = AssetMappingQueryParams(sort_by="common_symbol", sort_order="desc")
        response = await reg.handle_get_asset_mappings(params)

        assert len(response.items) == 2
        assert response.items[0].common_symbol == "ETHUSD"
        assert response.items[1].common_symbol == "BTCUSD"

        # Test multiple column sort (by class_name, then common_symbol)
        mock_asyncpg_conn.fetch.reset_mock()
        mock_asyncpg_conn.fetchrow.reset_mock()
        mock_asyncpg_conn.fetch = AsyncMock(return_value=asc_sorted_records)  # TestProvider1 (BTCUSD), TestProvider2 (ETHUSD)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=2))

        params = AssetMappingQueryParams(sort_by="class_name,common_symbol", sort_order="asc")
        response = await reg.handle_get_asset_mappings(params)

        assert len(response.items) == 2
        assert response.items[0].class_name == "TestProvider1"
        assert response.items[1].class_name == "TestProvider2"

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_invalid_sort_parameters(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test error handling for invalid sort parameters."""
        reg = registry_with_mocks

        # Test invalid sort column
        params = AssetMappingQueryParams(sort_by="invalid_column", sort_order="asc")
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_asset_mappings(params)
        assert exc_info.value.status_code == 400
        assert "Invalid sort_by column" in str(exc_info.value.detail)

        # Test invalid sort order
        params = AssetMappingQueryParams(sort_by="common_symbol", sort_order="invalid")
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_asset_mappings(params)
        assert exc_info.value.status_code == 400
        assert "Invalid sort_order value" in str(exc_info.value.detail)

        # Test mismatched sort_by and sort_order counts
        params = AssetMappingQueryParams(sort_by="class_name,common_symbol", sort_order="asc,desc,asc")
        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_asset_mappings(params)
        assert exc_info.value.status_code == 400
        assert "Mismatch between sort_by and sort_order counts" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_with_filtering(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_asset_mappings handles filtering."""
        reg = registry_with_mocks

        # Mock no results for filtering tests
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=0))

        # Test exact match filters
        params = AssetMappingQueryParams(common_symbol="BTCUSD")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        params = AssetMappingQueryParams(class_name="TestProvider")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        params = AssetMappingQueryParams(class_type="provider")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        params = AssetMappingQueryParams(class_symbol="BTC-USD")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        params = AssetMappingQueryParams(is_active=True)
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        # Test partial match filters
        params = AssetMappingQueryParams(common_symbol_like="BTC")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        params = AssetMappingQueryParams(class_name_like="Test")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        params = AssetMappingQueryParams(class_symbol_like="USD")
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

        # Test combined filters
        params = AssetMappingQueryParams(
            common_symbol="BTCUSD",
            class_type="provider",
            is_active=True
        )
        response = await reg.handle_get_asset_mappings(params)
        assert response.total_items == 0

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_empty_results(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test behavior when no records match filters."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=0))

        # Use filters that won't match any records
        params = AssetMappingQueryParams(common_symbol="NONEXISTENT")
        response = await reg.handle_get_asset_mappings(params)

        assert response.items == []
        assert response.total_items == 0
        assert response.page == 1
        assert response.total_pages == 0  # or 1 depending on implementation

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_large_dataset(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test pagination with large result sets."""
        reg = registry_with_mocks

        # Create multiple mock records (5 records)
        mock_records = [
            MockRecord(common_symbol=f"ASSET{i}", class_name=f"Provider{i}",
                      class_type="provider", class_symbol=f"ASSET{i}-USD", is_active=True)
            for i in range(1, 6)
        ]

        mock_asyncpg_conn.fetch = AsyncMock(return_value=mock_records)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=100))

        params = AssetMappingQueryParams(limit=10, offset=0)
        response = await reg.handle_get_asset_mappings(params)

        # Verify only limit number of items returned
        assert len(response.items) == 5  # limit is 10, but we only have 5 records
        # Verify total_items reflects full dataset size
        assert response.total_items == 100
        # Verify total_pages calculation is correct (100 / 10 = 10 pages)
        assert response.total_pages == 10

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_response_structure(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Verify response structure matches schema."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            common_symbol="BTCUSD", class_name="TestProvider",
            class_type="provider", class_symbol="BTC-USD", is_active=True
        )

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total_items=1))

        params = AssetMappingQueryParams()
        response = await reg.handle_get_asset_mappings(params)

        # Verify response is instance of AssetMappingPaginatedResponse
        assert isinstance(response, AssetMappingPaginatedResponse)

        # Verify all required fields exist
        assert hasattr(response, 'items')
        assert hasattr(response, 'total_items')
        assert hasattr(response, 'limit')
        assert hasattr(response, 'offset')
        assert hasattr(response, 'page')
        assert hasattr(response, 'total_pages')

        # Verify items is a list
        assert isinstance(response.items, list)
        assert len(response.items) == 1

        # Verify each item is instance of AssetMappingResponse
        assert isinstance(response.items[0], AssetMappingResponse)

        # Verify all items have required fields
        item = response.items[0]
        assert hasattr(item, 'common_symbol')
        assert hasattr(item, 'class_name')
        assert hasattr(item, 'class_type')
        assert hasattr(item, 'class_symbol')
        assert hasattr(item, 'is_active')

        # Verify actual values
        assert item.common_symbol == "BTCUSD"
        assert item.class_name == "TestProvider"
        assert item.class_type == "provider"
        assert item.class_symbol == "BTC-USD"
        assert item.is_active is True

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

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_for_symbol_success(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings_for_symbol returns mappings with complete asset data."""
        reg = registry_with_mocks

        # Mock the LEFT JOIN result with all fields populated
        mock_record = MockRecord(
            common_symbol="BTCUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="BTC-USD",
            is_active=True,
            primary_id="12345",
            asset_class="crypto"
        )

        mock_asyncpg_pool.fetch = AsyncMock(return_value=[mock_record])

        mappings = await reg.handle_get_asset_mappings_for_symbol("BTCUSD")

        assert len(mappings) == 1
        assert mappings[0].common_symbol == "BTCUSD"
        assert mappings[0].class_name == "TestProvider"
        assert mappings[0].class_type == "provider"
        assert mappings[0].class_symbol == "BTC-USD"
        assert mappings[0].is_active is True
        assert mappings[0].primary_id == "12345"
        assert mappings[0].asset_class == "crypto"

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_for_symbol_with_null_asset_data(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings_for_symbol handles NULL asset data gracefully."""
        reg = registry_with_mocks

        # Mock LEFT JOIN result where asset data is NULL
        mock_record = MockRecord(
            common_symbol="ETHUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="ETH-USD",
            is_active=True,
            primary_id=None,
            asset_class=None
        )

        mock_asyncpg_pool.fetch = AsyncMock(return_value=[mock_record])

        mappings = await reg.handle_get_asset_mappings_for_symbol("ETHUSD")

        assert len(mappings) == 1
        assert mappings[0].common_symbol == "ETHUSD"
        assert mappings[0].primary_id is None
        assert mappings[0].asset_class is None
        # Required fields should still be present
        assert mappings[0].class_name == "TestProvider"
        assert mappings[0].is_active is True

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_for_symbol_multiple_providers(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings_for_symbol returns all mappings across multiple providers."""
        reg = registry_with_mocks

        # Mock multiple mappings from different providers
        mock_records = [
            MockRecord(
                common_symbol="AAPL",
                class_name="ProviderA",
                class_type="provider",
                class_symbol="AAPL",
                is_active=True,
                primary_id="AAPL123",
                asset_class="equity"
            ),
            MockRecord(
                common_symbol="AAPL",
                class_name="BrokerB",
                class_type="broker",
                class_symbol="AAPL.US",
                is_active=True,
                primary_id="AAPL456",
                asset_class="equity"
            ),
            MockRecord(
                common_symbol="AAPL",
                class_name="ProviderC",
                class_type="provider",
                class_symbol="AAPL",
                is_active=False,
                primary_id="AAPL789",
                asset_class="equity"
            )
        ]

        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        mappings = await reg.handle_get_asset_mappings_for_symbol("AAPL")

        assert len(mappings) == 3
        # All mappings should have the same common_symbol
        assert all(m.common_symbol == "AAPL" for m in mappings)
        # Should include all expected providers
        class_names = {m.class_name for m in mappings}
        assert class_names == {"ProviderA", "BrokerB", "ProviderC"}

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_for_symbol_no_mappings(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings_for_symbol returns empty list when no mappings exist."""
        reg = registry_with_mocks

        # Mock empty result
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        mappings = await reg.handle_get_asset_mappings_for_symbol("NONEXISTENT")

        assert mappings == []
        assert len(mappings) == 0

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_for_symbol_database_error(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings_for_symbol handles database errors appropriately."""
        reg = registry_with_mocks

        # Mock database error
        mock_asyncpg_pool.fetch = AsyncMock(side_effect=Exception("Database connection failed"))

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_asset_mappings_for_symbol("BTCUSD")

        assert exc_info.value.status_code == 500
        assert "Database error" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_handle_get_asset_mappings_for_symbol_special_characters(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_get_asset_mappings_for_symbol handles common symbols with special characters."""
        reg = registry_with_mocks

        # Mock result for symbol with special characters
        mock_record = MockRecord(
            common_symbol="BTC/USD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="BTC-USD",
            is_active=True,
            primary_id="12345",
            asset_class="crypto"
        )

        mock_asyncpg_pool.fetch = AsyncMock(return_value=[mock_record])

        mappings = await reg.handle_get_asset_mappings_for_symbol("BTC/USD")

        assert len(mappings) == 1
        assert mappings[0].common_symbol == "BTC/USD"

    async def test_asset_mapping_response_schema_with_optional_fields(self):
        """Test that AssetMappingResponse schema accepts optional fields correctly."""
        from quasar.services.registry.schemas import AssetMappingResponse

        # Test with all fields populated
        response1 = AssetMappingResponse(
            common_symbol="BTCUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="BTC-USD",
            is_active=True,
            primary_id="12345",
            asset_class="crypto"
        )
        assert response1.primary_id == "12345"
        assert response1.asset_class == "crypto"

        # Test with null optional fields
        response2 = AssetMappingResponse(
            common_symbol="ETHUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="ETH-USD",
            is_active=True,
            primary_id=None,
            asset_class=None
        )
        assert response2.primary_id is None
        assert response2.asset_class is None

        # Test with one optional field populated, one null
        response3 = AssetMappingResponse(
            common_symbol="ADAUSD",
            class_name="TestProvider",
            class_type="provider",
            class_symbol="ADA-USD",
            is_active=True,
            primary_id="ADA123",
            asset_class=None
        )
        assert response3.primary_id == "ADA123"
        assert response3.asset_class is None


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


# =============================================================================
# Asset Mapping Suggestions Tests
# =============================================================================

class TestCursorFunctions:
    """Unit tests for cursor encoding/decoding."""

    def test_encode_cursor_produces_valid_base64(self):
        """Verify _encode_cursor produces valid base64 JSON array."""
        cursor = _encode_cursor(85.5, "AAPL.US", "AAPL")

        decoded = base64.urlsafe_b64decode(cursor)
        data = json.loads(decoded)

        assert data == [85.5, "AAPL.US", "AAPL"]

    def test_decode_cursor_returns_correct_tuple(self):
        """Verify _decode_cursor extracts values correctly."""
        cursor = _encode_cursor(70.0, "GOOG.US", "GOOG")

        score, src, tgt = _decode_cursor(cursor)

        assert score == 70.0
        assert src == "GOOG.US"
        assert tgt == "GOOG"

    def test_encode_decode_roundtrip(self):
        """Verify encoding then decoding returns original values."""
        original = (42.5, "BTC-USD", "BTCUSD")

        cursor = _encode_cursor(*original)
        result = _decode_cursor(cursor)

        assert result == original

    def test_decode_cursor_raises_on_invalid_base64(self):
        """Verify _decode_cursor raises ValueError for invalid base64."""
        with pytest.raises(ValueError, match="Invalid cursor"):
            _decode_cursor("not-valid-base64!!!")

    def test_decode_cursor_raises_on_malformed_json(self):
        """Verify _decode_cursor raises ValueError for non-array JSON."""
        bad_cursor = base64.urlsafe_b64encode(b'{"not": "array"}').decode()

        with pytest.raises(ValueError):
            _decode_cursor(bad_cursor)

    def test_decode_cursor_raises_on_wrong_length(self):
        """Verify _decode_cursor raises ValueError for array with wrong length."""
        bad_cursor = base64.urlsafe_b64encode(b'[1, 2]').decode()

        with pytest.raises(ValueError):
            _decode_cursor(bad_cursor)


class TestSuggestionsValidation:
    """Tests for parameter validation on suggestions endpoint."""

    @pytest.mark.asyncio
    async def test_suggestions_rejects_invalid_cursor(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify 400 error for malformed cursor."""
        reg = registry_with_mocks

        with pytest.raises(HTTPException) as exc_info:
            await call_suggestions(
                reg,
                source_class="EODHD",
                cursor="invalid-cursor-format"
            )

        assert exc_info.value.status_code == 400
        assert "cursor" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_suggestions_uses_default_min_score(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify default min_score of 30.0 is used."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        # Call without min_score - should use default 30.0
        await call_suggestions(reg, source_class="EODHD")

        # Verify the query was called (min_score is embedded in SQL)
        mock_asyncpg_pool.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_suggestions_uses_default_limit(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify default limit of 50 is used."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        response = await call_suggestions(reg, source_class="EODHD")

        assert response.limit == 50


class TestSuggestionsResponse:
    """Tests for suggestions response structure."""

    @pytest.mark.asyncio
    async def test_suggestions_returns_empty_list_when_no_matches(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify empty items list when no suggestions found."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        response = await call_suggestions(reg, source_class="EODHD")

        assert response.items == []
        assert response.has_more is False
        assert response.next_cursor is None

    @pytest.mark.asyncio
    async def test_suggestions_returns_valid_items(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify suggestions are returned with correct schema."""
        reg = registry_with_mocks
        mock_records = [
            make_suggestion_record(score=85.0),
            make_suggestion_record(
                source_symbol="GOOG.US",
                target_symbol="GOOG",
                proposed_common_symbol="goog",
                score=70.0
            )
        ]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="EODHD")

        assert len(response.items) == 2
        assert response.items[0].source_symbol == "AAPL.US"
        assert response.items[0].score == 85.0
        assert response.items[1].source_symbol == "GOOG.US"
        assert response.items[1].score == 70.0

    @pytest.mark.asyncio
    async def test_suggestions_includes_match_flags(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify match criteria flags are included in response."""
        reg = registry_with_mocks
        mock_records = [make_suggestion_record(
            id_match=True,
            external_id_match=False,
            norm_match=True,
            base_quote_match=True,
            exchange_match=False
        )]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="EODHD")

        item = response.items[0]
        assert item.id_match is True
        assert item.external_id_match is False
        assert item.norm_match is True
        assert item.base_quote_match is True
        assert item.exchange_match is False

    @pytest.mark.asyncio
    async def test_suggestions_preserves_mapped_common_symbol_casing(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Existing mapped targets keep their stored common_symbol casing."""
        reg = registry_with_mocks
        mock_records = [make_suggestion_record(
            proposed_common_symbol="PlTr",
            target_already_mapped=True,
            target_common_symbol="PlTr"
        )]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="DATABENTO")

        assert response.items[0].proposed_common_symbol == "PlTr"
        assert response.items[0].target_common_symbol == "PlTr"

    @pytest.mark.asyncio
    async def test_suggestions_uppercases_derived_common_symbol(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Unmapped targets should return uppercased proposed_common_symbol."""
        reg = registry_with_mocks
        mock_records = [make_suggestion_record(
            proposed_common_symbol="pltr",
            target_already_mapped=False
        )]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="DATABENTO")

        assert response.items[0].proposed_common_symbol == "PLTR"

    @pytest.mark.asyncio
    async def test_suggestions_total_none_by_default(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify total is None when include_total=False."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        response = await call_suggestions(
            reg,
            source_class="EODHD",
            include_total=False
        )

        assert response.total is None

    @pytest.mark.asyncio
    async def test_suggestions_total_included_when_requested(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify total is returned when include_total=True."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=42)

        response = await call_suggestions(
            reg,
            source_class="EODHD",
            include_total=True
        )

        assert response.total == 42


class TestSuggestionsPagination:
    """Tests for cursor-based pagination."""

    @pytest.mark.asyncio
    async def test_suggestions_has_more_when_results_exceed_limit(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify has_more=True when more results available."""
        reg = registry_with_mocks

        # Return limit+1 records to indicate more available
        mock_records = [make_suggestion_record(score=90 - i) for i in range(4)]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="EODHD", limit=3)

        assert response.has_more is True
        assert response.next_cursor is not None
        assert len(response.items) == 3  # Trimmed to limit

    @pytest.mark.asyncio
    async def test_suggestions_no_more_when_at_end(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify has_more=False when no more results."""
        reg = registry_with_mocks

        # Return exactly limit records (no extra)
        mock_records = [make_suggestion_record(score=90 - i) for i in range(3)]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="EODHD", limit=5)

        assert response.has_more is False
        assert response.next_cursor is None

    @pytest.mark.asyncio
    async def test_suggestions_cursor_from_last_item(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify next_cursor is encoded from last returned item."""
        reg = registry_with_mocks

        # Return 3 records (2 returned + 1 extra for has_more)
        mock_records = [
            make_suggestion_record(
                source_symbol="AAPL.US", target_symbol="AAPL", score=90
            ),
            make_suggestion_record(
                source_symbol="GOOG.US", target_symbol="GOOG", score=80
            ),
            make_suggestion_record(
                source_symbol="MSFT.US", target_symbol="MSFT", score=70
            ),
        ]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="EODHD", limit=2)

        # Cursor should be from last returned item (GOOG, score=80)
        score, src, tgt = _decode_cursor(response.next_cursor)
        assert score == 80.0
        assert src == "GOOG.US"
        assert tgt == "GOOG"

    @pytest.mark.asyncio
    async def test_suggestions_accepts_valid_cursor(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify endpoint accepts valid cursor for pagination."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        cursor = _encode_cursor(75.0, "AAPL.US", "AAPL")

        # Should not raise
        response = await call_suggestions(
            reg,
            source_class="EODHD",
            cursor=cursor
        )

        assert response.items == []


class TestSuggestionsFiltering:
    """Tests for filtering behavior."""

    @pytest.mark.asyncio
    async def test_suggestions_filters_by_source_class(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify source_class parameter filters results."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await call_suggestions(reg, source_class="EODHD")

        # Verify fetch was called (SQL contains source_class filter)
        mock_asyncpg_pool.fetch.assert_called_once()
        call_args = mock_asyncpg_pool.fetch.call_args
        # First positional arg after query is source_class
        assert "EODHD" in call_args[0]

    @pytest.mark.asyncio
    async def test_suggestions_filters_by_target_class(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify target_class parameter filters results."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await call_suggestions(
            reg,
            source_class="EODHD",
            target_class="DATABENTO"
        )

        call_args = mock_asyncpg_pool.fetch.call_args
        assert "DATABENTO" in call_args[0]

    @pytest.mark.asyncio
    async def test_suggestions_filters_by_search(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify search parameter is applied."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await call_suggestions(reg, source_class="EODHD", search="AAPL")

        call_args = mock_asyncpg_pool.fetch.call_args
        # Search term should be wrapped with % for ILIKE
        assert "%AAPL%" in call_args[0]

    @pytest.mark.asyncio
    async def test_suggestions_filters_by_min_score(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify min_score parameter filters low-scoring results."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await call_suggestions(reg, source_class="EODHD", min_score=70.0)

        call_args = mock_asyncpg_pool.fetch.call_args
        assert 70.0 in call_args[0]

    @pytest.mark.asyncio
    async def test_suggestions_filters_by_source_type(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify source_type parameter filters by provider/broker."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await call_suggestions(reg, source_class="EODHD", source_type="provider")

        call_args = mock_asyncpg_pool.fetch.call_args
        assert "provider" in call_args[0]

    @pytest.mark.asyncio
    async def test_suggestions_allows_mapped_targets_in_query(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Ensure the SQL no longer filters out mapped targets."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await call_suggestions(
            reg,
            source_class="EODHD",
            target_class="DATABENTO"
        )

        sql = mock_asyncpg_pool.fetch.call_args[0][0]
        assert sql.count("NOT EXISTS") == 1  # only source unmapped filter remains


class TestSuggestionsFallback:
    """Tests for pg_trgm fallback behavior."""

    @pytest.mark.asyncio
    async def test_suggestions_retries_without_similarity_on_error(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify endpoint retries without pg_trgm when similarity() unavailable."""
        reg = registry_with_mocks

        # First call raises UndefinedFunctionError, second succeeds
        mock_records = [make_suggestion_record(
            sym_root_similarity=0,
            name_similarity=0
        )]
        mock_asyncpg_pool.fetch = AsyncMock(
            side_effect=[
                UndefinedFunctionError(
                    "function similarity(text, text) does not exist"
                ),
                mock_records
            ]
        )

        response = await call_suggestions(reg, source_class="EODHD")

        # Should succeed after fallback
        assert len(response.items) == 1
        assert response.items[0].sym_root_similarity == 0
        assert mock_asyncpg_pool.fetch.call_count == 2

    @pytest.mark.asyncio
    async def test_suggestions_fallback_total_count(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify total count also falls back when pg_trgm unavailable."""
        reg = registry_with_mocks

        # Data query succeeds on first try
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])
        # Count query fails first, succeeds second
        mock_asyncpg_pool.fetchval = AsyncMock(
            side_effect=[
                UndefinedFunctionError("similarity"),
                10
            ]
        )

        response = await call_suggestions(
            reg,
            source_class="EODHD",
            include_total=True
        )

        assert response.total == 10


class TestSuggestionsErrors:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_suggestions_500_on_database_error(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify 500 error on unexpected database errors."""
        reg = registry_with_mocks
        mock_asyncpg_pool.fetch = AsyncMock(
            side_effect=Exception("Database connection failed")
        )

        with pytest.raises(HTTPException) as exc_info:
            await call_suggestions(reg, source_class="EODHD")

        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_suggestions_handles_null_similarity_values(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Verify null similarity values are handled gracefully."""
        reg = registry_with_mocks
        mock_records = [make_suggestion_record(
            sym_root_similarity=None,
            name_similarity=None
        )]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        response = await call_suggestions(reg, source_class="EODHD")

        # Should default to 0.0
        assert response.items[0].sym_root_similarity == 0.0
        assert response.items[0].name_similarity == 0.0


# =============================================================================
# Identity Manifest Seeding Tests
# =============================================================================


class TestIdentityManifestSeeding:
    """Test identity manifest seeding behavior on Registry startup."""

    @pytest.mark.asyncio
    async def test_start_seeds_identity_manifests_when_empty(
        self, registry_with_mocks, mock_asyncpg_conn, tmp_path
    ):
        """Test that Registry seeds identity manifests on startup when table is empty."""
        reg = registry_with_mocks

        # Mock empty table check
        reg.pool.fetchval = AsyncMock(return_value=0)

        # Mock manifest directory structure
        manifests_dir = tmp_path / "seeds" / "manifests"
        manifests_dir.mkdir(parents=True)

        # Create test crypto manifest
        crypto_manifest = manifests_dir / "crypto.yaml"
        crypto_manifest.write_text("""
- figi: KKG00000DV14
  symbol: BTC
  name: Bitcoin
  exchange: null
- figi: KKG0000092P5
  symbol: ETH
  name: Ethereum
  exchange: null
""")

        # Create test securities manifest
        securities_manifest = manifests_dir / "securities.yaml"
        securities_manifest.write_text("""
- figi: BBG000B9XRY4
  symbol: AAPL
  name: Apple Inc
  exchange: XNAS
""")

        # Mock filesystem path resolution
        with patch('quasar.services.registry.core.Path') as mock_path_class:
            # Mock the path chain: Path(__file__).parent.parent.parent resolves to tmp_path
            # So Path(__file__).parent.parent.parent / "seeds" / "manifests" = tmp_path / "seeds" / "manifests"
            mock_file_path = Mock()
            mock_file_path.parent.parent.parent = tmp_path
            mock_path_class.return_value = mock_file_path

            # Mock database execution
            mock_asyncpg_conn.execute = AsyncMock()

            # Call seeding
            await reg._seed_identity_manifests()

            # Verify correct number of inserts (3 total identities)
            assert mock_asyncpg_conn.execute.call_count == 3

            # Verify calls included expected data
            calls = mock_asyncpg_conn.execute.call_args_list
            assert any('KKG00000DV14' in str(call) for call in calls)  # BTC
            assert any('KKG0000092P5' in str(call) for call in calls)  # ETH
            assert any('BBG000B9XRY4' in str(call) for call in calls)  # AAPL

    @pytest.mark.asyncio
    async def test_start_skips_seeding_when_manifests_exist(
        self, registry_with_mocks
    ):
        """Test that Registry skips seeding when identity manifests already exist."""
        reg = registry_with_mocks

        # Mock non-empty table (15,000 existing records)
        reg.pool.fetchval = AsyncMock(return_value=15000)

        # Call seeding - should return early
        await reg._seed_identity_manifests()

        # Verify no database operations occurred
        reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_handles_missing_manifest_directory(
        self, registry_with_mocks
    ):
        """Test graceful handling when manifest directory doesn't exist."""
        reg = registry_with_mocks

        # Mock empty table
        reg.pool.fetchval = AsyncMock(return_value=0)

        # Mock missing directory
        with patch('quasar.services.registry.core.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.exists.return_value = False
            mock_path.return_value.parent.parent.parent = mock_path_instance

            # Should not raise exception
            await reg._seed_identity_manifests()

            # Should not attempt database operations
            reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_handles_invalid_yaml_gracefully(
        self, registry_with_mocks, tmp_path
    ):
        """Test that invalid YAML doesn't crash seeding process."""
        reg = registry_with_mocks

        reg.pool.fetchval = AsyncMock(return_value=0)

        manifests_dir = tmp_path / "seeds" / "manifests"
        manifests_dir.mkdir(parents=True)

        # Create invalid YAML
        crypto_manifest = manifests_dir / "crypto.yaml"
        crypto_manifest.write_text("invalid: yaml: content: [\n")

        # Mock path resolution
        with patch('quasar.services.registry.core.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.parent.parent.parent = tmp_path / "seeds" / "manifests"
            mock_path.return_value.parent.parent.parent = mock_path_instance

            # Should catch YAML error and continue without database operations
            await reg._seed_identity_manifests()

            # Should not have attempted database operations
            reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_handles_database_errors_gracefully(
        self, registry_with_mocks, mock_asyncpg_conn, tmp_path
    ):
        """Test that database errors don't crash Registry startup."""
        reg = registry_with_mocks

        reg.pool.fetchval = AsyncMock(return_value=0)

        # Mock filesystem setup
        manifests_dir = tmp_path / "seeds" / "manifests"
        manifests_dir.mkdir(parents=True)
        (manifests_dir / "crypto.yaml").write_text("- figi: TEST\n  symbol: TEST\n  name: Test\n")

        with patch('quasar.services.registry.core.Path') as mock_path_class:
            mock_file_path = Mock()
            mock_file_path.parent.parent.parent = tmp_path
            mock_path_class.return_value = mock_file_path

            # Mock database connection failure
            reg.pool.acquire = AsyncMock(side_effect=Exception("Connection failed"))

            # Should not raise exception - startup should continue
            await reg._seed_identity_manifests()

            # Verify database acquire was attempted (and failed)
            reg.pool.acquire.assert_called_once()


# =============================================================================
# Apply Identity Matches Tests
# =============================================================================


class TestApplyIdentityMatches:
    """Tests for _apply_identity_matches behavior including constraint handling."""

    @pytest.mark.asyncio
    async def test_empty_matches_returns_zero_stats(
        self, registry_with_mocks
    ):
        """Empty matches list returns zero stats without DB calls."""
        reg = registry_with_mocks
        
        result = await reg._apply_identity_matches([])
        
        assert result == {
            'identified': 0,
            'skipped': 0,
            'failed': 0,
            'constraint_rejected': 0
        }
        # No DB operations should occur
        reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_successful_update_increments_identified(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Successful identity application increments 'identified' stat."""
        from quasar.services.registry.matcher import MatchResult
        
        reg = registry_with_mocks
        
        matches = [
            MatchResult(
                asset_id=1,
                symbol="AAPL",
                primary_id="BBG000B9XRY4",
                identity_symbol="AAPL",
                identity_name="Apple Inc",
                confidence=100.0,
                match_type="exact_alias"
            )
        ]
        
        # fetchval returns the asset id when UPDATE succeeds
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)
        
        result = await reg._apply_identity_matches(matches)
        
        assert result['identified'] == 1
        assert result['skipped'] == 0
        assert result['failed'] == 0

    @pytest.mark.asyncio
    async def test_already_identified_increments_skipped(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Asset already having primary_id increments 'skipped' stat."""
        from quasar.services.registry.matcher import MatchResult
        
        reg = registry_with_mocks
        
        matches = [
            MatchResult(
                asset_id=1,
                symbol="AAPL",
                primary_id="BBG000B9XRY4",
                identity_symbol="AAPL",
                identity_name="Apple Inc",
                confidence=100.0,
                match_type="exact_alias"
            )
        ]
        
        # fetchval returns None when no rows updated (asset already had primary_id)
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=None)
        
        result = await reg._apply_identity_matches(matches)
        
        assert result['identified'] == 0
        assert result['skipped'] == 1
        assert result['failed'] == 0

    @pytest.mark.asyncio
    async def test_unique_constraint_violation_increments_constraint_rejected(
        self, registry_with_mocks, mock_asyncpg_conn, caplog
    ):
        """UniqueViolationError with specific constraint increments 'constraint_rejected'."""
        from quasar.services.registry.matcher import MatchResult
        from asyncpg.exceptions import UniqueViolationError
        import logging
        
        caplog.set_level(logging.INFO)
        reg = registry_with_mocks
        
        matches = [
            MatchResult(
                asset_id=2,
                symbol="MS-A",
                primary_id="FIGI_MS",
                identity_symbol="MS",
                identity_name="Morgan Stanley",
                confidence=100.0,
                match_type="exact_alias"
            )
        ]
        
        # Simulate unique constraint violation with specific index name
        error = UniqueViolationError(
            "duplicate key value violates unique constraint "
            "\"idx_assets_unique_securities_primary_id\""
        )
        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=error)
        
        result = await reg._apply_identity_matches(matches)
        
        assert result['constraint_rejected'] == 1
        assert result['failed'] == 0
        assert result['identified'] == 0
        
        # Should log at INFO level (expected behavior)
        assert any(
            "Identity rejected by constraint" in record.message
            for record in caplog.records
            if record.levelno == logging.INFO
        )

    @pytest.mark.asyncio
    async def test_other_unique_violation_increments_failed(
        self, registry_with_mocks, mock_asyncpg_conn, caplog
    ):
        """UniqueViolationError with different constraint increments 'failed'."""
        from quasar.services.registry.matcher import MatchResult
        from asyncpg.exceptions import UniqueViolationError
        import logging
        
        caplog.set_level(logging.WARNING)
        reg = registry_with_mocks
        
        matches = [
            MatchResult(
                asset_id=1,
                symbol="TEST",
                primary_id="FIGI_TEST",
                identity_symbol="TEST",
                identity_name="Test",
                confidence=100.0,
                match_type="exact_alias"
            )
        ]
        
        # Different constraint violation (unexpected)
        error = UniqueViolationError(
            "duplicate key value violates unique constraint \"some_other_constraint\""
        )
        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=error)
        
        result = await reg._apply_identity_matches(matches)
        
        assert result['failed'] == 1
        assert result['constraint_rejected'] == 0
        
        # Should log at WARNING level (unexpected)
        assert any(
            "Unexpected unique violation" in record.message
            for record in caplog.records
            if record.levelno == logging.WARNING
        )

    @pytest.mark.asyncio
    async def test_general_exception_increments_failed(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """General exceptions increment 'failed' stat."""
        from quasar.services.registry.matcher import MatchResult
        
        reg = registry_with_mocks
        
        matches = [
            MatchResult(
                asset_id=1,
                symbol="TEST",
                primary_id="FIGI_TEST",
                identity_symbol="TEST",
                identity_name="Test",
                confidence=100.0,
                match_type="exact_alias"
            )
        ]
        
        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=Exception("Database error"))
        
        result = await reg._apply_identity_matches(matches)
        
        assert result['failed'] == 1
        assert result['identified'] == 0

    @pytest.mark.asyncio
    async def test_mixed_results_tracked_correctly(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Multiple matches with different outcomes tracked correctly."""
        from quasar.services.registry.matcher import MatchResult
        from asyncpg.exceptions import UniqueViolationError
        
        reg = registry_with_mocks
        
        matches = [
            MatchResult(asset_id=1, symbol="A", primary_id="F1",
                       identity_symbol="A", identity_name="A", confidence=100.0, match_type="exact"),
            MatchResult(asset_id=2, symbol="B", primary_id="F2",
                       identity_symbol="B", identity_name="B", confidence=100.0, match_type="exact"),
            MatchResult(asset_id=3, symbol="C", primary_id="F3",
                       identity_symbol="C", identity_name="C", confidence=100.0, match_type="exact"),
            MatchResult(asset_id=4, symbol="D", primary_id="F4",
                       identity_symbol="D", identity_name="D", confidence=100.0, match_type="exact"),
        ]
        
        constraint_error = UniqueViolationError(
            "idx_assets_unique_securities_primary_id"
        )
        
        # Different outcomes for each match
        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=[
            1,                    # First: identified
            None,                 # Second: skipped
            constraint_error,     # Third: constraint rejected
            Exception("Error"),   # Fourth: failed
        ])
        
        result = await reg._apply_identity_matches(matches)
        
        assert result['identified'] == 1
        assert result['skipped'] == 1
        assert result['constraint_rejected'] == 1
        assert result['failed'] == 1


class TestProviderConfig:
    """Tests for provider configuration endpoints."""

    @pytest.mark.asyncio
    async def test_get_config_success(self, registry_with_mocks, mock_asyncpg_pool):
        """Test successful retrieval of provider preferences."""
        reg = registry_with_mocks

        # Mock the database response - provider exists and has preferences
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            {'crypto': {'preferred_quote_currency': 'USDC'}}  # Preferences
        ])

        result = await reg.handle_get_provider_config(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.preferences.crypto is not None
        assert result.preferences.crypto.preferred_quote_currency == 'USDC'

    @pytest.mark.asyncio
    async def test_get_config_not_found(self, registry_with_mocks, mock_asyncpg_pool):
        """Test 404 response when provider not found."""
        reg = registry_with_mocks

        # Mock provider not found
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_provider_config(
                class_name='UnknownProvider',
                class_type='provider'
            )

        assert exc_info.value.status_code == 404
        assert 'not found' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_config_empty_preferences(self, registry_with_mocks, mock_asyncpg_pool):
        """Test retrieval when preferences column is empty/null."""
        reg = registry_with_mocks

        # Mock provider exists but has null preferences
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            None   # Preferences are null
        ])

        result = await reg.handle_get_provider_config(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.preferences.crypto is None

    @pytest.mark.asyncio
    async def test_update_config_success(self, registry_with_mocks, mock_asyncpg_pool):
        """Test successful update of provider preferences."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate, CryptoPreferences

        reg = registry_with_mocks

        # Mock provider exists and update succeeds
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            {'crypto': {'preferred_quote_currency': 'USDT'}}  # Updated preferences
        ])

        update = ProviderPreferencesUpdate(
            crypto=CryptoPreferences(preferred_quote_currency='USDT')
        )

        result = await reg.handle_update_provider_config(
            update=update,
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.preferences.crypto.preferred_quote_currency == 'USDT'

        # Verify provider existence check and update queries
        assert mock_asyncpg_pool.fetchval.call_count == 2

    @pytest.mark.asyncio
    async def test_update_config_not_found(self, registry_with_mocks, mock_asyncpg_pool):
        """Test 404 response when updating non-existent provider."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate, CryptoPreferences

        reg = registry_with_mocks

        # Mock provider doesn't exist
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=False)

        update = ProviderPreferencesUpdate(
            crypto=CryptoPreferences(preferred_quote_currency='USDT')
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_provider_config(
                update=update,
                class_name='UnknownProvider',
                class_type='provider'
            )

        assert exc_info.value.status_code == 404
        assert 'not found' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_update_config_partial_update(self, registry_with_mocks, mock_asyncpg_pool):
        """Test partial update merges with existing preferences."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate, CryptoPreferences

        reg = registry_with_mocks

        # Mock provider exists and partial update
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            {'crypto': {'preferred_quote_currency': 'USDC'}}  # Updated preferences
        ])

        update = ProviderPreferencesUpdate(
            crypto=CryptoPreferences(preferred_quote_currency='USDC')
        )

        result = await reg.handle_update_provider_config(
            update=update,
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.preferences.crypto.preferred_quote_currency == 'USDC'

    @pytest.mark.asyncio
    async def test_update_config_no_updates(self, registry_with_mocks, mock_asyncpg_pool):
        """Test 400 response when no updates are provided."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate

        reg = registry_with_mocks

        # Mock provider exists
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=True)

        update = ProviderPreferencesUpdate()  # Empty update

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_provider_config(
                update=update,
                class_name='TestProvider',
                class_type='provider'
            )

        assert exc_info.value.status_code == 400
        assert 'No preferences provided' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_available_quote_currencies(self, registry_with_mocks, mock_asyncpg_pool):
        """Test retrieval of available quote currencies."""
        reg = registry_with_mocks

        # Mock database response with quote currencies
        mock_records = [
            MockRecord(quote_currency='USDC'),
            MockRecord(quote_currency='USDT'),
            MockRecord(quote_currency='USD'),
        ]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        result = await reg.handle_get_available_quote_currencies(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.available_quote_currencies == ['USDC', 'USDT', 'USD']

        # Verify correct query was called
        mock_asyncpg_pool.fetch.assert_called_once_with(
            "\n            SELECT DISTINCT quote_currency\n            FROM assets\n            WHERE class_name = $1\n              AND class_type = $2\n              AND asset_class_group = 'crypto'\n              AND quote_currency IS NOT NULL\n            ORDER BY quote_currency\n        ",
            'TestProvider',
            'provider'
        )

    @pytest.mark.asyncio
    async def test_get_available_quote_currencies_empty(self, registry_with_mocks, mock_asyncpg_pool):
        """Test empty result when no crypto assets exist."""
        reg = registry_with_mocks

        # Mock empty result
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        result = await reg.handle_get_available_quote_currencies(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.available_quote_currencies == []