"""Tests for asset mapping handlers."""

import pytest
from unittest.mock import AsyncMock

from fastapi import HTTPException
from asyncpg.exceptions import UndefinedFunctionError

from quasar.services.registry.core import _encode_cursor, _decode_cursor
from quasar.services.registry.schemas import (
    AssetMappingCreate, AssetMappingUpdate, AssetMappingQueryParams,
    AssetMappingPaginatedResponse, AssetMappingResponse,
    CommonSymbolRenameRequest, CommonSymbolRenameResponse,
)
from .conftest import MockRecord


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
        mock_asyncpg_conn.fetch = AsyncMock(return_value=asc_sorted_records)
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


class TestRenameCommonSymbol:
    """Tests for common symbol rename endpoint."""

    @pytest.mark.asyncio
    async def test_rename_common_symbol_success(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test successful rename of a common symbol."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # Mock the sequence of fetchval/fetchrow calls
        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=[
            1,    # old_symbol exists
            None, # new_symbol doesn't exist
            5,    # asset_mapping count
            2,    # index_memberships count
        ])
        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(symbol="BITCOIN")
        )

        request = CommonSymbolRenameRequest(new_symbol="BITCOIN")
        response = await reg.handle_rename_common_symbol("BTC", request)

        assert isinstance(response, CommonSymbolRenameResponse)
        assert response.old_symbol == "BTC"
        assert response.new_symbol == "BITCOIN"
        assert response.asset_mappings_updated == 5
        assert response.index_memberships_updated == 2

    @pytest.mark.asyncio
    async def test_rename_common_symbol_not_found(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test 404 when symbol doesn't exist."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # old_symbol does not exist
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=None)

        request = CommonSymbolRenameRequest(new_symbol="BITCOIN")

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_rename_common_symbol("NONEXISTENT", request)

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_rename_common_symbol_conflict(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test 409 when new_symbol already exists."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # Both symbols exist
        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=[
            1,  # old_symbol exists
            1,  # new_symbol also exists (conflict)
        ])

        request = CommonSymbolRenameRequest(new_symbol="ETH")

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_rename_common_symbol("BTC", request)

        assert exc_info.value.status_code == 409
        assert "already exists" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_rename_common_symbol_empty_new_symbol(
        self, registry_with_mocks
    ):
        """Test 400 when new_symbol is empty or whitespace."""
        reg = registry_with_mocks

        request = CommonSymbolRenameRequest(new_symbol="   ")

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_rename_common_symbol("BTC", request)

        assert exc_info.value.status_code == 400
        assert "non-empty" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_rename_common_symbol_same_name(
        self, registry_with_mocks
    ):
        """Test 400 when new_symbol equals old_symbol."""
        reg = registry_with_mocks

        request = CommonSymbolRenameRequest(new_symbol="BTC")

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_rename_common_symbol("BTC", request)

        assert exc_info.value.status_code == 400
        assert "different" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_rename_common_symbol_database_error(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test 500 on unexpected database error."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        mock_asyncpg_conn.fetchval = AsyncMock(
            side_effect=Exception("Database connection failed")
        )

        request = CommonSymbolRenameRequest(new_symbol="BITCOIN")

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_rename_common_symbol("BTC", request)

        assert exc_info.value.status_code == 500

    @pytest.mark.asyncio
    async def test_rename_common_symbol_zero_affected_rows(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test rename succeeds even when no mappings or memberships exist."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=[
            1,    # old_symbol exists
            None, # new_symbol doesn't exist
            0,    # zero asset_mappings
            0,    # zero index_memberships
        ])
        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(symbol="NEW_SYMBOL")
        )

        request = CommonSymbolRenameRequest(new_symbol="NEW_SYMBOL")
        response = await reg.handle_rename_common_symbol("OLD_SYMBOL", request)

        assert response.asset_mappings_updated == 0
        assert response.index_memberships_updated == 0

    @pytest.mark.asyncio
    async def test_rename_common_symbol_whitespace_trimmed(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that whitespace is trimmed from new_symbol."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        mock_asyncpg_conn.fetchval = AsyncMock(side_effect=[
            1,    # old_symbol exists
            None, # new_symbol doesn't exist
            1,    # asset_mapping count
            0,    # index_memberships count
        ])
        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(symbol="BITCOIN")
        )

        # Request with leading/trailing whitespace
        request = CommonSymbolRenameRequest(new_symbol="  BITCOIN  ")
        response = await reg.handle_rename_common_symbol("BTC", request)

        # Verify trimmed value is used
        assert response.new_symbol == "BITCOIN"

    @pytest.mark.asyncio
    async def test_rename_common_symbol_transaction_rollback_on_404(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Verify transaction context manager exits properly on 404 error."""
        reg = registry_with_mocks

        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # old_symbol does not exist
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=None)

        request = CommonSymbolRenameRequest(new_symbol="BITCOIN")

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_rename_common_symbol("NONEXISTENT", request)

        assert exc_info.value.status_code == 404
        # Verify transaction context manager was entered and exited
        txn.__aenter__.assert_awaited_once()
        txn.__aexit__.assert_awaited_once()


class TestRemapPreview:
    """Tests for handle_remap_preview endpoint."""

    @pytest.mark.asyncio
    async def test_remap_preview_single_provider(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test preview with single provider filter returns expected counts."""
        reg = registry_with_mocks

        # Mock count query result (12 mappings for KRAKEN provider)
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=12)

        # Mock providers_affected query result
        provider_records = [MockRecord(class_name="KRAKEN")]
        # Mock affected indices query result (empty)
        index_records = []

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            provider_records,  # First fetch call: providers query
            index_records,     # Second fetch call: affected indices query
        ])

        response = await reg.handle_remap_preview(
            class_name="KRAKEN",
            class_type="provider",
            asset_class=None
        )

        assert response.mappings_to_delete == 12
        assert response.providers_affected == ["KRAKEN"]
        assert response.affected_indices == []
        assert response.filter_applied == {"class_name": "KRAKEN", "class_type": "provider"}

    @pytest.mark.asyncio
    async def test_remap_preview_asset_class_filter(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test preview with asset_class filter returns filtered results."""
        reg = registry_with_mocks

        # Mock count query result (47 crypto mappings)
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=47)

        # Mock providers_affected query result (multiple providers)
        # The query has ORDER BY class_name, so return in sorted order
        provider_records = [
            MockRecord(class_name="EODHD"),
            MockRecord(class_name="KRAKEN"),
        ]
        # Mock affected indices (none)
        index_records = []

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            provider_records,
            index_records,
        ])

        response = await reg.handle_remap_preview(
            class_name=None,
            class_type=None,
            asset_class="crypto"
        )

        assert response.mappings_to_delete == 47
        assert response.providers_affected == ["EODHD", "KRAKEN"]  # Sorted by query
        assert response.affected_indices == []
        assert response.filter_applied == {"asset_class": "crypto"}

    @pytest.mark.asyncio
    async def test_remap_preview_affected_indices(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test preview identifies user indices that would be affected."""
        reg = registry_with_mocks

        # Mock count query result
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=25)

        # Mock providers_affected query result
        provider_records = [MockRecord(class_name="KRAKEN")]
        # Mock affected indices query result (2 indices would be affected)
        index_records = [
            MockRecord(index_class_name="MyPortfolio"),
            MockRecord(index_class_name="CryptoBasket"),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            provider_records,
            index_records,
        ])

        response = await reg.handle_remap_preview(
            class_name="KRAKEN",
            class_type="provider",
            asset_class="crypto"
        )

        assert response.mappings_to_delete == 25
        assert response.providers_affected == ["KRAKEN"]
        assert response.affected_indices == ["MyPortfolio", "CryptoBasket"]
        assert response.filter_applied == {
            "class_name": "KRAKEN",
            "class_type": "provider",
            "asset_class": "crypto"
        }

    @pytest.mark.asyncio
    async def test_remap_preview_class_name_without_class_type_raises_400(
        self, registry_with_mocks
    ):
        """Test 400 error when class_name is provided without class_type."""
        reg = registry_with_mocks

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_remap_preview(
                class_name="KRAKEN",
                class_type=None,
                asset_class=None
            )

        assert exc_info.value.status_code == 400
        assert "class_type is required" in exc_info.value.detail


class TestRemapExecute:
    """Tests for handle_remap_assets endpoint."""

    @pytest.mark.asyncio
    async def test_remap_execute_single_provider(
        self, registry_with_mocks, mock_asyncpg_conn, mock_asyncpg_pool
    ):
        """Test remap execution deletes and regenerates mappings for single provider."""
        reg = registry_with_mocks

        # Setup transaction mock
        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # Mock affected indices query (empty)
        index_records = []

        # Mock deleted rows (5 mappings deleted)
        deleted_rows = [
            MockRecord(common_symbol="BTCUSD", class_name="KRAKEN", class_type="provider", class_symbol="BTC/USD"),
            MockRecord(common_symbol="ETHUSD", class_name="KRAKEN", class_type="provider", class_symbol="ETH/USD"),
            MockRecord(common_symbol="XRPUSD", class_name="KRAKEN", class_type="provider", class_symbol="XRP/USD"),
            MockRecord(common_symbol="ADAUSD", class_name="KRAKEN", class_type="provider", class_symbol="ADA/USD"),
            MockRecord(common_symbol="DOTUSD", class_name="KRAKEN", class_type="provider", class_symbol="DOT/USD"),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            index_records,   # affected indices query
            deleted_rows,    # DELETE ... RETURNING query
        ])

        # Mock AutomatedMapper.generate_mapping_candidates_for_provider
        from unittest.mock import patch
        from quasar.services.registry.mapper import MappingCandidate

        mock_candidates = [
            MappingCandidate(
                common_symbol="BTCUSD", class_name="KRAKEN",
                class_type="provider", class_symbol="BTC/USD",
                primary_id="BTC", asset_class_group="crypto",
                reasoning="Symbol match"
            ),
            MappingCandidate(
                common_symbol="ETHUSD", class_name="KRAKEN",
                class_type="provider", class_symbol="ETH/USD",
                primary_id="ETH", asset_class_group="crypto",
                reasoning="Symbol match"
            ),
        ]

        # Mock _apply_automated_mappings return
        with patch.object(
            reg, '_apply_automated_mappings',
            AsyncMock(return_value={'created': 2, 'skipped': 0, 'failed': 0})
        ):
            with patch(
                'quasar.services.registry.handlers.mappings.AutomatedMapper'
            ) as MockMapper:
                mock_mapper_instance = AsyncMock()
                mock_mapper_instance.generate_mapping_candidates_for_provider = AsyncMock(
                    return_value=mock_candidates
                )
                MockMapper.return_value = mock_mapper_instance

                from quasar.services.registry.schemas import AssetMappingRemapRequest
                request = AssetMappingRemapRequest(
                    class_name="KRAKEN",
                    class_type="provider",
                    asset_class=None
                )

                response = await reg.handle_remap_assets(request)

        assert response.status == "success"
        assert response.deleted_mappings == 5
        assert response.created_mappings == 2
        assert response.skipped_mappings == 0
        assert response.failed_mappings == 0
        assert response.providers_affected == ["KRAKEN"]
        assert response.affected_indices == []

    @pytest.mark.asyncio
    async def test_remap_execute_no_matching_mappings(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test remap with no matching mappings returns no_mappings status."""
        reg = registry_with_mocks

        # Setup transaction mock
        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # Mock affected indices query (empty)
        index_records = []
        # Mock deleted rows (none - empty result)
        deleted_rows = []

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            index_records,
            deleted_rows,
        ])

        from quasar.services.registry.schemas import AssetMappingRemapRequest
        request = AssetMappingRemapRequest(
            class_name="NONEXISTENT",
            class_type="provider",
            asset_class=None
        )

        response = await reg.handle_remap_assets(request)

        assert response.status == "no_mappings"
        assert response.deleted_mappings == 0
        assert response.created_mappings == 0
        assert response.skipped_mappings == 0
        assert response.failed_mappings == 0
        assert response.providers_affected == []
        assert response.affected_indices == []

    @pytest.mark.asyncio
    async def test_remap_execute_rollback_on_error(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test remap operation rolls back on database error."""
        reg = registry_with_mocks

        # Setup transaction mock
        txn = mock_asyncpg_conn.transaction.return_value
        txn.__aenter__ = AsyncMock(return_value=None)
        txn.__aexit__ = AsyncMock(return_value=None)

        # First fetch succeeds (affected indices)
        # Second fetch fails with database error
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            [],  # affected indices query succeeds
            Exception("Database connection lost"),  # DELETE query fails
        ])

        from quasar.services.registry.schemas import AssetMappingRemapRequest
        request = AssetMappingRemapRequest(
            class_name="KRAKEN",
            class_type="provider",
            asset_class=None
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_remap_assets(request)

        assert exc_info.value.status_code == 500
        assert "rolled back" in exc_info.value.detail.lower()
        # Verify transaction context manager was properly exited (rollback)
        txn.__aexit__.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_remap_execute_class_name_without_class_type_raises_400(
        self, registry_with_mocks
    ):
        """Test 400 error when class_name is provided without class_type."""
        reg = registry_with_mocks

        from quasar.services.registry.schemas import AssetMappingRemapRequest
        request = AssetMappingRemapRequest(
            class_name="KRAKEN",
            class_type=None,
            asset_class=None
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_remap_assets(request)

        assert exc_info.value.status_code == 400
        assert "class_type is required" in exc_info.value.detail
