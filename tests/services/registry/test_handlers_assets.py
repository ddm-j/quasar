"""Tests for asset management handlers."""

import pytest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from quasar.services.registry.schemas import AssetQueryParams
from quasar.services.registry.matcher import MatchResult
from .conftest import MockRecord


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
