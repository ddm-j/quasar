"""Tests for Registry automated mapping integration behaviors."""
import pytest
from unittest.mock import Mock, AsyncMock, patch

from quasar.services.registry.core import Registry
from quasar.services.registry.mapper import MappingCandidate


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
        return self._data.keys()

    def __iter__(self):
        return iter(self._data)

    def items(self):
        return self._data.items()

    def values(self):
        return self._data.values()


def make_mapping_candidate(**kwargs) -> MappingCandidate:
    """Factory for creating MappingCandidate objects."""
    from quasar.services.registry.mapper import MappingCandidate
    defaults = {
        "class_name": "TestProvider",
        "class_type": "provider",
        "class_symbol": "AAPL",
        "common_symbol": "AAPL",
        "primary_id": "FIGI_AAPL",
        "asset_class_group": "securities",
        "reasoning": "Test mapping"
    }
    defaults.update(kwargs)
    return MappingCandidate(**defaults)


class TestRegistryAutomatedMappingIntegration:
    """Test Registry automated mapping integration behaviors."""

    @pytest.mark.asyncio
    async def test_asset_update_automatically_creates_mappings(self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session):
        """Behavior: Updating provider assets automatically creates mappings."""
        # Setup provider and assets
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Provider exists
        mock_aiohttp_session["response"].json = AsyncMock(return_value=[
            {
                "symbol": "AAPL",
                "matcher_symbol": "AAPL",
                "name": "Apple Inc"
            }
        ])

        # Mock database operations for asset upsert
        mock_asyncpg_conn.prepare = AsyncMock(return_value=mock_asyncpg_conn)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"xmax": 0})

        # Mock identity matching (no matches)
        with patch.object(registry_with_mocks.matcher, 'identify_unidentified_assets', new_callable=AsyncMock) as mock_identify:
            mock_identify.return_value = []

            # Mock mapping generation and application
            with patch.object(registry_with_mocks.mapper, 'generate_mapping_candidates_for_provider', new_callable=AsyncMock) as mock_generate, \
                 patch.object(registry_with_mocks, '_apply_automated_mappings', new_callable=AsyncMock) as mock_apply:

                mock_generate.return_value = [
                    make_mapping_candidate(class_name="TestProvider", class_symbol="AAPL", common_symbol="AAPL_COMMON")
                ]
                mock_apply.return_value = {"created": 1, "skipped": 0, "failed": 0}

                response = await registry_with_mocks.handle_update_assets("provider", "TestProvider")

                # Verify mapping was attempted
                mock_generate.assert_called_once_with("TestProvider", "provider")
                mock_apply.assert_called_once()

                # Verify mapping stats in response
                assert response.mappings_created == 1
                assert response.mappings_skipped == 0
                assert response.mappings_failed == 0

    @pytest.mark.asyncio
    async def test_mapping_failure_does_not_break_asset_update(self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session):
        """Behavior: Mapping failures don't break the asset update process."""
        # Setup provider and assets
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Provider exists
        mock_aiohttp_session["response"].json = AsyncMock(return_value=[
            {
                "symbol": "AAPL",
                "matcher_symbol": "AAPL",
                "name": "Apple Inc"
            }
        ])

        # Mock database operations for asset upsert
        mock_asyncpg_conn.prepare = AsyncMock(return_value=mock_asyncpg_conn)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"xmax": 0})

        # Mock identity matching (no matches)
        with patch.object(registry_with_mocks.matcher, 'identify_unidentified_assets', new_callable=AsyncMock) as mock_identify:
            mock_identify.return_value = []

            # Mock mapping generation failure
            with patch.object(registry_with_mocks.mapper, 'generate_mapping_candidates_for_provider', new_callable=AsyncMock) as mock_generate:
                mock_generate.side_effect = Exception("Mapping failed")

                # Should not raise exception
                response = await registry_with_mocks.handle_update_assets("provider", "TestProvider")

                # Asset update should still succeed
                assert response.status == 200
                assert response.added_symbols == 1
                # Mapping stats should be zero due to failure
                assert response.mappings_created == 0

    @pytest.mark.asyncio
    async def test_response_includes_mapping_statistics(self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session):
        """Behavior: Response includes mapping statistics."""
        # Setup provider
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Provider exists
        mock_aiohttp_session["response"].json = AsyncMock(return_value=[
            {
                "symbol": "AAPL",
                "matcher_symbol": "AAPL",
                "name": "Apple Inc"
            }
        ])

        # Mock database operations for asset upsert
        mock_asyncpg_conn.prepare = AsyncMock(return_value=mock_asyncpg_conn)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"xmax": 0})

        # Mock identity matching (no matches)
        with patch.object(registry_with_mocks.matcher, 'identify_unidentified_assets', new_callable=AsyncMock) as mock_identify, \
             patch.object(registry_with_mocks, '_apply_identity_matches', new_callable=AsyncMock) as mock_apply_identity:
            mock_identify.return_value = []
            mock_apply_identity.return_value = {"identified": 0, "skipped": 0, "failed": 0, "constraint_rejected": 0}

            # Mock mapping results
            with patch.object(registry_with_mocks.mapper, 'generate_mapping_candidates_for_provider', new_callable=AsyncMock) as mock_generate, \
                 patch.object(registry_with_mocks, '_apply_automated_mappings', new_callable=AsyncMock) as mock_apply:

                mock_generate.return_value = [
                    make_mapping_candidate(class_name="TestProvider", class_symbol="AAPL")
                ]
                mock_apply.return_value = {"created": 1, "skipped": 0, "failed": 0}

                response = await registry_with_mocks.handle_update_assets("provider", "TestProvider")

                # Verify response has mapping statistics fields
                assert hasattr(response, 'mappings_created')
                assert hasattr(response, 'mappings_skipped')
                assert hasattr(response, 'mappings_failed')
                assert response.mappings_created == 1
                assert response.mappings_skipped == 0
                assert response.mappings_failed == 0

    @pytest.mark.asyncio
    async def test_no_candidates_results_in_zero_mapping_stats(self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session):
        """Behavior: No mapping candidates results in zero stats."""
        # Setup provider
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Provider exists
        mock_aiohttp_session["response"].json = AsyncMock(return_value=[
            {
                "symbol": "AAPL",
                "matcher_symbol": "AAPL",
                "name": "Apple Inc"
            }
        ])

        # Mock database operations for asset upsert
        mock_asyncpg_conn.prepare = AsyncMock(return_value=mock_asyncpg_conn)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"xmax": 0})

        # Mock identity matching (no matches)
        with patch.object(registry_with_mocks.matcher, 'identify_unidentified_assets', new_callable=AsyncMock) as mock_identify, \
             patch.object(registry_with_mocks, '_apply_identity_matches', new_callable=AsyncMock) as mock_apply_identity:
            mock_identify.return_value = []
            mock_apply_identity.return_value = {"identified": 0, "skipped": 0, "failed": 0, "constraint_rejected": 0}

            # Mock no mapping candidates
            with patch.object(registry_with_mocks.mapper, 'generate_mapping_candidates_for_provider', new_callable=AsyncMock) as mock_generate:
                mock_generate.return_value = []  # No candidates

                response = await registry_with_mocks.handle_update_assets("provider", "TestProvider")

                # Mapping stats should be zero
                assert response.mappings_created == 0
                assert response.mappings_skipped == 0
                assert response.mappings_failed == 0

    @pytest.mark.asyncio
    async def test_partial_mapping_success_tracked_correctly(self, registry_with_mocks, mock_asyncpg_conn, mock_aiohttp_session):
        """Behavior: Partial mapping success is tracked correctly."""
        # Setup provider
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Provider exists
        mock_aiohttp_session["response"].json = AsyncMock(return_value=[
            {
                "symbol": "AAPL",
                "matcher_symbol": "AAPL",
                "name": "Apple Inc"
            }
        ])

        # Mock database operations for asset upsert
        mock_asyncpg_conn.prepare = AsyncMock(return_value=mock_asyncpg_conn)
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value={"xmax": 0})

        # Mock identity matching (no matches)
        with patch.object(registry_with_mocks.matcher, 'identify_unidentified_assets', new_callable=AsyncMock) as mock_identify, \
             patch.object(registry_with_mocks, '_apply_identity_matches', new_callable=AsyncMock) as mock_apply_identity:
            mock_identify.return_value = []
            mock_apply_identity.return_value = {"identified": 0, "skipped": 0, "failed": 0, "constraint_rejected": 0}

            # Mock partial mapping results
            with patch.object(registry_with_mocks.mapper, 'generate_mapping_candidates_for_provider', new_callable=AsyncMock) as mock_generate, \
                 patch.object(registry_with_mocks, '_apply_automated_mappings', new_callable=AsyncMock) as mock_apply:

                mock_generate.return_value = [
                    make_mapping_candidate(class_name="TestProvider", class_symbol="AAPL")
                ]
                mock_apply.return_value = {"created": 2, "skipped": 1, "failed": 1}  # Mixed results

                response = await registry_with_mocks.handle_update_assets("provider", "TestProvider")

                # Stats should reflect the mixed results
                assert response.mappings_created == 2
                assert response.mappings_skipped == 1
                assert response.mappings_failed == 1