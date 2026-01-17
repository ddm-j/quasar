"""Tests for AutomatedMapper - automated asset mapping behaviors."""
import pytest
from unittest.mock import AsyncMock, Mock, patch

from quasar.services.registry.mapper import AutomatedMapper, MappingCandidate


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


def make_asset_row_for_mapping(**kwargs) -> MockRecord:
    """Factory for creating mock asset rows with mapping-relevant fields."""
    defaults = {
        "id": 1,
        "class_name": "TestProvider",
        "class_type": "provider",
        "symbol": "AAPL",
        "primary_id": "FIGI_AAPL",
        "asset_class_group": "securities",
        "quote_currency": None,
        "sym_norm_root": "AAPL",
        "base_currency": None
    }
    defaults.update(kwargs)
    return MockRecord(**defaults)


def make_mapping_candidate(**kwargs) -> MappingCandidate:
    """Factory for creating MappingCandidate objects."""
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


@pytest.fixture
def mapper_with_mocks(mock_asyncpg_pool, mock_asyncpg_conn):
    """Create AutomatedMapper with mocked pool (same pattern as matcher_with_mocks)."""
    return AutomatedMapper(pool=mock_asyncpg_pool)


class TestGenerateMappingCandidatesForProvider:
    """Behavior tests for generate_mapping_candidates_for_provider method."""

    @pytest.mark.asyncio
    async def test_provider_with_no_assets_returns_empty_candidates(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Provider with no assets returns empty candidate list."""
        # Mock no assets returned for provider
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        result = await mapper_with_mocks.generate_mapping_candidates_for_provider("EmptyProvider", "provider")

        assert result == []

    @pytest.mark.asyncio
    async def test_unmapped_securities_create_candidates_with_common_symbols(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Unmapped securities get mapping candidates with consistent common symbols."""
        # Mock only EODHD assets (the provider being mapped)
        eodhd_assets = [
            make_asset_row_for_mapping(id=1, class_name="EODHD", class_type="provider", symbol="AAPL.US", primary_id="FIGI_AAPL", asset_class_group="securities"),
        ]

        # Mock no existing mappings for this primary_id
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            eodhd_assets,  # Provider assets query
            [],            # Existing mappings query (no existing mappings found)
            [],            # Conflict check query (no conflicts)
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        # Should create candidates for EODHD assets
        assert len(candidates) == 1
        assert candidates[0].class_name == "EODHD"
        assert candidates[0].class_symbol == "AAPL.US"
        assert candidates[0].common_symbol == "AAPL"  # Determined from symbol

    @pytest.mark.asyncio
    async def test_existing_mappings_reuse_common_symbols(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: When mappings exist, new candidates reuse existing common symbols."""
        # Mock only EODHD assets (the provider being mapped)
        eodhd_assets = [
            make_asset_row_for_mapping(id=1, class_name="EODHD", class_type="provider", symbol="AAPL.US", primary_id="FIGI_AAPL", asset_class_group="securities"),
        ]

        # Mock existing mapping for this primary_id from another provider
        existing_mappings = [
            MockRecord(class_name="BINANCE", class_type="provider", class_symbol="AAPL", common_symbol="AAPL_COMMON", primary_id="FIGI_AAPL")
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            eodhd_assets,       # Provider assets query
            existing_mappings,  # Existing mappings query
            [],                 # Conflict check query (not needed - reusing existing)
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        # Should reuse existing common symbol
        assert len(candidates) == 1
        assert candidates[0].common_symbol == "AAPL_COMMON"

    @pytest.mark.asyncio
    async def test_already_mapped_assets_reuse_common_symbols(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Assets that already have mappings still get candidates but reuse existing common symbols."""
        # Mock asset for EODHD
        eodhd_assets = [
            make_asset_row_for_mapping(id=1, class_name="EODHD", class_type="provider", symbol="AAPL.US", primary_id="FIGI_AAPL"),
        ]

        # Mock existing mapping for this exact asset
        existing_mappings = [
            MockRecord(class_name="EODHD", class_type="provider", class_symbol="AAPL.US", common_symbol="AAPL", primary_id="FIGI_AAPL")
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            eodhd_assets,       # Provider assets query
            existing_mappings,  # Existing mappings query
            [],                 # Conflict check query (not needed - reusing existing)
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        # Should still create candidate but reuse existing common symbol
        assert len(candidates) == 1
        assert candidates[0].common_symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_crypto_provider_applies_preferences_to_select_assets(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Crypto providers only get candidates for preferred quote currencies."""
        # Mock BTC assets with different quote currencies
        assets = [
            make_asset_row_for_mapping(id=1, class_name="KRAKEN", class_type="provider", symbol="XBT/USD",
                                      primary_id="FIGI_BTC", quote_currency="USD", asset_class_group="crypto"),
            make_asset_row_for_mapping(id=2, class_name="KRAKEN", class_type="provider", symbol="XBT/USDT",
                                      primary_id="FIGI_BTC", quote_currency="USDT", asset_class_group="crypto"),
        ]

        # Mock KRAKEN prefers USDT
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(preferred_quote="USDT"))
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,  # Provider assets query
            [],      # Existing mappings query
            [],      # Conflict check query
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("KRAKEN", "provider")

        # Should only create candidate for preferred USDT asset
        assert len(candidates) == 1
        assert candidates[0].class_symbol == "XBT/USDT"

    @pytest.mark.asyncio
    async def test_crypto_single_quote_currency_maps_directly(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Crypto single quote currency maps directly."""
        # Mock BTC assets with single quote currency
        assets = [
            make_asset_row_for_mapping(id=1, class_name="KRAKEN", class_type="provider", symbol="XBT/USD",
                                      primary_id="FIGI_BTC", quote_currency="USD", asset_class_group="crypto"),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,  # Provider assets query
            [],      # Existing mappings query
            [],      # Conflict check query
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("KRAKEN", "provider")

        # Should create candidate regardless of preferences
        assert len(candidates) == 1
        assert candidates[0].class_symbol == "XBT/USD"

    @pytest.mark.asyncio
    async def test_crypto_usd_fallback_when_no_preference_match(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Crypto USD fallback when no preference match."""
        # Mock BTC assets with different quote currencies, no preference match
        assets = [
            make_asset_row_for_mapping(id=1, class_name="KRAKEN", class_type="provider", symbol="XBT/EUR",
                                      primary_id="FIGI_BTC", quote_currency="EUR", asset_class_group="crypto"),
            make_asset_row_for_mapping(id=2, class_name="KRAKEN", class_type="provider", symbol="XBT/USD",
                                      primary_id="FIGI_BTC", quote_currency="USD", asset_class_group="crypto"),
        ]

        # Mock no preference set
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(preferred_quote=None))
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,  # Provider assets query
            [],      # Existing mappings query
            [],      # Conflict check query
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("KRAKEN", "provider")

        # Should select USD fallback
        assert len(candidates) == 1
        assert candidates[0].class_symbol == "XBT/USD"

    @pytest.mark.asyncio
    async def test_crypto_skips_when_no_suitable_usd_available(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Crypto skips when no suitable USD available."""
        # Mock BTC assets with non-USD quote currencies
        assets = [
            make_asset_row_for_mapping(id=1, class_name="KRAKEN", class_type="provider", symbol="XBT/EUR",
                                      primary_id="FIGI_BTC", quote_currency="EUR", asset_class_group="crypto"),
            make_asset_row_for_mapping(id=2, class_name="KRAKEN", class_type="provider", symbol="XBT/GBP",
                                      primary_id="FIGI_BTC", quote_currency="GBP", asset_class_group="crypto"),
        ]

        # Mock no preference set
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(preferred_quote=None))
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,  # Provider assets query
            [],      # Existing mappings query
            [],      # Conflict check query
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("KRAKEN", "provider")

        # Should skip when no suitable USD available
        assert candidates == []

    @pytest.mark.asyncio
    async def test_cross_provider_consistency_same_primary_id_same_symbol(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Same primary_id gets consistent common symbol determination."""
        # Mock EODHD assets with primary_id that has existing mapping from another provider
        eodhd_assets = [
            make_asset_row_for_mapping(id=1, class_name="EODHD", class_type="provider", symbol="AAPL.US", primary_id="FIGI_AAPL"),
        ]

        # Mock existing mapping for this primary_id from BINANCE
        existing_mappings = [
            MockRecord(class_name="BINANCE", class_type="provider", class_symbol="AAPL", common_symbol="AAPL_COMMON", primary_id="FIGI_AAPL")
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            eodhd_assets,       # Provider assets query
            existing_mappings,  # Existing mappings query
            [],                 # Conflict check query (not needed - reusing existing)
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        # Should reuse the existing common symbol for consistency
        assert len(candidates) == 1
        assert candidates[0].common_symbol == "AAPL_COMMON"

    @pytest.mark.asyncio
    async def test_securities_map_all_provider_assets(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Securities create candidates for all provider assets with same primary_id."""
        # Mock multiple securities with same primary_id for the provider
        eodhd_assets = [
            make_asset_row_for_mapping(id=1, class_name="EODHD", class_type="provider", symbol="MS", primary_id="FIGI_MS", asset_class_group="securities"),
            make_asset_row_for_mapping(id=2, class_name="EODHD", class_type="provider", symbol="MS-A", primary_id="FIGI_MS", asset_class_group="securities"),
            make_asset_row_for_mapping(id=3, class_name="EODHD", class_type="provider", symbol="MS-B", primary_id="FIGI_MS", asset_class_group="securities"),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            eodhd_assets,  # Provider assets query
            [],            # Existing mappings query
            [],            # Conflict check query
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        # Should create candidates for all provider assets
        assert len(candidates) == 3
        symbols = {c.class_symbol for c in candidates}
        assert symbols == {"MS", "MS-A", "MS-B"}

    @pytest.mark.asyncio
    async def test_mixed_asset_classes_not_mixed_in_single_group(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Assets are grouped by both primary_id and asset_class_group."""
        # Mock EODHD securities assets (only this provider's assets)
        eodhd_assets = [
            make_asset_row_for_mapping(id=1, class_name="EODHD", class_type="provider", symbol="AAPL.US",
                                      primary_id="FIGI_AAPL", asset_class_group="securities"),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            eodhd_assets,  # Provider assets query
            [],            # Existing mappings query
            [],            # Conflict check query
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        # Should only process the provider's securities assets
        assert len(candidates) == 1
        assert candidates[0].class_name == "EODHD"
        assert candidates[0].asset_class_group == "securities"


class TestFigiConflictResolution:
    """Tests for cross-provider FIGI conflict detection and resolution."""

    @pytest.mark.asyncio
    async def test_new_symbol_no_conflict_uses_proposed_symbol(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: When no conflict exists, use the proposed symbol directly."""
        assets = [
            make_asset_row_for_mapping(
                class_name="EODHD", symbol="AAPL.US",
                primary_id="FIGI_AAPL", sym_norm_root="aapl"
            ),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,  # Provider assets query
            [],      # Existing mappings query (none for this FIGI)
            [],      # Conflict check query (no conflicts)
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        assert len(candidates) == 1
        assert candidates[0].common_symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_conflict_different_figi_uses_unique_symbol(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: When symbol is claimed by different FIGI, use SYMBOL:FIGI format."""
        # New asset trying to use "BTC" as common symbol
        assets = [
            make_asset_row_for_mapping(
                class_name="EODHD", symbol="BTC.NYSE",
                primary_id="BBG000XYZ123",  # Different FIGI than existing
                sym_norm_root="btc",
                asset_class_group="securities"
            ),
        ]

        # "BTC" is already claimed by a crypto asset with different FIGI
        existing_claim = [
            MockRecord(common_symbol="BTC", primary_id="KKG00000DV14")
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,          # Provider assets query
            [],              # Existing mappings for this FIGI (none)
            existing_claim,  # Conflict check: BTC claimed by different FIGI
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        assert len(candidates) == 1
        assert candidates[0].common_symbol == "BTC:BBG000XYZ123"

    @pytest.mark.asyncio
    async def test_conflict_same_figi_reuses_existing_symbol(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: When mapping exists for same FIGI, reuse existing common_symbol."""
        assets = [
            make_asset_row_for_mapping(
                class_name="EODHD", symbol="AAPL.US",
                primary_id="FIGI_AAPL", sym_norm_root="aapl"
            ),
        ]

        # Existing mapping for same FIGI from another provider
        existing_mappings = [
            MockRecord(
                class_name="BINANCE", class_type="provider",
                class_symbol="AAPL", common_symbol="AAPL_COMMON",
                primary_id="FIGI_AAPL"
            )
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,            # Provider assets query
            existing_mappings, # Existing mappings for this FIGI
            [],                # Conflict check (empty - not needed when reusing)
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        assert len(candidates) == 1
        assert candidates[0].common_symbol == "AAPL_COMMON"  # Reuses existing

    @pytest.mark.asyncio
    async def test_multiple_groups_with_mixed_conflicts(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: Multiple groups processed correctly with some having conflicts."""
        assets = [
            # Group 1: BTC security (will conflict with existing crypto BTC)
            make_asset_row_for_mapping(
                id=1, class_name="EODHD", symbol="BTC.NYSE",
                primary_id="BBG000XYZ123", sym_norm_root="btc",
                asset_class_group="securities"
            ),
            # Group 2: AAPL (no conflict)
            make_asset_row_for_mapping(
                id=2, class_name="EODHD", symbol="AAPL.US",
                primary_id="FIGI_AAPL", sym_norm_root="aapl",
                asset_class_group="securities"
            ),
        ]

        # BTC is claimed by crypto FIGI, AAPL is not claimed
        conflict_results = [
            MockRecord(common_symbol="BTC", primary_id="KKG00000DV14")
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,           # Provider assets query
            [],               # Existing mappings (none)
            conflict_results, # Conflict check: only BTC is claimed
        ])

        candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

        assert len(candidates) == 2

        btc_candidate = next(c for c in candidates if "BTC" in c.common_symbol)
        aapl_candidate = next(c for c in candidates if "AAPL" in c.common_symbol)

        assert btc_candidate.common_symbol == "BTC:BBG000XYZ123"  # Conflict resolved
        assert aapl_candidate.common_symbol == "AAPL"  # No conflict

    @pytest.mark.asyncio
    async def test_conflict_logged_with_info_message(self, mapper_with_mocks, mock_asyncpg_conn):
        """Behavior: FIGI conflicts are logged with info level message."""
        assets = [
            make_asset_row_for_mapping(
                class_name="EODHD", symbol="BTC.NYSE",
                primary_id="BBG000XYZ123", sym_norm_root="btc"
            ),
        ]

        existing_claim = [
            MockRecord(common_symbol="BTC", primary_id="KKG00000DV14")
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            assets,
            [],
            existing_claim,
        ])

        with patch('quasar.services.registry.mapper.logger') as mock_logger:
            candidates = await mapper_with_mocks.generate_mapping_candidates_for_provider("EODHD", "provider")

            # Verify info log was called with conflict resolution message
            mock_logger.info.assert_called()
            # Check all info calls for the conflict resolution message
            all_calls_str = str(mock_logger.info.call_args_list)
            assert "FIGI conflict resolved" in all_calls_str
            assert "BTC" in all_calls_str