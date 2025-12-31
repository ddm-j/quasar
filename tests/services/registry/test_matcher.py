"""Tests for IdentityMatcher - asset identity matching and deduplication."""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from dataclasses import asdict

from quasar.services.registry.matcher import IdentityMatcher, MatchResult


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


def make_match_result(
    asset_id: int = 1,
    symbol: str = "AAPL",
    primary_id: str = "BBG000B9XRY4",
    identity_symbol: str = "AAPL",
    identity_name: str = "Apple Inc",
    confidence: float = 100.0,
    match_type: str = "exact_alias"
) -> MatchResult:
    """Factory for creating MatchResult objects."""
    return MatchResult(
        asset_id=asset_id,
        symbol=symbol,
        primary_id=primary_id,
        identity_symbol=identity_symbol,
        identity_name=identity_name,
        confidence=confidence,
        match_type=match_type
    )


def make_asset_row(
    id: int = 1,
    symbol: str = "AAPL",
    name: str = "Apple Inc",
    exchange: str = "XNAS",
    asset_class_group: str = "securities",
    matcher_symbol: str = "AAPL"
) -> MockRecord:
    """Factory for creating mock asset rows."""
    return MockRecord(
        id=id,
        symbol=symbol,
        name=name,
        exchange=exchange,
        asset_class_group=asset_class_group,
        matcher_symbol=matcher_symbol
    )


@pytest.fixture
def matcher_with_mocks(mock_asyncpg_pool, mock_asyncpg_conn):
    """Create IdentityMatcher with mocked pool using the same pattern as registry tests."""
    return IdentityMatcher(pool=mock_asyncpg_pool)


# =============================================================================
# MatchResult Dataclass Tests
# =============================================================================

class TestMatchResult:
    """Tests for MatchResult dataclass."""

    def test_match_result_creation(self):
        """Test MatchResult can be created with all fields."""
        result = MatchResult(
            asset_id=1,
            symbol="AAPL",
            primary_id="BBG000B9XRY4",
            identity_symbol="AAPL",
            identity_name="Apple Inc",
            confidence=100.0,
            match_type="exact_alias"
        )
        assert result.asset_id == 1
        assert result.symbol == "AAPL"
        assert result.primary_id == "BBG000B9XRY4"
        assert result.confidence == 100.0
        assert result.match_type == "exact_alias"

    def test_match_result_from_dict(self):
        """Test MatchResult can be created from dict (like DB row conversion)."""
        data = {
            "asset_id": 2,
            "symbol": "MSFT",
            "primary_id": "BBG000BPH459",
            "identity_symbol": "MSFT",
            "identity_name": "Microsoft Corp",
            "confidence": 95.5,
            "match_type": "fuzzy_symbol"
        }
        result = MatchResult(**data)
        assert result.asset_id == 2
        assert result.symbol == "MSFT"


# =============================================================================
# Deduplication Tests
# =============================================================================

class TestDeduplicateSecuritiesResults:
    """Tests for _deduplicate_securities_results behavior."""

    def test_empty_input_returns_empty(self, matcher_with_mocks):
        """Empty input should return empty list."""
        result = matcher_with_mocks._deduplicate_securities_results([])
        assert result == []

    def test_single_result_passes_through(self, matcher_with_mocks):
        """Single result per primary_id passes through unchanged."""
        matches = [make_match_result(asset_id=1, primary_id="FIGI1", symbol="AAPL")]
        result = matcher_with_mocks._deduplicate_securities_results(matches)

        assert len(result) == 1
        assert result[0].asset_id == 1
        assert result[0].symbol == "AAPL"

    def test_multiple_unique_primary_ids_all_pass(self, matcher_with_mocks):
        """Multiple results with different primary_ids all pass through."""
        matches = [
            make_match_result(asset_id=1, primary_id="FIGI1", symbol="AAPL"),
            make_match_result(asset_id=2, primary_id="FIGI2", symbol="MSFT"),
            make_match_result(asset_id=3, primary_id="FIGI3", symbol="GOOG"),
        ]
        result = matcher_with_mocks._deduplicate_securities_results(matches)

        assert len(result) == 3
        symbols = {r.symbol for r in result}
        assert symbols == {"AAPL", "MSFT", "GOOG"}

    def test_shortest_symbol_wins(self, matcher_with_mocks):
        """When duplicates exist, shortest symbol wins."""
        # MS, MS-A, MS-B all share same FIGI - MS should win
        matches = [
            make_match_result(asset_id=1, primary_id="FIGI_MS", symbol="MS-A"),
            make_match_result(asset_id=2, primary_id="FIGI_MS", symbol="MS"),
            make_match_result(asset_id=3, primary_id="FIGI_MS", symbol="MS-B"),
        ]
        result = matcher_with_mocks._deduplicate_securities_results(matches)

        assert len(result) == 1
        assert result[0].symbol == "MS"
        assert result[0].asset_id == 2

    def test_alphabetical_tiebreaker(self, matcher_with_mocks):
        """When symbol lengths are equal, alphabetical order breaks tie."""
        # AAA vs BBB vs CCC - all length 3, AAA should win
        matches = [
            make_match_result(asset_id=1, primary_id="FIGI1", symbol="CCC"),
            make_match_result(asset_id=2, primary_id="FIGI1", symbol="AAA"),
            make_match_result(asset_id=3, primary_id="FIGI1", symbol="BBB"),
        ]
        result = matcher_with_mocks._deduplicate_securities_results(matches)

        assert len(result) == 1
        assert result[0].symbol == "AAA"
        assert result[0].asset_id == 2

    def test_mixed_unique_and_duplicate_primary_ids(self, matcher_with_mocks):
        """Mixed unique and duplicate primary_ids are handled correctly."""
        matches = [
            # Unique primary_id
            make_match_result(asset_id=1, primary_id="FIGI_UNIQUE", symbol="AAPL"),
            # Duplicates - MS should win
            make_match_result(asset_id=2, primary_id="FIGI_DUP", symbol="MS-A"),
            make_match_result(asset_id=3, primary_id="FIGI_DUP", symbol="MS"),
            # Another unique
            make_match_result(asset_id=4, primary_id="FIGI_ANOTHER", symbol="GOOG"),
        ]
        result = matcher_with_mocks._deduplicate_securities_results(matches)

        assert len(result) == 3
        symbols = {r.symbol for r in result}
        assert symbols == {"AAPL", "MS", "GOOG"}

    def test_deduplication_logs_warning_on_rejections(self, matcher_with_mocks, caplog):
        """Warning is logged when rejections occur."""
        import logging
        caplog.set_level(logging.WARNING)

        matches = [
            make_match_result(asset_id=1, primary_id="FIGI1", symbol="MS"),
            make_match_result(asset_id=2, primary_id="FIGI1", symbol="MS-A"),
        ]
        matcher_with_mocks._deduplicate_securities_results(matches)

        # Should have logged a warning about rejections
        assert any("rejected" in record.message.lower() for record in caplog.records)

    def test_deduplication_preserves_all_match_fields(self, matcher_with_mocks):
        """Winning match retains all its original fields."""
        matches = [
            MatchResult(
                asset_id=1,
                symbol="ABC",
                primary_id="FIGI1",
                identity_symbol="ABC;ABCD",
                identity_name="ABC Company",
                confidence=100.0,
                match_type="exact_alias"
            ),
            MatchResult(
                asset_id=2,
                symbol="ABCD",
                primary_id="FIGI1",
                identity_symbol="ABC;ABCD",
                identity_name="ABC Company",
                confidence=100.0,
                match_type="exact_alias"
            ),
        ]
        result = matcher_with_mocks._deduplicate_securities_results(matches)

        assert len(result) == 1
        winner = result[0]
        assert winner.asset_id == 1  # ABC (shorter) wins
        assert winner.symbol == "ABC"
        assert winner.identity_symbol == "ABC;ABCD"
        assert winner.identity_name == "ABC Company"
        assert winner.confidence == 100.0
        assert winner.match_type == "exact_alias"


# =============================================================================
# Asset Identification Tests
# =============================================================================

class TestIdentifyUnidentifiedAssets:
    """Tests for identify_unidentified_assets behavior."""

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_unidentified_assets(self, matcher_with_mocks, mock_asyncpg_conn):
        """Returns empty list when no unidentified assets exist."""
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        result = await matcher_with_mocks.identify_unidentified_assets("EODHD", "provider")

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_match_results_for_identified_assets(self, matcher_with_mocks, mock_asyncpg_conn):
        """Returns MatchResults for assets that get identified."""
        # Mock unidentified assets
        asset_rows = [
            make_asset_row(id=1, symbol="AAPL", asset_class_group="securities"),
        ]

        # Mock exact match results
        exact_match_rows = [
            MockRecord(
                asset_id=1,
                symbol="AAPL",
                primary_id="BBG000B9XRY4",
                identity_symbol="AAPL",
                identity_name="Apple Inc",
                confidence=100.0,
                match_type="exact_alias"
            )
        ]

        # First call fetches unidentified assets, subsequent calls for matching
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[asset_rows, exact_match_rows, []])

        result = await matcher_with_mocks.identify_unidentified_assets("EODHD", "provider")

        assert len(result) == 1
        assert result[0].asset_id == 1
        assert result[0].primary_id == "BBG000B9XRY4"

    @pytest.mark.asyncio
    async def test_filters_by_class_name_and_type(self, matcher_with_mocks, mock_asyncpg_conn):
        """Verifies query filters by class_name and class_type."""
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        await matcher_with_mocks.identify_unidentified_assets("TestProvider", "broker")

        # Verify fetch was called with correct parameters
        call_args = mock_asyncpg_conn.fetch.call_args_list[0]
        assert "TestProvider" in call_args[0]
        assert "broker" in call_args[0]


class TestIdentifyAllUnidentifiedAssets:
    """Tests for identify_all_unidentified_assets behavior."""

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_unidentified_assets(self, matcher_with_mocks, mock_asyncpg_conn):
        """Returns empty list when no unidentified assets exist globally."""
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        result = await matcher_with_mocks.identify_all_unidentified_assets()

        assert result == []

    @pytest.mark.asyncio
    async def test_processes_assets_from_all_providers(self, matcher_with_mocks, mock_asyncpg_conn):
        """Processes unidentified assets across all providers."""
        # Mock assets from multiple providers (not filtered by class)
        asset_rows = [
            make_asset_row(id=1, symbol="AAPL", asset_class_group="securities"),
            make_asset_row(id=2, symbol="BTC", asset_class_group="crypto"),
        ]

        # Mock no matches for simplicity
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[asset_rows, [], [], [], []])

        await matcher_with_mocks.identify_all_unidentified_assets()

        # First call should not include class_name/class_type filters
        first_call_query = mock_asyncpg_conn.fetch.call_args_list[0][0][0]
        assert "class_name = $1" not in first_call_query


# =============================================================================
# Matching Pipeline Tests
# =============================================================================

class TestProcessMatching:
    """Tests for _process_matching pipeline behavior."""

    @pytest.mark.asyncio
    async def test_securities_assets_get_deduplicated(self, matcher_with_mocks, mock_asyncpg_conn):
        """Securities results go through deduplication."""
        # Two securities assets that will match to same primary_id
        asset_rows = [
            make_asset_row(id=1, symbol="MS", asset_class_group="securities"),
            make_asset_row(id=2, symbol="MS-A", asset_class_group="securities"),
        ]

        # Both match to same primary_id
        match_rows = [
            MockRecord(
                asset_id=1, symbol="MS", primary_id="FIGI_MS",
                identity_symbol="MS", identity_name="Morgan Stanley",
                confidence=100.0, match_type="exact_alias"
            ),
            MockRecord(
                asset_id=2, symbol="MS-A", primary_id="FIGI_MS",
                identity_symbol="MS", identity_name="Morgan Stanley",
                confidence=100.0, match_type="exact_alias"
            ),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[match_rows, []])

        result = await matcher_with_mocks._process_matching(asset_rows)

        # Only one should survive deduplication (MS, the shorter symbol)
        assert len(result) == 1
        assert result[0].symbol == "MS"

    @pytest.mark.asyncio
    async def test_crypto_assets_not_deduplicated(self, matcher_with_mocks, mock_asyncpg_conn):
        """Crypto results do NOT go through deduplication."""
        # Two crypto assets matching same primary_id (e.g., XBT/USD and XBT/USDC)
        asset_rows = [
            make_asset_row(id=1, symbol="XBTUSD", asset_class_group="crypto"),
            make_asset_row(id=2, symbol="XBTUSDC", asset_class_group="crypto"),
        ]

        # Both match to same primary_id (Bitcoin)
        match_rows = [
            MockRecord(
                asset_id=1, symbol="XBTUSD", primary_id="FIGI_BTC",
                identity_symbol="BTC", identity_name="Bitcoin",
                confidence=100.0, match_type="exact_alias"
            ),
            MockRecord(
                asset_id=2, symbol="XBTUSDC", primary_id="FIGI_BTC",
                identity_symbol="BTC", identity_name="Bitcoin",
                confidence=100.0, match_type="exact_alias"
            ),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[match_rows, []])

        result = await matcher_with_mocks._process_matching(asset_rows)

        # Both should remain - crypto doesn't deduplicate
        assert len(result) == 2
        symbols = {r.symbol for r in result}
        assert symbols == {"XBTUSD", "XBTUSDC"}

    @pytest.mark.asyncio
    async def test_mixed_securities_and_crypto_processed_separately(self, matcher_with_mocks, mock_asyncpg_conn):
        """Securities and crypto are processed through their respective pipelines."""
        asset_rows = [
            make_asset_row(id=1, symbol="AAPL", asset_class_group="securities"),
            make_asset_row(id=2, symbol="BTC", asset_class_group="crypto"),
        ]

        # Securities exact match
        securities_matches = [
            MockRecord(
                asset_id=1, symbol="AAPL", primary_id="FIGI_AAPL",
                identity_symbol="AAPL", identity_name="Apple Inc",
                confidence=100.0, match_type="exact_alias"
            ),
        ]

        # Crypto exact match
        crypto_matches = [
            MockRecord(
                asset_id=2, symbol="BTC", primary_id="FIGI_BTC",
                identity_symbol="BTC", identity_name="Bitcoin",
                confidence=100.0, match_type="exact_alias"
            ),
        ]

        # Mock returns: securities exact, securities fuzzy (empty), crypto exact, crypto fuzzy (empty)
        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[
            securities_matches, [],  # securities: exact, fuzzy
            crypto_matches, []       # crypto: exact, fuzzy
        ])

        result = await matcher_with_mocks._process_matching(asset_rows)

        assert len(result) == 2
        symbols = {r.symbol for r in result}
        assert symbols == {"AAPL", "BTC"}


class TestRunMatchingForGroup:
    """Tests for _run_matching_for_group two-phase matching."""

    @pytest.mark.asyncio
    async def test_exact_matches_skip_fuzzy(self, matcher_with_mocks, mock_asyncpg_conn):
        """When all assets match exactly, fuzzy phase is skipped."""
        assets = [make_asset_row(id=1, symbol="AAPL")]

        exact_matches = [
            MockRecord(
                asset_id=1, symbol="AAPL", primary_id="FIGI1",
                identity_symbol="AAPL", identity_name="Apple",
                confidence=100.0, match_type="exact_alias"
            )
        ]

        # Only one fetch for exact matching
        mock_asyncpg_conn.fetch = AsyncMock(return_value=exact_matches)

        result = await matcher_with_mocks._run_matching_for_group(assets, "securities")

        assert len(result) == 1
        assert result[0].match_type == "exact_alias"
        # fetch should only be called once (exact matching)
        assert mock_asyncpg_conn.fetch.call_count == 1

    @pytest.mark.asyncio
    async def test_no_exact_matches_runs_fuzzy(self, matcher_with_mocks, mock_asyncpg_conn):
        """When no exact matches, fuzzy matching runs."""
        assets = [make_asset_row(id=1, symbol="APPL")]  # Typo in symbol

        # No exact matches
        exact_matches = []

        # Fuzzy finds a match
        fuzzy_matches = [
            MockRecord(
                asset_id=1, symbol="APPL", primary_id="FIGI1",
                identity_symbol="AAPL", identity_name="Apple",
                confidence=85.0, match_type="fuzzy_symbol"
            )
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[exact_matches, fuzzy_matches])
        mock_asyncpg_conn.execute = AsyncMock()  # For SET pg_trgm.similarity_threshold

        result = await matcher_with_mocks._run_matching_for_group(assets, "securities")

        assert len(result) == 1
        assert result[0].match_type == "fuzzy_symbol"

    @pytest.mark.asyncio
    async def test_partial_exact_matches_fuzzy_for_remainder(self, matcher_with_mocks, mock_asyncpg_conn):
        """Some exact matches, remainder goes to fuzzy."""
        assets = [
            make_asset_row(id=1, symbol="AAPL"),
            make_asset_row(id=2, symbol="MSFT_TYPO"),  # Won't match exactly
        ]

        # Only AAPL matches exactly
        exact_matches = [
            MockRecord(
                asset_id=1, symbol="AAPL", primary_id="FIGI_AAPL",
                identity_symbol="AAPL", identity_name="Apple",
                confidence=100.0, match_type="exact_alias"
            )
        ]

        # MSFT_TYPO matched fuzzily
        fuzzy_matches = [
            MockRecord(
                asset_id=2, symbol="MSFT_TYPO", primary_id="FIGI_MSFT",
                identity_symbol="MSFT", identity_name="Microsoft",
                confidence=82.0, match_type="fuzzy_symbol"
            )
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[exact_matches, fuzzy_matches])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await matcher_with_mocks._run_matching_for_group(assets, "securities")

        assert len(result) == 2
        exact_result = next(r for r in result if r.match_type == "exact_alias")
        fuzzy_result = next(r for r in result if r.match_type == "fuzzy_symbol")
        assert exact_result.asset_id == 1
        assert fuzzy_result.asset_id == 2

    @pytest.mark.asyncio
    async def test_combines_exact_and_fuzzy_results(self, matcher_with_mocks, mock_asyncpg_conn):
        """Results from both phases are combined."""
        assets = [
            make_asset_row(id=1, symbol="AAPL"),
            make_asset_row(id=2, symbol="GOOG_X"),
        ]

        exact_matches = [
            MockRecord(
                asset_id=1, symbol="AAPL", primary_id="F1",
                identity_symbol="AAPL", identity_name="Apple",
                confidence=100.0, match_type="exact_alias"
            )
        ]

        fuzzy_matches = [
            MockRecord(
                asset_id=2, symbol="GOOG_X", primary_id="F2",
                identity_symbol="GOOG", identity_name="Google",
                confidence=80.0, match_type="fuzzy_symbol"
            )
        ]

        mock_asyncpg_conn.fetch = AsyncMock(side_effect=[exact_matches, fuzzy_matches])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await matcher_with_mocks._run_matching_for_group(assets, "securities")

        assert len(result) == 2
        match_types = {r.match_type for r in result}
        assert match_types == {"exact_alias", "fuzzy_symbol"}


class TestExactMatching:
    """Tests for _run_exact_matching behavior."""

    @pytest.mark.asyncio
    async def test_returns_match_results_from_db_rows(self, matcher_with_mocks, mock_asyncpg_conn):
        """Converts database rows to MatchResult objects."""
        assets = [make_asset_row(id=1, symbol="AAPL", matcher_symbol="AAPL")]

        db_rows = [
            MockRecord(
                asset_id=1, symbol="AAPL", primary_id="FIGI1",
                identity_symbol="AAPL", identity_name="Apple Inc",
                confidence=100.0, match_type="exact_alias"
            )
        ]

        mock_asyncpg_conn.fetch = AsyncMock(return_value=db_rows)

        result = await matcher_with_mocks._run_exact_matching(assets, "securities")

        assert len(result) == 1
        assert isinstance(result[0], MatchResult)
        assert result[0].asset_id == 1
        assert result[0].primary_id == "FIGI1"

    @pytest.mark.asyncio
    async def test_passes_correct_parameters(self, matcher_with_mocks, mock_asyncpg_conn):
        """Verifies correct parameters passed to query."""
        assets = [
            make_asset_row(id=5, matcher_symbol="TEST"),
            make_asset_row(id=10, matcher_symbol="OTHER"),
        ]

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        await matcher_with_mocks._run_exact_matching(assets, "crypto")

        call_args = mock_asyncpg_conn.fetch.call_args[0]
        # Args should include: query, asset_ids, matcher_symbols, group
        assert [5, 10] in call_args  # asset_ids
        assert ["TEST", "OTHER"] in call_args  # matcher_symbols
        assert "crypto" in call_args  # group


class TestFuzzyMatching:
    """Tests for fuzzy matching behavior."""

    @pytest.mark.asyncio
    async def test_batches_large_asset_lists(self, matcher_with_mocks, mock_asyncpg_conn):
        """Large asset lists are processed in batches."""
        # Create more assets than FUZZY_BATCH_SIZE (100)
        assets = [make_asset_row(id=i, symbol=f"SYM{i}") for i in range(250)]

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        mock_asyncpg_conn.execute = AsyncMock()

        await matcher_with_mocks._run_fuzzy_matching(assets, "securities")

        # Should have 3 batches: 100 + 100 + 50
        assert mock_asyncpg_conn.fetch.call_count == 3

    @pytest.mark.asyncio
    async def test_sets_similarity_threshold(self, matcher_with_mocks, mock_asyncpg_conn):
        """Similarity threshold is set before fuzzy query."""
        assets = [make_asset_row(id=1)]

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        mock_asyncpg_conn.execute = AsyncMock()

        await matcher_with_mocks._run_fuzzy_matching(assets, "securities")

        # Verify SET pg_trgm.similarity_threshold was called
        execute_call = mock_asyncpg_conn.execute.call_args[0][0]
        assert "pg_trgm.similarity_threshold" in execute_call
