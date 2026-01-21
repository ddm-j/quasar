"""Integration tests for DataHub and TradingCalendar behavioral gatekeeping."""
import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, date, timedelta, timezone
from quasar.services.datahub.core import DataHub
from quasar.lib.providers.core import ProviderType, Bar, Req


@pytest.fixture
def mock_live_provider():
    """Create a mock live provider for realtime tests."""
    mock_provider = MagicMock()
    mock_provider.provider_type = ProviderType.REALTIME
    mock_provider.close_buffer_seconds = 5

    # Mock the async iterator return for get_data
    async def mock_gen(*args, **kwargs):
        if False: yield None
    mock_provider.get_data.return_value = mock_gen()

    return mock_provider


@pytest.mark.asyncio
@patch("quasar.services.datahub.handlers.collection.TradingCalendar")
async def test_gatekeeper_skips_historical_on_weekend(mock_calendar, datahub_with_mocks, mock_asyncpg_conn):
    """Verify that historical data pulls are skipped if no sessions occurred in the gap."""
    hub = datahub_with_mocks

    # Setup mock: today is very far in the future to ensure yday > start.
    # Today is Saturday, Dec 27. yday is Friday, Dec 26.
    # If last update was Friday, Dec 26, start is Saturday Dec 27.
    # If last update was Thursday, Dec 25 (holiday), start is Friday Dec 26.

    # Let's fix the dates to be deterministic.
    # Mock datetime.now to return a Sunday
    fixed_now = datetime(2025, 12, 21, tzinfo=timezone.utc) # Sunday
    with patch("quasar.services.datahub.handlers.collection.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.fromisoformat = datetime.fromisoformat # keep other methods

        last_updated = date(2025, 12, 19) # Friday
        # start will be Saturday (Dec 20), yday will be Saturday (Dec 20)

        mock_calendar.has_sessions_in_range.return_value = False

        # Mock symbols and their state
        symbols = ["AAPL"]
        exchanges = ["XNAS"]
        mock_asyncpg_conn.fetch.return_value = [{"sym": "AAPL", "d": last_updated}]

        # Execute build reqs
        reqs = await hub._build_reqs_historical("EODHD", "1d", symbols, exchanges)

        # Verify: No requests generated
        assert len(reqs) == 0
        mock_calendar.has_sessions_in_range.assert_called_once()


@pytest.mark.asyncio
@patch("quasar.services.datahub.handlers.collection.TradingCalendar")
async def test_gatekeeper_allows_historical_backfill(mock_calendar, datahub_with_mocks, mock_asyncpg_conn):
    """Verify that a weekend run still triggers a pull if the gap contains sessions."""
    hub = datahub_with_mocks

    # Setup mock: system was down since Wednesday. Today is Sunday.
    # Gap (Thursday to Saturday) contains 2 sessions (Thu, Fri).
    last_updated = date(2025, 12, 17) # Wednesday
    mock_calendar.has_sessions_in_range.return_value = True

    symbols = ["AAPL"]
    exchanges = ["XNAS"]
    mock_asyncpg_conn.fetch.return_value = [{"sym": "AAPL", "d": last_updated}]

    reqs = await hub._build_reqs_historical("EODHD", "1d", symbols, exchanges)

    # Verify: Request generated
    assert len(reqs) == 1
    assert reqs[0].sym == "AAPL"


@pytest.mark.asyncio
@patch("quasar.services.datahub.handlers.collection.TradingCalendar")
async def test_new_subscription_exemption(mock_calendar, datahub_with_mocks, mock_asyncpg_conn):
    """Verify that new subscriptions (no last_updated) bypass the calendar check."""
    hub = datahub_with_mocks

    # Mock no data in DB for this symbol
    symbols = ["NEW_SYM"]
    exchanges = ["XNAS"]
    mock_asyncpg_conn.fetch.return_value = [] # Symbol not found in historical_symbol_state

    reqs = await hub._build_reqs_historical("EODHD", "1d", symbols, exchanges)

    # Verify: Request generated without asking calendar
    assert len(reqs) == 1
    mock_calendar.has_sessions_in_range.assert_not_called()


@pytest.mark.asyncio
@patch("quasar.services.datahub.handlers.collection.TradingCalendar")
async def test_live_gatekeeper_filters_closed_markets(mock_calendar, datahub_with_mocks, mock_live_provider):
    """Verify that live WebSocket connections are only opened for open markets."""
    hub = datahub_with_mocks

    # Add the mock provider to the hub
    hub._providers["MockLive"] = mock_live_provider

    # Mock: AAPL (XNAS) is closed, BTC (no exchange) is open
    def mock_is_open(mic):
        return mic is None
    mock_calendar.is_open_now.side_effect = mock_is_open

    # Symbols and MICs
    symbols = ["AAPL", "BTC/USD"]
    exchanges = ["XNAS", None]

    # Mock _insert_bars to do nothing
    with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
        await hub.get_data("MockLive", "1min", symbols, exchanges)

    # Verify: Provider was only called with BTC/USD
    mock_live_provider.get_data.assert_called_once()
    args, kwargs = mock_live_provider.get_data.call_args
    assert "BTC/USD" in args[1]
    assert "AAPL" not in args[1]
