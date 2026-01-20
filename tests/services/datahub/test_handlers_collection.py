"""Tests for data collection pipeline handlers."""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone, date, timedelta

from quasar.lib.providers.core import ProviderType, Req, Bar


class TestRefreshSubscriptions:
    """Tests for DataHub.refresh_subscriptions() method."""

    @pytest.mark.asyncio
    async def test_refresh_loads_new_providers(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions loads new providers from database."""
        hub = datahub_with_mocks

        # Mock database returning a subscription for a new provider
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "NewProvider",
                "interval": "1d",
                "cron": "0 0 * * *",
                "syms": ["SYM1"],
                "exchanges": ["XNAS"]
            }
        ])

        # Mock load_provider_cls to succeed and add the provider
        async def mock_load(name):
            hub._providers[name] = mock_provider_historical
            return True

        with patch.object(hub, 'load_provider_cls', side_effect=mock_load):
            await hub.refresh_subscriptions()

            hub.load_provider_cls.assert_called_once_with("NewProvider")
            assert "NewProvider" in hub._providers

    @pytest.mark.asyncio
    async def test_refresh_handles_provider_load_failure(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Test that refresh_subscriptions skips providers that fail to load."""
        hub = datahub_with_mocks

        # Mock database returning subscriptions for an invalid provider
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "InvalidProvider",
                "interval": "1d",
                "cron": "0 0 * * *",
                "syms": ["SYM1"],
                "exchanges": ["XNAS"]
            }
        ])

        # Mock load_provider_cls to fail
        with patch.object(hub, 'load_provider_cls', return_value=False):
            await hub.refresh_subscriptions()

            # Provider should not be in the registry
            assert "InvalidProvider" not in hub._providers
            # No job should be scheduled for invalid provider
            assert len(hub.job_keys) == 0

    @pytest.mark.asyncio
    async def test_refresh_removes_obsolete_providers_with_aclose(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that obsolete providers with aclose method are properly closed and removed."""
        hub = datahub_with_mocks

        # Pre-load an obsolete provider that has aclose
        hub._providers["ObsoleteProvider"] = mock_provider_historical

        # Database returns empty subscriptions - provider is now obsolete
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await hub.refresh_subscriptions()

        # Provider should be removed
        assert "ObsoleteProvider" not in hub._providers
        # aclose should have been called
        mock_provider_historical.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_removes_obsolete_providers_without_aclose(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Test that obsolete providers without aclose method are removed gracefully."""
        hub = datahub_with_mocks

        # Create provider without aclose method
        mock_provider = Mock()
        mock_provider.provider_type = ProviderType.HISTORICAL
        del mock_provider.aclose  # Ensure no aclose method
        hub._providers["ObsoleteProvider"] = mock_provider

        # Database returns empty subscriptions - provider is now obsolete
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await hub.refresh_subscriptions()

        # Provider should be removed without error
        assert "ObsoleteProvider" not in hub._providers

    @pytest.mark.asyncio
    async def test_refresh_adds_new_scheduled_job(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions adds new scheduled jobs for subscriptions."""
        hub = datahub_with_mocks

        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical

        # Mock database returning a new subscription
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "TestProvider",
                "interval": "1d",
                "cron": "0 0 * * *",
                "syms": ["SYM1", "SYM2"],
                "exchanges": ["XNAS", "XNAS"]
            }
        ])

        # Start the scheduler so we can add jobs
        hub._sched.start()

        await hub.refresh_subscriptions()

        # Job key should be tracked
        expected_key = "TestProvider|1d|0 0 * * *"
        assert expected_key in hub.job_keys
        # Job should exist in scheduler
        assert hub._sched.get_job(expected_key) is not None

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_updates_existing_job_symbols(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions updates job args when symbols change."""
        hub = datahub_with_mocks

        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical

        # Start the scheduler and add initial job
        hub._sched.start()
        job_key = "TestProvider|1d|0 0 * * *"
        hub.job_keys.add(job_key)
        hub._sched.add_job(
            func=hub.get_data,
            trigger='interval',
            seconds=3600,
            args=["TestProvider", "1d", ["OLD_SYM"], ["XNAS"]],
            id=job_key,
        )

        # Mock database returning updated symbols
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "TestProvider",
                "interval": "1d",
                "cron": "0 0 * * *",
                "syms": ["NEW_SYM1", "NEW_SYM2"],
                "exchanges": ["XNAS", "XNAS"]
            }
        ])

        await hub.refresh_subscriptions()

        # Job should still exist with updated args
        job = hub._sched.get_job(job_key)
        assert job is not None
        assert job.args == ["TestProvider", "1d", ["NEW_SYM1", "NEW_SYM2"], ["XNAS", "XNAS"]]

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_triggers_immediate_pull_for_new_symbols_in_existing_job(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions triggers an immediate pull for newly added symbols."""
        hub = datahub_with_mocks

        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical

        # Start the scheduler and add initial job
        hub._sched.start()
        job_key = "TestProvider|1d|0 0 * * *"
        hub.job_keys.add(job_key)
        hub._sched.add_job(
            func=hub.get_data,
            trigger='interval',
            seconds=3600,
            args=["TestProvider", "1d", ["AAPL"], ["XNAS"]],
            id=job_key,
        )

        # Mock database returning updated symbols with a new one
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {
                "provider": "TestProvider",
                "interval": "1d",
                "cron": "0 0 * * *",
                "syms": ["AAPL", "TSLA"],
                "exchanges": ["XNAS", "XNAS"]
            }
        ])

        # Mock get_data to verify it's called
        with patch.object(hub, 'get_data', new_callable=AsyncMock) as mock_get_data:
            await hub.refresh_subscriptions()

            # Allow control to return to the event loop so the task can start
            await asyncio.sleep(0)

            # Verify get_data was called for TSLA only
            mock_get_data.assert_called_once_with("TestProvider", "1d", ["TSLA"], ["XNAS"])

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_removes_unsubscribed_job(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn, mock_provider_historical
    ):
        """Test that refresh_subscriptions removes jobs for unsubscribed data."""
        hub = datahub_with_mocks

        # Pre-load the provider
        hub._providers["TestProvider"] = mock_provider_historical

        # Start the scheduler and add existing job
        hub._sched.start()
        job_key = "TestProvider|1d|0 0 * * *"
        hub.job_keys.add(job_key)
        hub._sched.add_job(
            func=hub.get_data,
            trigger='interval',
            seconds=3600,
            args=["TestProvider", "1d", ["SYM1"]],
            id=job_key,
        )

        # Mock database returning no subscriptions - job should be removed
        # Note: refresh_subscriptions calls self.pool.fetch() directly, not through a connection
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        await hub.refresh_subscriptions()

        # Job should be removed
        assert job_key not in hub.job_keys
        assert hub._sched.get_job(job_key) is None

        hub._sched.shutdown(wait=False)


class TestGetData:
    """Tests for DataHub.get_data() method."""

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.handlers.collection.TradingCalendar")
    async def test_get_data_historical_provider(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data() works with historical provider."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical

        # Mock last_updated query
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        # Mock insert_bars
        with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
            await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])

            # Verify insert was called (even if with empty batches)
            hub._insert_bars.assert_called()

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.handlers.collection.TradingCalendar")
    async def test_get_data_live_provider(
        self, mock_calendar, datahub_with_mocks, mock_provider_live
    ):
        """Test that get_data() works with live provider."""
        mock_calendar.is_open_now.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestLiveProvider"] = mock_provider_live

        with patch.object(hub, '_insert_bars', new_callable=AsyncMock):
            await hub.get_data("TestLiveProvider", "1h", ["TEST"], ["CRYPTO"])

            hub._insert_bars.assert_called()

    @pytest.mark.asyncio
    async def test_get_data_provider_not_found(
        self, datahub_with_mocks
    ):
        """Test that get_data() returns None for non-existent provider (error caught by safe_job)."""
        hub = datahub_with_mocks

        # get_data is decorated with @safe_job which catches exceptions
        result = await hub.get_data("NonExistentProvider", "1d", ["TEST"], ["XNAS"])
        assert result is None

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.handlers.collection.TradingCalendar")
    async def test_get_data_batch_insertion(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data() batches insertion at 500 bars."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks

        # Create a provider that yields many bars
        async def mock_get_data(reqs):
            for i in range(750):  # More than one batch
                yield Bar(
                    ts=datetime.now(timezone.utc),
                    sym="TEST",
                    o=100.0,
                    h=110.0,
                    l=95.0,
                    c=105.0,
                    v=1000
                )

        mock_provider = Mock()
        mock_provider.provider_type = ProviderType.HISTORICAL
        mock_provider.get_data = mock_get_data
        hub._providers["TestProvider"] = mock_provider

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        insert_calls = []
        async def track_insert(*args, **kwargs):
            insert_calls.append((args, kwargs))

        with patch.object(hub, '_insert_bars', side_effect=track_insert):
            await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])

            # Should have been called twice: once for 500 bars, once for 250
            assert len(insert_calls) == 2

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.handlers.collection.TradingCalendar")
    async def test_get_data_handles_unique_violation_fallback(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn, mock_asyncpg_pool
    ):
        """Test that _insert_bars falls back to INSERT ON CONFLICT when COPY fails with duplicates."""
        mock_calendar.has_sessions_in_range.return_value = True
        import asyncpg.exceptions

        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical

        # Mock last_updated query to return empty (so requests are generated)
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        # First call to copy_records_to_table raises UniqueViolationError
        mock_asyncpg_conn.copy_records_to_table = AsyncMock(
            side_effect=asyncpg.exceptions.UniqueViolationError("duplicate key")
        )
        # Fallback executemany should succeed
        mock_asyncpg_conn.executemany = AsyncMock()

        await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])

        # Verify fallback was used - executemany should have been called
        mock_asyncpg_conn.executemany.assert_called()

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.handlers.collection.TradingCalendar")
    async def test_get_data_reraises_non_unique_db_errors(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that non-UniqueViolation DB errors are caught by @safe_job decorator."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical

        # Mock last_updated query to return empty
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        # Raise a different database error
        mock_asyncpg_conn.copy_records_to_table = AsyncMock(
            side_effect=Exception("Connection lost")
        )

        # get_data is decorated with @safe_job which catches and logs errors
        result = await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])

        # Should return None (default from @safe_job) instead of raising
        assert result is None

    @pytest.mark.asyncio
    @patch("quasar.services.datahub.handlers.collection.TradingCalendar")
    async def test_get_data_historical_no_valid_requests_returns_early(
        self, mock_calendar, datahub_with_mocks, mock_provider_historical, mock_asyncpg_conn
    ):
        """Test that get_data returns early when all symbols are already up-to-date."""
        mock_calendar.has_sessions_in_range.return_value = True
        hub = datahub_with_mocks
        hub._providers["TestProvider"] = mock_provider_historical

        # Mock last_updated query to return symbols that are already up-to-date
        # All symbols have been updated recently, so no requests needed
        from datetime import date
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            {"sym": "TEST", "d": date.today()}  # Already updated today
        ])

        # Track if insert_bars is called
        insert_called = False
        async def track_insert(*args, **kwargs):
            nonlocal insert_called
            insert_called = True

        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with patch.object(hub, '_insert_bars', side_effect=track_insert):
                await hub.get_data("TestProvider", "1d", ["TEST"], ["XNAS"])

            # Should have issued a warning about no valid requests
            # Wait - with new logic, if start > yday, it just continues.
            # In build_reqs_historical, if reqs is empty, get_data returns early.
            pass

        # Insert should never have been called
        assert not insert_called

    @pytest.mark.asyncio
    async def test_get_data_invalid_provider_type_returns_none(
        self, datahub_with_mocks
    ):
        """Test that get_data returns None for provider with invalid type (caught by @safe_job)."""
        hub = datahub_with_mocks

        # Create a mock provider with an invalid provider_type
        mock_provider = Mock()
        mock_provider.provider_type = None  # Invalid type
        hub._providers["BadProvider"] = mock_provider

        # get_data is decorated with @safe_job which catches exceptions
        result = await hub.get_data("BadProvider", "1d", ["TEST"], ["XNAS"])

        # Should return None (default from @safe_job) due to the invalid type causing an error
        assert result is None


class TestRefreshIndexSyncJobs:
    """Tests for DataHub.refresh_index_sync_jobs() method."""

    @pytest.mark.asyncio
    async def test_refresh_index_sync_jobs_schedules_jobs_for_index_providers(
        self, datahub_with_mocks, mock_asyncpg_pool
    ):
        """Test that refresh_index_sync_jobs schedules jobs for each IndexProvider."""
        hub = datahub_with_mocks

        # Mock database returning IndexProviders with their sync frequencies
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {"class_name": "TestIndexProvider", "sync_frequency": "1w"},
            {"class_name": "AnotherIndexProvider", "sync_frequency": "1d"},
        ])

        # Mock the cron query to return cron templates
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=lambda q, interval: {
            "1w": "0 0 * * 1",  # Monday at midnight
            "1d": "0 0 * * *",  # Daily at midnight
            "1M": "0 0 1 * *",  # 1st of month at midnight
        }.get(interval))

        # Start the scheduler
        hub._sched.start()

        await hub.refresh_index_sync_jobs()

        # Verify jobs were scheduled for each IndexProvider
        assert hub._sched.get_job("index_sync_TestIndexProvider") is not None
        assert hub._sched.get_job("index_sync_AnotherIndexProvider") is not None

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_index_sync_jobs_uses_default_frequency_when_none_set(
        self, datahub_with_mocks, mock_asyncpg_pool
    ):
        """Test that refresh_index_sync_jobs uses default 1w frequency when none is configured."""
        hub = datahub_with_mocks

        # Mock database returning IndexProvider with NULL sync_frequency (uses default)
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {"class_name": "DefaultFreqProvider", "sync_frequency": "1w"},  # COALESCE default
        ])

        # Mock the cron query
        mock_asyncpg_pool.fetchval = AsyncMock(return_value="0 0 * * 1")

        hub._sched.start()

        await hub.refresh_index_sync_jobs()

        # Verify job was scheduled with weekly cron
        job = hub._sched.get_job("index_sync_DefaultFreqProvider")
        assert job is not None

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_index_sync_jobs_removes_obsolete_jobs(
        self, datahub_with_mocks, mock_asyncpg_pool
    ):
        """Test that refresh_index_sync_jobs removes jobs for deleted IndexProviders."""
        hub = datahub_with_mocks

        # Start scheduler and add an existing job that's no longer in the database
        hub._sched.start()
        hub._sched.add_job(
            func=lambda: None,
            trigger='interval',
            seconds=3600,
            id="index_sync_DeletedProvider",
        )
        hub.index_sync_job_keys = {"index_sync_DeletedProvider"}

        # Mock database returning empty (provider was deleted)
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)

        await hub.refresh_index_sync_jobs()

        # Verify the obsolete job was removed
        assert hub._sched.get_job("index_sync_DeletedProvider") is None
        assert "index_sync_DeletedProvider" not in hub.index_sync_job_keys

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_refresh_index_sync_jobs_updates_changed_frequency(
        self, datahub_with_mocks, mock_asyncpg_pool
    ):
        """Test that refresh_index_sync_jobs updates jobs when frequency changes."""
        hub = datahub_with_mocks

        # Start scheduler and add an existing job with old frequency
        hub._sched.start()
        hub._sched.add_job(
            func=lambda: None,
            trigger='cron',
            hour=0,
            minute=0,
            day_of_week='mon',  # Weekly (old)
            id="index_sync_ChangedProvider",
        )
        hub.index_sync_job_keys = {"index_sync_ChangedProvider"}

        # Mock database returning provider with new frequency (daily)
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {"class_name": "ChangedProvider", "sync_frequency": "1d"},
        ])

        # Mock cron query for daily schedule
        mock_asyncpg_pool.fetchval = AsyncMock(return_value="0 0 * * *")

        await hub.refresh_index_sync_jobs()

        # Verify job exists and was updated
        job = hub._sched.get_job("index_sync_ChangedProvider")
        assert job is not None
        # The job should have been replaced with the new schedule

        hub._sched.shutdown(wait=False)


class TestSyncIndexConstituents:
    """Tests for DataHub.sync_index_constituents() method."""

    @pytest.mark.asyncio
    async def test_sync_index_constituents_fetches_and_posts_to_registry(
        self, datahub_with_mocks, mock_asyncpg_pool, mock_asyncpg_conn
    ):
        """Test that sync_index_constituents loads provider, fetches constituents, and posts to registry."""
        hub = datahub_with_mocks

        # Create mock IndexProvider
        mock_index_provider = Mock()
        mock_index_provider.name = "TestIndexProvider"
        mock_index_provider.provider_type = ProviderType.INDEX
        mock_index_provider.get_constituents = AsyncMock(return_value=[
            {"symbol": "AAPL", "weight": 0.05},
            {"symbol": "MSFT", "weight": 0.04},
        ])
        mock_index_provider.aclose = AsyncMock()

        # Pre-load the provider
        hub._providers["TestIndexProvider"] = mock_index_provider

        # Mock the HTTP POST to registry
        with patch('quasar.services.datahub.handlers.collection.aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"members_added": 2, "members_removed": 0})

            mock_session = AsyncMock()
            mock_session.post = Mock(return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None)
            ))
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session

            await hub.sync_index_constituents("TestIndexProvider")

            # Verify get_constituents was called
            mock_index_provider.get_constituents.assert_called_once()

            # Verify POST was made to registry
            mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_sync_index_constituents_handles_provider_not_found(
        self, datahub_with_mocks
    ):
        """Test that sync_index_constituents handles missing provider gracefully."""
        hub = datahub_with_mocks

        # Provider not loaded - should handle gracefully (logged, not raised)
        # The method should be decorated with @safe_job
        result = await hub.sync_index_constituents("NonExistentProvider")

        # Should return None without raising (caught by safe_job decorator)
        assert result is None

    @pytest.mark.asyncio
    async def test_sync_index_constituents_handles_api_failure(
        self, datahub_with_mocks
    ):
        """Test that sync_index_constituents handles registry API failures gracefully."""
        hub = datahub_with_mocks

        # Create mock IndexProvider
        mock_index_provider = Mock()
        mock_index_provider.name = "TestIndexProvider"
        mock_index_provider.provider_type = ProviderType.INDEX
        mock_index_provider.get_constituents = AsyncMock(return_value=[
            {"symbol": "AAPL", "weight": 0.05},
        ])
        mock_index_provider.aclose = AsyncMock()

        hub._providers["TestIndexProvider"] = mock_index_provider

        # Mock the HTTP POST to fail
        with patch('quasar.services.datahub.handlers.collection.aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session.post = Mock(side_effect=Exception("Connection refused"))
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session

            # Should handle gracefully (safe_job decorator)
            result = await hub.sync_index_constituents("TestIndexProvider")

            assert result is None

    @pytest.mark.asyncio
    async def test_sync_index_constituents_logs_completion(
        self, datahub_with_mocks, caplog
    ):
        """Test that sync_index_constituents logs sync start and completion."""
        import logging
        hub = datahub_with_mocks

        # Create mock IndexProvider
        mock_index_provider = Mock()
        mock_index_provider.name = "TestIndexProvider"
        mock_index_provider.provider_type = ProviderType.INDEX
        mock_index_provider.get_constituents = AsyncMock(return_value=[
            {"symbol": "AAPL", "weight": 0.05},
        ])
        mock_index_provider.aclose = AsyncMock()

        hub._providers["TestIndexProvider"] = mock_index_provider

        # Mock successful HTTP POST
        with patch('quasar.services.datahub.handlers.collection.aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json = AsyncMock(return_value={"members_added": 1, "members_removed": 0})

            mock_session = AsyncMock()
            mock_session.post = Mock(return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock(return_value=None)
            ))
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_class.return_value = mock_session

            with caplog.at_level(logging.INFO):
                await hub.sync_index_constituents("TestIndexProvider")

            # Check that appropriate log messages were generated
            assert any("TestIndexProvider" in record.message for record in caplog.records)


class TestHandleRefreshIndexSyncJobs:
    """Tests for DataHub.handle_refresh_index_sync_jobs() endpoint handler."""

    @pytest.mark.asyncio
    async def test_handle_refresh_returns_success(self, datahub_with_mocks, mock_asyncpg_pool):
        """Test that handle_refresh_index_sync_jobs returns success status."""
        hub = datahub_with_mocks

        # Mock database returning no IndexProviders
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        response = await hub.handle_refresh_index_sync_jobs()

        assert response.status == "success"
        assert response.job_count == 0

    @pytest.mark.asyncio
    async def test_handle_refresh_calls_refresh_index_sync_jobs(
        self, datahub_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_refresh_index_sync_jobs calls the underlying refresh method."""
        hub = datahub_with_mocks

        # Mock database returning IndexProviders
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[
            {"class_name": "TestIndex", "sync_frequency": "1w"},
        ])
        mock_asyncpg_pool.fetchval = AsyncMock(return_value="0 0 * * 1")

        # Start scheduler for job scheduling
        hub._sched.start()

        response = await hub.handle_refresh_index_sync_jobs()

        # Should have created 1 job
        assert response.status == "success"
        assert response.job_count == 1
        assert "index_sync_TestIndex" in hub.index_sync_job_keys

        hub._sched.shutdown(wait=False)

    @pytest.mark.asyncio
    async def test_handle_refresh_logs_api_trigger(self, datahub_with_mocks, mock_asyncpg_pool, caplog):
        """Test that handle_refresh_index_sync_jobs logs the API trigger."""
        import logging
        hub = datahub_with_mocks

        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        with caplog.at_level(logging.INFO):
            await hub.handle_refresh_index_sync_jobs()

        assert any("refresh triggered via API" in record.message for record in caplog.records)
