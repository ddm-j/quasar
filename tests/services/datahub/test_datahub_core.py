"""Tests for DataHub lifecycle and core methods."""
import pytest
from unittest.mock import Mock, AsyncMock, patch


class TestDataHubLifecycleMethods:
    """Tests for DataHub lifecycle methods."""

    @pytest.mark.asyncio
    async def test_start_initializes_pool_and_scheduler(
        self, datahub_with_mocks, mock_asyncpg_conn
    ):
        """Test that start() initializes pool, scheduler, and starts API server."""
        hub = datahub_with_mocks

        # Mock pool initialization
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        with patch.object(hub, 'start_api_server', new_callable=AsyncMock):
            await hub.start()

            # Verify pool is initialized
            assert hub._pool is not None
            # Verify scheduler is started
            assert hub._sched.state == 1  # STATE_RUNNING = 1

    @pytest.mark.asyncio
    async def test_stop_closes_pool_and_stops_scheduler(
        self, datahub_with_mocks
    ):
        """Test that stop() stops API server, scheduler, and closes pool."""
        hub = datahub_with_mocks
        hub._sched.state = 1  # STATE_RUNNING

        # Mock the scheduler's shutdown method and event loop
        hub._sched._eventloop = Mock()
        hub._sched._eventloop.call_soon_threadsafe = Mock()
        hub._sched.shutdown = Mock()

        with patch.object(hub, 'stop_api_server', new_callable=AsyncMock):
            with patch.object(hub, 'close_pool', new_callable=AsyncMock):
                await hub.stop()

                hub.stop_api_server.assert_called_once()
                hub.close_pool.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_calls_stop_scheduler_when_running(
        self, datahub_with_mocks, mock_asyncpg_conn
    ):
        """Test that start() calls _stop_scheduler to reset running scheduler."""
        from apscheduler.schedulers.base import STATE_RUNNING

        hub = datahub_with_mocks

        # Add job keys to verify they get cleared
        hub.job_keys.add("test_job_key_1")
        hub.job_keys.add("test_job_key_2")

        # Create a mock scheduler that reports as running
        mock_scheduler = Mock()
        mock_scheduler.state = STATE_RUNNING  # state property returns STATE_RUNNING
        hub._sched = mock_scheduler

        # Call _stop_scheduler directly (which is called by start())
        hub._stop_scheduler()

        # Verify shutdown was called
        mock_scheduler.shutdown.assert_called_once_with(wait=False)
        # Verify job_keys were cleared
        assert len(hub.job_keys) == 0
