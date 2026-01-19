"""Integration tests for provider configuration endpoints.

Tests cover:
- T029: Contract test for GET /api/registry/config/schema endpoint
"""

import pytest
from unittest.mock import AsyncMock, Mock
from fastapi.testclient import TestClient

from quasar.lib.providers.core import (
    HistoricalDataProvider,
    LiveDataProvider,
    IndexProvider,
)


class TestGetConfigSchemaEndpoint:
    """T029: Contract tests for GET /api/registry/config/schema endpoint."""

    def test_schema_endpoint_returns_200_for_historical_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema endpoint returns 200 for valid historical provider."""
        # Mock database to return Historical subtype (production value)
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestHistoricalProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestHistoricalProvider"
        assert data["class_type"] == "provider"
        assert data["class_subtype"] == "Historical"
        assert "schema" in data

    def test_schema_endpoint_returns_200_for_realtime_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema endpoint returns 200 for valid realtime provider."""
        mock_asyncpg_pool.fetchval.return_value = "Live"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestLiveProvider"
        assert data["class_subtype"] == "Live"

    def test_schema_endpoint_returns_200_for_index_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema endpoint returns 200 for valid index provider."""
        mock_asyncpg_pool.fetchval.return_value = "IndexProvider"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestIndexProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestIndexProvider"
        assert data["class_subtype"] == "IndexProvider"

    def test_schema_endpoint_returns_404_for_unknown_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema endpoint returns 404 for non-existent provider."""
        mock_asyncpg_pool.fetchval.return_value = None

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "NonExistent", "class_type": "provider"}
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_schema_endpoint_requires_class_name_param(
        self,
        registry_client: TestClient
    ):
        """Schema endpoint requires class_name query parameter."""
        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_type": "provider"}
        )

        assert response.status_code == 422  # Validation error

    def test_schema_endpoint_requires_class_type_param(
        self,
        registry_client: TestClient
    ):
        """Schema endpoint requires class_type query parameter."""
        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider"}
        )

        assert response.status_code == 422  # Validation error

    def test_historical_schema_contains_scheduling_delay_hours(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Historical provider schema includes scheduling.delay_hours."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        assert "scheduling" in schema
        assert "delay_hours" in schema["scheduling"]

    def test_historical_schema_contains_data_lookback_days(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Historical provider schema includes data.lookback_days."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        assert "data" in schema
        assert "lookback_days" in schema["data"]

    def test_realtime_schema_contains_pre_post_close_seconds(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Realtime provider schema includes pre_close_seconds and post_close_seconds."""
        mock_asyncpg_pool.fetchval.return_value = "Live"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        assert "scheduling" in schema
        assert "pre_close_seconds" in schema["scheduling"]
        assert "post_close_seconds" in schema["scheduling"]

    def test_realtime_schema_does_not_contain_data_category(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Realtime provider schema does not include data category."""
        mock_asyncpg_pool.fetchval.return_value = "Live"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        assert "data" not in schema

    def test_index_schema_only_contains_crypto(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Index provider schema only includes crypto category."""
        mock_asyncpg_pool.fetchval.return_value = "IndexProvider"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestIndexProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        assert "crypto" in schema
        assert "scheduling" not in schema
        assert "data" not in schema

    def test_all_schemas_include_crypto_category(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """All provider types include crypto category from base DataProvider."""
        for subtype in ["Historical", "Live", "IndexProvider"]:
            mock_asyncpg_pool.fetchval.return_value = subtype

            response = registry_client.get(
                "/api/registry/config/schema",
                params={"class_name": f"Test{subtype.title()}Provider", "class_type": "provider"}
            )

            assert response.status_code == 200
            schema = response.json()["schema"]
            assert "crypto" in schema, f"crypto category missing for {subtype}"
            assert "preferred_quote_currency" in schema["crypto"]

    def test_schema_field_has_type_info(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema fields include type information as string."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        delay_hours = schema["scheduling"]["delay_hours"]
        # Type is serialized as JSON Schema type name (e.g., "integer", "string")
        assert "type" in delay_hours
        assert delay_hours["type"] == "integer"

    def test_schema_field_has_bounds_info(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema fields include min/max bounds."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        delay_hours = schema["scheduling"]["delay_hours"]
        assert "min" in delay_hours
        assert "max" in delay_hours
        assert delay_hours["min"] == 0
        assert delay_hours["max"] == 24

    def test_schema_field_has_default_value(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema fields include default values."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        delay_hours = schema["scheduling"]["delay_hours"]
        assert "default" in delay_hours
        assert delay_hours["default"] == 0

    def test_schema_field_has_description(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema fields include descriptions."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        delay_hours = schema["scheduling"]["delay_hours"]
        assert "description" in delay_hours
        assert len(delay_hours["description"]) > 0


class TestHistoricalProviderDelayOffset:
    """T037: Integration tests for historical provider job delay offset."""

    def test_historical_delay_offset_conversion(self):
        """Historical provider delay_hours is correctly converted to offset_seconds."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Test various delay_hours values and their conversion to offset_seconds
        # This tests the conversion logic used in refresh_subscriptions

        # delay_hours=6 should become offset_seconds=21600
        delay_hours = 6
        offset_seconds = delay_hours * 3600  # Same conversion as in collection.py
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",
            offset_seconds=offset_seconds,
            timezone="UTC"
        )
        assert trigger.offset_seconds == 21600
        assert trigger._sign == 1  # Positive offset

    def test_historical_default_zero_offset(self):
        """Historical provider with no delay_hours configured uses zero offset."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # When no preferences, delay_hours defaults to 0
        delay_hours = 0
        offset_seconds = delay_hours * 3600

        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",
            offset_seconds=offset_seconds,
            timezone="UTC"
        )
        assert trigger.offset_seconds == 0
        assert trigger._sign == 1

    def test_offset_seconds_calculation_for_all_valid_delay_hours(self):
        """Verify offset calculation for all valid delay_hours values (0-24)."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Test boundary and common values
        test_values = [0, 1, 6, 12, 23, 24]  # min, common values, max

        for delay_hours in test_values:
            offset_seconds = delay_hours * 3600
            trigger = OffsetCronTrigger.from_crontab(
                "0 0 * * *",
                offset_seconds=offset_seconds,
                timezone="UTC"
            )
            expected_offset = delay_hours * 3600
            assert trigger.offset_seconds == expected_offset, \
                f"For delay_hours={delay_hours}, expected offset {expected_offset}, got {trigger.offset_seconds}"
            assert trigger._sign == 1, f"Historical provider should have positive offset for delay_hours={delay_hours}"

    def test_historical_job_scheduled_at_correct_time_with_delay(self):
        """Historical provider with delay_hours=6 fires 6 hours after cron time."""
        from datetime import datetime, timezone, timedelta
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Create trigger for midnight UTC with 6 hour delay
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight UTC
            offset_seconds=6 * 3600,  # 6 hours delay
            timezone="UTC"
        )

        # Simulate "now" as 2024-01-14 at 23:00 UTC (before midnight)
        # This ensures the next cron fire is midnight Jan 15, then +6 hours = 6:00 AM Jan 15
        now = datetime(2024, 1, 14, 23, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # The cron would fire at midnight Jan 15, with +6 hours offset
        # it should fire at 06:00 UTC on 2024-01-15
        expected_fire = datetime(2024, 1, 15, 6, 0, 0, tzinfo=timezone.utc)
        assert next_fire == expected_fire, f"Expected {expected_fire}, got {next_fire}"

    def test_historical_job_next_day_when_past_delayed_time(self):
        """Historical provider schedules next day when past delayed fire time."""
        from datetime import datetime, timezone, timedelta
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Create trigger for midnight UTC with 6 hour delay
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight UTC
            offset_seconds=6 * 3600,  # 6 hours delay
            timezone="UTC"
        )

        # Simulate "now" as 2024-01-15 at 10:00 UTC (past the 06:00 fire time)
        now = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # Should schedule for next day at 06:00 UTC
        expected_fire = datetime(2024, 1, 16, 6, 0, 0, tzinfo=timezone.utc)
        assert next_fire == expected_fire, f"Expected {expected_fire}, got {next_fire}"


class TestLiveProviderPreCloseOffset:
    """T046: Integration tests for live provider job pre_close offset."""

    def test_live_pre_close_offset_conversion(self):
        """Live provider pre_close_seconds is correctly converted to negative offset."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Test various pre_close_seconds values and their conversion to negative offset
        # This tests the conversion logic used in refresh_subscriptions

        # pre_close_seconds=60 should become offset_seconds=-60
        pre_close_seconds = 60
        offset_seconds = -1 * pre_close_seconds  # Same conversion as in collection.py
        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM UTC (typical market close)
            offset_seconds=offset_seconds,
            timezone="UTC"
        )
        assert trigger.offset_seconds == 60
        assert trigger._sign == -1  # Negative offset

    def test_live_default_pre_close_offset(self):
        """Live provider with default pre_close_seconds uses default negative offset."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Default pre_close_seconds is 30 (DEFAULT_LIVE_OFFSET)
        pre_close_seconds = 30
        offset_seconds = -1 * pre_close_seconds

        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",
            offset_seconds=offset_seconds,
            timezone="UTC"
        )
        assert trigger.offset_seconds == 30
        assert trigger._sign == -1

    def test_offset_seconds_calculation_for_valid_pre_close_values(self):
        """Verify offset calculation for valid pre_close_seconds values (0-300)."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Test boundary and common values
        test_values = [0, 30, 60, 120, 180, 300]  # min, default, common values, max

        for pre_close_seconds in test_values:
            offset_seconds = -1 * pre_close_seconds
            trigger = OffsetCronTrigger.from_crontab(
                "0 16 * * *",
                offset_seconds=offset_seconds,
                timezone="UTC"
            )
            # For zero, the trigger still stores 0 and sign doesn't matter
            if pre_close_seconds == 0:
                assert trigger.offset_seconds == 0
            else:
                assert trigger.offset_seconds == pre_close_seconds, \
                    f"For pre_close_seconds={pre_close_seconds}, expected offset {pre_close_seconds}, got {trigger.offset_seconds}"
                assert trigger._sign == -1, f"Live provider should have negative offset for pre_close_seconds={pre_close_seconds}"

    def test_live_job_scheduled_before_cron_time_with_pre_close(self):
        """Live provider with pre_close_seconds=60 fires 60 seconds before cron time."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Create trigger for 4 PM UTC with 60 seconds pre_close (negative offset)
        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM UTC
            offset_seconds=-60,  # 60 seconds before
            timezone="UTC"
        )

        # Simulate "now" as 2024-01-14 at 15:00 UTC (before 4 PM)
        now = datetime(2024, 1, 14, 15, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # The cron would fire at 16:00, with -60 seconds offset
        # it should fire at 15:59:00 UTC on 2024-01-14
        expected_fire = datetime(2024, 1, 14, 15, 59, 0, tzinfo=timezone.utc)
        assert next_fire == expected_fire, f"Expected {expected_fire}, got {next_fire}"

    def test_live_job_next_day_when_past_pre_close_time(self):
        """Live provider schedules next day when past pre_close fire time."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Create trigger for 4 PM UTC with 60 seconds pre_close
        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM UTC
            offset_seconds=-60,  # 60 seconds before
            timezone="UTC"
        )

        # Simulate "now" as 2024-01-14 at 17:00 UTC (past the 15:59 fire time)
        now = datetime(2024, 1, 14, 17, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # Should schedule for next day at 15:59 UTC
        expected_fire = datetime(2024, 1, 15, 15, 59, 0, tzinfo=timezone.utc)
        assert next_fire == expected_fire, f"Expected {expected_fire}, got {next_fire}"

    def test_live_job_max_pre_close_seconds(self):
        """Live provider with maximum pre_close_seconds=300 fires 5 minutes early."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Create trigger for 4 PM UTC with 300 seconds (5 minutes) pre_close
        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM UTC
            offset_seconds=-300,  # 5 minutes before
            timezone="UTC"
        )

        # Simulate "now" as 2024-01-14 at 15:00 UTC
        now = datetime(2024, 1, 14, 15, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # Should fire at 15:55 UTC (5 minutes before 16:00)
        expected_fire = datetime(2024, 1, 14, 15, 55, 0, tzinfo=timezone.utc)
        assert next_fire == expected_fire, f"Expected {expected_fire}, got {next_fire}"

    def test_live_provider_fires_before_historical_at_same_cron(self):
        """Live provider fires before cron time, historical fires after."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Both providers scheduled for midnight
        now = datetime(2024, 1, 14, 23, 0, 0, tzinfo=timezone.utc)

        # Live provider with pre_close_seconds=60 (fires 1 minute BEFORE midnight)
        live_trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight
            offset_seconds=-60,  # 1 minute before
            timezone="UTC"
        )
        live_fire = live_trigger.get_next_fire_time(None, now)

        # Historical provider with delay_hours=1 (fires 1 hour AFTER midnight)
        historical_trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight
            offset_seconds=3600,  # 1 hour after
            timezone="UTC"
        )
        historical_fire = historical_trigger.get_next_fire_time(None, now)

        # Live should fire at 23:59 (before midnight)
        assert live_fire == datetime(2024, 1, 14, 23, 59, 0, tzinfo=timezone.utc)
        # Historical should fire at 01:00 (after midnight)
        assert historical_fire == datetime(2024, 1, 15, 1, 0, 0, tzinfo=timezone.utc)
        # Live fires before historical
        assert live_fire < historical_fire


class TestLookbackDaysIntegration:
    """T054: Integration tests for lookback_days in new subscriptions."""

    @pytest.fixture
    def collection_handler_with_prefs(
        self,
        datahub_with_mocks,
        mock_asyncpg_conn
    ):
        """Create a CollectionHandlersMixin instance with provider preferences."""
        # The datahub_with_mocks has CollectionHandlersMixin mixed in
        return datahub_with_mocks

    @pytest.mark.asyncio
    async def test_new_subscription_uses_configured_lookback_days(
        self,
        collection_handler_with_prefs,
        mock_asyncpg_conn
    ):
        """New subscription start date uses configured lookback_days preference."""
        from datetime import datetime, timezone, timedelta

        handler = collection_handler_with_prefs

        # Configure provider preferences with custom lookback_days=365
        handler._provider_preferences = {
            "TestHistoricalProvider": {
                "data": {"lookback_days": 365}
            }
        }

        # Mock database to return no last_updated (new subscription)
        mock_asyncpg_conn.fetch.return_value = []

        # Call _build_reqs_historical
        reqs = await handler._build_reqs_historical(
            provider="TestHistoricalProvider",
            interval="1d",
            symbols=["AAPL"],
            exchanges=["XNAS"]
        )

        # Verify request was created
        assert len(reqs) == 1
        req = reqs[0]

        # Calculate expected start date
        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)
        # lookback_days=365, so default_start = yday - 365 days
        # start = default_start + 1 day
        expected_start = yday - timedelta(days=365) + timedelta(days=1)

        assert req.start == expected_start
        assert req.sym == "AAPL"

    @pytest.mark.asyncio
    async def test_new_subscription_uses_default_lookback_when_no_preference(
        self,
        collection_handler_with_prefs,
        mock_asyncpg_conn
    ):
        """New subscription uses DEFAULT_LOOKBACK when no preference is set."""
        from datetime import datetime, timezone, timedelta
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK

        handler = collection_handler_with_prefs

        # No preferences configured
        handler._provider_preferences = {}

        # Mock database to return no last_updated (new subscription)
        mock_asyncpg_conn.fetch.return_value = []

        # Call _build_reqs_historical
        reqs = await handler._build_reqs_historical(
            provider="TestHistoricalProvider",
            interval="1d",
            symbols=["AAPL"],
            exchanges=["XNAS"]
        )

        # Verify request was created
        assert len(reqs) == 1
        req = reqs[0]

        # Calculate expected start date with DEFAULT_LOOKBACK
        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)
        expected_start = yday - timedelta(days=DEFAULT_LOOKBACK) + timedelta(days=1)

        assert req.start == expected_start

    @pytest.mark.asyncio
    async def test_lookback_days_only_applies_to_new_subscriptions(
        self,
        collection_handler_with_prefs,
        mock_asyncpg_conn
    ):
        """Existing subscriptions use incremental update, not lookback_days."""
        from datetime import datetime, timezone, timedelta

        handler = collection_handler_with_prefs

        # Configure provider with custom lookback_days
        handler._provider_preferences = {
            "TestHistoricalProvider": {
                "data": {"lookback_days": 30}  # Short lookback
            }
        }

        # Mock database to return existing last_updated
        last_updated = datetime.now(timezone.utc).date() - timedelta(days=5)
        mock_asyncpg_conn.fetch.return_value = [
            {"sym": "AAPL", "d": last_updated}
        ]

        # Call _build_reqs_historical
        reqs = await handler._build_reqs_historical(
            provider="TestHistoricalProvider",
            interval="1d",
            symbols=["AAPL"],
            exchanges=["XNAS"]
        )

        # Verify request was created with incremental start (day after last_updated)
        assert len(reqs) == 1
        req = reqs[0]

        # For existing subscriptions, start = last_updated + 1 day
        expected_start = last_updated + timedelta(days=1)
        assert req.start == expected_start

    @pytest.mark.asyncio
    async def test_lookback_days_boundary_values(
        self,
        collection_handler_with_prefs,
        mock_asyncpg_conn
    ):
        """Lookback days boundary values (1 and 8000) work correctly."""
        from datetime import datetime, timezone, timedelta

        handler = collection_handler_with_prefs

        # Mock database to return no last_updated (new subscription)
        mock_asyncpg_conn.fetch.return_value = []

        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)

        # Test minimum lookback_days=1
        handler._provider_preferences = {
            "TestHistoricalProvider": {"data": {"lookback_days": 1}}
        }
        reqs = await handler._build_reqs_historical(
            provider="TestHistoricalProvider",
            interval="1d",
            symbols=["AAPL"],
            exchanges=["XNAS"]
        )
        assert len(reqs) == 1
        expected_start_min = yday - timedelta(days=1) + timedelta(days=1)
        assert reqs[0].start == expected_start_min

        # Test maximum lookback_days=8000
        handler._provider_preferences = {
            "TestHistoricalProvider": {"data": {"lookback_days": 8000}}
        }
        reqs = await handler._build_reqs_historical(
            provider="TestHistoricalProvider",
            interval="1d",
            symbols=["MSFT"],
            exchanges=["XNAS"]
        )
        assert len(reqs) == 1
        expected_start_max = yday - timedelta(days=8000) + timedelta(days=1)
        assert reqs[0].start == expected_start_max


class TestGetSecretKeysEndpoint:
    """T069: Contract tests for GET /api/registry/config/secret-keys endpoint."""

    def test_secret_keys_endpoint_returns_200_for_provider_with_secrets(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Secret keys endpoint returns 200 and key names for provider with secrets."""
        import json

        # Mock database to return provider with encrypted secrets
        mock_asyncpg_pool.fetchrow.return_value = {
            'file_hash': b'test_file_hash_12345',
            'nonce': b'test_nonce_123',
            'ciphertext': b'encrypted_data'
        }

        # Mock the decryption to return a secrets dict
        mock_derived_context = Mock()
        mock_derived_context.decrypt = Mock(return_value=json.dumps({
            "api_key": "secret_key_value",
            "api_secret": "secret_secret_value"
        }).encode('utf-8'))
        registry_with_mocks.system_context.get_derived_context = Mock(return_value=mock_derived_context)

        response = registry_client.get(
            "/api/registry/config/secret-keys",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestProvider"
        assert data["class_type"] == "provider"
        assert "keys" in data
        assert set(data["keys"]) == {"api_key", "api_secret"}

    def test_secret_keys_endpoint_returns_empty_for_provider_without_secrets(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Secret keys endpoint returns empty list when provider has no stored secrets."""
        # Mock database to return provider without secrets (nonce/ciphertext are None)
        mock_asyncpg_pool.fetchrow.return_value = {
            'file_hash': b'test_file_hash_12345',
            'nonce': None,
            'ciphertext': None
        }

        response = registry_client.get(
            "/api/registry/config/secret-keys",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestProvider"
        assert data["class_type"] == "provider"
        assert data["keys"] == []

    def test_secret_keys_endpoint_returns_404_for_unknown_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Secret keys endpoint returns 404 for non-existent provider."""
        mock_asyncpg_pool.fetchrow.return_value = None

        response = registry_client.get(
            "/api/registry/config/secret-keys",
            params={"class_name": "NonExistent", "class_type": "provider"}
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_secret_keys_endpoint_requires_class_name_param(
        self,
        registry_client: TestClient
    ):
        """Secret keys endpoint requires class_name query parameter."""
        response = registry_client.get(
            "/api/registry/config/secret-keys",
            params={"class_type": "provider"}
        )

        assert response.status_code == 422  # Validation error

    def test_secret_keys_endpoint_requires_class_type_param(
        self,
        registry_client: TestClient
    ):
        """Secret keys endpoint requires class_type query parameter."""
        response = registry_client.get(
            "/api/registry/config/secret-keys",
            params={"class_name": "TestProvider"}
        )

        assert response.status_code == 422  # Validation error

    def test_secret_keys_endpoint_returns_only_key_names_not_values(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Secret keys endpoint only returns key names, never secret values."""
        import json

        # Mock database to return provider with encrypted secrets
        mock_asyncpg_pool.fetchrow.return_value = {
            'file_hash': b'test_file_hash_12345',
            'nonce': b'test_nonce_123',
            'ciphertext': b'encrypted_data'
        }

        # Mock decryption to return secrets with sensitive values
        mock_derived_context = Mock()
        mock_derived_context.decrypt = Mock(return_value=json.dumps({
            "api_key": "super_secret_key_do_not_expose",
            "password": "very_sensitive_password"
        }).encode('utf-8'))
        registry_with_mocks.system_context.get_derived_context = Mock(return_value=mock_derived_context)

        response = registry_client.get(
            "/api/registry/config/secret-keys",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        # Should only contain key names
        assert "keys" in data
        assert "api_key" in data["keys"]
        assert "password" in data["keys"]
        # Should NOT contain the secret values anywhere in response
        response_text = response.text
        assert "super_secret_key_do_not_expose" not in response_text
        assert "very_sensitive_password" not in response_text


class TestUpdateSecretsEndpoint:
    """T070: Contract tests for PATCH /api/registry/config/secrets endpoint."""

    def test_update_secrets_endpoint_returns_200_on_success(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        mock_aiohttp_session
    ):
        """Update secrets endpoint returns 200 and updated key names on success."""
        # Mock database to return provider file_hash
        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        # Mock encryption to succeed
        registry_with_mocks.system_context.create_context_data = Mock(
            return_value=(b'new_nonce', b'new_ciphertext')
        )

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {"api_key": "new_key_value", "api_secret": "new_secret_value"}}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "updated"
        assert set(data["keys"]) == {"api_key", "api_secret"}

    def test_update_secrets_endpoint_returns_404_for_unknown_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Update secrets endpoint returns 404 for non-existent provider."""
        mock_asyncpg_pool.fetchval.return_value = None

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "NonExistent", "class_type": "provider"},
            json={"secrets": {"api_key": "value"}}
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_update_secrets_endpoint_returns_400_for_empty_secrets(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Update secrets endpoint returns 400 when secrets dict is empty."""
        # No need to mock database - validation should fail first
        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {}}
        )

        assert response.status_code == 400
        assert "empty" in response.json()["detail"].lower()

    def test_update_secrets_endpoint_requires_class_name_param(
        self,
        registry_client: TestClient
    ):
        """Update secrets endpoint requires class_name query parameter."""
        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_type": "provider"},
            json={"secrets": {"api_key": "value"}}
        )

        assert response.status_code == 422  # Validation error

    def test_update_secrets_endpoint_requires_class_type_param(
        self,
        registry_client: TestClient
    ):
        """Update secrets endpoint requires class_type query parameter."""
        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider"},
            json={"secrets": {"api_key": "value"}}
        )

        assert response.status_code == 422  # Validation error

    def test_update_secrets_generates_new_nonce(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        mock_aiohttp_session
    ):
        """Update secrets endpoint generates new nonce for re-encryption (FR-016)."""
        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        # Track that create_context_data is called (which generates new nonce)
        mock_create_context = Mock(return_value=(b'new_unique_nonce', b'new_ciphertext'))
        registry_with_mocks.system_context.create_context_data = mock_create_context

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {"api_key": "new_value"}}
        )

        assert response.status_code == 200
        # Verify create_context_data was called (which generates new nonce)
        mock_create_context.assert_called_once()

    def test_update_secrets_accepts_multiple_credentials(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        mock_aiohttp_session
    ):
        """Update secrets endpoint accepts multiple credential key-value pairs."""
        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        registry_with_mocks.system_context.create_context_data = Mock(
            return_value=(b'new_nonce', b'new_ciphertext')
        )

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {
                "api_key": "key1",
                "api_secret": "secret1",
                "password": "pass1",
                "token": "token1"
            }}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["keys"]) == 4
        assert set(data["keys"]) == {"api_key", "api_secret", "password", "token"}


class TestSchemaEndpointCompleteMetadata:
    """T075-T078: Contract tests for schema endpoint returning complete metadata.

    These tests verify that the schema endpoint returns JSON Schema-compatible
    type names and complete field metadata (type, min, max, default, description)
    per the enhancements in T073-T074.
    """

    def test_historical_delay_hours_has_complete_metadata(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T075: Schema returns scheduling.delay_hours for historical with complete metadata."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestHistoricalProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify scheduling.delay_hours exists with complete metadata
        assert "scheduling" in schema
        delay_hours = schema["scheduling"]["delay_hours"]

        # Type is serialized as JSON Schema type name (per T074)
        assert delay_hours["type"] == "integer"
        assert delay_hours["default"] == 0
        assert delay_hours["min"] == 0
        assert delay_hours["max"] == 24
        assert "description" in delay_hours
        assert len(delay_hours["description"]) > 0

    def test_historical_lookback_days_has_complete_metadata(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T075: Schema returns data.lookback_days for historical with complete metadata."""
        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestHistoricalProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify data.lookback_days exists with complete metadata
        assert "data" in schema
        lookback_days = schema["data"]["lookback_days"]

        # Type is serialized as JSON Schema type name (per T074)
        assert lookback_days["type"] == "integer"
        assert lookback_days["default"] == 8000
        assert lookback_days["min"] == 1
        assert lookback_days["max"] == 8000
        assert "description" in lookback_days
        assert len(lookback_days["description"]) > 0

    def test_live_pre_close_seconds_has_complete_metadata(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T076: Schema returns scheduling.pre_close_seconds for live with complete metadata."""
        mock_asyncpg_pool.fetchval.return_value = "Live"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify scheduling.pre_close_seconds exists with complete metadata
        assert "scheduling" in schema
        pre_close = schema["scheduling"]["pre_close_seconds"]

        # Type is serialized as JSON Schema type name (per T074)
        assert pre_close["type"] == "integer"
        assert pre_close["default"] == 30
        assert pre_close["min"] == 0
        assert pre_close["max"] == 300
        assert "description" in pre_close
        assert len(pre_close["description"]) > 0

    def test_live_post_close_seconds_has_complete_metadata(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T076: Schema returns scheduling.post_close_seconds for live with complete metadata."""
        mock_asyncpg_pool.fetchval.return_value = "Live"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify scheduling.post_close_seconds exists with complete metadata
        assert "scheduling" in schema
        post_close = schema["scheduling"]["post_close_seconds"]

        # Type is serialized as JSON Schema type name (per T074)
        assert post_close["type"] == "integer"
        assert post_close["default"] == 5
        assert post_close["min"] == 0
        assert post_close["max"] == 60
        assert "description" in post_close
        assert len(post_close["description"]) > 0

    def test_index_schema_only_has_crypto(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T077: Schema returns only crypto category for index providers."""
        mock_asyncpg_pool.fetchval.return_value = "IndexProvider"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestIndexProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Index providers only have crypto category (inherited from DataProvider)
        assert "crypto" in schema
        assert "scheduling" not in schema, "Index providers should not have scheduling category"
        assert "data" not in schema, "Index providers should not have data category"

        # Verify crypto.preferred_quote_currency has expected metadata
        quote_currency = schema["crypto"]["preferred_quote_currency"]
        assert quote_currency["type"] == "string"
        assert quote_currency["default"] is None
        assert "description" in quote_currency

    def test_schema_matches_configurable_definition_historical(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T078: Schema response matches HistoricalDataProvider.CONFIGURABLE definition."""
        from quasar.lib.providers.core import HistoricalDataProvider

        mock_asyncpg_pool.fetchval.return_value = "Historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestHistoricalProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify all categories from CONFIGURABLE are present
        for category in HistoricalDataProvider.CONFIGURABLE.keys():
            assert category in schema, f"Category '{category}' missing from schema"

        # Verify all fields from CONFIGURABLE are present with correct metadata
        for category, fields in HistoricalDataProvider.CONFIGURABLE.items():
            for field_name, field_def in fields.items():
                assert field_name in schema[category], \
                    f"Field '{category}.{field_name}' missing from schema"

                schema_field = schema[category][field_name]
                # Verify key metadata fields are present
                assert "type" in schema_field, f"Type missing for {category}.{field_name}"
                assert "default" in schema_field, f"Default missing for {category}.{field_name}"
                assert "description" in schema_field, f"Description missing for {category}.{field_name}"

                # Verify min/max if present in CONFIGURABLE
                if "min" in field_def:
                    assert schema_field["min"] == field_def["min"]
                if "max" in field_def:
                    assert schema_field["max"] == field_def["max"]

    def test_schema_matches_configurable_definition_live(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T078: Schema response matches LiveDataProvider.CONFIGURABLE definition."""
        from quasar.lib.providers.core import LiveDataProvider

        mock_asyncpg_pool.fetchval.return_value = "Live"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify all categories from CONFIGURABLE are present
        for category in LiveDataProvider.CONFIGURABLE.keys():
            assert category in schema, f"Category '{category}' missing from schema"

        # Verify all fields from CONFIGURABLE are present with correct metadata
        for category, fields in LiveDataProvider.CONFIGURABLE.items():
            for field_name, field_def in fields.items():
                assert field_name in schema[category], \
                    f"Field '{category}.{field_name}' missing from schema"

                schema_field = schema[category][field_name]
                # Verify key metadata fields are present
                assert "type" in schema_field, f"Type missing for {category}.{field_name}"
                assert "default" in schema_field, f"Default missing for {category}.{field_name}"
                assert "description" in schema_field, f"Description missing for {category}.{field_name}"

                # Verify min/max if present in CONFIGURABLE
                if "min" in field_def:
                    assert schema_field["min"] == field_def["min"]
                if "max" in field_def:
                    assert schema_field["max"] == field_def["max"]

    def test_schema_matches_configurable_definition_index(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """T078: Schema response matches IndexProvider.CONFIGURABLE definition."""
        from quasar.lib.providers.core import IndexProvider

        mock_asyncpg_pool.fetchval.return_value = "IndexProvider"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestIndexProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]

        # Verify all categories from CONFIGURABLE are present
        for category in IndexProvider.CONFIGURABLE.keys():
            assert category in schema, f"Category '{category}' missing from schema"

        # Index provider should only have crypto category
        assert len(schema.keys()) == len(IndexProvider.CONFIGURABLE.keys()), \
            "Index provider schema should match CONFIGURABLE exactly (only crypto)"


class TestCredentialUpdateUnload:
    """T071: Integration tests for credential update triggering provider unload."""

    def test_credential_update_triggers_datahub_unload(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        mock_aiohttp_session
    ):
        """Credential update for provider triggers DataHub unload endpoint."""
        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        registry_with_mocks.system_context.create_context_data = Mock(
            return_value=(b'new_nonce', b'new_ciphertext')
        )

        # Configure mock response for DataHub unload
        mock_aiohttp_session["response"].status = 200

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {"api_key": "new_value"}}
        )

        assert response.status_code == 200
        # Verify unload endpoint was called
        mock_aiohttp_session["session"].post.assert_called()
        # The URL should be for the provider unload
        call_args = mock_aiohttp_session["session"].post.call_args
        assert "providers/TestProvider/unload" in str(call_args)

    def test_credential_update_succeeds_when_datahub_unreachable(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        monkeypatch
    ):
        """Credential update succeeds even when DataHub is unreachable."""

        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        registry_with_mocks.system_context.create_context_data = Mock(
            return_value=(b'new_nonce', b'new_ciphertext')
        )

        # Mock aiohttp to raise a generic connection error
        class MockClientSession:
            def __init__(self, *args, **kwargs):
                pass
            async def __aenter__(self):
                raise OSError("Connection refused")
            async def __aexit__(self, *args):
                return None

        monkeypatch.setattr('aiohttp.ClientSession', MockClientSession)

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {"api_key": "new_value"}}
        )

        # Secret update should succeed even if DataHub unload fails
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "updated"

    def test_credential_update_handles_datahub_404_gracefully(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        mock_aiohttp_session
    ):
        """Credential update handles 404 from DataHub (provider not loaded) gracefully."""
        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        registry_with_mocks.system_context.create_context_data = Mock(
            return_value=(b'new_nonce', b'new_ciphertext')
        )

        # DataHub returns 404 (provider not currently loaded)
        mock_aiohttp_session["response"].status = 404

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestProvider", "class_type": "provider"},
            json={"secrets": {"api_key": "new_value"}}
        )

        # Secret update should succeed even if provider wasn't loaded
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "updated"

    def test_credential_update_for_broker_does_not_trigger_unload(
        self,
        registry_with_mocks,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock,
        mock_aiohttp_session
    ):
        """Credential update for broker type does NOT trigger DataHub unload."""
        mock_asyncpg_pool.fetchval.return_value = b'test_file_hash_12345'
        mock_asyncpg_pool.execute = AsyncMock()

        registry_with_mocks.system_context.create_context_data = Mock(
            return_value=(b'new_nonce', b'new_ciphertext')
        )

        response = registry_client.patch(
            "/api/registry/config/secrets",
            params={"class_name": "TestBroker", "class_type": "broker"},
            json={"secrets": {"api_key": "new_value"}}
        )

        assert response.status_code == 200
        # Verify unload endpoint was NOT called for broker
        mock_aiohttp_session["session"].post.assert_not_called()
