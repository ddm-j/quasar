"""Integration tests for provider configuration endpoints.

Tests cover:
- T029: Contract test for GET /api/registry/config/schema endpoint
"""

import pytest
from unittest.mock import AsyncMock
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
        # Mock database to return historical subtype
        mock_asyncpg_pool.fetchval.return_value = "historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestHistoricalProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestHistoricalProvider"
        assert data["class_type"] == "provider"
        assert data["class_subtype"] == "historical"
        assert "schema" in data

    def test_schema_endpoint_returns_200_for_realtime_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema endpoint returns 200 for valid realtime provider."""
        mock_asyncpg_pool.fetchval.return_value = "realtime"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestLiveProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestLiveProvider"
        assert data["class_subtype"] == "realtime"

    def test_schema_endpoint_returns_200_for_index_provider(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema endpoint returns 200 for valid index provider."""
        mock_asyncpg_pool.fetchval.return_value = "index"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestIndexProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["class_name"] == "TestIndexProvider"
        assert data["class_subtype"] == "index"

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
        mock_asyncpg_pool.fetchval.return_value = "historical"

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
        mock_asyncpg_pool.fetchval.return_value = "historical"

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
        mock_asyncpg_pool.fetchval.return_value = "realtime"

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
        mock_asyncpg_pool.fetchval.return_value = "realtime"

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
        mock_asyncpg_pool.fetchval.return_value = "index"

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
        for subtype in ["historical", "realtime", "index"]:
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
        mock_asyncpg_pool.fetchval.return_value = "historical"

        response = registry_client.get(
            "/api/registry/config/schema",
            params={"class_name": "TestProvider", "class_type": "provider"}
        )

        assert response.status_code == 200
        schema = response.json()["schema"]
        delay_hours = schema["scheduling"]["delay_hours"]
        # Type is serialized as string (e.g., "int", "str")
        assert "type" in delay_hours
        assert delay_hours["type"] == "int"

    def test_schema_field_has_bounds_info(
        self,
        registry_client: TestClient,
        mock_asyncpg_pool: AsyncMock
    ):
        """Schema fields include min/max bounds."""
        mock_asyncpg_pool.fetchval.return_value = "historical"

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
        mock_asyncpg_pool.fetchval.return_value = "historical"

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
        mock_asyncpg_pool.fetchval.return_value = "historical"

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
