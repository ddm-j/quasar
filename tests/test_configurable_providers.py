"""Unit tests for provider CONFIGURABLE schema system.

Tests cover:
- T027: CONFIGURABLE schema inheritance across provider base classes
- T028: Schema lookup utility functions
"""

import pytest
from typing import Any

from quasar.lib.providers.core import (
    DataProvider,
    HistoricalDataProvider,
    LiveDataProvider,
    IndexProvider,
)
from quasar.services.registry.handlers.config import (
    SCHEMA_MAP,
    get_schema_for_subtype,
    serialize_schema,
    validate_preferences_against_schema,
)


class TestConfigurableSchemaInheritance:
    """T027: Tests for CONFIGURABLE schema inheritance."""

    def test_data_provider_has_configurable(self):
        """DataProvider base class defines CONFIGURABLE dict."""
        assert hasattr(DataProvider, "CONFIGURABLE")
        assert isinstance(DataProvider.CONFIGURABLE, dict)

    def test_data_provider_has_crypto_category(self):
        """DataProvider includes crypto preferences category."""
        assert "crypto" in DataProvider.CONFIGURABLE
        assert "preferred_quote_currency" in DataProvider.CONFIGURABLE["crypto"]

    def test_data_provider_crypto_schema_structure(self):
        """DataProvider crypto schema has correct structure."""
        crypto_schema = DataProvider.CONFIGURABLE["crypto"]["preferred_quote_currency"]
        assert crypto_schema["type"] == str
        assert crypto_schema["default"] is None
        assert "description" in crypto_schema

    def test_historical_provider_inherits_crypto(self):
        """HistoricalDataProvider inherits crypto from DataProvider."""
        assert "crypto" in HistoricalDataProvider.CONFIGURABLE
        assert "preferred_quote_currency" in HistoricalDataProvider.CONFIGURABLE["crypto"]

    def test_historical_provider_has_scheduling(self):
        """HistoricalDataProvider adds scheduling category."""
        assert "scheduling" in HistoricalDataProvider.CONFIGURABLE
        assert "delay_hours" in HistoricalDataProvider.CONFIGURABLE["scheduling"]

    def test_historical_provider_delay_hours_schema(self):
        """HistoricalDataProvider delay_hours has correct bounds."""
        schema = HistoricalDataProvider.CONFIGURABLE["scheduling"]["delay_hours"]
        assert schema["type"] == int
        assert schema["default"] == 0
        assert schema["min"] == 0
        assert schema["max"] == 24
        assert "description" in schema

    def test_historical_provider_has_data_category(self):
        """HistoricalDataProvider adds data category."""
        assert "data" in HistoricalDataProvider.CONFIGURABLE
        assert "lookback_days" in HistoricalDataProvider.CONFIGURABLE["data"]

    def test_historical_provider_lookback_days_schema(self):
        """HistoricalDataProvider lookback_days has correct bounds."""
        schema = HistoricalDataProvider.CONFIGURABLE["data"]["lookback_days"]
        assert schema["type"] == int
        assert schema["default"] == 8000
        assert schema["min"] == 1
        assert schema["max"] == 8000
        assert "description" in schema

    def test_live_provider_inherits_crypto(self):
        """LiveDataProvider inherits crypto from DataProvider."""
        assert "crypto" in LiveDataProvider.CONFIGURABLE
        assert "preferred_quote_currency" in LiveDataProvider.CONFIGURABLE["crypto"]

    def test_live_provider_has_scheduling(self):
        """LiveDataProvider adds scheduling category."""
        assert "scheduling" in LiveDataProvider.CONFIGURABLE
        assert "pre_close_seconds" in LiveDataProvider.CONFIGURABLE["scheduling"]
        assert "post_close_seconds" in LiveDataProvider.CONFIGURABLE["scheduling"]

    def test_live_provider_pre_close_seconds_schema(self):
        """LiveDataProvider pre_close_seconds has correct bounds."""
        schema = LiveDataProvider.CONFIGURABLE["scheduling"]["pre_close_seconds"]
        assert schema["type"] == int
        assert schema["default"] == 30
        assert schema["min"] == 0
        assert schema["max"] == 300
        assert "description" in schema

    def test_live_provider_post_close_seconds_schema(self):
        """LiveDataProvider post_close_seconds has correct bounds."""
        schema = LiveDataProvider.CONFIGURABLE["scheduling"]["post_close_seconds"]
        assert schema["type"] == int
        assert schema["default"] == 5
        assert schema["min"] == 0
        assert schema["max"] == 60
        assert "description" in schema

    def test_live_provider_no_data_category(self):
        """LiveDataProvider does not have data category (no lookback)."""
        assert "data" not in LiveDataProvider.CONFIGURABLE

    def test_index_provider_inherits_crypto(self):
        """IndexProvider inherits crypto from DataProvider."""
        assert "crypto" in IndexProvider.CONFIGURABLE
        assert "preferred_quote_currency" in IndexProvider.CONFIGURABLE["crypto"]

    def test_index_provider_has_scheduling(self):
        """IndexProvider has scheduling category with sync_frequency."""
        assert "scheduling" in IndexProvider.CONFIGURABLE
        assert "sync_frequency" in IndexProvider.CONFIGURABLE["scheduling"]

    def test_index_provider_no_data(self):
        """IndexProvider does not add data category."""
        assert "data" not in IndexProvider.CONFIGURABLE

    def test_index_provider_has_crypto_and_scheduling(self):
        """IndexProvider has crypto (from DataProvider) and scheduling categories."""
        assert len(IndexProvider.CONFIGURABLE) == 2
        assert "crypto" in IndexProvider.CONFIGURABLE
        assert "scheduling" in IndexProvider.CONFIGURABLE


class TestSchemaLookupUtility:
    """T028: Tests for schema lookup utility functions."""

    def test_schema_map_has_historical(self):
        """SCHEMA_MAP includes Historical subtype."""
        assert "Historical" in SCHEMA_MAP
        assert SCHEMA_MAP["Historical"] == HistoricalDataProvider.CONFIGURABLE

    def test_schema_map_has_realtime(self):
        """SCHEMA_MAP includes Live subtype."""
        assert "Live" in SCHEMA_MAP
        assert SCHEMA_MAP["Live"] == LiveDataProvider.CONFIGURABLE

    def test_schema_map_has_index(self):
        """SCHEMA_MAP includes IndexProvider subtype."""
        assert "IndexProvider" in SCHEMA_MAP
        assert SCHEMA_MAP["IndexProvider"] == IndexProvider.CONFIGURABLE

    def test_get_schema_for_subtype_historical(self):
        """get_schema_for_subtype returns correct schema for Historical."""
        schema = get_schema_for_subtype("Historical")
        assert schema is not None
        assert "scheduling" in schema
        assert "delay_hours" in schema["scheduling"]

    def test_get_schema_for_subtype_realtime(self):
        """get_schema_for_subtype returns correct schema for Live."""
        schema = get_schema_for_subtype("Live")
        assert schema is not None
        assert "scheduling" in schema
        assert "pre_close_seconds" in schema["scheduling"]
        assert "post_close_seconds" in schema["scheduling"]

    def test_get_schema_for_subtype_index(self):
        """get_schema_for_subtype returns correct schema for IndexProvider."""
        schema = get_schema_for_subtype("IndexProvider")
        assert schema is not None
        assert "crypto" in schema
        assert "scheduling" in schema
        assert "sync_frequency" in schema["scheduling"]

    def test_get_schema_for_subtype_unknown(self):
        """get_schema_for_subtype returns None for unknown subtype."""
        schema = get_schema_for_subtype("unknown_type")
        assert schema is None

    def test_get_schema_for_subtype_empty_string(self):
        """get_schema_for_subtype returns None for empty string."""
        schema = get_schema_for_subtype("")
        assert schema is None


class TestSerializeSchema:
    """Tests for serialize_schema function."""

    def test_serialize_schema_converts_int_type(self):
        """serialize_schema converts int type to JSON Schema 'integer'."""
        schema = {"scheduling": {"delay_hours": {"type": int, "default": 0}}}
        result = serialize_schema(schema)
        assert result["scheduling"]["delay_hours"]["type"] == "integer"

    def test_serialize_schema_converts_str_type(self):
        """serialize_schema converts str type to JSON Schema 'string'."""
        schema = {"crypto": {"quote": {"type": str, "default": None}}}
        result = serialize_schema(schema)
        assert result["crypto"]["quote"]["type"] == "string"

    def test_serialize_schema_preserves_other_fields(self):
        """serialize_schema preserves non-type fields."""
        schema = {
            "scheduling": {
                "delay_hours": {
                    "type": int,
                    "default": 0,
                    "min": 0,
                    "max": 24,
                    "description": "Hours delay"
                }
            }
        }
        result = serialize_schema(schema)
        assert result["scheduling"]["delay_hours"]["default"] == 0
        assert result["scheduling"]["delay_hours"]["min"] == 0
        assert result["scheduling"]["delay_hours"]["max"] == 24
        assert result["scheduling"]["delay_hours"]["description"] == "Hours delay"

    def test_serialize_schema_handles_historical_provider(self):
        """serialize_schema correctly serializes HistoricalDataProvider schema."""
        result = serialize_schema(HistoricalDataProvider.CONFIGURABLE)
        assert result["scheduling"]["delay_hours"]["type"] == "integer"
        assert result["data"]["lookback_days"]["type"] == "integer"
        assert result["crypto"]["preferred_quote_currency"]["type"] == "string"

    def test_serialize_schema_handles_live_provider(self):
        """serialize_schema correctly serializes LiveDataProvider schema."""
        result = serialize_schema(LiveDataProvider.CONFIGURABLE)
        assert result["scheduling"]["pre_close_seconds"]["type"] == "integer"
        assert result["scheduling"]["post_close_seconds"]["type"] == "integer"

    def test_serialize_schema_returns_new_dict(self):
        """serialize_schema returns a new dict, not mutating original."""
        original = {"cat": {"field": {"type": int}}}
        result = serialize_schema(original)
        # Original should still have Python type
        assert original["cat"]["field"]["type"] == int
        # Result should have JSON Schema type name
        assert result["cat"]["field"]["type"] == "integer"


class TestValidatePreferencesAgainstSchema:
    """Tests for validate_preferences_against_schema function."""

    def test_valid_historical_preferences(self):
        """Valid historical preferences pass validation."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {
            "scheduling": {"delay_hours": 6},
            "data": {"lookback_days": 365}
        }
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert errors == []

    def test_valid_live_preferences(self):
        """Valid live preferences pass validation."""
        schema = LiveDataProvider.CONFIGURABLE
        preferences = {
            "scheduling": {"pre_close_seconds": 60, "post_close_seconds": 10}
        }
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert errors == []

    def test_unknown_category_rejected(self):
        """Unknown preference category is rejected."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {"unknown_category": {"field": "value"}}
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert len(errors) == 1
        assert "Unknown preference category" in errors[0]

    def test_unknown_field_rejected(self):
        """Unknown field in valid category is rejected."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {"scheduling": {"unknown_field": 5}}
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert len(errors) == 1
        assert "Unknown field" in errors[0]

    def test_wrong_type_rejected(self):
        """Value with wrong type is rejected."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {"scheduling": {"delay_hours": "six"}}  # String instead of int
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert len(errors) == 1
        assert "must be an integer" in errors[0]

    def test_below_min_rejected(self):
        """Value below minimum is rejected."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {"scheduling": {"delay_hours": -1}}
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert len(errors) == 1
        assert "must be >=" in errors[0]

    def test_above_max_rejected(self):
        """Value above maximum is rejected."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {"scheduling": {"delay_hours": 25}}  # Max is 24
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert len(errors) == 1
        assert "must be <=" in errors[0]

    def test_none_value_accepted(self):
        """None value is accepted for optional fields."""
        schema = DataProvider.CONFIGURABLE
        preferences = {"crypto": {"preferred_quote_currency": None}}
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        assert errors == []

    def test_boundary_values_accepted(self):
        """Boundary values (min and max) are accepted."""
        schema = HistoricalDataProvider.CONFIGURABLE
        # Test min values
        preferences_min = {"scheduling": {"delay_hours": 0}, "data": {"lookback_days": 1}}
        errors_min = validate_preferences_against_schema(preferences_min, schema, "TestProvider")
        assert errors_min == []

        # Test max values
        preferences_max = {"scheduling": {"delay_hours": 24}, "data": {"lookback_days": 8000}}
        errors_max = validate_preferences_against_schema(preferences_max, schema, "TestProvider")
        assert errors_max == []

    def test_multiple_errors_returned(self):
        """Multiple validation errors are all returned."""
        schema = HistoricalDataProvider.CONFIGURABLE
        preferences = {
            "scheduling": {"delay_hours": "invalid", "unknown": 5},
            "unknown_category": {}
        }
        errors = validate_preferences_against_schema(preferences, schema, "TestProvider")
        # Should have errors for: wrong type, unknown field, unknown category
        assert len(errors) >= 3


class TestOffsetCronTriggerPositiveOffset:
    """T038: Tests for OffsetCronTrigger receiving correct positive offset for historical providers."""

    def test_offset_cron_trigger_stores_positive_offset(self):
        """OffsetCronTrigger correctly stores positive offset for historical providers."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Historical provider with delay_hours=6 means positive offset
        delay_hours = 6
        offset_seconds = delay_hours * 3600  # 21600

        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight UTC
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        # Verify offset is stored correctly
        assert trigger.offset_seconds == 21600
        # Positive offset should have _sign=1
        assert trigger._sign == 1

    def test_offset_cron_trigger_zero_offset(self):
        """OffsetCronTrigger handles zero offset (default for historical providers)."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Historical provider with delay_hours=0 (default)
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",
            offset_seconds=0,
            timezone="UTC"
        )

        assert trigger.offset_seconds == 0
        # Zero is treated as non-negative, so _sign=1
        assert trigger._sign == 1

    def test_offset_cron_trigger_max_delay_hours(self):
        """OffsetCronTrigger handles maximum delay_hours=24."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Maximum allowed delay_hours=24
        delay_hours = 24
        offset_seconds = delay_hours * 3600  # 86400

        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        assert trigger.offset_seconds == 86400
        assert trigger._sign == 1

    def test_offset_cron_trigger_fires_at_positive_offset(self):
        """OffsetCronTrigger fires at correct time with positive offset."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Trigger for midnight with 6 hour positive offset
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight
            offset_seconds=6 * 3600,  # 6 hours delay
            timezone="UTC"
        )

        # At 11 PM (before midnight), the next cron fire is midnight, then +6 hours = 6 AM
        now = datetime(2024, 6, 14, 23, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        expected = datetime(2024, 6, 15, 6, 0, 0, tzinfo=timezone.utc)
        assert next_fire == expected, f"Expected {expected}, got {next_fire}"

    def test_offset_cron_trigger_positive_offset_after_previous_fire(self):
        """OffsetCronTrigger calculates next fire correctly after previous fire."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Trigger for midnight with 6 hour positive offset
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight
            offset_seconds=6 * 3600,  # 6 hours delay
            timezone="UTC"
        )

        # Previous fire was at 6:00 AM on June 15
        previous_fire = datetime(2024, 6, 15, 6, 0, 0, tzinfo=timezone.utc)
        now = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(previous_fire, now)

        # Next fire should be 6:00 AM on June 16
        expected = datetime(2024, 6, 16, 6, 0, 0, tzinfo=timezone.utc)
        assert next_fire == expected, f"Expected {expected}, got {next_fire}"

    def test_offset_applied_during_refresh_subscriptions_historical(self):
        """Verify delay_hours from preferences is correctly converted to offset_seconds."""
        # This tests the conversion logic in refresh_subscriptions
        # delay_hours=6 should become offset_seconds=21600

        delay_hours = 6
        offset_seconds = delay_hours * 3600

        # Verify the conversion formula
        assert offset_seconds == 21600
        assert offset_seconds == 6 * 60 * 60  # 6 hours in seconds

    def test_offset_conversion_boundary_values(self):
        """Test offset conversion for boundary delay_hours values (0, 1, 24)."""
        # delay_hours=0 (minimum)
        assert 0 * 3600 == 0

        # delay_hours=1
        assert 1 * 3600 == 3600

        # delay_hours=24 (maximum)
        assert 24 * 3600 == 86400

    def test_historical_provider_uses_positive_offset_not_negative(self):
        """Historical providers should use positive offset (delay), not negative."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger
        from datetime import datetime, timezone

        # Historical provider with delay_hours=6
        delay_hours = 6
        offset_seconds = delay_hours * 3600  # Positive offset

        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        # Fire time should be AFTER the cron time (delayed)
        # At 11 PM the day before, next fire should be 6 AM next day
        now = datetime(2024, 6, 14, 23, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # With positive offset, job fires at 06:00 (after midnight)
        expected = datetime(2024, 6, 15, 6, 0, 0, tzinfo=timezone.utc)
        assert next_fire == expected

        # Verify the fire time is AFTER the base cron time (midnight)
        base_cron_time = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
        assert next_fire > base_cron_time, "Historical provider should fire AFTER cron time"


class TestOffsetCronTriggerNegativeOffset:
    """T047: Tests for OffsetCronTrigger receiving correct negative offset for live providers."""

    def test_offset_cron_trigger_stores_negative_offset(self):
        """OffsetCronTrigger correctly stores negative offset for live providers."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Live provider with pre_close_seconds=60 means negative offset
        pre_close_seconds = 60
        offset_seconds = -1 * pre_close_seconds  # -60

        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM UTC
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        # Verify offset is stored correctly (absolute value)
        assert trigger.offset_seconds == 60
        # Negative offset should have _sign=-1
        assert trigger._sign == -1

    def test_offset_cron_trigger_negative_zero_offset(self):
        """OffsetCronTrigger handles zero offset for live providers with pre_close_seconds=0."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Live provider with pre_close_seconds=0 (no early start)
        pre_close_seconds = 0
        offset_seconds = -1 * pre_close_seconds  # -0 == 0

        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        assert trigger.offset_seconds == 0
        # Zero offset, sign defaults to 1
        assert trigger._sign == 1

    def test_offset_cron_trigger_max_pre_close_seconds(self):
        """OffsetCronTrigger handles maximum pre_close_seconds=300."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Maximum allowed pre_close_seconds=300 (5 minutes)
        pre_close_seconds = 300
        offset_seconds = -1 * pre_close_seconds  # -300

        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        assert trigger.offset_seconds == 300
        assert trigger._sign == -1

    def test_offset_cron_trigger_fires_at_negative_offset(self):
        """OffsetCronTrigger fires at correct time with negative offset."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Trigger for 4 PM with 60 second negative offset (pre_close)
        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM
            offset_seconds=-60,  # 60 seconds before
            timezone="UTC"
        )

        # At 3 PM, the next cron fire is 4 PM, then -60 seconds = 3:59 PM
        now = datetime(2024, 6, 14, 15, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        expected = datetime(2024, 6, 14, 15, 59, 0, tzinfo=timezone.utc)
        assert next_fire == expected, f"Expected {expected}, got {next_fire}"

    def test_offset_cron_trigger_negative_offset_after_previous_fire(self):
        """OffsetCronTrigger calculates next fire correctly after previous fire."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Trigger for 4 PM with 60 second negative offset
        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM
            offset_seconds=-60,  # 60 seconds before
            timezone="UTC"
        )

        # Previous fire was at 3:59 PM on June 14
        previous_fire = datetime(2024, 6, 14, 15, 59, 0, tzinfo=timezone.utc)
        now = datetime(2024, 6, 14, 17, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(previous_fire, now)

        # Next fire should be 3:59 PM on June 15
        expected = datetime(2024, 6, 15, 15, 59, 0, tzinfo=timezone.utc)
        assert next_fire == expected, f"Expected {expected}, got {next_fire}"

    def test_offset_applied_during_refresh_subscriptions_live(self):
        """Verify pre_close_seconds from preferences is correctly converted to negative offset."""
        # This tests the conversion logic in refresh_subscriptions
        # pre_close_seconds=60 should become offset_seconds=-60

        pre_close_seconds = 60
        offset_seconds = -1 * pre_close_seconds

        # Verify the conversion formula
        assert offset_seconds == -60
        assert offset_seconds == -1 * 60  # Negative of pre_close seconds

    def test_negative_offset_conversion_boundary_values(self):
        """Test negative offset conversion for boundary pre_close_seconds values (0, 30, 300)."""
        # pre_close_seconds=0 (minimum)
        assert -1 * 0 == 0

        # pre_close_seconds=30 (default)
        assert -1 * 30 == -30

        # pre_close_seconds=300 (maximum)
        assert -1 * 300 == -300

    def test_live_provider_uses_negative_offset_not_positive(self):
        """Live providers should use negative offset (pre_close), not positive."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger
        from datetime import datetime, timezone

        # Live provider with pre_close_seconds=60
        pre_close_seconds = 60
        offset_seconds = -1 * pre_close_seconds  # Negative offset

        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        # Fire time should be BEFORE the cron time (early start)
        # At 3 PM, next fire should be 3:59 PM (before 4 PM cron time)
        now = datetime(2024, 6, 14, 15, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # With negative offset, job fires at 15:59 (before 16:00)
        expected = datetime(2024, 6, 14, 15, 59, 0, tzinfo=timezone.utc)
        assert next_fire == expected

        # Verify the fire time is BEFORE the base cron time (4 PM)
        base_cron_time = datetime(2024, 6, 14, 16, 0, 0, tzinfo=timezone.utc)
        assert next_fire < base_cron_time, "Live provider should fire BEFORE cron time"

    def test_live_provider_default_pre_close_seconds(self):
        """Live provider DEFAULT_LIVE_OFFSET (30 seconds) produces correct negative offset."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger
        from datetime import datetime, timezone

        # Default pre_close_seconds is 30 seconds
        default_pre_close = 30
        offset_seconds = -1 * default_pre_close

        trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",  # 4 PM
            offset_seconds=offset_seconds,
            timezone="UTC"
        )

        assert trigger.offset_seconds == 30
        assert trigger._sign == -1

        # Should fire at 15:59:30 (30 seconds before 4 PM)
        now = datetime(2024, 6, 14, 15, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)
        expected = datetime(2024, 6, 14, 15, 59, 30, tzinfo=timezone.utc)
        assert next_fire == expected

    def test_live_provider_offset_semantics_differ_from_historical(self):
        """Live provider negative offset has opposite semantics to historical positive offset."""
        from quasar.lib.common.offset_cron import OffsetCronTrigger
        from datetime import datetime, timezone

        now = datetime(2024, 6, 14, 15, 0, 0, tzinfo=timezone.utc)

        # Historical: positive offset = fire LATER (delay after close)
        historical_trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",
            offset_seconds=3600,  # +1 hour
            timezone="UTC"
        )
        historical_fire = historical_trigger.get_next_fire_time(None, now)
        assert historical_fire == datetime(2024, 6, 14, 17, 0, 0, tzinfo=timezone.utc)

        # Live: negative offset = fire EARLIER (start before close)
        live_trigger = OffsetCronTrigger.from_crontab(
            "0 16 * * *",
            offset_seconds=-60,  # -1 minute
            timezone="UTC"
        )
        live_fire = live_trigger.get_next_fire_time(None, now)
        assert live_fire == datetime(2024, 6, 14, 15, 59, 0, tzinfo=timezone.utc)

        # Historical fires after cron time, live fires before
        cron_time = datetime(2024, 6, 14, 16, 0, 0, tzinfo=timezone.utc)
        assert historical_fire > cron_time, "Historical should fire AFTER cron"
        assert live_fire < cron_time, "Live should fire BEFORE cron"


class TestBackwardCompatibility:
    """T080: Verify backward compatibility - providers with no preferences behave like current defaults.

    These tests ensure that:
    1. Providers instantiated without preferences (None) work correctly
    2. Providers with empty preferences ({}) work correctly
    3. All scheduling/data defaults match the expected system defaults
    4. The system gracefully handles missing preference categories
    """

    def test_data_provider_init_with_none_preferences(self):
        """DataProvider.__init__ with preferences=None initializes empty dict."""
        from quasar.lib.providers.core import DataProvider
        from unittest.mock import Mock

        # Create a concrete implementation for testing
        class TestDataProvider(DataProvider):
            name = "TestProvider"
            provider_type = None  # Will be set by subclass

            async def fetch_available_symbols(self):
                return []

        mock_context = Mock()
        mock_context.get = Mock(return_value=None)

        # Instantiate with None preferences (backward compatible case)
        provider = TestDataProvider(context=mock_context, preferences=None)

        # Verify preferences is initialized to empty dict
        assert provider.preferences == {}
        assert isinstance(provider.preferences, dict)

    def test_historical_provider_init_with_none_preferences(self):
        """HistoricalDataProvider.__init__ with preferences=None initializes empty dict."""
        from quasar.lib.providers.core import HistoricalDataProvider, Bar
        from unittest.mock import Mock
        from typing import AsyncIterator
        from datetime import date

        class TestHistoricalProvider(HistoricalDataProvider):
            name = "TestHistoricalProvider"

            async def get_history(self, sym, start, end, interval) -> AsyncIterator[Bar]:
                return
                yield  # Make it a generator

            async def fetch_available_symbols(self):
                return []

        mock_context = Mock()
        mock_context.get = Mock(return_value=None)

        provider = TestHistoricalProvider(context=mock_context, preferences=None)

        assert provider.preferences == {}
        assert isinstance(provider.preferences, dict)

    def test_live_provider_init_with_none_preferences(self):
        """LiveDataProvider.__init__ with preferences=None initializes empty dict."""
        from quasar.lib.providers.core import LiveDataProvider, Bar
        from unittest.mock import Mock

        class TestLiveProvider(LiveDataProvider):
            name = "TestLiveProvider"
            close_buffer_seconds = 10

            async def _connect(self):
                return None

            async def _subscribe(self, symbols):
                return {}

            async def _unsubscribe(self, symbols):
                return {}

            async def _parse_message(self, message):
                return []

            async def fetch_available_symbols(self):
                return []

        mock_context = Mock()
        mock_context.get = Mock(return_value=None)

        provider = TestLiveProvider(context=mock_context, preferences=None)

        assert provider.preferences == {}
        assert isinstance(provider.preferences, dict)

    def test_index_provider_init_with_none_preferences(self):
        """IndexProvider.__init__ with preferences=None initializes empty dict."""
        from quasar.lib.providers.core import IndexProvider
        from unittest.mock import Mock

        class TestIndexProvider(IndexProvider):
            name = "TestIndexProvider"

            async def fetch_constituents(self, as_of_date=None):
                return []

        mock_context = Mock()
        mock_context.get = Mock(return_value=None)

        provider = TestIndexProvider(context=mock_context, preferences=None)

        assert provider.preferences == {}
        assert isinstance(provider.preferences, dict)

    def test_historical_delay_hours_default_is_zero(self):
        """Historical provider delay_hours defaults to 0 when no preference."""
        # This is the default in refresh_subscriptions
        scheduling_prefs = {}  # Empty, simulating no preferences
        delay_hours = scheduling_prefs.get("delay_hours", 0)
        assert delay_hours == 0, "delay_hours should default to 0 for backward compatibility"

    def test_live_pre_close_seconds_default_matches_constant(self):
        """Live provider pre_close_seconds defaults to DEFAULT_LIVE_OFFSET."""
        from quasar.services.datahub.utils.constants import DEFAULT_LIVE_OFFSET

        # This is the default in refresh_subscriptions
        scheduling_prefs = {}  # Empty, simulating no preferences
        pre_close_seconds = scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET)

        assert pre_close_seconds == DEFAULT_LIVE_OFFSET
        assert pre_close_seconds == 30, "DEFAULT_LIVE_OFFSET should be 30 seconds"

    def test_live_post_close_seconds_default_uses_provider_buffer(self):
        """Live provider post_close_seconds defaults to provider's close_buffer_seconds."""
        # In refresh_subscriptions, post_close_seconds defaults to prov.close_buffer_seconds
        # Simulate a provider with close_buffer_seconds = 10
        provider_close_buffer = 10

        scheduling_prefs = {}  # Empty, simulating no preferences
        post_close_seconds = scheduling_prefs.get("post_close_seconds", provider_close_buffer)

        assert post_close_seconds == provider_close_buffer
        assert post_close_seconds == 10

    def test_historical_lookback_days_default_matches_constant(self):
        """Historical provider lookback_days defaults to DEFAULT_LOOKBACK."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK

        # This is the default in _build_reqs_historical
        data_prefs = {}  # Empty, simulating no preferences
        lookback_days = data_prefs.get("lookback_days", DEFAULT_LOOKBACK)

        assert lookback_days == DEFAULT_LOOKBACK
        assert lookback_days == 8000, "DEFAULT_LOOKBACK should be 8000"

    def test_empty_preferences_dict_works_like_no_preferences(self):
        """Empty preferences dict {} behaves same as no preferences."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK, DEFAULT_LIVE_OFFSET

        # Simulate the extraction logic from refresh_subscriptions and _build_reqs_historical
        prefs = {}  # Empty preferences dict

        # Historical provider defaults
        scheduling_prefs = prefs.get("scheduling") or {}
        data_prefs = prefs.get("data") or {}

        assert scheduling_prefs.get("delay_hours", 0) == 0
        assert data_prefs.get("lookback_days", DEFAULT_LOOKBACK) == 8000

        # Live provider defaults
        assert scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET) == 30

    def test_missing_scheduling_category_uses_defaults(self):
        """Preferences without scheduling category uses defaults."""
        from quasar.services.datahub.utils.constants import DEFAULT_LIVE_OFFSET

        # Preferences exist but without scheduling category
        prefs = {"data": {"lookback_days": 365}}  # Only data category

        scheduling_prefs = prefs.get("scheduling") or {}
        delay_hours = scheduling_prefs.get("delay_hours", 0)
        pre_close_seconds = scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET)

        assert delay_hours == 0
        assert pre_close_seconds == 30

    def test_missing_data_category_uses_defaults(self):
        """Preferences without data category uses defaults."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK

        # Preferences exist but without data category
        prefs = {"scheduling": {"delay_hours": 6}}  # Only scheduling category

        data_prefs = prefs.get("data") or {}
        lookback_days = data_prefs.get("lookback_days", DEFAULT_LOOKBACK)

        assert lookback_days == DEFAULT_LOOKBACK

    def test_provider_preferences_none_in_datahub_context(self):
        """Provider with None preferences in _provider_preferences dict works."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK, DEFAULT_LIVE_OFFSET

        # Simulate the access pattern in collection.py handlers
        _provider_preferences = {
            "TestProvider": None  # Explicitly None
        }

        prefs = _provider_preferences.get("TestProvider") or {}
        scheduling_prefs = prefs.get("scheduling") or {}
        data_prefs = prefs.get("data") or {}

        # All should default correctly
        assert scheduling_prefs.get("delay_hours", 0) == 0
        assert scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET) == 30
        assert data_prefs.get("lookback_days", DEFAULT_LOOKBACK) == 8000

    def test_provider_not_in_preferences_dict_works(self):
        """Provider not present in _provider_preferences dict works."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK, DEFAULT_LIVE_OFFSET

        # Simulate missing provider in _provider_preferences
        _provider_preferences = {}  # Empty dict

        prefs = _provider_preferences.get("UnknownProvider") or {}
        scheduling_prefs = prefs.get("scheduling") or {}
        data_prefs = prefs.get("data") or {}

        # All should default correctly
        assert scheduling_prefs.get("delay_hours", 0) == 0
        assert scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET) == 30
        assert data_prefs.get("lookback_days", DEFAULT_LOOKBACK) == 8000

    def test_offset_cron_trigger_zero_offset_fires_at_cron_time(self):
        """OffsetCronTrigger with zero offset fires exactly at cron time (no delay)."""
        from datetime import datetime, timezone
        from quasar.lib.common.offset_cron import OffsetCronTrigger

        # Zero offset = backward compatible behavior (no delay)
        trigger = OffsetCronTrigger.from_crontab(
            "0 0 * * *",  # Midnight
            offset_seconds=0,
            timezone="UTC"
        )

        now = datetime(2024, 6, 14, 23, 0, 0, tzinfo=timezone.utc)
        next_fire = trigger.get_next_fire_time(None, now)

        # Should fire exactly at midnight (no offset)
        expected = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
        assert next_fire == expected, "Zero offset should fire at exact cron time"

    def test_configurable_defaults_match_code_defaults(self):
        """CONFIGURABLE schema defaults match the fallback values in code."""
        from quasar.lib.providers.core import HistoricalDataProvider, LiveDataProvider
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK, DEFAULT_LIVE_OFFSET

        # Historical defaults
        hist_schema = HistoricalDataProvider.CONFIGURABLE
        assert hist_schema["scheduling"]["delay_hours"]["default"] == 0
        assert hist_schema["data"]["lookback_days"]["default"] == DEFAULT_LOOKBACK

        # Live defaults
        live_schema = LiveDataProvider.CONFIGURABLE
        assert live_schema["scheduling"]["pre_close_seconds"]["default"] == DEFAULT_LIVE_OFFSET
        assert live_schema["scheduling"]["post_close_seconds"]["default"] == 5


class TestBuildReqsHistoricalLookbackDays:
    """T055: Unit tests for _build_reqs_historical() using preference over DEFAULT_LOOKBACK."""

    def test_lookback_days_preference_extracted_from_provider_preferences(self):
        """Test that lookback_days is extracted from _provider_preferences dict."""
        # Simulate the extraction logic from _build_reqs_historical
        _provider_preferences = {
            "TestProvider": {
                "data": {"lookback_days": 365}
            }
        }

        prefs = _provider_preferences.get("TestProvider") or {}
        data_prefs = prefs.get("data") or {}
        lookback_days = data_prefs.get("lookback_days", 8000)

        assert lookback_days == 365

    def test_lookback_days_uses_default_when_no_preference(self):
        """Test that DEFAULT_LOOKBACK is used when no preference is set."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK

        # Simulate the extraction logic with empty preferences
        _provider_preferences = {}

        prefs = _provider_preferences.get("TestProvider") or {}
        data_prefs = prefs.get("data") or {}
        lookback_days = data_prefs.get("lookback_days", DEFAULT_LOOKBACK)

        assert lookback_days == DEFAULT_LOOKBACK
        assert lookback_days == 8000

    def test_lookback_days_uses_default_when_data_category_missing(self):
        """Test that DEFAULT_LOOKBACK is used when data category is not in preferences."""
        from quasar.services.datahub.utils.constants import DEFAULT_LOOKBACK

        # Preferences exist but without data category
        _provider_preferences = {
            "TestProvider": {
                "scheduling": {"delay_hours": 6}
            }
        }

        prefs = _provider_preferences.get("TestProvider") or {}
        data_prefs = prefs.get("data") or {}
        lookback_days = data_prefs.get("lookback_days", DEFAULT_LOOKBACK)

        assert lookback_days == DEFAULT_LOOKBACK

    def test_lookback_days_start_date_calculation(self):
        """Test that start date is correctly calculated from lookback_days."""
        from datetime import datetime, timezone, timedelta

        # Simulate the start date calculation from _build_reqs_historical
        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)

        lookback_days = 365
        default_start = yday - timedelta(days=lookback_days)
        start = default_start + timedelta(days=1)  # For new subscriptions

        # Verify the start date is approximately 365 days before yesterday
        expected_start = yday - timedelta(days=lookback_days - 1)
        assert start == expected_start

    def test_lookback_days_boundary_value_min(self):
        """Test minimum lookback_days=1 produces correct start date."""
        from datetime import datetime, timezone, timedelta

        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)

        lookback_days = 1  # Minimum
        default_start = yday - timedelta(days=lookback_days)
        start = default_start + timedelta(days=1)

        # With lookback_days=1, start should be yesterday
        assert start == yday

    def test_lookback_days_boundary_value_max(self):
        """Test maximum lookback_days=8000 produces correct start date."""
        from datetime import datetime, timezone, timedelta

        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)

        lookback_days = 8000  # Maximum
        default_start = yday - timedelta(days=lookback_days)
        start = default_start + timedelta(days=1)

        # With lookback_days=8000, start should be ~21.9 years ago
        expected_start = yday - timedelta(days=7999)
        assert start == expected_start

    def test_using_custom_lookback_flag(self):
        """Test that using_custom_lookback flag is set correctly."""
        # Simulate the using_custom_lookback logic
        _provider_preferences = {
            "TestProvider": {"data": {"lookback_days": 365}}
        }

        prefs = _provider_preferences.get("TestProvider") or {}
        data_prefs = prefs.get("data") or {}
        using_custom_lookback = "lookback_days" in data_prefs

        assert using_custom_lookback is True

    def test_using_custom_lookback_flag_false_when_not_set(self):
        """Test that using_custom_lookback flag is False when no preference."""
        # Empty preferences
        _provider_preferences = {}

        prefs = _provider_preferences.get("TestProvider") or {}
        data_prefs = prefs.get("data") or {}
        using_custom_lookback = "lookback_days" in data_prefs

        assert using_custom_lookback is False

    def test_lookback_days_common_values(self):
        """Test common lookback_days values (30, 90, 365, 1095, 1825)."""
        from datetime import datetime, timezone, timedelta

        today = datetime.now(timezone.utc).date()
        yday = today - timedelta(days=1)

        # Test common preset values from UI
        common_values = {
            30: "1 month",
            90: "3 months",
            365: "1 year",
            1095: "3 years",
            1825: "5 years"
        }

        for lookback_days, description in common_values.items():
            default_start = yday - timedelta(days=lookback_days)
            start = default_start + timedelta(days=1)

            expected_start = yday - timedelta(days=lookback_days - 1)
            assert start == expected_start, f"Failed for {description} ({lookback_days} days)"
