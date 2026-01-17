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

    def test_index_provider_no_scheduling(self):
        """IndexProvider does not add scheduling category."""
        assert "scheduling" not in IndexProvider.CONFIGURABLE

    def test_index_provider_no_data(self):
        """IndexProvider does not add data category."""
        assert "data" not in IndexProvider.CONFIGURABLE

    def test_index_provider_only_has_crypto(self):
        """IndexProvider only has crypto category (from DataProvider)."""
        assert len(IndexProvider.CONFIGURABLE) == 1
        assert "crypto" in IndexProvider.CONFIGURABLE


class TestSchemaLookupUtility:
    """T028: Tests for schema lookup utility functions."""

    def test_schema_map_has_historical(self):
        """SCHEMA_MAP includes historical subtype."""
        assert "historical" in SCHEMA_MAP
        assert SCHEMA_MAP["historical"] == HistoricalDataProvider.CONFIGURABLE

    def test_schema_map_has_realtime(self):
        """SCHEMA_MAP includes realtime subtype."""
        assert "realtime" in SCHEMA_MAP
        assert SCHEMA_MAP["realtime"] == LiveDataProvider.CONFIGURABLE

    def test_schema_map_has_index(self):
        """SCHEMA_MAP includes index subtype."""
        assert "index" in SCHEMA_MAP
        assert SCHEMA_MAP["index"] == IndexProvider.CONFIGURABLE

    def test_get_schema_for_subtype_historical(self):
        """get_schema_for_subtype returns correct schema for historical."""
        schema = get_schema_for_subtype("historical")
        assert schema is not None
        assert "scheduling" in schema
        assert "delay_hours" in schema["scheduling"]

    def test_get_schema_for_subtype_realtime(self):
        """get_schema_for_subtype returns correct schema for realtime."""
        schema = get_schema_for_subtype("realtime")
        assert schema is not None
        assert "scheduling" in schema
        assert "pre_close_seconds" in schema["scheduling"]
        assert "post_close_seconds" in schema["scheduling"]

    def test_get_schema_for_subtype_index(self):
        """get_schema_for_subtype returns correct schema for index."""
        schema = get_schema_for_subtype("index")
        assert schema is not None
        assert "crypto" in schema
        assert "scheduling" not in schema

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
        """serialize_schema converts int type to string."""
        schema = {"scheduling": {"delay_hours": {"type": int, "default": 0}}}
        result = serialize_schema(schema)
        assert result["scheduling"]["delay_hours"]["type"] == "int"

    def test_serialize_schema_converts_str_type(self):
        """serialize_schema converts str type to string."""
        schema = {"crypto": {"quote": {"type": str, "default": None}}}
        result = serialize_schema(schema)
        assert result["crypto"]["quote"]["type"] == "str"

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
        assert result["scheduling"]["delay_hours"]["type"] == "int"
        assert result["data"]["lookback_days"]["type"] == "int"
        assert result["crypto"]["preferred_quote_currency"]["type"] == "str"

    def test_serialize_schema_handles_live_provider(self):
        """serialize_schema correctly serializes LiveDataProvider schema."""
        result = serialize_schema(LiveDataProvider.CONFIGURABLE)
        assert result["scheduling"]["pre_close_seconds"]["type"] == "int"
        assert result["scheduling"]["post_close_seconds"]["type"] == "int"

    def test_serialize_schema_returns_new_dict(self):
        """serialize_schema returns a new dict, not mutating original."""
        original = {"cat": {"field": {"type": int}}}
        result = serialize_schema(original)
        # Original should still have Python type
        assert original["cat"]["field"]["type"] == int
        # Result should have string
        assert result["cat"]["field"]["type"] == "int"


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
