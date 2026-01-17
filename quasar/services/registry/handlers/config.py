"""Provider/broker configuration handlers for Registry."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, List

from fastapi import HTTPException, Query

from quasar.lib.providers.core import (
    DataProvider,
    HistoricalDataProvider,
    LiveDataProvider,
    IndexProvider,
)
from quasar.services.registry.handlers.base import HandlerMixin
from quasar.services.registry.schemas import (
    AvailableQuoteCurrenciesResponse,
    ClassSummaryItem,
    ClassType,
    ConfigSchemaResponse,
    ProviderPreferences,
    ProviderPreferencesResponse,
    ProviderPreferencesUpdate,
    SecretKeysResponse,
)

logger = logging.getLogger(__name__)

# Schema map: class_subtype -> CONFIGURABLE dict
SCHEMA_MAP: dict[str, dict[str, dict[str, Any]]] = {
    "historical": HistoricalDataProvider.CONFIGURABLE,
    "realtime": LiveDataProvider.CONFIGURABLE,
    "index": IndexProvider.CONFIGURABLE,
}


def get_schema_for_subtype(class_subtype: str) -> dict[str, dict[str, Any]] | None:
    """Get the CONFIGURABLE schema for a given class_subtype.

    Args:
        class_subtype: The provider subtype (e.g., "historical", "realtime", "index").

    Returns:
        The CONFIGURABLE dict for the subtype, or None if not found.
    """
    return SCHEMA_MAP.get(class_subtype)


def serialize_schema(schema: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Convert schema with Python type objects to JSON-serializable format.

    Converts Python type objects (int, str, etc.) to string representations
    so the schema can be serialized to JSON.

    Args:
        schema: The CONFIGURABLE schema dict with Python type objects.

    Returns:
        A JSON-serializable copy of the schema with types as strings.
    """
    result: dict[str, dict[str, Any]] = {}
    for category, fields in schema.items():
        result[category] = {}
        for field_name, field_def in fields.items():
            result[category][field_name] = {}
            for key, value in field_def.items():
                if key == "type" and isinstance(value, type):
                    # Convert Python type to string representation
                    result[category][field_name][key] = value.__name__
                else:
                    result[category][field_name][key] = value
    return result


def log_validation_failure(
    class_name: str,
    class_type: str,
    reason: str
) -> None:
    """Log a preference validation failure per FR-026.

    Logs validation failures with provider name, timestamp, and reason for rejection.
    Uses structured logging format for consistency and searchability.

    Args:
        class_name: The provider/broker name.
        class_type: The class type (provider/broker).
        reason: The specific validation failure reason.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    logger.warning(
        f"Preference validation failure: provider={class_name}, "
        f"type={class_type}, timestamp={timestamp}, reason={reason}"
    )


def log_preference_change(
    class_name: str,
    class_type: str,
    change_categories: list[str]
) -> None:
    """Log a preference change per FR-025.

    Logs preference changes with provider name, timestamp, and change type.
    Uses structured logging format for consistency and searchability.

    Args:
        class_name: The provider/broker name.
        class_type: The class type (provider/broker).
        change_categories: List of changed preference categories (e.g., ["scheduling", "data", "crypto"]).
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    change_type = ", ".join(change_categories)
    logger.info(
        f"Preference change: provider={class_name}, "
        f"type={class_type}, timestamp={timestamp}, change_type={change_type}"
    )


def validate_preferences_against_schema(
    preferences: dict[str, Any],
    schema: dict[str, dict[str, Any]],
    class_name: str,
    class_type: str = "provider"
) -> list[str]:
    """Validate preferences dict against a CONFIGURABLE schema.

    Checks:
    - Field existence: Only allows fields declared in schema
    - Type checking: Values must match expected types
    - Range checking: Numeric values must be within min/max bounds

    Each validation failure is logged per FR-026 with provider name,
    timestamp, and reason for rejection.

    Args:
        preferences: The preferences dict to validate (e.g., {"scheduling": {"delay_hours": 6}}).
        schema: The CONFIGURABLE schema dict for the provider type.
        class_name: Provider name for error messages.
        class_type: Class type (provider/broker) for logging.

    Returns:
        List of validation error messages (empty if valid).
    """
    errors: list[str] = []

    for category, fields in preferences.items():
        # Check if category exists in schema
        if category not in schema:
            reason = f"Unknown preference category '{category}'"
            errors.append(f"{reason} for provider {class_name}")
            log_validation_failure(class_name, class_type, reason)
            continue

        # Fields must be a dict
        if not isinstance(fields, dict):
            reason = f"Preference category '{category}' must be an object"
            errors.append(reason)
            log_validation_failure(class_name, class_type, reason)
            continue

        schema_category = schema[category]

        for field_name, value in fields.items():
            # Check if field exists in schema category
            if field_name not in schema_category:
                reason = f"Unknown field '{category}.{field_name}'"
                errors.append(f"{reason} for provider {class_name}")
                log_validation_failure(class_name, class_type, reason)
                continue

            field_schema = schema_category[field_name]
            expected_type = field_schema.get("type")

            # Skip None values (they're valid for optional fields)
            if value is None:
                continue

            # Type validation
            if expected_type is not None:
                if expected_type == int and not isinstance(value, int):
                    reason = f"Field '{category}.{field_name}' must be an integer, got {type(value).__name__}"
                    errors.append(reason)
                    log_validation_failure(class_name, class_type, reason)
                    continue
                elif expected_type == str and not isinstance(value, str):
                    reason = f"Field '{category}.{field_name}' must be a string, got {type(value).__name__}"
                    errors.append(reason)
                    log_validation_failure(class_name, class_type, reason)
                    continue

            # Range validation for numeric types
            if isinstance(value, (int, float)):
                min_val = field_schema.get("min")
                max_val = field_schema.get("max")

                if min_val is not None and value < min_val:
                    reason = f"Field '{category}.{field_name}' must be >= {min_val}, got {value}"
                    errors.append(reason)
                    log_validation_failure(class_name, class_type, reason)
                if max_val is not None and value > max_val:
                    reason = f"Field '{category}.{field_name}' must be <= {max_val}, got {value}"
                    errors.append(reason)
                    log_validation_failure(class_name, class_type, reason)

    return errors


class ConfigHandlersMixin(HandlerMixin):
    """Mixin providing provider/broker configuration handlers.

    Handles:
        - Classes summary listing
        - Provider configuration (preferences) get/update
        - Quote currency availability queries
    """

    async def handle_get_classes_summary(self) -> List[ClassSummaryItem]:
        """Return summary information for registered providers and brokers."""
        logger.info("Registry.handle_get_classes_summary: Received request for classes summary.")

        # Query to get registered classes and a count of their assets
        # This query joins code_registry with assets to count assets per class.
        # It uses a LEFT JOIN to include classes that might not have any assets yet.
        query = """
            SELECT
                cr.id,
                cr.class_name,
                cr.class_type,
                cr.class_subtype,
                cr.uploaded_at::TEXT AS uploaded_at,
                COUNT(a.symbol) AS asset_count
            FROM
                code_registry cr
            LEFT JOIN
                assets a ON cr.class_name = a.class_name AND cr.class_type = a.class_type
            WHERE
                cr.class_type IN ('provider', 'broker')
            GROUP BY
                cr.id, cr.class_name, cr.class_type, cr.class_subtype,
                cr.uploaded_at
            ORDER BY
                cr.class_type, cr.class_name;
        """

        classes_summary: List[ClassSummaryItem] = []
        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query)

            if not records:
                logger.info("Registry.handle_get_classes_summary: No registered classes found.")
                return []  # Return empty list if none found

            for record in records:
                classes_summary.append(ClassSummaryItem(**dict(record)))  # Convert asyncpg.Record to dict then to Pydantic model

            logger.info(f"Registry.handle_get_classes_summary: Returning summary for {len(classes_summary)} classes.")
            return classes_summary

        except Exception as e_db_fetch:
            logger.error(f"Registry.handle_get_classes_summary: Error fetching classes summary: {e_db_fetch}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching classes summary")

    async def handle_get_provider_config(
        self,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> ProviderPreferencesResponse:
        """Get provider configuration preferences.

        Args:
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            ProviderPreferencesResponse: Current preferences for the provider.
        """
        logger.info(f"Registry.handle_get_provider_config: Getting config for {class_name}/{class_type}")

        # First verify provider exists
        exists_query = """
            SELECT 1 FROM code_registry WHERE class_name = $1 AND class_type = $2
        """
        preferences_query = """
            SELECT preferences
            FROM code_registry
            WHERE class_name = $1 AND class_type = $2
        """

        try:
            exists = await self.pool.fetchval(exists_query, class_name, class_type)
            if not exists:
                logger.warning(f"Registry.handle_get_provider_config: Provider {class_name}/{class_type} not found")
                raise HTTPException(status_code=404, detail=f"Provider '{class_name}' ({class_type}) not found")

            preferences_data = await self.pool.fetchval(preferences_query, class_name, class_type)

            # Convert JSONB to ProviderPreferences model, defaulting to empty if null
            if preferences_data:
                # Handle both dict (parsed JSONB) and string (raw JSONB) cases
                if isinstance(preferences_data, str):
                    preferences_data = json.loads(preferences_data)
                preferences = ProviderPreferences(**preferences_data)
            else:
                preferences = ProviderPreferences()

            logger.info(f"Registry.handle_get_provider_config: Retrieved config for {class_name}/{class_type}")
            return ProviderPreferencesResponse(
                class_name=class_name,
                class_type=class_type,
                preferences=preferences
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_get_provider_config: Unexpected error for {class_name}/{class_type}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving provider config")

    async def handle_get_config_schema(
        self,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> ConfigSchemaResponse:
        """Get the configuration schema for a provider.

        Returns the CONFIGURABLE schema defining available preferences
        for a provider based on its class_subtype (historical, realtime, index).

        Args:
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            ConfigSchemaResponse: Schema with configurable fields.
        """
        logger.info(f"Registry.handle_get_config_schema: Getting schema for {class_name}/{class_type}")

        # Query to get provider's class_subtype
        query = """
            SELECT class_subtype
            FROM code_registry
            WHERE class_name = $1 AND class_type = $2
        """

        try:
            class_subtype = await self.pool.fetchval(query, class_name, class_type)

            if not class_subtype:
                logger.warning(f"Registry.handle_get_config_schema: Provider {class_name}/{class_type} not found")
                raise HTTPException(status_code=404, detail=f"Provider '{class_name}' ({class_type}) not found")

            # Get the schema for this subtype
            schema = get_schema_for_subtype(class_subtype)
            if schema is None:
                logger.warning(f"Registry.handle_get_config_schema: No schema found for subtype '{class_subtype}'")
                # Return empty schema if subtype not recognized
                serialized_schema: dict[str, dict[str, Any]] = {}
            else:
                # Convert Python type objects to JSON-serializable strings
                serialized_schema = serialize_schema(schema)

            logger.info(f"Registry.handle_get_config_schema: Returning schema for {class_name}/{class_type} (subtype: {class_subtype})")
            return ConfigSchemaResponse(
                class_name=class_name,
                class_type=class_type,
                class_subtype=class_subtype,
                schema=serialized_schema
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_get_config_schema: Unexpected error for {class_name}/{class_type}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving config schema")

    async def handle_update_provider_config(
        self,
        update: ProviderPreferencesUpdate,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> ProviderPreferencesResponse:
        """Update provider configuration preferences.

        Validates updates against the provider-type-specific CONFIGURABLE schema
        before persisting to ensure field existence, type correctness, and range bounds.

        Args:
            update (ProviderPreferencesUpdate): Preferences to update.
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            ProviderPreferencesResponse: Updated preferences for the provider.

        Raises:
            HTTPException: 404 if provider not found, 400 if validation fails.
        """
        logger.info(f"Registry.handle_update_provider_config: Updating config for {class_name}/{class_type}")

        # First verify provider exists and get class_subtype for schema lookup
        subtype_query = """
            SELECT class_subtype FROM code_registry WHERE class_name = $1 AND class_type = $2
        """
        class_subtype = await self.pool.fetchval(subtype_query, class_name, class_type)
        if not class_subtype:
            logger.warning(f"Registry.handle_update_provider_config: Provider {class_name}/{class_type} not found")
            raise HTTPException(status_code=404, detail=f"Provider '{class_name}' ({class_type}) not found")

        # Convert update model to dict, removing None values
        update_dict = update.model_dump(exclude_unset=True, exclude_none=True)
        if not update_dict:
            logger.warning(f"Registry.handle_update_provider_config: No updates provided for {class_name}/{class_type}")
            raise HTTPException(status_code=400, detail="No preferences provided for update")

        # Validate against provider-type-specific schema
        schema = get_schema_for_subtype(class_subtype)
        if schema:
            validation_errors = validate_preferences_against_schema(
                update_dict, schema, class_name, class_type
            )
            if validation_errors:
                error_detail = "; ".join(validation_errors)
                # Note: Individual validation failures are already logged by log_validation_failure (FR-026)
                raise HTTPException(status_code=400, detail=f"Validation error: {error_detail}")

        # Update preferences using JSONB merge
        update_query = """
            UPDATE code_registry
            SET preferences = jsonb_strip_nulls(COALESCE(preferences, '{}'::jsonb) || $3::jsonb)
            WHERE class_name = $1 AND class_type = $2
            RETURNING preferences
        """

        try:

            # Convert dict to JSON string for asyncpg JSONB parameter
            update_json = json.dumps(update_dict)

            updated_preferences = await self.pool.fetchval(
                update_query,
                class_name,
                class_type,
                update_json
            )

            # Convert back to ProviderPreferences model
            if updated_preferences:
                # Handle both dict (parsed JSONB) and string (raw JSONB) cases
                if isinstance(updated_preferences, str):
                    updated_preferences = json.loads(updated_preferences)
                preferences = ProviderPreferences(**updated_preferences)
            else:
                preferences = ProviderPreferences()

            # Log preference change per FR-025
            change_categories = list(update_dict.keys())
            log_preference_change(class_name, class_type, change_categories)

            logger.info(f"Registry.handle_update_provider_config: Updated config for {class_name}/{class_type}")
            return ProviderPreferencesResponse(
                class_name=class_name,
                class_type=class_type,
                preferences=preferences
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_update_provider_config: Unexpected error for {class_name}/{class_type}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while updating provider config")

    async def handle_get_available_quote_currencies(
        self,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> AvailableQuoteCurrenciesResponse:
        """Get available quote currencies for crypto assets of a provider.

        Args:
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            AvailableQuoteCurrenciesResponse: List of available quote currencies.
        """
        logger.info(f"Registry.handle_get_available_quote_currencies: Getting quote currencies for {class_name}/{class_type}")

        query = """
            SELECT DISTINCT quote_currency
            FROM assets
            WHERE class_name = $1
              AND class_type = $2
              AND asset_class_group = 'crypto'
              AND quote_currency IS NOT NULL
            ORDER BY quote_currency
        """

        try:
            records = await self.pool.fetch(query, class_name, class_type)
            quote_currencies = [record['quote_currency'] for record in records]

            logger.info(f"Registry.handle_get_available_quote_currencies: Found {len(quote_currencies)} quote currencies for {class_name}/{class_type}")
            return AvailableQuoteCurrenciesResponse(
                class_name=class_name,
                class_type=class_type,
                available_quote_currencies=quote_currencies
            )
        except Exception as e:
            logger.error(f"Registry.handle_get_available_quote_currencies: Unexpected error for {class_name}/{class_type}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving available quote currencies")

    async def handle_get_secret_keys(
        self,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> SecretKeysResponse:
        """Get stored secret key names for a provider (not the values).

        Decrypts the stored secrets and returns only the key names,
        used by the frontend to render credential update form fields.

        Args:
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            SecretKeysResponse: List of secret key names.

        Raises:
            HTTPException: 404 if provider not found, 500 on decryption failure.
        """
        logger.info(f"Registry.handle_get_secret_keys: Getting secret keys for {class_name}/{class_type}")

        # Query to get file_hash, nonce, and ciphertext for the provider
        query = """
            SELECT file_hash, nonce, ciphertext
            FROM code_registry
            WHERE class_name = $1 AND class_type = $2
        """

        try:
            row = await self.pool.fetchrow(query, class_name, class_type)

            if not row:
                logger.warning(f"Registry.handle_get_secret_keys: Provider {class_name}/{class_type} not found")
                raise HTTPException(status_code=404, detail=f"Provider '{class_name}' ({class_type}) not found")

            file_hash = row['file_hash']
            nonce = row['nonce']
            ciphertext = row['ciphertext']

            # Check if provider has stored secrets
            if not nonce or not ciphertext:
                logger.info(f"Registry.handle_get_secret_keys: No secrets stored for {class_name}/{class_type}")
                return SecretKeysResponse(
                    class_name=class_name,
                    class_type=class_type,
                    keys=[]
                )

            # Decrypt secrets to extract key names
            try:
                derived_context = self.system_context.get_derived_context(file_hash)
                decrypted = derived_context.decrypt(nonce, ciphertext, None)
                secrets_dict = json.loads(decrypted.decode('utf-8'))
                keys = list(secrets_dict.keys())
            except Exception as e:
                logger.error(f"Registry.handle_get_secret_keys: Failed to decrypt secrets for {class_name}/{class_type}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to decrypt provider secrets")

            logger.info(f"Registry.handle_get_secret_keys: Found {len(keys)} secret keys for {class_name}/{class_type}")
            return SecretKeysResponse(
                class_name=class_name,
                class_type=class_type,
                keys=keys
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_get_secret_keys: Unexpected error for {class_name}/{class_type}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving secret keys")
