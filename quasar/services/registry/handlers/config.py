"""Provider/broker configuration handlers for Registry."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, List

import aiohttp
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
    SecretsUpdateRequest,
    SecretsUpdateResponse,
)

logger = logging.getLogger(__name__)

# Schema map: class_subtype -> CONFIGURABLE dict
SCHEMA_MAP: dict[str, dict[str, dict[str, Any]]] = {
    "Historical": HistoricalDataProvider.CONFIGURABLE,
    "Live": LiveDataProvider.CONFIGURABLE,
    "IndexProvider": IndexProvider.CONFIGURABLE,
}


def get_schema_for_subtype(class_subtype: str) -> dict[str, dict[str, Any]] | None:
    """Get the CONFIGURABLE schema for a given class_subtype.

    Args:
        class_subtype: The provider subtype (e.g., "Historical", "Live", "IndexProvider").

    Returns:
        The CONFIGURABLE dict for the subtype, or None if not found.
    """
    return SCHEMA_MAP.get(class_subtype)


# Mapping from Python types to JSON Schema type names
PYTHON_TYPE_TO_JSON: dict[type, str] = {
    int: "integer",
    str: "string",
    float: "number",
    bool: "boolean",
}


def serialize_schema(schema: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Convert schema with Python type objects to JSON-serializable format.

    Converts Python type objects to JSON Schema-friendly string representations
    so the schema can be serialized to JSON:
    - int → "integer"
    - str → "string"
    - float → "number"
    - bool → "boolean"

    The output includes complete metadata for each field: type, default,
    min, max (where applicable), and description.

    Args:
        schema: The CONFIGURABLE schema dict with Python type objects.

    Returns:
        A JSON-serializable copy of the schema with types as JSON Schema strings.
    """
    result: dict[str, dict[str, Any]] = {}
    for category, fields in schema.items():
        result[category] = {}
        for field_name, field_def in fields.items():
            result[category][field_name] = {}
            for key, value in field_def.items():
                if key == "type" and isinstance(value, type):
                    # Convert Python type to JSON Schema type name
                    result[category][field_name][key] = PYTHON_TYPE_TO_JSON.get(
                        value, value.__name__
                    )
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


def log_credential_update(
    class_name: str,
    class_type: str,
    key_count: int,
    unload_triggered: bool
) -> None:
    """Log a credential update operation.

    Logs credential updates with provider name, timestamp, and unload status.
    Uses structured logging format for consistency and searchability.

    Args:
        class_name: The provider/broker name.
        class_type: The class type (provider/broker).
        key_count: Number of credential keys updated.
        unload_triggered: Whether the DataHub unload was successfully triggered.
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"Credential update: provider={class_name}, "
        f"type={class_type}, timestamp={timestamp}, "
        f"keys_updated={key_count}, unload_triggered={unload_triggered}"
    )


async def _trigger_index_sync_refresh(class_name: str) -> bool:
    """Notify DataHub to refresh index sync jobs after frequency change.

    This is a best-effort notification - failures are logged but don't
    cause the preference update to fail. DataHub will still pick up
    the change on its next periodic refresh cycle.

    Args:
        class_name: The IndexProvider name (for logging).

    Returns:
        True if refresh was triggered successfully, False otherwise.
    """
    url = "http://datahub:8080/api/datahub/index-sync/refresh"
    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url) as response:
                if response.status == 200:
                    logger.info(f"Index sync jobs refreshed after {class_name} frequency update")
                    return True
                else:
                    error_text = await response.text()
                    logger.warning(f"Index sync refresh returned {response.status}: {error_text}")
                    return False
    except aiohttp.ClientConnectorError as e:
        logger.warning(f"Cannot connect to DataHub for index sync refresh: {e}")
        return False
    except TimeoutError:
        logger.warning(f"Timeout calling DataHub index sync refresh (5s exceeded)")
        return False
    except Exception as e:
        logger.warning(f"Error calling DataHub index sync refresh: {e}")
        return False


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

            # Allowed values validation for string enums
            allowed_values = field_schema.get("allowed")
            if allowed_values is not None and isinstance(value, str):
                if value not in allowed_values:
                    allowed_str = ", ".join(allowed_values)
                    reason = f"Field '{category}.{field_name}' must be one of [{allowed_str}], got '{value}'"
                    errors.append(reason)
                    log_validation_failure(class_name, class_type, reason)

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

            # Trigger DataHub to refresh index sync jobs if sync_frequency was updated
            if class_subtype == "IndexProvider" and "scheduling" in update_dict:
                await _trigger_index_sync_refresh(class_name)

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
            except json.JSONDecodeError as e:
                logger.error(f"Registry.handle_get_secret_keys: Corrupted credentials for {class_name}/{class_type}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail="Stored credentials are corrupted")
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

    async def handle_update_secrets(
        self,
        update: SecretsUpdateRequest,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> SecretsUpdateResponse:
        """Update stored credentials for a provider with re-encryption.

        Re-encrypts and stores new credentials using a new nonce (FR-016).
        All secret keys must be provided (all-or-nothing update per FR-015).

        Args:
            update (SecretsUpdateRequest): New credentials to store.
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            SecretsUpdateResponse: Status and list of updated key names.

        Raises:
            HTTPException: 404 if provider not found, 400 if secrets empty, 500 on encryption failure.
        """
        logger.info(f"Registry.handle_update_secrets: Updating secrets for {class_name}/{class_type}")

        # Validate that secrets dict is not empty
        if not update.secrets:
            logger.warning(f"Registry.handle_update_secrets: Empty secrets provided for {class_name}/{class_type}")
            raise HTTPException(status_code=400, detail="Secrets dict cannot be empty")

        # Query to get file_hash for key derivation
        query = """
            SELECT file_hash
            FROM code_registry
            WHERE class_name = $1 AND class_type = $2
        """

        try:
            file_hash = await self.pool.fetchval(query, class_name, class_type)

            if not file_hash:
                logger.warning(f"Registry.handle_update_secrets: Provider {class_name}/{class_type} not found")
                raise HTTPException(status_code=404, detail=f"Provider '{class_name}' ({class_type}) not found")

            # Convert secrets dict to JSON bytes for encryption
            secrets_bytes = json.dumps(update.secrets).encode('utf-8')

            # Re-encrypt with new nonce (FR-016)
            try:
                new_nonce, new_ciphertext = self.system_context.create_context_data(file_hash, secrets_bytes)
            except Exception as e:
                logger.error(f"Registry.handle_update_secrets: Failed to encrypt secrets for {class_name}/{class_type}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail="Failed to encrypt secrets")

            # Update database with new nonce and ciphertext
            update_query = """
                UPDATE code_registry
                SET nonce = $3, ciphertext = $4
                WHERE class_name = $1 AND class_type = $2
            """
            await self.pool.execute(update_query, class_name, class_type, new_nonce, new_ciphertext)

            keys = list(update.secrets.keys())
            logger.info(f"Registry.handle_update_secrets: Successfully updated {len(keys)} secrets for {class_name}/{class_type}")

            # Call DataHub unload endpoint to force provider reload with new credentials
            # This is best-effort - we don't fail the secret update if DataHub is unreachable
            unload_triggered = False
            if class_type == "provider":
                unload_url = f"http://datahub:8080/api/datahub/providers/{class_name}/unload"
                timeout = aiohttp.ClientTimeout(total=5)  # 5-second timeout
                try:
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.post(unload_url) as response:
                            if response.status == 200:
                                unload_triggered = True
                                logger.info(f"Registry.handle_update_secrets: Triggered unload for provider {class_name}")
                            elif response.status == 404:
                                # Provider not loaded in DataHub - this is fine
                                logger.info(f"Registry.handle_update_secrets: Provider {class_name} not loaded in DataHub, skipping unload")
                            else:
                                error_text = await response.text()
                                logger.warning(f"Registry.handle_update_secrets: DataHub unload returned {response.status} for {class_name}: {error_text}")
                except aiohttp.ClientConnectorError as e_conn:
                    logger.warning(f"Registry.handle_update_secrets: Cannot connect to DataHub for unload: {e_conn}")
                except TimeoutError:
                    logger.warning(f"Registry.handle_update_secrets: Timeout calling DataHub unload for {class_name} (5s exceeded)")
                except Exception as e_unload:
                    logger.warning(f"Registry.handle_update_secrets: Error calling DataHub unload for {class_name}: {e_unload}")

            # Log credential update with structured format
            log_credential_update(class_name, class_type, len(keys), unload_triggered)

            return SecretsUpdateResponse(
                status="updated",
                keys=keys
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_update_secrets: Unexpected error for {class_name}/{class_type}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while updating secrets")
