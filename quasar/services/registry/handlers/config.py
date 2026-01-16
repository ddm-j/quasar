"""Provider/broker configuration handlers for Registry."""

import json
import logging
from typing import List

from fastapi import HTTPException, Query

from quasar.services.registry.handlers.base import HandlerMixin
from quasar.services.registry.schemas import (
    AvailableQuoteCurrenciesResponse,
    ClassSummaryItem,
    ClassType,
    ProviderPreferences,
    ProviderPreferencesResponse,
    ProviderPreferencesUpdate,
)

logger = logging.getLogger(__name__)


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

    async def handle_update_provider_config(
        self,
        update: ProviderPreferencesUpdate,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'")
    ) -> ProviderPreferencesResponse:
        """Update provider configuration preferences.

        Args:
            update (ProviderPreferencesUpdate): Preferences to update.
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.

        Returns:
            ProviderPreferencesResponse: Updated preferences for the provider.
        """
        logger.info(f"Registry.handle_update_provider_config: Updating config for {class_name}/{class_type}")

        # First verify provider exists
        exists_query = """
            SELECT 1 FROM code_registry WHERE class_name = $1 AND class_type = $2
        """
        exists = await self.pool.fetchval(exists_query, class_name, class_type)
        if not exists:
            logger.warning(f"Registry.handle_update_provider_config: Provider {class_name}/{class_type} not found")
            raise HTTPException(status_code=404, detail=f"Provider '{class_name}' ({class_type}) not found")

        # Update preferences using JSONB merge
        update_query = """
            UPDATE code_registry
            SET preferences = jsonb_strip_nulls(COALESCE(preferences, '{}'::jsonb) || $3::jsonb)
            WHERE class_name = $1 AND class_type = $2
            RETURNING preferences
        """

        try:
            # Convert update model to dict, removing None values
            update_dict = update.model_dump(exclude_unset=True, exclude_none=True)
            if not update_dict:
                logger.warning(f"Registry.handle_update_provider_config: No updates provided for {class_name}/{class_type}")
                raise HTTPException(status_code=400, detail="No preferences provided for update")

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
