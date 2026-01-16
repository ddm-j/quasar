"""Asset management handlers for Registry."""

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import unquote_plus

import aiohttp
from asyncpg.exceptions import UniqueViolationError
from fastapi import Depends, HTTPException, Query

from quasar.lib.enums import ASSET_CLASSES, normalize_asset_class
from quasar.services.registry.handlers.base import HandlerMixin
from quasar.services.registry.matcher import MatchResult
from quasar.services.registry.schemas import (
    AssetItem,
    AssetQueryParams,
    AssetResponse,
    ClassType,
    CommonSymbolItem,
    CommonSymbolQueryParams,
    CommonSymbolResponse,
    UpdateAssetsResponse,
)
from quasar.services.registry.utils import FilterBuilder

logger = logging.getLogger(__name__)


class AssetHandlersMixin(HandlerMixin):
    """Mixin providing asset management handlers.

    Handles:
        - Asset update (single provider and all providers)
        - Asset query with filtering, sorting, and pagination
        - Common symbol queries
        - Identity matching application
    """

    async def handle_update_assets(
        self,
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'"),
        class_name: str = Query(..., description="Class name (provider/broker name)")
    ) -> UpdateAssetsResponse:
        """Update assets for a specific registered provider or broker.

        Args:
            class_type (ClassType): ``provider`` or ``broker``.
            class_name (str): Registered class name.

        Returns:
            UpdateAssetsResponse: Summary statistics for the operation.
        """
        # Verify if the class_name and class_type are registered
        query_provider_exists = """
            SELECT id FROM code_registry WHERE class_name = $1 AND class_type = $2;
            """
        try:
            provider_reg_id = await self.pool.fetchval(query_provider_exists, class_name, class_type)
            if not provider_reg_id:
                logger.warning(f"Registry.handle_update_assets: Class '{class_name}' ({class_type}) is not registered.")
                raise HTTPException(status_code=404, detail=f"Class '{class_name}' ({class_type}) is not registered.")
        except HTTPException:
            raise
        except Exception as e_db_check:
            logger.error(f"Registry.handle_update_assets: Error checking registration for {class_name} ({class_type}): {e_db_check}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while checking registration")

        # Call internal method to update assets
        stats = await self._update_assets_for_provider(class_name, class_type)
        if stats.get('status') != 200:
            logger.error(f"Registry.handle_update_assets: Error updating assets for {class_name} ({class_type}): {stats.get('error')}")
            raise HTTPException(status_code=stats.get('status', 500), detail=stats.get('error', 'Unknown error'))

        # Return the stats as a response model
        return UpdateAssetsResponse(**stats)

    async def handle_update_all_assets(self) -> List[UpdateAssetsResponse]:
        """Trigger asset updates for all registered providers and brokers.

        After updating assets from each provider, runs a global identity matching
        pass to identify any remaining unidentified assets across all providers.
        """
        logger.info("Registry.handle_update_all_assets: Triggering asset update for all registered providers.")
        # Fetch all registered providers
        query_providers = """
            SELECT class_name, class_type FROM code_registry;
            """
        try:
            async with self.pool.acquire() as conn:
                providers = await conn.fetch(query_providers)
        except Exception as e_db_fetch:
            logger.error(f"Registry.handle_update_all_assets: Error fetching registered providers: {e_db_fetch}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching registered providers")

        if not providers:
            logger.info("Registry.handle_update_all_assets: No registered providers found.")
            return []

        # Update assets for each provider (identity matching runs per-provider)
        stats_list = []
        for provider in providers:
            class_name = provider['class_name']
            class_type = provider['class_type']
            stats = await self._update_assets_for_provider(class_name, class_type)
            stats_list.append(UpdateAssetsResponse(**stats))

        # Run global identity matching for any remaining unidentified assets
        # This catches assets that may have been missed by per-provider matching
        try:
            all_matches = await self.matcher.identify_all_unidentified_assets()
            if all_matches:
                global_stats = await self._apply_identity_matches(all_matches)
                logger.info(
                    f"Registry.handle_update_all_assets: Global identity matching complete: "
                    f"identified={global_stats['identified']}, skipped={global_stats['skipped']}"
                )
        except Exception as e:
            logger.warning(f"Registry.handle_update_all_assets: Global identity matching failed: {e}")

        return stats_list

    async def _update_assets_for_provider(self, class_name: str, class_type: str) -> dict[str, Any]:
        """
        Updates the 'assets' table for a given provider by fetching its available symbols
        from DataHub and upserting them into the database.
        Assumes the provider (class_name, class_type) is already verified as registered and active.

        Args:
            class_name: The name of the provider (maps to code_registry.class_name).
            class_type: The type of the code (e.g., 'provider', 'broker').

        Returns:
            A dictionary containing statistics of the operation (added, updated, failed).
        """
        stats = {
            'class_name': class_name,
            'class_type': class_type,
            'total_symbols': 0,
            'processed_symbols': 0,
            'added_symbols': 0,
            'updated_symbols': 0,
            'failed_symbols': 0,
            'identity_matched': 0,
            'identity_skipped': 0,
            'mappings_created': 0,
            'mappings_skipped': 0,
            'mappings_failed': 0,
            'members_added': 0,
            'members_removed': 0,
            'members_unchanged': 0,
            'status': 200
        }

        # Get class_subtype to determine provider type
        async with self.pool.acquire() as conn:
            subtype_record = await conn.fetchrow(
                "SELECT class_subtype FROM code_registry WHERE class_name = $1 AND class_type = $2",
                class_name, class_type
            )
            class_subtype = subtype_record['class_subtype'] if subtype_record else None

        is_index_provider = (class_subtype == 'IndexProvider')
        constituent_weights: dict[str, float | None] = {}  # For membership sync

        # Fetch available symbols/constituents from DataHub
        if is_index_provider:
            datahub_url = 'http://datahub:8080/internal/providers/constituents'
            logger.info(f"Registry._update_assets_for_provider: Fetching constituents for IndexProvider {class_name}")
        else:
            datahub_url = 'http://datahub:8080/internal/providers/available-symbols'
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(datahub_url, params={'provider_name': class_name}) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        # Handle wrapped response format: {"items": [...]}
                        if isinstance(response_data, dict) and 'items' in response_data:
                            symbol_info_list = response_data['items']
                        elif isinstance(response_data, list):
                            symbol_info_list = response_data
                        else:
                            logger.warning(f"Invalid response format from DataHub")
                            stats['error'] = "Invalid response format from DataHub"
                            stats['status'] = 500
                            return stats
                        stats['total_symbols'] = len(symbol_info_list)
                        logger.info(f"Registry._update_assets_for_provider: Received {stats['total_symbols']} symbols from DataHub for {class_name}.")
                    elif response.status == 404:  # Provider not found/loaded in DataHub
                        logger.warning(f"Registry._update_assets_for_provider: DataHub reported provider {class_name} not found or not loaded. This might indicate an issue if it's registered.")
                        stats["error"] = f"DataHub: Provider {class_name} not found/loaded"
                        stats["status"] = 404
                        return stats
                    elif response.status == 501:  # Not Implemented by provider in DataHub
                        logger.warning(f"Registry._update_assets_for_provider: DataHub: Provider {class_name} does not support symbol discovery.")
                        stats["error"] = f"DataHub: Provider {class_name} does not support symbol discovery"
                        stats["status"] = 501
                        return stats
                    else:
                        error_detail = await response.text()
                        logger.error(f"Registry._update_assets_for_provider: Error fetching symbols from DataHub for {class_name}: {response.status} - {error_detail}")
                        stats["error"] = f"DataHub error {response.status}"
                        stats["status"] = response.status
                        return stats
        except aiohttp.ClientConnectorError as e_conn:
            logger.error(f"Registry._update_assets_for_provider: Cannot connect to DataHub at {datahub_url}: {e_conn}")
            stats["error"] = "Cannot connect to DataHub"
            stats["status"] = 503
            return stats
        except Exception as e_http:
            logger.error(f"Registry._update_assets_for_provider: Exception calling DataHub for {class_name}: {e_http}", exc_info=True)
            stats["error"] = f"Exception calling DataHub: {str(e_http)}"
            stats["status"] = 500
            return stats

        if not symbol_info_list:
            if is_index_provider:
                # For IndexProviders, empty constituents means preserve existing memberships
                logger.warning(f"Registry._update_assets_for_provider: Empty constituents returned for IndexProvider '{class_name}'. Preserving existing memberships.")
                stats["message"] = "No constituents returned from provider. Existing memberships preserved."
                stats["status"] = 200
                return stats
            else:
                logger.info(f"Registry._update_assets_for_provider: No symbols returned or fetched from DataHub for provider {class_name}.")
                stats["message"] = "No symbols returned from DataHub"
                stats["status"] = 204
                return stats

        # For IndexProviders, store weights and convert constituents to symbol format
        if is_index_provider:
            constituent_weights = {c['symbol']: c.get('weight') for c in symbol_info_list}
            symbol_info_list = [
                {
                    'provider': class_name,
                    'provider_id': None,
                    'symbol': c['symbol'],
                    'matcher_symbol': c.get('matcher_symbol') or c['symbol'],
                    'name': c.get('name') or '',
                    'exchange': c.get('exchange') or '',
                    'asset_class': c.get('asset_class') or '',
                    'base_currency': c.get('base_currency') or '',
                    'quote_currency': c.get('quote_currency') or '',
                }
                for c in symbol_info_list
            ]
            logger.info(f"Registry._update_assets_for_provider: Converted {len(symbol_info_list)} constituents to symbol format for {class_name}")

        # Upsert symbols into the database
        # When provider supplies primary_id, set primary_id_source = 'provider'
        # On conflict: only update primary_id if provider supplies one (preserve matcher IDs)
        # Note: $4::TEXT cast required for asyncpg prepared statement type inference
        upsert_query = """
                        INSERT INTO assets (
                            class_name, class_type, external_id, primary_id, primary_id_source, symbol,
                            matcher_symbol, name, exchange, asset_class,
                            base_currency, quote_currency, country
                        ) VALUES (
                            $1, $2, $3, $4::TEXT,
                            CASE WHEN $4::TEXT IS NOT NULL THEN 'provider' ELSE NULL END,
                            $5, $6, $7, $8, $9, $10, $11, $12
                        )
                        ON CONFLICT (class_name, class_type, symbol) DO UPDATE SET
                            external_id = EXCLUDED.external_id,
                            -- Only update primary_id if provider supplies one (preserve matcher IDs)
                            primary_id = CASE
                                WHEN EXCLUDED.primary_id IS NOT NULL THEN EXCLUDED.primary_id
                                ELSE assets.primary_id
                            END,
                            primary_id_source = CASE
                                WHEN EXCLUDED.primary_id IS NOT NULL THEN 'provider'
                                ELSE assets.primary_id_source
                            END,
                            matcher_symbol = EXCLUDED.matcher_symbol,
                            name = EXCLUDED.name,
                            exchange = EXCLUDED.exchange,
                            asset_class = EXCLUDED.asset_class,
                            base_currency = EXCLUDED.base_currency,
                            quote_currency = EXCLUDED.quote_currency,
                            country = EXCLUDED.country
                        RETURNING xmax;
                    """
        processed_symbols = set()

        async with self.pool.acquire() as conn:
            prepared_upsert = await conn.prepare(upsert_query)
            savepoint_counter = 0
            async with conn.transaction():
                async def _exec_savepoint(cmd: str) -> None:
                    """Execute savepoint-related commands without aborting the transaction."""
                    try:
                        await conn.execute(cmd)
                    except Exception as exc:
                        logger.warning("Registry._update_assets_for_provider: Savepoint command '%s' failed: %s", cmd, exc, exc_info=True)
                for symbol_info in symbol_info_list:
                    if not isinstance(symbol_info, dict):
                        logger.warning(f"Invalid symbol info format: {symbol_info}")
                        stats['failed_symbols'] += 1
                        continue

                    symbol = symbol_info.get('symbol')
                    if not symbol:
                        logger.warning(f"Symbol is empty: {symbol_info}")
                        stats['failed_symbols'] += 1
                        continue
                    if symbol in processed_symbols:
                        logger.warning(f"Duplicate symbol found in response: {symbol}")
                        stats['failed_symbols'] += 1
                        continue

                    raw_asset_class = symbol_info.get('asset_class')
                    normalized_asset_class = normalize_asset_class(raw_asset_class)
                    if raw_asset_class and (normalized_asset_class not in ASSET_CLASSES):
                        logger.warning(f"Skipping symbol {symbol}: invalid asset_class '{raw_asset_class}'")
                        stats['failed_symbols'] += 1
                        continue

                    record_values = (
                        class_name,
                        class_type,
                        symbol_info.get('provider_id'),
                        symbol_info.get('primary_id'),
                        symbol,
                        symbol_info.get('matcher_symbol') or symbol,  # Fallback to symbol if not provided
                        symbol_info.get('name'),
                        symbol_info.get('exchange'),
                        normalized_asset_class,
                        symbol_info.get('base_currency'),
                        symbol_info.get('quote_currency'),
                        symbol_info.get('country')
                    )

                    savepoint_name = f"symbol_upsert_{savepoint_counter}"
                    savepoint_counter += 1
                    await _exec_savepoint(f"SAVEPOINT {savepoint_name}")
                    try:
                        result = await prepared_upsert.fetchrow(*record_values)
                        if result:
                            if result['xmax'] == 0:
                                stats['added_symbols'] += 1
                            else:
                                stats['updated_symbols'] += 1
                            processed_symbols.add(symbol)
                        else:
                            logger.warning(f"Failed to upsert symbol {symbol} for {class_name}.")
                            stats['failed_symbols'] += 1
                    except Exception as e_upsert:
                        logger.error(f"Registry._update_assets_for_provider: Error upserting symbol {symbol} for {class_name}: {e_upsert}", exc_info=True)
                        stats['failed_symbols'] += 1
                        await _exec_savepoint(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    finally:
                        await _exec_savepoint(f"RELEASE SAVEPOINT {savepoint_name}")

        stats['processed_symbols'] = stats['added_symbols'] + \
                                stats['updated_symbols'] + \
                                stats['failed_symbols']
        logger.info(f"Registry._update_assets_for_provider: Asset update summary for {class_name} ({class_type}): " \
                    f"Added={stats['added_symbols']}, Updated={stats['updated_symbols']}, Failed={stats['failed_symbols']}")

        # Run identity matching for unidentified assets
        try:
            match_results = await self.matcher.identify_unidentified_assets(
                class_name, class_type
            )
            if match_results:
                match_stats = await self._apply_identity_matches(match_results)
                stats['identity_matched'] = match_stats['identified']
                stats['identity_skipped'] = match_stats['skipped']
                logger.info(
                    f"Registry._update_assets_for_provider: Identity matching for {class_name}: "
                    f"identified={match_stats['identified']}, skipped={match_stats['skipped']}"
                )
        except Exception as e:
            logger.warning(f"Registry._update_assets_for_provider: Identity matching failed for {class_name}: {e}")
            # Don't fail the whole operation, just log the warning

        # Run automated mapping for newly identified assets
        try:
            logger.info(
                f"Registry._update_assets_for_provider: Starting automated mapping "
                f"for {class_name} ({class_type})"
            )

            mapping_candidates = await self.mapper.generate_mapping_candidates_for_provider(
                class_name, class_type
            )

            if mapping_candidates:
                mapping_stats = await self._apply_automated_mappings(mapping_candidates)
                stats['mappings_created'] = mapping_stats['created']
                stats['mappings_skipped'] = mapping_stats['skipped']
                stats['mappings_failed'] = mapping_stats['failed']

                logger.info(
                    f"Registry._update_assets_for_provider: Automated mapping complete "
                    f"for {class_name}: created={mapping_stats['created']}, "
                    f"skipped={mapping_stats['skipped']}, failed={mapping_stats['failed']}"
                )
            else:
                logger.info(
                    f"Registry._update_assets_for_provider: No mapping candidates "
                    f"generated for {class_name} ({class_type})"
                )
        except Exception as e:
            logger.warning(
                f"Registry._update_assets_for_provider: Automated mapping failed "
                f"for {class_name}: {e}"
            )
            # Don't fail the whole operation, just log the warning

        # Sync index memberships (IndexProvider only)
        if is_index_provider and constituent_weights:
            try:
                logger.info(f"Registry._update_assets_for_provider: Starting membership sync for IndexProvider {class_name}")
                membership_stats = await self._sync_index_memberships(
                    class_name,
                    class_type,
                    constituent_weights
                )
                stats['members_added'] = membership_stats.get('added', 0)
                stats['members_removed'] = membership_stats.get('removed', 0)
                stats['members_unchanged'] = membership_stats.get('unchanged', 0)
                logger.info(
                    f"Registry._update_assets_for_provider: Membership sync complete for {class_name}: "
                    f"added={stats['members_added']}, removed={stats['members_removed']}, "
                    f"unchanged={stats['members_unchanged']}"
                )
            except Exception as e:
                logger.warning(
                    f"Registry._update_assets_for_provider: Membership sync failed for {class_name}: {e}",
                    exc_info=True
                )
                stats['message'] = f"Assets updated but membership sync failed: {e}"
                # Don't fail the whole operation, just log the warning

        return stats

    async def _apply_identity_matches(self, matches: List[MatchResult]) -> dict:
        """Apply identity matcher results to assets table.

        Only updates assets where primary_id IS NULL (never overwrites provider-supplied IDs).

        Args:
            matches: List of MatchResult from identity matcher.

        Returns:
            Dict with counts: identified, skipped, failed, constraint_rejected
        """
        if not matches:
            return {'identified': 0, 'skipped': 0, 'failed': 0, 'constraint_rejected': 0}

        update_query = """
            UPDATE assets
            SET primary_id = $2,
                primary_id_source = 'matcher',
                identity_conf = $3,
                identity_match_type = $4,
                identity_updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
              AND primary_id IS NULL
            RETURNING id
        """

        stats = {'identified': 0, 'skipped': 0, 'failed': 0, 'constraint_rejected': 0}

        async with self.pool.acquire() as conn:
            for match in matches:
                try:
                    result = await conn.fetchval(
                        update_query,
                        match.asset_id,
                        match.primary_id,
                        match.confidence,
                        match.match_type
                    )
                    if result:
                        stats['identified'] += 1
                    else:
                        stats['skipped'] += 1
                except UniqueViolationError as e:
                    # This is expected when deduplication missed a duplicate or
                    # when re-attempting identification of previously rejected assets
                    if 'idx_assets_unique_securities_primary_id' in str(e):
                        logger.info(
                            f"Identity rejected by constraint for asset {match.identity_symbol} "
                            f"(primary_id={match.primary_id}): another asset already has this identity"
                        )
                        stats['constraint_rejected'] += 1
                    else:
                        logger.warning(f"Unexpected unique violation for asset {match.asset_id}: {e}")
                        stats['failed'] += 1
                except Exception as e:
                    logger.warning(f"Failed to apply match for asset {match.asset_id}: {e}")
                    stats['failed'] += 1

        return stats

    async def handle_get_assets(self, params: AssetQueryParams = Depends()) -> AssetResponse:
        """Return assets with optional filtering, sorting, and pagination.

        Args:
            params (AssetQueryParams): Query parameters parsed by FastAPI.

        Returns:
            AssetResponse: Paginated asset list and counts.
        """
        logger.info("Registry.handle_get_assets: Received request for assets.")

        try:
            # Pagination (already validated by Pydantic)
            limit = params.limit
            offset = params.offset

            # Sorting
            sort_by_str = params.sort_by
            sort_order_str = params.sort_order

            valid_sort_columns = [
                'id', 'class_name', 'class_type', 'symbol', 'name', 'exchange',
                'asset_class', 'base_currency', 'quote_currency', 'country',
                'primary_id', 'primary_id_source', 'matcher_symbol', 'identity_conf',
                'identity_match_type', 'identity_updated_at', 'asset_class_group',
                'sym_norm_full', 'sym_norm_root', 'external_id'
            ]

            sort_by_cols = [col.strip() for col in sort_by_str.split(',')]
            sort_orders = [order.strip().lower() for order in sort_order_str.split(',')]

            if not all(col in valid_sort_columns for col in sort_by_cols):
                raise HTTPException(status_code=400, detail="Invalid sort_by column")
            if not all(order in ['asc', 'desc'] for order in sort_orders):
                raise HTTPException(status_code=400, detail="Invalid sort_order value")

            if len(sort_orders) == 1 and len(sort_by_cols) > 1:  # Apply single order to all sort columns
                sort_orders = [sort_orders[0]] * len(sort_by_cols)
            elif len(sort_orders) != len(sort_by_cols):
                raise HTTPException(status_code=400, detail="Mismatch between sort_by and sort_order counts")

            order_by_clauses = [f"{col} {order.upper()}" for col, order in zip(sort_by_cols, sort_orders)]
            order_by_sql = ", ".join(order_by_clauses)

            # Filtering
            builder = FilterBuilder()
            builder.add('class_name', params.class_name_like, partial_match=True)
            builder.add('class_type', params.class_type)
            if params.asset_class is not None:
                norm_ac = normalize_asset_class(params.asset_class)
                if norm_ac not in ASSET_CLASSES:
                    raise HTTPException(status_code=400, detail=f"Invalid asset_class: {params.asset_class}")
                builder.add('asset_class', norm_ac)
            builder.add('base_currency', params.base_currency_like, partial_match=True)
            builder.add('quote_currency', params.quote_currency_like, partial_match=True)
            builder.add('country', params.country_like, partial_match=True)
            builder.add('symbol', params.symbol_like, partial_match=True)
            builder.add('name', params.name_like, partial_match=True)
            builder.add('exchange', params.exchange_like, partial_match=True)
            builder.add('primary_id', params.primary_id_like, partial_match=True)
            builder.add('primary_id_source', params.primary_id_source)
            builder.add('matcher_symbol', params.matcher_symbol_like, partial_match=True)
            builder.add('identity_match_type', params.identity_match_type)
            builder.add('asset_class_group', params.asset_class_group)

            # Build queries
            select_columns = """
                id, class_name, class_type, external_id, primary_id, primary_id_source,
                symbol, matcher_symbol, name, exchange, asset_class, base_currency,
                quote_currency, country, identity_conf, identity_match_type,
                identity_updated_at, asset_class_group, sym_norm_full, sym_norm_root
            """

            data_query = f"""
                SELECT {select_columns}
                FROM assets
                WHERE {builder.where_clause}
                ORDER BY {order_by_sql}
                LIMIT ${builder.next_param_idx} OFFSET ${builder.next_param_idx + 1};
            """
            count_query = f"""
                SELECT COUNT(*) as total_items
                FROM assets
                WHERE {builder.where_clause};
            """

            data_params = builder.params + [limit, offset]
            count_params = builder.params

            async with self.pool.acquire() as conn:
                logger.debug(f"Executing data query: {data_query} with params: {data_params}")
                asset_records = await conn.fetch(data_query, *data_params)

                logger.debug(f"Executing count query: {count_query} with params: {count_params}")
                total_items_record = await conn.fetchrow(count_query, *count_params)

            assets_list = [AssetItem(**dict(record)) for record in asset_records]
            total_items = total_items_record['total_items'] if total_items_record else 0

            logger.info(f"Registry.handle_get_assets: Returning {len(assets_list)} assets out of {total_items} total matching criteria.")
            return AssetResponse(
                items=assets_list,
                total_items=total_items,
                limit=limit,
                offset=offset,
                page=(offset // limit) + 1 if limit > 0 else 1,
                total_pages=(total_items + limit - 1) // limit if limit > 0 else 1
            )

        except HTTPException:
            raise
        except ValueError as ve:
            logger.warning(f"Registry.handle_get_assets: Invalid input value: {ve}")
            raise HTTPException(status_code=400, detail=f"Invalid input value: {ve}")
        except Exception as e:
            logger.error(f"Registry.handle_get_assets: Error fetching assets: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching assets")

    async def handle_get_common_symbols(self, params: CommonSymbolQueryParams = Depends()) -> CommonSymbolResponse:
        """Return common symbols with optional filtering, sorting, and pagination.

        Args:
            params (CommonSymbolQueryParams): Query parameters parsed by FastAPI.

        Returns:
            CommonSymbolResponse: Paginated common symbol list and counts.
        """
        logger.info("Registry.handle_get_common_symbols: Received request for common symbols.")

        try:
            # Pagination (already validated by Pydantic)
            limit = params.limit
            offset = params.offset

            # Sorting
            sort_by_str = params.sort_by
            sort_order_str = params.sort_order

            # Map API column names to DB column names
            column_mapping = {'common_symbol': 'symbol', 'provider_count': 'ref_count'}
            valid_sort_columns = list(column_mapping.keys())

            sort_by_cols = [col.strip() for col in sort_by_str.split(',')]
            sort_orders = [order.strip().lower() for order in sort_order_str.split(',')]

            if not all(col in valid_sort_columns for col in sort_by_cols):
                raise HTTPException(status_code=400, detail="Invalid sort_by column")
            if not all(order in ['asc', 'desc'] for order in sort_orders):
                raise HTTPException(status_code=400, detail="Invalid sort_order value")

            if len(sort_orders) == 1 and len(sort_by_cols) > 1:  # Apply single order to all sort columns
                sort_orders = [sort_orders[0]] * len(sort_by_cols)
            elif len(sort_orders) != len(sort_by_cols):
                raise HTTPException(status_code=400, detail="Mismatch between sort_by and sort_order counts")

            order_by_clauses = [f"{column_mapping[col]} {order.upper()}" for col, order in zip(sort_by_cols, sort_orders)]
            order_by_sql = ", ".join(order_by_clauses)

            # Filtering
            builder = FilterBuilder()
            builder.add('symbol', params.common_symbol_like, partial_match=True)

            # Build queries - use common_symbols table directly
            data_query = f"""
                SELECT symbol AS common_symbol, ref_count AS provider_count
                FROM common_symbols
                WHERE {builder.where_clause}
                ORDER BY {order_by_sql}
                LIMIT ${builder.next_param_idx} OFFSET ${builder.next_param_idx + 1};
            """
            count_query = f"""
                SELECT COUNT(*) AS total_items
                FROM common_symbols
                WHERE {builder.where_clause};
            """

            data_params = builder.params + [limit, offset]
            count_params = builder.params

            async with self.pool.acquire() as conn:
                logger.debug(f"Executing data query: {data_query} with params: {data_params}")
                common_symbol_records = await conn.fetch(data_query, *data_params)

                logger.debug(f"Executing count query: {count_query} with params: {count_params}")
                total_items_record = await conn.fetchrow(count_query, *count_params)

            common_symbol_items = [CommonSymbolItem(**dict(record)) for record in common_symbol_records]
            total_items = total_items_record['total_items'] if total_items_record else 0

            logger.info(f"Registry.handle_get_common_symbols: Returning {len(common_symbol_items)} common symbols out of {total_items} total matching criteria.")
            return CommonSymbolResponse(
                items=common_symbol_items,
                total_items=total_items,
                limit=limit,
                offset=offset,
                page=(offset // limit) + 1 if limit > 0 else 1,
                total_pages=(total_items + limit - 1) // limit if limit > 0 else 1
            )

        except HTTPException:
            raise
        except ValueError as ve:
            logger.warning(f"Registry.handle_get_common_symbols: Invalid input value: {ve}")
            raise HTTPException(status_code=400, detail=f"Invalid input value: {ve}")
        except Exception as e:
            logger.error(f"Registry.handle_get_common_symbols: Error fetching common symbols: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching common symbols")
