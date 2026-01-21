"""Asset mapping handlers for Registry service."""

from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import unquote_plus
import asyncpg

from fastapi import HTTPException, Depends, Query, Body
from fastapi.responses import Response
from asyncpg.exceptions import UndefinedFunctionError

from quasar.services.registry.handlers.base import HandlerMixin
from quasar.services.registry.utils import encode_cursor, decode_cursor, FilterBuilder
from quasar.services.registry.schemas import (
    ClassType,
    AssetMappingCreate, AssetMappingCreateRequest, AssetMappingCreateResponse,
    AssetMappingResponse, AssetMappingUpdate, AssetMappingQueryParams, AssetMappingPaginatedResponse,
    SuggestionsResponse, SuggestionItem,
    CommonSymbolRenameRequest, CommonSymbolRenameResponse,
)
from quasar.services.registry.mapper import MappingCandidate

import logging
logger = logging.getLogger(__name__)


class MappingHandlersMixin(HandlerMixin):
    """Asset mapping API handlers."""

    async def _apply_automated_mappings(
        self,
        candidates: List[MappingCandidate]
    ) -> Dict[str, int]:
        """Bulk insert mapping candidates using prepared statements and savepoints.

        Args:
            candidates: List of MappingCandidate objects to insert

        Returns:
            Dict with 'created', 'skipped', 'failed' counts
        """
        stats = {'created': 0, 'skipped': 0, 'failed': 0}

        if not candidates:
            return stats

        mapping_insert_query = """
            INSERT INTO asset_mapping (common_symbol, class_name, class_type, class_symbol, is_active)
            VALUES ($1, $2, $3, $4, true)
            ON CONFLICT (class_name, class_type, class_symbol) DO NOTHING
            RETURNING common_symbol;
        """

        async with self.pool.acquire() as conn:
            prepared_insert = await conn.prepare(mapping_insert_query)
            savepoint_counter = 0
            async with conn.transaction():
                async def _exec_savepoint(cmd: str) -> None:
                    """Execute savepoint commands without aborting transaction."""
                    try:
                        await conn.execute(cmd)
                    except Exception as exc:
                        logger.warning(f"Registry._apply_automated_mappings: Savepoint command '{cmd}' failed: {exc}", exc_info=True)

                for candidate in candidates:
                    savepoint_name = f"mapping_insert_{savepoint_counter}"
                    savepoint_counter += 1
                    await _exec_savepoint(f"SAVEPOINT {savepoint_name}")

                    try:
                        result = await prepared_insert.fetchrow(
                            candidate.common_symbol,
                            candidate.class_name,
                            candidate.class_type,
                            candidate.class_symbol
                        )
                        if result:
                            stats['created'] += 1
                        else:
                            # ON CONFLICT DO NOTHING - mapping already exists
                            stats['skipped'] += 1
                        await _exec_savepoint(f"RELEASE SAVEPOINT {savepoint_name}")
                    except Exception as e:
                        logger.error(f"Registry._apply_automated_mappings: Error inserting mapping for {candidate.class_symbol}: {e}", exc_info=True)
                        stats['failed'] += 1
                        await _exec_savepoint(f"ROLLBACK TO SAVEPOINT {savepoint_name}")

        return stats

    def _build_remap_filter_query(
        self,
        class_name: Optional[str] = None,
        class_type: Optional[str] = None,
        asset_class: Optional[str] = None,
        for_delete: bool = False
    ) -> Tuple[str, List[Any]]:
        """Build SQL query to select asset_mapping rows with optional filters.

        Constructs a query that can filter mappings by provider (class_name/class_type)
        and/or asset_class. When asset_class is specified, the query JOINs to the assets
        table to filter by the asset's asset_class.

        Args:
            class_name: Provider/broker name to filter by.
            class_type: Required when class_name is specified ('provider' or 'broker').
            asset_class: Asset class to filter by (e.g., 'crypto', 'us_equity').
            for_delete: If True, returns a DELETE query with RETURNING clause.
                       If False, returns a SELECT query for preview/counting.

        Returns:
            Tuple of (sql_query, params_list) ready for execution.
        """
        params: List[Any] = []
        param_idx = 1
        where_clauses: List[str] = []

        # Provider filter
        if class_name:
            where_clauses.append(f"am.class_name = ${param_idx}")
            params.append(class_name)
            param_idx += 1
            if class_type:
                where_clauses.append(f"am.class_type = ${param_idx}")
                params.append(class_type)
                param_idx += 1

        # Asset class filter requires JOIN to assets table
        needs_join = asset_class is not None
        if asset_class:
            where_clauses.append(f"a.asset_class = ${param_idx}")
            params.append(asset_class)
            param_idx += 1

        # Build WHERE clause (default to TRUE if no filters)
        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        # Build FROM clause with optional JOIN
        if needs_join:
            from_sql = """
                asset_mapping am
                JOIN assets a ON am.class_name = a.class_name
                              AND am.class_type = a.class_type
                              AND am.class_symbol = a.symbol
            """
        else:
            from_sql = "asset_mapping am"

        if for_delete:
            # DELETE query - need to use subquery since DELETE doesn't support JOIN directly
            if needs_join:
                query = f"""
                    DELETE FROM asset_mapping
                    WHERE (class_name, class_type, class_symbol) IN (
                        SELECT am.class_name, am.class_type, am.class_symbol
                        FROM {from_sql}
                        WHERE {where_sql}
                    )
                    RETURNING common_symbol, class_name, class_type, class_symbol
                """
            else:
                query = f"""
                    DELETE FROM asset_mapping am
                    WHERE {where_sql}
                    RETURNING common_symbol, class_name, class_type, class_symbol
                """
        else:
            # SELECT query for preview/counting
            query = f"""
                SELECT am.common_symbol, am.class_name, am.class_type, am.class_symbol
                FROM {from_sql}
                WHERE {where_sql}
            """

        return query, params

    def _get_affected_indices_query(
        self,
        class_name: Optional[str] = None,
        class_type: Optional[str] = None,
        asset_class: Optional[str] = None
    ) -> Tuple[str, List[Any]]:
        """Build SQL query to find user indices affected by a re-map operation.

        A user index is "affected" if it has memberships referencing common_symbols
        that would lose all their asset_mapping references when the filtered
        mappings are deleted. This happens when:
        1. The common_symbol is referenced by mappings matching the filter
        2. The common_symbol has NO other mappings outside the filter
        3. When ref_count reaches 0, common_symbol is deleted, CASCADE deletes memberships

        Args:
            class_name: Provider/broker name to filter by.
            class_type: Required when class_name is specified ('provider' or 'broker').
            asset_class: Asset class to filter by (e.g., 'crypto', 'us_equity').

        Returns:
            Tuple of (sql_query, params_list) that returns DISTINCT index_class_name values.
        """
        # First, get the query to identify mappings that match the filter
        filter_query, params = self._build_remap_filter_query(
            class_name=class_name,
            class_type=class_type,
            asset_class=asset_class,
            for_delete=False
        )

        # Find common_symbols that will become orphaned:
        # - They are used by mappings matching the filter
        # - They have NO mappings outside the filter (so ref_count will reach 0)
        #
        # An index is affected if it has memberships referencing these orphaned symbols.
        query = f"""
            WITH filtered_mappings AS (
                {filter_query}
            ),
            -- Common symbols used by filtered mappings
            filtered_symbols AS (
                SELECT DISTINCT common_symbol FROM filtered_mappings
            ),
            -- Common symbols that will be orphaned (no mappings remain after deletion)
            orphaned_symbols AS (
                SELECT fs.common_symbol
                FROM filtered_symbols fs
                WHERE NOT EXISTS (
                    -- Check if any mapping for this symbol is NOT in the filtered set
                    SELECT 1 FROM asset_mapping am
                    WHERE am.common_symbol = fs.common_symbol
                    AND NOT EXISTS (
                        SELECT 1 FROM filtered_mappings fm
                        WHERE fm.class_name = am.class_name
                        AND fm.class_type = am.class_type
                        AND fm.class_symbol = am.class_symbol
                    )
                )
            )
            -- Find distinct index names that have current memberships referencing orphaned symbols
            SELECT DISTINCT im.index_class_name
            FROM index_memberships im
            JOIN orphaned_symbols os ON im.common_symbol = os.common_symbol
            WHERE im.valid_to IS NULL
        """

        return query, params

    async def handle_create_asset_mapping(
        self,
        mapping: AssetMappingCreateRequest
    ) -> AssetMappingCreateResponse:
        """Create one or more asset mappings.

        Args:
            mapping: A single mapping or a list of mappings to create.

        Returns:
            List[AssetMappingResponse]: The created mappings (always returned as a list).

        Raises:
            HTTPException: 400 if no mappings are provided.
            HTTPException: 404 if related entities are missing (foreign key violations).
            HTTPException: 409 if unique constraints are violated (duplicate mapping).
            HTTPException: 500 for unexpected errors.
        """
        insert_query = """
            INSERT INTO asset_mapping (common_symbol, class_name, class_type, class_symbol, is_active)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING common_symbol, class_name, class_type, class_symbol, is_active;
        """

        # Normalize payload to a list to support single or batch requests.
        mappings: List[AssetMappingCreate] = [mapping] if not isinstance(mapping, list) else list(mapping)

        if len(mappings) == 0:
            raise HTTPException(status_code=400, detail="At least one mapping is required.")

        current_item: AssetMappingCreate | None = None
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    created_records: List[AssetMappingResponse] = []
                    for current_item in mappings:
                        new_mapping = await conn.fetchrow(
                            insert_query,
                            current_item.common_symbol,
                            current_item.class_name,
                            current_item.class_type,
                            current_item.class_symbol,
                            current_item.is_active
                        )
                        if not new_mapping:
                            logger.error("Registry.handle_create_asset_mapping: Failed to create asset mapping, no record returned.")
                            raise HTTPException(status_code=500, detail="Failed to create asset mapping")

                        created_records.append(AssetMappingResponse(**dict(new_mapping)))

            logger.info(
                "Registry.handle_create_asset_mapping: Successfully created %s mapping(s).",
                len(mappings)
            )

            return created_records

        except HTTPException:
            # HTTPExceptions are intentional; they will propagate with transaction rolled back.
            raise
        except asyncpg.exceptions.ForeignKeyViolationError as fke:
            constraint_name = fke.constraint_name
            detail = fke.detail
            logger.warning(
                "Registry.handle_create_asset_mapping: Foreign key violation. Constraint: %s, Detail: %s.",
                constraint_name,
                detail
            )
            item = current_item or mappings[0]
            error_message = "Failed to create mapping due to missing related entity. "
            if constraint_name == 'fk_asset_mapping_class_name':
                error_message += f"The class '{item.class_name}' ({item.class_type}) is not registered."
            elif constraint_name == 'fk_asset_mapping_to_assets':
                error_message += f"The asset '{item.class_symbol}' for class '{item.class_name}' ({item.class_type}) does not exist."
            else:
                error_message += "A referenced entity does not exist."

            raise HTTPException(status_code=404, detail=error_message)
        except asyncpg.exceptions.UniqueViolationError as uve:
            constraint_name = uve.constraint_name
            detail = uve.detail
            logger.warning(
                "Registry.handle_create_asset_mapping: Unique constraint violation. Constraint: %s, Detail: %s.",
                constraint_name,
                detail
            )
            item = current_item or mappings[0]
            error_message = "Failed to create mapping due to a conflict. "
            if constraint_name == 'asset_mapping_pkey':
                error_message += f"The provider symbol '{item.class_symbol}' for class '{item.class_name}' ({item.class_type}) is already mapped."
            elif constraint_name == 'uq_common_per_class':
                error_message += f"The common symbol '{item.common_symbol}' is already mapped for class '{item.class_name}' ({item.class_type})."
            else:
                error_message += "This mapping would create a duplicate entry."

            raise HTTPException(status_code=409, detail=error_message)
        except Exception as e:
            logger.error(
                "Registry.handle_create_asset_mapping: Unexpected error creating asset mapping: %s",
                e,
                exc_info=True
            )
            raise HTTPException(status_code=500, detail="An unexpected error occurred")

    async def handle_get_asset_mappings(
        self,
        params: AssetMappingQueryParams = Depends()
    ) -> AssetMappingPaginatedResponse:
        """Return asset mappings with optional filtering, sorting, and pagination.

        Args:
            params (AssetMappingQueryParams): Query parameters parsed by FastAPI.

        Returns:
            AssetMappingPaginatedResponse: Paginated asset mapping list and counts.
        """
        logger.info("Registry.handle_get_asset_mappings: Received request for asset mappings.")

        try:
            # Pagination (already validated by Pydantic)
            limit = params.limit
            offset = params.offset

            # Sorting
            sort_by_str = params.sort_by
            sort_order_str = params.sort_order

            valid_sort_columns = ['common_symbol', 'class_name', 'class_type', 'class_symbol', 'is_active']

            sort_by_cols = [col.strip() for col in sort_by_str.split(',')]
            sort_orders = [order.strip().lower() for order in sort_order_str.split(',')]

            if not all(col in valid_sort_columns for col in sort_by_cols):
                raise HTTPException(status_code=400, detail="Invalid sort_by column")
            if not all(order in ['asc', 'desc'] for order in sort_orders):
                raise HTTPException(status_code=400, detail="Invalid sort_order value")

            if len(sort_orders) == 1 and len(sort_by_cols) > 1: # Apply single order to all sort columns
                sort_orders = [sort_orders[0]] * len(sort_by_cols)
            elif len(sort_orders) != len(sort_by_cols):
                raise HTTPException(status_code=400, detail="Mismatch between sort_by and sort_order counts")

            order_by_clauses = [f"{col} {order.upper()}" for col, order in zip(sort_by_cols, sort_orders)]
            order_by_sql = ", ".join(order_by_clauses)

            # Filtering
            builder = FilterBuilder()
            builder.add('common_symbol', params.common_symbol)
            builder.add('common_symbol', params.common_symbol_like, partial_match=True)
            builder.add('class_name', params.class_name)
            builder.add('class_name', params.class_name_like, partial_match=True)
            builder.add('class_type', params.class_type)
            builder.add('class_symbol', params.class_symbol)
            builder.add('class_symbol', params.class_symbol_like, partial_match=True)
            builder.add('is_active', params.is_active)

            # Build queries
            select_columns = "common_symbol, class_name, class_type, class_symbol, is_active"

            data_query = f"""
                SELECT {select_columns}
                FROM asset_mapping
                WHERE {builder.where_clause}
                ORDER BY {order_by_sql}
                LIMIT ${builder.next_param_idx} OFFSET ${builder.next_param_idx + 1};
            """
            count_query = f"""
                SELECT COUNT(*) as total_items
                FROM asset_mapping
                WHERE {builder.where_clause};
            """

            data_params = builder.params + [limit, offset]
            count_params = builder.params

            async with self.pool.acquire() as conn:
                logger.debug(f"Executing data query: {data_query} with params: {data_params}")
                mapping_records = await conn.fetch(data_query, *data_params)

                logger.debug(f"Executing count query: {count_query} with params: {count_params}")
                total_items_record = await conn.fetchrow(count_query, *count_params)

            mappings_list = [AssetMappingResponse(**dict(record)) for record in mapping_records]
            total_items = total_items_record['total_items'] if total_items_record else 0

            logger.info(f"Registry.handle_get_asset_mappings: Returning {len(mappings_list)} asset mappings out of {total_items} total matching criteria.")
            return AssetMappingPaginatedResponse(
                items=mappings_list,
                total_items=total_items,
                limit=limit,
                offset=offset,
                page=(offset // limit) + 1 if limit > 0 else 1,
                total_pages=(total_items + limit - 1) // limit if limit > 0 else 1
            )

        except HTTPException:
            raise
        except ValueError as ve:
            logger.warning(f"Registry.handle_get_asset_mappings: Invalid input value: {ve}")
            raise HTTPException(status_code=400, detail=f"Invalid input value: {ve}")
        except Exception as e:
            logger.error(f"Registry.handle_get_asset_mappings: Error fetching asset mappings: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching asset mappings")

    async def handle_get_asset_mappings_for_symbol(
        self,
        common_symbol: str
    ) -> List[AssetMappingResponse]:
        """Get all asset mappings for a specific common symbol, including asset details.

        Args:
            common_symbol (str): The common symbol to filter by.

        Returns:
            List[AssetMappingResponse]: All mappings for the specified common symbol with asset details.
        """
        logger.info(f"Registry.handle_get_asset_mappings_for_symbol: Received request for asset mappings with common_symbol='{common_symbol}'.")

        query = """
            SELECT
                am.common_symbol,
                am.class_name,
                am.class_type,
                am.class_symbol,
                am.is_active,
                a.primary_id,
                a.asset_class
            FROM asset_mapping am
            LEFT JOIN assets a ON am.class_name = a.class_name
                               AND am.class_type = a.class_type
                               AND am.class_symbol = a.symbol
            WHERE am.common_symbol = $1
            ORDER BY am.class_name, am.class_type, am.class_symbol
        """

        try:
            mappings_records = await self.pool.fetch(query, common_symbol)

            # Convert records to dict and handle potential None values
            mappings_list = []
            for record in mappings_records:
                record_dict = dict(record)
                # Ensure asset_class is properly handled (it might be None)
                if record_dict.get('asset_class') is None:
                    record_dict['asset_class'] = None
                mappings_list.append(AssetMappingResponse(**record_dict))

            logger.info(f"Registry.handle_get_asset_mappings_for_symbol: Returning {len(mappings_list)} asset mappings for common_symbol='{common_symbol}'.")
            return mappings_list

        except Exception as e:
            logger.error(f"Registry.handle_get_asset_mappings_for_symbol: Error fetching asset mappings for common_symbol='{common_symbol}': {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching asset mappings")

    async def handle_get_asset_mapping_suggestions(
        self,
        source_class: str = Query(..., description="Provider/broker to suggest mappings for"),
        source_type: Optional[ClassType] = Query(None, description="Optional source class type"),
        target_class: Optional[str] = Query(None, description="Optional target provider/broker to match against"),
        target_type: Optional[ClassType] = Query(None, description="Optional target class type (defaults to provider if omitted)"),
        search: Optional[str] = Query(None, description="Optional search filter across source/target symbols and names"),
        min_score: float = Query(30.0, description="Minimum score threshold for suggestions"),
        limit: int = Query(50, ge=1, le=200, description="Max results to return"),
        offset: int = Query(0, ge=0, description="Deprecated: use cursor for pagination"),
        cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
        include_total: bool = Query(False, description="Include total count (slower)")
    ) -> SuggestionsResponse:
        """Return suggested asset mappings using optimized DB-side scoring.

        This endpoint uses UNION ALL queries for efficient index utilization and
        cursor-based pagination for consistent, fast paging through results.

        The query:
        - Excludes symbols already mapped.
        - Reuses an existing common_symbol from the target if present.
        - Matches only within the same asset_class (or both NULL).
        - Uses pg_trgm similarity if available; falls back if not installed.

        Args:
            source_class (str): Provider/broker to suggest mappings for.
            source_type (ClassType | None): Optional source class type filter.
            target_class (str | None): Optional target provider/broker to match against.
            target_type (ClassType | None): Optional target class type (defaults to provider if omitted).
            search (str | None): Optional search filter across source/target symbols and names.
            min_score (float): Minimum score threshold for suggestions (default: 30.0).
            limit (int): Max results to return (1-200, default: 50).
            offset (int): Deprecated - use cursor for pagination instead.
            cursor (str | None): Pagination cursor from previous response.
            include_total (bool): Include total count in response (adds latency, default: False).

        Returns:
            SuggestionsResponse: Paginated list of suggested mappings with match scores and criteria.

        Raises:
            HTTPException: 400 if cursor format is invalid.
            HTTPException: 500 if database error occurs.
        """
        logger.info(
            "Registry.handle_get_asset_mapping_suggestions: source=%s, target=%s, min_score=%s, limit=%s, cursor=%s",
            source_class, target_class, min_score, limit, cursor[:20] + "..." if cursor else None
        )

        # Decode cursor if provided
        cursor_score: Optional[float] = None
        cursor_src_sym: Optional[str] = None
        cursor_tgt_sym: Optional[str] = None
        if cursor:
            try:
                cursor_score, cursor_src_sym, cursor_tgt_sym = decode_cursor(cursor)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        def build_sql(use_similarity: bool, for_count: bool = False) -> tuple[str, list]:
            """Build the SQL query for suggestions.

            Uses UNION ALL to enable index usage on each join condition separately,
            then deduplicates with DISTINCT ON.
            """
            params: list = []
            param_idx = 1

            # Source filter
            src_filters = [f"a.class_name = ${param_idx}"]
            params.append(source_class)
            param_idx += 1
            if source_type:
                src_filters.append(f"a.class_type = ${param_idx}")
                params.append(source_type)
                param_idx += 1

            # Target filter
            tgt_filters = [f"a.class_name <> $1"]  # reuse source_class param
            if target_class:
                tgt_filters.append(f"a.class_name = ${param_idx}")
                params.append(target_class)
                param_idx += 1
            if target_type:
                tgt_filters.append(f"a.class_type = ${param_idx}")
                params.append(target_type)
                param_idx += 1

            # Search clause (applied at the end)
            search_param_idx = None
            if search:
                search_param_idx = param_idx
                params.append(f"%{search}%")
                param_idx += 1

            # Similarity expressions for use in deduplicated CTE (using column names, not table aliases)
            # These use the aliased column names from the matched CTE output
            name_sim_col = "COALESCE(similarity(source_name, target_name), 0)" if use_similarity else "0"
            sym_sim_col = "COALESCE(similarity(s_sym_root, t_sym_root), 0)" if use_similarity else "0"
            sym_sim_expr = "COALESCE(similarity(s_sym_root, t_sym_root) * 15, 0)" if use_similarity else "0"
            name_sim_expr = "COALESCE(similarity(source_name, target_name) * 10, 0)" if use_similarity else "0"

            # Score expression for use in deduplicated CTE (using column names from matched output)
            score_expr = f"""(
                CASE WHEN t_primary_id IS NOT NULL AND s_primary_id = t_primary_id THEN 70 ELSE 0 END +
                CASE WHEN t_ext_id IS NOT NULL AND s_ext_id = t_ext_id THEN 50 ELSE 0 END +
                CASE WHEN (s_sym_full = t_sym_full OR s_sym_root = t_sym_root) THEN 30 ELSE 0 END +
                CASE WHEN s_base = t_base AND s_quote = t_quote THEN 10 ELSE 0 END +
                CASE WHEN s_exchange = t_exchange THEN 5 ELSE 0 END +
                {sym_sim_expr} +
                {name_sim_expr}
            )"""

            asset_class_clause = "(s.asset_class = t.asset_class OR (s.asset_class IS NULL AND t.asset_class IS NULL))"

            # Unmapped subquery - reused for src and tgt
            unmapped_filter = """
                NOT EXISTS (
                    SELECT 1 FROM asset_mapping m
                    WHERE m.class_name = a.class_name
                      AND m.class_type = a.class_type
                      AND m.class_symbol = a.symbol
                )
            """

            # Build UNION ALL query for indexed joins
            # Each branch joins on a single indexed condition
            select_cols = f"""
                s.class_name AS source_class,
                s.class_type AS source_type,
                s.symbol AS source_symbol,
                s.name AS source_name,
                t.class_name AS target_class,
                t.class_type AS target_type,
                t.symbol AS target_symbol,
                t.name AS target_name,
                s.sym_norm_root,
                s.primary_id AS s_primary_id, t.primary_id AS t_primary_id,
                s.external_id AS s_ext_id, t.external_id AS t_ext_id,
                s.sym_norm_full AS s_sym_full, t.sym_norm_full AS t_sym_full,
                s.sym_norm_root AS s_sym_root, t.sym_norm_root AS t_sym_root,
                s.base_currency AS s_base, t.base_currency AS t_base,
                s.quote_currency AS s_quote, t.quote_currency AS t_quote,
                s.exchange AS s_exchange, t.exchange AS t_exchange
            """

            src_cte = f"""
                SELECT a.* FROM assets a
                WHERE {' AND '.join(src_filters)}
                  AND {unmapped_filter}
            """
            tgt_cte = f"""
                SELECT a.* FROM assets a
                WHERE {' AND '.join(tgt_filters)}
            """

            union_query = f"""
                WITH src AS ({src_cte}),
                     tgt AS ({tgt_cte}),
                matched AS (
                    -- Primary ID matches (indexed)
                    SELECT {select_cols}
                    FROM src s JOIN tgt t ON s.primary_id = t.primary_id
                    WHERE s.primary_id IS NOT NULL AND {asset_class_clause}

                    UNION ALL

                    -- External ID matches (indexed)
                    SELECT {select_cols}
                    FROM src s JOIN tgt t ON s.external_id = t.external_id
                    WHERE s.external_id IS NOT NULL AND {asset_class_clause}

                    UNION ALL

                    -- Symbol root matches (indexed)
                    SELECT {select_cols}
                    FROM src s JOIN tgt t ON s.sym_norm_root = t.sym_norm_root
                    WHERE {asset_class_clause}

                    UNION ALL

                    -- Symbol full matches (indexed, catches cases where root differs)
                    SELECT {select_cols}
                    FROM src s JOIN tgt t ON s.sym_norm_full = t.sym_norm_full
                    WHERE s.sym_norm_full <> s.sym_norm_root AND {asset_class_clause}
                ),
                deduplicated AS (
                    SELECT DISTINCT ON (source_symbol, target_symbol)
                        source_class, source_type, source_symbol, source_name,
                        target_class, target_type, target_symbol, target_name,
                        sym_norm_root,
                        COALESCE(t_primary_id IS NOT NULL AND s_primary_id = t_primary_id, FALSE) AS id_match,
                        COALESCE(t_ext_id IS NOT NULL AND s_ext_id = t_ext_id, FALSE) AS external_id_match,
                        COALESCE(s_sym_full = t_sym_full OR s_sym_root = t_sym_root, FALSE) AS norm_match,
                        COALESCE(s_base = t_base AND s_quote = t_quote, FALSE) AS base_quote_match,
                        COALESCE(s_exchange = t_exchange, FALSE) AS exchange_match,
                        {sym_sim_col} AS sym_root_similarity,
                        {name_sim_col} AS name_similarity,
                        {score_expr} AS score
                    FROM matched
                    ORDER BY source_symbol, target_symbol, {score_expr} DESC
                ),
                scored AS (
                    SELECT d.*,
                           tm.common_symbol AS target_common_symbol,
                           COALESCE(tm.common_symbol, UPPER(d.sym_norm_root)) AS proposed_common_symbol,
                           (tm.common_symbol IS NOT NULL) AS target_already_mapped
                    FROM deduplicated d
                    LEFT JOIN asset_mapping tm
                      ON tm.class_name = d.target_class
                     AND tm.class_type = d.target_type
                     AND tm.class_symbol = d.target_symbol
                    WHERE d.score >= ${param_idx}
                )
            """
            params.append(min_score)
            param_idx += 1

            # Add search filter if provided
            search_filter = ""
            if search_param_idx:
                search_filter = f"""
                    AND (source_symbol ILIKE ${search_param_idx}
                         OR source_name ILIKE ${search_param_idx}
                         OR target_symbol ILIKE ${search_param_idx}
                         OR target_name ILIKE ${search_param_idx})
                """

            if for_count:
                # Count query - just count the scored results
                query = f"""
                    {union_query}
                    SELECT COUNT(*) AS total FROM scored
                    WHERE TRUE {search_filter};
                """
            else:
                # Data query with cursor-based pagination
                cursor_filter = ""
                if cursor_score is not None:
                    cursor_filter = f"""
                        AND (
                            score < ${param_idx}
                            OR (score = ${param_idx} AND source_symbol > ${param_idx + 1})
                            OR (score = ${param_idx} AND source_symbol = ${param_idx + 1} AND target_symbol > ${param_idx + 2})
                        )
                    """
                    params.extend([cursor_score, cursor_src_sym, cursor_tgt_sym])
                    param_idx += 3
                elif offset > 0:
                    # Fallback to offset if no cursor but offset provided (backwards compat)
                    cursor_filter = f" OFFSET {offset}"

                query = f"""
                    {union_query}
                    SELECT
                        source_class, source_type, source_symbol, source_name,
                        target_class, target_type, target_symbol, target_name,
                        target_common_symbol, proposed_common_symbol, score,
                        id_match, external_id_match, norm_match,
                        base_quote_match, exchange_match,
                        sym_root_similarity, name_similarity,
                        target_already_mapped
                    FROM scored
                    WHERE TRUE {search_filter} {cursor_filter if cursor_score is not None else ''}
                    ORDER BY score DESC, source_symbol ASC, target_symbol ASC
                    LIMIT ${param_idx}{'' if cursor_score is not None else f' OFFSET {offset}' if offset > 0 else ''};
                """
                params.append(limit + 1)  # Fetch one extra to check has_more

            return query, params

        try:
            query, params = build_sql(use_similarity=True)
            records = await self.pool.fetch(query, *params)
        except UndefinedFunctionError:
            logger.warning("Registry.handle_get_asset_mapping_suggestions: similarity() unavailable, retrying without pg_trgm.")
            try:
                query, params = build_sql(use_similarity=False)
                records = await self.pool.fetch(query, *params)
            except Exception as e:
                logger.error(
                    f"Registry.handle_get_asset_mapping_suggestions: Error fetching suggestions (fallback without pg_trgm): {e}",
                    exc_info=True
                )
                raise HTTPException(status_code=500, detail="Database error while fetching asset mapping suggestions")
        except Exception as e:
            logger.error(f"Registry.handle_get_asset_mapping_suggestions: Error fetching suggestions: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching asset mapping suggestions")

        # Determine if there are more results
        has_more = len(records) > limit
        if has_more:
            records = records[:limit]

        # Build items
        items: List[SuggestionItem] = []
        for record in records:
            proposed_common_symbol = record["proposed_common_symbol"]
            target_common_symbol = record.get("target_common_symbol")
            if not record["target_already_mapped"] and proposed_common_symbol:
                proposed_common_symbol = proposed_common_symbol.upper()

            items.append(SuggestionItem(
                source_class=record["source_class"],
                source_type=record["source_type"],
                source_symbol=record["source_symbol"],
                source_name=record["source_name"],
                target_class=record["target_class"],
                target_type=record["target_type"],
                target_symbol=record["target_symbol"],
                target_name=record["target_name"],
                target_common_symbol=target_common_symbol,
                proposed_common_symbol=proposed_common_symbol,
                score=float(record["score"]),
                id_match=record["id_match"],
                external_id_match=record["external_id_match"],
                norm_match=record["norm_match"],
                base_quote_match=record["base_quote_match"],
                exchange_match=record["exchange_match"],
                sym_root_similarity=float(record["sym_root_similarity"]) if record["sym_root_similarity"] else 0.0,
                name_similarity=float(record["name_similarity"]) if record["name_similarity"] else 0.0,
                target_already_mapped=record["target_already_mapped"]
            ))

        # Generate next cursor from last item
        next_cursor: Optional[str] = None
        if has_more and items:
            last = items[-1]
            next_cursor = encode_cursor(last.score, last.source_symbol, last.target_symbol)

        # Fetch total count only if requested
        total: Optional[int] = None
        if include_total:
            try:
                count_query, count_params = build_sql(use_similarity=True, for_count=True)
                count_result = await self.pool.fetchval(count_query, *count_params)
                total = count_result or 0
            except UndefinedFunctionError:
                try:
                    count_query, count_params = build_sql(use_similarity=False, for_count=True)
                    count_result = await self.pool.fetchval(count_query, *count_params)
                    total = count_result or 0
                except Exception as e:
                    logger.warning(
                        f"Registry.handle_get_asset_mapping_suggestions: Error fetching count (fallback without pg_trgm): {e}"
                    )
                    total = None
            except Exception as e:
                logger.warning(f"Registry.handle_get_asset_mapping_suggestions: Error fetching count: {e}")
                total = None

        logger.info(
            "Registry.handle_get_asset_mapping_suggestions: Returning %s suggestions (has_more=%s, total=%s).",
            len(items), has_more, total
        )
        return SuggestionsResponse(
            items=items,
            total=total,
            limit=limit,
            offset=offset,
            next_cursor=next_cursor,
            has_more=has_more
        )

    async def handle_update_asset_mapping(
        self,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'"),
        class_symbol: str = Query(..., description="Class-specific symbol"),
        update: AssetMappingUpdate = Body(...)
    ) -> AssetMappingResponse:
        """Update an existing asset mapping.

        Args:
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.
            class_symbol (str): Provider-specific symbol.
            update (AssetMappingUpdate): Fields to modify.

        Returns:
            AssetMappingResponse: Updated mapping.
        """
        logger.info(f"Registry.handle_update_asset_mapping: Received PUT request for "
                    f"{class_type}/{class_name}/{class_symbol}")

        # Fields to update
        updates = {}
        if update.common_symbol is not None:
            if not update.common_symbol.strip():
                raise HTTPException(status_code=400, detail="common_symbol must be a non-empty string")
            updates['common_symbol'] = update.common_symbol.strip()

        if update.is_active is not None:
            updates['is_active'] = update.is_active

        if not updates:
            raise HTTPException(status_code=400, detail="No fields provided for update. Provide 'common_symbol' or 'is_active'.")

        # Build the SET part of the query
        set_clauses = []
        params = []
        param_idx = 1
        for key, value in updates.items():
            set_clauses.append(f"{key} = ${param_idx}")
            params.append(value)
            param_idx += 1

        # Add WHERE clause parameters
        params.extend([class_name, class_type, class_symbol])

        update_query = f"""
            UPDATE asset_mapping
            SET {', '.join(set_clauses)}
            WHERE class_name = ${param_idx} AND class_type = ${param_idx + 1} AND class_symbol = ${param_idx + 2}
            RETURNING common_symbol, class_name, class_type, class_symbol, is_active;
        """

        try:
            updated_mapping = await self.pool.fetchrow(update_query, *params)
            if updated_mapping:
                logger.info(f"Registry.handle_update_asset_mapping: Successfully updated asset mapping: {dict(updated_mapping)}")
                return AssetMappingResponse(**dict(updated_mapping))
            else:
                # This means the WHERE clause didn't match any rows
                logger.warning(
                    f"Registry.handle_update_asset_mapping: Asset mapping not found for "
                    f"{class_name}/{class_type}/{class_symbol}."
                )
                raise HTTPException(status_code=404, detail="Asset mapping not found")
        except HTTPException:
            raise
        except asyncpg.exceptions.UniqueViolationError as uve:
            # This typically happens if updating common_symbol violates uq_common_per_class
            constraint_name = uve.constraint_name
            detail = uve.detail
            logger.warning(
                f"Registry.handle_update_asset_mapping: Unique constraint violation. "
                f"Constraint: {constraint_name}, Detail: {detail}."
            )
            error_message = "Failed to update mapping due to a conflict. "
            if constraint_name == 'uq_common_per_class':
                 error_message += (f"The common symbol '{updates.get('common_symbol')}' is already mapped "
                                   f"for class '{class_name}' ({class_type}).")
            else:
                error_message += "This update would create a duplicate entry."
            raise HTTPException(status_code=409, detail=error_message)
        except Exception as e:
            logger.error(f"Registry.handle_update_asset_mapping: Unexpected error updating asset mapping: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected error occurred")

    async def handle_delete_asset_mapping(
        self,
        class_name: str = Query(..., description="Class name (provider/broker name)"),
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'"),
        class_symbol: str = Query(..., description="Class-specific symbol")
    ) -> Response:
        """Delete an asset mapping identified by provider/broker and symbol.

        Args:
            class_name (str): Provider/broker name.
            class_type (ClassType): Provider or broker.
            class_symbol (str): Provider-specific symbol.

        Returns:
            Response: Empty 204 response on success.
        """
        logger.info(
            f"Registry.handle_delete_asset_mapping: Received DELETE request for "
            f"{class_name}/{class_type}/{class_symbol}"
        )

        delete_query = """
            DELETE FROM asset_mapping
            WHERE class_name = $1 AND class_type = $2 AND class_symbol = $3
            RETURNING common_symbol;
        """
        try:
            deleted_record = await self.pool.fetchval(
                delete_query,
                class_name,
                class_type,
                class_symbol
            )
            if deleted_record is not None:
                logger.info(
                    f"Registry.handle_delete_asset_mapping: Successfully deleted asset mapping for "
                    f"{class_name}/{class_type}/{class_symbol} (was common_symbol: {deleted_record})."
                )
                return Response(status_code=204)  # 204 No Content for successful deletion
            else:
                # This means the WHERE clause didn't match any rows
                logger.warning(
                    f"Registry.handle_delete_asset_mapping: Asset mapping not found for deletion: "
                    f"{class_name}/{class_type}/{class_symbol}."
                )
                raise HTTPException(status_code=404, detail="Asset mapping not found")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_delete_asset_mapping: Unexpected error deleting asset mapping: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected error occurred")

    async def handle_rename_common_symbol(
        self,
        symbol: str,
        request: CommonSymbolRenameRequest = Body(...)
    ) -> CommonSymbolRenameResponse:
        """Rename a common symbol, cascading to asset_mapping and index_memberships.

        The rename is performed as a single UPDATE on common_symbols.symbol,
        which cascades to dependent tables via ON UPDATE CASCADE foreign keys.

        Args:
            symbol: Current symbol name (path parameter).
            request: Contains the new_symbol value.

        Returns:
            CommonSymbolRenameResponse: Summary of the rename operation.

        Raises:
            HTTPException: 400 if new_symbol is empty or same as old.
            HTTPException: 404 if the symbol does not exist.
            HTTPException: 409 if new_symbol already exists.
            HTTPException: 500 for unexpected errors.
        """
        old_symbol = symbol
        new_symbol = request.new_symbol.strip()

        logger.info(
            "Registry.handle_rename_common_symbol: Renaming '%s' to '%s'",
            old_symbol, new_symbol
        )

        # Validation: new_symbol must not be empty
        if not new_symbol:
            raise HTTPException(
                status_code=400,
                detail="new_symbol must be a non-empty string"
            )

        # Validation: new_symbol must differ from old_symbol
        if new_symbol == old_symbol:
            raise HTTPException(
                status_code=400,
                detail="new_symbol must be different from the current symbol"
            )

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Check if old_symbol exists
                    exists_check = await conn.fetchval(
                        "SELECT 1 FROM common_symbols WHERE symbol = $1",
                        old_symbol
                    )
                    if not exists_check:
                        raise HTTPException(
                            status_code=404,
                            detail=f"Common symbol '{old_symbol}' not found"
                        )

                    # 2. Check if new_symbol already exists (conflict)
                    conflict_check = await conn.fetchval(
                        "SELECT 1 FROM common_symbols WHERE symbol = $1",
                        new_symbol
                    )
                    if conflict_check:
                        raise HTTPException(
                            status_code=409,
                            detail=f"Common symbol '{new_symbol}' already exists"
                        )

                    # 3. Perform the rename (CASCADE propagates to FK tables)
                    result = await conn.fetchrow(
                        """
                        UPDATE common_symbols
                        SET symbol = $1
                        WHERE symbol = $2
                        RETURNING symbol
                        """,
                        new_symbol,
                        old_symbol
                    )

                    if not result:
                        raise HTTPException(
                            status_code=500,
                            detail="Failed to rename common symbol"
                        )

                    # 4. Count affected rows AFTER cascade completes
                    mapping_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM asset_mapping WHERE common_symbol = $1",
                        new_symbol
                    )
                    membership_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM index_memberships WHERE common_symbol = $1",
                        new_symbol
                    )

            logger.info(
                "Registry.handle_rename_common_symbol: Successfully renamed '%s' to '%s'. "
                "Updated %d asset_mapping rows, %d index_memberships rows.",
                old_symbol, new_symbol, mapping_count, membership_count
            )

            return CommonSymbolRenameResponse(
                old_symbol=old_symbol,
                new_symbol=new_symbol,
                asset_mappings_updated=mapping_count or 0,
                index_memberships_updated=membership_count or 0
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                "Registry.handle_rename_common_symbol: Unexpected error: %s",
                e, exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail="An unexpected error occurred while renaming the symbol"
            )
