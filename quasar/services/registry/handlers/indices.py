"""Index management handlers for Registry service."""

from dataclasses import dataclass
from typing import List, Any
from datetime import datetime
from collections import defaultdict
import json
import asyncpg

from fastapi import HTTPException, Depends, Body
from fastapi.responses import Response

from quasar.services.registry.handlers.base import HandlerMixin
from quasar.services.registry.schemas import (
    IndexQueryParams, IndexMemberQueryParams,
    UserIndexCreate, UserIndexMembersUpdate, IndexSyncRequest,
    IndexItem, IndexMemberItem, IndexDetailResponse,
    IndexListResponse, IndexMembersResponse, IndexSyncResponse,
    IndexHistoryEvent, IndexHistoryChange, IndexHistoryResponse,
)
from quasar.services.registry.utils import FilterBuilder
from quasar.lib.enums import ASSET_CLASSES, normalize_asset_class

import logging
logger = logging.getLogger(__name__)


def _weights_equal(w1: float | None, w2: float | None) -> bool:
    """Compare two weights for equality within tolerance.

    Args:
        w1: First weight value.
        w2: Second weight value.

    Returns:
        True if weights are equal within 1e-9 tolerance.
    """
    if w1 is None and w2 is None:
        return True
    if w1 is None or w2 is None:
        return False
    return abs(w1 - w2) < 1e-9


@dataclass
class MembershipSyncResult:
    """Result of index membership synchronization.

    Attributes:
        added: Number of new memberships created.
        removed: Number of memberships closed.
        unchanged: Number of memberships with no changes.
        weights_updated: Number of memberships with weight changes.
    """
    added: int
    removed: int
    unchanged: int
    weights_updated: int


class IndexHandlersMixin(HandlerMixin):
    """Index management API handlers."""

    async def _sync_memberships_core(
        self,
        conn: asyncpg.Connection,
        index_name: str,
        index_type: str,
        constituent_weights: dict[str, float | None],
        *,
        use_scd: bool = False,
        source: str = 'api'
    ) -> MembershipSyncResult:
        """Sync index memberships within an existing transaction.

        Computes diff between incoming constituents and current active memberships.
        Handles additions, removals, and weight changes according to the specified mode.

        Args:
            conn: Active database connection (caller manages transaction).
            index_name: Index class_name.
            index_type: Index class_type (e.g., 'provider').
            constituent_weights: Dict mapping symbol to weight.
            use_scd: If True, use SCD Type 2 for weight changes (close old, insert new).
                If False, update weights in place.
            source: Source identifier for new membership records.

        Returns:
            MembershipSyncResult with counts of added, removed, unchanged, and weights_updated.

        Note:
            This method expects to be called within an active transaction context.
            The caller is responsible for transaction management.
        """
        result = MembershipSyncResult(added=0, removed=0, unchanged=0, weights_updated=0)
        incoming_symbols = set(constituent_weights.keys())

        # Get current active memberships
        current_members = await conn.fetch(
            """
            SELECT id, asset_symbol, weight
            FROM index_memberships
            WHERE index_class_name = $1 AND valid_to IS NULL
            """,
            index_name
        )
        current_symbols = {r['asset_symbol']: (r['id'], r['weight']) for r in current_members}
        current_symbol_set = set(current_symbols.keys())

        # Compute diff
        to_add = incoming_symbols - current_symbol_set
        to_remove = current_symbol_set - incoming_symbols
        potentially_unchanged = current_symbol_set & incoming_symbols

        # Close removed memberships
        if to_remove:
            await conn.execute(
                """
                UPDATE index_memberships
                SET valid_to = CURRENT_TIMESTAMP
                WHERE index_class_name = $1
                  AND asset_symbol = ANY($2)
                  AND valid_to IS NULL
                """,
                index_name, list(to_remove)
            )
            result.removed = len(to_remove)
            logger.info(f"Registry._sync_memberships_core: Closed {len(to_remove)} memberships for {index_name}")

        # Insert new memberships
        for symbol in to_add:
            weight = constituent_weights.get(symbol)
            await conn.execute(
                """
                INSERT INTO index_memberships
                (index_class_name, index_class_type, asset_class_name, asset_class_type,
                 asset_symbol, weight, source)
                VALUES ($1, $2, $1, $2, $3, $4, $5)
                """,
                index_name, index_type, symbol, weight, source
            )
            result.added += 1

        if to_add:
            logger.info(f"Registry._sync_memberships_core: Added {len(to_add)} memberships for {index_name}")

        # Handle weight changes for potentially unchanged members
        for symbol in potentially_unchanged:
            membership_id, current_weight = current_symbols[symbol]
            new_weight = constituent_weights.get(symbol)

            if not _weights_equal(current_weight, new_weight):
                if use_scd:
                    # SCD Type 2: Close old record, insert new
                    await conn.execute(
                        "UPDATE index_memberships SET valid_to = CURRENT_TIMESTAMP WHERE id = $1",
                        membership_id
                    )
                    await conn.execute(
                        """
                        INSERT INTO index_memberships
                        (index_class_name, index_class_type, asset_class_name, asset_class_type,
                         asset_symbol, weight, source)
                        VALUES ($1, $2, $1, $2, $3, $4, $5)
                        """,
                        index_name, index_type, symbol, new_weight, source
                    )
                    # In SCD mode, weight changes count as both removal and addition
                    result.removed += 1
                    result.added += 1
                else:
                    # In-place update
                    await conn.execute(
                        "UPDATE index_memberships SET weight = $1 WHERE id = $2",
                        new_weight, membership_id
                    )
                result.weights_updated += 1
            else:
                result.unchanged += 1

        if result.weights_updated:
            mode = "SCD Type 2" if use_scd else "in-place"
            logger.info(
                f"Registry._sync_memberships_core: Updated {result.weights_updated} weights "
                f"for {index_name} ({mode})"
            )

        return result

    async def _sync_index_memberships(
        self,
        index_name: str,
        index_type: str,
        constituent_weights: dict[str, float | None]
    ) -> dict:
        """Sync index memberships based on current constituents.

        Computes diff between incoming constituents and current active memberships.
        Closes removed memberships, adds new ones, and updates weights in place.

        Args:
            index_name: Index class_name.
            index_type: Index class_type ('provider').
            constituent_weights: Dict mapping symbol to weight.

        Returns:
            Dict with keys: added, removed, unchanged.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                result = await self._sync_memberships_core(
                    conn, index_name, index_type, constituent_weights,
                    use_scd=False, source='api'
                )
        return {'added': result.added, 'removed': result.removed, 'unchanged': result.unchanged}

    async def handle_get_indices(
        self,
        params: IndexQueryParams = Depends()
    ) -> IndexListResponse:
        """List all indices with pagination.

        Args:
            params: Query parameters for filtering and pagination.

        Returns:
            Paginated list of indices.
        """
        logger.info(f"Registry.handle_get_indices: Fetching indices with params {params}")

        try:
            # Filtering
            builder = FilterBuilder()
            builder.add('index_type', params.index_type)

            # Validate sort_by
            valid_sort_columns = ["class_name", "class_type", "index_type", "uploaded_at", "current_member_count"]
            sort_by = params.sort_by if params.sort_by in valid_sort_columns else "class_name"
            sort_order = "DESC" if params.sort_order.lower() == "desc" else "ASC"

            # Count query
            count_query = f"SELECT COUNT(*) as total FROM index_summary WHERE {builder.where_clause}"

            # Data query
            data_query = f"""
                SELECT class_name, class_type, index_type, uploaded_at,
                       current_member_count, preferences
                FROM index_summary
                WHERE {builder.where_clause}
                ORDER BY {sort_by} {sort_order}
                LIMIT ${builder.next_param_idx} OFFSET ${builder.next_param_idx + 1}
            """
            data_params = builder.params + [params.limit, params.offset]

            async with self.pool.acquire() as conn:
                count_result = await conn.fetchrow(count_query, *builder.params)
                records = await conn.fetch(data_query, *data_params)

            total_items = count_result['total'] if count_result else 0
            items = [
                IndexItem(
                    class_name=r['class_name'],
                    class_type=r['class_type'],
                    index_type=r['index_type'],
                    uploaded_at=r['uploaded_at'],
                    current_member_count=r['current_member_count'],
                    preferences=json.loads(r['preferences']) if r['preferences'] else None
                )
                for r in records
            ]

            return IndexListResponse(
                items=items,
                total_items=total_items,
                limit=params.limit,
                offset=params.offset,
                page=(params.offset // params.limit) + 1 if params.limit > 0 else 1,
                total_pages=(total_items + params.limit - 1) // params.limit if params.limit > 0 else 1
            )

        except Exception as e:
            logger.error(f"Registry.handle_get_indices: Unexpected error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving indices")

    async def handle_get_index(
        self,
        index_name: str
    ) -> IndexDetailResponse:
        """Get index details with current members.

        Args:
            index_name: The index class_name.

        Returns:
            Index details with current members.
        """
        logger.info(f"Registry.handle_get_index: Fetching index {index_name}")

        try:
            async with self.pool.acquire() as conn:
                # Get index info
                index_query = """
                    SELECT class_name, class_type, index_type, uploaded_at,
                           current_member_count, preferences
                    FROM index_summary
                    WHERE class_name = $1
                """
                index_record = await conn.fetchrow(index_query, index_name)

                if not index_record:
                    raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")

                # Get current members with mapped common_symbol from asset_mapping
                members_query = """
                    SELECT cim.id, cim.asset_class_name, cim.asset_class_type, cim.asset_symbol,
                           cim.common_symbol, cim.effective_symbol, cim.weight, cim.valid_from, cim.source,
                           am.common_symbol as mapped_common_symbol
                    FROM current_index_memberships cim
                    LEFT JOIN asset_mapping am
                        ON am.class_name = cim.asset_class_name
                       AND am.class_type = cim.asset_class_type
                       AND am.class_symbol = cim.asset_symbol
                    WHERE cim.index_class_name = $1
                    ORDER BY cim.weight DESC NULLS LAST
                """
                member_records = await conn.fetch(members_query, index_name)

            index_item = IndexItem(
                class_name=index_record['class_name'],
                class_type=index_record['class_type'],
                index_type=index_record['index_type'],
                uploaded_at=index_record['uploaded_at'],
                current_member_count=index_record['current_member_count'],
                preferences=json.loads(index_record['preferences']) if index_record['preferences'] else None
            )

            members = [
                IndexMemberItem(
                    id=r['id'],
                    asset_class_name=r['asset_class_name'],
                    asset_class_type=r['asset_class_type'],
                    asset_symbol=r['asset_symbol'],
                    common_symbol=r['common_symbol'],
                    mapped_common_symbol=r['mapped_common_symbol'],
                    effective_symbol=r['effective_symbol'],
                    weight=r['weight'],
                    valid_from=r['valid_from'],
                    source=r['source']
                )
                for r in member_records
            ]

            return IndexDetailResponse(index=index_item, members=members)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_get_index: Unexpected error for {index_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving index")

    async def handle_get_index_members(
        self,
        index_name: str,
        params: IndexMemberQueryParams = Depends()
    ) -> IndexMembersResponse:
        """Get index members with optional point-in-time query.

        Args:
            index_name: The index class_name.
            params: Query parameters including optional as_of timestamp.

        Returns:
            Paginated list of index members.
        """
        logger.info(f"Registry.handle_get_index_members: Fetching members for {index_name} with params {params}")

        try:
            # Validate sort_by
            valid_sort_columns = ["weight", "asset_symbol", "common_symbol", "valid_from", "mapped_common_symbol"]
            sort_by = params.sort_by if params.sort_by in valid_sort_columns else "weight"
            sort_order = "DESC" if params.sort_order.lower() == "desc" else "ASC"
            # Handle NULL weights in sorting
            nulls = "NULLS LAST" if sort_order == "DESC" else "NULLS FIRST"

            async with self.pool.acquire() as conn:
                # Check index exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM code_registry WHERE class_name = $1 AND class_subtype IN ('IndexProvider', 'UserIndex')",
                    index_name
                )
                if not exists:
                    raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")

                if params.as_of:
                    # Point-in-time query with mapped common_symbol from asset_mapping
                    count_query = """
                        SELECT COUNT(*) FROM get_index_members_at($1, 'provider', $2)
                    """
                    data_query = f"""
                        SELECT im.id, im.asset_class_name, im.asset_class_type, im.asset_symbol,
                               im.common_symbol,
                               COALESCE(im.asset_symbol, im.common_symbol) as effective_symbol,
                               im.weight, im.valid_from, im.source,
                               am.common_symbol as mapped_common_symbol
                        FROM index_memberships im
                        LEFT JOIN asset_mapping am
                            ON am.class_name = im.asset_class_name
                           AND am.class_type = im.asset_class_type
                           AND am.class_symbol = im.asset_symbol
                        WHERE im.index_class_name = $1
                          AND im.valid_from <= $2
                          AND (im.valid_to IS NULL OR im.valid_to > $2)
                        ORDER BY {sort_by} {sort_order} {nulls}
                        LIMIT $3 OFFSET $4
                    """
                    count_result = await conn.fetchrow(count_query, index_name, params.as_of)
                    records = await conn.fetch(data_query, index_name, params.as_of, params.limit, params.offset)
                else:
                    # Current members query with mapped common_symbol from asset_mapping
                    count_query = """
                        SELECT COUNT(*) FROM current_index_memberships WHERE index_class_name = $1
                    """
                    data_query = f"""
                        SELECT cim.id, cim.asset_class_name, cim.asset_class_type, cim.asset_symbol,
                               cim.common_symbol, cim.effective_symbol, cim.weight, cim.valid_from, cim.source,
                               am.common_symbol as mapped_common_symbol
                        FROM current_index_memberships cim
                        LEFT JOIN asset_mapping am
                            ON am.class_name = cim.asset_class_name
                           AND am.class_type = cim.asset_class_type
                           AND am.class_symbol = cim.asset_symbol
                        WHERE cim.index_class_name = $1
                        ORDER BY {sort_by} {sort_order} {nulls}
                        LIMIT $2 OFFSET $3
                    """
                    count_result = await conn.fetchrow(count_query, index_name)
                    records = await conn.fetch(data_query, index_name, params.limit, params.offset)

            total_items = count_result['count'] if count_result else 0
            items = [
                IndexMemberItem(
                    id=r['id'],
                    asset_class_name=r['asset_class_name'],
                    asset_class_type=r['asset_class_type'],
                    asset_symbol=r['asset_symbol'],
                    common_symbol=r['common_symbol'],
                    mapped_common_symbol=r['mapped_common_symbol'],
                    effective_symbol=r['effective_symbol'],
                    weight=r['weight'],
                    valid_from=r['valid_from'],
                    source=r['source']
                )
                for r in records
            ]

            return IndexMembersResponse(
                items=items,
                total_items=total_items,
                limit=params.limit,
                offset=params.offset,
                page=(params.offset // params.limit) + 1 if params.limit > 0 else 1,
                total_pages=(total_items + params.limit - 1) // params.limit if params.limit > 0 else 1
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_get_index_members: Unexpected error for {index_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving index members")

    async def handle_get_index_history(
        self,
        index_name: str
    ) -> IndexHistoryResponse:
        """Get membership change history for an index (timeline view).

        Returns all membership additions and removals grouped by date,
        sorted newest first. This enables the timeline UI showing when
        members were added or removed from the index.

        Args:
            index_name: The index class_name.

        Returns:
            IndexHistoryResponse with changes grouped by date.
        """
        logger.info(f"Registry.handle_get_index_history: Fetching history for '{index_name}'")

        try:
            async with self.pool.acquire() as conn:
                # Verify index exists
                exists = await conn.fetchval(
                    """SELECT 1 FROM code_registry
                       WHERE class_name = $1
                       AND class_subtype IN ('IndexProvider', 'UserIndex')""",
                    index_name
                )
                if not exists:
                    raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")

                # Fetch all membership records (current and historical)
                query = """
                    SELECT
                        COALESCE(asset_symbol, common_symbol) as symbol,
                        weight,
                        valid_from,
                        valid_to
                    FROM index_memberships
                    WHERE index_class_name = $1
                    ORDER BY valid_from DESC
                """
                records = await conn.fetch(query, index_name)

            # Build change events grouped by date
            # We need to track both additions (valid_from) and removals (valid_to)
            changes_by_date: dict[str, list[IndexHistoryEvent]] = defaultdict(list)

            for r in records:
                symbol = r['symbol']
                weight = r['weight']

                # Addition event: when this membership started
                add_date = r['valid_from'].strftime('%Y-%m-%d')
                changes_by_date[add_date].append(
                    IndexHistoryEvent(type="added", symbol=symbol, weight=weight)
                )

                # Removal event: when this membership ended (if it did)
                if r['valid_to'] is not None:
                    remove_date = r['valid_to'].strftime('%Y-%m-%d')
                    changes_by_date[remove_date].append(
                        IndexHistoryEvent(type="removed", symbol=symbol, weight=weight)
                    )

            # Convert to list of IndexHistoryChange, sorted by date descending
            changes = []
            for date_str in sorted(changes_by_date.keys(), reverse=True):
                events = changes_by_date[date_str]
                # Sort events: removals first, then additions, then by symbol
                events.sort(key=lambda e: (0 if e.type == "removed" else 1, e.symbol))
                changes.append(
                    IndexHistoryChange(
                        date=datetime.fromisoformat(date_str),
                        events=events
                    )
                )

            return IndexHistoryResponse(changes=changes)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_get_index_history: Unexpected error for {index_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while retrieving index history")

    async def handle_create_user_index(
        self,
        body: UserIndexCreate = Body(...)
    ) -> IndexItem:
        """Create a new UserIndex.

        Args:
            body: UserIndex creation request with name and optional description.

        Returns:
            Created index item.
        """
        logger.info(f"Registry.handle_create_user_index: Creating UserIndex '{body.name}'")

        try:
            preferences = {"description": body.description} if body.description else {}

            query = """
                INSERT INTO code_registry (class_name, class_type, class_subtype, preferences)
                VALUES ($1, 'provider', 'UserIndex', $2)
                RETURNING class_name, class_type, class_subtype, uploaded_at, preferences
            """

            async with self.pool.acquire() as conn:
                record = await conn.fetchrow(query, body.name, json.dumps(preferences))

            return IndexItem(
                class_name=record['class_name'],
                class_type=record['class_type'],
                index_type=record['class_subtype'],
                uploaded_at=record['uploaded_at'],
                current_member_count=0,
                preferences=json.loads(record['preferences']) if record['preferences'] else None
            )

        except asyncpg.exceptions.UniqueViolationError:
            raise HTTPException(status_code=409, detail=f"Index '{body.name}' already exists")
        except Exception as e:
            logger.error(f"Registry.handle_create_user_index: Unexpected error: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while creating index")

    async def handle_delete_index(
        self,
        index_name: str
    ) -> Response:
        """Delete a UserIndex (not allowed for IndexProvider).

        Args:
            index_name: The index class_name.

        Returns:
            204 No Content on success.
        """
        logger.info(f"Registry.handle_delete_index: Deleting index '{index_name}'")

        try:
            async with self.pool.acquire() as conn:
                # Check index type
                record = await conn.fetchrow(
                    "SELECT class_subtype FROM code_registry WHERE class_name = $1 AND class_type = 'provider'",
                    index_name
                )

                if not record:
                    raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")

                if record['class_subtype'] != 'UserIndex':
                    raise HTTPException(
                        status_code=403,
                        detail=f"Cannot delete IndexProvider '{index_name}'. Only UserIndex can be deleted."
                    )

                # Delete (memberships cascade)
                await conn.execute(
                    "DELETE FROM code_registry WHERE class_name = $1 AND class_type = 'provider'",
                    index_name
                )

            return Response(status_code=204)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_delete_index: Unexpected error for {index_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while deleting index")

    async def handle_update_user_index_members(
        self,
        index_name: str,
        body: UserIndexMembersUpdate = Body(...)
    ) -> IndexMembersResponse:
        """Replace UserIndex members (full replacement).

        Args:
            index_name: The index class_name.
            body: New member list with common_symbols and optional weights.

        Returns:
            Updated member list.
        """
        logger.info(f"Registry.handle_update_user_index_members: Updating members for '{index_name}'")

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Verify index exists and is UserIndex
                    record = await conn.fetchrow(
                        "SELECT class_subtype FROM code_registry WHERE class_name = $1 AND class_type = 'provider'",
                        index_name
                    )

                    if not record:
                        raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")

                    if record['class_subtype'] != 'UserIndex':
                        raise HTTPException(
                            status_code=403,
                            detail=f"Cannot update members for IndexProvider '{index_name}'. Use sync endpoint instead."
                        )

                    # Validate common_symbols exist
                    common_symbols = [m.common_symbol for m in body.members]
                    if common_symbols:
                        existing = await conn.fetch(
                            "SELECT symbol FROM common_symbols WHERE symbol = ANY($1)",
                            common_symbols
                        )
                        existing_symbols = {r['symbol'] for r in existing}
                        missing = set(common_symbols) - existing_symbols
                        if missing:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Invalid common_symbols: {list(missing)}"
                            )

                    # Close all current members
                    await conn.execute(
                        """
                        UPDATE index_memberships
                        SET valid_to = CURRENT_TIMESTAMP
                        WHERE index_class_name = $1 AND valid_to IS NULL
                        """,
                        index_name
                    )

                    # Insert new members
                    new_members = []
                    for member in body.members:
                        result = await conn.fetchrow(
                            """
                            INSERT INTO index_memberships
                            (index_class_name, index_class_type, common_symbol, weight, source)
                            VALUES ($1, 'provider', $2, $3, 'manual')
                            RETURNING id, common_symbol, weight, valid_from, source
                            """,
                            index_name, member.common_symbol, member.weight
                        )
                        new_members.append(result)

            items = [
                IndexMemberItem(
                    id=r['id'],
                    asset_class_name=None,
                    asset_class_type=None,
                    asset_symbol=None,
                    common_symbol=r['common_symbol'],
                    effective_symbol=r['common_symbol'],
                    weight=r['weight'],
                    valid_from=r['valid_from'],
                    source=r['source']
                )
                for r in new_members
            ]

            return IndexMembersResponse(
                items=items,
                total_items=len(items),
                limit=len(items),
                offset=0,
                page=1,
                total_pages=1
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_update_user_index_members: Unexpected error for {index_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while updating index members")

    async def handle_sync_index(
        self,
        index_name: str,
        body: IndexSyncRequest = Body(...)
    ) -> IndexSyncResponse:
        """Sync API index constituents (called by DataHub).

        Args:
            index_name: The index class_name.
            body: Constituents list from IndexProvider.

        Returns:
            Sync statistics.
        """
        logger.info(f"Registry.handle_sync_index: Syncing {len(body.constituents)} constituents for '{index_name}'")

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Verify index exists and is IndexProvider
                    record = await conn.fetchrow(
                        "SELECT class_subtype FROM code_registry WHERE class_name = $1 AND class_type = 'provider'",
                        index_name
                    )

                    if not record:
                        raise HTTPException(status_code=404, detail=f"Index '{index_name}' not found")

                    if record['class_subtype'] != 'IndexProvider':
                        raise HTTPException(
                            status_code=403,
                            detail=f"Cannot sync UserIndex '{index_name}'. Use PUT members endpoint instead."
                        )

                    stats = {
                        "assets_created": 0,
                        "assets_updated": 0,
                        "members_added": 0,
                        "members_removed": 0,
                        "members_unchanged": 0
                    }

                    # Step 1: Upsert assets from constituents
                    asset_upsert_query = """
                        INSERT INTO assets (
                            class_name, class_type, symbol, matcher_symbol, name,
                            exchange, asset_class, base_currency, quote_currency
                        ) VALUES ($1, 'provider', $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (class_name, class_type, symbol) DO UPDATE SET
                            matcher_symbol = EXCLUDED.matcher_symbol,
                            name = EXCLUDED.name,
                            exchange = EXCLUDED.exchange,
                            asset_class = EXCLUDED.asset_class,
                            base_currency = EXCLUDED.base_currency,
                            quote_currency = EXCLUDED.quote_currency
                        RETURNING xmax
                    """

                    constituent_weights = {}
                    for c in body.constituents:
                        # Normalize asset_class
                        asset_class = normalize_asset_class(c.asset_class) if c.asset_class else None
                        if asset_class and asset_class not in ASSET_CLASSES:
                            asset_class = None

                        result = await conn.fetchrow(
                            asset_upsert_query,
                            index_name,
                            c.symbol,
                            c.matcher_symbol or c.symbol,
                            c.name or "",
                            c.exchange or "",
                            asset_class,
                            c.base_currency or "",
                            c.quote_currency or ""
                        )

                        # xmax == 0 means INSERT, xmax > 0 means UPDATE
                        if result and result['xmax'] == 0:
                            stats["assets_created"] += 1
                        else:
                            stats["assets_updated"] += 1

                        constituent_weights[c.symbol] = c.weight

                    # Step 2: Sync memberships using shared helper (SCD Type 2)
                    membership_result = await self._sync_memberships_core(
                        conn, index_name, 'provider', constituent_weights,
                        use_scd=True, source='api'
                    )
                    stats["members_added"] = membership_result.added
                    stats["members_removed"] = membership_result.removed
                    stats["members_unchanged"] = membership_result.unchanged

            logger.info(f"Registry.handle_sync_index: Sync complete for '{index_name}': {stats}")
            return IndexSyncResponse(index_class_name=index_name, **stats)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Registry.handle_sync_index: Unexpected error for {index_name}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while syncing index")
