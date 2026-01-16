"""Tests for index management handlers."""

import pytest
from unittest.mock import AsyncMock, Mock
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException

from .conftest import MockRecord


class TestIndexListEndpoint:
    """Tests for GET /api/registry/indices endpoint."""

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_indices(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns empty list when no indices exist."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexQueryParams

        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total=0))
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        result = await reg.handle_get_indices(IndexQueryParams())

        assert result.items == []
        assert result.total_items == 0

    @pytest.mark.asyncio
    async def test_returns_indices_with_member_counts(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns indices with their member counts."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexQueryParams

        mock_records = [
            MockRecord(
                class_name='CCI30',
                class_type='provider',
                index_type='IndexProvider',
                uploaded_at=datetime.now(timezone.utc),
                current_member_count=30,
                preferences='{}'
            ),
            MockRecord(
                class_name='MyIndex',
                class_type='provider',
                index_type='UserIndex',
                uploaded_at=datetime.now(timezone.utc),
                current_member_count=5,
                preferences='{"description": "My custom index"}'
            ),
        ]
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total=2))
        mock_asyncpg_conn.fetch = AsyncMock(return_value=mock_records)

        result = await reg.handle_get_indices(IndexQueryParams())

        assert result.total_items == 2
        assert len(result.items) == 2
        assert result.items[0].class_name == 'CCI30'
        assert result.items[0].current_member_count == 30
        assert result.items[1].index_type == 'UserIndex'

    @pytest.mark.asyncio
    async def test_filters_by_index_type(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Filters indices by index_type parameter."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexQueryParams

        mock_records = [
            MockRecord(
                class_name='MyIndex',
                class_type='provider',
                index_type='UserIndex',
                uploaded_at=datetime.now(timezone.utc),
                current_member_count=5,
                preferences=None
            ),
        ]
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(total=1))
        mock_asyncpg_conn.fetch = AsyncMock(return_value=mock_records)

        result = await reg.handle_get_indices(IndexQueryParams(index_type='UserIndex'))

        assert result.total_items == 1
        assert result.items[0].index_type == 'UserIndex'


class TestCreateUserIndexEndpoint:
    """Tests for POST /api/registry/indices endpoint."""

    @pytest.mark.asyncio
    async def test_creates_user_index_successfully(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Successfully creates a UserIndex."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import UserIndexCreate

        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(
            class_name='MyIndex',
            class_type='provider',
            class_subtype='UserIndex',
            uploaded_at=datetime.now(timezone.utc),
            preferences='{"description": "Test index"}'
        ))

        result = await reg.handle_create_user_index(
            UserIndexCreate(name='MyIndex', description='Test index')
        )

        assert result.class_name == 'MyIndex'
        assert result.index_type == 'UserIndex'
        assert result.current_member_count == 0

    @pytest.mark.asyncio
    async def test_returns_409_when_name_exists(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 409 Conflict when index name already exists."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import UserIndexCreate
        from asyncpg.exceptions import UniqueViolationError

        mock_asyncpg_conn.fetchrow = AsyncMock(
            side_effect=UniqueViolationError("duplicate key value")
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_create_user_index(
                UserIndexCreate(name='ExistingIndex')
            )

        assert exc_info.value.status_code == 409
        assert 'already exists' in exc_info.value.detail


class TestGetIndexEndpoint:
    """Tests for GET /api/registry/indices/{name} endpoint."""

    @pytest.mark.asyncio
    async def test_returns_index_with_members(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns index details with current members."""
        reg = registry_with_mocks

        index_record = MockRecord(
            class_name='CCI30',
            class_type='provider',
            index_type='IndexProvider',
            uploaded_at=datetime.now(timezone.utc),
            current_member_count=2,
            preferences=None
        )
        member_records = [
            MockRecord(
                id=1,
                asset_class_name='CCI30',
                asset_class_type='provider',
                asset_symbol='BTC',
                common_symbol=None,
                mapped_common_symbol='BTCUSD',
                effective_symbol='BTC',
                weight=0.27,
                valid_from=datetime.now(timezone.utc),
                source='api'
            ),
            MockRecord(
                id=2,
                asset_class_name='CCI30',
                asset_class_type='provider',
                asset_symbol='ETH',
                common_symbol=None,
                mapped_common_symbol='ETHUSD',
                effective_symbol='ETH',
                weight=0.12,
                valid_from=datetime.now(timezone.utc),
                source='api'
            ),
        ]
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=index_record)
        mock_asyncpg_conn.fetch = AsyncMock(return_value=member_records)

        result = await reg.handle_get_index('CCI30')

        assert result.index.class_name == 'CCI30'
        assert len(result.members) == 2
        assert result.members[0].asset_symbol == 'BTC'
        assert result.members[0].weight == 0.27
        assert result.members[0].mapped_common_symbol == 'BTCUSD'
        assert result.members[1].mapped_common_symbol == 'ETHUSD'

    @pytest.mark.asyncio
    async def test_returns_404_when_not_found(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 404 when index doesn't exist."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_index('NonExistent')

        assert exc_info.value.status_code == 404
        assert 'not found' in exc_info.value.detail


class TestDeleteIndexEndpoint:
    """Tests for DELETE /api/registry/indices/{name} endpoint."""

    @pytest.mark.asyncio
    async def test_deletes_user_index_successfully(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Successfully deletes a UserIndex."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(class_subtype='UserIndex')
        )
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg.handle_delete_index('MyIndex')

        assert result.status_code == 204

    @pytest.mark.asyncio
    async def test_returns_403_for_index_provider(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 403 Forbidden when trying to delete an IndexProvider."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(class_subtype='IndexProvider')
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_delete_index('CCI30')

        assert exc_info.value.status_code == 403
        assert 'Cannot delete IndexProvider' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_returns_404_when_not_found(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 404 when index doesn't exist."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_delete_index('NonExistent')

        assert exc_info.value.status_code == 404


class TestGetIndexMembersEndpoint:
    """Tests for GET /api/registry/indices/{name}/members endpoint."""

    @pytest.mark.asyncio
    async def test_returns_current_members(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns current members of an index."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexMemberQueryParams

        member_records = [
            MockRecord(
                id=1,
                asset_class_name='CCI30',
                asset_class_type='provider',
                asset_symbol='BTC',
                common_symbol=None,
                mapped_common_symbol='BTCUSD',
                effective_symbol='BTC',
                weight=0.27,
                valid_from=datetime.now(timezone.utc),
                source='api'
            ),
        ]
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Index exists
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(count=1))
        mock_asyncpg_conn.fetch = AsyncMock(return_value=member_records)

        result = await reg.handle_get_index_members('CCI30', IndexMemberQueryParams())

        assert result.total_items == 1
        assert len(result.items) == 1
        assert result.items[0].asset_symbol == 'BTC'
        assert result.items[0].mapped_common_symbol == 'BTCUSD'

    @pytest.mark.asyncio
    async def test_returns_404_when_index_not_found(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 404 when index doesn't exist."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexMemberQueryParams

        mock_asyncpg_conn.fetchval = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_index_members('NonExistent', IndexMemberQueryParams())

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_none_mapped_common_symbol_when_no_mapping(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns None for mapped_common_symbol when asset has no mapping."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexMemberQueryParams

        member_records = [
            MockRecord(
                id=1,
                asset_class_name='CCI30',
                asset_class_type='provider',
                asset_symbol='NEWCOIN',
                common_symbol=None,
                mapped_common_symbol=None,  # No mapping exists
                effective_symbol='NEWCOIN',
                weight=0.10,
                valid_from=datetime.now(timezone.utc),
                source='api'
            ),
        ]
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Index exists
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(count=1))
        mock_asyncpg_conn.fetch = AsyncMock(return_value=member_records)

        result = await reg.handle_get_index_members('CCI30', IndexMemberQueryParams())

        assert result.items[0].mapped_common_symbol is None
        assert result.items[0].asset_symbol == 'NEWCOIN'

    @pytest.mark.asyncio
    async def test_user_index_members_have_common_symbol_not_mapped(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """UserIndex members have common_symbol set directly, mapped_common_symbol is None."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexMemberQueryParams

        member_records = [
            MockRecord(
                id=1,
                asset_class_name=None,
                asset_class_type=None,
                asset_symbol=None,
                common_symbol='BTCUSD',  # UserIndex uses common_symbol directly
                mapped_common_symbol=None,  # No join match (asset columns are NULL)
                effective_symbol='BTCUSD',
                weight=0.5,
                valid_from=datetime.now(timezone.utc),
                source='manual'
            ),
        ]
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)  # Index exists
        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=MockRecord(count=1))
        mock_asyncpg_conn.fetch = AsyncMock(return_value=member_records)

        result = await reg.handle_get_index_members('MyUserIndex', IndexMemberQueryParams())

        assert result.items[0].common_symbol == 'BTCUSD'
        assert result.items[0].mapped_common_symbol is None
        assert result.items[0].asset_symbol is None


class TestUpdateUserIndexMembersEndpoint:
    """Tests for PUT /api/registry/indices/{name}/members endpoint."""

    @pytest.mark.asyncio
    async def test_replaces_members_successfully(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Successfully replaces UserIndex members."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import UserIndexMembersUpdate, UserIndexMemberCreate

        # Mock transaction context
        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            MockRecord(class_subtype='UserIndex'),  # Index check
            MockRecord(id=1, common_symbol='BTCUSD', weight=0.5,
                      valid_from=datetime.now(timezone.utc), source='manual'),
            MockRecord(id=2, common_symbol='ETHUSD', weight=0.5,
                      valid_from=datetime.now(timezone.utc), source='manual'),
        ])
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(symbol='BTCUSD'),
            MockRecord(symbol='ETHUSD'),
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg.handle_update_user_index_members(
            'MyIndex',
            UserIndexMembersUpdate(members=[
                UserIndexMemberCreate(common_symbol='BTCUSD', weight=0.5),
                UserIndexMemberCreate(common_symbol='ETHUSD', weight=0.5),
            ])
        )

        assert len(result.items) == 2

    @pytest.mark.asyncio
    async def test_returns_403_for_index_provider(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 403 when trying to update IndexProvider members."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import UserIndexMembersUpdate, UserIndexMemberCreate

        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(class_subtype='IndexProvider')
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_user_index_members(
                'CCI30',
                UserIndexMembersUpdate(members=[
                    UserIndexMemberCreate(common_symbol='BTCUSD'),
                ])
            )

        assert exc_info.value.status_code == 403
        assert 'Use sync endpoint' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_returns_400_for_invalid_common_symbols(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 400 when common_symbols don't exist."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import UserIndexMembersUpdate, UserIndexMemberCreate

        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(class_subtype='UserIndex')
        )
        # Return empty list - no matching common_symbols found
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_user_index_members(
                'MyIndex',
                UserIndexMembersUpdate(members=[
                    UserIndexMemberCreate(common_symbol='INVALID'),
                ])
            )

        assert exc_info.value.status_code == 400
        assert 'Invalid common_symbols' in exc_info.value.detail


class TestSyncIndexEndpoint:
    """Tests for POST /api/registry/indices/{name}/sync endpoint."""

    @pytest.mark.asyncio
    async def test_syncs_constituents_successfully(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Successfully syncs constituents, creating assets and memberships."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexSyncRequest, IndexConstituentSync

        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            MockRecord(class_subtype='IndexProvider'),  # Index check
            MockRecord(xmax=0),  # Asset upsert - created
            MockRecord(xmax=0),  # Asset upsert - created
        ])
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])  # No current members
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg.handle_sync_index(
            'CCI30',
            IndexSyncRequest(constituents=[
                IndexConstituentSync(symbol='BTC', weight=0.27, asset_class='crypto'),
                IndexConstituentSync(symbol='ETH', weight=0.12, asset_class='crypto'),
            ])
        )

        assert result.index_class_name == 'CCI30'
        assert result.assets_created == 2
        assert result.members_added == 2
        assert result.members_removed == 0

    @pytest.mark.asyncio
    async def test_removes_members_not_in_new_list(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Removes members that are not in the new constituent list."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexSyncRequest, IndexConstituentSync

        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(side_effect=[
            MockRecord(class_subtype='IndexProvider'),  # Index check
            MockRecord(xmax=1),  # Asset upsert - updated
        ])
        # Current members include XRP which will be removed
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=0.27),
            MockRecord(id=2, asset_symbol='XRP', weight=0.05),  # Will be removed
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg.handle_sync_index(
            'CCI30',
            IndexSyncRequest(constituents=[
                IndexConstituentSync(symbol='BTC', weight=0.27),  # Unchanged
            ])
        )

        assert result.members_removed == 1
        assert result.members_unchanged == 1

    @pytest.mark.asyncio
    async def test_returns_403_for_user_index(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 403 when trying to sync a UserIndex."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexSyncRequest, IndexConstituentSync

        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(
            return_value=MockRecord(class_subtype='UserIndex')
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_sync_index(
                'MyIndex',
                IndexSyncRequest(constituents=[
                    IndexConstituentSync(symbol='BTC'),
                ])
            )

        assert exc_info.value.status_code == 403
        assert 'Cannot sync UserIndex' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_returns_404_when_index_not_found(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 404 when index doesn't exist."""
        reg = registry_with_mocks
        from quasar.services.registry.schemas import IndexSyncRequest, IndexConstituentSync

        mock_transaction = AsyncMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=mock_transaction)
        mock_transaction.__aexit__ = AsyncMock(return_value=None)
        mock_asyncpg_conn.transaction = Mock(return_value=mock_transaction)

        mock_asyncpg_conn.fetchrow = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_sync_index(
                'NonExistent',
                IndexSyncRequest(constituents=[
                    IndexConstituentSync(symbol='BTC'),
                ])
            )

        assert exc_info.value.status_code == 404


class TestGetIndexHistoryEndpoint:
    """Tests for GET /api/registry/indices/{name}/history endpoint."""

    @pytest.mark.asyncio
    async def test_returns_history_with_changes_grouped_by_date(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns membership changes grouped by date, sorted newest first."""
        reg = registry_with_mocks

        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(weeks=1)
        two_weeks_ago = now - timedelta(weeks=2)

        # Mock index exists check
        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)

        # Mock membership records with history
        member_records = [
            # Current member (added recently)
            MockRecord(
                symbol='BTC',
                weight=0.30,
                valid_from=week_ago,
                valid_to=None
            ),
            # Removed member (was added 2 weeks ago, removed 1 week ago)
            MockRecord(
                symbol='ETH',
                weight=0.25,
                valid_from=two_weeks_ago,
                valid_to=week_ago
            ),
            # Current member (added 2 weeks ago, still active)
            MockRecord(
                symbol='SOL',
                weight=0.20,
                valid_from=two_weeks_ago,
                valid_to=None
            ),
        ]
        mock_asyncpg_conn.fetch = AsyncMock(return_value=member_records)

        result = await reg.handle_get_index_history('TestIndex')

        # Should have 2 dates with changes
        assert len(result.changes) == 2

        # First change (most recent = week ago)
        first_change = result.changes[0]
        assert first_change.date.strftime('%Y-%m-%d') == week_ago.strftime('%Y-%m-%d')
        # Should have removal of ETH and addition of BTC
        event_types = [(e.type, e.symbol) for e in first_change.events]
        assert ('removed', 'ETH') in event_types
        assert ('added', 'BTC') in event_types

        # Second change (older = two weeks ago)
        second_change = result.changes[1]
        assert second_change.date.strftime('%Y-%m-%d') == two_weeks_ago.strftime('%Y-%m-%d')
        # Should have additions of ETH and SOL
        event_types = [(e.type, e.symbol) for e in second_change.events]
        assert ('added', 'ETH') in event_types
        assert ('added', 'SOL') in event_types

    @pytest.mark.asyncio
    async def test_returns_empty_changes_for_index_with_no_history(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns empty changes list for index with no membership records."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetchval = AsyncMock(return_value=1)
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        result = await reg.handle_get_index_history('EmptyIndex')

        assert len(result.changes) == 0

    @pytest.mark.asyncio
    async def test_returns_404_when_index_not_found(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Returns 404 when index doesn't exist."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetchval = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_index_history('NonExistent')

        assert exc_info.value.status_code == 404
        assert 'not found' in exc_info.value.detail


class TestWeightsEqualHelper:
    """Tests for the _weights_equal utility function."""

    def test_both_none_returns_true(self):
        """Both None weights are considered equal."""
        from quasar.services.registry.core import _weights_equal
        assert _weights_equal(None, None) is True

    def test_one_none_returns_false(self):
        """One None and one non-None are not equal."""
        from quasar.services.registry.core import _weights_equal
        assert _weights_equal(None, 0.5) is False
        assert _weights_equal(0.5, None) is False

    def test_within_tolerance_returns_true(self):
        """Weights within 1e-9 tolerance are considered equal."""
        from quasar.services.registry.core import _weights_equal
        assert _weights_equal(0.25, 0.25 + 1e-10) is True
        assert _weights_equal(0.25, 0.25 - 1e-10) is True

    def test_outside_tolerance_returns_false(self):
        """Weights outside 1e-9 tolerance are not equal."""
        from quasar.services.registry.core import _weights_equal
        assert _weights_equal(0.25, 0.25 + 1e-8) is False
        assert _weights_equal(0.25, 0.25 - 1e-8) is False

    def test_exact_match_returns_true(self):
        """Exactly equal weights are considered equal."""
        from quasar.services.registry.core import _weights_equal
        assert _weights_equal(0.5, 0.5) is True
        assert _weights_equal(0.0, 0.0) is True


class TestMembershipSyncCore:
    """Tests for the unified _sync_memberships_core helper."""

    @pytest.mark.asyncio
    async def test_in_place_weight_update_does_not_create_history(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """In-place mode (use_scd=False) updates weights without closing records."""
        reg = registry_with_mocks

        # Setup: existing member with different weight
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=0.25),
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': 0.30},  # Weight changed
            use_scd=False
        )

        # Should update in place, not close+insert
        assert result.weights_updated == 1
        assert result.added == 0  # No new records for weight change
        assert result.removed == 0
        assert result.unchanged == 0

        # Verify UPDATE was called with weight change (not INSERT for weight change)
        execute_calls = [str(call) for call in mock_asyncpg_conn.execute.call_args_list]
        assert any('SET weight' in call for call in execute_calls)

    @pytest.mark.asyncio
    async def test_scd_weight_update_creates_history(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """SCD mode (use_scd=True) closes old record and creates new for weight changes."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=0.25),
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': 0.30},
            use_scd=True
        )

        # SCD mode: weight change = remove + add
        assert result.weights_updated == 1
        assert result.added == 1
        assert result.removed == 1
        assert result.unchanged == 0

        # Verify close + insert pattern
        execute_calls = [str(call) for call in mock_asyncpg_conn.execute.call_args_list]
        assert any('valid_to = CURRENT_TIMESTAMP' in call for call in execute_calls)
        assert any('INSERT INTO index_memberships' in call for call in execute_calls)

    @pytest.mark.asyncio
    async def test_no_change_when_weights_equal(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """No updates when weights are equal within tolerance."""
        reg = registry_with_mocks

        # 0.25 + 1e-10 is within the 1e-9 tolerance (diff = 1e-10 < 1e-9)
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=0.2500000001),
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': 0.25},  # Within 1e-9 tolerance
            use_scd=False
        )

        assert result.unchanged == 1
        assert result.weights_updated == 0
        assert result.added == 0
        assert result.removed == 0

    @pytest.mark.asyncio
    async def test_handles_none_weights(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Correctly handles None weights (equal-weighted indices)."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=None),
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        # None to None should be unchanged
        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': None},
            use_scd=False
        )

        assert result.unchanged == 1
        assert result.weights_updated == 0

    @pytest.mark.asyncio
    async def test_none_to_value_triggers_update(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Changing from None to a value triggers weight update."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=None),
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': 0.5},
            use_scd=False
        )

        assert result.weights_updated == 1
        assert result.unchanged == 0

    @pytest.mark.asyncio
    async def test_adds_new_members(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Correctly adds new members that don't exist."""
        reg = registry_with_mocks

        # No existing members
        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': 0.5, 'ETH': 0.3},
            use_scd=False
        )

        assert result.added == 2
        assert result.removed == 0
        assert result.unchanged == 0

    @pytest.mark.asyncio
    async def test_removes_members_not_in_new_list(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Correctly removes members not in the new list."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[
            MockRecord(id=1, asset_symbol='BTC', weight=0.5),
            MockRecord(id=2, asset_symbol='XRP', weight=0.3),  # Will be removed
        ])
        mock_asyncpg_conn.execute = AsyncMock()

        result = await reg._sync_memberships_core(
            mock_asyncpg_conn,
            'TestIndex',
            'provider',
            {'BTC': 0.5},  # Only BTC remains
            use_scd=False
        )

        assert result.removed == 1
        assert result.unchanged == 1
        assert result.added == 0
