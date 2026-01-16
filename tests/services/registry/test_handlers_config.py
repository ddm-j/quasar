"""Tests for provider configuration handlers."""

import pytest
from unittest.mock import AsyncMock

from fastapi import HTTPException

from .conftest import MockRecord


class TestRegistryGetClassesSummary:
    """Tests for get classes summary endpoint."""

    @pytest.mark.asyncio
    async def test_handle_get_classes_summary_with_classes(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_classes_summary returns classes with asset counts."""
        reg = registry_with_mocks

        mock_record = MockRecord(
            id=1, class_name="TestProvider", class_type="provider",
            class_subtype="Historical", uploaded_at="2024-01-01", asset_count=5
        )

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[mock_record])

        summary = await reg.handle_get_classes_summary()

        assert len(summary) == 1
        assert summary[0].class_name == "TestProvider"
        assert summary[0].asset_count == 5

    @pytest.mark.asyncio
    async def test_handle_get_classes_summary_empty_registry(
        self, registry_with_mocks, mock_asyncpg_conn
    ):
        """Test that handle_get_classes_summary returns empty list for empty registry."""
        reg = registry_with_mocks

        mock_asyncpg_conn.fetch = AsyncMock(return_value=[])

        summary = await reg.handle_get_classes_summary()

        assert summary == []


class TestProviderConfig:
    """Tests for provider configuration endpoints."""

    @pytest.mark.asyncio
    async def test_get_config_success(self, registry_with_mocks, mock_asyncpg_pool):
        """Test successful retrieval of provider preferences."""
        reg = registry_with_mocks

        # Mock the database response - provider exists and has preferences
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            {'crypto': {'preferred_quote_currency': 'USDC'}}  # Preferences
        ])

        result = await reg.handle_get_provider_config(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.preferences.crypto is not None
        assert result.preferences.crypto.preferred_quote_currency == 'USDC'

    @pytest.mark.asyncio
    async def test_get_config_not_found(self, registry_with_mocks, mock_asyncpg_pool):
        """Test 404 response when provider not found."""
        reg = registry_with_mocks

        # Mock provider not found
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_get_provider_config(
                class_name='UnknownProvider',
                class_type='provider'
            )

        assert exc_info.value.status_code == 404
        assert 'not found' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_config_empty_preferences(self, registry_with_mocks, mock_asyncpg_pool):
        """Test retrieval when preferences column is empty/null."""
        reg = registry_with_mocks

        # Mock provider exists but has null preferences
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            None   # Preferences are null
        ])

        result = await reg.handle_get_provider_config(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.preferences.crypto is None

    @pytest.mark.asyncio
    async def test_update_config_success(self, registry_with_mocks, mock_asyncpg_pool):
        """Test successful update of provider preferences."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate, CryptoPreferences

        reg = registry_with_mocks

        # Mock provider exists and update succeeds
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            {'crypto': {'preferred_quote_currency': 'USDT'}}  # Updated preferences
        ])

        update = ProviderPreferencesUpdate(
            crypto=CryptoPreferences(preferred_quote_currency='USDT')
        )

        result = await reg.handle_update_provider_config(
            update=update,
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.preferences.crypto.preferred_quote_currency == 'USDT'

        # Verify provider existence check and update queries
        assert mock_asyncpg_pool.fetchval.call_count == 2

    @pytest.mark.asyncio
    async def test_update_config_not_found(self, registry_with_mocks, mock_asyncpg_pool):
        """Test 404 response when updating non-existent provider."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate, CryptoPreferences

        reg = registry_with_mocks

        # Mock provider doesn't exist
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=False)

        update = ProviderPreferencesUpdate(
            crypto=CryptoPreferences(preferred_quote_currency='USDT')
        )

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_provider_config(
                update=update,
                class_name='UnknownProvider',
                class_type='provider'
            )

        assert exc_info.value.status_code == 404
        assert 'not found' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_update_config_partial_update(self, registry_with_mocks, mock_asyncpg_pool):
        """Test partial update merges with existing preferences."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate, CryptoPreferences

        reg = registry_with_mocks

        # Mock provider exists and partial update
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            True,  # Provider exists
            {'crypto': {'preferred_quote_currency': 'USDC'}}  # Updated preferences
        ])

        update = ProviderPreferencesUpdate(
            crypto=CryptoPreferences(preferred_quote_currency='USDC')
        )

        result = await reg.handle_update_provider_config(
            update=update,
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.preferences.crypto.preferred_quote_currency == 'USDC'

    @pytest.mark.asyncio
    async def test_update_config_no_updates(self, registry_with_mocks, mock_asyncpg_pool):
        """Test 400 response when no updates are provided."""
        from quasar.services.registry.schemas import ProviderPreferencesUpdate

        reg = registry_with_mocks

        # Mock provider exists
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=True)

        update = ProviderPreferencesUpdate()  # Empty update

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_update_provider_config(
                update=update,
                class_name='TestProvider',
                class_type='provider'
            )

        assert exc_info.value.status_code == 400
        assert 'No preferences provided' in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_available_quote_currencies(self, registry_with_mocks, mock_asyncpg_pool):
        """Test retrieval of available quote currencies."""
        reg = registry_with_mocks

        # Mock database response with quote currencies
        mock_records = [
            MockRecord(quote_currency='USDC'),
            MockRecord(quote_currency='USDT'),
            MockRecord(quote_currency='USD'),
        ]
        mock_asyncpg_pool.fetch = AsyncMock(return_value=mock_records)

        result = await reg.handle_get_available_quote_currencies(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.available_quote_currencies == ['USDC', 'USDT', 'USD']

        # Verify correct query was called
        mock_asyncpg_pool.fetch.assert_called_once_with(
            "\n            SELECT DISTINCT quote_currency\n            FROM assets\n            WHERE class_name = $1\n              AND class_type = $2\n              AND asset_class_group = 'crypto'\n              AND quote_currency IS NOT NULL\n            ORDER BY quote_currency\n        ",
            'TestProvider',
            'provider'
        )

    @pytest.mark.asyncio
    async def test_get_available_quote_currencies_empty(self, registry_with_mocks, mock_asyncpg_pool):
        """Test empty result when no crypto assets exist."""
        reg = registry_with_mocks

        # Mock empty result
        mock_asyncpg_pool.fetch = AsyncMock(return_value=[])

        result = await reg.handle_get_available_quote_currencies(
            class_name='TestProvider',
            class_type='provider'
        )

        assert result.class_name == 'TestProvider'
        assert result.class_type == 'provider'
        assert result.available_quote_currencies == []
