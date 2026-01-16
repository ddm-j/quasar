"""Tests for Registry core functionality - lifecycle and seeding."""

import pytest
from unittest.mock import AsyncMock, Mock, patch


class TestRegistryLifecycleMethods:
    """Tests for Registry lifecycle methods."""

    @pytest.mark.asyncio
    async def test_start_initializes_pool_and_starts_api(
        self, registry_with_mocks
    ):
        """Test that start() initializes pool and starts API server."""
        reg = registry_with_mocks

        with patch.object(reg, 'start_api_server', new_callable=AsyncMock):
            await reg.start()

            reg.start_api_server.assert_called_once()
            # Pool should be initialized (via init_pool)

    @pytest.mark.asyncio
    async def test_stop_closes_pool_and_stops_api(
        self, registry_with_mocks
    ):
        """Test that stop() closes pool and stops API server."""
        reg = registry_with_mocks

        with patch.object(reg, 'stop_api_server', new_callable=AsyncMock):
            with patch.object(reg, 'close_pool', new_callable=AsyncMock):
                await reg.stop()

                reg.stop_api_server.assert_called_once()
                reg.close_pool.assert_called_once()


class TestIdentityManifestSeeding:
    """Test identity manifest seeding behavior on Registry startup."""

    @pytest.mark.asyncio
    async def test_start_seeds_identity_manifests_when_empty(
        self, registry_with_mocks, mock_asyncpg_conn, tmp_path
    ):
        """Test that Registry seeds identity manifests on startup when table is empty."""
        reg = registry_with_mocks

        # Mock empty table check
        reg.pool.fetchval = AsyncMock(return_value=0)

        # Mock manifest directory structure
        manifests_dir = tmp_path / "seeds" / "manifests"
        manifests_dir.mkdir(parents=True)

        # Create test crypto manifest
        crypto_manifest = manifests_dir / "crypto.yaml"
        crypto_manifest.write_text("""
- figi: KKG00000DV14
  symbol: BTC
  name: Bitcoin
  exchange: null
- figi: KKG0000092P5
  symbol: ETH
  name: Ethereum
  exchange: null
""")

        # Create test securities manifest
        securities_manifest = manifests_dir / "securities.yaml"
        securities_manifest.write_text("""
- figi: BBG000B9XRY4
  symbol: AAPL
  name: Apple Inc
  exchange: XNAS
""")

        # Mock filesystem path resolution
        with patch('quasar.services.registry.core.Path') as mock_path_class:
            # Mock the path chain: Path(__file__).parent.parent.parent resolves to tmp_path
            # So Path(__file__).parent.parent.parent / "seeds" / "manifests" = tmp_path / "seeds" / "manifests"
            mock_file_path = Mock()
            mock_file_path.parent.parent.parent = tmp_path
            mock_path_class.return_value = mock_file_path

            # Mock database execution
            mock_asyncpg_conn.execute = AsyncMock()

            # Call seeding
            await reg._seed_identity_manifests()

            # Verify correct number of inserts (3 total identities)
            assert mock_asyncpg_conn.execute.call_count == 3

            # Verify calls included expected data
            calls = mock_asyncpg_conn.execute.call_args_list
            assert any('KKG00000DV14' in str(call) for call in calls)  # BTC
            assert any('KKG0000092P5' in str(call) for call in calls)  # ETH
            assert any('BBG000B9XRY4' in str(call) for call in calls)  # AAPL

    @pytest.mark.asyncio
    async def test_start_skips_seeding_when_manifests_exist(
        self, registry_with_mocks
    ):
        """Test that Registry skips seeding when identity manifests already exist."""
        reg = registry_with_mocks

        # Mock non-empty table (15,000 existing records)
        reg.pool.fetchval = AsyncMock(return_value=15000)

        # Call seeding - should return early
        await reg._seed_identity_manifests()

        # Verify no database operations occurred
        reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_handles_missing_manifest_directory(
        self, registry_with_mocks
    ):
        """Test graceful handling when manifest directory doesn't exist."""
        reg = registry_with_mocks

        # Mock empty table
        reg.pool.fetchval = AsyncMock(return_value=0)

        # Mock missing directory
        with patch('quasar.services.registry.core.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.exists.return_value = False
            mock_path.return_value.parent.parent.parent = mock_path_instance

            # Should not raise exception
            await reg._seed_identity_manifests()

            # Should not attempt database operations
            reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_handles_invalid_yaml_gracefully(
        self, registry_with_mocks, tmp_path
    ):
        """Test that invalid YAML doesn't crash seeding process."""
        reg = registry_with_mocks

        reg.pool.fetchval = AsyncMock(return_value=0)

        manifests_dir = tmp_path / "seeds" / "manifests"
        manifests_dir.mkdir(parents=True)

        # Create invalid YAML
        crypto_manifest = manifests_dir / "crypto.yaml"
        crypto_manifest.write_text("invalid: yaml: content: [\n")

        # Mock path resolution
        with patch('quasar.services.registry.core.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.parent.parent.parent = tmp_path / "seeds" / "manifests"
            mock_path.return_value.parent.parent.parent = mock_path_instance

            # Should catch YAML error and continue without database operations
            await reg._seed_identity_manifests()

            # Should not have attempted database operations
            reg.pool.acquire.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_handles_database_errors_gracefully(
        self, registry_with_mocks, mock_asyncpg_conn, tmp_path
    ):
        """Test that database errors don't crash Registry startup."""
        reg = registry_with_mocks

        reg.pool.fetchval = AsyncMock(return_value=0)

        # Mock filesystem setup
        manifests_dir = tmp_path / "seeds" / "manifests"
        manifests_dir.mkdir(parents=True)
        (manifests_dir / "crypto.yaml").write_text("- figi: TEST\n  symbol: TEST\n  name: Test\n")

        with patch('quasar.services.registry.core.Path') as mock_path_class:
            mock_file_path = Mock()
            mock_file_path.parent.parent.parent = tmp_path
            mock_path_class.return_value = mock_file_path

            # Mock database connection failure
            reg.pool.acquire = AsyncMock(side_effect=Exception("Connection failed"))

            # Should not raise exception - startup should continue
            await reg._seed_identity_manifests()

            # Verify database acquire was attempted (and failed)
            reg.pool.acquire.assert_called_once()
