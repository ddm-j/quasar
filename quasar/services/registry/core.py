"""Registry service core: code uploads, asset catalog, and mappings.

This module defines the Registry class which coordinates all registry operations
through handler mixins. The actual handler implementations are in the handlers/
subpackage, organized by domain:

- handlers/assets.py: Asset management handlers
- handlers/code.py: Code upload and management handlers
- handlers/config.py: Provider configuration handlers
- handlers/indices.py: Index management handlers
- handlers/mappings.py: Asset mapping handlers

Utility modules:
- utils/pagination.py: Cursor encoding/decoding
- utils/query_builder.py: Dynamic SQL filter building
"""

from typing import Optional, List, Dict
from pathlib import Path
import os
import asyncpg
import yaml

from quasar.lib.common.database_handler import DatabaseHandler
from quasar.lib.common.api_handler import APIHandler
from quasar.lib.common.context import SystemContext
from quasar.lib.common.enum_guard import validate_enums

# Handler mixins
from quasar.services.registry.handlers import (
    AssetHandlersMixin,
    CodeHandlersMixin,
    ConfigHandlersMixin,
    IndexHandlersMixin,
    MappingHandlersMixin,
    MembershipSyncResult,
    _weights_equal,
)

# Re-export for backward compatibility with tests
from quasar.services.registry.utils.pagination import (
    encode_cursor as _encode_cursor,
    decode_cursor as _decode_cursor,
)

# Schema imports for route response models
from quasar.services.registry.schemas import (
    ClassSummaryItem,
    ConfigSchemaResponse,
    FileUploadResponse, UpdateAssetsResponse, DeleteClassResponse,
    AssetResponse, AssetMappingCreateResponse, AssetMappingPaginatedResponse,
    AssetMappingResponse, SuggestionsResponse,
    AssetMappingRemapPreview, AssetMappingRemapResponse,
    ProviderPreferencesResponse, AvailableQuoteCurrenciesResponse,
    SecretKeysResponse, SecretsUpdateResponse,
    CommonSymbolResponse, CommonSymbolRenameResponse,
    IndexListResponse, IndexDetailResponse, IndexMembersResponse,
    IndexSyncResponse, IndexHistoryResponse, IndexItem,
)

from quasar.services.registry.matcher import IdentityMatcher
from quasar.services.registry.mapper import AutomatedMapper

import logging
logger = logging.getLogger(__name__)

# Re-export for backward compatibility
__all__ = [
    'Registry',
    'MembershipSyncResult',
    '_weights_equal',
    '_encode_cursor',
    '_decode_cursor',
]


class Registry(
    AssetHandlersMixin,
    CodeHandlersMixin,
    ConfigHandlersMixin,
    IndexHandlersMixin,
    MappingHandlersMixin,
    DatabaseHandler,
    APIHandler
):
    """Manage uploaded provider/broker code and asset mappings.

    This class combines multiple handler mixins to provide a complete
    registry service. Each mixin handles a specific domain area while
    sharing common resources (pool, matcher, mapper) through the base class.

    Handler Domains:
        - AssetHandlersMixin: Asset CRUD, updates, identity matching
        - CodeHandlersMixin: File upload, validation, code registration
        - ConfigHandlersMixin: Provider preferences, quote currencies
        - IndexHandlersMixin: Index management, membership sync
        - MappingHandlersMixin: Asset mapping suggestions and CRUD
    """

    name = "Registry"
    dynamic_provider = '/app/dynamic_providers'
    dynamic_broker = '/app/dynamic_brokers'
    system_context = SystemContext()
    enum_guard_mode = os.getenv("ENUM_GUARD_MODE", "off").lower()

    def __init__(
            self,
            dsn: str | None = None,
            pool: Optional[asyncpg.Pool] = None,
            refresh_seconds: int = 30,
            api_host: str = '0.0.0.0',
            api_port: int = 8080) -> None:
        """Create a Registry instance.

        Args:
            dsn (str | None): Database DSN for internal pool creation.
            pool (asyncpg.Pool | None): Existing pool to reuse.
            refresh_seconds (int): Reserved for future use.
            api_host (str): Host interface for the internal API.
            api_port (int): Port number for the internal API.
        """
        # Initialize Supers
        DatabaseHandler.__init__(self, dsn=dsn, pool=pool)
        APIHandler.__init__(self, api_host=api_host, api_port=api_port)

        # Initialize Matcher
        self.matcher = IdentityMatcher(dsn=dsn, pool=pool)

        # Initialize AutomatedMapper
        self.mapper = AutomatedMapper(dsn=dsn, pool=pool)

    def _setup_routes(self) -> None:
        """Define API routes for the Registry."""
        logger.info("Registry: Setting up API routes")

        # General Registry Routes (public API)
        self._api_app.router.add_api_route(
            '/api/registry/upload',
            self.handle_upload_file,
            methods=['POST'],
            response_model=FileUploadResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/update-assets',
            self.handle_update_assets,
            methods=['POST'],
            response_model=UpdateAssetsResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/update-all-assets',
            self.handle_update_all_assets,
            methods=['POST'],
            response_model=List[UpdateAssetsResponse]
        )
        self._api_app.router.add_api_route(
            '/api/registry/classes/summary',
            self.handle_get_classes_summary,
            methods=['GET'],
            response_model=List[ClassSummaryItem]
        )
        self._api_app.router.add_api_route(
            '/api/registry/delete',
            self.handle_delete_class,
            methods=['DELETE'],
            response_model=DeleteClassResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/assets',
            self.handle_get_assets,
            methods=['GET'],
            response_model=AssetResponse
        )

        # Asset Mapping Routes (public API)
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings',
            self.handle_create_asset_mapping,
            methods=['POST'],
            response_model=AssetMappingCreateResponse,
            status_code=201
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings',
            self.handle_get_asset_mappings,
            methods=['GET'],
            response_model=AssetMappingPaginatedResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings',
            self.handle_update_asset_mapping,
            methods=['PUT'],
            response_model=AssetMappingResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings',
            self.handle_delete_asset_mapping,
            methods=['DELETE'],
            status_code=204
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings/{common_symbol}',
            self.handle_get_asset_mappings_for_symbol,
            methods=['GET'],
            response_model=List[AssetMappingResponse]
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings/common-symbol/{symbol}/rename',
            self.handle_rename_common_symbol,
            methods=['PUT'],
            response_model=CommonSymbolRenameResponse
        )

        # Asset Re-mapping Routes (public API)
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings/re-map/preview',
            self.handle_remap_preview,
            methods=['GET'],
            response_model=AssetMappingRemapPreview
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings/re-map',
            self.handle_remap_assets,
            methods=['POST'],
            response_model=AssetMappingRemapResponse
        )

        # Common Symbols Routes (public API)
        # Note: Placed under asset-mappings/ since common symbols are derived from mappings
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings/common-symbols',
            self.handle_get_common_symbols,
            methods=['GET'],
            response_model=CommonSymbolResponse
        )

        # Asset Mapping Suggestions (public API)
        self._api_app.router.add_api_route(
            '/api/registry/asset-mapping-suggestions',
            self.handle_get_asset_mapping_suggestions,
            methods=['GET'],
            response_model=SuggestionsResponse
        )

        # Provider Configuration Routes (public API)
        self._api_app.router.add_api_route(
            '/api/registry/config',
            self.handle_get_provider_config,
            methods=['GET'],
            response_model=ProviderPreferencesResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/config',
            self.handle_update_provider_config,
            methods=['PUT'],
            response_model=ProviderPreferencesResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/config/schema',
            self.handle_get_config_schema,
            methods=['GET'],
            response_model=ConfigSchemaResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/config/available-quote-currencies',
            self.handle_get_available_quote_currencies,
            methods=['GET'],
            response_model=AvailableQuoteCurrenciesResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/config/secret-keys',
            self.handle_get_secret_keys,
            methods=['GET'],
            response_model=SecretKeysResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/config/secrets',
            self.handle_update_secrets,
            methods=['PATCH'],
            response_model=SecretsUpdateResponse
        )

        # Index Management Routes (public API)
        self._api_app.router.add_api_route(
            '/api/registry/indices',
            self.handle_get_indices,
            methods=['GET'],
            response_model=IndexListResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices',
            self.handle_create_user_index,
            methods=['POST'],
            response_model=IndexItem,
            status_code=201
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices/{index_name}',
            self.handle_get_index,
            methods=['GET'],
            response_model=IndexDetailResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices/{index_name}',
            self.handle_delete_index,
            methods=['DELETE'],
            status_code=204
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices/{index_name}/members',
            self.handle_get_index_members,
            methods=['GET'],
            response_model=IndexMembersResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices/{index_name}/members',
            self.handle_update_user_index_members,
            methods=['PUT'],
            response_model=IndexMembersResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices/{index_name}/sync',
            self.handle_sync_index,
            methods=['POST'],
            response_model=IndexSyncResponse
        )
        self._api_app.router.add_api_route(
            '/api/registry/indices/{index_name}/history',
            self.handle_get_index_history,
            methods=['GET'],
            response_model=IndexHistoryResponse
        )

    # OBJECT LIFECYCLE
    # ---------------------------------------------------------------------
    async def start(self) -> None:
        """Start the Registry."""
        # Start API
        await self.start_api_server()

        # Start Database
        await self.init_pool()
        self.matcher._pool = self.pool  # Share the initialized pool with the matcher
        self.mapper._pool = self.pool  # Share the initialized pool with the mapper
        await self._run_enum_guard()
        await self._seed_identity_manifests()

    async def stop(self) -> None:
        """Stop the Registry."""
        # Stop Database
        await self.close_pool()

        # Stop API
        await self.stop_api_server()
    # ---------------------------------------------------------------------

    async def _run_enum_guard(self) -> None:
        """Optional enum/runtime sanity check against DB lookup tables."""
        mode = self.enum_guard_mode
        if mode == "off":
            return
        strict = mode == "strict"
        await validate_enums(self.pool, strict=strict)

    async def _seed_identity_manifests(self) -> None:
        """Seed identity_manifest table with bundled manifests if empty.

        Loads YAML manifests from quasar/seeds/manifests/ and bulk inserts
        into the database. Idempotent - only seeds if table is empty.
        Uses prepared statements and transactions for efficiency.
        """
        try:
            # Check if table is already seeded
            count = await self.pool.fetchval("SELECT COUNT(*) FROM identity_manifest")
            if count > 0:
                logger.info("Identity manifests already seeded, skipping.")
                return

            # Locate manifests directory relative to this file
            manifests_dir = Path(__file__).parent.parent.parent / "seeds" / "manifests"
            if not manifests_dir.exists():
                logger.warning(f"Manifests directory not found: {manifests_dir}")
                return

            total_seeded = 0

            # Process each manifest file
            for manifest_file in sorted(manifests_dir.glob("*.yaml")):
                asset_class_group = manifest_file.stem  # 'crypto' or 'securities'

                if asset_class_group not in ['crypto', 'securities']:
                    logger.warning(f"Skipping unknown manifest file: {manifest_file.name}")
                    continue

                logger.info(f"Seeding {asset_class_group} manifest from {manifest_file}")

                try:
                    # Load YAML file
                    with open(manifest_file, 'r', encoding='utf-8') as f:
                        identities = yaml.safe_load(f) or []

                    if not isinstance(identities, list):
                        logger.error(f"Invalid manifest format in {manifest_file.name}: expected list")
                        continue

                    # Bulk insert using prepared statement
                    seeded_count = await self._bulk_insert_manifest(
                        identities,
                        asset_class_group,
                        source='bundled'
                    )
                    total_seeded += seeded_count
                    logger.info(f"Seeded {seeded_count} {asset_class_group} identities from {manifest_file.name}")

                except yaml.YAMLError as e:
                    logger.error(f"YAML parsing error in {manifest_file.name}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Failed to seed {asset_class_group} manifest: {e}", exc_info=True)
                    continue

            logger.info(f"Identity manifest seeding complete. Total identities seeded: {total_seeded}")

        except Exception as e:
            logger.error(f"Error during identity manifest seeding: {e}", exc_info=True)
            # Don't fail startup for seeding errors - manifests are optional
            pass

    async def _bulk_insert_manifest(
        self,
        identities: List[Dict],
        asset_class_group: str,
        source: str
    ) -> int:
        """Bulk insert identities into identity_manifest table.

        Uses conn.execute() directly following the savepoint pattern.
        Uses transactions for efficiency and atomicity.

        Args:
            identities: List of identity dicts with keys: figi (mapped to primary_id), symbol, name, exchange
            asset_class_group: 'securities' or 'crypto'
            source: Source identifier ('bundled', 'api_upload', etc.)

        Returns:
            Number of identities successfully inserted
        """
        if not identities:
            return 0

        insert_query = """
            INSERT INTO identity_manifest
            (primary_id, symbol, name, exchange, asset_class_group, source)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (primary_id, asset_class_group) DO NOTHING
        """

        inserted_count = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for identity in identities:
                    try:
                        # Validate required fields
                        # YAML manifests use 'figi' key which maps to primary_id column
                        primary_id = identity.get('figi')
                        symbol = identity.get('symbol')
                        name = identity.get('name')

                        if not primary_id or not symbol or not name:
                            logger.warning(
                                f"Skipping identity with missing required fields: {identity}"
                            )
                            continue

                        # Insert using conn.execute() following savepoint pattern
                        await conn.execute(
                            insert_query,
                            primary_id,
                            symbol,
                            name,
                            identity.get('exchange'),  # Can be None/null
                            asset_class_group,
                            source
                        )
                        inserted_count += 1

                    except Exception as e:
                        logger.warning(
                            f"Failed to insert identity {identity.get('symbol', 'unknown')}: {e}"
                        )
                        continue

        return inserted_count
