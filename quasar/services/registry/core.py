"""Registry service core: code uploads, asset catalog, and mappings."""

from typing import Optional, List, Dict, Any
import os, asyncpg, base64, hashlib, json
import aiohttp
from fastapi import HTTPException, UploadFile, File, Form, Depends, Query, Body
from fastapi.responses import Response
from urllib.parse import unquote_plus
from asyncpg.exceptions import UndefinedFunctionError

from quasar.lib.common.database_handler import DatabaseHandler
from quasar.lib.common.api_handler import APIHandler
from quasar.lib.common.context import SystemContext, DerivedContext
from quasar.services.registry.schemas import (
    ClassType, FileUploadResponse, UpdateAssetsResponse, ClassSummaryItem,
    DeleteClassResponse, AssetQueryParams, AssetResponse, AssetItem,
    AssetMappingCreate, AssetMappingResponse, AssetMappingUpdate,
    SuggestionsResponse, SuggestionItem
)

import logging
logger = logging.getLogger(__name__)


def _encode_cursor(score: float, src_sym: str, tgt_sym: str) -> str:
    """Encode pagination cursor as base64 JSON.

    Args:
        score: The score value of the last item.
        src_sym: Source symbol of the last item.
        tgt_sym: Target symbol of the last item.

    Returns:
        Base64-encoded cursor string.
    """
    return base64.urlsafe_b64encode(
        json.dumps([score, src_sym, tgt_sym]).encode()
    ).decode()


def _decode_cursor(cursor: str) -> tuple[float, str, str]:
    """Decode pagination cursor from base64 JSON.

    Args:
        cursor: Base64-encoded cursor string.

    Returns:
        Tuple of (score, source_symbol, target_symbol).

    Raises:
        ValueError: If cursor is malformed.
    """
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor))
        return (float(data[0]), str(data[1]), str(data[2]))
    except Exception as e:
        raise ValueError(f"Invalid cursor format: {e}")


class Registry(DatabaseHandler, APIHandler):
    """Manage uploaded provider/broker code and asset mappings."""
    name = "Registry"
    dynamic_provider = '/app/dynamic_providers'
    dynamic_broker = '/app/dynamic_brokers'
    system_context = SystemContext()

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
            response_model=AssetMappingResponse,
            status_code=201
        )
        self._api_app.router.add_api_route(
            '/api/registry/asset-mappings',
            self.handle_get_asset_mappings,
            methods=['GET'],
            response_model=List[AssetMappingResponse]
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

        # Asset Mapping Suggestions (public API)
        self._api_app.router.add_api_route(
            '/api/registry/asset-mapping-suggestions',
            self.handle_get_asset_mapping_suggestions,
            methods=['GET'],
            response_model=SuggestionsResponse
        )

    # OBJECT LIFECYCLE
    # ---------------------------------------------------------------------
    async def start(self) -> None:
        """
        Start the Registry.
        """
        # Start API
        await self.start_api_server()

        # Start Database
        await self.init_pool()

    async def stop(self) -> None:
        """
        Stop the Registry.
        """
        # Stop Database
        await self.close_pool()

        # Stop API
        await self.stop_api_server()
    # ---------------------------------------------------------------------

    # API ENDPOINTS
    # ---------------------------------------------------------------------
    # # CODE UPLOAD / REGISTRATION
    async def handle_upload_file(
        self,
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'"),
        file: UploadFile = File(...),
        secrets: str = Form(...)
    ) -> FileUploadResponse:
        """Upload custom provider/broker code and register it.

        Args:
            class_type (ClassType): Type of class being uploaded.
            file (UploadFile): Python file containing the class.
            secrets (str): Secrets payload for the class, encrypted before storage.

        Returns:
            FileUploadResponse: Status message and registered ID.
        """
        logger.info(f"Registry.handle_upload_file: Received POST request for {class_type} upload")
        
        if class_type not in ['provider', 'broker']:
            logger.warning(f"Invalid class type '{class_type}' in upload request.")
            raise HTTPException(status_code=400, detail="Invalid class type in URL, must be 'provider' or 'broker'")

        original_filename = file.filename
        if not original_filename:
            logger.warning("Upload request missing filename.")
            raise HTTPException(status_code=400, detail="No file uploaded or missing filename")
        
        logger.info(f"Received {class_type} upload with filename: {original_filename}")

        # Check filetype
        if not original_filename.lower().endswith('.py'):
            logger.warning(f"Invalid file type '{original_filename}'. Only .py files are allowed.")
            raise HTTPException(status_code=415, detail="Invalid file type, only .py files are allowed")
        
        # Generate a unique filename
        unique_id = base64.urlsafe_b64encode(os.urandom(32))[:8].decode('utf-8')
        fname, ext = os.path.splitext(original_filename)
        unique_filename = f"{unique_id}_{fname}.py"
        logger.info(f"Generated unique filename: {unique_filename}")

        # Storage Directory
        try:
            storage_dir = getattr(self, f'dynamic_{class_type}')
        except Exception as e:
            logger.error(f"Error accessing storage directory: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal server error")

        # Full File Path
        FILE_PATH = os.path.join(storage_dir, unique_filename)

        # Make sure file doesn't already exist
        if os.path.exists(FILE_PATH):
            logger.warning(f"File {FILE_PATH} already exists.")
            raise HTTPException(status_code=500, detail="File already exists, developers need to check unique ID generation")

        # Compute File Hash and read file content
        file_hash_object = hashlib.sha256()
        file_chunks = []
        try:
            content = await file.read()
            total_size = len(content)
            if total_size == 0:
                logger.warning("Uploaded file is empty.")
                raise HTTPException(status_code=400, detail="Uploaded file is empty")
            
            file_hash_object.update(content)
            file_chunks.append(content)
            HASH_BYTES = file_hash_object.digest()
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error reading file: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail="Failed to read uploaded file")

        # Convert secrets string to bytes
        try:
            secrets_bytes = secrets.encode('utf-8') if isinstance(secrets, str) else secrets
        except Exception as e:
            logger.error(f"Error processing secrets: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail="Invalid secrets format")

        # Encrypt Secrets
        try:
            NONCE, CIPHERTEXT = self.system_context.create_context_data(HASH_BYTES, secrets_bytes)
        except Exception as e:
            logger.warning(f"Error encrypting secrets: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to encrypt secrets")

        # WRITE FILE
        try:
            with open(FILE_PATH, 'wb') as f:
                f.write(b''.join(file_chunks))
            logger.info(f"File {FILE_PATH} written successfully.")
        except Exception as e:
            logger.error(f"Error writing file {FILE_PATH}: {e}", exc_info=True)
            if os.path.exists(FILE_PATH):
                os.remove(FILE_PATH)
            raise HTTPException(status_code=500, detail="Failed to write file")

        # VALIDATE FILE
        validation_endpoints = {
            'provider': 'http://datahub:8080/internal/provider/validate',
            'broker': 'http://portfoliomanager:8082/internal/broker/validate'
        }
        validation_url = validation_endpoints[class_type]
        payload = {
            'file_path': FILE_PATH
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(validation_url, json=payload) as response:
                    if response.status != 200:
                        logger.warning(f"Validation failed for {class_type} file: {response.status} from {validation_url}")
                        os.remove(FILE_PATH)
                        try:
                            # Attempt to parse DataHub's response as JSON
                            error_payload = await response.json()
                            # If DataHub sent a JSON error, forward it with DataHub's status
                            raise HTTPException(status_code=response.status, detail=error_payload.get('error', 'Validation failed'))
                        except HTTPException:
                            raise
                        except Exception as e_parse: # Includes JSONDecodeError, ContentTypeError
                            # DataHub did not send valid JSON. Log its actual response.
                            error_body_text = await response.text() # Get raw text
                            logger.error(f"DataHub validation error (status {response.status}) was not valid JSON. Body: '{error_body_text[:200]}...'. Parse error: {e_parse}", exc_info=False)
                            # Return a structured JSON error from Registry
                            raise HTTPException(
                                status_code=502,
                                detail=f"Validation service returned an invalid or non-JSON response. Status: {response.status}"
                            )
                    else:
                        response_json = await response.json()
                        NAME = response_json.get('class_name')
                        SUBCLASS = response_json.get('subclass_type')
                        if not NAME:
                            logger.warning(f"Validation response missing class name for {class_type} file.")
                            os.remove(FILE_PATH)
                            raise HTTPException(status_code=400, detail="Validation response missing class name")
                        if not SUBCLASS:
                            logger.warning(f"Validation response missing subclass type for {class_type} file.")
                            os.remove(FILE_PATH)
                            raise HTTPException(status_code=400, detail="Validation response missing subclass type")
                        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error during validation request: {e}", exc_info=True)
            if os.path.exists(FILE_PATH):
                os.remove(FILE_PATH)
            raise HTTPException(status_code=500, detail="Validation request failed")

        # WRITE TO DB
        registered_id = await self._register_code(
            class_name=NAME,
            class_type=class_type,
            class_subtype=SUBCLASS,
            file_path=FILE_PATH,
            file_hash=HASH_BYTES,
            nonce=NONCE,
            ciphertext=CIPHERTEXT
        )

        return FileUploadResponse(
            status=f"File {unique_filename} uploaded successfully. Registered ID: {registered_id}"
        )

    async def _register_code(
            self,
            class_name: str,
            class_type: str,
            class_subtype: str,
            file_path: str,
            file_hash: bytes,
            nonce: bytes,
            ciphertext: bytes) -> int | None:
        """Persist uploaded code metadata and encrypted secrets.

        Args:
            class_name (str): Provider/broker class name.
            class_type (str): ``provider`` or ``broker``.
            class_subtype (str): Specific subclass type.
            file_path (str): Stored file path.
            file_hash (bytes): SHA256 hash of the file contents.
            nonce (bytes): Encryption nonce.
            ciphertext (bytes): Encrypted secrets payload.

        Returns:
            int | None: Registered row id or ``None`` when duplicate.
        """
        sql_insert_query = """
        INSERT INTO code_registry
        (class_name, class_type, class_subtype, file_path, file_hash, nonce, ciphertext)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id;
        """
        try:
            registered_id = await self.pool.fetchval(
                sql_insert_query,
                class_name,
                class_type,
                class_subtype,
                file_path,
                file_hash,
                nonce,
                ciphertext
            )
            logger.info(f"Registered {class_type}.{class_subtype} '{class_name}' with ID {registered_id}.")
            return registered_id
        except asyncpg.exceptions.UniqueViolationError as uve:
            logger.warning(
                f"Registry._register_code: Failed to register code for class '{class_name}' ({class_type}) "
                f"due to unique constraint violation. Constraint: {uve.constraint_name}, Detail: {uve.detail}. "
            )
            return None
        except Exception as e:
            logger.error(
                f"Registry._register_code: An unexpected error occurred while registering code for class '{class_name}' ({class_type}): {e}",
                exc_info=True
            )
            return None

    # # DELETE REGISTERED CLASS
    async def handle_delete_class(
        self,
        class_type: ClassType = Query(..., description="Class type: 'provider' or 'broker'"),
        class_name: str = Query(..., description="Class name (provider/broker name)")
    ) -> DeleteClassResponse:
        """Delete a registered provider or broker and its stored file.

        Args:
            class_type (ClassType): ``provider`` or ``broker``.
            class_name (str): Registered class name.

        Returns:
            DeleteClassResponse: Deletion status and file removal outcome.
        """
        # Verify if the class_name and class_type are registered
        query_file_path = """
            SELECT file_path FROM code_registry WHERE class_name = $1 AND class_type = $2;
            """
        file_path_to_delete = None
        try:
            file_path_to_delete = await self.pool.fetchval(query_file_path, class_name, class_type)
            if not file_path_to_delete:
                logger.warning(f"Registry.handle_delete_class: Class '{class_name}' ({class_type}) is not registered.")
                raise HTTPException(status_code=404, detail=f"Class '{class_name}' ({class_type}) is not registered.")
        except HTTPException:
            raise
        except Exception as e_db_check:
            logger.error(f"Registry.handle_delete_class: Error checking registration for {class_name} ({class_type}): {e_db_check}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while checking registration")
        
        # Delete the class from the database
        delete_query = """
            DELETE FROM code_registry WHERE class_name = $1 AND class_type = $2 RETURNING id;
            """
        deleted_id = None
        try:
            deleted_id = await self.pool.fetchval(delete_query, class_name, class_type)
            if not deleted_id:
                logger.warning(f"Registry.handle_delete_class: Class '{class_name}' ({class_type}) was not found for deletion.")
                raise HTTPException(status_code=404, detail=f"Class '{class_name}' ({class_type}) was not found for deletion.")
        except HTTPException:
            raise
        except Exception as e_db_delete:
            logger.error(f"Registry.handle_delete_class: Error deleting class {class_name} ({class_type}): {e_db_delete}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while deleting class")
        
        # Delete file from filesystem
        file_deleted_success = False
        if file_path_to_delete:
            try:
                if os.path.exists(file_path_to_delete):
                    os.remove(file_path_to_delete)
                    logger.info(f"Registry.handle_delete_class: Successfully deleted file {file_path_to_delete}.")
                    file_deleted_success = True
                else:
                    logger.warning(f"Registry.handle_delete_class: File {file_path_to_delete} does not exist, cannot delete.")
                    file_deleted_success = True
            except Exception as e_file_delete:
                logger.error(f"Registry.handle_delete_class: Error deleting file {file_path_to_delete}: {e_file_delete}", exc_info=True)
                # Return success for DB deletion but note file deletion error
                return DeleteClassResponse(
                    message=f"Class '{class_name}' ({class_type}) deleted from database, but failed to delete associated file: {file_path_to_delete}. Error: {e_file_delete}",
                    class_name=class_name,
                    class_type=class_type,
                    file_deleted=False
                )

        return DeleteClassResponse(
            message=f"Class '{class_name}' ({class_type}) deleted successfully.",
            class_name=class_name,
            class_type=class_type,
            file_deleted=file_deleted_success
        )

    # # ASSET UPDATE / REGISRATION
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
        """Trigger asset updates for all registered providers and brokers."""
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

        # Update assets for each provider
        stats_list = []
        for provider in providers:
            class_name = provider['class_name']
            class_type = provider['class_type']
            stats = await self._update_assets_for_provider(class_name, class_type)
            stats_list.append(UpdateAssetsResponse(**stats))

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
            'status': 200
        }

        # Fetch available symbols from DataHub
        datahub_url = f'http://datahub:8080/internal/providers/available-symbols'
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(datahub_url, params={'provider_name': class_name}) as response:
                    if response.status == 200:
                        symbol_info_list = await response.json()
                        if not isinstance(symbol_info_list, list):
                            logger.warning(f"Invalid response format from DataHub (not a list)")
                            stats['error'] = "Invalid response format from DataHub"
                            stats['status'] = 500
                            return stats
                        stats['total_symbols'] = len(symbol_info_list)
                        logger.info(f"Registry._update_assets_for_provider: Received {stats['total_symbols']} symbols from DataHub for {class_name}.")
                    elif response.status == 404: # Provider not found/loaded in DataHub
                        logger.warning(f"Registry._update_assets_for_provider: DataHub reported provider {class_name} not found or not loaded. This might indicate an issue if it's registered.")
                        stats["error"] = f"DataHub: Provider {class_name} not found/loaded"
                        stats["status"] = 404
                        return stats
                    elif response.status == 501: # Not Implemented by provider in DataHub
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
            logger.info(f"Registry._update_assets_for_provider: No symbols returned or fetched from DataHub for provider {class_name}.")
            stats["message"] = "No symbols returned from DataHub"
            stats["status"] = 204
            return stats

        # Upsert symbols into the database
        upsert_query = """
                        INSERT INTO assets (
                            class_name, class_type, external_id, isin, symbol, 
                            name, exchange, asset_class, base_currency, quote_currency, country
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (class_name, class_type, symbol) DO UPDATE SET
                            external_id = EXCLUDED.external_id,
                            isin = EXCLUDED.isin,
                            name = EXCLUDED.name,
                            exchange = EXCLUDED.exchange,
                            asset_class = EXCLUDED.asset_class,
                            base_currency = EXCLUDED.base_currency,
                            quote_currency = EXCLUDED.quote_currency,
                            country = EXCLUDED.country
                        RETURNING xmax; 
                    """      
                    # May want to add updated_at = CURRENT_TIMESTAMP (later)
                            # updated_at = CURRENT_TIMESTAMP -- Assuming you have an updated_at column
        processed_symbols = set()
        
        async with self.pool.acquire() as conn:
            prepared_upsert = await conn.prepare(upsert_query)
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

                record_values = (
                    class_name,
                    class_type,
                    symbol_info.get('provider_id'),
                    symbol_info.get('isin'),
                    symbol,
                    symbol_info.get('name'),
                    symbol_info.get('exchange'),
                    symbol_info.get('asset_class'),
                    symbol_info.get('base_currency'),
                    symbol_info.get('quote_currency'),
                    symbol_info.get('country')
                )

                try:
                    result = await prepared_upsert.fetchrow(*record_values)
                    if result:
                        if result['xmax'] == 0:
                            stats['added_symbols'] += 1
                        else:
                            stats['updated_symbols'] += 1
                    else:
                        logger.warning(f"Failed to upsert symbol {symbol} for {class_name}.")
                        stats['failed_symbols'] += 1
                except Exception as e_upsert:
                    logger.error(f"Registry._update_assets_for_provider: Error upserting symbol {symbol} for {class_name}: {e_upsert}", exc_info=True)
                    stats['failed_symbols'] += 1

        stats['processed_symbols'] = stats['added_symbols'] + \
                                stats['updated_symbols'] + \
                                stats['failed_symbols']
        logger.info(f"Registry._update_assets_for_provider: Asset update summary for {class_name} ({class_type}):" \
                    f"Added={stats['added_symbols']}, Updated={stats['updated_symbols']}, Failed={stats['failed_symbols']}")
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
                'class_name', 'class_type', 'symbol', 'name', 
                'exchange', 'asset_class', 'base_currency', 'quote_currency', 'country'
            ]
            
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
            filters = []
            db_params: List[Any] = []
            param_idx = 1

            def add_filter(column: str, value: Optional[str], partial_match: bool = False, is_list: bool = False):
                nonlocal param_idx
                if value is not None and value.strip() != "":
                    decoded_value = unquote_plus(value.strip())
                    if is_list:
                        # Assuming comma-separated list for IN clause
                        list_values = [v.strip() for v in decoded_value.split(',')]
                        if list_values:
                            placeholders = ', '.join([f'${param_idx + i}' for i in range(len(list_values))])
                            filters.append(f"{column} IN ({placeholders})")
                            db_params.extend(list_values)
                            param_idx += len(list_values)
                    elif partial_match:
                        filters.append(f"LOWER({column}) LIKE LOWER(${param_idx})")
                        db_params.append(f"%{decoded_value}%")
                        param_idx += 1
                    else: # Exact match
                        filters.append(f"{column} = ${param_idx}")
                        db_params.append(decoded_value)
                        param_idx += 1
            
            add_filter('class_name', params.class_name_like, partial_match=True)
            add_filter('class_type', params.class_type)
            add_filter('asset_class', params.asset_class) # Exact match for dropdown
            add_filter('base_currency', params.base_currency_like, partial_match=True)
            add_filter('quote_currency', params.quote_currency_like, partial_match=True)
            add_filter('country', params.country_like, partial_match=True)
            add_filter('symbol', params.symbol_like, partial_match=True)
            add_filter('name', params.name_like, partial_match=True)
            add_filter('exchange', params.exchange_like, partial_match=True)

            where_clause = " AND ".join(filters) if filters else "TRUE"

            # Build queries
            select_columns = "id, class_name, class_type, external_id, isin, symbol, name, exchange, asset_class, base_currency, quote_currency, country"
            
            data_query = f"""
                SELECT {select_columns}
                FROM assets
                WHERE {where_clause}
                ORDER BY {order_by_sql}
                LIMIT ${param_idx} OFFSET ${param_idx + 1};
            """
            count_query = f"""
                SELECT COUNT(*) as total_items
                FROM assets
                WHERE {where_clause};
            """
            
            data_params = db_params + [limit, offset]
            count_params = db_params # Count query doesn't use limit/offset

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

    # # GET REGISTERED CLASSES
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
                return [] # Return empty list if none found

            for record in records:
                classes_summary.append(ClassSummaryItem(**dict(record))) # Convert asyncpg.Record to dict then to Pydantic model

            logger.info(f"Registry.handle_get_classes_summary: Returning summary for {len(classes_summary)} classes.")
            return classes_summary

        except Exception as e_db_fetch:
            logger.error(f"Registry.handle_get_classes_summary: Error fetching classes summary: {e_db_fetch}", exc_info=True)
            raise HTTPException(status_code=500, detail="Database error while fetching classes summary")

    # # ASSET MAPPINGS
    async def handle_create_asset_mapping(self, mapping: AssetMappingCreate) -> AssetMappingResponse:
        """Create a mapping between a common symbol and provider-specific symbol.

        Args:
            mapping (AssetMappingCreate): Mapping payload.

        Returns:
            AssetMappingResponse: Created mapping.
        """
        insert_query = """
            INSERT INTO asset_mapping (common_symbol, class_name, class_type, class_symbol, is_active)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING common_symbol, class_name, class_type, class_symbol, is_active;
        """
        try:
            new_mapping = await self.pool.fetchrow(
                insert_query,
                mapping.common_symbol,
                mapping.class_name,
                mapping.class_type,
                mapping.class_symbol,
                mapping.is_active
            )
            if new_mapping:
                logger.info(f"Registry.handle_create_asset_mapping: Successfully created asset mapping: {dict(new_mapping)}")
                return AssetMappingResponse(**dict(new_mapping))
            else:
                # This case should ideally not be reached if INSERT RETURNING is used and no error occurs
                logger.error("Registry.handle_create_asset_mapping: Failed to create asset mapping, no record returned.")
                raise HTTPException(status_code=500, detail="Failed to create asset mapping")

        except HTTPException:
            raise
        except asyncpg.exceptions.ForeignKeyViolationError as fke:
            # This error means that the (class_name, class_type) does not exist in code_registry
            # OR (class_name, class_type, class_symbol) does not exist in assets.
            constraint_name = fke.constraint_name
            detail = fke.detail
            logger.warning(
                f"Registry.handle_create_asset_mapping: Foreign key violation. "
                f"Constraint: {constraint_name}, Detail: {detail}."
            )
            error_message = "Failed to create mapping due to missing related entity. "
            if constraint_name == 'fk_asset_mapping_class_name':
                error_message += f"The class '{mapping.class_name}' ({mapping.class_type}) is not registered."
            elif constraint_name == 'fk_asset_mapping_to_assets':
                error_message += f"The asset '{mapping.class_symbol}' for class '{mapping.class_name}' ({mapping.class_type}) does not exist."
            else:
                error_message += "A referenced entity does not exist."
            
            raise HTTPException(status_code=404, detail=error_message)
        except asyncpg.exceptions.UniqueViolationError as uve:
            # This error means the mapping would violate a unique constraint.
            constraint_name = uve.constraint_name
            detail = uve.detail
            logger.warning(
                f"Registry.handle_create_asset_mapping: Unique constraint violation. "
                f"Constraint: {constraint_name}, Detail: {detail}."
            )
            error_message = "Failed to create mapping due to a conflict. "
            if constraint_name == 'asset_mapping_pkey':
                error_message += f"The provider symbol '{mapping.class_symbol}' for class '{mapping.class_name}' ({mapping.class_type}) is already mapped."
            elif constraint_name == 'uq_common_per_class':
                error_message += f"The common symbol '{mapping.common_symbol}' is already mapped for class '{mapping.class_name}' ({mapping.class_type})."
            else:
                error_message += "This mapping would create a duplicate entry."

            raise HTTPException(status_code=409, detail=error_message)
        except Exception as e:
            logger.error(f"Registry.handle_create_asset_mapping: Unexpected error creating asset mapping: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected error occurred")

    async def handle_get_asset_mappings(
        self,
        common_symbol: Optional[str] = Query(None),
        class_name: Optional[str] = Query(None),
        class_type: Optional[ClassType] = Query(None),
        class_symbol: Optional[str] = Query(None),
        is_active: Optional[bool] = Query(None)
    ) -> List[AssetMappingResponse]:
        """List asset mappings with optional filters.

        Args:
            common_symbol (str | None): Common symbol filter.
            class_name (str | None): Provider/broker name filter.
            class_type (ClassType | None): Provider or broker filter.
            class_symbol (str | None): Provider-specific symbol filter.
            is_active (bool | None): Active flag filter.

        Returns:
            list[AssetMappingResponse]: Matching mappings.
        """
        logger.info("Registry.handle_get_asset_mappings: Received request for asset mappings.")

        # Build the query
        query_base = "SELECT common_symbol, class_name, class_type, class_symbol, is_active FROM asset_mapping"
        conditions = []
        params = []
        param_idx = 1

        if common_symbol is not None:
            conditions.append(f"common_symbol = ${param_idx}")
            params.append(common_symbol)
            param_idx += 1
        if class_name is not None:
            conditions.append(f"class_name = ${param_idx}")
            params.append(class_name)
            param_idx += 1
        if class_type is not None:
            conditions.append(f"class_type = ${param_idx}")
            params.append(class_type)
            param_idx += 1
        if class_symbol is not None:
            conditions.append(f"class_symbol = ${param_idx}")
            params.append(class_symbol)
            param_idx += 1
        if is_active is not None:
            conditions.append(f"is_active = ${param_idx}")
            params.append(is_active)
            param_idx += 1

        if conditions:
            query_base += " WHERE " + " AND ".join(conditions)
        
        query_base += " ORDER BY class_name, class_type, common_symbol, class_symbol;"

        try:
            mappings_records = await self.pool.fetch(query_base, *params)
            mappings_list = [AssetMappingResponse(**dict(record)) for record in mappings_records]
            
            logger.info(f"Registry.handle_get_asset_mappings: Returning {len(mappings_list)} asset mappings.")
            return mappings_list

        except Exception as e:
            logger.error(f"Registry.handle_get_asset_mappings: Error fetching asset mappings: {e}", exc_info=True)
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
                cursor_score, cursor_src_sym, cursor_tgt_sym = _decode_cursor(cursor)
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
                CASE WHEN t_isin IS NOT NULL AND s_isin = t_isin THEN 70 ELSE 0 END +
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
                s.isin AS s_isin, t.isin AS t_isin,
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
                  AND {unmapped_filter}
            """

            union_query = f"""
                WITH src AS ({src_cte}),
                     tgt AS ({tgt_cte}),
                matched AS (
                    -- ISIN matches (indexed)
                    SELECT {select_cols}
                    FROM src s JOIN tgt t ON s.isin = t.isin
                    WHERE s.isin IS NOT NULL AND {asset_class_clause}

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
                        COALESCE(t_isin IS NOT NULL AND s_isin = t_isin, FALSE) AS isin_match,
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
                           COALESCE(tm.common_symbol, d.sym_norm_root) AS proposed_common_symbol,
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
                        proposed_common_symbol, score,
                        isin_match, external_id_match, norm_match,
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
            items.append(SuggestionItem(
                source_class=record["source_class"],
                source_type=record["source_type"],
                source_symbol=record["source_symbol"],
                source_name=record["source_name"],
                target_class=record["target_class"],
                target_type=record["target_type"],
                target_symbol=record["target_symbol"],
                target_name=record["target_name"],
                proposed_common_symbol=record["proposed_common_symbol"],
                score=float(record["score"]),
                isin_match=record["isin_match"],
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
            next_cursor = _encode_cursor(last.score, last.source_symbol, last.target_symbol)

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
