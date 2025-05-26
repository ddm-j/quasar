from typing import Optional, List, Dict, Any
import os, asyncpg, base64, hashlib
import aiohttp
from aiohttp import web
import aiohttp_cors


from quasar.common.database_handler import DatabaseHandler
from quasar.common.api_handler import APIHandler
from quasar.common.context import SystemContext, DerivedContext

import logging
logger = logging.getLogger(__name__)

class Registry(DatabaseHandler, APIHandler):
    """
    A class that serves as a registry for the Quasar framework.
    Both registering/unregistering new data providers/brokers, registering assets, and 
    managing asset mappings between data providers and brokers.
    """
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

        # Initialize Supers
        DatabaseHandler.__init__(self, dsn=dsn, pool=pool)
        APIHandler.__init__(self, api_host=api_host, api_port=api_port) 


    def _setup_routes(self) -> None:
        """Define API routes for the Registry."""
        logger.info("Registry: Setting up API routes")

        # General Registry Routes
        route_upload = self._api_app.router.add_post(
            '/internal/{class_type}/upload',
            self._handle_upload_file
        )
        route_update_assets = self._api_app.router.add_post(
            '/internal/{class_type}/{class_name}/update-assets',
            self._handle_update_assets
        )
        route_update_all_assets = self._api_app.router.add_post(
            '/internal/update-all-assets',
            self._handle_update_all_assets
        )
        route_classes_summary = self._api_app.router.add_get(
            '/internal/classes/summary',
            self._handle_get_classes_summary
        )
        route_delete_class = self._api_app.router.add_delete(
            '/internal/delete/{class_type}/{class_name}',
            self._handle_delete_class
        )

        # Asset Mapping Routes
        route_create_asset_mapping = self._api_app.router.add_post(
            '/internal/asset-mappings',
            self._handle_create_asset_mapping
        )
        route_get_asset_mappings = self._api_app.router.add_get(
            '/internal/asset-mappings',
            self._handle_get_asset_mappings
        )
        route_update_asset_mapping = self._api_app.router.add_put(
            '/internal/asset-mappings/{class_name}/{class_type}/{class_symbol}',
            self._handle_update_asset_mapping
        )
        route_delete_asset_mapping = self._api_app.router.add_delete(
            '/internal/asset-mappings/{class_name}/{class_type}/{class_symbol}',
            self._handle_delete_asset_mapping
        )

        self._cors.add(route_upload)
        self._cors.add(route_update_assets)
        self._cors.add(route_update_all_assets)
        self._cors.add(route_classes_summary)
        self._cors.add(route_delete_class)
        self._cors.add(route_create_asset_mapping)
        self._cors.add(route_get_asset_mappings)
        self._cors.add(route_update_asset_mapping)
        self._cors.add(route_delete_asset_mapping)

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
    async def _handle_upload_file(self, request: web.Request) -> web.Response:
        """
        Custom code upload endpoint
        """
        logger.info(f"Registry._upload_file: Received POST request for {request.path_qs}")
        # Get Type of Custom Class
        class_type = request.match_info.get('class_type')
        if class_type not in ['provider', 'broker']:
            logger.warning(f"Invalid class type '{class_type}' in upload request.")
            return web.json_response(
                {"error": "Invalid class type in URL, mut be 'provider' or 'broker'"},
                status=400
            )

        # LOAD FILE DATA
        try:
            reader = await request.multipart()
            field = await reader.next()
        except Exception as e:
            logger.error(f"Error reading multipart request: {e}", exc_info=True)
            return web.json_response(
                {"error": "Failed to read multipart request"},
                status=400
            )
        if field is None or not hasattr(field, 'filename') or not field.filename:
            logger.warning("Upload request missing file field or filename.")
            return web.json_response(
                {"error": "No file uploaded or missing file field or filename"},
                status=400
            )
        original_filename = field.filename
        logger.info(f"Received {class_type} upload with filename: {original_filename}")

        # Check filetype
        if not original_filename.lower().endswith('.py'):
            logger.warning(f"Invalid file type '{original_filename}'. Only .py files are allowed.")
            return web.json_response(
                {"error": "Invalid file type, only .py files are allowed"},
                status=415
            )
        
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
            return web.json_response(
                {"error": "Internal server error"},
                status=500
            )

        # Full File Path
        FILE_PATH = os.path.join(storage_dir, unique_filename)

        # Make sure file doesn't already exist
        if os.path.exists(FILE_PATH):
            logger.warning(f"File {FILE_PATH} already exists.")
            return web.json_response(
                {"error": "File {file_path} already exists, developers need to check unique ID generation"},
                status=500
            )

        # Compute File Hash
        file_hash_object = hashlib.sha256()
        total_size = 0
        file_chunks = []
        while True:
            chunk = await field.read_chunk()
            if not chunk:
                break
            total_size += len(chunk)
            file_hash_object.update(chunk)
            file_chunks.append(chunk)
        if total_size == 0:
            logger.warning("Uploaded file is empty.")
            if os.path.exists(FILE_PATH):
                os.remove(FILE_PATH)
            return web.json_response(
                {"error": "Uploaded file is empty"},
                status=400
            )
        HASH_BYTES = file_hash_object.digest()

        # LOAD SECRETS DATA
        secrets_field = await reader.next()
        if not hasattr(secrets_field, 'name'):
            logger.warning("Upload request missing secrets field.")
            return web.json_response(
                {"error": "No secrets field in the upload request"},
                status=400
            )
        secrets = await secrets_field.read(decode=False)

        # Encrypt Secrets
        try:
            NONCE, CIPHERTEXT = self.system_context.create_context_data(HASH_BYTES, secrets)
        except Exception as e:
            logger.warning(f"Error encrypting secrets: {e}", exc_info=True)
            return web.json_response(
                {"error": "Failed to encrypt secrets"},
                status=500
            )

        # WRITE FILE
        try:
            with open(FILE_PATH, 'wb') as f:
                f.write(b''.join(file_chunks))
            logger.info(f"File {FILE_PATH} written successfully.")
        except Exception as e:
            logger.error(f"Error writing file {FILE_PATH}: {e}", exc_info=True)
            if os.path.exists(FILE_PATH):
                os.remove(FILE_PATH)
            return web.json_response(
                {"error": "Failed to write file"},
                status=500
            )

        # VALIDATE FILE
        validation_endpoints = {
            'provider': 'http://datahub:8080/internal/provider/validate',
            'broker': 'http://portfoliomanager:8082/internal/broker/validate'
        }
        validation_url = validation_endpoints[class_type]
        payload ={
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
                            return web.json_response(error_payload, status=response.status)
                        except Exception as e_parse: # Includes JSONDecodeError, ContentTypeError
                            # DataHub did not send valid JSON. Log its actual response.
                            error_body_text = await response.text() # Get raw text
                            logger.error(f"DataHub validation error (status {response.status}) was not valid JSON. Body: '{error_body_text[:200]}...'. Parse error: {e_parse}", exc_info=False) # Set exc_info=False if e_parse is already informative
                            # Return a structured JSON error from Registry, using DataHub's status or a specific gateway error status.
                            return web.json_response(
                                {
                                    "error": "Validation service returned an invalid or non-JSON response",
                                    "details": f"Validation service at {validation_url} responded with status {response.status}.",
                                    "original_response_snippet": error_body_text[:200] # Include a snippet for debugging
                                },
                                status=502 # Bad Gateway, indicating Registry had an issue with the upstream (DataHub) response.
                                           # Alternatively, you could use response.status here if you want to mirror DataHub's exact status,
                                           # but 502 clearly signals the issue was with the upstream service's response format.
                            )
                    else:
                        response_json = await response.json()
                        NAME = response_json.get('class_name')
                        SUBCLASS = response_json.get('subclass_type')
                        if not NAME:
                            logger.warning(f"Validation response missing class name for {class_type} file.")
                            os.remove(FILE_PATH)
                            return web.json_response(
                                {"error": "Validation response missing class name"},
                                status=400
                            )
                        if not SUBCLASS:
                            logger.warning(f"Validation response missing subclass type for {class_type} file.")
                            os.remove(FILE_PATH)
                            return web.json_response(
                                {"error": "Validation response missing subclass type"},
                                status=400
                            )
                        
        except Exception as e:
            logger.error(f"Error during validation request: {e}", exc_info=True)
            os.remove(FILE_PATH)
            return web.json_response(
                {"error": "Validation request failed"},
                status=500
            )

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

        return web.json_response(
            {
                "status": f"File {unique_filename} uploaded successfully. Registered ID: {registered_id}"
            },
            status=200
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
        """
        Register the uploaded code in the database.
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
    async def _handle_delete_class(self, request: web.Request) -> web.Response:
        """
        Endpoint to delete a registered class (provider or broker) by its class_name and class_type.
        """
        class_type = request.match_info.get('class_type')
        class_name = request.match_info.get('class_name')
        if not class_name or not class_type:
            logger.warning(f"Invalid request: Missing class_name or class_type in URL.")
            return web.json_response(
                {"error": "Missing class_name or class_type in URL"},
                status=400
            )
        if class_type not in ['provider', 'broker']:
            logger.warning(f"Invalid class type '{class_type}' in URL.")
            return web.json_response(
                {"error": "Invalid class type in URL, must be 'provider' or 'broker'"},
                status=400
            )
        # Verify if the class_name and class_type are registered
        query_file_path = """
            SELECT file_path FROM code_registry WHERE class_name = $1 AND class_type = $2;
            """
        file_path_to_delete = None
        try:
            file_path_to_delete = await self.pool.fetchval(query_file_path, class_name, class_type)
            if not file_path_to_delete:
                logger.warning(f"Registry._handle_delete_class: Class '{class_name}' ({class_type}) is not registered.")
                return web.json_response(
                    {"error": f"Class '{class_name}' ({class_type}) is not registered."},
                    status=404
                )
        except Exception as e_db_check:
            logger.error(f"Registry._handle_delete_class: Error checking registration for {class_name} ({class_type}): {e_db_check}", exc_info=True)
            return web.json_response(
                {"error": "Database error while checking registration"},
                status=500
            )
        # Delete the class from the database
        delete_query = """
            DELETE FROM code_registry WHERE class_name = $1 AND class_type = $2 RETURNING id;
            """
        deleted_id = None
        try:
            deleted_id = await self.pool.fetchval(delete_query, class_name, class_type)
            if not deleted_id:
                logger.warning(f"Registry._handle_delete_class: Class '{class_name}' ({class_type}) was not found for deletion.")
                return web.json_response(
                    {"error": f"Class '{class_name}' ({class_type}) was not found for deletion."},
                    status=404
                )
        except Exception as e_db_delete:
            logger.error(f"Registry._handle_delete_class: Error deleting class {class_name} ({class_type}): {e_db_delete}", exc_info=True)
            return web.json_response(
                {"error": "Database error while deleting class"},
                status=500
            )
        # Delete file from filesystem
        file_deleted_success = False
        if file_path_to_delete:
            try:
                if os.path.exists(file_path_to_delete):
                    os.remove(file_path_to_delete)
                    logger.info(f"Registry._handle_delete_class: Successfully deleted file {file_path_to_delete}.")
                    file_deleted_success = True
                else:
                    logger.warning(f"Registry._handle_delete_class: File {file_path_to_delete} does not exist, cannot delete.")
                    file_deleted_success = True
            except Exception as e_file_delete:
                logger.error(f"Registry._handle_delete_class: Error deleting file {file_path_to_delete}: {e_file_delete}", exc_info=True)
                return web.json_response(
                    {
                        "message": f"Class '{class_name}' ({class_type}) deleted from database, but failed to delete associated file: {file_path_to_delete}. Error: {e_file_delete}",
                        "class_name": class_name,
                        "class_type": class_type,
                        "file_deletion_error": str(e_file_delete)
                    },
                    status=207 # Multi-Status: DB operation succeeded, file operation might have failed.
                )

        return web.json_response(
            {
                "message": f"Class '{class_name}' ({class_type}) deleted successfully.",
                "class_name": class_name,
                "class_type": class_type,
                "file_deleted": file_deleted_success
            },
            status=200
        )

    # # ASSET UPDATE / REGISRATION
    async def _handle_update_assets(self, request: web.Request) -> web.Response:
        """
        Endpoint to update assets for a given registered class_name and class_type.
        """
        class_type = request.match_info.get('class_type')
        class_name = request.match_info.get('class_name')
        if not class_name or not class_type:
            logger.warning(f"Invalid request: Missing class_name or class_type in URL.")
            return web.json_response(
                {"error": "Missing class_name or class_type in URL"},
                status=400
            )
        if class_type not in ['provider', 'broker']:
            logger.warning(f"Invalid class type '{class_type}' in URL.")
            return web.json_response(
                {"error": "Invalid class type in URL, must be 'provider' or 'broker'"},
                status=400
            )
        
        # Verify if the class_name and class_type are registered
        query_provider_exists = """
            SELECT id FROM code_registry WHERE class_name = $1 AND class_type = $2;
            """
        try:
            provider_reg_id = await self.pool.fetchval(query_provider_exists, class_name, class_type)
            if not provider_reg_id:
                logger.warning(f"Registry._handle_update_assets: Class '{class_name}' ({class_type}) is not registered.")
                return web.json_response(
                    {"error": f"Class '{class_name}' ({class_type}) is not registered."},
                    status=404
                )
        except Exception as e_db_check:
            logger.error(f"Registry._handle_update_assets: Error checking registration for {class_name} ({class_type}): {e_db_check}", exc_info=True)
            return web.json_response(
                {"error": "Database error while checking registration"},
                status=500
            )

        # Call internal method to update assets
        stats = await self._update_assets_for_provider(class_name, class_type)
        if stats.get('status') != 200:
            logger.error(f"Registry._handle_update_assets: Error updating assets for {class_name} ({class_type}): {stats.get('error')}")
            return web.json_response(
                {"error": stats.get('error')},
                status=stats.get('status', 500)
            )

        # Return the stats as a JSON response
        return web.json_response(
            stats,
            status=200
        )

    async def _handle_update_all_assets(self, request: web.Request) -> web.Response:
        """
        API Endpoint to trigger assets update for all registered code
        """
        logger.info("Registry._handle_update_all_assets: Triggering asset update for all registered providers.")
        # Fetch all registered providers
        query_providers = """
            SELECT class_name, class_type FROM code_registry;
            """
        try:
            async with self.pool.acquire() as conn:
                providers = await conn.fetch(query_providers)
        except Exception as e_db_fetch:
            logger.error(f"Registry._handle_update_all_assets: Error fetching registered providers: {e_db_fetch}", exc_info=True)
            return web.json_response(
                {"error": "Database error while fetching registered providers"},
                status=500
            )

        if not providers:
            logger.info("Registry._handle_update_all_assets: No registered providers found.")
            return web.json_response(
                {"message": "No registered providers found."},
                status=204
            )

        # Update assets for each provider
        stats_list = []
        for provider in providers:
            class_name = provider['class_name']
            class_type = provider['class_type']
            stats = await self._update_assets_for_provider(class_name, class_type)
            stats_list.append(stats)

        return web.json_response(
            stats_list,
            status=200
        )

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
        datahub_url = f'http://datahub:8080/internal/providers/{class_name}/available-symbols'
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(datahub_url) as response:
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
            async with conn.transaction():
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

    # # GET REGISTERED CLASSES
    async def _handle_get_classes_summary(self, request: web.Request) -> web.Response:
        """
        API endpoint to get a summary of all registered classes (providers/brokers)
        and basic statistics like the number of assets associated with them.
        """
        logger.info("Registry._handle_get_classes_summary: Received request for classes summary.")

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
        
        classes_summary: List[Dict[str, Any]] = []
        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query)
            
            if not records:
                logger.info("Registry._handle_get_classes_summary: No registered classes found.")
                return web.json_response([], status=200) # Return empty list if none found

            for record in records:
                classes_summary.append(dict(record)) # Convert asyncpg.Record to dict

            logger.info(f"Registry._handle_get_classes_summary: Returning summary for {len(classes_summary)} classes.")
            return web.json_response(classes_summary, status=200)

        except Exception as e_db_fetch:
            logger.error(f"Registry._handle_get_classes_summary: Error fetching classes summary: {e_db_fetch}", exc_info=True)
            return web.json_response(
                {"error": "Database error while fetching classes summary"},
                status=500
            )

    # # ASSET MAPPINGS
    async def _handle_create_asset_mapping(self, request: web.Request) -> web.Response:
        """
        API endpoint to create a mapping between a common symbol and a specific asset.
        """

    async def _handle_get_asset_mappings(self, request: web.Request) -> web.Response:
        """
        API endpoint to get all asset mappings.
        """

    async def _handle_update_asset_mapping(self, request: web.Request) -> web.Response:
        """
        API endpoint to update an existing asset mapping.
        """

    async def _handle_delete_asset_mapping(self, request: web.Request) -> web.Response: 
        """
        API endpoint to delete an existing asset mapping.
        """