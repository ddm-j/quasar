"""Code upload and management handlers for Registry."""

import base64
import hashlib
import logging
import os
from typing import TYPE_CHECKING

import aiohttp
import asyncpg
from fastapi import File, Form, HTTPException, Query, UploadFile

from quasar.services.registry.handlers.base import HandlerMixin
from quasar.services.registry.schemas import (
    ClassType,
    DeleteClassResponse,
    FileUploadResponse,
)

if TYPE_CHECKING:
    from quasar.lib.common.context import SystemContext

logger = logging.getLogger(__name__)


class CodeHandlersMixin(HandlerMixin):
    """Mixin providing code upload and management handlers.

    Handles:
        - Provider/broker file upload and validation
        - Code registration in database
        - Class deletion (DB + filesystem)
    """

    # These are provided by Registry class
    system_context: 'SystemContext'
    dynamic_provider: str
    dynamic_broker: str

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
                        except Exception as e_parse:  # Includes JSONDecodeError, ContentTypeError
                            # DataHub did not send valid JSON. Log its actual response.
                            error_body_text = await response.text()  # Get raw text
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
