"""Tests for code upload and management handlers."""

import pytest
from unittest.mock import AsyncMock, patch, mock_open
from fastapi import HTTPException, UploadFile
from io import BytesIO


class TestRegistryFileUpload:
    """Tests for file upload endpoint."""

    @pytest.mark.asyncio
    async def test_handle_upload_file_valid_provider(
        self, registry_with_mocks, mock_aiohttp_session
    ):
        """Test that handle_upload_file succeeds with valid provider file."""
        reg = registry_with_mocks

        # Mock validation response
        mock_aiohttp_session["response"].status = 200
        mock_aiohttp_session["response"].json = AsyncMock(return_value={
            "class_name": "TestProvider",
            "subclass_type": "Historical"
        })

        # Mock file
        file_content = b"class TestProvider: pass"
        file = UploadFile(
            filename="test_provider.py",
            file=BytesIO(file_content)
        )

        # Mock file operations
        with patch('os.path.exists', return_value=False):
            with patch('builtins.open', mock_open()):
                with patch.object(reg, '_register_code', new_callable=AsyncMock) as mock_register:
                    mock_register.return_value = 1

                    response = await reg.handle_upload_file(
                        class_type="provider",
                        file=file,
                        secrets='{"api_key": "test"}'
                    )

                    assert response.status.startswith("File")
                    assert "uploaded successfully" in response.status

    @pytest.mark.asyncio
    async def test_handle_upload_file_invalid_class_type(self, registry_with_mocks):
        """Test that handle_upload_file returns 400 for invalid class type."""
        reg = registry_with_mocks

        file = UploadFile(filename="test.py", file=BytesIO(b"content"))

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_upload_file(
                class_type="invalid",
                file=file,
                secrets='{}'
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_handle_upload_file_invalid_file_type(self, registry_with_mocks):
        """Test that handle_upload_file returns 415 for non-Python file."""
        reg = registry_with_mocks

        file = UploadFile(filename="test.txt", file=BytesIO(b"content"))

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_upload_file(
                class_type="provider",
                file=file,
                secrets='{}'
            )

        assert exc_info.value.status_code == 415

    @pytest.mark.asyncio
    async def test_handle_upload_file_empty_file(self, registry_with_mocks):
        """Test that handle_upload_file returns 400 for empty file."""
        reg = registry_with_mocks

        file = UploadFile(filename="test.py", file=BytesIO(b""))

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_upload_file(
                class_type="provider",
                file=file,
                secrets='{}'
            )

        assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_handle_upload_file_validation_failure(
        self, registry_with_mocks, mock_aiohttp_session
    ):
        """Test that handle_upload_file handles validation failure."""
        reg = registry_with_mocks

        mock_aiohttp_session["response"].status = 500
        mock_aiohttp_session["response"].json = AsyncMock(return_value={
            "detail": "Validation failed"
        })

        file = UploadFile(filename="test.py", file=BytesIO(b"content"))

        with patch('os.path.exists', return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                await reg.handle_upload_file(
                    class_type="provider",
                    file=file,
                    secrets='{}'
                )

            assert exc_info.value.status_code == 500


class TestRegistryDeleteClass:
    """Tests for delete class endpoint."""

    @pytest.mark.asyncio
    async def test_handle_delete_class_success(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_delete_class successfully deletes class."""
        reg = registry_with_mocks

        # handle_delete_class uses pool.fetchval() directly
        mock_asyncpg_pool.fetchval = AsyncMock(side_effect=[
            "/app/dynamic_providers/test.py",  # File path query
            1  # Delete query returns ID
        ])

        with patch('os.path.exists', return_value=True):
            with patch('os.remove'):
                response = await reg.handle_delete_class("provider", "TestProvider")

                assert response.class_name == "TestProvider"
                assert response.class_type == "provider"
                assert response.file_deleted is True

    @pytest.mark.asyncio
    async def test_handle_delete_class_not_found(
        self, registry_with_mocks, mock_asyncpg_pool
    ):
        """Test that handle_delete_class returns 404 for non-existent class."""
        reg = registry_with_mocks

        # Mock pool.fetchval() directly (the method uses pool.fetchval, not conn.fetchval)
        # First call gets file_path, returns None to indicate class not found
        mock_asyncpg_pool.fetchval = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc_info:
            await reg.handle_delete_class("provider", "NonExistent")

        assert exc_info.value.status_code == 404
