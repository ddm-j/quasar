"""Tests for SecretStore."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import json
import tempfile
import os

from quasar.common.secret_store import SecretStore, SecretsFileNotFoundError


class TestSecretStore:
    """Tests for SecretStore."""
    
    @pytest.mark.asyncio
    async def test_get_returns_cached_value(self):
        """Test that get() returns cached value on second call."""
        store = SecretStore(mode="local")
        store._cache = {"test_provider": {"api_key": "cached_key"}}
        
        value = await store.get("test_provider")
        
        assert value == {"api_key": "cached_key"}
    
    @pytest.mark.asyncio
    async def test_get_loads_from_file_in_local_mode(self):
        """Test that get() loads from file in local mode."""
        # Create temporary secrets file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            secrets = {
                "test_provider": {
                    "api_key": "test_key",
                    "api_secret": "test_secret"
                }
            }
            json.dump(secrets, f)
            temp_path = f.name
        
        try:
            store = SecretStore(mode="local")
            
            # Mock Path to return our temp file
            with patch.object(store, 'load_cfg_from_file') as mock_load:
                mock_load.return_value = secrets["test_provider"]
                
                # Set the default path to our temp file
                import quasar.common.secret_store as secret_store_module
                original_paths = secret_store_module._DEFAULT_PATHS
                secret_store_module._DEFAULT_PATHS = [Path(temp_path)]
                
                value = await store.get("test_provider")
                
                assert value == secrets["test_provider"]
                assert "test_provider" in store._cache
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_get_loads_from_file_in_auto_mode_file_found(self):
        """Test that get() finds file in auto mode."""
        # Create temporary secrets file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            secrets = {
                "test_provider": {
                    "api_key": "test_key"
                }
            }
            json.dump(secrets, f)
            temp_path = f.name
        
        try:
            store = SecretStore(mode="auto")
            
            # Mock Path.is_file to return True for our temp file
            with patch('quasar.common.secret_store.Path') as mock_path:
                mock_path_instance = Mock()
                mock_path_instance.is_file.return_value = True
                mock_path_instance.read_text.return_value = json.dumps(secrets)
                mock_path.return_value = mock_path_instance
                
                with patch.object(store, 'load_cfg_from_file') as mock_load:
                    mock_load.return_value = secrets["test_provider"]
                    
                    value = await store.get("test_provider")
                    
                    assert value == secrets["test_provider"]
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_get_raises_error_in_auto_mode_file_not_found(self):
        """Test that get() raises error when file not found in auto mode."""
        import quasar.common.secret_store as secret_store_module
        
        store = SecretStore(mode="auto")
        store._cache = {}
        
        # Ensure _DEFAULT_PATHS has at least 3 elements for the error message
        original_paths = secret_store_module._DEFAULT_PATHS
        secret_store_module._DEFAULT_PATHS = [
            Path("/path1"),
            Path("/path2"),
            Path("/path3")
        ]
        
        try:
            # Mock load_cfg_from_file to raise FileNotFoundError for all paths
            with patch.object(store, 'load_cfg_from_file', side_effect=FileNotFoundError("File not found")):
                with pytest.raises(SecretsFileNotFoundError):
                    await store.get("test_provider")
        finally:
            secret_store_module._DEFAULT_PATHS = original_paths
    
    @pytest.mark.asyncio
    async def test_get_loads_from_aws_in_aws_mode(self):
        """Test that get() loads from AWS SSM in aws mode."""
        store = SecretStore(mode="aws", aws_region="us-east-1")
        
        mock_ssm_response = {
            "Parameter": {
                "Value": json.dumps({"api_key": "aws_key"})
            }
        }
        
        store._ssm = Mock()
        store._ssm.get_parameter = Mock(return_value=mock_ssm_response)
        
        value = await store.get("test_provider")
        
        assert value == {"api_key": "aws_key"}
        assert "test_provider" in store._cache
        store._ssm.get_parameter.assert_called_once_with(
            Name="/quasar/test_provider",
            WithDecryption=True
        )
    
    def test_load_cfg_from_file_returns_provider_config(self):
        """Test that load_cfg_from_file returns provider config."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            secrets = {
                "test_provider": {
                    "api_key": "test_key"
                }
            }
            json.dump(secrets, f)
            temp_path = f.name
        
        try:
            store = SecretStore()
            config = store.load_cfg_from_file("test_provider", Path(temp_path))
            
            assert config == secrets["test_provider"]
        finally:
            os.unlink(temp_path)
    
    def test_load_cfg_from_file_raises_file_not_found(self):
        """Test that load_cfg_from_file raises FileNotFoundError for missing file."""
        store = SecretStore()
        non_existent_path = Path("/nonexistent/path/secrets.json")
        
        with pytest.raises(FileNotFoundError):
            store.load_cfg_from_file("test_provider", non_existent_path)
    
    def test_load_cfg_from_file_raises_keyerror_for_missing_provider(self):
        """Test that load_cfg_from_file raises KeyError for missing provider."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            secrets = {
                "other_provider": {
                    "api_key": "test_key"
                }
            }
            json.dump(secrets, f)
            temp_path = f.name
        
        try:
            store = SecretStore()
            
            with pytest.raises(KeyError, match="test_provider"):
                store.load_cfg_from_file("test_provider", Path(temp_path))
        finally:
            os.unlink(temp_path)

