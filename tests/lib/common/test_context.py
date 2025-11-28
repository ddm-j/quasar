"""Tests for SystemContext and DerivedContext."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from quasar.lib.common.context import SystemContext, DerivedContext


class TestSystemContext:
    """Tests for SystemContext singleton."""
    
    def test_singleton_instantiation_succeeds(self, monkeypatch: pytest.MonkeyPatch):
        """Test that SystemContext can be instantiated once."""
        # Create a temporary file for system context
        import tempfile
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test_system_context_key_32_bytes_long_')
            temp_path = f.name
        
        try:
            monkeypatch.setenv("QUASAR_SYSTEM_CONTEXT", temp_path)
            # Reset singleton
            SystemContext._instance = None
            SystemContext._system_context_path = None
            
            context = SystemContext()
            assert context is not None
        finally:
            os.unlink(temp_path)
            SystemContext._instance = None
            SystemContext._system_context_path = None
    
    def test_singleton_returns_same_instance(self, monkeypatch: pytest.MonkeyPatch):
        """Test that SystemContext returns the same instance on multiple instantiations."""
        import tempfile
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test_system_context_key_32_bytes_long_')
            temp_path = f.name
        
        try:
            monkeypatch.setenv("QUASAR_SYSTEM_CONTEXT", temp_path)
            # Reset singleton
            SystemContext._instance = None
            SystemContext._system_context_path = None
            
            context1 = SystemContext()
            context2 = SystemContext()
            
            # Should return the same instance (proper singleton behavior)
            assert context1 is context2
        finally:
            os.unlink(temp_path)
            SystemContext._instance = None
            SystemContext._system_context_path = None
    
    def test_get_derived_context_returns_aesgcm(self, monkeypatch: pytest.MonkeyPatch):
        """Test that get_derived_context returns AESGCM instance."""
        import tempfile
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test_system_context_key_32_bytes_long_')
            temp_path = f.name
        
        try:
            monkeypatch.setenv("QUASAR_SYSTEM_CONTEXT", temp_path)
            # Reset singleton
            SystemContext._instance = None
            SystemContext._system_context_path = None
            
            context = SystemContext()
            test_hash = b'test_hash_32_bytes_long_here!'
            
            aesgcm = context.get_derived_context(test_hash)
            assert isinstance(aesgcm, AESGCM)
        finally:
            os.unlink(temp_path)
            SystemContext._instance = None
            SystemContext._system_context_path = None
    
    def test_create_context_data_encrypts_data(self, monkeypatch: pytest.MonkeyPatch):
        """Test that create_context_data encrypts data and returns nonce and ciphertext."""
        import tempfile
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test_system_context_key_32_bytes_long_')
            temp_path = f.name
        
        try:
            monkeypatch.setenv("QUASAR_SYSTEM_CONTEXT", temp_path)
            # Reset singleton
            SystemContext._instance = None
            SystemContext._system_context_path = None
            
            context = SystemContext()
            test_hash = b'test_hash_32_bytes_long_here!'
            test_data = b'test_secret_data'
            
            nonce, ciphertext = context.create_context_data(test_hash, test_data)
            
            assert isinstance(nonce, bytes)
            assert len(nonce) == 12  # AES-GCM nonce is 12 bytes
            assert isinstance(ciphertext, bytes)
            assert len(ciphertext) > 0
        finally:
            os.unlink(temp_path)
            SystemContext._instance = None
            SystemContext._system_context_path = None


class TestDerivedContext:
    """Tests for DerivedContext."""
    
    def test_get_returns_value_for_valid_key(self, monkeypatch: pytest.MonkeyPatch):
        """Test that get() returns value for valid key."""
        import tempfile
        import json
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test_system_context_key_32_bytes_long_')
            temp_path = f.name
        
        try:
            monkeypatch.setenv("QUASAR_SYSTEM_CONTEXT", temp_path)
            # Reset singleton
            SystemContext._instance = None
            SystemContext._system_context_path = None
            
            sys_context = SystemContext()
            test_hash = b'test_hash_32_bytes_long_here!'
            
            # Create test data
            test_secrets = {"api_key": "secret_key_123", "api_secret": "secret_secret"}
            test_data_bytes = json.dumps(test_secrets).encode('utf-8')
            
            nonce, ciphertext = sys_context.create_context_data(test_hash, test_data_bytes)
            aesgcm = sys_context.get_derived_context(test_hash)
            
            derived = DerivedContext(aesgcm=aesgcm, nonce=nonce, ciphertext=ciphertext)
            
            value = derived.get("api_key")
            assert value == "secret_key_123"
        finally:
            os.unlink(temp_path)
            SystemContext._instance = None
            SystemContext._system_context_path = None
    
    def test_get_raises_keyerror_for_invalid_key(self, monkeypatch: pytest.MonkeyPatch):
        """Test that get() raises KeyError for invalid key."""
        import tempfile
        import json
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            f.write(b'test_system_context_key_32_bytes_long_')
            temp_path = f.name
        
        try:
            monkeypatch.setenv("QUASAR_SYSTEM_CONTEXT", temp_path)
            # Reset singleton
            SystemContext._instance = None
            SystemContext._system_context_path = None
            
            sys_context = SystemContext()
            test_hash = b'test_hash_32_bytes_long_here!'
            
            test_secrets = {"api_key": "secret_key_123"}
            test_data_bytes = json.dumps(test_secrets).encode('utf-8')
            
            nonce, ciphertext = sys_context.create_context_data(test_hash, test_data_bytes)
            aesgcm = sys_context.get_derived_context(test_hash)
            
            derived = DerivedContext(aesgcm=aesgcm, nonce=nonce, ciphertext=ciphertext)
            
            with pytest.raises(KeyError, match="invalid_key"):
                derived.get("invalid_key")
        finally:
            os.unlink(temp_path)
            SystemContext._instance = None
            SystemContext._system_context_path = None

