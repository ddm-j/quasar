import os, json
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.backends import default_backend
import logging
from pathlib import Path

class SystemContext:
    _instance = None
    _system_context_path: Path | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SystemContext, cls).__new__(cls)
            cls._instance._system_context_path = Path(os.getenv("QUASAR_SYSTEM_CONTEXT", ""))
            if not cls._instance._system_context_path.is_file():
                logging.error(f"CRITIAL: System context path {cls._instance._system_context_path} does not exist.")
                raise FileNotFoundError(f"System context path {cls._instance._system_context_path} does not exist.")
        # Return existing instance if already created (proper singleton behavior)
        return cls._instance

    def _read_system_context(self):
        if not self._system_context_path:
            raise ValueError("System context path is not set.")
        try:
            return self._system_context_path.read_bytes().strip()
        except FileNotFoundError:
            logging.error(f"System context file {self._system_context_path} not found.")
            raise
        except Exception as e:
            logging.error(f"Error reading system context file: {e}")
            raise

    def get_derived_context(self, hash: bytes) -> AESGCM | None:
        hkdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=hash,
            backend=default_backend()
        )
        return AESGCM(hkdf.derive(self._read_system_context()))

    def create_context_data(self, hash: bytes, data: bytes) -> tuple[bytes, bytes]:
        derived_context = self.get_derived_context(hash)
        nonce = os.urandom(12)
        ciphertext = derived_context.encrypt(nonce, data, None)
        return nonce, ciphertext

class DerivedContext:
    def __init__(self, aesgcm: AESGCM, nonce: bytes, ciphertext: bytes):
        self.aesgcm = aesgcm
        self.nonce = nonce
        self.ciphertext = ciphertext

    def get(self, key: str) -> str:
        try:
            dat = self.aesgcm.decrypt(self.nonce, self.ciphertext, None).decode('utf-8')
            dat = json.loads(dat)
            if key not in dat:
                raise KeyError(f"Key {key} not found in derived context.")
            return dat.get(key)
        except Exception as e:
            logging.error(f"Error accessing derived context: {e}")
            raise