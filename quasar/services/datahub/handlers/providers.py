"""Provider lifecycle handlers: loading, validation, capability queries."""
import asyncio
import hashlib
import importlib.util
import inspect
import logging
import warnings
from itertools import compress
from pathlib import Path

from fastapi import Query, HTTPException

from quasar.lib.providers.core import (
    DataProvider, HistoricalDataProvider, LiveDataProvider, IndexProvider
)
from quasar.lib.common.context import DerivedContext

from .base import HandlerMixin
from ..utils.constants import QUERIES, ALLOWED_DYNAMIC_PATH
from ..schemas import (
    ProviderValidateRequest,
    ProviderValidateResponse,
    AvailableSymbolsResponse,
    ConstituentsResponse,
)

logger = logging.getLogger(__name__)


def load_provider_from_file_path(file_path: str, expected_class_name: str) -> type:
    """Load a provider class from a file path and verify its name.

    Args:
        file_path (str): Path to the provider implementation.
        expected_class_name (str): Expected ``name`` attribute of the provider.

    Returns:
        type: Loaded provider class.

    Raises:
        FileNotFoundError: If the file is missing.
        ImportError: When the provider cannot be imported or validated.
    """
    if not Path(file_path).is_file():
        raise FileNotFoundError(f"Provider file not found: {file_path}")

    module_name = Path(file_path).stem
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create module spec for {file_path}")

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise ImportError(f"Error executing module {file_path}: {e}")

    provider_classes = []
    for name, member_class in inspect.getmembers(module, inspect.isclass):
        if member_class.__module__ == module.__name__ and \
        (issubclass(member_class, HistoricalDataProvider) or
         issubclass(member_class, LiveDataProvider) or
         issubclass(member_class, IndexProvider)):
            provider_classes.append(member_class)

    if not provider_classes:
        raise ImportError(f"No valid provider class found in {file_path}")
    if len(provider_classes) > 1:
        # This should ideally be caught during validation by Registry/DataHub's validation endpoint
        raise ImportError(f"Multiple provider classes found in {file_path}. Only one is allowed.")

    loaded_class = provider_classes[0]

    if expected_class_name and getattr(loaded_class, 'name', None) != expected_class_name:
        raise ImportError(
            f"Loaded provider class from {file_path} has name '{getattr(loaded_class, 'name', None)}', "
            f"but expected '{expected_class_name}'."
        )
    return loaded_class


def _compute_file_hash(file_path: str) -> bytes:
    """Compute SHA256 hash of a file.

    Args:
        file_path: Path to the file to hash.

    Returns:
        SHA256 digest as bytes.
    """
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.digest()


class ProviderHandlersMixin(HandlerMixin):
    """Mixin providing provider lifecycle methods for DataHub."""

    async def load_provider_cls(self, name: str) -> bool:
        """Load a provider class by name from the registry table.

        Args:
            name (str): Provider class name stored in ``code_registry``.

        Returns:
            bool: ``True`` when the provider was loaded or already present.
        """
        try:
            # Check if already loaded
            if name in self._providers.keys():
                logger.info(f"Provider {name} already loaded, skipping.")
                return True

            # Query Database for Provider Configuration
            query = QUERIES['get_registered_provider']
            async with self.pool.acquire() as conn:
                provider_reg_data = await conn.fetchrow(query, name)
                if not provider_reg_data:
                    logger.warning(f"Provider {name} not found in database.")
                    warnings.warn(f"Provider {name} not found in database.")
                    return False

            # Get Provider Info
            FILE_PATH = provider_reg_data['file_path']
            FILE_HASH = provider_reg_data['file_hash']
            NONCE = provider_reg_data['nonce']
            CIPHERTEXT = provider_reg_data['ciphertext']

            # Ensure the File Exists
            if not FILE_PATH.startswith(ALLOWED_DYNAMIC_PATH):
                logger.warning(f"File {FILE_PATH} not in allowed path {ALLOWED_DYNAMIC_PATH}")
                warnings.warn(f"File {FILE_PATH} not in allowed path {ALLOWED_DYNAMIC_PATH}")
                return False
            if not Path(FILE_PATH).is_file():
                logger.warning(f"File {FILE_PATH} not found")
                warnings.warn(f"File {FILE_PATH} not found")
                return False

            # Verify File Hash (run in thread pool to avoid blocking event loop)
            sha256_hash = await asyncio.to_thread(_compute_file_hash, FILE_PATH)
            if sha256_hash != FILE_HASH:
                logger.warning(f"File {FILE_PATH} hash does not match database hash. {FILE_HASH} != {sha256_hash}")
                warnings.warn(f"File {FILE_PATH} hash does not match database hash")
                return False

            # Try Loading the Provider Class
            try:
                # Load the provider class from the file path
                ProviderCls = load_provider_from_file_path(FILE_PATH, name)
                logger.info(f"Provider {name} class loaded successfully.")
            except Exception as e:
                logger.warning(f"Unable to load provider {name} class. This provider will be skipped. Error message: {e}")
                warnings.warn(f"Unable to load provider {name} class. This provider will be skipped.")
                return False

            # Configure Provider Context
            context = DerivedContext(
                aesgcm=self.system_context.get_derived_context(sha256_hash),
                nonce=NONCE,
                ciphertext=CIPHERTEXT
            )

            # Create Provider Instance
            prov = ProviderCls(
                context=context
            )
            # Initialize the provider's async resources (e.g., aiohttp session)
            await prov.__aenter__()
            self._providers[name] = prov
            logger.info(f"Provider {name} instance created successfully.")
            return True
        except Exception as e:
            logger.error(f"Error loading provider {name}: {e}", exc_info=True)
            return False

    async def handle_get_available_symbols(
        self,
        provider_name: str = Query(..., description="Provider name")
    ) -> AvailableSymbolsResponse:
        """Return available symbols for the requested provider.

        Args:
            provider_name (str): Provider class name.

        Returns:
            AvailableSymbolsResponse: Wrapped list of provider-specific symbol metadata.

        Raises:
            HTTPException: When the provider is missing or unimplemented.
        """
        logger.info(f"API request: Get available symbols for provider '{provider_name}'")
        provider_instance = self._providers.get(provider_name)
        if not provider_instance:
            # Attempt to load the provider if not already loaded
            didLoad = await self.load_provider_cls(provider_name)
            if didLoad:
                provider_instance = self._providers.get(provider_name)

        if not provider_instance:
            logger.warning(f"Provider '{provider_name}' not found or not loaded for API request.")
            raise HTTPException(status_code=404, detail=f"Provider '{provider_name}' not found or not loaded")

        if not hasattr(provider_instance, 'fetch_available_symbols'):
            logger.error(f"Provider '{provider_name}' does not implement fetch_available_symbols method.")
            raise HTTPException(status_code=501, detail=f"Provider '{provider_name}' does not support symbol discovery")

        try:
            symbols = await provider_instance.get_available_symbols()
            # ProviderSymbolInfo is a TypedDict, which is inherently JSON serializable if its contents are.
            # Convert to list of dicts for JSON serialization
            items = [dict(symbol) if isinstance(symbol, dict) else symbol for symbol in symbols]
            return AvailableSymbolsResponse(items=items)
        except NotImplementedError:
            logger.error(f"fetch_available_symbols not implemented for provider '{provider_name}'.")
            raise HTTPException(status_code=501, detail=f"Symbol discovery not implemented for provider '{provider_name}'")
        except Exception as e:
            logger.error(f"Error fetching symbols for provider '{provider_name}': {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error while fetching symbols for '{provider_name}'")

    async def handle_get_constituents(
        self,
        provider_name: str = Query(..., description="IndexProvider name")
    ) -> ConstituentsResponse:
        """Return index constituents for the requested IndexProvider.

        Args:
            provider_name: IndexProvider class name.

        Returns:
            ConstituentsResponse: Wrapped list of constituent dicts with symbol, weight, and metadata.

        Raises:
            HTTPException: 404 if not found, 501 if not IndexProvider, 500 on error.
        """
        logger.info(f"API request: Get constituents for provider '{provider_name}'")

        # Lazy load provider if not cached
        provider_instance = self._providers.get(provider_name)
        if not provider_instance:
            didLoad = await self.load_provider_cls(provider_name)
            if didLoad:
                provider_instance = self._providers.get(provider_name)

        if not provider_instance:
            logger.warning(f"Provider '{provider_name}' not found or not loaded for API request.")
            raise HTTPException(status_code=404, detail=f"Provider '{provider_name}' not found or not loaded")

        # Verify it's an IndexProvider
        if not hasattr(provider_instance, 'fetch_constituents'):
            logger.error(f"Provider '{provider_name}' is not an IndexProvider (no fetch_constituents method).")
            raise HTTPException(status_code=501, detail=f"Provider '{provider_name}' is not an IndexProvider")

        try:
            constituents = await provider_instance.get_constituents()
            items = [dict(c) for c in constituents]
            return ConstituentsResponse(items=items)
        except NotImplementedError:
            logger.error(f"fetch_constituents not implemented for provider '{provider_name}'.")
            raise HTTPException(status_code=501, detail=f"fetch_constituents not implemented for '{provider_name}'")
        except Exception as e:
            logger.error(f"Error fetching constituents for provider '{provider_name}': {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error fetching constituents for '{provider_name}'")

    async def validate_provider(self, request: ProviderValidateRequest) -> ProviderValidateResponse:
        """Validate uploaded provider code for class shape and metadata.

        Args:
            request (ProviderValidateRequest): Validation request payload.

        Returns:
            ProviderValidateResponse: Validated provider metadata.
        """
        try:
            file_path = request.file_path
            if not file_path:
                raise HTTPException(status_code=500, detail='Internal API Error, file path not provided to datahub')
            if not file_path.startswith(ALLOWED_DYNAMIC_PATH):
                raise HTTPException(status_code=403, detail=f'File {file_path} not in allowed path {ALLOWED_DYNAMIC_PATH}')
            if not Path(file_path).is_file():
                raise HTTPException(status_code=404, detail=f'File {file_path} not found')

            # Dynamically Import the Module
            module_name = Path(file_path).stem
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                raise HTTPException(status_code=500, detail=f'Unable to load module {module_name} from {file_path}')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check Class Definitions
            defined_classes = []
            for name, member_class in inspect.getmembers(module, inspect.isclass):
                if member_class.__module__ == module.__name__:
                    defined_classes.append(member_class)
            if not defined_classes:
                raise HTTPException(status_code=500, detail=f'No classes found in {file_path}')
            if len(defined_classes) > 1:
                raise HTTPException(status_code=500, detail=f'Multiple classes found in {file_path}')

            # Check if Class is the correct subclass
            the_class = defined_classes[0]
            is_valid_subclass = [
                issubclass(the_class, HistoricalDataProvider),
                issubclass(the_class, LiveDataProvider),
                issubclass(the_class, IndexProvider)
            ]
            if not any(is_valid_subclass):
                raise HTTPException(status_code=500, detail=f'Class {the_class.__name__} in {file_path} is not a valid provider subclass')
            subclass_types = ['Historical', 'Live', 'IndexProvider']
            subclass_type = list(compress(subclass_types, is_valid_subclass))[0]

            # Get Class Name Attribute
            class_name = None
            if hasattr(the_class, 'name'):
                class_name = getattr(the_class, 'name')
                if not isinstance(class_name, str):
                    class_name = None
            if class_name is None:
                raise HTTPException(status_code=500, detail=f'Class {the_class.__name__} in {file_path} does not have a valid name attribute')

            logger.info(f"Provider {class_name} validated successfully.")
            return ProviderValidateResponse(
                status='success',
                class_name=class_name,
                subclass_type=subclass_type,
                module_name=module_name,
                file_path=file_path
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error validating provider: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f'Internal API Error: {e}')
