"""Registry handler mixins package.

This package contains domain-specific handler mixins that are combined
into the Registry class via multiple inheritance. Each mixin handles
a specific domain area:

- AssetHandlersMixin: Asset management (CRUD, updates, queries)
- CodeHandlersMixin: Code upload and management
- ConfigHandlersMixin: Provider/broker configuration
- IndexHandlersMixin: Index management
- MappingHandlersMixin: Asset mapping operations
"""

from quasar.services.registry.handlers.assets import AssetHandlersMixin
from quasar.services.registry.handlers.code import CodeHandlersMixin
from quasar.services.registry.handlers.config import ConfigHandlersMixin
from quasar.services.registry.handlers.indices import (
    IndexHandlersMixin,
    MembershipSyncResult,
    _weights_equal,
)
from quasar.services.registry.handlers.mappings import MappingHandlersMixin

__all__ = [
    'AssetHandlersMixin',
    'CodeHandlersMixin',
    'ConfigHandlersMixin',
    'IndexHandlersMixin',
    'MappingHandlersMixin',
    'MembershipSyncResult',
    '_weights_equal',
]
