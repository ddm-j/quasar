"""Base class for Registry handler mixins."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg
    from quasar.services.registry.matcher import IdentityMatcher
    from quasar.services.registry.mapper import AutomatedMapper


class HandlerMixin:
    """Base mixin providing access to Registry dependencies.

    Handler mixins inherit from this to access shared resources.
    The actual implementations of these properties come from the
    Registry class that inherits from the mixins.

    Type hints are provided for IDE support.
    """

    # These are provided by Registry class
    pool: 'asyncpg.Pool'
    matcher: 'IdentityMatcher'
    mapper: 'AutomatedMapper'
