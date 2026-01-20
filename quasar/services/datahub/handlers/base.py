"""Base mixin providing type hints for DataHub dependencies."""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from quasar.lib.common.secret_store import SecretStore
    from quasar.lib.common.context import SystemContext
    from quasar.lib.providers.core import DataProvider


class HandlerMixin:
    """Base mixin providing type hints for DataHub dependencies.

    These attributes are set by the DataHub class and provide access
    to shared resources for all handler mixins.
    """
    pool: 'asyncpg.Pool'
    secret_store: 'SecretStore'
    system_context: 'SystemContext'
    _sched: 'AsyncIOScheduler'
    _providers: dict[str, 'DataProvider']
    _provider_preferences: dict[str, dict | None]
    job_keys: set[str]
    index_sync_job_keys: set[str]
    _refresh_seconds: int
