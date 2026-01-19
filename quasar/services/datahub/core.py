"""DataHub service core: scheduler, provider loading, and API handlers."""

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from typing import Optional
import os

from quasar.lib.common.secret_store import SecretStore
from quasar.lib.common.database_handler import DatabaseHandler
from quasar.lib.common.api_handler import APIHandler
from quasar.lib.common.context import SystemContext
from quasar.lib.providers import HistoricalDataProvider
from quasar.lib.common.enum_guard import validate_enums
from quasar.services.datahub.schemas import (
    ProviderValidateResponse, AvailableSymbolsResponse, ConstituentsResponse,
    SymbolSearchResponse, OHLCDataResponse, SymbolMetadataResponse,
    ProviderUnloadResponse
)
from quasar.services.datahub.utils.constants import ALLOWED_DYNAMIC_PATH
from quasar.services.datahub.handlers.collection import CollectionHandlersMixin, safe_job
from quasar.services.datahub.handlers.providers import ProviderHandlersMixin, load_provider_from_file_path
from quasar.services.datahub.handlers.data_explorer import DataExplorerHandlersMixin

import logging
logger = logging.getLogger(__name__)

ENUM_GUARD_MODE = os.getenv("ENUM_GUARD_MODE", "off").lower()


class DataHub(ProviderHandlersMixin, CollectionHandlersMixin, DataExplorerHandlersMixin, DatabaseHandler, APIHandler):
    """Schedule data provider jobs, storage, and the DataHub API."""

    name = "DataHub"
    system_context = SystemContext()

    def __init__(
            self, *,
            secret_store: SecretStore,
            dsn: str | None = None,
            pool: Optional[asyncpg.Pool] = None,
            refresh_seconds: int = 30,
            api_host: str = '0.0.0.0',
            api_port: int = 8080): 
        """Create a DataHub instance.

        Args:
            secret_store (SecretStore): Provider secret loader.
            dsn (str | None): Database DSN when creating the pool internally.
            pool (asyncpg.Pool | None): Reusable pool if managed externally.
            refresh_seconds (int): Interval to refresh provider subscriptions.
            api_host (str): Host interface for the internal API.
            api_port (int): Port number for the internal API.
        """
        
        # Initialize parent class
        DatabaseHandler.__init__(self, dsn=dsn, pool=pool)
        APIHandler.__init__(self, api_host=api_host, api_port=api_port) 

        # Secret Store
        self.secret_store = secret_store

        # Store DataProvider objects
        self._providers: dict[str, HistoricalDataProvider] = {}

        # Store provider preferences for runtime access
        self._provider_preferences: dict[str, dict | None] = {}

        # Job Key Tracking
        self.job_keys: set[str] = set()
        self.index_sync_job_keys: set[str] = set()

        # Provider Subscription Refreshing
        logger.debug("Creating async DataHub scheduler.")
        self._sched = AsyncIOScheduler(timezone='UTC')
        self._stop_scheduler()
        self._refresh_seconds = refresh_seconds
    
    def _stop_scheduler(self):
        """Shut down any running scheduler instance and clear tracked jobs."""
        if self._sched.state == STATE_RUNNING:
            logger.debug("Existing scheduler is running. Shutting it down.")
            self._sched.shutdown(wait=False)
            self.job_keys.clear()

    def _setup_routes(self) -> None:
        """Define API routes for the DataHub."""
        logger.info("DataHub: Setting up API routes")
        self._api_app.router.add_api_route(
            '/internal/provider/validate',
            self.validate_provider,
            methods=['POST'],
            response_model=ProviderValidateResponse
        )
        self._api_app.router.add_api_route(
            '/internal/providers/available-symbols',
            self.handle_get_available_symbols,
            methods=['GET'],
            response_model=AvailableSymbolsResponse
        )
        self._api_app.router.add_api_route(
            '/internal/providers/constituents',
            self.handle_get_constituents,
            methods=['GET'],
            response_model=ConstituentsResponse
        )
        # Data Explorer API routes (public API)
        self._api_app.router.add_api_route(
            '/api/datahub/symbols/search',
            self.handle_search_symbols,
            methods=['GET'],
            response_model=SymbolSearchResponse
        )
        self._api_app.router.add_api_route(
            '/api/datahub/data',
            self.handle_get_ohlc_data,
            methods=['GET'],
            response_model=OHLCDataResponse
        )
        self._api_app.router.add_api_route(
            '/api/datahub/symbols/{provider}/{symbol}',
            self.handle_get_symbol_metadata,
            methods=['GET'],
            response_model=SymbolMetadataResponse
        )
        # Provider lifecycle
        self._api_app.router.add_api_route(
            '/api/datahub/providers/{name}/unload',
            self.handle_unload_provider,
            methods=['POST'],
            response_model=ProviderUnloadResponse
        )

    # OBJECT LIFECYCLE
    # ---------------------------------------------------------------------
    async def start(self):
        """Start database pool, refresh subscriptions, and run the API server."""
        
        # Start Database Pool
        await self.init_pool()
        await self._run_enum_guard()

        # Stop A Previous Scheduler if it is running
        self._stop_scheduler()

        # Refresh DataProvider Subscriptions, then schedule them to run
        await self.refresh_subscriptions()
        self._sched.add_job(
            self.refresh_subscriptions,
            trigger=IntervalTrigger(seconds=self._refresh_seconds),
            id='subscription_refresh',
            replace_existing=True,
        )

        # Initialize IndexProvider sync jobs
        await self.refresh_index_sync_jobs()

        self._sched.start()
        logger.info("DataHub started, subscription refresh interval: %ss", self._refresh_seconds)

        # Start Internal API Server
        await self.start_api_server()


    async def stop(self):
        """Stop API server, scheduler, and close database pool."""
        logger.info("DataHub shutting down.")

        # Stop Internal API Server
        await self.stop_api_server()

        # Stop Scheduler
        if self._sched and self._sched.state == STATE_RUNNING:
            self._sched.shutdown(wait=False)

        # Close Database Pool
        await self.close_pool()

    async def _run_enum_guard(self) -> None:
        """Optional enum/runtime sanity check against DB lookup tables."""
        mode = ENUM_GUARD_MODE
        if mode == "off":
            return
        strict = mode == "strict"
        await validate_enums(self.pool, strict=strict)