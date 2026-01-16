"""DataHub service core: scheduler, provider loading, and API handlers."""

import asyncpg
import importlib.util
import inspect
import hashlib
import warnings
from itertools import compress
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from typing import Optional, Annotated, Any, Awaitable, Callable
from datetime import datetime, timezone, timedelta
from functools import wraps
from fastapi import HTTPException, Query
import asyncio
from pathlib import Path
import os

from quasar.lib.common.secret_store import SecretStore
from quasar.lib.common.offset_cron import OffsetCronTrigger
from quasar.lib.common.database_handler import DatabaseHandler
from quasar.lib.common.api_handler import APIHandler
from quasar.lib.common.context import SystemContext, DerivedContext
from quasar.lib.common.calendar import TradingCalendar
from quasar.lib.providers import HistoricalDataProvider, LiveDataProvider, IndexProvider, Req, Bar, ProviderType, load_provider, DataProvider
from quasar.lib.common.enum_guard import validate_enums
from quasar.services.datahub.schemas import (
    ProviderValidateRequest, ProviderValidateResponse,
    AvailableSymbolsResponse, ConstituentsResponse,
    SymbolSearchResponse, SymbolSearchItem, OHLCDataResponse, OHLCBar,
    SymbolMetadataResponse, DataTypeInfo, AssetInfo, OtherProvider
)
from quasar.services.datahub.utils.constants import (
    QUERIES, ALLOWED_DYNAMIC_PATH, BATCH_SIZE,
    DEFAULT_LOOKBACK, DEFAULT_LIVE_OFFSET, IMMEDIATE_PULL
)

import logging
logger = logging.getLogger(__name__)

ENUM_GUARD_MODE = os.getenv("ENUM_GUARD_MODE", "off").lower()

def safe_job(default_return: Any = None) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Decorator to wrap scheduled jobs and swallow exceptions.

    Args:
        default_return: Value to return if the wrapped coroutine fails.

    Returns:
        Callable: Wrapped coroutine that logs and returns ``default_return`` on failure.
    """
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Job {func.__name__} failed with error: {e}", exc_info=True)
                return default_return
        return wrapper
    return decorator


class DataHub(DatabaseHandler, APIHandler):
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

        # Job Key Tracking
        self.job_keys: set[str] = set()

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
            methods=['GET']
        )
        self._api_app.router.add_api_route(
            '/api/datahub/data',
            self.handle_get_ohlc_data,
            methods=['GET']
        )
        self._api_app.router.add_api_route(
            '/api/datahub/symbols/{provider}/{symbol}',
            self.handle_get_symbol_metadata,
            methods=['GET']
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

    # ---------------------------------------------------------------------
    # Provider Loading
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

            # Ensure the File Exsits
            if not FILE_PATH.startswith(ALLOWED_DYNAMIC_PATH):
                logger.warning(f"File {FILE_PATH} not in allowed path {ALLOWED_DYNAMIC_PATH}")
                warnings.warn(f"File {FILE_PATH} not in allowed path {ALLOWED_DYNAMIC_PATH}")
                return False
            if not Path(FILE_PATH).is_file():
                logger.warning(f"File {FILE_PATH} not found")
                warnings.warn(f"File {FILE_PATH} not found")
                return False
            
            # Verify File Hash
            sha256 = hashlib.sha256()
            with open(FILE_PATH, 'rb') as f:
                while chunk := f.read(8192):
                    sha256.update(chunk)
            sha256_hash = sha256.digest()
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

    async def refresh_subscriptions(self):
        """Synchronize scheduled jobs with the ``provider_subscription`` table."""
        logger.debug("Refreshing subscriptions.")
        # Fetch Current Subscriptions in the DB
        query = QUERIES['get_subscriptions']
        rows = await self.pool.fetch(query)

        # Load Provider Objects
        current_providers = set(self._providers)
        seen_providers = set(r["provider"] for r in rows)
        invalid_providers = set()
        for name in seen_providers - current_providers:
            didLoad = await self.load_provider_cls(name)
            if not didLoad:
                invalid_providers.add(name)

        # Drop Providers that Aren't Needed Anymore
        for obsolete in current_providers - seen_providers:
            prov = self._providers[obsolete]
            if isinstance(prov, DataProvider) and prov.in_use:
                logger.debug(f"Skipping unload of {obsolete} - currently in use")
                continue
            logger.info(f"Removing obsolete provider from registry: {obsolete}")
            if hasattr(prov, 'aclose'):
                await prov.aclose()
            del self._providers[obsolete]
        
        # Update Scheduled Jobs
        new_keys = set()
        for r in rows:
            # Skip Providers that don't exist
            if r['provider'] in invalid_providers:
                continue

            key = f"{r['provider']}|{r['interval']}|{r['cron']}"
            new_keys.add(key)
            prov_type = self._providers[r["provider"]].provider_type
            if key not in self.job_keys:
                # Subscription Schedule Detected
                offset_seconds = 0 if prov_type == ProviderType.HISTORICAL else -1*DEFAULT_LIVE_OFFSET
                logger.debug(f"Scheduling new job: {key}, with offset: {offset_seconds}, from specified cron: {r['cron']}")
                self._sched.add_job(
                    func=self.get_data,
                    trigger=OffsetCronTrigger.from_crontab(r["cron"], offset_seconds=offset_seconds),
                    args=[r["provider"], r["interval"], r["syms"], r["exchanges"]],
                    id=key,
                )
                # Immediate Data Pull (development purposes)
                if IMMEDIATE_PULL and prov_type == ProviderType.HISTORICAL:
                    logger.info(f"Immediate data pull for new subscription: {r['provider']}, {r['interval']}, {r['syms']}")
                    asyncio.create_task(self.get_data(r["provider"], r["interval"], r["syms"], r["exchanges"]))
            else:
                # Update Scheduled Job (symbol subscription may have changed)
                logger.debug(f"Updating scheduled job: {key}")
                
                job = self._sched.get_job(key)
                if job and IMMEDIATE_PULL and prov_type == ProviderType.HISTORICAL:
                    # Identify symbols currently in the scheduled job (stored in args[2])
                    old_syms = set(job.args[2])
                    new_syms = set(r['syms'])
                    
                    # Find the symbols that were just added
                    added_syms = list(new_syms - old_syms)
                    
                    if added_syms:
                        # Extract the correct exchange MICs for the added symbols
                        # (r['syms'] and r['exchanges'] are already aligned by the SQL query)
                        added_exchanges = [
                            exc for sym, exc in zip(r['syms'], r['exchanges']) 
                            if sym in added_syms
                        ]
                        
                        logger.info(f"Symbols added to existing subscription {key}. Triggering immediate pull for: {added_syms}")
                        asyncio.create_task(self.get_data(r["provider"], r["interval"], added_syms, added_exchanges))

                self._sched.get_job(key).modify(
                    args=[r["provider"], r["interval"], r["syms"], r["exchanges"]]
                )

        # Remove Jobs if no longer subscribed
        for gone in self.job_keys - new_keys:
            logger.info(f"Removing scheduled job: {gone}")
            self._sched.remove_job(gone)

        self.job_keys = new_keys

    async def _build_reqs_historical(
            self,
            provider: str,
            interval: str,
            symbols: list[str],
            exchanges: list[str]) -> list[Req]:
            """Build provider requests for historical bars.

            Args:
                provider (str): Provider class name.
                interval (str): Interval string (e.g., ``1d``).
                symbols (list[str]): Symbols to request.
                exchanges (list[str]): Corresponding exchange MICs.

            Returns:
                list[Req]: Requests grouped by symbol.
            """
            logger.info(f'Building provider requests for: {provider}, {interval}')

            today = datetime.now(timezone.utc).date()
            yday = today - timedelta(days=1)
            
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    QUERIES['get_last_updated'],
                    provider,
                    symbols
                )
            last_map = {r['sym']: r['d'] for r in rows}
            
            reqs: list[Req] = []
            default_start = yday - timedelta(days=DEFAULT_LOOKBACK)
            for sym, mic in zip(symbols, exchanges):
                last_updated = last_map.get(sym)
                
                if last_updated is None:
                    # New Subscription: Bypass calendar check and pull 8000 bars
                    start = default_start + timedelta(days=1)
                    logger.info(f"New subscription for {sym} ({mic}). Requesting full backfill from {start}.")
                else:
                    # Incremental Update: Apply Smart Gap detection
                    start = last_updated + timedelta(days=1)
                    
                    if start > yday:
                        continue # Already caught up to yesterday
                        
                    # Check if any actual trading sessions occurred in the gap
                    if not TradingCalendar.has_sessions_in_range(mic, start, yday):
                        logger.info(f"Skipping {sym} ({mic}) - no trading sessions between {start} and {yday}.")
                        continue

                # Add valid request
                reqs.append(Req(
                    sym=sym,
                    start=start,
                    end=yday,
                    interval=interval
                ))

            return reqs

    async def _insert_with_conflict_handling(
            self,
            conn: asyncpg.Connection,
            table: str,
            records: list[tuple]
    ):
        """Insert records with ON CONFLICT handling as a fallback path.

        Args:
            conn (asyncpg.Connection): Database connection.
            table (str): Target table name.
            records (list[tuple]): Records to insert.
        """
        insert_query = f"""
            INSERT INTO {table} (ts, sym, provider, provider_class_type, interval, o, h, l, c, v)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (ts, sym, interval, provider) DO NOTHING
        """
        await conn.executemany(insert_query, records)

    async def _insert_bars(
            self,
            provider_type: ProviderType,
            provider: str,
            interval: str,
            bars: list[Bar]
    ):
        """Insert bars into the appropriate table with duplicate handling.

        Args:
            provider_type (ProviderType): Provider category (historical/live).
            provider (str): Provider class name.
            interval (str): Interval string.
            bars (list[Bar]): Bars to persist.
        """
        dbs = ['historical_data', 'live_data']
        db = dbs[provider_type.value]
        logger.info(f'Inserting {len(bars)} bars into {db}: {provider}, {interval}')
        records = [
            (b['ts'], b['sym'], provider, 'provider', interval, b['o'], b['h'], b['l'], b['c'], b['v']) 
            for b in bars
        ]
        async with self.pool.acquire() as conn:
            try:
                # Try fast COPY method first
                # Records tuple order: (ts, sym, provider, provider_class_type, interval, o, h, l, c, v)
                await conn.copy_records_to_table(db, records=records)
            except asyncpg.exceptions.UniqueViolationError:
                # Duplicates detected - fall back to INSERT with ON CONFLICT
                # When copy_records_to_table fails, the connection enters an aborted transaction state
                # Use a fresh connection for the fallback to avoid transaction state issues
                logger.warning(
                    f"Duplicate keys detected in batch for {provider}/{interval}. "
                    f"Falling back to INSERT with ON CONFLICT handling."
                )
                # Acquire a fresh connection for the fallback to avoid aborted transaction state
                async with self.pool.acquire() as fallback_conn:
                    await self._insert_with_conflict_handling(fallback_conn, db, records)
            except Exception as e:
                # Re-raise other exceptions
                logger.error(f"Error inserting bars into {db}: {e}", exc_info=True)
                raise

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

    # Helper Methods for Data Explorer API
    # ---------------------------------------------------------------------
    def _parse_timestamp(self, timestamp: str | int | float | None) -> Optional[datetime]:
        """Parse an ISO 8601 string or Unix timestamp to UTC datetime.

        Args:
            timestamp (str | int | float | None): Input value to parse.

        Returns:
            datetime | None: Parsed datetime in UTC, or ``None`` when input is ``None``.

        Raises:
            ValueError: When the timestamp cannot be parsed.
        """
        if timestamp is None:
            return None
        
        try:
            # Try parsing as Unix timestamp (int or float)
            if isinstance(timestamp, (int, float)):
                return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
            
            # Try parsing as ISO 8601 string
            if isinstance(timestamp, str):
                # Try parsing with timezone info first
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc)
                except ValueError:
                    # If that fails, try parsing as Unix timestamp string
                    try:
                        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
                    except (ValueError, TypeError):
                        raise ValueError(f"Unable to parse timestamp: {timestamp}")
            
            raise ValueError(f"Unsupported timestamp type: {type(timestamp)}")
        except Exception as e:
            logger.error(f"Error parsing timestamp {timestamp}: {e}")
            raise ValueError(f"Invalid timestamp format: {timestamp}")

    # Data Explorer API Endpoints
    # ---------------------------------------------------------------------
    async def handle_search_symbols(
        self,
        q: Annotated[str, Query(description="Search query string")],
        data_type: Annotated[Optional[str], Query(description="Filter by data type: 'historical' or 'live'")] = None,
        provider: Annotated[Optional[str], Query(description="Filter by provider class name")] = None,
        limit: Annotated[int, Query(ge=1, le=200, description="Maximum number of results")] = 50
    ) -> SymbolSearchResponse:
        """Search symbols by common symbol, provider symbol, or asset name.

        Args:
            q (str): Search query text.
            data_type (str | None): Filter to ``historical`` or ``live``.
            provider (str | None): Provider class name filter.
            limit (int): Maximum results to return.

        Returns:
            SymbolSearchResponse: Matched symbols with availability data.
        """
        logger.info(f"API request: Search symbols with query '{q}', data_type={data_type}, provider={provider}, limit={limit}")
        
        try:
            # Build the search query
            params = []
            param_idx = 1
            
            # Base query with joins
            query = """
                SELECT DISTINCT
                    am.common_symbol,
                    am.class_name AS provider,
                    am.class_symbol AS provider_symbol,
                    am.is_active,
                    a.name AS asset_name,
                    a.base_currency,
                    a.quote_currency,
                    a.exchange,
                    a.asset_class
                FROM asset_mapping am
                LEFT JOIN assets a ON (
                    a.class_name = am.class_name 
                    AND a.class_type = am.class_type 
                    AND a.symbol = am.class_symbol
                )
                WHERE am.is_active = TRUE
            """
            
            # Add search conditions (ILIKE for case-insensitive pattern matching)
            search_pattern = f"%{q}%"
            query += f" AND (am.common_symbol ILIKE ${param_idx} OR am.class_symbol ILIKE ${param_idx} OR a.name ILIKE ${param_idx})"
            params.append(search_pattern)
            param_idx += 1
            
            # Filter by provider if specified
            if provider:
                query += f" AND am.class_name = ${param_idx}"
                params.append(provider)
                param_idx += 1
            
            query += " ORDER BY am.common_symbol, am.class_name, am.class_symbol LIMIT $" + str(param_idx)
            params.append(limit)
            
            # Execute search query and check data availability using a single connection
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query, *params)
                
                if not records:
                    return SymbolSearchResponse(items=[], total=0, limit=limit)
                
                # For each result, check data availability
                items = []
                for record in records:
                    provider_name = record['provider']
                    provider_symbol = record['provider_symbol']
                    
                    # Check historical data availability
                    has_historical = False
                    has_live = False
                    available_intervals = []
                    last_updated = None
                    
                    # Check historical data
                    hist_query = """
                        SELECT DISTINCT interval, MAX(ts) as max_ts
                        FROM historical_data
                        WHERE provider = $1 AND sym = $2
                        GROUP BY interval
                    """
                    hist_records = await conn.fetch(hist_query, provider_name, provider_symbol)
                    if hist_records:
                        has_historical = True
                        for hrec in hist_records:
                            interval = hrec['interval']
                            if interval not in available_intervals:
                                available_intervals.append(interval)
                            max_ts = hrec['max_ts']
                            if last_updated is None or (max_ts and max_ts > last_updated):
                                last_updated = max_ts
                    
                    # Check live data
                    live_query = """
                        SELECT DISTINCT interval, MAX(ts) as max_ts
                        FROM live_data
                        WHERE provider = $1 AND sym = $2
                        GROUP BY interval
                    """
                    live_records = await conn.fetch(live_query, provider_name, provider_symbol)
                    if live_records:
                        has_live = True
                        for lrec in live_records:
                            interval = lrec['interval']
                            if interval not in available_intervals:
                                available_intervals.append(interval)
                            max_ts = lrec['max_ts']
                            if last_updated is None or (max_ts and max_ts > last_updated):
                                last_updated = max_ts
                    
                    # Filter by data_type if specified
                    if data_type == "historical" and not has_historical:
                        continue
                    if data_type == "live" and not has_live:
                        continue
                    
                    # Build asset info
                    asset_info = None
                    if record['asset_name'] or record['base_currency'] or record['quote_currency']:
                        asset_info = AssetInfo(
                            name=record['asset_name'],
                            base_currency=record['base_currency'],
                            quote_currency=record['quote_currency'],
                            exchange=record['exchange'],
                            asset_class=record['asset_class']
                        )
                    
                    items.append(SymbolSearchItem(
                        common_symbol=record['common_symbol'],
                        provider=provider_name,
                        provider_symbol=provider_symbol,
                        has_historical=has_historical,
                        has_live=has_live,
                        available_intervals=sorted(available_intervals),
                        last_updated=last_updated,
                        asset_info=asset_info
                    ))
            
            return SymbolSearchResponse(items=items, total=len(items), limit=limit)
            
        except Exception as e:
            logger.error(f"Error searching symbols: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error while searching symbols: {str(e)}")

    async def handle_get_ohlc_data(
        self,
        provider: Annotated[str, Query(description="Provider class name")],
        symbol: Annotated[str, Query(description="Provider-specific symbol")],
        data_type: Annotated[str, Query(description="Data type: 'historical' or 'live'")],
        interval: Annotated[str, Query(description="Interval string")],
        limit: Annotated[int, Query(ge=1, le=5000, description="Number of bars to return")] = 500,
        from_time: Annotated[Optional[str], Query(alias="from", description="Start time (ISO 8601 or Unix timestamp)")] = None,
        to_time: Annotated[Optional[str], Query(alias="to", description="End time (ISO 8601 or Unix timestamp)")] = None,
        order: Annotated[str, Query(description="Order: 'asc' or 'desc'")] = "desc"
    ) -> OHLCDataResponse:
        """Retrieve OHLC data for a specific symbol/provider combination.

        Args:
            provider (str): Provider class name.
            symbol (str): Provider-specific symbol.
            data_type (str): ``historical`` or ``live``.
            interval (str): Interval string.
            limit (int): Maximum bars to return.
            from_time (str | None): Inclusive start time filter.
            to_time (str | None): Exclusive end time filter.
            order (str): Sort order, ``asc`` or ``desc``.

        Returns:
            OHLCDataResponse: Bars and metadata for the request.
        """
        logger.info(f"API request: Get OHLC data for {provider}/{symbol}, type={data_type}, interval={interval}, limit={limit}")
        
        try:
            # Validate data_type
            if data_type not in ["historical", "live"]:
                raise HTTPException(status_code=400, detail="data_type must be 'historical' or 'live'")
            
            # Validate order
            if order not in ["asc", "desc"]:
                raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
            
            # Determine table name
            table_name = "historical_data" if data_type == "historical" else "live_data"
            
            # Parse timestamps
            from_dt = self._parse_timestamp(from_time) if from_time else None
            to_dt = self._parse_timestamp(to_time) if to_time else datetime.now(timezone.utc)
            
            # Build query parameters
            params = []
            param_idx = 1
            
            # Build WHERE clause conditions
            where_conditions = [f"provider = ${param_idx}", f"sym = ${param_idx + 1}", f"interval = ${param_idx + 2}"]
            params.extend([provider, symbol, interval])
            param_idx += 3
            
            # Add time range filters
            if from_dt:
                where_conditions.append(f"ts >= ${param_idx}")
                params.append(from_dt)
                param_idx += 1
            
            if to_dt:
                where_conditions.append(f"ts < ${param_idx}")
                params.append(to_dt)
                param_idx += 1
            
            # Build WHERE clause
            where_clause = " AND ".join(where_conditions)
            
            # Add ordering
            order_clause = "ASC" if order == "asc" else "DESC"
            
            # Build data query
            data_query = f"""
                SELECT ts, o, h, l, c, v
                FROM {table_name}
                WHERE {where_clause}
                ORDER BY ts {order_clause}
                LIMIT ${param_idx}
            """
            params.append(limit)
            
            # Build count query explicitly (without ORDER BY and LIMIT)
            count_query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {where_clause}
            """
            count_params = params[:-1]  # Remove limit param
            
            # Execute all queries using a single connection
            async with self.pool.acquire() as conn:
                records = await conn.fetch(data_query, *params)
                
                if not records:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No data found for provider '{provider}', symbol '{symbol}', interval '{interval}', data_type '{data_type}'"
                    )
                
                # Check if more data exists
                count_record = await conn.fetchrow(count_query, *count_params)
                total_count = count_record['count'] if count_record else 0
                has_more = total_count > len(records)
                
                # Get common_symbol from asset_mapping
                mapping_query = """
                    SELECT common_symbol
                    FROM asset_mapping
                    WHERE class_name = $1 AND class_type = 'provider' AND class_symbol = $2
                """
                mapping_record = await conn.fetchrow(mapping_query, provider, symbol)
                common_symbol = mapping_record['common_symbol'] if mapping_record else None
            
            # Convert to OHLCBar objects
            bars = []
            from_time_dt = None
            to_time_dt = None
            
            for record in records:
                ts = record['ts']
                if isinstance(ts, datetime):
                    unix_time = int(ts.timestamp())
                else:
                    unix_time = int(ts)
                
                bars.append(OHLCBar(
                    time=unix_time,
                    open=float(record['o']),
                    high=float(record['h']),
                    low=float(record['l']),
                    close=float(record['c']),
                    volume=float(record['v'])
                ))
                
                # Track time range
                if from_time_dt is None or ts < from_time_dt:
                    from_time_dt = ts
                if to_time_dt is None or ts > to_time_dt:
                    to_time_dt = ts
            
            return OHLCDataResponse(
                provider=provider,
                symbol=symbol,
                common_symbol=common_symbol,
                data_type=data_type,
                interval=interval,
                bars=bars,
                count=len(bars),
                from_time=from_time_dt,
                to_time=to_time_dt,
                has_more=has_more
            )
            
        except HTTPException:
            raise
        except ValueError as e:
            logger.error(f"Invalid parameter in get_ohlc_data: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error retrieving OHLC data: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error while retrieving OHLC data: {str(e)}")

    async def handle_get_symbol_metadata(
        self,
        provider: str,
        symbol: str,
        data_type: Annotated[Optional[str], Query(description="Filter to 'historical' or 'live' (default: both)")] = None
    ) -> SymbolMetadataResponse:
        """Get detailed metadata for a specific symbol/provider combination.

        Args:
            provider (str): Provider class name.
            symbol (str): Provider-specific symbol.
            data_type (str | None): Optional filter to ``historical`` or ``live``.

        Returns:
            SymbolMetadataResponse: Metadata and availability information.
        """
        logger.info(f"API request: Get metadata for {provider}/{symbol}, data_type={data_type}")
        
        try:
            # Get mapping and asset info
            query = """
                SELECT 
                    am.common_symbol,
                    am.class_name AS provider,
                    am.class_symbol AS provider_symbol,
                    a.name AS asset_name,
                    a.base_currency,
                    a.quote_currency,
                    a.exchange,
                    a.asset_class
                FROM asset_mapping am
                LEFT JOIN assets a ON (
                    a.class_name = am.class_name 
                    AND a.class_type = am.class_type 
                    AND a.symbol = am.class_symbol
                )
                WHERE am.class_name = $1 AND am.class_type = 'provider' AND am.class_symbol = $2 AND am.is_active = TRUE
            """
            async with self.pool.acquire() as conn:
                record = await conn.fetchrow(query, provider, symbol)
            
            if not record:
                raise HTTPException(
                    status_code=404,
                    detail=f"Symbol not found: provider '{provider}', symbol '{symbol}'"
                )
            
            common_symbol = record['common_symbol']
            
            # Build asset info
            asset_info = None
            if record['asset_name'] or record['base_currency'] or record['quote_currency']:
                asset_info = AssetInfo(
                    name=record['asset_name'],
                    base_currency=record['base_currency'],
                    quote_currency=record['quote_currency'],
                    exchange=record['exchange'],
                    asset_class=record['asset_class']
                )
            
            # Check data availability for both types
            data_types_info = {}
            
            # Historical data
            if data_type is None or data_type == "historical":
                hist_query = """
                    SELECT 
                        COUNT(*) > 0 as has_data,
                        COALESCE(array_agg(DISTINCT interval), ARRAY[]::text[]) as intervals,
                        MIN(ts) as earliest,
                        MAX(ts) as latest
                    FROM historical_data
                    WHERE provider = $1 AND sym = $2
                """
                async with self.pool.acquire() as conn:
                    hist_record = await conn.fetchrow(hist_query, provider, symbol)
                
                if hist_record and hist_record['has_data']:
                    intervals = hist_record['intervals'] if hist_record['intervals'] else []
                    data_types_info["historical"] = DataTypeInfo(
                        available=True,
                        intervals=sorted(intervals),
                        earliest=hist_record['earliest'],
                        latest=hist_record['latest'],
                        last_updated=hist_record['latest']
                    )
                else:
                    data_types_info["historical"] = DataTypeInfo(
                        available=False,
                        intervals=[],
                        earliest=None,
                        latest=None,
                        last_updated=None
                    )
            
            # Live data
            if data_type is None or data_type == "live":
                live_query = """
                    SELECT 
                        COUNT(*) > 0 as has_data,
                        COALESCE(array_agg(DISTINCT interval), ARRAY[]::text[]) as intervals,
                        MIN(ts) as earliest,
                        MAX(ts) as latest
                    FROM live_data
                    WHERE provider = $1 AND sym = $2
                """
                async with self.pool.acquire() as conn:
                    live_record = await conn.fetchrow(live_query, provider, symbol)
                
                if live_record and live_record['has_data']:
                    intervals = live_record['intervals'] if live_record['intervals'] else []
                    data_types_info["live"] = DataTypeInfo(
                        available=True,
                        intervals=sorted(intervals),
                        earliest=live_record['earliest'],
                        latest=live_record['latest'],
                        last_updated=live_record['latest']
                    )
                else:
                    data_types_info["live"] = DataTypeInfo(
                        available=False,
                        intervals=[],
                        earliest=None,
                        latest=None,
                        last_updated=None
                    )
            
            # Get other providers for same common_symbol
            other_providers_query = """
                SELECT 
                    am.class_name AS provider,
                    am.class_symbol AS provider_symbol,
                    (SELECT COUNT(*) > 0 FROM historical_data WHERE provider = am.class_name AND sym = am.class_symbol) as has_historical,
                    (SELECT COUNT(*) > 0 FROM live_data WHERE provider = am.class_name AND sym = am.class_symbol) as has_live
                FROM asset_mapping am
                WHERE am.common_symbol = $1 
                    AND am.is_active = TRUE
                    AND NOT (am.class_name = $2 AND am.class_symbol = $3)
            """
            async with self.pool.acquire() as conn:
                other_providers_records = await conn.fetch(other_providers_query, common_symbol, provider, symbol)
            
            other_providers = []
            for op_record in other_providers_records:
                other_providers.append(OtherProvider(
                    provider=op_record['provider'],
                    provider_symbol=op_record['provider_symbol'],
                    has_historical=op_record['has_historical'],
                    has_live=op_record['has_live']
                ))
            
            return SymbolMetadataResponse(
                common_symbol=common_symbol,
                provider=provider,
                provider_symbol=symbol,
                data_types=data_types_info,
                asset_info=asset_info,
                other_providers=other_providers
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving symbol metadata: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Internal server error while retrieving symbol metadata: {str(e)}")

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

    @safe_job(default_return=None)
    async def get_data(self, provider: str, interval: str, symbols: list[str], exchanges: list[str]):
        """Dispatch data pulls for the given provider and symbols.

        Args:
            provider (str): Provider class name.
            interval (str): Interval string.
            symbols (list[str]): Symbols to pull.
            exchanges (list[str]): Corresponding exchange MICs.
        """
        # Load Provider Class
        if provider not in self._providers:
            logger.error(f"Provider {provider} not found.")
            raise ValueError(f"Provider {provider} not found.")
        prov = self._providers[provider]

        if prov.provider_type == ProviderType.HISTORICAL:
            # Build Requests For Historical Data Provider
            reqs = await self._build_reqs_historical(provider, interval, symbols, exchanges)
            if not reqs:
                logger.info(f"{provider} has no valid sessions to pull at this time.")
                return
            args = [reqs]
            kwargs = {}
        elif prov.provider_type == ProviderType.REALTIME:
            # Filter symbols by current market status
            open_symbols = []
            for sym, mic in zip(symbols, exchanges):
                if TradingCalendar.is_open_now(mic):
                    open_symbols.append(sym)
                else:
                    logger.info(f"Skipping {sym} ({mic}) - market is currently closed.")
            
            if not open_symbols:
                logger.info(f"No markets are open for {provider} realtime session. Skipping.")
                return

            # Create Request for Live Data Provider
            args = [interval, open_symbols]
            # Add Timeout to prevent hung jobs
            kwargs = {'timeout': DEFAULT_LIVE_OFFSET+prov.close_buffer_seconds+30}
        else:
            logger.error(f"Provider {provider} is not a valid provider type.")
            raise ValueError(f"Provider {provider} is not a valid provider type.")

        # Pull / Insert Data Into QuasarDB
        prov = self._providers[provider]
        buf = []
        logger.info(f"Requesting data from provider.")
        async for bar in prov.get_data(*args, **kwargs):
            buf.append(bar)
            if len(buf) >= BATCH_SIZE:
                await self._insert_bars(prov.provider_type, provider, interval, buf)
                buf.clear()
        if buf:
            await self._insert_bars(prov.provider_type, provider, interval, buf)


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