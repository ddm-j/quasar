import asyncpg
import importlib.util
import inspect
import hashlib
from itertools import compress
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from typing import Optional
from datetime import datetime, timezone, timedelta
from functools import wraps
from aiohttp import web
import asyncio
from pathlib import Path

from quasar.common.secret_store import SecretStore
from quasar.common.offset_cron import OffsetCronTrigger
from quasar.common.database_handler import DatabaseHandler
from quasar.common.api_handler import APIHandler
from quasar.common.context import SystemContext, DerivedContext
from quasar.providers import HistoricalDataProvider, LiveDataProvider, Req, Bar, ProviderType, load_provider

import logging
logger = logging.getLogger(__name__)

DEFAULT_LIVE_OFFSET = 30 # Default number of seconds to offset the subscription cron job for live data providers
DEFAULT_LOOKBACK = 8000 # Default Number of bars to pull if we don't already have data
BATCH_SIZE = 500 # Number of bars to batch insert into the database 
QUERIES = {
          'get_subscriptions': """SELECT provider, interval, cron, array_agg(sym) AS syms
                                  FROM provider_subscription
                                  GROUP BY provider, interval, cron
                                  """,
           'get_last_updated': """SELECT sym, last_updated::date AS d
                                  FROM   historical_symbol_state
                                  WHERE  provider = $1
                                  AND  sym = ANY($2::text[])
                                  """,
    'get_registered_provider': """SELECT file_path, file_hash, nonce, ciphertext
                                  FROM code_registry
                                  WHERE class_name = $1 AND class_type = 'provider';
                                  """
}
ALLOWED_DYNAMIC_PATH = '/app/dynamic_providers'

def safe_job(default_return=None):
    """
    Decorator to catch exceptions in provider jobs and prevent system hangs.
    Logs the exception and optionally returns a default value.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Job {func.__name__} failed with error: {e}", exc_info=True)
                return default_return
        return wrapper
    return decorator


class DataHub(DatabaseHandler, APIHandler):
    """
    DataHub Class

    Parameters
    ----------
    secret_store : SecretStore instance
    dsn : str
        Postgres / TimescaleDB DSN, e.g. "postgresql://pg:pg@localhost:5432/pg".
    pool : asyncpg.Pool, optional
        If you already created one outside, pass it in and we’ll reuse it.
    """
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
        if self._sched.state == STATE_RUNNING:
            logger.debug("Existing scheduler is running. Shutting it down.")
            self._sched.shutdown(wait=False)
            self.job_keys.clear()

    def _setup_routes(self) -> None:
        """Define API routes for the DataHub."""
        logger.info("DataHub: Setting up API routes")
        self._api_app.add_routes([
            web.post('/internal/provider/validate', self._validate_provider),
            web.get('/internal/providers/{provider_name}/available-symbols', self._handle_get_available_symbols)
        ])

    # OBJECT LIFECYCLE
    # ---------------------------------------------------------------------
    async def start(self):
        
        # Start Database Pool
        await self.init_pool()

        # Stop A Previous Scheduler if it is running
        self._stop_scheduler()

        # Refresh DataProvider Subscriptions, then schedule them to run
        await self._refresh_subscriptions()
        self._sched.add_job(
            self._refresh_subscriptions,
            trigger=IntervalTrigger(seconds=self._refresh_seconds),
            id='subscription_refresh',
            replace_existing=True,
        )
        self._sched.start()
        logger.info("DataHub started, subscription refresh interval: %ss", self._refresh_seconds)

        # Start Internal API Server
        await self.start_api_server()


    async def stop(self):
        """Close pool we own – call on shutdown / tests tear‑down."""
        logger.info("DataHub shutting down.")

        # Stop Internal API Server
        await self.stop_api_server()

        # Stop Scheduler
        if self._sched and self._sched.state == STATE_RUNNING:
            self._sched.shutdown(wait=False)

        # Close Database Pool
        await self.close_pool()

    # ---------------------------------------------------------------------
    # Private Methods
    async def _load_provider_cls(self, name: str) -> bool:
        """
        Load Provider Class
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
                    Warning(f"Provider {name} not found in database.")
                    return False

            # Get Provider Info
            FILE_PATH = provider_reg_data['file_path']
            FILE_HASH = provider_reg_data['file_hash']
            NONCE = provider_reg_data['nonce']
            CIPHERTEXT = provider_reg_data['ciphertext']

            # Ensure the File Exsits
            if not FILE_PATH.startswith(ALLOWED_DYNAMIC_PATH):
                logger.warning(f"File {FILE_PATH} not in allowed path {ALLOWED_DYNAMIC_PATH}")
                Warning(f"File {FILE_PATH} not in allowed path {ALLOWED_DYNAMIC_PATH}")
                return False
            if not Path(FILE_PATH).is_file():
                logger.warning(f"File {FILE_PATH} not found")
                Warning(f"File {FILE_PATH} not found")
                return False
            
            # Verify File Hash
            sha256 = hashlib.sha256()
            with open(FILE_PATH, 'rb') as f:
                while chunk := f.read(8192):
                    sha256.update(chunk)
            sha256_hash = sha256.digest()
            if sha256_hash != FILE_HASH:
                logger.warning(f"File {FILE_PATH} hash does not match database hash. {FILE_HASH} != {sha256_hash}")
                Warning(f"File {FILE_PATH} hash does not match database hash")
                return False

            # Try Loading the Provider Class
            try:
                # Load the provider class from the file path
                ProviderCls = load_provider_from_file_path(FILE_PATH, name)
                logger.info(f"Provider {name} class loaded successfully.")
            except Exception as e:
                logger.warning(f"Unable to load provider {name} class. This provider will be skipped. Error message: {e}")
                Warning(f"Unable to load provider {name} class. This provider will be skipped.")
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
            self._providers[name] = prov
            logger.info(f"Provider {name} instance created successfully.")
            return True
        except Exception as e:
            logger.error(f"Error loading provider {name}: {e}", exc_info=True)
            return False

    async def _refresh_subscriptions(self):
        """
        Refresh Data Provider Subscriptions
        """
        logger.debug("Refreshing subscriptions.")
        # Fetch Current Subscriptions in the DB
        query = QUERIES['get_subscriptions']
        rows = await self.pool.fetch(query)

        # Load Provider Objects
        current_providers = set(self._providers)
        seen_providers = set(r["provider"] for r in rows)
        invalid_providers = set()
        for name in seen_providers - current_providers:
            didLoad = await self._load_provider_cls(name)
            if not didLoad:
                invalid_providers.add(name)

        # Drop Providers that Aren't Needed Anymore
        for obsolete in current_providers - seen_providers:
            logger.info(f"Removing obsolete provider from registry: {obsolete}")
            if hasattr(self._providers[obsolete], 'aclose'):
                await self._providers[obsolete].aclose()
            del self._providers[obsolete]
        
        # Update Scheduled Jobs
        new_keys = set()
        for r in rows:
            # Skip Providers that don't exist
            if r['provider'] in invalid_providers:
                continue

            key = f"{r['provider']}|{r['interval']}|{r['cron']}"
            new_keys.add(key)
            if key not in self.job_keys:
                # Subscription Schedule Detected
                prov_type = self._providers[r["provider"]].provider_type
                offset_seconds = 0 if prov_type == ProviderType.HISTORICAL else -1*DEFAULT_LIVE_OFFSET
                logger.debug(f"Scheduling new job: {key}, with offset: {offset_seconds}, from specified cron: {r['cron']}")
                self._sched.add_job(
                    func=self.get_data,
                    trigger=OffsetCronTrigger.from_crontab(r["cron"], offset_seconds=offset_seconds),
                    args=[r["provider"], r["interval"], r["syms"]],
                    id=key,
                )
            else:
                # Update Scheduled Job (symbol subscription may have changed)
                logger.debug(f"Updating scheduled job: {key}")
                self._sched.get_job(key).modify(
                    args=[r["provider"], r["interval"], r["syms"]]
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
            symbols: list[str]) -> list[Req]:
            """Build Req iterable to send to the data provider. """
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
            for sym in symbols:
                start = last_map.get(sym, default_start) + timedelta(days=1)
                if start <= yday:
                    reqs.append(Req(
                        sym=sym,
                        start=start,
                        end=yday,
                        interval=interval
                    ))

            return reqs

    async def _insert_bars(
            self,
            provider_type: ProviderType,
            provider: str,
            interval: str,
            bars: list[Bar]
    ):
        """Batch Insertion of records into QuasarDB"""
        dbs = ['historical_data', 'live_data']
        db = dbs[provider_type.value]
        logger.info(f'Inserting provider data into database {db}: {provider}, {interval}')
        records = [
            (b['ts'], b['sym'], provider, interval, b['o'], b['h'], b['l'], b['c'], b['v']) for b in bars
        ]
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(db, records=records)

    async def _handle_get_available_symbols(self, request: web.Request) -> web.Response:
        provider_name = request.match_info.get('provider_name')
        if not provider_name:
            return web.json_response({'error': 'Provider name missing'}, status=400)

        logger.info(f"API request: Get available symbols for provider '{provider_name}'")
        provider_instance = self._providers.get(provider_name)
        if not provider_instance:
            # Attempt to load the provider if not already loaded
            didLoad = await self._load_provider_cls(provider_name)
            if didLoad:
                provider_instance = self._providers.get(provider_name)

        if not provider_instance:
            logger.warning(f"Provider '{provider_name}' not found or not loaded for API request.")
            return web.json_response({'error': f"Provider '{provider_name}' not found or not loaded"}, status=404)

        if not hasattr(provider_instance, 'get_available_symbols'):
            logger.error(f"Provider '{provider_name}' does not implement get_available_symbols method.")
            return web.json_response({'error': f"Provider '{provider_name}' does not support symbol discovery"}, status=501)

        try:
            # Assuming get_available_symbols is an async method
            symbols = await provider_instance.get_available_symbols()
            # ProviderSymbolInfo is a TypedDict, which is inherently JSON serializable if its contents are.
            return web.json_response(symbols, status=200)
        except NotImplementedError:
            logger.error(f"get_available_symbols not implemented for provider '{provider_name}'.")
            return web.json_response({'error': f"Symbol discovery not implemented for provider '{provider_name}'"}, status=501)
        except Exception as e:
            logger.error(f"Error fetching symbols for provider '{provider_name}': {e}", exc_info=True)
            return web.json_response({'error': f"Internal server error while fetching symbols for '{provider_name}'"}, status=500)

    async def _validate_provider(self, request: web.Request) -> web.Response:
        """
        Validate Custom Provider Code
        Service to Service API endpoint
        """
        try:
            # Get File Path
            data = await request.json()
            file_path = data.get('file_path')
            if not file_path:
                return web.json_response({'error': 'Internal API Error, file path not provided to datahub'}, status=500)
            if not file_path.startswith(ALLOWED_DYNAMIC_PATH):
                return web.json_response({'error': f'File {file_path} not in allowed path {ALLOWED_DYNAMIC_PATH}'}, status=403)
            if not Path(file_path).is_file():
                return web.json_response({'error': f'File {file_path} not found'}, status=404)
            
            # Dynamically Import the Module
            module_name = Path(file_path).stem
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                return web.json_response({'error': f'Unable to load module {module_name} from {file_path}'}, status=500)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Check Class Definitions
            defined_classes = []
            for name, member_class in inspect.getmembers(module, inspect.isclass):
                if member_class.__module__ == module.__name__:
                    defined_classes.append(member_class)
            if not defined_classes:
                return web.json_response({'error': f'No classes found in {file_path}'}, status=500)
            if len(defined_classes) > 1:
                return web.json_response({'error': f'Multiple classes found in {file_path}'}, status=500)
            
            # Check if Class is the correct subclass
            the_class = defined_classes[0]
            is_valid_subclass = [issubclass(the_class, HistoricalDataProvider), \
                issubclass(the_class, LiveDataProvider)]
            if not any(is_valid_subclass):
                return web.json_response({'error': f'Class {the_class.__name__} in {file_path} is not a valid provider subclass'}, status=500)
            subclass_types = ['Historical', 'Live']
            subclass_type = list(compress(subclass_types, is_valid_subclass))[0]

            # Get Class Name Attribute
            class_name = None
            if hasattr(the_class, 'name'):
                class_name = getattr(the_class, 'name')
                if not isinstance(class_name, str):    
                    class_name = None
            if class_name is None:
                return web.json_response({'error': f'Class {the_class.__name__} in {file_path} does not have a valid name attribute'}, status=500)

            logger.info(f"Provider {class_name} validated successfully.")
            resp = web.json_response({
                'status': 'success',
                'class_name': class_name,
                'subclass_type': subclass_type,
                'module_name': module_name,
                'file_path': file_path
            }, status=200)
            return resp

        except Exception as e:
            logger.error(f"Error validating provider: {e}", exc_info=True)
            return web.json_response({'error': f'Internal API Error: {e}'}, status=500)

    @safe_job(default_return=None)
    async def get_data(self, provider: str, interval: str, symbols: list[str]):
        """
        Get Data from the DataHub
        """
        # Load Provider Class
        if provider not in self._providers:
            logger.error(f"Provider {provider} not found.")
            raise ValueError(f"Provider {provider} not found.")
        prov = self._providers[provider]

        if prov.provider_type == ProviderType.HISTORICAL:
            # Build Requests For Historical Data Provider
            reqs = await self._build_reqs_historical(provider, interval, symbols)
            if not reqs:
                logger.warning("{provider} has no valid requests to make.")
                Warning("{provider} has no valid requests to make.")
                return
            args = [reqs]
            kwargs = {}
        elif prov.provider_type == ProviderType.REALTIME:
            # Create Request for Live Data Provider
            args = [interval, symbols]
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
    """
    Dynamically loads a provider class from a given file path.
    Optionally checks if the loaded class has the expected_class_name.
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
        (issubclass(member_class, HistoricalDataProvider) or issubclass(member_class, LiveDataProvider)):
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