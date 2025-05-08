import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from typing import Optional
from datetime import datetime, timezone, timedelta
from functools import wraps

from quasar.common.secret_store import SecretStore
from quasar.common.offset_cron import OffsetCronTrigger
from quasar.providers import HistoricalDataProvider, Req, Bar, ProviderType, load_provider

import logging
logger = logging.getLogger(__name__)

DEFAULT_LIVE_OFFSET = 30 # Default number of seconds to offset the subscription cron job for live data providers
DEFAULT_LOOKBACK = 8000 # Default Number of bars to pull if we don't already have data
BATCH_SIZE = 500 # Number of bars to batch insert into the database 
QUERIES = {
    'get_subscriptions': """SELECT provider, interval, cron, array_agg(sym) AS syms
                            FROM provider_subscription
                            GROUP BY provider, interval, cron""",
     'get_last_updated': """SELECT sym, last_updated::date AS d
                            FROM   historical_symbol_state
                            WHERE  provider = $1
                            AND  sym = ANY($2::text[])"""
}

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


class DataHub:
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

    def __init__(
            self, *,
            secret_store: SecretStore,
            dsn: str | None = None,
            pool: Optional[asyncpg.Pool] = None,
            refresh_seconds: int = 30):

        # Secret Store
        self.secret_store = secret_store

        # Store DataProvider objects
        self._providers: dict[str, HistoricalDataProvider] = {}

        # Pool Setup
        if not dsn and not pool:
            logger.error("DataHub was intialized without DSN or Pool for database connection.")
            raise ValueError("Provide either dsn or pool")
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = pool
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

    # OBJECT LIFECYCLE
    # ---------------------------------------------------------------------
    async def start(self):
        """Create pool (if we own it) – call once at boot."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
        
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

    async def stop(self):
        """Close pool we own – call on shutdown / tests tear‑down."""
        logger.info("DataHub shutting down.")
        if self._sched and self._sched.state == STATE_RUNNING:
            self._sched.shutdown(wait=False)

        if self._pool is not None and not self._pool._closed:   # type: ignore
            await self._pool.close()

    # ---------------------------------------------------------------------
    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("DataHub not started yet")
        return self._pool

    # Subscription Refresh
    async def refresh_subscriptions(self):
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
            # Load Provider Configuration Secrets
            try:
                cfg = await self.secret_store.get(name)
                logger.info(f"Provider {name} configuration secrets loaded successfully.")
            except Exception as e:
                logger.warning(f"Unable to load provider {name} configuration data. This provider will be skipped. Error message: {e}")
                Warning(f"Unable to load provider {name} configuration data. This provider will be skipped.")
                invalid_providers.add(name)
                continue
            
            # Load Provider Code
            try:
                ProviderCls = load_provider(name)
                self._providers[name] = ProviderCls(**cfg)
                logger.info(f"Provider {name} class definition loaded successfully.")
            except Exception as e:
                logger.warning(f"Unable to load provider {name} class definition. This provider will be skipped. Error message: {e}")
                Warning(f"Unable to load provider {name} class definition. This provider will be skipped.")
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


