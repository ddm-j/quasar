import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from typing import Optional
from datetime import datetime, timezone, timedelta

from quasar.secret_store import SecretStore
from quasar.provider_base import HistoricalDataProvider, Req, Bar
from quasar.providers import load_provider

import logging
logger = logging.getLogger(__name__)

DEFAULT_LOOKBACK = 8000 # Default Number of bars to pull if we don't already have data
BATCH_SIZE = 500 # 
QUERIES = {
    'get_subscriptions': """SELECT provider, interval, cron, array_agg(sym) AS syms
                            FROM provider_subscription
                            GROUP BY provider, interval, cron""",
     'get_last_updated': """SELECT sym, last_updated::date AS d
                            FROM   symbol_state
                            WHERE  provider = $1
                            AND  sym = ANY($2::text[])"""
}


class DataHub:
    """
    Minimal skeleton – just opens and closes a connection pool.

    Parameters
    ----------
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

    # placeholder for future logic
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
                logger.debug(f"Scheduling new job: {key}")
                self._sched.add_job(
                    func=self._provider_job,
                    trigger=CronTrigger.from_crontab(r["cron"]),
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
            logger.info(f"Removing scheduled job: {key}")
            self._sched.remove_job(gone)

        self.job_keys = new_keys

        # if self._sched.state == STATE_RUNNING:
        #     for j in self._sched.get_jobs():
        #         print(j.id, j.next_run_time)

    async def _build_reqs(
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
            provider: str,
            interval: str,
            bars: list[Bar]
    ):
        """Batch Insertion of records into QuasarDB"""
        logger.info(f'Inserting provider data into database: {provider}, {interval}')
        records = [
            (b['ts'], b['sym'], provider, interval, b['o'], b['h'], b['l'], b['c'], b['v']) for b in bars
        ]
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table('historical_data', records=records)

    async def _provider_job(
            self,
            provider: str,
            interval: str,
            symbols: list[str]):
        logger.info(f"Running scheduled job: {provider}, {interval}")

        # Build Requests
        reqs = await self._build_reqs(
            provider,
            interval,
            symbols
        )
        if not reqs:
            logger.warning("Provider job has no valid requests to make.")
            Warning(f"No requests made for job: {provider}")
            return

        # Pull / Insert Data Into QuasarDB
        prov = self._providers[provider]
        buf = []
        logger.info(f"Requesting data from provider.")
        async for bar in prov.get_history_many(reqs):
            buf.append(bar)
            if len(buf) >= BATCH_SIZE:
                await self._insert_bars(provider, interval, buf)
                buf.clear()
        if buf:
            await self._insert_bars(provider, interval, buf)

