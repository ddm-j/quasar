"""Data collection handlers: scheduling, fetching, storage."""
import asyncio
import logging
import os
import warnings
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Callable, Awaitable

import aiohttp
from apscheduler.triggers.cron import CronTrigger
import asyncpg.exceptions

from quasar.lib.providers.core import ProviderType, Req, Bar, DataProvider
from quasar.lib.common.calendar import TradingCalendar
from quasar.lib.common.offset_cron import OffsetCronTrigger

from .base import HandlerMixin
from ..utils.constants import (
    QUERIES, BATCH_SIZE, DEFAULT_LOOKBACK,
    DEFAULT_LIVE_OFFSET, IMMEDIATE_PULL
)

# Registry service URL for index sync API calls
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://registry:8080")

logger = logging.getLogger(__name__)


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


class CollectionHandlersMixin(HandlerMixin):
    """Mixin providing data collection methods for DataHub."""

    async def refresh_index_sync_jobs(self):
        """Synchronize scheduled jobs for IndexProvider constituent sync."""
        logger.debug("Refreshing index sync jobs.")

        # Fetch IndexProviders with their sync_frequency preferences
        query = QUERIES['get_index_providers_sync_config']
        rows = await self.pool.fetch(query)

        # Build new job keys and schedule jobs
        new_keys = set()
        for r in rows:
            provider_name = r['class_name']
            sync_frequency = r['sync_frequency']  # Already has COALESCE default of '1w'
            job_key = f"index_sync_{provider_name}"
            new_keys.add(job_key)

            # Look up cron template for this frequency
            cron = await self.pool.fetchval(
                "SELECT cron FROM accepted_intervals WHERE interval = $1",
                sync_frequency
            )
            if cron is None:
                logger.warning(
                    f"No cron template found for sync_frequency '{sync_frequency}' "
                    f"for provider {provider_name}. Skipping."
                )
                new_keys.discard(job_key)
                continue

            if job_key not in self.index_sync_job_keys:
                # New job - schedule it
                logger.info(f"Scheduling index sync job for {provider_name} with frequency {sync_frequency}")
                self._sched.add_job(
                    func=self.sync_index_constituents,
                    trigger=CronTrigger.from_crontab(cron),
                    args=[provider_name],
                    id=job_key,
                )
            else:
                # Job exists - check if cron needs to be updated
                existing_job = self._sched.get_job(job_key)
                if existing_job is not None:
                    # Replace the job with updated trigger
                    logger.debug(f"Updating index sync job for {provider_name}")
                    self._sched.remove_job(job_key)
                    self._sched.add_job(
                        func=self.sync_index_constituents,
                        trigger=CronTrigger.from_crontab(cron),
                        args=[provider_name],
                        id=job_key,
                    )

        # Remove obsolete jobs (providers that were deleted)
        for gone in self.index_sync_job_keys - new_keys:
            logger.info(f"Removing index sync job: {gone}")
            job = self._sched.get_job(gone)
            if job is not None:
                self._sched.remove_job(gone)

        self.index_sync_job_keys = new_keys

    @safe_job(default_return=None)
    async def sync_index_constituents(self, provider_name: str):
        """Sync constituents for an IndexProvider to the registry.

        This method is called by scheduled jobs to fetch index constituents
        and post them to the registry service.

        Args:
            provider_name (str): Name of the IndexProvider to sync.
        """
        logger.info(f"Index sync started: {provider_name}")

        try:
            # Load provider if not already loaded
            if provider_name not in self._providers:
                loaded = await self.load_provider_cls(provider_name)
                if not loaded:
                    raise ValueError(f"Failed to load IndexProvider: {provider_name}")

            provider = self._providers[provider_name]

            # Fetch constituents from the provider
            constituents = await provider.get_constituents()
            logger.info(f"Index sync: {provider_name} fetched {len(constituents)} constituents")

            # POST to Registry sync endpoint
            url = f"{REGISTRY_URL}/api/registry/indices/{provider_name}/sync"
            payload = {"constituents": constituents}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(
                            f"Index sync complete: {provider_name} - "
                            f"added={result.get('members_added', 0)}, "
                            f"removed={result.get('members_removed', 0)}, "
                            f"unchanged={result.get('members_unchanged', 0)}"
                        )
                    else:
                        error_text = await response.text()
                        raise RuntimeError(
                            f"Registry sync failed for {provider_name}: "
                            f"status={response.status}, body={error_text}"
                        )
        except Exception as e:
            logger.error(f"Index sync failed: {provider_name} - {e}")
            raise

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
                # Get scheduling preferences for the provider
                prefs = self._provider_preferences.get(r["provider"]) or {}
                scheduling_prefs = prefs.get("scheduling") or {}

                if prov_type == ProviderType.HISTORICAL:
                    # Historical providers: positive offset delays job execution
                    delay_hours = scheduling_prefs.get("delay_hours", 0)
                    offset_seconds = delay_hours * 3600
                else:
                    # Live providers: negative offset starts before close
                    pre_close_seconds = scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET)
                    offset_seconds = -1 * pre_close_seconds
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

            # Get lookback_days from provider preferences, fallback to DEFAULT_LOOKBACK
            prefs = self._provider_preferences.get(provider) or {}
            data_prefs = prefs.get("data") or {}
            lookback_days = data_prefs.get("lookback_days", DEFAULT_LOOKBACK)
            using_custom_lookback = "lookback_days" in data_prefs

            reqs: list[Req] = []
            default_start = yday - timedelta(days=lookback_days)
            for sym, mic in zip(symbols, exchanges):
                last_updated = last_map.get(sym)

                if last_updated is None:
                    # New Subscription: Bypass calendar check and pull lookback_days bars
                    start = default_start + timedelta(days=1)
                    if using_custom_lookback:
                        logger.info(
                            f"New subscription for {sym} ({mic}). "
                            f"Applying configured lookback_days={lookback_days} (preference). "
                            f"Requesting backfill from {start}."
                        )
                    else:
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
            conn: 'asyncpg.Connection',
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
            # Get scheduling preferences for timeout calculation
            prefs = self._provider_preferences.get(provider) or {}
            scheduling_prefs = prefs.get("scheduling") or {}
            pre_close_seconds = scheduling_prefs.get("pre_close_seconds", DEFAULT_LIVE_OFFSET)
            post_close_seconds = scheduling_prefs.get("post_close_seconds", prov.close_buffer_seconds)
            # Timeout = pre_close + post_close + 30s buffer for processing
            kwargs = {'timeout': pre_close_seconds + post_close_seconds + 30}
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
