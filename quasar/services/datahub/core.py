"""DataHub service core: scheduler, provider loading, and API handlers."""

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from typing import Optional, Annotated
from datetime import datetime, timezone
from fastapi import HTTPException, Query
import os

from quasar.lib.common.secret_store import SecretStore
from quasar.lib.common.database_handler import DatabaseHandler
from quasar.lib.common.api_handler import APIHandler
from quasar.lib.common.context import SystemContext
from quasar.lib.providers import HistoricalDataProvider
from quasar.lib.common.enum_guard import validate_enums
from quasar.services.datahub.schemas import (
    ProviderValidateRequest, ProviderValidateResponse,
    AvailableSymbolsResponse, ConstituentsResponse,
    SymbolSearchResponse, SymbolSearchItem, OHLCDataResponse, OHLCBar,
    SymbolMetadataResponse, DataTypeInfo, AssetInfo, OtherProvider
)
from quasar.services.datahub.utils.constants import ALLOWED_DYNAMIC_PATH
from quasar.services.datahub.handlers.collection import CollectionHandlersMixin, safe_job
from quasar.services.datahub.handlers.providers import ProviderHandlersMixin, load_provider_from_file_path

import logging
logger = logging.getLogger(__name__)

ENUM_GUARD_MODE = os.getenv("ENUM_GUARD_MODE", "off").lower()


class DataHub(ProviderHandlersMixin, CollectionHandlersMixin, DatabaseHandler, APIHandler):
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