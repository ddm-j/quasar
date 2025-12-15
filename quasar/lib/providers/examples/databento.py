"""Built-in historical data provider for Databento US Equities."""

from quasar.lib.enums import AssetClass, Interval
from quasar.lib.providers.core import HistoricalDataProvider, Bar, SymbolInfo, Req
from quasar.lib.providers import register_provider
from datetime import date, datetime, timezone, timedelta
from typing import AsyncIterator, Iterable
import aiohttp
import logging

logger = logging.getLogger(__name__)

BASE = 'https://hist.databento.com/v0'


@register_provider
class DatabentoProvider(HistoricalDataProvider):
    """Historical data provider for Databento US Equities (EQUS.SUMMARY dataset)."""
    
    name = 'DATABENTO'
    RATE_LIMIT = (100, 1)  # 100 requests per second per Databento limits

    def __init__(self, context):
        """Initialize provider.
        
        Args:
            context: Context containing provider secrets (api_key).
        """
        super().__init__(context)

    async def _api_get(self, url: str) -> list:
        """Perform a rate-limited HTTP GET with Authorization header.

        Args:
            url (str): URL to fetch.

        Returns:
            list: Parsed JSON response (handles NDJSON format).

        Raises:
            aiohttp.ClientResponseError: If the request fails.
        """
        import base64
        import json
        
        # Databento uses Basic auth with API key as username and empty password
        auth_string = base64.b64encode(f"{self.context.get('api_key')}:".encode()).decode()
        headers = {"Authorization": f"Basic {auth_string}"}
        
        async with self._limiter:
            async with self._session.get(url, headers=headers) as r:
                if r.status == 401:
                    body = await r.text()
                    logger.error(f"Auth failed. Status: {r.status}, Body: {body}")
                    raise ValueError(f"Databento API authentication failed. Check your api_key. Response: {body}")
                if r.status == 404:
                    # No data found for the request
                    return []
                if r.status >= 400:
                    body = await r.text()
                    logger.error(f"API error. Status: {r.status}, Body: {body}")
                    r.raise_for_status()
                
                # Databento returns NDJSON (newline-delimited JSON)
                # Each line is a separate JSON object
                body = await r.text()
                if not body.strip():
                    return []
                
                records = []
                for line in body.strip().split('\n'):
                    if line.strip():
                        records.append(json.loads(line))
                return records

    async def _api_get_json(self, url: str) -> dict:
        """Perform a rate-limited HTTP GET for standard JSON responses.

        Args:
            url (str): URL to fetch.

        Returns:
            dict: Parsed JSON response.

        Raises:
            aiohttp.ClientResponseError: If the request fails.
        """
        import base64
        
        # Databento uses Basic auth with API key as username and empty password
        auth_string = base64.b64encode(f"{self.context.get('api_key')}:".encode()).decode()
        headers = {"Authorization": f"Basic {auth_string}"}
        
        async with self._limiter:
            async with self._session.get(url, headers=headers) as r:
                if r.status == 401:
                    body = await r.text()
                    logger.error(f"Auth failed. Status: {r.status}, Body: {body}")
                    raise ValueError(f"Databento API authentication failed. Check your api_key.")
                if r.status >= 400:
                    body = await r.text()
                    logger.error(f"API error. Status: {r.status}, Body: {body}")
                    r.raise_for_status()
                return await r.json()

    async def _get_dataset_range(self) -> tuple[date, date]:
        """Fetch available date range for EQUS.SUMMARY dataset.
        
        Uses the metadata.get_dataset_range endpoint to determine what dates
        have data available.
        
        Returns:
            tuple: (available_start, available_end) as date objects.
        """
        url = f"{BASE}/metadata.get_dataset_range?dataset=EQUS.SUMMARY"
        
        data = await self._api_get_json(url)
        
        # Parse ISO timestamps to date objects
        # Response format: {"start": "2024-07-01T00:00:00.000000000Z", "end": "2025-12-06T00:00:00.000000000Z", ...}
        start_str = data.get('start', '')
        end_str = data.get('end', '')
        
        # Parse the ISO format timestamps
        available_start = datetime.fromisoformat(start_str.replace('Z', '+00:00')).date()
        available_end = datetime.fromisoformat(end_str.replace('Z', '+00:00')).date()
        
        logger.info(f"Databento EQUS.SUMMARY available range: {available_start} to {available_end}")
        
        return (available_start, available_end)

    def _clamp_dates(
        self,
        start: date,
        end: date,
        available_start: date,
        available_end: date
    ) -> tuple[date, date, bool]:
        """Clamp requested dates to available range.
        
        Args:
            start: Requested start date.
            end: Requested end date.
            available_start: First date with available data.
            available_end: Last date with available data.
            
        Returns:
            tuple: (clamped_start, clamped_end, was_clamped)
        """
        clamped_start = max(start, available_start)
        clamped_end = min(end, available_end)
        was_clamped = (clamped_start != start) or (clamped_end != end)
        return clamped_start, clamped_end, was_clamped

    async def _fetch_bars(
        self,
        sym: str,
        start: date,
        end: date,
        interval: Interval,
    ) -> AsyncIterator[Bar]:
        """Fetch bars from Databento API for pre-validated date range.
        
        Internal method that assumes dates are already clamped to available range.
        
        Args:
            sym: Symbol to fetch (e.g., 'AAPL', 'MSFT').
            start: Start date (inclusive, pre-validated).
            end: End date (inclusive, pre-validated).
            interval: Bar interval (only '1d' supported currently).
            
        Yields:
            Bar: OHLCV bars ordered oldest to newest.
            
        Raises:
            ValueError: If interval is not supported.
        """
        # Validate interval - only 1d supported initially
        if interval != Interval.I_1D:
            raise ValueError(
                f"Unsupported interval: {interval}. "
                f"DatabentoProvider currently only supports '1d' interval."
            )
        
        # Build API URL with query parameters
        start_ts = f"{start.isoformat()}T00:00:00Z"
        end_ts = f"{end.isoformat()}T23:59:59Z"
        
        url = (
            f"{BASE}/timeseries.get_range"
            f"?dataset=EQUS.SUMMARY"
            f"&symbols={sym}"
            f"&schema=ohlcv-1d"
            f"&start={start_ts}"
            f"&end={end_ts}"
            f"&encoding=json"
        )
        
        # Make Request (Uses Built-in Rate Limiter)
        data = await self._api_get(url)
        
        # Handle empty response
        if not data:
            logger.warning(f"No data returned for {sym} from {start} to {end}")
            return
        
        # Process and yield bars
        for record in data:
            # ts_event is nested under 'hd' (header), in nanoseconds
            hd = record.get('hd', {})
            ts_ns = int(hd.get('ts_event', 0))
            ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
            
            # Extract OHLCV values - Databento returns prices scaled by 1e9
            # Price format: "229520000000" = $229.52
            open_val = int(record.get('open', 0)) / 1e9
            high_val = int(record.get('high', 0)) / 1e9
            low_val = int(record.get('low', 0)) / 1e9
            close_val = int(record.get('close', 0)) / 1e9
            volume_val = int(record.get('volume', 0))
            
            yield Bar(
                ts=ts,
                sym=sym,
                o=float(open_val),
                h=float(high_val),
                l=float(low_val),
                c=float(close_val),
                v=volume_val,
            )

    async def get_history_many(self, reqs: Iterable[Req]) -> AsyncIterator[Bar]:
        """Yield bars for multiple requests with single range lookup.
        
        Overrides base class to fetch dataset range once per batch instead of
        per-symbol, reducing API calls.
        
        Args:
            reqs: Iterable of Req objects containing symbol, date range, interval.
            
        Yields:
            Bar: OHLCV bars ordered oldest to newest per symbol.
        """
        reqs = list(reqs)
        if not reqs:
            return
        
        # Fetch range ONCE for entire batch
        available_start, available_end = await self._get_dataset_range()
        
        for req in reqs:
            # Clamp dates to available range
            start, end, was_clamped = self._clamp_dates(
                req.start, req.end, available_start, available_end
            )
            
            if was_clamped:
                logger.warning(
                    f"Date range adjusted for {req.sym}: "
                    f"available data is {available_start} to {available_end}"
                )
            
            # Skip if no valid range after clamping
            if start > end:
                logger.warning(f"No data available for {req.sym} in requested range")
                continue
            
            # Fetch bars with pre-validated dates
            async for bar in self._fetch_bars(req.sym, start, end, req.interval):
                yield bar

    async def get_available_symbols(self) -> list[SymbolInfo]:
        """Return available symbols from Databento EQUS.SUMMARY dataset.
        
        Fetches instrument definitions using the 'definition' schema with
        ALL_SYMBOLS to discover all available US equity symbols.
        
        Note: This is a billable API call charged per byte consumed.
        Uses a single day to minimize data transfer costs since the
        definition schema returns all active instruments for that date.
        """
        # Use dataset range to determine valid date for symbol query
        available_start, available_end = await self._get_dataset_range()
        
        # Request definitions for a single day
        # Note: available_end is exclusive, so subtract 1 day to get the last valid date
        # The definition schema returns ALL active instruments for that date,
        # so multiple days are unnecessary and only increase data costs
        query_date = available_end - timedelta(days=1)
        start_date = query_date
        end_date = query_date
        
        url = (
            f"{BASE}/timeseries.get_range"
            f"?dataset=EQUS.SUMMARY"
            f"&symbols=ALL_SYMBOLS"
            f"&schema=definition"
            f"&start={start_date.isoformat()}T00:00:00Z"
            f"&end={end_date.isoformat()}T23:59:59Z"
            f"&encoding=json"
        )
        
        logger.info("Fetching instrument definitions from Databento EQUS.SUMMARY...")
        data = await self._api_get(url)
        
        if not data:
            logger.warning("No instrument definitions returned from Databento")
            return []
        
        symbol_info = []
        seen_symbols = set()  # Avoid duplicates
        
        for record in data:
            raw_symbol = record.get('raw_symbol') or record.get('symbol')
            if not raw_symbol or raw_symbol in seen_symbols:
                continue
            seen_symbols.add(raw_symbol)
            
            # Extract instrument metadata
            instrument_class = record.get('instrument_class', '')
            exchange = record.get('exchange', '') or 'US'
            
            # instrument_id is nested under 'hd' (header)
            hd = record.get('hd', {})
            instrument_id = hd.get('instrument_id', '')
            
            # Map instrument class to asset class
            # Databento instrument classes: 'K' = Stock, 'F' = Future, 'O' = Option
            asset_class = AssetClass.EQUITY.value  # Default for EQUS dataset
            if instrument_class == 'F':
                asset_class = AssetClass.FUTURE.value
            elif instrument_class == 'O':
                asset_class = AssetClass.OPTION.value
            elif instrument_class == 'B':
                asset_class = AssetClass.BOND.value
            
            # Currency from record, default to USD for US equities
            currency = record.get('currency', '') or 'USD'
            
            syminfo = SymbolInfo(
                provider=self.name,
                provider_id=str(instrument_id) if instrument_id else raw_symbol,
                isin=None,  # Databento doesn't provide ISIN
                symbol=raw_symbol,
                name=raw_symbol,  # Databento doesn't provide description in definition
                exchange=exchange,
                asset_class=asset_class,
                base_currency=currency,
                quote_currency=None,
                country='US'
            )
            symbol_info.append(syminfo)
        
        logger.info(f"Retrieved {len(symbol_info)} symbols from Databento EQUS.SUMMARY")
        return symbol_info

    async def get_history(
        self,
        sym: str,
        start: date,
        end: date,
        interval: Interval,
    ) -> AsyncIterator[Bar]:
        """Yield historical bars from Databento for the given symbol and range.
        
        Standalone method for single-symbol requests. Fetches dataset range,
        clamps dates, and delegates to _fetch_bars.

        Args:
            sym: Symbol to fetch (e.g., 'AAPL', 'MSFT').
            start: Start date (inclusive).
            end: End date (inclusive).
        interval: Bar interval (only '1d' supported currently).

        Yields:
            Bar: OHLCV bars ordered oldest to newest.

        Raises:
            ValueError: If interval is not supported.
        """
        # Get available range and clamp dates
        available_start, available_end = await self._get_dataset_range()
        start, end, was_clamped = self._clamp_dates(start, end, available_start, available_end)
        
        if was_clamped:
            logger.warning(
                f"Date range adjusted for {sym}: "
                f"available data is {available_start} to {available_end}"
            )
        
        # Check if range is still valid after clamping
        if start > end:
            logger.warning(f"No data available for {sym} in requested range")
            return
        
        # Delegate to internal fetch method
        async for bar in self._fetch_bars(sym, start, end, interval):
            yield bar
