"""Built-in index provider for CCI30 (Crypto Currency Index 30)."""

from __future__ import annotations

import math
from datetime import date
from io import StringIO

import pandas as pd

from quasar.lib.enums import AssetClass
from quasar.lib.providers.core import IndexProvider, IndexConstituent
from quasar.lib.providers import register_provider


ENDPOINT = "https://cci30.com/ajax/getConstituentsDivProd.php"


@register_provider
class CCI30Provider(IndexProvider):
    """Index provider for the CCI30 cryptocurrency index.

    The CCI30 index tracks the top 30 cryptocurrencies by market cap,
    weighted using the square root of market capitalization.

    See: https://cci30.com/
    """

    name = "CCI30"
    RATE_LIMIT = (10, 60)  # Conservative rate limit

    async def _fetch_json(self, url: str) -> dict:
        """Fetch JSON from URL, handling text/html content type.

        The CCI30 API returns JSON with text/html mimetype, which aiohttp
        rejects by default. This method handles that case.
        """
        async with self._limiter:
            async with self._session.get(url) as r:
                # CCI30 returns JSON with text/html content type
                return await r.json(content_type=None)

    async def fetch_constituents(
        self,
        as_of_date: date | None = None
    ) -> list[IndexConstituent]:
        """Fetch current CCI30 index constituents.

        Args:
            as_of_date: Not supported by this provider. If provided, it is ignored
                        and current constituents are returned.

        Returns:
            List of constituents with symbols and market-cap-weighted weights.
        """
        # Fetch data from CCI30 API
        data = await self._fetch_json(ENDPOINT)

        # Parse HTML tables from the response
        html = data["constituentsDiv"]
        tables = pd.read_html(StringIO(html))

        # Combine all tables (CCI30 splits constituents across multiple tables)
        df = pd.concat(tables, ignore_index=True) if len(tables) > 1 else tables[0]

        # Extract symbols from the Name column (e.g., "Bitcoin BTC" -> "BTC")
        df["symbol"] = (
            df["Name"]
            .astype(str)
            .str.extract(r"\b([A-Z0-9]{2,10})\b")[0]
        )

        # Extract name by removing the symbol from the Name column
        df["asset_name"] = (
            df["Name"]
            .astype(str)
            .str.replace(r"\b[A-Z0-9]{2,10}\b", "", regex=True)
            .str.strip()
        )

        # Parse market cap: remove $ and commas, convert to float
        df["market_cap"] = (
            df["Market cap"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .astype(float)
        )

        # Calculate weights using square root of market cap
        # This is the CCI30 weighting methodology
        sqrt_market_caps = df["market_cap"].apply(math.sqrt)
        total_sqrt = sqrt_market_caps.sum()
        df["weight"] = sqrt_market_caps / total_sqrt

        # Build constituent list with enriched metadata
        constituents: list[IndexConstituent] = []
        for _, row in df.iterrows():
            symbol = row["symbol"]
            if pd.isna(symbol) or not symbol:
                continue

            constituents.append({
                "symbol": symbol,
                "weight": float(row["weight"]),
                "name": row["asset_name"] if pd.notna(row["asset_name"]) else None,
                "asset_class": AssetClass.CRYPTO.value,
                "matcher_symbol": symbol,
                "base_currency": symbol,
                "quote_currency": "USD",
            })

        return constituents
