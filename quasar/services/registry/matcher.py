"""Identity matching utility for resolving asset primary identifiers from manifest data."""

import logging
import time
from typing import List, Optional
from dataclasses import dataclass
import asyncpg

from quasar.lib.common.database_handler import DatabaseHandler

logger = logging.getLogger(__name__)

# Matching parameters (tuned via empirical testing)
SYM_BOOST = 50.0
EXCHANGE_BOOST = 35.0
NAME_BOOST = 8.0
FUZZY_THRESHOLD = 0.35
AUTO_THRESHOLD = 80.0
FUZZY_BATCH_SIZE = 100


@dataclass
class MatchResult:
    """Represents a matching result for an asset."""
    asset_id: int
    primary_id: str
    identity_symbol: str
    identity_name: str
    confidence: float
    match_type: str

class IdentityMatcher(DatabaseHandler):
    """Utility for identifying assets against the identity manifest."""

    name = "IdentityMatcher"

    def __init__(self, dsn: Optional[str] = None, pool: Optional[asyncpg.Pool] = None):
        super().__init__(dsn=dsn, pool=pool)

    async def identify_unidentified_assets(
        self,
        class_name: str,
        class_type: str
    ) -> List[MatchResult]:
        """
        Identify unidentified assets for a specific provider/broker.

        Args:
            class_name: Name of the class (e.g., 'EODHD')
            class_type: Type of the class ('provider' or 'broker')

        Returns:
            List of matches with confidence scores.
        """
        method_start = time.time()
        logger.info(f"IdentityMatcher: Identifying assets for {class_name} ({class_type})")

        query = """
            SELECT id, symbol, name, exchange, asset_class_group, matcher_symbol
            FROM assets
            WHERE class_name = $1 AND class_type = $2
              AND primary_id IS NULL
              AND asset_class_group IS NOT NULL
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, class_name, class_type)

        if not rows:
            logger.info(f"No unidentified assets found for {class_name}")
            return []

        results = await self._process_matching(rows)

        logger.info(
            f"Performance[identify_unidentified_assets]: "
            f"assets={len(rows)}, results={len(results)}, time={time.time() - method_start:.3f}s"
        )
        return results

    async def identify_all_unidentified_assets(self) -> List[MatchResult]:
        """
        Identify all unidentified assets across all providers.

        Returns:
            List of matches with confidence scores.
        """
        method_start = time.time()
        logger.info("IdentityMatcher: Identifying all unidentified assets")

        query = """
            SELECT id, symbol, name, exchange, asset_class_group, matcher_symbol
            FROM assets
            WHERE primary_id IS NULL
              AND asset_class_group IS NOT NULL
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)

        if not rows:
            logger.info("No unidentified assets found")
            return []

        results = await self._process_matching(rows)

        logger.info(
            f"Performance[identify_all_unidentified_assets]: "
            f"assets={len(rows)}, results={len(results)}, time={time.time() - method_start:.3f}s"
        )
        return results

    async def _process_matching(self, asset_rows: List[asyncpg.Record]) -> List[MatchResult]:
        """Run matching pipeline by asset class group."""
        securities_assets = [r for r in asset_rows if r['asset_class_group'] == 'securities']
        crypto_assets = [r for r in asset_rows if r['asset_class_group'] == 'crypto']

        results = []

        if securities_assets:
            results.extend(await self._run_matching_for_group(securities_assets, 'securities'))

        if crypto_assets:
            results.extend(await self._run_matching_for_group(crypto_assets, 'crypto'))

        return results

    async def _run_matching_for_group(
        self,
        assets: List[asyncpg.Record],
        group: str
    ) -> List[MatchResult]:
        """Run two-phase matching for a specific asset class group."""
        method_start = time.time()

        exact_results = await self._run_exact_matching(assets, group)
        matched_ids = {res.asset_id for res in exact_results}
        unmatched_assets = [r for r in assets if r['id'] not in matched_ids]

        fuzzy_results = []
        if unmatched_assets:
            fuzzy_results = await self._run_fuzzy_matching(unmatched_assets, group)

        logger.info(
            f"Performance[_run_matching_for_group]: "
            f"group={group}, assets={len(assets)}, "
            f"exact={len(exact_results)}, fuzzy={len(fuzzy_results)}, "
            f"time={time.time() - method_start:.3f}s"
        )

        return exact_results + fuzzy_results

    async def _run_exact_matching(
        self,
        assets: List[asyncpg.Record],
        group: str
    ) -> List[MatchResult]:
        """Exact alias/symbol matching via array overlap."""
        asset_ids = [r['id'] for r in assets]
        matcher_symbols = [r['matcher_symbol'] for r in assets]

        query = """
            WITH input AS (
                SELECT unnest($1::int[]) as id, unnest($2::text[]) as matcher_symbol
            )
            SELECT
                i.id as asset_id,
                im.primary_id,
                im.symbol as identity_symbol,
                im.name as identity_name,
                100.0 as confidence,
                'exact_alias' as match_type
            FROM input i
            JOIN identity_manifest im ON (
                im.asset_class_group = $3 AND
                string_to_array(im.symbol, ';') && ARRAY[i.matcher_symbol]
            )
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, asset_ids, matcher_symbols, group)

        return [MatchResult(**dict(r)) for r in rows]

    async def _run_fuzzy_matching(
        self,
        assets: List[asyncpg.Record],
        group: str
    ) -> List[MatchResult]:
        """Fuzzy matching using GIN trigram index with batching."""
        batches = [assets[i:i+FUZZY_BATCH_SIZE]
                   for i in range(0, len(assets), FUZZY_BATCH_SIZE)]

        results = []
        for batch in batches:
            results.extend(await self._process_fuzzy_batch(batch, group))

        return results

    async def _process_fuzzy_batch(
        self,
        assets: List[asyncpg.Record],
        group: str
    ) -> List[MatchResult]:
        """Process a batch of assets for fuzzy matching using trigram similarity."""
        asset_ids = [r['id'] for r in assets]
        matcher_symbols = [r['matcher_symbol'] for r in assets]
        names = [r['name'] or '' for r in assets]
        exchanges = [r['exchange'] or '' for r in assets]

        query = """
            WITH asset_input AS (
                SELECT
                    unnest($1::int[]) as id,
                    unnest($2::text[]) as matcher_symbol,
                    unnest($3::text[]) as name,
                    unnest($4::text[]) as exchange
            ),
            candidates AS (
                SELECT
                    ai.id as asset_id,
                    ai.matcher_symbol,
                    ai.name as asset_name,
                    ai.exchange as asset_exchange,
                    cand.primary_id,
                    cand.symbol as identity_symbol,
                    cand.name as identity_name,
                    cand.exchange as identity_exchange,
                    cand.sym_sim
                FROM asset_input ai
                CROSS JOIN LATERAL (
                    SELECT
                        im.primary_id,
                        im.symbol,
                        im.name,
                        im.exchange,
                        similarity(ai.matcher_symbol, im.symbol) as sym_sim
                    FROM identity_manifest im
                    WHERE im.asset_class_group = $5
                      AND im.symbol % ai.matcher_symbol
                    LIMIT 20
                ) cand
            ),
            scored AS (
                SELECT
                    asset_id,
                    primary_id,
                    identity_symbol,
                    identity_name,
                    (
                        CASE
                            WHEN sym_sim > 0.8 THEN 80.0
                            WHEN sym_sim > 0.6 THEN 60.0
                            ELSE sym_sim * $6
                        END +
                        CASE WHEN asset_exchange = identity_exchange THEN $7 ELSE 0.0 END +
                        COALESCE(similarity(asset_name, identity_name), 0) * $8
                    ) as confidence,
                    'fuzzy_symbol' as match_type
                FROM candidates
            ),
            ranked AS (
                SELECT
                    asset_id,
                    primary_id,
                    identity_symbol,
                    identity_name,
                    confidence,
                    match_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY asset_id
                        ORDER BY confidence DESC
                    ) as rn
                FROM scored
            )
            SELECT
                asset_id,
                primary_id,
                identity_symbol,
                identity_name,
                confidence,
                match_type
            FROM ranked
            WHERE rn = 1
              AND confidence >= $9
            ORDER BY confidence DESC
        """

        async with self.pool.acquire() as conn:
            await conn.execute(f"SET pg_trgm.similarity_threshold = {FUZZY_THRESHOLD}")
            rows = await conn.fetch(
                query,
                asset_ids,
                matcher_symbols,
                names,
                exchanges,
                group,
                SYM_BOOST,
                EXCHANGE_BOOST,
                NAME_BOOST,
                AUTO_THRESHOLD
            )

        return [MatchResult(**dict(r)) for r in rows]

