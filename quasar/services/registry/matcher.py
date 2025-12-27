"""Identity matching utility for resolving asset ISINs from manifest data."""

import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import asyncpg

from quasar.lib.common.database_handler import DatabaseHandler

logger = logging.getLogger(__name__)

# Production-tuned parameters from research phase
SYM_BOOST = 50.0
EXCHANGE_BOOST = 35.0
NAME_BOOST = 8.0
FUZZY_THRESHOLD = 0.35
AUTO_THRESHOLD = 80.0

@dataclass
class MatchResult:
    """Represents a matching result for an asset."""
    asset_id: int
    isin: str
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
        Attempt to identify unidentified assets for a specific provider/broker.
        
        Args:
            class_name: Name of the class (e.g., 'EODHD')
            class_type: Type of the class ('provider' or 'broker')
            
        Returns:
            List[MatchResult]: Discovered matches with confidence scores.
        """
        logger.info(f"IdentityMatcher: Identifying assets for {class_name} ({class_type})")
        
        # 1. Fetch unidentified assets that belong to a matching group
        query = """
            SELECT id, symbol, name, exchange, asset_class_group, sym_norm_root
            FROM assets
            WHERE class_name = $1 AND class_type = $2
              AND isin IS NULL
              AND asset_class_group IS NOT NULL
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, class_name, class_type)
            
            if not rows:
                logger.info(f"No unidentified assets found for {class_name}")
                return []
            
            logger.info(f"Found {len(rows)} unidentified assets to process")
            
            # 2. Run matching phases
            return await self._process_matching(rows)

    async def identify_all_unidentified_assets(self) -> List[MatchResult]:
        """
        Attempt to identify all unidentified assets across all providers.
        
        Returns:
            List[MatchResult]: Discovered matches with confidence scores.
        """
        logger.info("IdentityMatcher: Identifying all unidentified assets")
        
        query = """
            SELECT id, symbol, name, exchange, asset_class_group, sym_norm_root
            FROM assets
            WHERE isin IS NULL
              AND asset_class_group IS NOT NULL
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            if not rows:
                logger.info("No unidentified assets found")
                return []
            
            logger.info(f"Found {len(rows)} unidentified assets to process")
            return await self._process_matching(rows)

    async def _process_matching(self, asset_rows: List[asyncpg.Record]) -> List[MatchResult]:
        """Unified matching pipeline: Exact Phase followed by Fuzzy Phase."""
        # Split rows by asset_class_group for targeted matching
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
        # Phase 1: Fast Exact Alias Matching
        exact_results = await self._run_exact_matching(assets, group)
        
        # Determine which assets are still unmatched
        matched_ids = {res.asset_id for res in exact_results}
        unmatched_assets = [r for r in assets if r['id'] not in matched_ids]
        
        # Phase 2: Fuzzy Matching for remainders
        fuzzy_results = []
        if unmatched_assets:
            fuzzy_results = await self._run_fuzzy_matching(unmatched_assets, group)
            
        return exact_results + fuzzy_results

    async def _run_exact_matching(
        self, 
        assets: List[asyncpg.Record], 
        group: str
    ) -> List[MatchResult]:
        """Phase 1: Fast exact alias/symbol matching via JOIN."""
        # We unnest the asset IDs and normalized symbols to join in SQL
        asset_ids = [r['id'] for r in assets]
        norm_roots = [r['sym_norm_root'] for r in assets]
        
        query = """
            WITH input AS (
                SELECT unnest($1::int[]) as id, unnest($2::text[]) as sym_norm_root
            )
            SELECT
                i.id as asset_id,
                im.isin,
                im.symbol as identity_symbol,
                im.name as identity_name,
                100.0 as confidence,
                'exact_alias' as match_type
            FROM input i
            JOIN identity_manifest im ON (
                im.asset_class_group = $3 AND
                string_to_array(im.symbol, ';') && ARRAY[i.sym_norm_root]
            )
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, asset_ids, norm_roots, group)
            
        return [MatchResult(**dict(r)) for r in rows]

    async def _run_fuzzy_matching(
        self, 
        assets: List[asyncpg.Record], 
        group: str
    ) -> List[MatchResult]:
        """Phase 2: Targeted fuzzy matching with weighted scoring."""
        asset_ids = [r['id'] for r in assets]
        sym_roots = [r['sym_norm_root'] for r in assets]
        names = [r['name'] or '' for r in assets]
        exchanges = [r['exchange'] or '' for r in assets]
        
        query = """
            WITH asset_input AS (
                SELECT
                    unnest($1::int[]) as id,
                    unnest($2::text[]) as sym_root,
                    unnest($3::text[]) as name,
                    unnest($4::text[]) as exchange
            ),
            matches AS (
                SELECT
                    ai.id as asset_id,
                    im.isin,
                    im.symbol as identity_symbol,
                    im.name as identity_name,

                    -- Weighted scoring logic
                    (
                        CASE
                            WHEN ai.sym_root = ANY(string_to_array(im.symbol, ';')) THEN 95.0
                            WHEN similarity(ai.sym_root, im.symbol) > 0.8 THEN 80.0
                            WHEN similarity(ai.sym_root, im.symbol) > 0.6 THEN 60.0
                            ELSE similarity(ai.sym_root, im.symbol) * $5
                        END +
                        CASE WHEN ai.exchange = im.exchange THEN $6 ELSE 0.0 END +
                        COALESCE(similarity(ai.name, im.name), 0) * $7
                    ) as confidence,

                    CASE
                        WHEN ai.sym_root = ANY(string_to_array(im.symbol, ';')) THEN 'exact_alias'
                        ELSE 'fuzzy_symbol'
                    END as match_type,

                    ROW_NUMBER() OVER (
                        PARTITION BY ai.id
                        ORDER BY (
                            CASE
                                WHEN ai.sym_root = ANY(string_to_array(im.symbol, ';')) THEN 95.0
                                WHEN similarity(ai.sym_root, im.symbol) > 0.8 THEN 80.0
                                WHEN similarity(ai.sym_root, im.symbol) > 0.6 THEN 60.0
                                ELSE similarity(ai.sym_root, im.symbol) * $5
                            END +
                            CASE WHEN ai.exchange = im.exchange THEN $6 ELSE 0.0 END +
                            COALESCE(similarity(ai.name, im.name), 0) * $7
                        ) DESC
                    ) as rn

                FROM asset_input ai
                CROSS JOIN identity_manifest im
                WHERE im.asset_class_group = $8
                  AND (
                    ai.sym_root = ANY(string_to_array(im.symbol, ';')) OR
                    similarity(ai.sym_root, im.symbol) > $9
                  )
            )
            SELECT
                asset_id,
                isin,
                identity_symbol,
                identity_name,
                confidence,
                match_type
            FROM matches
            WHERE rn = 1  -- Only take top match per asset
              AND confidence >= $10  -- Apply auto-trade threshold
            ORDER BY confidence DESC
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                query, 
                asset_ids, 
                sym_roots, 
                names, 
                exchanges,
                SYM_BOOST, 
                EXCHANGE_BOOST, 
                NAME_BOOST, 
                group, 
                FUZZY_THRESHOLD,
                AUTO_THRESHOLD
            )
            
        return [MatchResult(**dict(r)) for r in rows]


