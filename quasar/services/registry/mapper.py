"""Automated mapping utility for creating cross-provider asset mappings using primary_id relationships."""

import logging
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import asyncpg

from quasar.lib.common.database_handler import DatabaseHandler

logger = logging.getLogger(__name__)


@dataclass
class MappingCandidate:
    """Represents a potential mapping to be created."""
    class_name: str
    class_type: str
    class_symbol: str
    common_symbol: str
    primary_id: str
    asset_class_group: str
    reasoning: str  # Why this mapping was chosen


@dataclass
class CryptoProviderSelection:
    """Tracks crypto provider selection logic."""
    provider_name: str
    preferred_quote_currency: Optional[str]
    available_assets: List[Dict]
    selected_asset: Optional[Dict]
    skipped_assets: List[Dict]
    reasoning: str


@dataclass
class PrimaryIdGroup:
    """Assets grouped by primary_id with mapping analysis."""
    primary_id: str
    asset_class_group: str
    assets: List[Dict]
    existing_mappings: List[Dict]
    determined_common_symbol: Optional[str]
    mapping_candidates: List[MappingCandidate]
    crypto_selections: List[CryptoProviderSelection]
    conflicts: List[str]




class AutomatedMapper(DatabaseHandler):
    """Utility for creating automated cross-provider asset mappings using primary_id relationships."""

    name = "AutomatedMapper"

    def __init__(self, dsn: Optional[str] = None, pool: Optional[asyncpg.Pool] = None):
        super().__init__(dsn=dsn, pool=pool)

    async def generate_mapping_candidates_for_provider(
        self,
        provider_name: str,
        provider_type: str
    ) -> List[MappingCandidate]:
        """
        Generate automated mapping candidates for all assets belonging to a specific provider.

        This method:
        1. Finds all assets for the provider that have primary_ids
        2. Groups them by primary_id with assets from other providers
        3. Determines appropriate common_symbols
        4. Applies crypto preferences where applicable
        5. Returns mapping candidates for unmapped assets

        Args:
            provider_name: Name of the provider (e.g., 'EODHD', 'BINANCE')
            provider_type: Type of provider ('provider' or 'broker')

        Returns:
            List of MappingCandidate objects for assets that should be mapped
        """
        start_time = time.time()

        try:
            # Get all assets with primary_ids that include this provider
            assets = await self._query_assets_for_provider_mapping(provider_name, provider_type)

            if not assets:
                logger.info(f"No assets found for provider {provider_name} ({provider_type})")
                return []

            # Group assets by primary_id
            primary_id_groups = self._group_assets_by_primary_id(assets)

            # Load existing mappings for fast lookup (both asset-specific and cross-provider)
            asset_lookup, primary_id_lookup = await self._load_all_existing_mappings(assets)

            # Process each group to generate candidates
            all_candidates = []

            for group in primary_id_groups:
                try:
                    # Check existing mappings
                    self._check_existing_mappings_fast(group, asset_lookup, primary_id_lookup)

                    # Determine common symbol
                    self._determine_common_symbol(group)

                    # Apply crypto preferences if needed
                    if group.asset_class_group == 'crypto':
                        await self._apply_crypto_preferences_for_provider(group, provider_name, provider_type)

                    # Generate mapping candidates
                    self._generate_mapping_candidates(group)

                    # Collect candidates for this group
                    all_candidates.extend(group.mapping_candidates)

                except Exception as e:
                    error_msg = f"Error processing group {group.primary_id}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    # For now, we'll skip problematic groups rather than fail entirely

            execution_time = time.time() - start_time

            logger.info(
                f"AutomatedMapper: Generated {len(all_candidates)} mapping candidates "
                f"for provider {provider_name} ({provider_type}) in {execution_time:.2f}s"
            )

            return all_candidates

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Critical error in automated mapping: {str(e)}"
            logger.error(error_msg, exc_info=True)

            return []

    async def _query_assets_for_provider_mapping(self, provider_name: str, provider_type: str) -> List[Dict]:
        """Query all assets with primary_ids that include the specified provider."""
        query = """
            SELECT
                class_name, class_type, symbol, primary_id, asset_class_group,
                base_currency, quote_currency, sym_norm_root
            FROM assets
            WHERE primary_id IS NOT NULL
              AND (class_name = $1 AND class_type = $2)
            ORDER BY primary_id, class_name, class_type
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, provider_name, provider_type)

        return [dict(row) for row in rows]

    async def _load_all_existing_mappings(self, all_assets: List[Dict]) -> Tuple[Dict[Tuple[str, str, str], str], Dict[str, str]]:
        """Load all existing mappings and create both asset-specific and primary_id lookups."""
        if not all_assets:
            return {}, {}

        # Build set of primary_ids we're interested in for cross-provider lookup
        relevant_primary_ids = {a['primary_id'] for a in all_assets if a.get('primary_id')}

        if not relevant_primary_ids:
            return {}, {}

        # Query ALL existing mappings with primary_id via JOIN
        query = """
            SELECT am.class_name, am.class_type, am.class_symbol, am.common_symbol, a.primary_id
            FROM asset_mapping am
            JOIN assets a ON am.class_name = a.class_name
                          AND am.class_type = a.class_type
                          AND am.class_symbol = a.symbol
            WHERE a.primary_id = ANY($1)
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, list(relevant_primary_ids))

        # Create both lookup dictionaries
        asset_lookup = {}
        primary_id_lookup = {}

        for row in rows:
            # Asset-specific lookup: (class_name, class_type, class_symbol) -> common_symbol
            asset_key = (row['class_name'], row['class_type'], row['class_symbol'])
            asset_lookup[asset_key] = row['common_symbol']

            # Primary ID lookup: primary_id -> common_symbol (take first one, they should be consistent)
            primary_id = row['primary_id']
            if primary_id not in primary_id_lookup:
                primary_id_lookup[primary_id] = row['common_symbol']

        return asset_lookup, primary_id_lookup

    def _group_assets_by_primary_id(self, assets: List[Dict]) -> List[PrimaryIdGroup]:
        """Group assets by primary_id and asset_class_group."""
        groups_by_key = {}

        for asset in assets:
            key = (asset['primary_id'], asset['asset_class_group'])
            if key not in groups_by_key:
                groups_by_key[key] = PrimaryIdGroup(
                    primary_id=asset['primary_id'],
                    asset_class_group=asset['asset_class_group'],
                    assets=[],
                    existing_mappings=[],
                    determined_common_symbol=None,
                    mapping_candidates=[],
                    crypto_selections=[],
                    conflicts=[]
                )
            groups_by_key[key].assets.append(asset)

        return list(groups_by_key.values())

    def _check_existing_mappings_fast(self, group: PrimaryIdGroup,
                                    asset_lookup: Dict[Tuple[str, str, str], str],
                                    primary_id_lookup: Dict[str, str]):
        """Check existing mappings using pre-loaded lookup dicts."""
        common_symbols = set()

        # Check mappings for specific assets in this group
        for asset in group.assets:
            key = (asset['class_name'], asset['class_type'], asset['symbol'])
            if key in asset_lookup:
                common_symbols.add(asset_lookup[key])

        # Check cross-provider mappings for this primary_id
        if group.primary_id in primary_id_lookup:
            common_symbols.add(primary_id_lookup[group.primary_id])

        # Convert back to the expected format
        group.existing_mappings = [{'common_symbol': cs} for cs in common_symbols]

        # Check for conflicts (multiple different common_symbols - shouldn't happen)
        if len(common_symbols) > 1:
            group.conflicts.append(
                f"Multiple existing common_symbols found: {', '.join(common_symbols)}"
            )

    def _determine_common_symbol(self, group: PrimaryIdGroup):
        """Determine the common_symbol for this group."""
        if group.existing_mappings:
            # Use existing common_symbol
            group.determined_common_symbol = group.existing_mappings[0]['common_symbol']
        else:
            # Generate new common_symbol from sym_norm_root
            assets_with_norm_root = [a for a in group.assets if a.get('sym_norm_root')]
            if assets_with_norm_root:
                # Sort by length then alphabetically
                best_asset = min(assets_with_norm_root,
                               key=lambda a: (len(a['sym_norm_root'] or ''), a['sym_norm_root'] or ''))
                group.determined_common_symbol = (best_asset['sym_norm_root'] or '').upper()
            else:
                # Fallback to first symbol
                group.determined_common_symbol = group.assets[0]['symbol'].upper()

    async def _apply_crypto_preferences_for_provider(self, group: PrimaryIdGroup, provider_name: str, provider_type: str):
        """Apply crypto preferences for a specific provider in this group."""
        # Get provider's preferred quote currency
        preferred_quote = await self._get_provider_crypto_preference(provider_name, provider_type)

        # Get all assets for this provider in the group
        provider_assets = [a for a in group.assets
                         if a['class_name'] == provider_name and a['class_type'] == provider_type]

        # Apply selection logic
        selected_asset, skipped_assets, reasoning = self._select_crypto_asset_for_provider(
            provider_assets, preferred_quote
        )

        # Track the selection
        group.crypto_selections.append(CryptoProviderSelection(
            provider_name=provider_name,
            preferred_quote_currency=preferred_quote,
            available_assets=provider_assets,
            selected_asset=selected_asset,
            skipped_assets=skipped_assets,
            reasoning=reasoning
        ))

    async def _get_provider_crypto_preference(self, class_name: str, class_type: str) -> Optional[str]:
        """Get crypto quote currency preference for a provider."""
        query = """
            SELECT preferences->'crypto'->>'preferred_quote_currency' as preferred_quote
            FROM code_registry
            WHERE class_name = $1 AND class_type = $2
        """

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, class_name, class_type)

        return row['preferred_quote'] if row and row['preferred_quote'] else None

    def _select_crypto_asset_for_provider(self, provider_assets: List[Dict], preferred_quote: Optional[str]) -> Tuple[Optional[Dict], List[Dict], str]:
        """Select which crypto asset to map for a provider based on preferences."""

        # Count unique quote currencies
        unique_quotes = set(a.get('quote_currency') for a in provider_assets if a.get('quote_currency'))

        # If only one quote currency, map it (ignore preferences)
        if len(unique_quotes) == 1:
            selected = provider_assets[0]  # Just pick first (they're equivalent)
            return selected, [], f"Single quote currency available: {selected.get('quote_currency')}"

        # Multiple quote currencies - apply preference hierarchy
        candidates = []

        # Level 1: Exact preferred match
        if preferred_quote:
            candidates = [a for a in provider_assets if a.get('quote_currency') == preferred_quote]
            if candidates:
                selected = min(candidates, key=lambda a: a['symbol'])
                skipped = [a for a in provider_assets if a != selected]
                return selected, skipped, f"Selected preferred quote: {preferred_quote}"

        # Level 2: USD fallback (any asset containing "USD")
        candidates = [a for a in provider_assets if 'USD' in str(a.get('quote_currency', ''))]
        if candidates:
            selected = min(candidates, key=lambda a: a['symbol'])
            skipped = [a for a in provider_assets if a != selected]
            return selected, skipped, f"Selected USD fallback: {selected.get('quote_currency')}"

        # Level 3: No suitable match found - skip entirely
        return None, provider_assets, "No suitable USD quote currency available"

    def _generate_mapping_candidates(self, group: PrimaryIdGroup):
        """Generate mapping candidates for this group."""
        if not group.determined_common_symbol:
            group.conflicts.append("No common_symbol determined")
            return

        if group.asset_class_group == 'crypto':
            # For crypto, only map selected assets
            for selection in group.crypto_selections:
                if selection.selected_asset:
                    group.mapping_candidates.append(MappingCandidate(
                        class_name=selection.selected_asset['class_name'],
                        class_type=selection.selected_asset['class_type'],
                        class_symbol=selection.selected_asset['symbol'],
                        common_symbol=group.determined_common_symbol,
                        primary_id=group.primary_id,
                        asset_class_group=group.asset_class_group,
                        reasoning=selection.reasoning
                    ))
        else:
            # For securities, map all assets
            for asset in group.assets:
                group.mapping_candidates.append(MappingCandidate(
                    class_name=asset['class_name'],
                    class_type=asset['class_type'],
                    class_symbol=asset['symbol'],
                    common_symbol=group.determined_common_symbol,
                    primary_id=group.primary_id,
                    asset_class_group=group.asset_class_group,
                    reasoning="Securities group - all assets mapped" if not group.existing_mappings
                               else "Reusing existing common_symbol"
                ))
