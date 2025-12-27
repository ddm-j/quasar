-- Migration: Add Identity Manifest Fuzzy Matching Indexes
-- Adds indexes required for efficient fuzzy matching and alias lookups
-- in the identity_manifest table for symbol discovery.
--
-- Usage:
--   docker exec -i quasardb psql -U postgres -d postgres < db/migrations/005_add_identity_matching_indexes.sql
--
-- Or connect manually:
--   docker exec -it quasardb psql -U postgres -d postgres
--   \i /path/to/005_add_identity_matching_indexes.sql

BEGIN;

-- Step 1: Enable trigram extension for fuzzy text matching
-- Required for similarity() and trigram-based fuzzy matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Step 2: Add trigram indexes for fuzzy symbol matching
-- Enables fast similarity searches on symbol field
CREATE INDEX IF NOT EXISTS idx_identity_manifest_symbol_trgm
ON identity_manifest USING gin (symbol gin_trgm_ops);

-- Step 3: Add trigram indexes for fuzzy name matching
-- Enables fast similarity searches on name field (especially for crypto)
CREATE INDEX IF NOT EXISTS idx_identity_manifest_name_trgm
ON identity_manifest USING gin (name gin_trgm_ops);

-- Step 4: Add array index for alias matching
-- Handles semicolon-separated aliases in symbol field (e.g., "XBT;BTC")
-- Uses GIN index on array operations for fast containment queries
CREATE INDEX IF NOT EXISTS idx_identity_manifest_symbol_aliases
ON identity_manifest USING gin (string_to_array(symbol, ';'));

-- Step 5: Add composite index for asset class + symbol queries
-- Optimizes the common query pattern of filtering by asset_class_group then symbol
CREATE INDEX IF NOT EXISTS idx_identity_manifest_asset_class_symbol
ON identity_manifest (asset_class_group, symbol);

-- Step 6: Add index for exchange-based filtering (securities)
-- Optimizes exchange matching for securities identification
CREATE INDEX IF NOT EXISTS idx_identity_manifest_exchange
ON identity_manifest (exchange) WHERE exchange IS NOT NULL;

COMMIT;

-- Verify the migration
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'identity_manifest'
  AND indexname LIKE 'idx_identity_manifest%'
ORDER BY indexname;
