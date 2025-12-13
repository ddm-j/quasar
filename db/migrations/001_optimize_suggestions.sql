-- Migration: Optimize Asset Mapping Suggestions
-- Run this on existing databases to add generated columns and new indexes.
-- For fresh deployments, these changes are already in 03_assets.sql and 08_mapping_suggestions_support.sql.
--
-- Usage:
--   docker exec -i quasardb psql -U postgres -d postgres < db/migrations/001_optimize_suggestions.sql
--
-- Or connect manually:
--   docker exec -it quasardb psql -U postgres -d postgres
--   \i /path/to/001_optimize_suggestions.sql

BEGIN;

-- Step 1: Add generated columns to existing assets table
-- These columns are auto-computed from the symbol column on INSERT/UPDATE.
ALTER TABLE assets 
ADD COLUMN IF NOT EXISTS sym_norm_full TEXT GENERATED ALWAYS AS (
    regexp_replace(lower(symbol), '[^a-z0-9]', '', 'g')
) STORED;

ALTER TABLE assets 
ADD COLUMN IF NOT EXISTS sym_norm_root TEXT GENERATED ALWAYS AS (
    regexp_replace(lower(split_part(symbol, '.', 1)), '[^a-z0-9]', '', 'g')
) STORED;

-- Step 2: Drop old expression index (from original 08_mapping_suggestions_support.sql)
DROP INDEX IF EXISTS idx_assets_sym_norm;

-- Step 3: Create new indexes for optimized queries

-- Enable trigram extension if not already enabled
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Indexes on generated columns
CREATE INDEX IF NOT EXISTS idx_assets_sym_norm_full 
ON assets (sym_norm_full);

CREATE INDEX IF NOT EXISTS idx_assets_sym_norm_root 
ON assets (sym_norm_root);

-- Partial indexes for nullable join columns
CREATE INDEX IF NOT EXISTS idx_assets_isin 
ON assets (isin) WHERE isin IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_assets_external_id 
ON assets (external_id) WHERE external_id IS NOT NULL;

-- Trigram index on asset names to speed similarity/ILIKE searches
CREATE INDEX IF NOT EXISTS idx_assets_name_trgm
ON assets USING gin (name gin_trgm_ops);

-- Helper index for asset_class scoping
CREATE INDEX IF NOT EXISTS idx_assets_asset_class
ON assets (asset_class);

-- Composite index for unmapped filter (used in NOT EXISTS subquery)
CREATE INDEX IF NOT EXISTS idx_asset_mapping_lookup 
ON asset_mapping (class_name, class_type, class_symbol);

-- Composite index for class-based filtering and keyset pagination
CREATE INDEX IF NOT EXISTS idx_assets_class_symbol 
ON assets (class_name, class_type, symbol);

COMMIT;

-- Verify the migration
SELECT 
    column_name, 
    data_type, 
    generation_expression
FROM information_schema.columns 
WHERE table_name = 'assets' 
  AND column_name IN ('sym_norm_full', 'sym_norm_root');
