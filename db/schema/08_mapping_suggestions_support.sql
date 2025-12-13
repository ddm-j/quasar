-- Support objects for asset-mapping suggestions
-- Idempotent: safe to run multiple times

-- Enable trigram extension for fuzzy name matching (if permitted)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Indexes on generated columns (sym_norm_full and sym_norm_root are in 03_assets.sql)
CREATE INDEX IF NOT EXISTS idx_assets_sym_norm_full 
ON assets (sym_norm_full);

CREATE INDEX IF NOT EXISTS idx_assets_sym_norm_root 
ON assets (sym_norm_root);

-- Partial indexes for nullable join columns (faster lookups on non-null values)
CREATE INDEX IF NOT EXISTS idx_assets_isin 
ON assets (isin) WHERE isin IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_assets_external_id 
ON assets (external_id) WHERE external_id IS NOT NULL;

-- Trigram index on asset names to speed similarity/ILIKE searches
CREATE INDEX IF NOT EXISTS idx_assets_name_trgm
ON assets USING gin (name gin_trgm_ops);

-- Helper index for asset_class scoping in suggestions
CREATE INDEX IF NOT EXISTS idx_assets_asset_class
ON assets (asset_class);

-- Composite index for unmapped filter (used in NOT EXISTS subquery)
CREATE INDEX IF NOT EXISTS idx_asset_mapping_lookup 
ON asset_mapping (class_name, class_type, class_symbol);

-- Composite index for class-based filtering and keyset pagination
CREATE INDEX IF NOT EXISTS idx_assets_class_symbol 
ON assets (class_name, class_type, symbol);
