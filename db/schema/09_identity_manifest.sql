-- IDENTITY MANIFEST TABLE
-- Stores canonical identity mappings for asset identification during symbol discovery
CREATE TABLE IF NOT EXISTS identity_manifest (
    id SERIAL PRIMARY KEY,
    isin TEXT NOT NULL, -- Renamed from canonical_id (e.g., "US92202V5425", "XT4V541JG149")
    symbol TEXT NOT NULL, -- Ticker symbol (e.g., "AAPL", "BTC")
    name TEXT NOT NULL, -- Full name
    exchange TEXT, -- MIC code or null
    asset_class_group TEXT NOT NULL, -- 'securities' or 'crypto'
    source TEXT NOT NULL DEFAULT 'bundled', -- 'bundled', 'api_upload', 'github_sync'
    metadata JSONB DEFAULT '{}', -- Future extensibility
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure isin uniqueness within each asset class group
    UNIQUE (isin, asset_class_group)
);

-- Indexes for fast lookups during symbol discovery
CREATE INDEX IF NOT EXISTS idx_identity_manifest_symbol
ON identity_manifest (symbol);

CREATE INDEX IF NOT EXISTS idx_identity_manifest_isin
ON identity_manifest (isin);

CREATE INDEX IF NOT EXISTS idx_identity_manifest_asset_class
ON identity_manifest (asset_class_group);

-- Composite index for common query pattern (asset_class + symbol)
CREATE INDEX IF NOT EXISTS idx_identity_manifest_class_symbol
ON identity_manifest (asset_class_group, symbol);

-- Trigram and Array Indexes for Fuzzy Matching
-- Step 1: Enable trigram extension for fuzzy text matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Step 2: Add trigram indexes for fuzzy symbol matching
CREATE INDEX IF NOT EXISTS idx_identity_manifest_symbol_trgm
ON identity_manifest USING gin (symbol gin_trgm_ops);

-- Step 3: Add trigram indexes for fuzzy name matching
CREATE INDEX IF NOT EXISTS idx_identity_manifest_name_trgm
ON identity_manifest USING gin (name gin_trgm_ops);

-- Step 4: Add array index for alias matching (handles semicolon-separated aliases)
CREATE INDEX IF NOT EXISTS idx_identity_manifest_symbol_aliases
ON identity_manifest USING gin (string_to_array(symbol, ';'));

-- Step 5: Add index for exchange-based filtering (securities)
CREATE INDEX IF NOT EXISTS idx_identity_manifest_exchange
ON identity_manifest (exchange) WHERE exchange IS NOT NULL;

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_identity_manifest_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update updated_at
CREATE TRIGGER trigger_update_identity_manifest_updated_at
    BEFORE UPDATE ON identity_manifest
    FOR EACH ROW
    EXECUTE FUNCTION update_identity_manifest_updated_at();
