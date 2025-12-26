-- IDENTITY MANIFEST TABLE
-- Stores canonical identity mappings for asset identification during symbol discovery
CREATE TABLE IF NOT EXISTS identity_manifest (
    id SERIAL PRIMARY KEY,
    canonical_id TEXT NOT NULL, -- ISIN (e.g., "US92202V5425", "XT4V541JG149")
    symbol TEXT NOT NULL, -- Ticker symbol (e.g., "AAPL", "BTC")
    name TEXT NOT NULL, -- Full name
    exchange TEXT, -- MIC code or null
    asset_class_group TEXT NOT NULL, -- 'securities' or 'crypto'
    source TEXT NOT NULL DEFAULT 'bundled', -- 'bundled', 'api_upload', 'github_sync'
    metadata JSONB DEFAULT '{}', -- Future extensibility
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure canonical_id uniqueness within each asset class group
    UNIQUE (canonical_id, asset_class_group)
);

-- Indexes for fast lookups during symbol discovery
CREATE INDEX IF NOT EXISTS idx_identity_manifest_symbol
ON identity_manifest (symbol);

CREATE INDEX IF NOT EXISTS idx_identity_manifest_canonical_id
ON identity_manifest (canonical_id);

CREATE INDEX IF NOT EXISTS idx_identity_manifest_asset_class
ON identity_manifest (asset_class_group);

-- Composite index for common query pattern (asset_class + symbol)
CREATE INDEX IF NOT EXISTS idx_identity_manifest_class_symbol
ON identity_manifest (asset_class_group, symbol);

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
