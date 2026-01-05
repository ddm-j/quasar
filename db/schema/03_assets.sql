-- ASSET TABLE
-- Stores all known assets
CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    class_name TEXT NOT NULL,
    class_type TEXT NOT NULL,
    external_id TEXT,
    primary_id TEXT,
    primary_id_source TEXT, -- 'provider', 'matcher', 'manual' - tracks primary_id origin
    symbol TEXT NOT NULL,
    matcher_symbol TEXT NOT NULL,
    name TEXT,
    exchange TEXT,
    asset_class TEXT,
    base_currency TEXT,
    quote_currency TEXT,
    country TEXT,

    -- Identity Matching Tracking
    identity_conf DOUBLE PRECISION,
    identity_match_type TEXT, -- 'exact_alias', 'fuzzy_symbol', etc.
    identity_updated_at TIMESTAMP,

    -- Generated Grouping for Matcher
    asset_class_group TEXT GENERATED ALWAYS AS (
        CASE 
            WHEN asset_class = 'crypto' THEN 'crypto'
            WHEN asset_class IN ('equity', 'fund', 'etf', 'bond', 'preferred', 'warrant', 'adr', 'mutual_fund', 'index') THEN 'securities'
            ELSE NULL
        END
    ) STORED,

    -- Normalized symbol columns for efficient matching (computed on INSERT/UPDATE)
    sym_norm_full TEXT GENERATED ALWAYS AS (
        regexp_replace(lower(symbol), '[^a-z0-9]', '', 'g')
    ) STORED,
    sym_norm_root TEXT GENERATED ALWAYS AS (
        regexp_replace(lower(split_part(symbol, '.', 1)), '[^a-z0-9]', '', 'g')
    ) STORED,

    UNIQUE (class_name, class_type, symbol),

    CONSTRAINT fk_assets_to_code_registry
        FOREIGN KEY (class_name, class_type)
        REFERENCES code_registry (class_name, class_type)
        ON DELETE CASCADE,

    CONSTRAINT fk_assets_asset_class
        FOREIGN KEY (asset_class)
        REFERENCES asset_class (code),

    CONSTRAINT chk_primary_id_source
        CHECK (primary_id_source IN ('provider', 'matcher', 'manual') OR primary_id_source IS NULL)
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_assets_identity_group ON assets (asset_class_group) WHERE asset_class_group IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_assets_unidentified ON assets (primary_id) WHERE primary_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_assets_primary_id ON assets (primary_id) WHERE primary_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_assets_matcher_symbol ON assets (matcher_symbol);
CREATE INDEX IF NOT EXISTS idx_assets_primary_id_source ON assets (primary_id_source) WHERE primary_id_source IS NOT NULL;

-- Indexes for identity column filtering and sorting
CREATE INDEX IF NOT EXISTS idx_assets_identity_match_type
ON assets (identity_match_type) WHERE identity_match_type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_assets_identity_conf
ON assets (identity_conf) WHERE identity_conf IS NOT NULL;

-- Unique constraint: Only one asset per provider can have a given primary_id for securities
-- This prevents duplicate identity assignments after deduplication
CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_unique_securities_primary_id 
ON assets (class_name, class_type, primary_id) 
WHERE asset_class_group = 'securities' AND primary_id IS NOT NULL;
