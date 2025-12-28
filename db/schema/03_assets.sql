-- ASSET TABLE
-- Stores all known assets
CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    class_name TEXT NOT NULL,
    class_type TEXT NOT NULL,
    external_id TEXT,
    isin TEXT,
    isin_source TEXT, -- 'provider', 'matcher', 'manual' - tracks ISIN origin
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

    CONSTRAINT chk_isin_source
        CHECK (isin_source IN ('provider', 'matcher', 'manual') OR isin_source IS NULL)
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_assets_identity_group ON assets (asset_class_group) WHERE asset_class_group IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_assets_unidentified ON assets (isin) WHERE isin IS NULL;
CREATE INDEX IF NOT EXISTS idx_assets_isin ON assets (isin) WHERE isin IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_assets_matcher_symbol ON assets (matcher_symbol);
CREATE INDEX IF NOT EXISTS idx_assets_isin_source ON assets (isin_source) WHERE isin_source IS NOT NULL;
