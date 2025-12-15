-- ASSET TABLE
-- Stores all known assets
CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    class_name TEXT NOT NULL,
    class_type TEXT NOT NULL,
    external_id TEXT,
    isin TEXT,
    symbol TEXT NOT NULL,
    name TEXT,
    exchange TEXT,
    asset_class TEXT,
    base_currency TEXT,
    quote_currency TEXT,
    country TEXT,

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
        REFERENCES asset_class (code)
);