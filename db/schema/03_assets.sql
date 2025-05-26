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

    UNIQUE (class_name, class_type, symbol),

    CONSTRAINT fk_assets_to_code_registry
        FOREIGN KEY (class_name, class_type)
        REFERENCES code_registry (class_name, class_type)
        ON DELETE CASCADE
);