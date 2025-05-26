-- ASSET MAPPING TABLE
-- This table serves as the central dictionary for translating provider-specific symbols
-- to internal common symbols.
CREATE TABLE IF NOT EXISTS asset_mapping (
    common_symbol TEXT NOT NULL,    
    class_name TEXT NOT NULL,    
    class_type TEXT NOT NULL,
    class_symbol TEXT NOT NULL,  
    is_active BOOLEAN DEFAULT TRUE, 
    PRIMARY KEY (class_name, class_type, class_symbol),

    CONSTRAINT uq_common_per_class UNIQUE (common_symbol, class_name, class_type),

    CONSTRAINT fk_asset_mapping_class_name
        FOREIGN KEY (class_name, class_type)
        REFERENCES code_registry (class_name, class_type)
        ON DELETE CASCADE,

    CONSTRAINT fk_asset_mapping_to_assets
        FOREIGN KEY (class_name, class_type, class_symbol)
        REFERENCES assets (class_name, class_type, symbol)
        ON DELETE CASCADE
);

-- Index on common_symbol for quick lookups
CREATE INDEX IF NOT EXISTS idx_asset_mapping_common_provider
ON asset_mapping (common_symbol, class_symbol);

-- Index for mappings by provider
CREATE INDEX IF NOT EXISTS idx_asset_mapping_class_name on asset_mapping (
    class_name,
    class_type
)