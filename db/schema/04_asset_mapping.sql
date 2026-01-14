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
);

-- COMMON SYMBOLS TABLE
-- Reference-counted table of valid common symbols, auto-maintained via triggers on asset_mapping.
-- This table serves as the source of truth for what common symbols exist in the system.
-- UserIndex memberships FK to this table to ensure referential integrity.
CREATE TABLE IF NOT EXISTS common_symbols (
    symbol TEXT PRIMARY KEY,
    ref_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_ref_count_non_negative CHECK (ref_count >= 0)
);

-- Index for potential future queries
CREATE INDEX IF NOT EXISTS idx_common_symbols_created
    ON common_symbols (created_at);

-- TRIGGER FUNCTIONS FOR MAINTAINING common_symbols

-- BEFORE INSERT: Ensure common_symbol exists in common_symbols (for FK check)
CREATE OR REPLACE FUNCTION before_asset_mapping_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO common_symbols (symbol, ref_count)
    VALUES (NEW.common_symbol, 0)
    ON CONFLICT (symbol) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- AFTER INSERT: Increment ref_count
CREATE OR REPLACE FUNCTION after_asset_mapping_insert()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE common_symbols
    SET ref_count = ref_count + 1
    WHERE symbol = NEW.common_symbol;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- AFTER DELETE: Decrement ref_count, delete if zero
CREATE OR REPLACE FUNCTION after_asset_mapping_delete()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE common_symbols
    SET ref_count = ref_count - 1
    WHERE symbol = OLD.common_symbol;

    DELETE FROM common_symbols
    WHERE symbol = OLD.common_symbol AND ref_count = 0;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- BEFORE UPDATE: Ensure new common_symbol exists (for FK check)
CREATE OR REPLACE FUNCTION before_asset_mapping_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.common_symbol IS DISTINCT FROM NEW.common_symbol THEN
        INSERT INTO common_symbols (symbol, ref_count)
        VALUES (NEW.common_symbol, 0)
        ON CONFLICT (symbol) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- AFTER UPDATE: Adjust ref_counts for old and new symbols
CREATE OR REPLACE FUNCTION after_asset_mapping_update()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.common_symbol IS DISTINCT FROM NEW.common_symbol THEN
        -- Increment new symbol
        UPDATE common_symbols
        SET ref_count = ref_count + 1
        WHERE symbol = NEW.common_symbol;

        -- Decrement old symbol
        UPDATE common_symbols
        SET ref_count = ref_count - 1
        WHERE symbol = OLD.common_symbol;

        -- Delete old symbol if no longer referenced
        DELETE FROM common_symbols
        WHERE symbol = OLD.common_symbol AND ref_count = 0;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGERS ON asset_mapping

-- Drop existing triggers if they exist (for idempotency)
DROP TRIGGER IF EXISTS trg_before_asset_mapping_insert ON asset_mapping;
DROP TRIGGER IF EXISTS trg_after_asset_mapping_insert ON asset_mapping;
DROP TRIGGER IF EXISTS trg_after_asset_mapping_delete ON asset_mapping;
DROP TRIGGER IF EXISTS trg_before_asset_mapping_update ON asset_mapping;
DROP TRIGGER IF EXISTS trg_after_asset_mapping_update ON asset_mapping;

-- Create triggers
CREATE TRIGGER trg_before_asset_mapping_insert
    BEFORE INSERT ON asset_mapping
    FOR EACH ROW
    EXECUTE FUNCTION before_asset_mapping_insert();

CREATE TRIGGER trg_after_asset_mapping_insert
    AFTER INSERT ON asset_mapping
    FOR EACH ROW
    EXECUTE FUNCTION after_asset_mapping_insert();

CREATE TRIGGER trg_after_asset_mapping_delete
    AFTER DELETE ON asset_mapping
    FOR EACH ROW
    EXECUTE FUNCTION after_asset_mapping_delete();

CREATE TRIGGER trg_before_asset_mapping_update
    BEFORE UPDATE ON asset_mapping
    FOR EACH ROW
    EXECUTE FUNCTION before_asset_mapping_update();

CREATE TRIGGER trg_after_asset_mapping_update
    AFTER UPDATE ON asset_mapping
    FOR EACH ROW
    EXECUTE FUNCTION after_asset_mapping_update();

-- FK to common_symbols (enables rename cascading)
-- Note: BEFORE INSERT trigger ensures symbol exists before this check
ALTER TABLE asset_mapping
    DROP CONSTRAINT IF EXISTS fk_asset_mapping_common_symbol;
ALTER TABLE asset_mapping
    ADD CONSTRAINT fk_asset_mapping_common_symbol
    FOREIGN KEY (common_symbol)
    REFERENCES common_symbols(symbol)
    ON UPDATE CASCADE
    ON DELETE RESTRICT