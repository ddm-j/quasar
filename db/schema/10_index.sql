-- INDEX MEMBERSHIPS TABLE
-- Stores temporal membership records for index constituents (SCD Type 2)
-- Supports both API indices (referencing assets) and user indices (using common_symbol)
CREATE TABLE IF NOT EXISTS index_memberships (
    id SERIAL PRIMARY KEY,

    -- Reference to the index definition in code_registry
    index_class_name TEXT NOT NULL,
    index_class_type TEXT NOT NULL DEFAULT 'provider',

    -- For API indices: reference to specific asset in assets table
    asset_class_name TEXT,
    asset_class_type TEXT,
    asset_symbol TEXT,

    -- For user indices: reference by common_symbol
    common_symbol TEXT,

    -- Weight for weighted indices (NULL = equal weight)
    weight DOUBLE PRECISION,

    -- SCD Type 2 temporal columns
    valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMPTZ,

    -- Audit columns
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Source of the membership data
    source TEXT NOT NULL DEFAULT 'manual',

    -- Optional metadata
    metadata JSONB DEFAULT '{}',

    CONSTRAINT fk_membership_to_index
        FOREIGN KEY (index_class_name, index_class_type)
        REFERENCES code_registry (class_name, class_type)
        ON DELETE CASCADE,

    CONSTRAINT fk_membership_to_asset
        FOREIGN KEY (asset_class_name, asset_class_type, asset_symbol)
        REFERENCES assets (class_name, class_type, symbol)
        ON DELETE CASCADE,

    CONSTRAINT chk_weight_positive CHECK (weight IS NULL OR weight > 0),
    CONSTRAINT chk_valid_range CHECK (valid_to IS NULL OR valid_to > valid_from),
    CONSTRAINT chk_membership_source CHECK (source IN ('api', 'manual', 'rebalance')),

    -- XOR: exactly one reference pattern (asset columns OR common_symbol)
    CONSTRAINT chk_membership_reference_xor CHECK (
        (asset_class_name IS NOT NULL AND asset_class_type IS NOT NULL
         AND asset_symbol IS NOT NULL AND common_symbol IS NULL)
        OR
        (asset_class_name IS NULL AND asset_class_type IS NULL
         AND asset_symbol IS NULL AND common_symbol IS NOT NULL)
    )
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_memberships_current
    ON index_memberships (index_class_name, index_class_type) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_memberships_temporal
    ON index_memberships (index_class_name, index_class_type, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_memberships_common_symbol
    ON index_memberships (common_symbol) WHERE common_symbol IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_memberships_asset
    ON index_memberships (asset_class_name, asset_class_type, asset_symbol) WHERE asset_class_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_memberships_index_fk
    ON index_memberships (index_class_name, index_class_type);

-- Unique constraints for active memberships
CREATE UNIQUE INDEX IF NOT EXISTS idx_memberships_unique_api_active
    ON index_memberships (index_class_name, index_class_type, asset_class_name, asset_class_type, asset_symbol)
    WHERE valid_to IS NULL AND asset_class_name IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_memberships_unique_user_active
    ON index_memberships (index_class_name, index_class_type, common_symbol)
    WHERE valid_to IS NULL AND common_symbol IS NOT NULL;

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_index_memberships_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_memberships_updated_at ON index_memberships;
CREATE TRIGGER trigger_update_memberships_updated_at
    BEFORE UPDATE ON index_memberships
    FOR EACH ROW
    EXECUTE FUNCTION update_index_memberships_updated_at();

-- View: Current index memberships
CREATE OR REPLACE VIEW current_index_memberships AS
SELECT
    im.id, im.index_class_name, im.index_class_type,
    im.asset_class_name, im.asset_class_type, im.asset_symbol,
    im.common_symbol, im.weight, im.valid_from, im.source, im.metadata,
    cr.class_subtype as index_type,
    COALESCE(im.asset_symbol, im.common_symbol) as effective_symbol
FROM index_memberships im
JOIN code_registry cr ON cr.class_name = im.index_class_name AND cr.class_type = im.index_class_type
WHERE im.valid_to IS NULL;

-- View: Index summary
CREATE OR REPLACE VIEW index_summary AS
SELECT
    cr.class_name, cr.class_type, cr.class_subtype as index_type,
    cr.uploaded_at, cr.preferences,
    COUNT(im.id) FILTER (WHERE im.valid_to IS NULL) as current_member_count,
    MAX(im.updated_at) as last_membership_change
FROM code_registry cr
LEFT JOIN index_memberships im ON im.index_class_name = cr.class_name AND im.index_class_type = cr.class_type
WHERE cr.class_subtype IN ('IndexProvider', 'UserIndex')
GROUP BY cr.class_name, cr.class_type, cr.class_subtype, cr.uploaded_at, cr.preferences;

-- Function: Point-in-time membership query
CREATE OR REPLACE FUNCTION get_index_members_at(
    p_index_class_name TEXT,
    p_index_class_type TEXT DEFAULT 'provider',
    p_as_of TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
)
RETURNS TABLE (
    asset_class_name TEXT, asset_class_type TEXT, asset_symbol TEXT,
    common_symbol TEXT, weight DOUBLE PRECISION, effective_symbol TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT im.asset_class_name, im.asset_class_type, im.asset_symbol,
           im.common_symbol, im.weight,
           COALESCE(im.asset_symbol, im.common_symbol)
    FROM index_memberships im
    WHERE im.index_class_name = p_index_class_name
      AND im.index_class_type = p_index_class_type
      AND im.valid_from <= p_as_of
      AND (im.valid_to IS NULL OR im.valid_to > p_as_of);
END;
$$ LANGUAGE plpgsql STABLE;

-- FK to common_symbols for UserIndex memberships
-- Note: Only affects rows where common_symbol IS NOT NULL (UserIndex)
-- IndexProvider rows have common_symbol = NULL, so FK is not checked
ALTER TABLE index_memberships
    DROP CONSTRAINT IF EXISTS fk_membership_common_symbol;
ALTER TABLE index_memberships
    ADD CONSTRAINT fk_membership_common_symbol
    FOREIGN KEY (common_symbol)
    REFERENCES common_symbols(symbol)
    ON UPDATE CASCADE
    ON DELETE CASCADE;
