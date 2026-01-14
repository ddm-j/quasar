-- CODE REGISTRY TABLE
-- Serves as a registry for provider/broker code and user-defined indices
-- For providers/brokers: code columns (file_path, file_hash, nonce, ciphertext) are required
-- For UserIndex entries: code columns are NULL (no uploaded code)
CREATE TABLE IF NOT EXISTS code_registry (
    id SERIAL PRIMARY KEY,
    uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    class_name TEXT NOT NULL,
    class_type TEXT NOT NULL,
    class_subtype TEXT NOT NULL,
    file_path TEXT,
    file_hash BYTEA,
    nonce BYTEA,
    ciphertext BYTEA,
    preferences JSONB DEFAULT '{}',

    CONSTRAINT eq_class_name_type UNIQUE (class_name, class_type),

    -- Code columns must all be populated (normal providers) or all NULL (UserIndex only)
    CONSTRAINT chk_code_columns_consistency CHECK (
        (file_path IS NOT NULL AND file_hash IS NOT NULL AND nonce IS NOT NULL AND ciphertext IS NOT NULL)
        OR
        (file_path IS NULL AND file_hash IS NULL AND nonce IS NULL AND ciphertext IS NULL
         AND class_subtype = 'UserIndex')
    )
);

-- Index for class lookup
CREATE INDEX IF NOT EXISTS idx_code_registry_class_name_type ON code_registry (
    class_name,
    class_type
);

-- Partial unique indexes for code columns (allow multiple NULLs, enforce uniqueness on non-NULL)
CREATE UNIQUE INDEX IF NOT EXISTS idx_code_registry_file_path_unique
    ON code_registry (file_path) WHERE file_path IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_code_registry_file_hash_unique
    ON code_registry (file_hash) WHERE file_hash IS NOT NULL;

-- Index for filtering by class_subtype
CREATE INDEX IF NOT EXISTS idx_code_registry_class_subtype
    ON code_registry (class_subtype);

-- Partial index for quick lookup of index-type entries
CREATE INDEX IF NOT EXISTS idx_code_registry_indices
    ON code_registry (class_name, class_type)
    WHERE class_subtype IN ('IndexProvider', 'UserIndex')