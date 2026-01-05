-- CODE REGISTRY TABLE
-- Serves as a registry for provider/broker code
CREATE TABLE IF NOT EXISTS code_registry (
    id SERIAL PRIMARY KEY,
    uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    class_name TEXT NOT NULL,
    class_type TEXT NOT NULL,
    class_subtype TEXT NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    file_hash BYTEA NOT NULL UNIQUE,
    nonce BYTEA NOT NULL,
    ciphertext BYTEA NOT NULL,
    preferences JSONB DEFAULT '{}',
    CONSTRAINT eq_class_name_type UNIQUE (class_name, class_type)
);
CREATE INDEX IF NOT EXISTS idx_code_registry_class_name_type ON code_registry (
    class_name,
    class_type  
)