-- Migration: Add indexes for identity column filtering and sorting
-- Date: 2026-01-02
-- Description: Supports Phase 2 backend API enhancements for asset identity columns

-- Index for filtering by identity match type (exact_alias, fuzzy_symbol)
CREATE INDEX IF NOT EXISTS idx_assets_identity_match_type
ON assets (identity_match_type) WHERE identity_match_type IS NOT NULL;

-- Index for sorting by confidence score
CREATE INDEX IF NOT EXISTS idx_assets_identity_conf
ON assets (identity_conf) WHERE identity_conf IS NOT NULL;