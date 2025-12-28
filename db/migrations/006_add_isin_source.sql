-- Migration: Add isin_source column to track ISIN origin
-- Tracks whether ISIN came from provider directly, identity matcher, or manual entry
--
-- Usage:
--   docker exec -i quasardb psql -U postgres -d postgres < db/migrations/006_add_isin_source.sql
--
-- Or connect manually:
--   docker exec -it quasardb psql -U postgres -d postgres
--   \i /path/to/006_add_isin_source.sql

BEGIN;

-- Step 1: Add isin_source column
ALTER TABLE assets 
ADD COLUMN IF NOT EXISTS isin_source TEXT;

-- Step 2: Add check constraint for valid values
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_isin_source'
    ) THEN
        ALTER TABLE assets 
        ADD CONSTRAINT chk_isin_source 
        CHECK (isin_source IN ('provider', 'matcher', 'manual') OR isin_source IS NULL);
    END IF;
END
$$;

-- Step 3: Backfill existing data - assume provider-supplied if ISIN exists
UPDATE assets 
SET isin_source = 'provider' 
WHERE isin IS NOT NULL AND isin_source IS NULL;

-- Step 4: Index for efficient filtering by source
CREATE INDEX IF NOT EXISTS idx_assets_isin_source 
ON assets (isin_source) WHERE isin_source IS NOT NULL;

COMMIT;

-- Verify the migration
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM information_schema.columns 
WHERE table_name = 'assets' 
  AND column_name = 'isin_source';

SELECT 
    conname, 
    pg_get_constraintdef(oid) 
FROM pg_constraint 
WHERE conname = 'chk_isin_source';

