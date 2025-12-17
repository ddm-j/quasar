-- Migration: Remove FK constraints from hypertables to assets table
-- 
-- Problem: Deleting a provider with ~500 assets was taking 13-15 seconds because
-- each asset deletion triggered FK constraint checks across all TimescaleDB chunks.
-- With 1144 chunks × 525 assets = 601,125 constraint checks.
--
-- Solution: Remove the fk_*_to_assets constraints from hypertables. Provider-level
-- integrity is still maintained via fk_*_to_code_registry constraints.
--
-- Expected improvement: Provider deletion from ~14s to ~500ms

-- Drop FK constraints from historical_data hypertable
-- (This propagates to all chunks automatically)
ALTER TABLE historical_data 
    DROP CONSTRAINT IF EXISTS fk_historical_data_to_assets;

-- Drop FK constraints from live_data hypertable
ALTER TABLE live_data 
    DROP CONSTRAINT IF EXISTS fk_live_data_to_assets;

-- Drop indexes that were added specifically for FK performance (migration 003)
-- These are no longer needed without the FK constraints
DROP INDEX IF EXISTS idx_historical_data_asset_fk;
DROP INDEX IF EXISTS idx_live_data_asset_fk;

