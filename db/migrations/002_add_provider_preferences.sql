-- Migration: Add provider preferences column
-- Date: 2026-01-02
-- Description: Adds preferences JSONB column to code_registry table for provider configuration options

-- Add preferences column to store provider-specific configuration
ALTER TABLE code_registry
ADD COLUMN IF NOT EXISTS preferences JSONB DEFAULT '{}';