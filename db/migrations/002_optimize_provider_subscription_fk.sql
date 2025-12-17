-- Add supporting index so provider_subscription FK cascades use b-tree lookups.
CREATE INDEX IF NOT EXISTS idx_provider_subscription_asset_fk
    ON provider_subscription (provider, provider_class_type, sym);

