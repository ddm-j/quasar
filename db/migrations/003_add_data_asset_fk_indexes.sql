-- Support cascading deletes by indexing FK columns for historical/live data.
CREATE INDEX IF NOT EXISTS idx_historical_data_asset_fk
    ON historical_data (provider, provider_class_type, sym);

CREATE INDEX IF NOT EXISTS idx_live_data_asset_fk
    ON live_data (provider, provider_class_type, sym);

