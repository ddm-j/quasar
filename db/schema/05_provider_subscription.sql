-- PROVDER SUBSCRIPTIONS TABLE
CREATE TABLE IF NOT EXISTS provider_subscription (
    provider TEXT NOT NULL,
    provider_class_type TEXT NOT NULL DEFAULT 'provider',
    interval TEXT NOT NULL,
    sym TEXT NOT NULL,
    cron TEXT,
    PRIMARY KEY (provider, interval, sym),

    CONSTRAINT fk_subscription_interval
        FOREIGN KEY (interval) REFERENCES accepted_intervals(interval), 

    CONSTRAINT fk_subscription_to_code_registry
        FOREIGN KEY (provider, provider_class_type)
        REFERENCES code_registry (class_name, class_type)
        ON DELETE CASCADE,

    CONSTRAINT fk_subscription_to_assets
        FOREIGN KEY (provider, provider_class_type, sym)
        REFERENCES assets (class_name, class_type, symbol)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS sub_cron_bucket
    ON provider_subscription (provider, interval, cron);


-- Trigger function to populate the cron field
CREATE OR REPLACE FUNCTION set_cron_field()
RETURNS TRIGGER AS $$
BEGIN
    -- Populate the cron field based on the interval
    NEW.cron := (SELECT cron FROM accepted_intervals WHERE interval = NEW.interval);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the function on INSERT or UPDATE
CREATE TRIGGER trigger_set_cron
BEFORE INSERT OR UPDATE ON provider_subscription
FOR EACH ROW
EXECUTE FUNCTION set_cron_field();