-- HISTORICAL DATA TABLE
CREATE TABLE IF NOT EXISTS historical_data (
    ts     timestamptz,
    sym           text,
    provider      text,
    interval      text,
    o DOUBLE PRECISION,
    h DOUBLE PRECISION,
    l DOUBLE PRECISION,
    c DOUBLE PRECISION,
    v DOUBLE PRECISION, 
    PRIMARY KEY (ts, sym, interval, provider)
);
-- Hypertable
SELECT create_hypertable('historical_data', 'ts', if_not_exists => TRUE);
-- Indexing
CREATE INDEX IF NOT EXISTS historical_data_sym_prov_ts
ON historical_data (sym, provider, ts DESC);

-- SYMBOL STATE TRACKER VIEW
CREATE or REPLACE VIEW symbol_state AS
SELECT 
    sym,
    provider,
    max(ts) AS last_updated
FROM historical_data
GROUP BY sym, provider;

-- PROVDER SUBSCRIPTIONS TABLE
CREATE TABLE IF NOT EXISTS provider_subscription (
    provider TEXT,
    interval TEXT,
    sym TEXT,
    cron TEXT,
    PRIMARY KEY (provider, interval, sym)
);
CREATE INDEX IF NOT EXISTS sub_cron_bucket
    ON provider_subscription (provider, interval, cron);