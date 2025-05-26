-- HISTORICAL DATA TABLE
CREATE TABLE IF NOT EXISTS historical_data (
    ts     timestamptz NOT NULL,
    sym           text NOT NULL,
    provider      text NOT NULL,
    provider_class_type text NOT NULL DEFAULT 'provider',
    interval      text NOT NULL,
    o DOUBLE PRECISION NOT NULL,
    h DOUBLE PRECISION NOT NULL,
    l DOUBLE PRECISION NOT NULL,
    c DOUBLE PRECISION NOT NULL,
    v DOUBLE PRECISION NOT NULL, 
    PRIMARY KEY (ts, sym, interval, provider),

    CONSTRAINT fk_historical_data_interval
        FOREIGN KEY (interval) REFERENCES accepted_intervals(interval),

    CONSTRAINT fk_historical_data_to_code_registry
        FOREIGN KEY (provider, provider_class_type)
        REFERENCES code_registry (class_name, class_type)
        ON DELETE CASCADE,
    
    CONSTRAINT fk_historical_data_to_assets
        FOREIGN KEY (provider, provider_class_type, sym)
        REFERENCES assets (class_name, class_type, symbol)
        ON DELETE CASCADE
);
-- Hypertable
SELECT create_hypertable('historical_data', 'ts', if_not_exists => TRUE);
-- Indexing
CREATE INDEX IF NOT EXISTS historical_data_sym_prov_ts
ON historical_data (sym, provider, ts DESC);

-- SYMBOL STATE TRACKER VIEW
CREATE or REPLACE VIEW historical_symbol_state AS
SELECT 
    sym,
    provider,
    max(ts) AS last_updated
FROM historical_data
GROUP BY sym, provider;