-- Enabel TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ACCEPTED DATA INERVALS TABLE
CREATE TABLE IF NOT EXISTS accepted_intervals (
    interval TEXT PRIMARY KEY,
    cron TEXT NOT NULL
);

-- Populate the table with default values
INSERT INTO accepted_intervals (interval, cron) VALUES
    ('1min', '* * * * *'),
    ('5min', '*/5 * * * *'),
    ('15min', '*/15 * * * *'),
    ('30min', '*/30 * * * *'),
    ('1h', '0 * * * *'),
    ('4h', '0 */4 * * *'),
    ('1d', '0 0 * * *'),
    ('1w', '0 0 * * 1'),
    ('1M', '0 0 1 * *')
ON CONFLICT (interval) DO NOTHING;