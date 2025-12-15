-- Generated from enums YAML. Do not edit by hand.
-- Asset classes lookup
CREATE TABLE IF NOT EXISTS asset_class (
    code TEXT PRIMARY KEY
);
INSERT INTO asset_class (code) VALUES
    ('equity'),
    ('fund'),
    ('etf'),
    ('bond'),
    ('crypto'),
    ('currency'),
    ('future'),
    ('option'),
    ('index'),
    ('commodity'),
    ('derivative'),
    ('cfd'),
    ('warrant'),
    ('adr'),
    ('preferred'),
    ('mutual_fund'),
    ('money_market'),
    ('rates'),
    ('mbs'),
    ('muni'),
    ('structured_product')
ON CONFLICT (code) DO NOTHING;

-- Accepted intervals with cron
CREATE TABLE IF NOT EXISTS accepted_intervals (
    interval TEXT PRIMARY KEY,
    cron TEXT NOT NULL
);
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
