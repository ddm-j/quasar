-- Asset Mappings
INSERT INTO asset_mapping (common_symbol, provider_name, provider_symbol) VALUES
    ('BTCUSD', 'EODHD', 'BTC-USD.CC'),
    ('ETHUSD', 'EODHD', 'ETH-USD.CC'),
    ('ADAUSD', 'EODHD', 'ADA-USD.CC'),
    ('XRPUSD', 'EODHD', 'XRP-USD.CC'),

    ('BTCUSD', 'KRAKEN', 'BTC/USD'),
    ('ETHUSD', 'KRAKEN', 'ETH/USD'),
    ('ADAUSD', 'KRAKEN', 'ADA/USD'),
    ('XRPUSD', 'KRAKEN', 'XRP/USD')
ON CONFLICT (provider_name, provider_symbol) DO NOTHING;

-- Testing Subscriptions to Initialize System
INSERT INTO provider_subscription
(provider, interval, sym)
VALUES
-- historical subscriptions
('EODHD', '1d', 'BTC-USD.CC'),
('EODHD', '1d', 'ETH-USD.CC'),
('EODHD', '1d', 'ADA-USD.CC'),
('EODHD', '1d', 'XRP-USD.CC'),
-- live subscriptions
('KRAKEN', '1h', 'BTC/USD'),
('KRAKEN', '1h', 'ETH/USD'),
('KRAKEN', '1h', 'ADA/USD'),
('KRAKEN', '1h', 'XRP/USD');