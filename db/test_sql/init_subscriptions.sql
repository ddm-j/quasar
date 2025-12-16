-- Asset Mappings
INSERT INTO asset_mapping (common_symbol, class_name, class_type, class_symbol) VALUES
    ('BTCUSD', 'EODHD', 'provider', 'BTC-USD.CC'),
    ('ETHUSD', 'EODHD', 'provider', 'ETH-USD.CC'),
    ('ADAUSD', 'EODHD', 'provider', 'ADA-USD.CC'),
    ('XRPUSD', 'EODHD', 'provider', 'XRP-USD.CC'),

--    ('BTCUSD', 'KRAKEN', 'provider', 'BTC/USD'),
    ('ETHUSD', 'KRAKEN', 'provider', 'ETH/USD'),
    ('ADAUSD', 'KRAKEN', 'provider', 'ADA/USD'),
    ('XRPUSD', 'KRAKEN', 'provider', 'XRP/USD')
ON CONFLICT (class_name, class_type, class_symbol) DO NOTHING;

-- Testing Subscriptions to Initialize System
INSERT INTO provider_subscription
(provider, provider_class_type, interval, sym)
VALUES
-- historical subscriptions
('EODHD', 'provider', '1d', 'BTC-USD.CC'),
('EODHD', 'provider', '1d', 'ETH-USD.CC'),
('EODHD', 'provider', '1d', 'ADA-USD.CC'),
('EODHD', 'provider', '1d', 'XRP-USD.CC'),
-- live subscriptions
--('KRAKEN', 'provider', '1h', 'BTC/USD'),
('KRAKEN', 'provider', '1h', 'ETH/USD'),
('KRAKEN', 'provider', '1h', 'ADA/USD'),
('KRAKEN', 'provider', '1h', 'XRP/USD');


-- Testing Subscriptions to Initialize System
INSERT INTO provider_subscription
(provider, provider_class_type, interval, sym)
VALUES
-- historical subscriptions
('EODHD', 'provider', '1d', 'AAPL.US'),
('EODHD', 'provider', '1d', 'MSFT.US'),
('EODHD', 'provider', '1d', 'PLTR.US');


-- Testing Subscriptions to Initialize System
INSERT INTO provider_subscription
(provider, provider_class_type, interval, sym)
VALUES
-- historical subscriptions
('DATABENTO', 'provider', '1d', 'AAPL');
--('DATABENTO', 'provider', '1d', 'MSFT');
--('DATABENTO', 'provider', '1d', 'PLTR');