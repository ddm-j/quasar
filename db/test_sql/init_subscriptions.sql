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