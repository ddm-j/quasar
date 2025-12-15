// Generated enum definitions. Do not edit by hand.
export const ASSET_CLASSES = ['equity', 'fund', 'etf', 'bond', 'crypto', 'currency', 'future', 'option', 'index', 'commodity', 'derivative', 'cfd', 'warrant', 'adr', 'preferred', 'mutual_fund', 'money_market', 'rates', 'mbs', 'muni', 'structured_product'];
export const INTERVALS = ['1min', '5min', '15min', '30min', '1h', '4h', '1d', '1w', '1M'];

export const ASSET_CLASS_ALIASES = {
  'adr_pref': 'preferred',
  'bond_etf': 'etf',
  'futures': 'future',
  'fx': 'currency',
  'index_option': 'option',
  'mmf': 'money_market',
  'perp': 'future',
  'perps': 'future',
  'stock': 'equity'
};
export const INTERVAL_ALIASES = {
  'daily': '1d',
  'one_minute': '1min'
};
export const ASSET_CLASS_CANONICAL_MAP = Object.fromEntries(ASSET_CLASSES.map((c) => [c.toLowerCase(), c]));
export const INTERVAL_CANONICAL_MAP = Object.fromEntries(INTERVALS.map((c) => [c.toLowerCase(), c]));

export function normalizeAssetClass(value) {
  if (value == null) return null;
  const v = value.trim();
  if (!v) return null;
  const lower = v.toLowerCase();
  if (ASSET_CLASS_ALIASES[lower]) return ASSET_CLASS_ALIASES[lower];
  if (ASSET_CLASS_CANONICAL_MAP[lower]) return ASSET_CLASS_CANONICAL_MAP[lower];
  return lower; // leave as-is; caller may decide to reject
}

export function normalizeInterval(value) {
  if (value == null) return null;
  const v = value.trim();
  if (!v) return null;
  const lower = v.toLowerCase();
  if (INTERVAL_ALIASES[lower]) return INTERVAL_ALIASES[lower];
  if (INTERVAL_CANONICAL_MAP[lower]) return INTERVAL_CANONICAL_MAP[lower];
  return lower; // leave as-is; caller may decide to reject
}
