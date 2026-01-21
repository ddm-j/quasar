/**
 * Asset column configuration for the Assets view.
 *
 * This file defines all available columns from the assets table,
 * their UI-friendly labels, filter types, and default visibility.
 *
 * Adding a new column:
 * 1. Add entry to ASSET_COLUMNS with the DB column name as key
 * 2. Set appropriate filterType, defaultVisible, and other properties
 * 3. If dropdown filter, add options to dropdownOptions or reference an enum
 */

import { ASSET_CLASSES } from '../enums'

// Filter type constants
export const FILTER_TYPES = {
  TEXT: 'text',
  DROPDOWN: 'dropdown',
  NONE: 'none',
}

// Dropdown options for specific columns
export const DROPDOWN_OPTIONS = {
  class_type: [
    { value: '', label: 'All' },
    { value: 'provider', label: 'Provider' },
    { value: 'broker', label: 'Broker' },
  ],
  primary_id_source: [
    { value: '', label: 'All' },
    { value: 'provider', label: 'Provider' },
    { value: 'matcher', label: 'Matcher' },
    { value: 'manual', label: 'Manual' },
  ],
  identity_match_type: [
    { value: '', label: 'All' },
    { value: 'exact_alias', label: 'Exact Alias' },
    { value: 'fuzzy_symbol', label: 'Fuzzy Symbol' },
  ],
  asset_class_group: [
    { value: '', label: 'All' },
    { value: 'securities', label: 'Securities' },
    { value: 'crypto', label: 'Crypto' },
  ],
}

// Generate asset_class dropdown options from enum
const formatLabel = (value) => {
  if (!value) return ''
  const withSpaces = value.replace(/_/g, ' ')
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1)
}

DROPDOWN_OPTIONS.asset_class = [
  { value: '', label: 'All' },
  ...ASSET_CLASSES.map((ac) => ({ value: ac, label: formatLabel(ac) })),
]

/**
 * Column definitions for the assets table.
 *
 * Properties:
 * - key: DB column name (used as unique identifier)
 * - label: UI-friendly display name
 * - filterType: FILTER_TYPES.TEXT | FILTER_TYPES.DROPDOWN | FILTER_TYPES.NONE
 * - defaultVisible: Whether column is visible by default
 * - sortable: Whether column supports sorting (default true)
 * - props: Additional props for CSmartTable column (e.g., className)
 * - apiFilterKey: Backend filter parameter name (for future use)
 * - apiExactMatch: Whether filter uses exact match vs partial (for future use)
 * - render: Hint for custom rendering (badge, date, number)
 */
export const ASSET_COLUMNS = {
  id: {
    key: 'id',
    label: 'ID',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: false,
    sortable: true,
    apiFilterKey: 'id',
    apiExactMatch: true,
  },
  symbol: {
    key: 'symbol',
    label: 'Symbol',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    props: { className: 'fw-semibold' },
    apiFilterKey: 'symbol_like',
    apiExactMatch: false,
  },
  name: {
    key: 'name',
    label: 'Name',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'name_like',
    apiExactMatch: false,
  },
  class_name: {
    key: 'class_name',
    label: 'Provider/Broker',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'class_name_like',
    apiExactMatch: false,
  },
  class_type: {
    key: 'class_type',
    label: 'Type',
    filterType: FILTER_TYPES.DROPDOWN,
    defaultVisible: true,
    sortable: false,
    render: 'badge',
    apiFilterKey: 'class_type',
    apiExactMatch: true,
  },
  asset_class: {
    key: 'asset_class',
    label: 'Asset Class',
    filterType: FILTER_TYPES.DROPDOWN,
    defaultVisible: true,
    sortable: false,
    render: 'badge',
    apiFilterKey: 'asset_class',
    apiExactMatch: true,
  },
  exchange: {
    key: 'exchange',
    label: 'Exchange',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'exchange_like',
    apiExactMatch: false,
  },
  base_currency: {
    key: 'base_currency',
    label: 'Base Currency',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'base_currency_like',
    apiExactMatch: false,
  },
  quote_currency: {
    key: 'quote_currency',
    label: 'Quote Currency',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'quote_currency_like',
    apiExactMatch: false,
  },
  country: {
    key: 'country',
    label: 'Country',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'country_like',
    apiExactMatch: false,
  },
  external_id: {
    key: 'external_id',
    label: 'External ID',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: false,
    sortable: true,
    apiFilterKey: 'external_id_like',
    apiExactMatch: false,
  },
  primary_id: {
    key: 'primary_id',
    label: 'Primary ID',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: true,
    sortable: true,
    apiFilterKey: 'primary_id_like',
    apiExactMatch: false,
  },
  primary_id_source: {
    key: 'primary_id_source',
    label: 'ID Source',
    filterType: FILTER_TYPES.DROPDOWN,
    defaultVisible: false,
    sortable: true,
    render: 'badge',
    apiFilterKey: 'primary_id_source',
    apiExactMatch: true,
  },
  matcher_symbol: {
    key: 'matcher_symbol',
    label: 'Matcher Symbol',
    filterType: FILTER_TYPES.TEXT,
    defaultVisible: false,
    sortable: true,
    apiFilterKey: 'matcher_symbol_like',
    apiExactMatch: false,
  },
  identity_conf: {
    key: 'identity_conf',
    label: 'Confidence',
    filterType: FILTER_TYPES.NONE,
    defaultVisible: false,
    sortable: true,
    render: 'number',
  },
  identity_match_type: {
    key: 'identity_match_type',
    label: 'Match Type',
    filterType: FILTER_TYPES.DROPDOWN,
    defaultVisible: false,
    sortable: true,
    render: 'badge',
    apiFilterKey: 'identity_match_type',
    apiExactMatch: true,
  },
  identity_updated_at: {
    key: 'identity_updated_at',
    label: 'Identity Updated',
    filterType: FILTER_TYPES.NONE,
    defaultVisible: false,
    sortable: true,
    render: 'date',
  },
  asset_class_group: {
    key: 'asset_class_group',
    label: 'Asset Group',
    filterType: FILTER_TYPES.DROPDOWN,
    defaultVisible: false,
    sortable: true,
    render: 'badge',
    apiFilterKey: 'asset_class_group',
    apiExactMatch: true,
  },
  sym_norm_full: {
    key: 'sym_norm_full',
    label: 'Normalized Symbol',
    filterType: FILTER_TYPES.NONE,
    defaultVisible: false,
    sortable: true,
  },
  sym_norm_root: {
    key: 'sym_norm_root',
    label: 'Root Symbol',
    filterType: FILTER_TYPES.NONE,
    defaultVisible: false,
    sortable: true,
  },
}

/**
 * Get array of all column keys in display order.
 */
export const getColumnKeys = () => Object.keys(ASSET_COLUMNS)

/**
 * Get array of column keys that are visible by default.
 */
export const getDefaultVisibleColumns = () =>
  Object.values(ASSET_COLUMNS)
    .filter((col) => col.defaultVisible)
    .map((col) => col.key)

/**
 * Get array of column keys that use text input filtering.
 */
export const getTextFilterKeys = () =>
  Object.values(ASSET_COLUMNS)
    .filter((col) => col.filterType === FILTER_TYPES.TEXT)
    .map((col) => col.key)

/**
 * Get dropdown options for a specific column.
 * @param {string} columnKey - The column key
 * @returns {Array|null} - Array of {value, label} options or null if not dropdown
 */
export const getDropdownOptions = (columnKey) => DROPDOWN_OPTIONS[columnKey] || null

/**
 * Get column configuration by key.
 * @param {string} columnKey - The column key
 * @returns {Object|null} - Column config object or null if not found
 */
export const getColumnConfig = (columnKey) => ASSET_COLUMNS[columnKey] || null
