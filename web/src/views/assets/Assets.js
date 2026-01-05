/**
 * Assets view component with configurable columns.
 * 
 * Columns are driven by the assetColumns.js configuration file.
 * Users can toggle column visibility via the Column Selector modal.
 */
import React, { useState, useEffect, useMemo } from 'react'; 
import { 
    CCard, 
    CCardBody, 
    CCardHeader, 
    CCol, 
    CRow,
    CSmartTable,
    CSmartPagination,
    CDropdown,
    CDropdownToggle,
    CDropdownMenu,
    CDropdownItem,
    CFormSelect,
    CBadge,
    CAlert,
    CButton,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilSettings } from '@coreui/icons';
import { getAssets } from '../services/registry_api';
import { 
    ASSET_COLUMNS, 
    FILTER_TYPES, 
    getDefaultVisibleColumns, 
    getTextFilterKeys,
    getDropdownOptions 
} from '../../configs/assetColumns';
import ColumnSelectorModal from './ColumnSelectorModal';

// LocalStorage key for persisting column visibility preferences
const STORAGE_KEY = 'quasar_assets_visible_columns';

const Assets = () => {
    // State
    const [assets, setAssets] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [totalItems, setTotalItems] = useState(0); 
    const [activePage, setActivePage] = useState(1); 
    const [itemsPerPage, setItemsPerPage] = useState(10); 
    const [sorter, setSorter] = useState({}); 
    const [columnFilter, setColumnFilter] = useState({}); 
    const [liveTextInputFilters, setLiveTextInputFilters] = useState({});
    
    // Column visibility state (initialized from localStorage or config defaults)
    const [visibleColumns, setVisibleColumns] = useState(() => {
        try {
            const saved = localStorage.getItem(STORAGE_KEY);
            if (saved) {
                const parsed = JSON.parse(saved);
                // Validate that parsed is an array of strings
                if (Array.isArray(parsed) && parsed.every(k => typeof k === 'string')) {
                    return parsed;
                }
            }
        } catch (e) {
            console.warn('Failed to load column preferences from localStorage:', e);
        }
        return getDefaultVisibleColumns();
    });
    const [isColumnSelectorVisible, setIsColumnSelectorVisible] = useState(false);

    // Persist column visibility to localStorage when it changes
    useEffect(() => {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(visibleColumns));
        } catch (e) {
            console.warn('Failed to save column preferences to localStorage:', e);
        }
    }, [visibleColumns]);

    // Get text filter keys from config
    const textInputFilterKeys = useMemo(() => getTextFilterKeys(), []);

    // Badge color mappings
    const classTypeBadgeColors = {
        provider: 'primary',
        broker: 'secondary'
    };

    const assetClassBadgeColors = {
        equity: 'success',
        fund: 'info',
        etf: 'warning',
        bond: 'dark',
        crypto: 'danger',
        currency: 'primary',
    };

    const primaryIdSourceBadgeColors = {
        provider: 'info',
        matcher: 'success',
        manual: 'warning'
    };

    const identityMatchTypeBadgeColors = {
        exact_alias: 'success',
        fuzzy_symbol: 'warning'
    };

    const assetClassGroupBadgeColors = {
        securities: 'primary',
        crypto: 'danger'
    };

    // Utility function for formatting labels
    const formatLabel = (value) => {
        if (!value) return '';
        const withSpaces = value.replace(/_/g, ' ');
        return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1);
    };

    // Utility function to format confidence as percentage
    const formatConfidence = (value) => {
        if (value === null || value === undefined) return '';
        return `${Math.round(value)}%`;
    };

    // Utility function to format date
    const formatDate = (value) => {
        if (!value) return '';
        try {
            return new Date(value).toLocaleDateString();
        } catch {
            return value;
        }
    };

    // Factory function to create dropdown filter component
    const createDropdownFilter = (columnKey, options, setColumnFilter) => {
        return (columnValues, setFilterValue, currentFilterValue) => {
            const currentLabel = options.find(opt => opt.value === currentFilterValue)?.label || 'All';

            return (
                <CDropdown size="sm">
                    <CDropdownToggle size="sm" color="secondary" variant="outline" style={{ width: '100%', textAlign: 'start' }}>
                        {currentLabel}
                    </CDropdownToggle>
                    <CDropdownMenu style={{ maxHeight: '200px', overflowY: 'auto', width: '100%' }}>
                        {options.map(option => (
                            <CDropdownItem
                                key={option.value}
                                active={option.value === currentFilterValue}
                                onClick={() => {
                                    // Update CSmartTable's internal state
                                    setFilterValue(option.value);
                                    // Update parent's columnFilter state for API calls
                                    setColumnFilter(prev => {
                                        const updated = { ...prev };
                                        if (option.value === '') {
                                            // Remove filter when "All" is selected
                                            delete updated[columnKey];
                                        } else {
                                            // Set filter value
                                            updated[columnKey] = option.value;
                                        }
                                        return updated;
                                    });
                                }}
                            >
                                {option.label}
                            </CDropdownItem>
                        ))}
                    </CDropdownMenu>
                </CDropdown>
            );
        };
    };

    // Generate columns array from configuration based on visible columns
    const columns = useMemo(() => {
        return visibleColumns
            .filter(key => ASSET_COLUMNS[key]) // Ensure column exists in config
            .map(key => {
                const config = ASSET_COLUMNS[key];
                const column = {
                    key: config.key,
                    label: config.label,
                    _props: config.props || {},
                    sorter: config.sortable !== false,
                };

                // Handle filter type
                if (config.filterType === FILTER_TYPES.DROPDOWN) {
                    const options = getDropdownOptions(config.key);
                    if (options) {
                        column.filter = createDropdownFilter(config.key, options, setColumnFilter);
                    }
                    column.sorter = false; // Dropdowns typically don't sort
                } else if (config.filterType === FILTER_TYPES.NONE) {
                    column.filter = false;
                }
                // TEXT filter type uses default behavior (text input)

                return column;
            });
    }, [visibleColumns, columnFilter, setColumnFilter]);

    // Generate scoped columns for custom rendering
    const scopedColumns = useMemo(() => {
        const scoped = {};

        visibleColumns.forEach(key => {
            const config = ASSET_COLUMNS[key];
            if (!config) return;

            // Handle badge rendering
            if (config.render === 'badge') {
                if (key === 'class_type') {
                    scoped[key] = (item) => (
                        <td>
                            <CBadge color={classTypeBadgeColors[item.class_type] || 'light'}>
                                {item.class_type ? formatLabel(item.class_type) : ''}
                            </CBadge>
                        </td>
                    );
                } else if (key === 'asset_class') {
                    scoped[key] = (item) => (
                        <td>
                            <CBadge color={assetClassBadgeColors[item.asset_class] || 'secondary'}>
                                {item.asset_class ? formatLabel(item.asset_class) : ''}
                            </CBadge>
                        </td>
                    );
                } else if (key === 'primary_id_source') {
                    scoped[key] = (item) => (
                        <td>
                            {item.primary_id_source ? (
                                <CBadge color={primaryIdSourceBadgeColors[item.primary_id_source] || 'light'}>
                                    {formatLabel(item.primary_id_source)}
                                </CBadge>
                            ) : ''}
                        </td>
                    );
                } else if (key === 'identity_match_type') {
                    scoped[key] = (item) => (
                        <td>
                            {item.identity_match_type ? (
                                <CBadge color={identityMatchTypeBadgeColors[item.identity_match_type] || 'light'}>
                                    {formatLabel(item.identity_match_type)}
                                </CBadge>
                            ) : ''}
                        </td>
                    );
                } else if (key === 'asset_class_group') {
                    scoped[key] = (item) => (
                        <td>
                            {item.asset_class_group ? (
                                <CBadge color={assetClassGroupBadgeColors[item.asset_class_group] || 'light'}>
                                    {formatLabel(item.asset_class_group)}
                                </CBadge>
                            ) : ''}
                        </td>
                    );
                }
            }
            // Handle number rendering (confidence)
            else if (config.render === 'number') {
                if (key === 'identity_conf') {
                    scoped[key] = (item) => (
                        <td>{formatConfidence(item.identity_conf)}</td>
                    );
                }
            }
            // Handle date rendering
            else if (config.render === 'date') {
                scoped[key] = (item) => (
                    <td>{formatDate(item[key])}</td>
                );
            }
        });

        return scoped;
    }, [visibleColumns]);

    const fetchAssetsData = async () => { 
        setLoading(true);
        const apiParams = {
            limit: itemsPerPage,
            offset: (activePage - 1) * itemsPerPage,
        };

        if (sorter && sorter.column) {
            apiParams.sort_by = sorter.column;
            apiParams.sort_order = sorter.direction || 'asc';
        }
        
        // Iterate over the main columnFilter for API params
        for (const key in columnFilter) {
            if (columnFilter[key]) {
                if (textInputFilterKeys.includes(key)) {
                    apiParams[`${key}_like`] = columnFilter[key];
                } else {
                    apiParams[key] = columnFilter[key]; 
                }
            }
        }
        
        console.log(`[Assets.js] fetchAssetsData: For activePage=${activePage}, itemsPerPage=${itemsPerPage}. Requesting with API params: ${JSON.stringify(apiParams)}`);

        try {
            const data = await getAssets(apiParams);
            console.log(`[Assets.js] fetchAssetsData: API Response for page ${activePage}: items_count=${data.items?.length}, total_items_from_api=${data.total_items}`);
            
            setAssets(data.items || []);
            setTotalItems(data.total_items || 0); 

        } catch (err) {
            console.error(`[Assets.js] fetchAssetsData: Error fetching assets for page ${activePage}:`, err);
            setError(err.message || 'Failed to fetch assets');
            setAssets([]); 
            setTotalItems(0); 
        } finally {
            setLoading(false);
        }
    };

    // useEffect for debouncing live text input filters
    useEffect(() => {
        const handler = setTimeout(() => {
            setColumnFilter(prevMainFilters => {
                const newMainFilters = { ...prevMainFilters };

                textInputFilterKeys.forEach(key => {
                    if (liveTextInputFilters[key]) {
                        newMainFilters[key] = liveTextInputFilters[key];
                    } else {
                        delete newMainFilters[key];
                    }
                });
                return newMainFilters;
            });
            setActivePage(1);
        }, 1000);

        return () => {
            clearTimeout(handler);
        };
    }, [liveTextInputFilters, textInputFilterKeys]);

    useEffect(() => {
        setError(null); 
        console.log(`[Assets.js] useEffect: Triggering fetch. Current state: activePage=${activePage}, itemsPerPage=${itemsPerPage}, sorter=${JSON.stringify(sorter)}, columnFilter=${JSON.stringify(columnFilter)}`);
        fetchAssetsData(); 
    }, [activePage, itemsPerPage, sorter, columnFilter]);

    const calculatedPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 1;

    const handleItemsPerPageChange = (event) => {
        const newSize = parseInt(event.target.value, 10);
        console.log(`[Assets.js] handleItemsPerPageChange: newSize=${newSize}. Current itemsPerPage state is ${itemsPerPage}.`);
        setActivePage(1);
        setItemsPerPage(newSize);
    };

    const handleSorterChange = (sorterData) => {
        console.log('[Assets.js] handleSorterChange:', sorterData);

        // CSmartTable provides SorterValue: { column: string, state: 'asc' | 'desc' | 0 }
        // When resetable=true, state can be 0 (null/cleared) on third click

        if (!sorterData) {
            // No sorting - clear state
            setSorter({});
            setActivePage(1);
            return;
        }

        // Handle single sorter (non-multiple mode)
        if (sorterData.column && sorterData.state !== 0) {
            setSorter({
                column: sorterData.column,
                direction: sorterData.state  // Map 'state' to 'direction' for API
            });
            setActivePage(1);
        } else {
            // State is 0 (cleared) or invalid
            setSorter({});
            setActivePage(1);
        }
    };

    return (
        <>
            <CRow>
                <CCol xs={12}>
                    <CCard>
                        <CCardHeader>
                            <CRow className="align-items-center">
                                <CCol xs={6} md={8} xl={9} className="text-start">
                                    <h5>Available Assets</h5>
                                </CCol>
                                <CCol xs={6} md={4} xl={3} className="d-flex justify-content-end">
                                    <CButton 
                                        color="secondary" 
                                        variant="outline"
                                        onClick={() => setIsColumnSelectorVisible(true)}
                                    >
                                        <CIcon icon={cilSettings} className="me-1" />
                                        Columns
                                    </CButton>
                                </CCol>
                            </CRow>
                        </CCardHeader>
                        <CCardBody>
                            {error && <CAlert color="danger">{error}</CAlert>}
                            <CSmartTable
                                items={assets}
                                columns={columns}
                                loading={loading}
                                itemsPerPage={itemsPerPage}
                                pagination={false}
                                columnSorter={{ external: true, resetable: true }}
                                onSorterChange={handleSorterChange}
                                tableProps={{
                                    striped: true,
                                    hover: true,
                                    responsive: true,
                                    className: 'align-middle' 
                                }}

                                columnFilter
                                columnFilterValue={liveTextInputFilters}
                                onColumnFilterChange={setLiveTextInputFilters}

                                scopedColumns={scopedColumns}
                            />

                            <CRow className="mb-3 align-items-center justify-content-center">
                                {calculatedPages > 1 && (
                                    <CCol xs="auto">
                                        <CSmartPagination
                                            activePage={activePage}
                                            pages={calculatedPages}
                                            onActivePageChange={setActivePage}
                                        />
                                    </CCol>
                                )}
                                <CCol xs="auto" className="me-2">
                                    <label htmlFor="itemsPerPageSelect" className="col-form-label">Items per page:</label>
                                </CCol>
                                <CCol xs="auto">
                                    <CFormSelect 
                                        id="itemsPerPageSelect"
                                        value={itemsPerPage}
                                        onChange={handleItemsPerPageChange}
                                        options={[
                                            { label: '5', value: 5 },
                                            { label: '10', value: 10 },
                                            { label: '20', value: 20 },
                                            { label: '50', value: 50 },
                                            { label: '100', value: 100 },
                                        ]}
                                    />
                                </CCol>
                            </CRow>
                        </CCardBody>
                    </CCard>
                </CCol>
            </CRow>

            <ColumnSelectorModal
                visible={isColumnSelectorVisible}
                onClose={() => setIsColumnSelectorVisible(false)}
                visibleColumns={visibleColumns}
                setVisibleColumns={setVisibleColumns}
            />
        </>
    );
};

export default Assets;
