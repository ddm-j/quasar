// Empty "Assets" view component
import { React, useState, useEffect } from 'react'; 
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
} from '@coreui/react-pro';
import { getAssets } from '../services/registry_api';

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

    const textInputFilterKeys = ['symbol', 'name', 'class_name', 'exchange', 'base_currency', 'quote_currency', 'country'];

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
            if (columnFilter[key]) { // Ensure value exists
                if (textInputFilterKeys.includes(key)) { // Check if it's a key that expects '_like'
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
            // Apply the debounced text inputs to the main columnFilter state
            setColumnFilter(prevMainFilters => {
                const newMainFilters = { ...prevMainFilters }; // Preserve existing dropdown filters

                // Update or remove text input based filters
                textInputFilterKeys.forEach(key => {
                    if (liveTextInputFilters[key]) {
                        newMainFilters[key] = liveTextInputFilters[key];
                    } else {
                        delete newMainFilters[key]; // Remove if text input for this key is empty
                    }
                });
                return newMainFilters;
            });
            setActivePage(1);
        }, 1000); // 500ms delay, adjust as needed

        return () => {
            clearTimeout(handler);
        };
    }, [liveTextInputFilters, setActivePage]); // Rerun when live text inputs change

    useEffect(() => {
        setError(null); 
        console.log(`[Assets.js] useEffect: Triggering fetch. Current state: activePage=${activePage}, itemsPerPage=${itemsPerPage}, sorter=${JSON.stringify(sorter)}, columnFilter=${JSON.stringify(columnFilter)}`);
        fetchAssetsData(); 
    }, [activePage, itemsPerPage, sorter, columnFilter]);

    
    const getClassBadge = (class_type) => {
    switch (class_type) {
        case 'provider': return 'primary';
        case 'broker': return 'secondary';
        default: return 'light';
        }
    }
    const getAssetClassBadge = (asset_class) => {
        switch (asset_class) {
            case 'equity': return 'success';
            case 'fund': return 'info';
            case 'etf': return 'warning';
            case 'bond': return 'dark';
            case 'crypto': return 'danger';
            case 'currency': return 'primary';
            default: return 'secondary';
        }
    }

    const classTypeFilterOptions = [
        { value: '', label: 'All' }, 
        { value: 'provider', label: 'Provider' },
        { value: 'broker', label: 'Broker' },
    ]
    const assetClassFilterOptions = [
        { value: '', label: 'All' }, 
        { value: 'equity', label: 'Equity' },
        { value: 'fund', label: 'Fund' },
        { value: 'etf', label: 'ETF' },
        { value: 'bond', label: 'Bond' },
        { value: 'crypto', label: 'Crypto' },
        { value: 'currency', label: 'Currency' },
    ]

    const columns = [
        { key: 'symbol', label: "Symbol", _props: { className: 'fw-semibold' } },
        { key: 'name', label: "Name" },
        { key: 'class_name', label: "Class Name" },
        { 
            key: 'class_type', 
            label: "Class Type", 
            filter: () => { 
                const currentFilterValue = columnFilter.class_type || '';
                const currentLabel = classTypeFilterOptions.find(opt => opt.value === currentFilterValue)?.label || 'Select Type';

                return (
                    <CDropdown size="sm">
                        <CDropdownToggle size="sm" color="secondary" variant="outline" style={{ width: '100%', textAlign: 'start' }}>
                            {currentLabel}
                        </CDropdownToggle>
                        <CDropdownMenu style={{ maxHeight: '200px', overflowY: 'auto', width: '100%' }}>
                            {classTypeFilterOptions.map(option => (
                                <CDropdownItem
                                    key={option.value}
                                    active={option.value === currentFilterValue}
                                    onClick={() => {
                                        setColumnFilter(prevFilters => {
                                            const newFilters = { ...prevFilters };
                                            if (option.value === '') {
                                                delete newFilters.class_type; // Remove filter if 'All' is selected
                                            } else {
                                                newFilters.class_type = option.value; // Set specific asset class
                                            }
                                            return newFilters;
                                        });
                                        setActivePage(1); // Reset to page 1 when filter changes
                                    }}
                                >
                                    {option.label}
                                </CDropdownItem>
                            ))}
                        </CDropdownMenu>
                    </CDropdown>
                );
            },
            sorter: false,
        },
        { 
            key: 'asset_class', 
            label: "Asset Class", 
            filter: () => { 
                const currentFilterValue = columnFilter.asset_class || '';
                const currentLabel = assetClassFilterOptions.find(opt => opt.value === currentFilterValue)?.label || 'Select Type';

                return (
                    <CDropdown size="sm">
                        <CDropdownToggle size="sm" color="secondary" variant="outline" style={{ width: '100%', textAlign: 'start' }}>
                            {currentLabel}
                        </CDropdownToggle>
                        <CDropdownMenu style={{ maxHeight: '200px', overflowY: 'auto', width: '100%' }}>
                            {assetClassFilterOptions.map(option => (
                                <CDropdownItem
                                    key={option.value}
                                    active={option.value === currentFilterValue}
                                    onClick={() => {
                                        setColumnFilter(prevFilters => {
                                            const newFilters = { ...prevFilters };
                                            if (option.value === '') {
                                                delete newFilters.asset_class; // Remove filter if 'All' is selected
                                            } else {
                                                newFilters.asset_class = option.value; // Set specific asset class
                                            }
                                            return newFilters;
                                        });
                                        setActivePage(1); // Reset to page 1 when filter changes
                                    }}
                                >
                                    {option.label}
                                </CDropdownItem>
                            ))}
                        </CDropdownMenu>
                    </CDropdown>
                );
            },
            sorter: false 
        },
        { key: 'exchange', label: "Exchange"},
        { key: 'base_currency', label: "Base CCY" },
        { key: 'quote_currency', label: "Quote CCY" },
        { key: 'country', label: "Country" },
    ];
    
    const calculatedPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 1;

    const handleItemsPerPageChange = (event) => {
        const newSize = parseInt(event.target.value, 10);
        console.log(`[Assets.js] handleItemsPerPageChange: newSize=${newSize}. Current itemsPerPage state is ${itemsPerPage}.`);
        setActivePage(1); // Reset to page 1 when items per page changes
        setItemsPerPage(newSize);
    };
    
    return (
        <CRow>
        <CCol xs={12}>
            <CCard>
                <CCardHeader>
                    <h5>Available Assets</h5>
                </CCardHeader>
                <CCardBody>
                    {error && <CAlert color="danger">{error}</CAlert>}
                    {/* Items per page selector */}
                    <CSmartTable
                        items={assets}
                        columns={columns}
                        loading={loading}
                        itemsPerPage={itemsPerPage}
                        pagination={false} // Disable CSmartTable's internal pagination
                        // All other pagination props removed from CSmartTable
                        tableProps={{
                            striped: true,
                            hover: true,
                            responsive: true,
                            className: 'align-middle' 
                        }}

                        // Filtering and sorting
                        columnFilter
                        columnFilterValue={liveTextInputFilters}
                        onColumnFilterChange={setLiveTextInputFilters}

                        scopedColumns={{
                            class_type: (item) => (
                                <td>
                                    <CBadge color={getClassBadge(item.class_type)}>
                                        {item.class_type ? item.class_type.charAt(0).toUpperCase() + item.class_type.slice(1) : ''}
                                    </CBadge>
                                </td>
                            ),
                            asset_class: (item) => (
                                <td>
                                    <CBadge color={getAssetClassBadge(item.asset_class)}>
                                        {item.asset_class ? item.asset_class.charAt(0).toUpperCase() + item.asset_class.slice(1) : ''}
                                    </CBadge>
                                </td>
                            ),
                        }}
                    />

                    {/* External CSmartPagination */}
                    <CRow className="mb-3 align-items-center justify-content-center">
                        {calculatedPages > 1 && ( // Only show pagination if there's more than one page
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
    )
}
export default Assets;