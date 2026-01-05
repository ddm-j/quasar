import React, { useState, useEffect, useMemo } from 'react';
import {
    CCard,
    CCardBody,
    CSmartTable,
    CSmartPagination,
    CFormSelect,
    CAlert,
    CRow,
    CCol,
    CBadge,
} from '@coreui/react-pro';
import { getCommonSymbols } from '../services/registry_api';
import CommonSymbolDetailModal from './CommonSymbolDetailModal';

const CommonSymbolsTab = () => {
    // State
    const [commonSymbols, setCommonSymbols] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [totalItems, setTotalItems] = useState(0);
    const [activePage, setActivePage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(10);
    const [sorter, setSorter] = useState({});
    const [columnFilter, setColumnFilter] = useState({});
    const [liveTextInputFilters, setLiveTextInputFilters] = useState({});

    // Modal state
    const [selectedSymbol, setSelectedSymbol] = useState(null);
    const [isDetailModalVisible, setIsDetailModalVisible] = useState(false);

    // Define columns for CSmartTable
    const columns = [
        { key: 'common_symbol', label: "Common Symbol", _props: { className: 'fw-semibold' }, sortable: true },
        { key: 'provider_count', label: "Providers", sortable: true }
    ];

    const fetchCommonSymbolsData = async () => {
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
                if (key === 'common_symbol_like') {
                    apiParams[key] = columnFilter[key];
                } else {
                    apiParams[key] = columnFilter[key];
                }
            }
        }

        console.log(`[CommonSymbolsTab.js] fetchCommonSymbolsData: For activePage=${activePage}, itemsPerPage=${itemsPerPage}. Requesting with API params: ${JSON.stringify(apiParams)}`);

        try {
            const data = await getCommonSymbols(apiParams);
            console.log(`[CommonSymbolsTab.js] fetchCommonSymbolsData: API Response for page ${activePage}: items_count=${data.items?.length}, total_items_from_api=${data.total_items}`);

            setCommonSymbols(data.items || []);
            setTotalItems(data.total_items || 0);

        } catch (err) {
            console.error(`[CommonSymbolsTab.js] fetchCommonSymbolsData: Error fetching common symbols for page ${activePage}:`, err);
            setError(err.message || 'Failed to fetch common symbols');
            setCommonSymbols([]);
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
                // For common symbols, we only have common_symbol_like filter
                if (liveTextInputFilters.common_symbol) {
                    newMainFilters.common_symbol_like = liveTextInputFilters.common_symbol;
                } else {
                    delete newMainFilters.common_symbol_like;
                }
                return newMainFilters;
            });
            setActivePage(1);
        }, 1000);

        return () => {
            clearTimeout(handler);
        };
    }, [liveTextInputFilters]);

    useEffect(() => {
        setError(null);
        console.log(`[CommonSymbolsTab.js] useEffect: Triggering fetch. Current state: activePage=${activePage}, itemsPerPage=${itemsPerPage}, sorter=${JSON.stringify(sorter)}, columnFilter=${JSON.stringify(columnFilter)}`);
        fetchCommonSymbolsData();
    }, [activePage, itemsPerPage, sorter, columnFilter]);

    const calculatedPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 1;

    const handleItemsPerPageChange = (event) => {
        const newSize = parseInt(event.target.value, 10);
        console.log(`[CommonSymbolsTab.js] handleItemsPerPageChange: newSize=${newSize}. Current itemsPerPage state is ${itemsPerPage}.`);
        setActivePage(1);
        setItemsPerPage(newSize);
    };

    const handleSorterChange = (sorterData) => {
        console.log('[CommonSymbolsTab.js] handleSorterChange:', sorterData);

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
        <CCard>
            <CCardBody>
                {error && <CAlert color="danger">{error}</CAlert>}
                <CSmartTable
                    items={commonSymbols}
                    columns={columns}
                    loading={loading}
                    itemsPerPage={itemsPerPage}
                    pagination={false}
                    columnSorter={{ external: true, resetable: true }}
                    onSorterChange={handleSorterChange}
                    columnFilter
                    columnFilterValue={liveTextInputFilters}
                    onColumnFilterChange={setLiveTextInputFilters}
                    scopedColumns={{
                        common_symbol: (item) => (
                            <td>
                                <span
                                    style={{ cursor: 'pointer', textDecoration: 'underline' }}
                                    onClick={() => {
                                        setSelectedSymbol(item.common_symbol);
                                        setIsDetailModalVisible(true);
                                    }}
                                >
                                    {item.common_symbol}
                                </span>
                            </td>
                        ),
                        provider_count: (item) => (
                            <td>
                                <CBadge color="info">
                                    {item.provider_count}
                                </CBadge>
                            </td>
                        ),
                    }}
                    tableProps={{
                        striped: true,
                        hover: true,
                        responsive: true,
                        className: 'align-middle'
                    }}
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

        <CommonSymbolDetailModal
            visible={isDetailModalVisible}
            commonSymbol={selectedSymbol}
            onClose={() => {
                setIsDetailModalVisible(false);
                setSelectedSymbol(null);
            }}
        />
    </>;
};

export default CommonSymbolsTab;