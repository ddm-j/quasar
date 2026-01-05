/**
 * Common Symbols tab component with server-side pagination, sorting, and filtering.
 * 
 * Follows the same patterns as Assets.js for server-side data management.
 */
import React, { useState, useEffect } from 'react';
import {
    CCard,
    CCardBody,
    CCardHeader,
    CCol,
    CRow,
    CSmartTable,
    CSmartPagination,
    CFormSelect,
    CBadge,
    CAlert,
    CButton,
    CFormInput,
    CInputGroup,
    CInputGroupText,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilSearch, cilZoom } from '@coreui/icons';
import { getCommonSymbols } from '../services/registry_api';
import CommonSymbolDetailModal from './CommonSymbolDetailModal';

const CommonSymbolsTab = () => {
    // Data state
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [totalItems, setTotalItems] = useState(0);

    // Pagination state (simple primitives, not objects)
    const [activePage, setActivePage] = useState(1);
    const [itemsPerPage, setItemsPerPage] = useState(25);

    // Sorting state
    const [sorter, setSorter] = useState({ column: 'common_symbol', direction: 'asc' });

    // Filter state
    const [searchInput, setSearchInput] = useState('');
    const [searchFilter, setSearchFilter] = useState('');

    // Modal state
    const [isDetailModalVisible, setIsDetailModalVisible] = useState(false);
    const [selectedSymbol, setSelectedSymbol] = useState(null);

    // Fetch data function (not wrapped in useCallback - defined inline like Assets.js)
    const fetchData = async () => {
        setLoading(true);
        setError(null);

        const apiParams = {
            limit: itemsPerPage,
            offset: (activePage - 1) * itemsPerPage,
        };

        if (sorter.column) {
            apiParams.sort_by = sorter.column;
            apiParams.sort_order = sorter.direction || 'asc';
        }

        if (searchFilter) {
            apiParams.common_symbol_like = searchFilter;
        }

        try {
            const data = await getCommonSymbols(apiParams);
            setItems(data.items || []);
            setTotalItems(data.total_items || 0);
        } catch (err) {
            console.error('Error fetching common symbols:', err);
            setError(err.message || 'Failed to fetch common symbols');
            setItems([]);
            setTotalItems(0);
        } finally {
            setLoading(false);
        }
    };

    // Debounce search input -> searchFilter
    useEffect(() => {
        const timer = setTimeout(() => {
            if (searchInput !== searchFilter) {
                setSearchFilter(searchInput);
                setActivePage(1); // Reset to first page on search
            }
        }, 500);
        return () => clearTimeout(timer);
    }, [searchInput, searchFilter]);

    // Fetch data when dependencies change (simple primitives only)
    useEffect(() => {
        fetchData();
    }, [activePage, itemsPerPage, sorter.column, sorter.direction, searchFilter]);

    // Calculate total pages
    const totalPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 1;

    // Event handlers
    const handleItemsPerPageChange = (event) => {
        const newSize = parseInt(event.target.value, 10);
        setActivePage(1);
        setItemsPerPage(newSize);
    };

    const handleSorterChange = (sorterData) => {
        if (!sorterData || sorterData.state === 0) {
            setSorter({ column: 'common_symbol', direction: 'asc' });
        } else {
            setSorter({ column: sorterData.column, direction: sorterData.state });
        }
        setActivePage(1);
    };

    const handleRowClick = (item) => {
        setSelectedSymbol(item.common_symbol);
        setIsDetailModalVisible(true);
    };

    // Column definitions
    const columns = [
        {
            key: 'common_symbol',
            label: 'Common Symbol',
            _props: { className: 'fw-semibold' },
        },
        {
            key: 'provider_count',
            label: 'Provider/Broker Count',
            _props: { className: 'text-center' },
            _style: { width: '25%' },
        },
        {
            key: 'actions',
            label: 'Actions',
            _style: { width: '15%' },
            filter: false,
            sorter: false,
            _props: { className: 'text-center' },
        }
    ];

    // Scoped column renderers
    const scopedColumns = {
        common_symbol: (item) => (
            <td>
                <strong 
                    className="text-primary" 
                    style={{ cursor: 'pointer' }}
                    onClick={() => handleRowClick(item)}
                >
                    {item.common_symbol}
                </strong>
            </td>
        ),
        provider_count: (item) => (
            <td className="text-center">
                <CBadge color="info" className="fs-6">
                    {item.provider_count}
                </CBadge>
            </td>
        ),
        actions: (item) => (
            <td className="text-center">
                <CButton
                    variant="ghost"
                    color="primary"
                    size="sm"
                    onClick={() => handleRowClick(item)}
                    title="View Details"
                >
                    <CIcon icon={cilZoom} />
                </CButton>
            </td>
        ),
    };

    return (
        <>
            <CRow>
                <CCol xs={12}>
                    <CCard>
                        <CCardHeader>
                            <CRow className="align-items-center">
                                <CCol xs={6} md={8} xl={9} className="text-start">
                                    <h5>Common Symbols</h5>
                                </CCol>
                                <CCol xs={6} md={4} xl={3} className="text-end">
                                    <small className="text-muted">
                                        {totalItems} total symbols
                                    </small>
                                </CCol>
                            </CRow>
                        </CCardHeader>
                        <CCardBody>
                            {/* Search Input */}
                            <CRow className="mb-3">
                                <CCol xs={12} md={6}>
                                    <CInputGroup>
                                        <CInputGroupText>
                                            <CIcon icon={cilSearch} />
                                        </CInputGroupText>
                                        <CFormInput
                                            type="text"
                                            placeholder="Search common symbols..."
                                            value={searchInput}
                                            onChange={(e) => setSearchInput(e.target.value)}
                                        />
                                    </CInputGroup>
                                </CCol>
                            </CRow>

                            {error && <CAlert color="danger">{error}</CAlert>}

                            <CSmartTable
                                items={items}
                                columns={columns}
                                loading={loading}
                                itemsPerPage={itemsPerPage}
                                pagination={false}
                                columnSorter={{ external: true, resetable: true }}
                                onSorterChange={handleSorterChange}
                                sorterValue={{ column: sorter.column, state: sorter.direction }}
                                clickableRows
                                onRowClick={handleRowClick}
                                scopedColumns={scopedColumns}
                                tableProps={{
                                    striped: true,
                                    hover: true,
                                    responsive: true,
                                }}
                            />

                            {/* Pagination Controls */}
                            <CRow className="mt-3 align-items-center justify-content-center">
                                {totalPages > 1 && (
                                    <CCol xs="auto">
                                        <CSmartPagination
                                            activePage={activePage}
                                            pages={totalPages}
                                            onActivePageChange={setActivePage}
                                        />
                                    </CCol>
                                )}
                                <CCol xs="auto" className="me-2">
                                    <label htmlFor="itemsPerPageSelect" className="col-form-label">
                                        Items per page:
                                    </label>
                                </CCol>
                                <CCol xs="auto">
                                    <CFormSelect
                                        id="itemsPerPageSelect"
                                        value={itemsPerPage}
                                        onChange={handleItemsPerPageChange}
                                        options={[
                                            { label: '10', value: 10 },
                                            { label: '25', value: 25 },
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

            {/* Detail Modal */}
            <CommonSymbolDetailModal
                visible={isDetailModalVisible}
                onClose={() => {
                    setIsDetailModalVisible(false);
                    setSelectedSymbol(null);
                }}
                commonSymbol={selectedSymbol}
            />
        </>
    );
};

export default CommonSymbolsTab;
