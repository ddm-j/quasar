import { React, useState, useEffect, useRef, useMemo } from 'react';
import {
    CCard,
    CCardBody,
    CCardHeader,
    CCol,
    CRow,
    CSmartTable,
    CSmartPagination,
    CFormSelect,
    CAlert,
    CButton,
    CBadge,
    CToaster,
    CToast,
    CToastHeader,
    CToastBody,
    CNav,
    CNavItem,
    CNavLink,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react'
import { 
    cilTrash,
    cilPencil,
} from '@coreui/icons'

// Add Mapping Modal
import MappingAddModal from './MappingAddModal';
// Edit Mapping Modal
import MappingEditModal from './MappingEditModal';
// Suggest Mappings Modal
import SuggestMappingsModal from './SuggestMappingsModal';
// Common Symbols Tab
import CommonSymbolsTab from './CommonSymbolsTab';

// API Imports
import {
    getAssetMappings,
    deleteAssetMapping,
    getRegisteredClasses,
} from '../services/registry_api';

// Asset class options for filtering (matches backend enums.py)
const ASSET_CLASS_OPTIONS = [
  { label: 'All Asset Classes', value: '' },
  { label: 'Equity', value: 'equity' },
  { label: 'Fund', value: 'fund' },
  { label: 'ETF', value: 'etf' },
  { label: 'Bond', value: 'bond' },
  { label: 'Crypto', value: 'crypto' },
  { label: 'Currency', value: 'currency' },
  { label: 'Future', value: 'future' },
  { label: 'Option', value: 'option' },
  { label: 'Index', value: 'index' },
  { label: 'Commodity', value: 'commodity' },
];

const Mappings = () => {
  // State
  const [mappings, setMappings] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isAddModalVisible, setIsAddModalVisible] = useState(false);
  const [isEditModalVisible, setIsEditModalVisible] = useState(false);
  const [isSuggestModalVisible, setIsSuggestModalVisible] = useState(false);
  const [currentMapping, setCurrentMapping] = useState(null);
  const [toastToShow, setToastToShow] = useState(null);
  const toasterRef = useRef(null);

  // Tab state
  const [activeTab, setActiveTab] = useState('mappings');

  // Filter states for re-map functionality
  const [assetClassFilter, setAssetClassFilter] = useState('');
  const [providerFilter, setProviderFilter] = useState('');
  const [providerOptions, setProviderOptions] = useState([]);
  const [isLoadingProviders, setIsLoadingProviders] = useState(false);

  // Pagination state
  const [totalItems, setTotalItems] = useState(0);
  const [activePage, setActivePage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(10);
  const [sorter, setSorter] = useState({});
  const [columnFilter, setColumnFilter] = useState({});
  const [liveTextInputFilters, setLiveTextInputFilters] = useState({});

  // Get text filter keys for mappings (columns that support LIKE filtering)
  const textInputFilterKeys = useMemo(() => ['common_symbol', 'class_symbol', 'class_name'], []);

  const pushToast = ({ title, body, color = 'danger', icon = null }) => {
    const toast = (
      <CToast autohide={false} color={color}>
        <CToastHeader closeButton>
          {icon && <CIcon icon={icon} className="me-2" />}
          <strong className="me-auto">{title}</strong>
        </CToastHeader>
        <CToastBody>{body}</CToastBody>
      </CToast>
    );
    setToastToShow(toast);
  };

  // Fetch Mappings
  const fetchMappings = async () => {
    setLoading(true);
    setError(null);

    const apiParams = {
      limit: itemsPerPage,
      offset: (activePage - 1) * itemsPerPage,
    };

    // Add sorting parameters
    if (sorter && sorter.column) {
      apiParams.sort_by = sorter.column;
      apiParams.sort_order = sorter.direction || 'asc';
    }

    // Add filter parameters
    for (const key in columnFilter) {
      if (columnFilter[key]) {
        if (textInputFilterKeys.includes(key)) {
          apiParams[`${key}_like`] = columnFilter[key];
        } else {
          apiParams[key] = columnFilter[key];
        }
      }
    }

    // Add asset class filter if set
    if (assetClassFilter) {
      apiParams.asset_class = assetClassFilter;
    }

    // Add provider filter if set
    if (providerFilter) {
      apiParams.class_name = providerFilter;
    }

    try {
      const data = await getAssetMappings(apiParams);
      setMappings(data.items || []);
      setTotalItems(data.total_items || 0);
    } catch (err) {
      setError(err.message || 'Failed to fetch mappings');
      setMappings([]);
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
    }, 300); // 300ms debounce

    return () => {
      clearTimeout(handler);
    };
  }, [liveTextInputFilters, textInputFilterKeys]);

  // Fetch provider options on mount
  useEffect(() => {
    const fetchProviders = async () => {
      setIsLoadingProviders(true);
      try {
        const data = await getRegisteredClasses();
        // Transform to dropdown options with "All Providers" default
        const options = [
          { label: 'All Providers', value: '' },
          ...(data || []).map(cls => ({
            label: `${cls.class_name} (${cls.class_type})`,
            value: cls.class_name,
            class_type: cls.class_type,
          })),
        ];
        setProviderOptions(options);
      } catch (err) {
        console.error('Error fetching providers:', err);
        // Still set default option on error
        setProviderOptions([{ label: 'All Providers', value: '' }]);
      } finally {
        setIsLoadingProviders(false);
      }
    };
    fetchProviders();
  }, []);

  useEffect(() => {
    if (activeTab === 'mappings') {
      setError(null);
      fetchMappings();
    }
  }, [activeTab, activePage, itemsPerPage, sorter, columnFilter, assetClassFilter, providerFilter]);

  // Define columns for CSmartTable
  const columns = [
    { key: 'common_symbol', label: 'Common Symbol', _props: { className: 'fw-semibold' }, sorter: true },
    { key: 'class_symbol', label: 'Class Symbol', sorter: true },
    { key: 'class_name', label: 'Class Name', sorter: true },
    { key: 'class_type', label: 'Class Type', sorter: true },
    {
      key: 'is_active',
      label: 'Active',
      _style: { width: '10%' },
      sorter: true,  // Enable server-side sorting
      filter: false, // Keep filter disabled (boolean handled differently)
      _props: { className: 'text-center' }
    },
    {
      key: 'actions',
      label: 'Actions',
      _style: { width: '15%' },
      filter: false,
      sorter: false,
      _props: { className: 'text-center' },
    }
  ]


  const getClassBadge = (class_type) => {
    switch (class_type) {
      case 'provider': {
        return 'primary'
      }
      case 'broker': {
        return 'secondary'
      }
      default: {
        return 'primary'
      }
    }
  }
  const getActiveBadge = (is_active) => {
    return is_active ? 'success' : 'danger';
  }

  const handleDelete = async (item) => {
    // Confirm deletion
    const confirmMessage = `Are you sure you want to delete the mapping for ${item.common_symbol} (${item.class_name}/${item.class_type})?`;
    if (!window.confirm(confirmMessage)) {
      return;
    }

    try {
      await deleteAssetMapping(item.class_name, item.class_type, item.class_symbol);
      // Refresh list after successful deletion
      await fetchMappings();
    } catch (err) {
      setError(err.message || 'Failed to delete mapping');
      alert(`Error deleting mapping: ${err.message}`);
    }
  };

  const handleEdit = (item) => {
    setCurrentMapping(item);
    setIsEditModalVisible(true);
  };
  const handleAdd = () => {
    setIsAddModalVisible(true);
  }

  const handleItemsPerPageChange = (event) => {
    const newSize = parseInt(event.target.value, 10);
    setActivePage(1); // Reset to first page when changing page size
    setItemsPerPage(newSize);
  };

  const handleAssetClassFilterChange = (event) => {
    setActivePage(1); // Reset to first page when changing filter
    setAssetClassFilter(event.target.value);
  };

  const handleProviderFilterChange = (event) => {
    setActivePage(1); // Reset to first page when changing filter
    setProviderFilter(event.target.value);
  };

  const handleSorterChange = (sorterData) => {
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
      setActivePage(1); // Reset to first page when sorting changes
    } else {
      // State is 0 (cleared) or invalid
      setSorter({});
      setActivePage(1);
    }
  };

  const calculatedPages = totalItems > 0 ? Math.ceil(totalItems / itemsPerPage) : 1;

  return (
    <>
      <CToaster ref={toasterRef} push={toastToShow} placement="top-end" />
      <CRow>
        <CCol xs={12}>
          {/* Tab Navigation */}
          <CNav variant="tabs" className="mb-3">
            <CNavItem>
              <CNavLink
                active={activeTab === 'mappings'}
                onClick={() => setActiveTab('mappings')}
                style={{ cursor: 'pointer' }}
              >
                Mappings
              </CNavLink>
            </CNavItem>
            <CNavItem>
              <CNavLink
                active={activeTab === 'common-symbols'}
                onClick={() => setActiveTab('common-symbols')}
                style={{ cursor: 'pointer' }}
              >
                Common Symbols
              </CNavLink>
            </CNavItem>
          </CNav>

          {/* Tab Content - Use conditional rendering to prevent mounting inactive tabs */}
          {activeTab === 'mappings' && (
            <CCard>
              <CCardHeader>
                <CRow className="align-items-center">
                  <CCol xs={6} md={8} xl={9} className="text-start">
                    <h5>Mappings</h5>
                  </CCol>
                  <CCol xs={6} md={4} xl={3} className="d-flex justify-content-end gap-2">
                    <CButton color="primary" onClick={handleAdd}>
                      Add Mapping
                    </CButton>
                    <CButton color="success" onClick={() => setIsSuggestModalVisible(true)}>
                      Suggest Mappings
                    </CButton>
                  </CCol>
                </CRow>
              </CCardHeader>
              <CCardBody>
                {/* Filter Row */}
                <CRow className="mb-3 align-items-center">
                  <CCol xs="auto">
                    <label htmlFor="assetClassFilter" className="col-form-label">Asset Class:</label>
                  </CCol>
                  <CCol xs="auto">
                    <CFormSelect
                      id="assetClassFilter"
                      value={assetClassFilter}
                      onChange={handleAssetClassFilterChange}
                      options={ASSET_CLASS_OPTIONS}
                      style={{ minWidth: '180px' }}
                    />
                  </CCol>
                  <CCol xs="auto">
                    <label htmlFor="providerFilter" className="col-form-label">Provider:</label>
                  </CCol>
                  <CCol xs="auto">
                    <CFormSelect
                      id="providerFilter"
                      value={providerFilter}
                      onChange={handleProviderFilterChange}
                      options={providerOptions}
                      disabled={isLoadingProviders}
                      style={{ minWidth: '200px' }}
                    />
                  </CCol>
                </CRow>
                <CSmartTable
                  loading={loading}
                  items={mappings}
                  columns={columns}
                  pagination={false}  // Disable built-in pagination
                  columnSorter={{ external: true, resetable: true }}  // External sorting
                  onSorterChange={handleSorterChange}  // Custom sorter handler
                  columnFilter  // Enable filtering
                  columnFilterValue={liveTextInputFilters}
                  onColumnFilterChange={setLiveTextInputFilters}
                  scopedColumns={{
                    class_type: (item) => (
                      <td className="text-center">
                        <CBadge color={getClassBadge(item.class_type)}>
                          {item.class_type.charAt(0).toUpperCase() + item.class_type.slice(1)}
                        </CBadge>
                      </td>
                    ),
                    is_active: (item) => (
                      <td className="text-center">
                        <CBadge color={getActiveBadge(item.is_active)}>
                          {item.is_active ? 'Yes' : 'No'}
                        </CBadge>
                      </td>
                    ),
                    actions: (item) => (
                      <td className="text-center">
                        <CButton
                          variant="ghost"
                          color="body"
                          size="sm"
                          onClick={() => handleEdit(item)}
                          className="p-1 me-2"
                          title="Edit Mapping"
                        >
                          <CIcon icon={cilPencil} className="sm" />
                        </CButton>
                        <CButton
                          variant="ghost"
                          color="danger"
                          size="sm"
                          onClick={() => handleDelete(item)}
                          className="p-1 me-2"
                          title="Delete Mapping"
                        >
                          <CIcon icon={cilTrash} className="sm" />
                        </CButton>
                      </td>
                    ),
                  }}
                  tableProps={{
                    striped: true,
                    hover: true,
                    responsive: true,
                  }}
                />

                {error && <CAlert color="danger" className="mb-3">{error}</CAlert>}

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
                        { label: '25', value: 25 },
                        { label: '50', value: 50 },
                      ]}
                    />
                  </CCol>
                </CRow>
              </CCardBody>
            </CCard>
          )}

          {activeTab === 'common-symbols' && <CommonSymbolsTab />}
        </CCol>
      </CRow>
      <MappingAddModal
        visible={isAddModalVisible}
        onClose={() => setIsAddModalVisible(false)}
        onSuccess={() => {
          setActivePage(1); // Reset to first page after add
          fetchMappings();
        }}
        pushToast={pushToast}
      />
      <MappingEditModal
        visible={isEditModalVisible}
        onClose={() => {
          setIsEditModalVisible(false);
          setCurrentMapping(null);
        }}
        onSuccess={() => {
          setActivePage(1); // Reset to first page after edit
          fetchMappings();
        }}
        mapping={currentMapping}
      />
      <SuggestMappingsModal
        visible={isSuggestModalVisible}
        onClose={() => setIsSuggestModalVisible(false)}
        onSuccess={() => {
          setActivePage(1); // Reset to first page after suggest
          fetchMappings();
        }}
        pushToast={pushToast}
      />
    </>
  );
}

export default Mappings;