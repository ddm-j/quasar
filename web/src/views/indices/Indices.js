import React, { useState, useEffect, useRef } from 'react';
import {
  CButton,
  CCard,
  CCardBody,
  CCardHeader,
  CCol,
  CRow,
  CSmartTable,
  CAlert,
  CBadge,
  CToaster,
  CToast,
  CToastHeader,
  CToastBody,
  CSpinner,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilCheckCircle, cilWarning, cilPlus } from '@coreui/icons';

import IndexDetailModal from './IndexDetailModal';
import CreateIndexModal from './CreateIndexModal';
import { getIndices } from '../services/registry_api';
import { formatDate } from '../../utils/formatting';

const Indices = () => {
  // Data state
  const [indices, setIndices] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Modal state
  const [selectedIndex, setSelectedIndex] = useState(null);
  const [isDetailModalVisible, setIsDetailModalVisible] = useState(false);
  const [isCreateModalVisible, setIsCreateModalVisible] = useState(false);

  // Toast state
  const [toastToShow, setToastToShow] = useState(null);
  const toasterRef = useRef(null);

  const pushToast = ({ title, body, color = 'info', icon = null }) => {
    const toast = (
      <CToast autohide={true} delay={5000} color={color}>
        <CToastHeader closeButton>
          {icon && <CIcon icon={icon} className="me-2" />}
          <strong className="me-auto">{title}</strong>
        </CToastHeader>
        <CToastBody>{body}</CToastBody>
      </CToast>
    );
    setToastToShow(toast);
  };

  // Fetch all indices
  const fetchIndices = async () => {
    setLoading(true);
    setError(null);

    try {
      const data = await getIndices({ limit: 100 });
      setIndices(data.items || []);
    } catch (err) {
      setError(err.message || 'Failed to fetch indices');
      setIndices([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchIndices();
  }, []);

  // Handle row click to open detail modal
  const handleRowClick = (item) => {
    setSelectedIndex(item);
    setIsDetailModalVisible(true);
  };

  // Handle modal close
  const handleModalClose = () => {
    setIsDetailModalVisible(false);
    setSelectedIndex(null);
  };

  // Handle refresh callback from modal
  const handleRefresh = () => {
    fetchIndices();
    pushToast({
      title: 'Index Refreshed',
      body: 'Index data has been updated successfully.',
      color: 'success',
      icon: cilCheckCircle,
    });
  };

  // Get badge color for index type
  const getTypeBadgeColor = (indexType) => {
    return indexType === 'IndexProvider' ? 'primary' : 'success';
  };

  // Format sync frequency for display
  const formatSyncFrequency = (item) => {
    // Only IndexProviders have sync frequency
    if (item.index_type !== 'IndexProvider') {
      return 'N/A';
    }
    const freq = item.preferences?.scheduling?.sync_frequency;
    if (!freq) {
      return 'Weekly'; // Default per spec
    }
    switch (freq) {
      case '1d':
        return 'Daily';
      case '1w':
        return 'Weekly';
      case '1M':
        return 'Monthly';
      default:
        return freq;
    }
  };

  // Table columns - use width: '1%' + whiteSpace: 'nowrap' for auto-fit
  const columns = [
    {
      key: 'class_name',
      label: 'Name',
      _style: { width: '1%', whiteSpace: 'nowrap' },
      _props: { className: 'fw-semibold' },
      sorter: true,
    },
    {
      key: 'index_type',
      label: 'Type',
      _style: { width: '1%', whiteSpace: 'nowrap' },
      sorter: true,
      filter: false,
    },
    {
      key: 'sync_frequency',
      label: 'Sync Frequency',
      _style: { width: '1%', whiteSpace: 'nowrap' },
      sorter: true,
      filter: false,
    },
    {
      key: 'current_member_count',
      label: 'Members',
      _style: { width: '1%', whiteSpace: 'nowrap' },
      sorter: true,
      filter: false,
      _props: { className: 'text-center' },
    },
    {
      key: 'uploaded_at',
      label: 'Last Updated',
      _style: { width: '1%', whiteSpace: 'nowrap' },
      sorter: true,
      filter: false,
    },
  ];

  return (
    <>
      <CRow>
        <CCol xs={12}>
          <CCard>
            <CCardHeader>
              <CRow className="align-items-center">
                <CCol>
                  <h5 className="mb-0">Indices</h5>
                </CCol>
                <CCol xs="auto">
                  <CButton
                    color="primary"
                    onClick={() => setIsCreateModalVisible(true)}
                  >
                    <CIcon icon={cilPlus} className="me-1" />
                    Create Index
                  </CButton>
                </CCol>
              </CRow>
            </CCardHeader>
            <CCardBody>
              {error && (
                <CAlert color="danger" className="d-flex align-items-center">
                  <CIcon icon={cilWarning} className="me-2" />
                  {error}
                </CAlert>
              )}

              {loading ? (
                <div className="text-center py-5">
                  <CSpinner color="primary" />
                  <p className="mt-2 text-muted">Loading indices...</p>
                </div>
              ) : indices.length === 0 && !error ? (
                <CAlert color="info">
                  No indices found. Index providers will appear here once registered.
                </CAlert>
              ) : (
                <CSmartTable
                  items={indices}
                  columns={columns}
                  columnFilter
                  columnSorter
                  pagination
                  itemsPerPage={10}
                  itemsPerPageSelect
                  clickableRows
                  onRowClick={handleRowClick}
                  tableProps={{
                    striped: true,
                    hover: true,
                    responsive: true,
                  }}
                  scopedColumns={{
                    class_name: (item) => (
                      <td className="fw-semibold" style={{ cursor: 'pointer' }}>
                        {item.class_name}
                      </td>
                    ),
                    index_type: (item) => (
                      <td>
                        <CBadge color={getTypeBadgeColor(item.index_type)}>
                          {item.index_type}
                        </CBadge>
                      </td>
                    ),
                    sync_frequency: (item) => (
                      <td>{formatSyncFrequency(item)}</td>
                    ),
                    current_member_count: (item) => (
                      <td className="text-center">
                        {item.current_member_count ?? 0}
                      </td>
                    ),
                    uploaded_at: (item) => (
                      <td>{formatDate(item.uploaded_at)}</td>
                    ),
                  }}
                  noItemsLabel="No indices found"
                />
              )}
            </CCardBody>
          </CCard>
        </CCol>
      </CRow>

      <IndexDetailModal
        visible={isDetailModalVisible}
        onClose={handleModalClose}
        indexItem={selectedIndex}
        onRefresh={handleRefresh}
        pushToast={pushToast}
      />

      <CreateIndexModal
        visible={isCreateModalVisible}
        onClose={() => setIsCreateModalVisible(false)}
        onSuccess={fetchIndices}
        pushToast={pushToast}
      />

      <CToaster ref={toasterRef} push={toastToShow} placement="top-end" />
    </>
  );
};

export default Indices;
