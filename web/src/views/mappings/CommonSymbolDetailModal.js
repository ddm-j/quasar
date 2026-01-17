import { React, useState, useEffect } from 'react';
import {
    CModal,
    CModalHeader,
    CModalTitle,
    CModalBody,
    CModalFooter,
    CButton,
    CTable,
    CTableHead,
    CTableRow,
    CTableHeaderCell,
    CTableBody,
    CTableDataCell,
    CBadge,
    CSpinner,
    CAlert,
} from '@coreui/react-pro';

// API Imports
import { getAssetMappingsForSymbol } from '../services/registry_api';

// Component Imports
import CommonSymbolRenameModal from './CommonSymbolRenameModal';

const CommonSymbolDetailModal = ({ visible, onClose, onRenameSuccess, commonSymbol }) => {
  const [mappings, setMappings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isRenameModalVisible, setIsRenameModalVisible] = useState(false);

  const handleRenameSuccess = (result) => {
    setIsRenameModalVisible(false);
    if (onRenameSuccess) {
      onRenameSuccess(result);
    }
    onClose();
  };

  useEffect(() => {
    const fetchMappingsForSymbol = async () => {
      if (!commonSymbol) return;

      setLoading(true);
      setError(null);
      try {
        // Use the new efficient API endpoint that filters by common symbol server-side
        const mappings = await getAssetMappingsForSymbol(commonSymbol);
        setMappings(mappings);
      } catch (err) {
        console.error('Error fetching mappings for symbol:', err);
        setError(err.message || 'Failed to fetch mappings for this symbol');
      } finally {
        setLoading(false);
      }
    };

    if (visible && commonSymbol) {
      fetchMappingsForSymbol();
    }
  }, [visible, commonSymbol]);

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
  };

  const getActiveBadge = (is_active) => {
    return is_active ? 'success' : 'danger';
  };

  return (
    <CModal
      visible={visible}
      onClose={onClose}
      backdrop="static"
      fullscreen="lg"
      size="xl"
      className="common-symbol-detail-modal"
      scrollable
    >
      <CModalHeader onClose={onClose}>
        <CModalTitle>
          Mappings for Common Symbol: <strong>{commonSymbol}</strong>
        </CModalTitle>
      </CModalHeader>

      <CModalBody>
        {loading && (
          <div className="text-center py-4">
            <CSpinner color="primary" />
            <div className="mt-2">Loading mappings...</div>
          </div>
        )}

        {error && (
          <CAlert color="danger">
            <strong>Error:</strong> {error}
          </CAlert>
        )}

        {!loading && !error && mappings.length === 0 && (
          <CAlert color="info">
            No mappings found for common symbol "{commonSymbol}".
          </CAlert>
        )}

        {!loading && !error && mappings.length > 0 && (
          <>
            <div className="mb-3">
              <small className="text-muted">
                Found {mappings.length} mapping{mappings.length !== 1 ? 's' : ''} for this symbol
              </small>
            </div>

            <CTable striped hover responsive>
              <CTableHead>
                <CTableRow>
                  <CTableHeaderCell>Provider/Broker</CTableHeaderCell>
                  <CTableHeaderCell>Class Symbol</CTableHeaderCell>
                  <CTableHeaderCell>Primary ID</CTableHeaderCell>
                  <CTableHeaderCell>Asset Class</CTableHeaderCell>
                  <CTableHeaderCell>Status</CTableHeaderCell>
                </CTableRow>
              </CTableHead>
              <CTableBody>
                {mappings.map((mapping, index) => (
                  <CTableRow key={`${mapping.class_name}-${mapping.class_symbol}-${index}`}>
                    <CTableDataCell>
                      <div className="d-flex align-items-center">
                        <CBadge color={getClassBadge(mapping.class_type)} className="me-2">
                          {mapping.class_type.charAt(0).toUpperCase() + mapping.class_type.slice(1)}
                        </CBadge>
                        <strong>{mapping.class_name}</strong>
                      </div>
                    </CTableDataCell>
                    <CTableDataCell>
                      <code>{mapping.class_symbol}</code>
                    </CTableDataCell>
                    <CTableDataCell>
                      {mapping.primary_id || '-'}
                    </CTableDataCell>
                    <CTableDataCell>
                      {mapping.asset_class || '-'}
                    </CTableDataCell>
                    <CTableDataCell className="text-center">
                      <CBadge color={getActiveBadge(mapping.is_active)}>
                        {mapping.is_active ? 'Active' : 'Inactive'}
                      </CBadge>
                    </CTableDataCell>
                  </CTableRow>
                ))}
              </CTableBody>
            </CTable>
          </>
        )}
      </CModalBody>

      <CModalFooter>
        <CButton color="primary" onClick={() => setIsRenameModalVisible(true)}>
          Rename
        </CButton>
        <CButton color="secondary" onClick={onClose}>
          Close
        </CButton>
      </CModalFooter>

      <CommonSymbolRenameModal
        visible={isRenameModalVisible}
        onClose={() => setIsRenameModalVisible(false)}
        onSuccess={handleRenameSuccess}
        commonSymbol={commonSymbol}
      />
    </CModal>
  );
};

export default CommonSymbolDetailModal;