// Basic, empty component for now
import { React, useState, useEffect, useRef } from 'react';
import { 
    CCard, 
    CCardBody, 
    CCardHeader, 
    CCol, 
    CRow,
    CSmartTable,
    CButton,
    CBadge,
    CToaster,
    CToast,
    CToastHeader,
    CToastBody,
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

// API Imports
import { 
    getAssetMappings,
    deleteAssetMapping,
} from '../services/registry_api';

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
    try {
      const data = await getAssetMappings();
      console.log('Fetched Mappings:', data); // Debugging log
      setMappings(data);
    } catch (err) {
      setError(err.message || 'Failed to fetch mappings');
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    fetchMappings();
  }, []);

  // Define columns for CSmartTable
  const columns = [
    { key: 'common_symbol', label: "Common Symbol", _props: { className: 'fw-semibold' } },
    { key: 'class_symbol', label: "Class Symbol" },
    { key: 'class_name', label: "Class Name" },
    { key: 'class_type', label: "Class Type" },
    {
      key: 'is_active',
      label: 'Active',
      _style: { width: '10%' },
      sorter: false, // Sorting boolean might be tricky, handle server-side if needed
      filter: false, // Filtering boolean might be tricky
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
    const confirmMessage = `Are you sure you want to delete the mapping for "${item.common_symbol}" (${item.class_name}/${item.class_symbol})?`;
    if (!window.confirm(confirmMessage)) {
      return;
    }

    try {
      await deleteAssetMapping(item.class_name, item.class_type, item.class_symbol);
      // Refresh list after successful deletion
      // Log out arguments to the deleteAssetMapping function
      console.log('Delete arguments:', item.class_name, item.class_type, item.class_symbol);
      await fetchMappings();
    } catch (err) {
      setError(err.message || 'Failed to delete mapping');
      alert(`Error deleting mapping: ${err.message || 'Unknown error'}`);
    }
  };

  const handleEdit = (item) => {
    setCurrentMapping(item);
    setIsEditModalVisible(true);
  };
  const handleAdd = () => {
    // setCurrentItem(null); // For a new item
    setIsAddModalVisible(true);
    console.log('Add new mapping');
  }

  return (
    <>
        <CToaster ref={toasterRef} push={toastToShow} placement="top-end" />
        <CRow>
        <CCol xs={12}>
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
                <CSmartTable
                loading={loading}
                activePage={1}
                cleaner
                clickableRows
                columns={columns}
                columnFilter
                columnSorter
                items={mappings}
                itemsPerPageSelect
                itemsPerPage={10}
                pagination
                scopedColumns={{
                    class_type: (item) => (
                        <td className="text-center">
                            {/* Class Type Badge */}
                            <CBadge color={getClassBadge(item.class_type)}>
                                {item.class_type.charAt(0).toUpperCase() + item.class_type.slice(1)}
                            </CBadge>
                        </td>
                    ),
                    is_active: (item) => (
                        <td className="text-center">
                            {/* Yes/No Badge for Boolean */}
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
            </CCardBody>
            </CCard>
        </CCol>
        </CRow>
        <MappingAddModal
            visible={isAddModalVisible}
            onClose={() => setIsAddModalVisible(false)}
            onSuccess={() => fetchMappings()}
            pushToast={pushToast}
        />
        <MappingEditModal
            visible={isEditModalVisible}
            onClose={() => {
              setIsEditModalVisible(false);
              setCurrentMapping(null);
            }}
            onSuccess={() => fetchMappings()}
            mapping={currentMapping}
        />
        <SuggestMappingsModal
            visible={isSuggestModalVisible}
            onClose={() => setIsSuggestModalVisible(false)}
            onSuccess={() => fetchMappings()}
            pushToast={pushToast}
        />
    </>
  );
}

export default Mappings;