/**
 * ColumnSelectorModal - Modal for selecting which columns to display in the Assets table.
 */
import React from 'react';
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CFormCheck,
  CRow,
  CCol
} from '@coreui/react-pro';
import { ASSET_COLUMNS, getDefaultVisibleColumns, getColumnKeys } from '../../configs/assetColumns';

const ColumnSelectorModal = ({ visible, onClose, visibleColumns, setVisibleColumns }) => {
  const allColumnKeys = getColumnKeys();

  const handleToggleColumn = (columnKey) => {
    setVisibleColumns((prev) => {
      if (prev.includes(columnKey)) {
        return prev.filter((key) => key !== columnKey);
      } else {
        return [...prev, columnKey];
      }
    });
  };

  const handleSelectAll = () => {
    setVisibleColumns(allColumnKeys);
  };

  const handleDeselectAll = () => {
    setVisibleColumns([]);
  };

  const handleResetToDefaults = () => {
    setVisibleColumns(getDefaultVisibleColumns());
  };

  return (
    <CModal visible={visible} onClose={onClose} size="lg">
      <CModalHeader onClose={onClose}>
        <CModalTitle>Select Columns</CModalTitle>
      </CModalHeader>
      <CModalBody>
        <CRow className="mb-3">
          <CCol>
            <CButton color="primary" size="sm" onClick={handleSelectAll} className="me-2">
              Select All
            </CButton>
            <CButton color="secondary" size="sm" onClick={handleDeselectAll} className="me-2">
              Deselect All
            </CButton>
            <CButton color="info" size="sm" onClick={handleResetToDefaults}>
              Reset to Defaults
            </CButton>
          </CCol>
        </CRow>
        <CRow>
          {allColumnKeys.map((columnKey) => {
            const columnConfig = ASSET_COLUMNS[columnKey];
            return (
              <CCol xs={6} md={4} lg={3} key={columnKey} className="mb-2">
                <CFormCheck
                  id={`col-check-${columnKey}`}
                  label={columnConfig.label}
                  checked={visibleColumns.includes(columnKey)}
                  onChange={() => handleToggleColumn(columnKey)}
                />
              </CCol>
            );
          })}
        </CRow>
      </CModalBody>
      <CModalFooter>
        <CButton color="primary" onClick={onClose}>
          Done
        </CButton>
      </CModalFooter>
    </CModal>
  );
};

export default ColumnSelectorModal;
