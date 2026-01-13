import React, { useCallback } from 'react';
import {
  CTable,
  CTableHead,
  CTableRow,
  CTableHeaderCell,
  CTableBody,
  CTableDataCell,
  CButton,
  CFormInput,
  CAlert,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilTrash, cilPlus, cilWarning } from '@coreui/icons';
import AsyncSelect from 'react-select/async';
import { getCommonSymbols } from '../services/registry_api';
import { formatWeightForInput } from '../../utils/formatting';

// Simple counter for generating unique row IDs
let rowIdCounter = 0;
const generateId = () => `new_${++rowIdCounter}`;

const EditableMembersTable = ({ members, onChange, disabled }) => {
  // Load common symbol options for AsyncSelect
  const loadSymbolOptions = useCallback(async (inputValue, callback) => {
    if (inputValue.length < 1) {
      callback([]);
      return;
    }

    try {
      const data = await getCommonSymbols({
        common_symbol_like: inputValue,
        limit: 50,
      });

      const options = (data.items || []).map((item) => ({
        value: item.common_symbol,
        label: item.common_symbol,
      }));

      callback(options);
    } catch (error) {
      console.error('Error loading symbol options:', error);
      callback([]);
    }
  }, []);

  // Handle symbol change for a row
  const handleSymbolChange = (rowId, selectedOption) => {
    const updated = members.map((m) =>
      m.id === rowId
        ? {
            ...m,
            common_symbol: selectedOption?.value || null,
            selectOption: selectedOption,
          }
        : m
    );
    onChange(updated);
  };

  // Handle weight change for a row (input is percentage, store as decimal)
  const handleWeightChange = (rowId, value) => {
    const numValue = value === '' ? null : parseFloat(value) / 100;
    const updated = members.map((m) =>
      m.id === rowId ? { ...m, weight: numValue } : m
    );
    onChange(updated);
  };

  // Handle delete row
  const handleDeleteRow = (rowId) => {
    const updated = members.filter((m) => m.id !== rowId);
    onChange(updated);
  };

  // Handle add new row
  const handleAddRow = () => {
    const newRow = {
      id: generateId(),
      common_symbol: null,
      weight: null,
      isNew: true,
      selectOption: null,
    };
    onChange([...members, newRow]);
  };

  // Calculate validation warnings
  const getValidationWarnings = () => {
    const warnings = [];

    // Check for duplicate symbols
    const symbols = members
      .map((m) => m.common_symbol)
      .filter((s) => s !== null);
    const duplicates = symbols.filter(
      (s, i) => symbols.indexOf(s) !== i
    );
    if (duplicates.length > 0) {
      warnings.push(`Duplicate symbols: ${[...new Set(duplicates)].join(', ')}`);
    }

    // Check for empty symbols in new rows
    const emptyNewRows = members.filter((m) => m.isNew && !m.common_symbol);
    if (emptyNewRows.length > 0) {
      warnings.push(`${emptyNewRows.length} row(s) have no symbol selected`);
    }

    return warnings;
  };

  const warnings = getValidationWarnings();

  return (
    <div>
      {warnings.length > 0 && (
        <CAlert color="warning" className="py-2">
          <CIcon icon={cilWarning} className="me-1" />
          {warnings.map((w, i) => (
            <div key={i}>{w}</div>
          ))}
        </CAlert>
      )}

      <CTable striped hover responsive>
        <CTableHead>
          <CTableRow>
            <CTableHeaderCell style={{ width: '50%' }}>Symbol</CTableHeaderCell>
            <CTableHeaderCell style={{ width: '35%' }} className="text-end">
              Weight (%)
            </CTableHeaderCell>
            <CTableHeaderCell style={{ width: '15%' }} className="text-center">
              Actions
            </CTableHeaderCell>
          </CTableRow>
        </CTableHead>
        <CTableBody>
          {members.map((member) => (
            <CTableRow key={member.id}>
              <CTableDataCell>
                {member.isNew ? (
                  <AsyncSelect
                    cacheOptions
                    loadOptions={loadSymbolOptions}
                    defaultOptions
                    value={member.selectOption}
                    onChange={(opt) => handleSymbolChange(member.id, opt)}
                    placeholder="Search common symbol..."
                    isDisabled={disabled}
                    isClearable
                    noOptionsMessage={({ inputValue }) =>
                      !inputValue ? 'Type to search...' : 'No symbols found'
                    }
                    loadingMessage={() => 'Loading...'}
                    menuPortalTarget={document.body}
                    menuPosition="fixed"
                    styles={{
                      control: (base) => ({
                        ...base,
                        minHeight: '38px',
                      }),
                      menuPortal: (base) => ({
                        ...base,
                        zIndex: 9999,
                      }),
                    }}
                  />
                ) : (
                  <span className="fw-semibold">{member.common_symbol}</span>
                )}
              </CTableDataCell>
              <CTableDataCell className="text-end">
                <CFormInput
                  type="number"
                  min="0"
                  max="100"
                  step="0.1"
                  value={formatWeightForInput(member.weight)}
                  onChange={(e) => handleWeightChange(member.id, e.target.value)}
                  placeholder="—"
                  disabled={disabled}
                  style={{ width: '100px', marginLeft: 'auto', textAlign: 'right' }}
                />
              </CTableDataCell>
              <CTableDataCell className="text-center">
                <CButton
                  color="danger"
                  variant="ghost"
                  size="sm"
                  onClick={() => handleDeleteRow(member.id)}
                  disabled={disabled}
                  title="Remove member"
                >
                  <CIcon icon={cilTrash} />
                </CButton>
              </CTableDataCell>
            </CTableRow>
          ))}
          {members.length === 0 && (
            <CTableRow>
              <CTableDataCell colSpan={3} className="text-center text-muted py-4">
                No members. Click &quot;Add Member&quot; to add symbols to this index.
              </CTableDataCell>
            </CTableRow>
          )}
        </CTableBody>
      </CTable>

      <div className="mt-2">
        <CButton
          color="primary"
          variant="outline"
          size="sm"
          onClick={handleAddRow}
          disabled={disabled}
        >
          <CIcon icon={cilPlus} className="me-1" />
          Add Member
        </CButton>
      </div>
    </div>
  );
};

export default EditableMembersTable;
