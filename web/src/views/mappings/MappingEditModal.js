import React, { useState, useCallback, useEffect } from 'react';
import {
  CModal, CModalHeader, CModalTitle, CModalBody, CModalFooter,
  CButton, CForm, CRow, CCol, CFormLabel, CFormInput, CFormCheck,
  CSpinner
} from '@coreui/react-pro';
import AsyncCreatableSelect from 'react-select/async-creatable';
import { 
  getAssetMappings,
  updateAssetMapping,
} from '../services/registry_api';  

const MappingEditModal = ({ visible, onClose, onSuccess, mapping }) => {
  // State for editable fields
  const [commonSymbol, setCommonSymbol] = useState(null);
  const [isActive, setIsActive] = useState(true);

  // State for loading common symbols
  const [isLoadingCommonSymbols, setIsLoadingCommonSymbols] = useState(false);

  // State for saving
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState(null);

  // Initialize form when modal opens or mapping changes
  useEffect(() => {
    if (visible && mapping) {
      // Pre-populate form with existing mapping data
      setCommonSymbol({
        value: mapping.common_symbol,
        label: mapping.common_symbol,
      });
      setIsActive(mapping.is_active);
      setSaveError(null);
    }
  }, [visible, mapping]);

  // This function will be called by AsyncCreatableSelect to load existing common symbol options
  const loadCommonSymbolOptions = useCallback(
    async (inputValue, callback) => {
      if (inputValue.length < 1) {
        // Load some default options when input is empty
        setIsLoadingCommonSymbols(true);
        try {
          const mappings = await getAssetMappings();
          // Extract unique common symbols
          const uniqueSymbols = [...new Set(mappings.map(m => m.common_symbol))].sort();
          const options = uniqueSymbols.map(symbol => ({
            value: symbol,
            label: symbol,
          }));
          callback(options);
        } catch (error) {
          console.error("Error loading common symbol options:", error);
          callback([]);
        } finally {
          setIsLoadingCommonSymbols(false);
        }
        return;
      }

      // Filter existing common symbols based on input
      setIsLoadingCommonSymbols(true);
      try {
        const mappings = await getAssetMappings();
        // Filter common symbols that match the input (case-insensitive)
        const filteredSymbols = [...new Set(mappings.map(m => m.common_symbol))]
          .filter(symbol => symbol.toLowerCase().includes(inputValue.toLowerCase()))
          .sort();
        
        const options = filteredSymbols.map(symbol => ({
          value: symbol,
          label: symbol,
        }));
        callback(options);
      } catch (error) {
        console.error("Error loading common symbol options:", error);
        callback([]);
      } finally {
        setIsLoadingCommonSymbols(false);
      }
    },
    [] // No dependencies - loads all mappings
  );

  const handleSave = async () => {
    // Validate required fields
    if (!mapping || !commonSymbol) {
      setSaveError("Common symbol is required.");
      return;
    }

    // Extract common symbol value from react-select option object
    const commonSymbolValue = commonSymbol.value ? commonSymbol.value.trim() : null;
    
    if (!commonSymbolValue) {
      setSaveError("Common symbol cannot be empty.");
      return;
    }

    // Build the update payload (only fields that can be updated)
    const updateData = {
      common_symbol: commonSymbolValue,
      is_active: isActive,
    };

    setIsSaving(true);
    setSaveError(null);

    try {
      await updateAssetMapping(
        mapping.class_name,
        mapping.class_type,
        mapping.class_symbol,
        updateData
      );
      // Success - close modal and refresh parent list
      if (onSuccess) {
        onSuccess();
      }
      onClose();
    } catch (error) {
      console.error("Error updating asset mapping:", error);
      setSaveError(error.message || "Failed to update mapping. Please try again.");
    } finally {
      setIsSaving(false);
    }
  };

  // Don't render if no mapping provided
  if (!mapping) {
    return null;
  }

  return (
    <CModal visible={visible} onClose={onClose} backdrop="static" size="lg">
      <CModalHeader onClose={onClose}>
        <CModalTitle>Edit Asset Mapping</CModalTitle>
      </CModalHeader>
      <CForm onSubmit={(e) => e.preventDefault()}>
        <CModalBody>
          <CRow>
            {/* Read-only fields for mapping identifier */}
            <CCol md={6}>
              <h5 className="mb-3">Mapping Identifier (Read-only)</h5>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="class_name">Class Name</CFormLabel>
                  <CFormInput
                    id="class_name"
                    type="text"
                    value={mapping.class_name}
                    disabled
                    readOnly
                  />
                </CCol>
              </CRow>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="class_type">Class Type</CFormLabel>
                  <CFormInput
                    id="class_type"
                    type="text"
                    value={mapping.class_type}
                    disabled
                    readOnly
                  />
                </CCol>
              </CRow>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="class_symbol">Class Symbol</CFormLabel>
                  <CFormInput
                    id="class_symbol"
                    type="text"
                    value={mapping.class_symbol}
                    disabled
                    readOnly
                  />
                </CCol>
              </CRow>
            </CCol>

            {/* Editable fields */}
            <CCol md={6}>
              <h5 className="mb-3">Editable Fields</h5>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="common_symbol">Common Symbol</CFormLabel>
                  <AsyncCreatableSelect
                    id="common_symbol"
                    cacheOptions
                    loadOptions={loadCommonSymbolOptions}
                    defaultOptions
                    value={commonSymbol}
                    onChange={(selectedOption) => setCommonSymbol(selectedOption)}
                    placeholder="Type to search or create new common symbol..."
                    isDisabled={isSaving}
                    isLoading={isLoadingCommonSymbols}
                    isClearable
                    formatCreateLabel={(inputValue) => `Create "${inputValue}"`}
                    noOptionsMessage={({ inputValue }) => 
                      !inputValue ? "Type to search or create..." : `No matches found. Press Enter to create "${inputValue}"`
                    }
                    loadingMessage={() => "Loading common symbols..."}
                    classNamePrefix="themed-select"
                  />
                </CCol>
              </CRow>
              <CRow className="mb-3">
                <CCol>
                  <CFormCheck
                    id="is_active"
                    type="checkbox"
                    label="Active"
                    checked={isActive}
                    onChange={(e) => setIsActive(e.target.checked)}
                    disabled={isSaving}
                  />
                </CCol>
              </CRow>
            </CCol>
          </CRow>
          {saveError && (
            <CRow>
              <CCol>
                <div className="alert alert-danger" role="alert">
                  {saveError}
                </div>
              </CCol>
            </CRow>
          )}
        </CModalBody>
        <CModalFooter>
          <CButton color="secondary" onClick={onClose} disabled={isSaving}>
            Cancel
          </CButton>
          <CButton color="primary" onClick={handleSave} disabled={!commonSymbol || isSaving}>
            {isSaving ? (
              <>
                <CSpinner size="sm" className="me-2" />
                Saving...
              </>
            ) : (
              'Update Mapping'
            )}
          </CButton>
        </CModalFooter>
      </CForm>
    </CModal>
  );
};

export default MappingEditModal;

