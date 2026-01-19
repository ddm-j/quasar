import React, { useState, useCallback, useEffect } from 'react';
import {
  CModal, CModalHeader, CModalTitle, CModalBody, CModalFooter,
  CButton, CForm, CRow, CCol, CFormLabel, CFormSelect, CFormInput,
  CSpinner
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilArrowRight } from '@coreui/icons';
import AsyncSelect from 'react-select/async'; // For asynchronous server-side search
import AsyncCreatableSelect from 'react-select/async-creatable'; // For async search with create option
import { 
  getAssets,
  getRegisteredClasses,
  createAssetMapping,
  getAssetMappings,
} from '../services/registry_api';  

// Format option label for custom display in dropdown and selected value
const formatOptionLabel = (data, { context }) => {
  if (context === 'menu') {
    // Dropdown menu option display
    return (
      <div>
        <div style={{ fontWeight: 'bold' }}>{data.symbol}</div>
        <div style={{ fontSize: '0.85em', opacity: 0.7 }}>
          {data.name}
        </div>
      </div>
    );
  }
  // Selected value display
  return (
    <span>
      {data.symbol} <span style={{ fontSize: '0.9em', opacity: 0.7 }}>({data.name})</span>
    </span>
  );
};


const MappingAddModal = ({ visible, onClose, onSuccess, pushToast }) => {
  const [fromClassName, setFromClassName] = useState('');
  // For react-select, the value is an object or null
  const [fromClassSymbol, setFromClassSymbol] = useState(null);
  // For common symbol, using react-select format (object with value/label) or string for new values
  const [toCommonSymbol, setToCommonSymbol] = useState(null);

  const [isLoadingSymbols, setIsLoadingSymbols] = useState(false);
  const [isLoadingCommonSymbols, setIsLoadingCommonSymbols] = useState(false);

  // State for loading class names
  const [classData, setClassData] = useState([]);
  const [isLoadingClassData, setIsLoadingClassData] = useState(false);
  const [errorClassData, setErrorClassData] = useState(null);

  // State for saving
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState(null);

  useEffect(() => {
    if (visible) {
      setFromClassName("");
      setFromClassSymbol(null);
      setToCommonSymbol(null);
      setErrorClassData(null);
      setClassData([]);
      setSaveError(null);

      const fetchClassNames = async () => {
        setIsLoadingClassData(true);
        try {
          const data = await getRegisteredClasses();
          setClassData(data || []);
        } catch (error) {
          console.error("Error fetching class names:", error);
          setErrorClassData(error.message);
        } finally {
          setIsLoadingClassData(false);
        }
      }
      fetchClassNames();
    }
  }, [visible]); // Fetch class names when modal opens

  // This function will be called by AsyncSelect to load options
  const loadSymbolOptions = useCallback(
    async (inputValue, callback) => {
      if (!fromClassName || inputValue.length < 1) { // Don't search if no class or input is too short
        callback([]);
        return;
      }
      setIsLoadingSymbols(true);
      try {
        const params = {
          class_name_like: fromClassName,
          symbol_like: inputValue,
          // name_like: inputValue,
          limit: 50,
        }
        console.log("Loading symbols with params:", params);
        const assetData = await getAssets(params);

        // Format the Options
        const options = (assetData.items || []).map(item => ({
          value: item.symbol,
          label: `${item.symbol} (${item.name || 'N/A'})`,
          symbol: item.symbol,
          name: item.name || 'N/A',
        }));

        callback(options);
      } catch (error) {
        console.error("Error loading symbol options:", error);
        callback([]); // Send empty array on error
      } finally {
        setIsLoadingSymbols(false);
      }
    },
    [fromClassName] // Dependency: re-create this function if fromClassName changes
  );

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

  const handleFromClassNameChange = (e) => {
    setFromClassName(e.target.value);
    setFromClassSymbol(null); // Reset symbol when class name changes
  };

  const handleSave = async () => {
    // Validate required fields
    if (!fromClassName || !fromClassSymbol || !toCommonSymbol) {
      setSaveError("Please fill in all required fields.");
      return;
    }

    // Extract common symbol value from react-select option object
    // AsyncCreatableSelect always provides an object with { value, label } format
    const commonSymbolValue = toCommonSymbol.value ? toCommonSymbol.value.trim() : null;
    
    if (!commonSymbolValue) {
      setSaveError("Common symbol cannot be empty.");
      return;
    }

    // Find the class type from classData
    const selectedClass = classData.find(cls => cls.class_name === fromClassName);
    if (!selectedClass) {
      setSaveError("Selected class not found.");
      return;
    }

    const classType = selectedClass.class_type;
    const classSymbol = fromClassSymbol.value;

    // Build the request payload
    const mappingData = {
      common_symbol: commonSymbolValue,
      class_name: fromClassName,
      class_type: classType,
      class_symbol: classSymbol,
      is_active: true,
    };

    setIsSaving(true);
    setSaveError(null);

    try {
      const created = await createAssetMapping(mappingData);
      const createdMapping = Array.isArray(created) ? created[0] : created;
      if (!createdMapping) {
        throw new Error("Failed to create mapping");
      }
      // Success - close modal and refresh parent list
      if (onSuccess) {
        onSuccess();
      }
      onClose();
    } catch (error) {
      console.error("Error creating asset mapping:", error);
      if (pushToast) {
        pushToast({
          title: "Create mapping failed",
          body: error.message || "Failed to create mapping. Please try again.",
          color: "danger",
        });
      } else {
        setSaveError(error.message || "Failed to create mapping. Please try again.");
      }
    } finally {
      setIsSaving(false);
    }
  };


  return (
    <CModal visible={visible} onClose={onClose} backdrop="static" size="lg">
      <CModalHeader onClose={onClose}>
        <CModalTitle>Create New Asset Mapping (Basic)</CModalTitle>
      </CModalHeader>
      <CForm onSubmit={(e) => e.preventDefault()}> {/* Prevent default form submission */}
        <CModalBody>
          <CRow>
            {/* "FROM" Side */}
            <CCol md={5}>
              <h5 className="mb-3">From (Source)</h5>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="from_class_name">Class Name</CFormLabel>
                  <CFormSelect
                    id="from_class_name"
                    value={fromClassName}
                    onChange={handleFromClassNameChange}
                    disabled={isLoadingClassData}
                  >
                      <option value="" disabled>Select Class Name</option>
                    {classData.map(opt => (
                      <option key={opt.class_name} value={opt.class_name}>{opt.class_name}</option>
                    ))}
                  </CFormSelect>
                </CCol>
              </CRow>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="from_class_symbol">Class Symbol (Searchable)</CFormLabel>
                  <AsyncSelect
                    key={fromClassName}
                    id="from_class_symbol"
                    cacheOptions
                    loadOptions={loadSymbolOptions}
                    defaultOptions
                    value={fromClassSymbol}
                    onChange={(selectedOption) => setFromClassSymbol(selectedOption)}
                    placeholder="Type to search symbol or name..."
                    isDisabled={!fromClassName}
                    isLoading={isLoadingSymbols}
                    isClearable
                    formatOptionLabel={formatOptionLabel}
                    classNamePrefix="themed-select"
                    noOptionsMessage={({ inputValue }) =>
                        !inputValue ? "Type to search..." : "No symbols found"
                    }
                    loadingMessage={() => "Loading symbols..."}
                  />
                </CCol>
              </CRow>
            </CCol>

            {/* Arrow Separator */}
            <CCol md={1} className="d-flex align-items-center justify-content-center mt-4 pt-2">
              <CIcon icon={cilArrowRight} size="3xl" />
            </CCol>

            {/* "TO" Side (Common Symbol) */}
            <CCol md={6}>
              <h5 className="mb-3">To (Common)</h5>
              <CRow className="mb-3">
                <CCol>
                  <CFormLabel htmlFor="to_common_symbol">Common Symbol</CFormLabel>
                  <AsyncCreatableSelect
                    id="to_common_symbol"
                    cacheOptions
                    loadOptions={loadCommonSymbolOptions}
                    defaultOptions
                    value={toCommonSymbol}
                    onChange={(selectedOption) => setToCommonSymbol(selectedOption)}
                    placeholder="Type to search or create new common symbol..."
                    isDisabled={!fromClassSymbol || isSaving}
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
          <CButton color="primary" onClick={handleSave} disabled={!fromClassName || !fromClassSymbol || !toCommonSymbol || isSaving}>
            {isSaving ? (
              <>
                <CSpinner size="sm" className="me-2" />
                Saving...
              </>
            ) : (
              'Save Mapping'
            )}
          </CButton>
        </CModalFooter>
      </CForm>
    </CModal>
  );
};

export default MappingAddModal;