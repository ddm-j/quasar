import React, { useState, useCallback, useEffect } from 'react';
import {
  CModal, CModalHeader, CModalTitle, CModalBody, CModalFooter,
  CButton, CForm, CRow, CCol, CFormLabel, CFormSelect, CFormInput,
  CSpinner
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilArrowRight } from '@coreui/icons';
import Select, { components } from 'react-select'; // For synchronous search if options are pre-loaded or filtered client-side
import AsyncSelect from 'react-select/async'; // For asynchronous server-side search
import { 
  getAssets,
  getRegisteredClasses,
} from '../services/registry_api';  

// Custom component to render options with symbol and name
const CustomOption = (props) => {
  const { innerProps, data, isFocused, isSelected } = props; 

  const optionStyles = {
    padding: '8px 12px',
    cursor: 'pointer',
    backgroundColor: isSelected ? '#007bff' : isFocused ? '#e9ecef' : 'white', // Example: blue if selected, light grey if focused
    color: isSelected ? 'white' : 'black', // Example: white text if selected
  };

  return (
    <div {...innerProps} style={optionStyles}> 
      <div style={{ fontWeight: 'bold' }}>{data.symbol}</div>
      <div style={{ fontSize: '0.9em', color: isSelected ? '#f0f0f0' : '#555' }}> 
        {data.name}
      </div>
    </div>
  );
};

// Custom component for the selected value display (optional, can use default label)
const SingleValue = (props) => {
    const { data } = props;
    return (
      <components.SingleValue {...props}>
        {data.symbol} <span style={{ fontSize: '0.9em', color: '#777' }}>({data.name})</span>
      </components.SingleValue>
    );
};


const MappingAddModal = ({ visible, onClose }) => {
  const [fromClassName, setFromClassName] = useState('');
  // For react-select, the value is an object or null
  const [fromClassSymbol, setFromClassSymbol] = useState(null);
  const [toCommonSymbol, setToCommonSymbol] = useState('');

  const [isLoadingSymbols, setIsLoadingSymbols] = useState(false);

  // State for loading class names
  const [classData, setClassData] = useState([]);
  const [isLoadingClassData, setIsLoadingClassData] = useState(false);
  const [errorClassData, setErrorClassData] = useState(null);

  useEffect(() => {
    if (visible) {
      setFromClassName("");
      setFromClassSymbol(null);
      setToCommonSymbol("");
      setErrorClassData(null);
      setClassData([]);

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

  const handleFromClassNameChange = (e) => {
    setFromClassName(e.target.value);
    setFromClassSymbol(null); // Reset symbol when class name changes
  };

  const handleSave = () => {
    console.log("Saving Mapping:", {
      toCommonSymbol,
      fromClassName,
      fromClassType: classData.find(cls => cls.class_name === fromClassName)?.class_type || null,
      fromClassSymbol: fromClassSymbol ? fromClassSymbol.value : null, // Extract the actual symbol value
    });
    // onClose(); // Close modal on save
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
                    cacheOptions // Caches results for same search term
                    loadOptions={loadSymbolOptions} // Function to fetch options
                    defaultOptions // Load some default options on first focus (can be true or an array)
                    value={fromClassSymbol}
                    onChange={(selectedOption) => setFromClassSymbol(selectedOption)}
                    placeholder="Type to search symbol or name..."
                    isDisabled={!fromClassName} // Disable if no class_name selected
                    isLoading={isLoadingSymbols}
                    isClearable
                    components={{ Option: CustomOption, SingleValue }} // Use custom rendering
                    // formatOptionLabel={formatOptionLabel} // Alternative way to customize option display
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
                  <CFormInput
                    type="text"
                    id="to_common_symbol"
                    value={toCommonSymbol}
                    onChange={(e) => setToCommonSymbol(e.target.value)}
                    disabled={!fromClassSymbol}
                  />
                </CCol>
              </CRow>
            </CCol>
          </CRow>
        </CModalBody>
        <CModalFooter>
          <CButton color="secondary" onClick={onClose}>
            Cancel
          </CButton>
          <CButton color="primary" onClick={handleSave} disabled={!fromClassName || !fromClassSymbol || !toCommonSymbol}>
            Save Mapping (Simulated)
          </CButton>
        </CModalFooter>
      </CForm>
    </CModal>
  );
};

export default MappingAddModal;