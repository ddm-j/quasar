import React, { useState } from 'react'
import {
  CCard,
  CCardBody,
  CCol,
  CRow,
  CNav,
  CNavItem,
  CNavLink,
  CTabContent,
  CTabPane,
  CFormInput,
  CFormSelect,
  CButton,
  CSpinner,
  CInputGroup,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilMagnifyingGlass } from '@coreui/icons'
import CandlestickChart from './CandlestickChart'
import { searchSymbols } from '../services/datahub_api'

const DataExplorer = () => {
  const [activeKey, setActiveKey] = useState(1)
  
  // Search state
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState([])
  const [isSearching, setIsSearching] = useState(false)
  const [searchError, setSearchError] = useState(null)
  
  // Selection state
  const [selectedSymbol, setSelectedSymbol] = useState(null)
  const [selectedProvider, setSelectedProvider] = useState('')
  const [selectedSymbolName, setSelectedSymbolName] = useState('')
  const [selectedDataType, setSelectedDataType] = useState('')
  const [selectedInterval, setSelectedInterval] = useState('')
  const [availableIntervals, setAvailableIntervals] = useState([])

  // Handle symbol search
  const handleSearch = async () => {
    if (!searchQuery.trim()) {
      setSearchResults([])
      // Clear selection when clearing search
      setSelectedSymbol(null)
      setSelectedProvider('')
      setSelectedSymbolName('')
      setSelectedDataType('')
      setSelectedInterval('')
      setAvailableIntervals([])
      return
    }

    setIsSearching(true)
    setSearchError(null)
    
    // Clear previous selection when performing new search
    // This prevents stale object references from breaking the select dropdown
    setSelectedSymbol(null)
    setSelectedProvider('')
    setSelectedSymbolName('')
    setSelectedDataType('')
    setSelectedInterval('')
    setAvailableIntervals([])

    try {
      const response = await searchSymbols(searchQuery.trim(), { limit: 50 })
      setSearchResults(response.items || [])
    } catch (err) {
      console.error('Error searching symbols:', err)
      setSearchError(err.message || 'Failed to search symbols')
      setSearchResults([])
    } finally {
      setIsSearching(false)
    }
  }

  // Handle symbol selection
  const handleSymbolSelect = (e) => {
    const selectedIndex = e.target.value
    if (selectedIndex === '' || selectedIndex === '-1') {
      setSelectedSymbol(null)
      setSelectedProvider('')
      setSelectedSymbolName('')
      setSelectedDataType('')
      setSelectedInterval('')
      setAvailableIntervals([])
      return
    }

    const symbol = searchResults[parseInt(selectedIndex)]
    if (symbol) {
      setSelectedSymbol(symbol)
      setSelectedProvider(symbol.provider)
      setSelectedSymbolName(symbol.provider_symbol)
      setAvailableIntervals(symbol.available_intervals || [])
      
      // Auto-select data type if only one is available
      if (symbol.has_historical && !symbol.has_live) {
        setSelectedDataType('historical')
      } else if (symbol.has_live && !symbol.has_historical) {
        setSelectedDataType('live')
      } else {
        // Default to historical if both available
        setSelectedDataType('historical')
      }
      
      // Auto-select first interval if available
      if (symbol.available_intervals && symbol.available_intervals.length > 0) {
        setSelectedInterval(symbol.available_intervals[0])
      } else {
        setSelectedInterval('')
      }
    }
  }

  // Handle data type change
  const handleDataTypeChange = (e) => {
    setSelectedDataType(e.target.value)
    setSelectedInterval('')
    
    // Update available intervals based on selected symbol and data type
    if (selectedSymbol) {
      // For now, use all intervals from the symbol
      // In a more advanced implementation, we could filter by data type
      setAvailableIntervals(selectedSymbol.available_intervals || [])
      if (selectedSymbol.available_intervals && selectedSymbol.available_intervals.length > 0) {
        setSelectedInterval(selectedSymbol.available_intervals[0])
      }
    }
  }

  // Handle search on Enter key
  const handleSearchKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch()
    }
  }

  return (
    <CRow>
      <CCol xs={12}>
        <CCard>
          <CCardBody>
            <CNav variant="tabs" role="tablist">
              <CNavItem>
                <CNavLink
                  href="#"
                  active={activeKey === 1}
                  onClick={(e) => {
                    e.preventDefault()
                    setActiveKey(1)
                  }}
                >
                  Chart View
                </CNavLink>
              </CNavItem>
              <CNavItem>
                <CNavLink
                  href="#"
                  active={activeKey === 2}
                  onClick={(e) => {
                    e.preventDefault()
                    setActiveKey(2)
                  }}
                >
                  Table View
                </CNavLink>
              </CNavItem>
            </CNav>
            <CTabContent>
              <CTabPane visible={activeKey === 1} className="p-3">
                {/* Compact Toolbar */}
                <div className="d-flex flex-wrap gap-2 align-items-end mb-3">
                  {/* Search Group */}
                  <div style={{ minWidth: '200px', maxWidth: '300px', flex: '0 1 auto' }}>
                    <CInputGroup>
                      <CFormInput
                        type="text"
                        placeholder="Search symbols..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        onKeyPress={handleSearchKeyPress}
                        aria-label="Search for trading symbols"
                        aria-describedby={searchError ? "search-error" : undefined}
                        aria-busy={isSearching}
                      />
                      <CButton
                        color="primary"
                        onClick={handleSearch}
                        disabled={isSearching}
                        aria-label={isSearching ? "Searching symbols" : "Search for symbols"}
                        title="Search symbols"
                      >
                        {isSearching ? <CSpinner size="sm" aria-hidden="true" /> : <CIcon icon={cilMagnifyingGlass} />}
                      </CButton>
                    </CInputGroup>
                    {searchError && (
                      <div 
                        id="search-error" 
                        className="text-danger small mt-1" 
                        role="alert"
                        aria-live="polite"
                      >
                        {searchError}
                      </div>
                    )}
                  </div>
                  
                  {/* Symbol Select */}
                  <div className="flex-grow-1" style={{ minWidth: '280px' }}>
                    <CFormSelect
                      value={selectedSymbol ? searchResults.indexOf(selectedSymbol).toString() : ''}
                      onChange={handleSymbolSelect}
                      disabled={searchResults.length === 0}
                      aria-label="Select a symbol from search results"
                    >
                      <option value="">Symbol...</option>
                      {searchResults.map((symbol, index) => (
                        <option key={index} value={index}>
                          {symbol.common_symbol} ({symbol.provider}/{symbol.provider_symbol})
                          {symbol.has_historical && symbol.has_live ? ' [H+L]' : symbol.has_historical ? ' [H]' : symbol.has_live ? ' [L]' : ''}
                        </option>
                      ))}
                    </CFormSelect>
                  </div>
                  
                  {/* Data Type - only show when symbol selected */}
                  {selectedSymbol && (
                    <div style={{ minWidth: '140px' }}>
                      <CFormSelect
                        value={selectedDataType}
                        onChange={handleDataTypeChange}
                        aria-label="Select data type"
                        aria-describedby={!selectedDataType ? "data-type-help" : undefined}
                      >
                        <option value="">Type...</option>
                        {selectedSymbol.has_historical && (
                          <option value="historical">Historical</option>
                        )}
                        {selectedSymbol.has_live && (
                          <option value="live">Live</option>
                        )}
                      </CFormSelect>
                      {!selectedDataType && (
                        <div id="data-type-help" className="sr-only">
                          Select whether to view historical or live data
                        </div>
                      )}
                    </div>
                  )}
                  
                  {/* Interval - only show when data type selected */}
                  {selectedDataType && (
                    <div style={{ minWidth: '120px' }}>
                      <CFormSelect
                        value={selectedInterval}
                        onChange={(e) => setSelectedInterval(e.target.value)}
                        disabled={availableIntervals.length === 0}
                        aria-label="Select time interval"
                        aria-describedby={availableIntervals.length === 0 ? "interval-help" : undefined}
                      >
                        <option value="">Interval...</option>
                        {availableIntervals.map((interval) => (
                          <option key={interval} value={interval}>
                            {interval}
                          </option>
                        ))}
                      </CFormSelect>
                      {availableIntervals.length === 0 && (
                        <div id="interval-help" className="text-muted small mt-1" role="status" aria-live="polite">
                          No intervals available
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {/* Chart */}
                <CandlestickChart
                  provider={selectedProvider}
                  symbol={selectedSymbolName}
                  dataType={selectedDataType}
                  interval={selectedInterval}
                  limit={5000}
                />

                {/* Symbol Info Footer */}
                {selectedSymbol && (
                  <div className="mt-2 pt-2 border-top">
                    <small className="text-muted">
                      <strong>Common:</strong> {selectedSymbol.common_symbol} | 
                      <strong> Provider:</strong> {selectedSymbol.provider} | 
                      <strong> Symbol:</strong> {selectedSymbol.provider_symbol}
                    </small>
                  </div>
                )}
              </CTabPane>
              <CTabPane visible={activeKey === 2} className="p-3">
                <h6>Data Table</h6>
                <p className="text-body-secondary">
                  Table view coming soon. This will include a filterable table with symbols, number of bars, most recent data, etc.
                </p>
              </CTabPane>
            </CTabContent>
          </CCardBody>
        </CCard>
      </CCol>
    </CRow>
  )
}

export default DataExplorer

