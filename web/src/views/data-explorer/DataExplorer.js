import React, { useState, useCallback } from 'react'
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
} from '@coreui/react-pro'
import CandlestickChart from './CandlestickChart'
import OHLCTable from './OHLCTable'
import DataExplorerToolbar from './DataExplorerToolbar'
import { searchSymbols } from '../services/datahub_api'
import { downloadCSV } from '../../utils/csvExport'
import { INTERVALS } from '../../enums'

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

  // Data state for download functionality
  const [currentData, setCurrentData] = useState([])

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
    setCurrentData([]) // Clear data when clearing search

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
      const symbolIntervals = symbol.available_intervals || []
      const canonicalIntervals = symbolIntervals.filter((iv) => INTERVALS.includes(iv))
      const intervalsToUse = canonicalIntervals.length > 0 ? canonicalIntervals : symbolIntervals
      setAvailableIntervals(intervalsToUse)

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
      if (intervalsToUse && intervalsToUse.length > 0) {
        setSelectedInterval(intervalsToUse[0])
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
      const symbolIntervals = selectedSymbol.available_intervals || []
      const canonicalIntervals = symbolIntervals.filter((iv) => INTERVALS.includes(iv))
      const intervalsToUse = canonicalIntervals.length > 0 ? canonicalIntervals : symbolIntervals
      setAvailableIntervals(intervalsToUse)
      if (intervalsToUse && intervalsToUse.length > 0) {
        setSelectedInterval(intervalsToUse[0])
      }
    }
  }

  // Handle search on Enter key
  const handleSearchKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch()
    }
  }

  // Handle data updates from child components
  // Wrapped in useCallback to provide stable reference for child component dependencies
  const handleDataChange = useCallback((data) => {
    setCurrentData(data || [])
  }, [])

  // Handle download click
  const handleDownload = () => {
    if (
      currentData &&
      currentData.length > 0 &&
      selectedSymbol &&
      selectedDataType &&
      selectedInterval
    ) {
      // Convert chart data format to OHLC format if needed
      // Chart data has time, open, high, low, close (no volume in chartData)
      // We need to get the full data from the API response
      // For now, we'll download what we have
      downloadCSV(currentData, selectedSymbol, selectedDataType, selectedInterval)
    }
  }

  // Determine if download button should be enabled
  const canDownload =
    selectedSymbol && selectedDataType && selectedInterval && currentData && currentData.length > 0

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
                <DataExplorerToolbar
                  idPrefix="chart"
                  searchQuery={searchQuery}
                  setSearchQuery={setSearchQuery}
                  handleSearch={handleSearch}
                  handleSearchKeyPress={handleSearchKeyPress}
                  searchResults={searchResults}
                  isSearching={isSearching}
                  searchError={searchError}
                  selectedSymbol={selectedSymbol}
                  handleSymbolSelect={handleSymbolSelect}
                  selectedDataType={selectedDataType}
                  handleDataTypeChange={handleDataTypeChange}
                  selectedInterval={selectedInterval}
                  setSelectedInterval={setSelectedInterval}
                  availableIntervals={availableIntervals}
                  handleDownload={handleDownload}
                  canDownload={canDownload}
                />

                {/* Chart */}
                <CandlestickChart
                  provider={selectedProvider}
                  symbol={selectedSymbolName}
                  dataType={selectedDataType}
                  interval={selectedInterval}
                  limit={5000}
                  onDataChange={handleDataChange}
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
                {/* Compact Toolbar */}
                <DataExplorerToolbar
                  idPrefix="table"
                  searchQuery={searchQuery}
                  setSearchQuery={setSearchQuery}
                  handleSearch={handleSearch}
                  handleSearchKeyPress={handleSearchKeyPress}
                  searchResults={searchResults}
                  isSearching={isSearching}
                  searchError={searchError}
                  selectedSymbol={selectedSymbol}
                  handleSymbolSelect={handleSymbolSelect}
                  selectedDataType={selectedDataType}
                  handleDataTypeChange={handleDataTypeChange}
                  selectedInterval={selectedInterval}
                  setSelectedInterval={setSelectedInterval}
                  availableIntervals={availableIntervals}
                  handleDownload={handleDownload}
                  canDownload={canDownload}
                />

                {/* Table */}
                <OHLCTable
                  provider={selectedProvider}
                  symbol={selectedSymbolName}
                  dataType={selectedDataType}
                  interval={selectedInterval}
                  limit={5000}
                  onDataChange={handleDataChange}
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
            </CTabContent>
          </CCardBody>
        </CCard>
      </CCol>
    </CRow>
  )
}

export default DataExplorer
