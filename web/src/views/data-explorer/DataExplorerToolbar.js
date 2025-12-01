import React from 'react'
import PropTypes from 'prop-types'
import {
  CFormInput,
  CFormSelect,
  CButton,
  CSpinner,
  CInputGroup,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilMagnifyingGlass, cilCloudDownload } from '@coreui/icons'

/**
 * DataExplorerToolbar Component
 * 
 * Reusable toolbar component for Data Explorer views (Chart and Table).
 * Uses idPrefix to generate unique accessibility IDs for each view instance.
 * 
 * @param {string} idPrefix - Prefix for generating unique IDs (e.g., 'chart' or 'table')
 * @param {string} searchQuery - Current search query value
 * @param {function} setSearchQuery - Handler to update search query
 * @param {function} handleSearch - Handler for search button click
 * @param {function} handleSearchKeyPress - Handler for Enter key in search input
 * @param {Array} searchResults - Array of search result symbols
 * @param {boolean} isSearching - Whether search is in progress
 * @param {string|null} searchError - Search error message, if any
 * @param {Object|null} selectedSymbol - Currently selected symbol object
 * @param {function} handleSymbolSelect - Handler for symbol selection
 * @param {string} selectedDataType - Currently selected data type ('historical' or 'live')
 * @param {function} handleDataTypeChange - Handler for data type change
 * @param {string} selectedInterval - Currently selected interval
 * @param {function} setSelectedInterval - Handler to update selected interval
 * @param {Array} availableIntervals - Array of available intervals for selected symbol
 * @param {function} handleDownload - Handler for download button click
 * @param {boolean} canDownload - Whether download button should be enabled
 */
const DataExplorerToolbar = ({
  idPrefix,
  searchQuery,
  setSearchQuery,
  handleSearch,
  handleSearchKeyPress,
  searchResults,
  isSearching,
  searchError,
  selectedSymbol,
  handleSymbolSelect,
  selectedDataType,
  handleDataTypeChange,
  selectedInterval,
  setSelectedInterval,
  availableIntervals,
  handleDownload,
  canDownload,
}) => {
  // Generate unique IDs based on prefix for accessibility
  const errorId = `${idPrefix}-search-error`
  const dataTypeHelpId = `${idPrefix}-data-type-help`
  const intervalHelpId = `${idPrefix}-interval-help`

  return (
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
            aria-describedby={searchError ? errorId : undefined}
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
            id={errorId}
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
            aria-describedby={!selectedDataType ? dataTypeHelpId : undefined}
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
            <div id={dataTypeHelpId} className="sr-only">
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
            aria-describedby={availableIntervals.length === 0 ? intervalHelpId : undefined}
          >
            <option value="">Interval...</option>
            {availableIntervals.map((interval) => (
              <option key={interval} value={interval}>
                {interval}
              </option>
            ))}
          </CFormSelect>
          {availableIntervals.length === 0 && (
            <div id={intervalHelpId} className="text-muted small mt-1" role="status" aria-live="polite">
              No intervals available
            </div>
          )}
        </div>
      )}
      
      {/* Download Button - progressive disclosure */}
      {selectedSymbol && selectedDataType && selectedInterval && (
        <div>
          <CButton
            color="secondary"
            variant="outline"
            size="sm"
            onClick={handleDownload}
            disabled={!canDownload}
            title={canDownload ? "Download data as CSV" : "Select data to download"}
            aria-label={canDownload ? "Download data as CSV file" : "Select data to download"}
          >
            <CIcon icon={cilCloudDownload} size="sm" />
          </CButton>
        </div>
      )}
    </div>
  )
}

DataExplorerToolbar.propTypes = {
  idPrefix: PropTypes.string.isRequired,
  searchQuery: PropTypes.string.isRequired,
  setSearchQuery: PropTypes.func.isRequired,
  handleSearch: PropTypes.func.isRequired,
  handleSearchKeyPress: PropTypes.func.isRequired,
  searchResults: PropTypes.array.isRequired,
  isSearching: PropTypes.bool.isRequired,
  searchError: PropTypes.string,
  selectedSymbol: PropTypes.object,
  handleSymbolSelect: PropTypes.func.isRequired,
  selectedDataType: PropTypes.string.isRequired,
  handleDataTypeChange: PropTypes.func.isRequired,
  selectedInterval: PropTypes.string.isRequired,
  setSelectedInterval: PropTypes.func.isRequired,
  availableIntervals: PropTypes.array.isRequired,
  handleDownload: PropTypes.func.isRequired,
  canDownload: PropTypes.bool.isRequired,
}

DataExplorerToolbar.defaultProps = {
  searchError: null,
  selectedSymbol: null,
}

export default DataExplorerToolbar

