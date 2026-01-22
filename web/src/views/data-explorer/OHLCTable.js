import React, { useState, useEffect, useRef } from 'react'
import PropTypes from 'prop-types'
import {
  CTable,
  CTableHead,
  CTableBody,
  CTableRow,
  CTableHeaderCell,
  CTableDataCell,
  CSmartPagination,
  CFormSelect,
  CSpinner,
  CRow,
  CCol,
} from '@coreui/react-pro'
import { getOHLCData } from '../services/datahub_api'
import CIcon from '@coreui/icons-react'
import { cilArrowTop, cilArrowBottom } from '@coreui/icons'

const OHLCTable = ({ provider, symbol, dataType, interval, limit = 5000, onDataChange }) => {
  // State management
  const [allData, setAllData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(50)
  const [sortDirection, setSortDirection] = useState('desc') // 'desc' | 'asc'
  const cancelledRef = useRef(false)

  // Format Unix timestamp to readable date
  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp * 1000) // Convert seconds to milliseconds
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
    })
  }

  // Format price with 2 decimal places
  const formatPrice = (price) => {
    return typeof price === 'number' ? price.toFixed(2) : price
  }

  // Format volume
  const formatVolume = (volume) => {
    return typeof volume === 'number'
      ? volume.toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })
      : volume
  }

  // Determine if price went up (green) or down (red)
  const getPriceChange = (open, close) => {
    if (close > open) return 'up'
    if (close < open) return 'down'
    return 'neutral'
  }

  // Sort data by time
  const getSortedData = () => {
    const sorted = [...allData]
    sorted.sort((a, b) => {
      if (sortDirection === 'desc') {
        return b.time - a.time // Most recent first (default)
      } else {
        return a.time - b.time // Oldest first
      }
    })
    return sorted
  }

  // Fetch data when props change
  useEffect(() => {
    if (!provider || !symbol || !dataType || !interval) {
      setAllData([])
      setError(null)
      setCurrentPage(1)
      // Notify parent that data is cleared
      if (onDataChange) {
        onDataChange([])
      }
      return
    }

    cancelledRef.current = false

    const fetchData = async () => {
      setLoading(true)
      setError(null)

      try {
        const response = await getOHLCData(provider, symbol, dataType, interval, {
          limit: limit,
          order: 'desc', // Get most recent data first
        })

        if (cancelledRef.current) return

        if (response.bars && response.bars.length > 0) {
          setAllData(response.bars)
          setCurrentPage(1) // Reset to first page
          // Notify parent component of data change
          if (onDataChange && !cancelledRef.current) {
            onDataChange(response.bars)
          }
        } else {
          setAllData([])
          setError('No data available for the selected symbol and interval.')
          // Notify parent that data is empty
          if (onDataChange && !cancelledRef.current) {
            onDataChange([])
          }
        }
      } catch (err) {
        if (!cancelledRef.current) {
          console.error('Error fetching OHLC data:', err)
          setError(err.message || 'Failed to load table data')
          setAllData([])
        }
      } finally {
        if (!cancelledRef.current) {
          setLoading(false)
        }
      }
    }

    fetchData()

    // Cleanup function to cancel request if props change
    return () => {
      cancelledRef.current = true
    }
  }, [provider, symbol, dataType, interval, limit, onDataChange])

  // Handle sort toggle
  const handleSortToggle = () => {
    setSortDirection((prev) => (prev === 'desc' ? 'asc' : 'desc'))
    setCurrentPage(1) // Reset to first page when sorting changes
  }

  // Handle page size change
  const handlePageSizeChange = (e) => {
    const newPageSize = parseInt(e.target.value, 10)
    setPageSize(newPageSize)
    setCurrentPage(1) // Reset to first page
  }

  // Calculate pagination
  const sortedData = getSortedData()
  const totalPages = Math.ceil(sortedData.length / pageSize)
  const startIndex = (currentPage - 1) * pageSize
  const endIndex = startIndex + pageSize
  const currentPageData = sortedData.slice(startIndex, endIndex)

  return (
    <div>
      {/* Page size selector and record count */}
      {!loading && allData.length > 0 && (
        <div className="d-flex justify-content-between align-items-center mb-3">
          <div className="d-flex align-items-center gap-2">
            <label htmlFor="page-size" className="small text-muted mb-0">
              Rows per page:
            </label>
            <CFormSelect
              id="page-size"
              size="sm"
              style={{ width: 'auto' }}
              value={pageSize}
              onChange={handlePageSizeChange}
              aria-label="Select number of rows per page"
            >
              <option value={50}>50</option>
              <option value={75}>75</option>
              <option value={100}>100</option>
            </CFormSelect>
          </div>
          <div className="small text-muted">
            Showing {startIndex + 1}-{Math.min(endIndex, sortedData.length)} of {sortedData.length}{' '}
            records
          </div>
        </div>
      )}

      {/* Table */}
      <div className="table-responsive">
        <CTable striped hover responsive>
          <CTableHead>
            <CTableRow>
              <CTableHeaderCell
                style={{ cursor: 'pointer', userSelect: 'none' }}
                onClick={handleSortToggle}
                aria-sort={sortDirection === 'desc' ? 'descending' : 'ascending'}
              >
                <div className="d-flex align-items-center gap-2">
                  <span>Datetime</span>
                  {sortDirection === 'desc' ? (
                    <CIcon icon={cilArrowBottom} size="sm" />
                  ) : (
                    <CIcon icon={cilArrowTop} size="sm" />
                  )}
                </div>
              </CTableHeaderCell>
              <CTableHeaderCell>Open</CTableHeaderCell>
              <CTableHeaderCell>High</CTableHeaderCell>
              <CTableHeaderCell>Low</CTableHeaderCell>
              <CTableHeaderCell>Close</CTableHeaderCell>
              <CTableHeaderCell>Volume</CTableHeaderCell>
            </CTableRow>
          </CTableHead>
          <CTableBody>
            {loading ? (
              <CTableRow>
                <CTableDataCell colSpan={6} className="text-center py-5">
                  <CSpinner />
                  <div className="mt-2">Loading data...</div>
                </CTableDataCell>
              </CTableRow>
            ) : error ? (
              <CTableRow>
                <CTableDataCell colSpan={6} className="text-center py-5 text-danger">
                  <div>{error}</div>
                </CTableDataCell>
              </CTableRow>
            ) : currentPageData.length === 0 ? (
              <CTableRow>
                <CTableDataCell colSpan={6} className="text-center py-5 text-muted">
                  {!provider || !symbol || !dataType || !interval
                    ? 'Please select a symbol, data type, and interval to view the table.'
                    : 'No data available.'}
                </CTableDataCell>
              </CTableRow>
            ) : (
              currentPageData.map((bar, index) => {
                const priceChange = getPriceChange(bar.open, bar.close)
                const textColor =
                  priceChange === 'up'
                    ? 'text-success'
                    : priceChange === 'down'
                      ? 'text-danger'
                      : 'text-muted'

                return (
                  <CTableRow key={`${bar.time}-${index}`}>
                    <CTableDataCell>{formatTimestamp(bar.time)}</CTableDataCell>
                    <CTableDataCell className={textColor}>{formatPrice(bar.open)}</CTableDataCell>
                    <CTableDataCell className={textColor}>{formatPrice(bar.high)}</CTableDataCell>
                    <CTableDataCell className={textColor}>{formatPrice(bar.low)}</CTableDataCell>
                    <CTableDataCell className={textColor}>{formatPrice(bar.close)}</CTableDataCell>
                    <CTableDataCell>{formatVolume(bar.volume)}</CTableDataCell>
                  </CTableRow>
                )
              })
            )}
          </CTableBody>
        </CTable>
      </div>

      {/* Pagination controls */}
      {!loading && totalPages > 1 && (
        <CRow className="mt-3">
          <CCol className="d-flex justify-content-center">
            <CSmartPagination
              activePage={currentPage}
              pages={totalPages}
              onActivePageChange={setCurrentPage}
            />
          </CCol>
        </CRow>
      )}
    </div>
  )
}

OHLCTable.propTypes = {
  provider: PropTypes.string,
  symbol: PropTypes.string,
  dataType: PropTypes.oneOf(['historical', 'live']),
  interval: PropTypes.string,
  limit: PropTypes.number,
  onDataChange: PropTypes.func,
}

OHLCTable.defaultProps = {
  provider: null,
  symbol: null,
  dataType: null,
  interval: null,
  limit: 5000,
}

export default OHLCTable
