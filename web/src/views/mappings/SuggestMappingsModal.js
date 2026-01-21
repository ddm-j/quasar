import React, { useState, useEffect } from 'react'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CRow,
  CCol,
  CFormLabel,
  CFormSelect,
  CFormInput,
  CSpinner,
  CAlert,
  CTable,
  CTableHead,
  CTableRow,
  CTableHeaderCell,
  CTableBody,
  CTableDataCell,
  CBadge,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilArrowRight, cilWarning, cilLink } from '@coreui/icons'
import {
  getRegisteredClasses,
  getAssetMappingSuggestions,
  createAssetMapping,
} from '../services/registry_api'

const SuggestMappingsModal = ({ visible, onClose, onSuccess, pushToast }) => {
  // Dropdown selections
  const [sourceClass, setSourceClass] = useState('')
  const [targetClass, setTargetClass] = useState('')

  // Class data from API
  const [classData, setClassData] = useState([])
  const [isLoadingClasses, setIsLoadingClasses] = useState(false)
  const [errorClasses, setErrorClasses] = useState(null)

  // Suggestions data
  const [suggestions, setSuggestions] = useState([])
  const [isLoadingSuggestions, setIsLoadingSuggestions] = useState(false)
  const [errorSuggestions, setErrorSuggestions] = useState(null)

  // Pagination
  const [nextCursor, setNextCursor] = useState(null)
  const [hasMore, setHasMore] = useState(false)
  const [totalCount, setTotalCount] = useState(null)

  // Editable common symbols and creation state
  const [editedSymbols, setEditedSymbols] = useState({})
  const [creatingRows, setCreatingRows] = useState({})
  const [createdRows, setCreatedRows] = useState({})

  // Search and filter state
  const [searchTerm, setSearchTerm] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [minScoreFilter, setMinScoreFilter] = useState(30)
  const [rowErrors, setRowErrors] = useState({})

  // Helper function for score badge styling with proper contrast
  const getScoreBadgeStyle = (score) => {
    if (score >= 70) {
      return { color: 'success', textColor: 'white' } // Green - white text
    }
    if (score >= 50) {
      return { color: 'warning', textColor: 'dark' } // Yellow/Orange - dark text
    }
    return { color: 'secondary', textColor: 'white' } // Grey - white text is fine for secondary
  }

  // Helper for ID match badge styling
  const getIdMatchBadgeStyle = (isMatch) => {
    if (isMatch) {
      return { color: 'success', textColor: 'white' } // Green - white text
    }
    return { color: 'light', textColor: 'dark' } // Light grey - dark text for contrast
  }

  // Debounce search input (400ms)
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchTerm)
    }, 400)
    return () => clearTimeout(timer)
  }, [searchTerm])

  // Fetch classes when modal opens
  useEffect(() => {
    if (visible) {
      // Reset all state when modal opens
      setSourceClass('')
      setTargetClass('')
      setErrorClasses(null)
      setClassData([])
      // Reset suggestions state
      setSuggestions([])
      setNextCursor(null)
      setHasMore(false)
      setTotalCount(null)
      setErrorSuggestions(null)
      // Reset editing state
      setEditedSymbols({})
      setCreatingRows({})
      setCreatedRows({})
      // Reset filters and errors
      setSearchTerm('')
      setDebouncedSearch('')
      setMinScoreFilter(30)
      setRowErrors({})

      const fetchClasses = async () => {
        setIsLoadingClasses(true)
        try {
          const data = await getRegisteredClasses()
          setClassData(data || [])
        } catch (error) {
          console.error('Error fetching classes:', error)
          setErrorClasses(error.message)
        } finally {
          setIsLoadingClasses(false)
        }
      }
      fetchClasses()
    }
  }, [visible])

  // Fetch suggestions when source, target, or filters change
  useEffect(() => {
    if (sourceClass && targetClass) {
      // Reset suggestions state for new query
      setSuggestions([])
      setNextCursor(null)
      setHasMore(false)
      setTotalCount(null)
      setErrorSuggestions(null)
      // Reset editing state
      setEditedSymbols({})
      setCreatingRows({})
      setCreatedRows({})
      setRowErrors({})

      fetchSuggestions()
    }
  }, [sourceClass, targetClass, debouncedSearch, minScoreFilter])

  const fetchSuggestions = async (cursor = null) => {
    setIsLoadingSuggestions(true)
    setErrorSuggestions(null)

    try {
      const params = {
        source_class: sourceClass,
        target_class: targetClass,
        limit: 50,
        min_score: minScoreFilter,
        include_total: cursor === null, // Only get total on first fetch
      }
      if (cursor) {
        params.cursor = cursor
      }
      if (debouncedSearch) {
        params.search = debouncedSearch
      }

      const data = await getAssetMappingSuggestions(params)

      if (cursor) {
        // Append to existing suggestions
        setSuggestions((prev) => [...prev, ...data.items])
      } else {
        // Replace suggestions (first load)
        setSuggestions(data.items || [])
        if (data.total !== null && data.total !== undefined) {
          setTotalCount(data.total)
        }
      }

      setNextCursor(data.next_cursor)
      setHasMore(data.has_more)
    } catch (error) {
      console.error('Error fetching suggestions:', error)
      setErrorSuggestions(error.message)
    } finally {
      setIsLoadingSuggestions(false)
    }
  }

  const handleLoadMore = () => {
    if (nextCursor && !isLoadingSuggestions) {
      fetchSuggestions(nextCursor)
    }
  }

  // Handle source change - reset target if it matches new source
  const handleSourceChange = (e) => {
    const newSource = e.target.value
    setSourceClass(newSource)
    // Reset target if it was the same as new source
    if (targetClass === newSource) {
      setTargetClass('')
    }
    // Reset filters when source changes
    setSearchTerm('')
    setMinScoreFilter(30)
    setRowErrors({})
  }

  // Handle target change
  const handleTargetChange = (e) => {
    setTargetClass(e.target.value)
    // Reset filters when target changes
    setSearchTerm('')
    setMinScoreFilter(30)
    setRowErrors({})
  }

  // Get the common symbol for a row (edited value or proposed value)
  const getCommonSymbol = (item, index) => {
    return editedSymbols[index] !== undefined ? editedSymbols[index] : item.proposed_common_symbol
  }

  const isPairCompletion = (item) => {
    return Boolean(
      item.target_already_mapped &&
        item.target_common_symbol &&
        item.proposed_common_symbol === item.target_common_symbol,
    )
  }

  const isConflict = (item) => {
    return Boolean(item.target_already_mapped && !isPairCompletion(item))
  }

  // Handle common symbol editing
  const handleSymbolChange = (index, value) => {
    setEditedSymbols((prev) => ({ ...prev, [index]: value }))
    // Clear any existing error for this row
    if (rowErrors[index] || createdRows[index]) {
      setRowErrors((prev) => {
        const newErrors = { ...prev }
        delete newErrors[index]
        return newErrors
      })
      // Reset created flag on edit to avoid stale badge
      setCreatedRows((prev) => {
        const newCreated = { ...prev }
        delete newCreated[index]
        return newCreated
      })
    }
  }

  // Handle creating mappings for a suggestion row
  const handleCreateMapping = async (item, index) => {
    const commonSymbol = getCommonSymbol(item, index)
    if (!commonSymbol?.trim()) {
      return
    }

    if (isConflict(item)) {
      const context = `${item.target_class}/${item.target_symbol}`
      const message = `Target already mapped to a different symbol (${item.target_common_symbol || 'unknown'}).`
      if (pushToast) {
        pushToast({
          title: 'Cannot create mapping',
          body: `${context}: ${message}`,
          color: 'warning',
        })
      }
      return
    }

    const pairCompletion = isPairCompletion(item)

    setCreatingRows((prev) => ({ ...prev, [index]: true }))

    try {
      const payload = [
        {
          common_symbol: commonSymbol.trim(),
          class_name: item.source_class,
          class_type: item.source_type,
          class_symbol: item.source_symbol,
          is_active: true,
        },
      ]

      // Only add target mapping when it isn't already mapped to the same common_symbol
      if (!pairCompletion) {
        payload.push({
          common_symbol: commonSymbol.trim(),
          class_name: item.target_class,
          class_type: item.target_type,
          class_symbol: item.target_symbol,
          is_active: true,
        })
      }

      await createAssetMapping(payload)

      // Mark as created only when both succeed
      setCreatedRows((prev) => ({ ...prev, [index]: true }))

      // Call onSuccess to refresh parent data if provided
      if (onSuccess) {
        onSuccess()
      }
    } catch (error) {
      console.error('Error creating mapping:', error)
      const context = `${item.source_class}/${item.source_symbol} → ${item.target_class}/${item.target_symbol}`
      if (pushToast) {
        pushToast({
          title: 'Create mappings failed',
          body: `${context}: ${error.message || 'Failed to create mappings.'}`,
          color: 'danger',
        })
      }
    } finally {
      setCreatingRows((prev) => ({ ...prev, [index]: false }))
    }
  }

  return (
    <CModal
      visible={visible}
      onClose={onClose}
      backdrop="static"
      fullscreen="lg"
      size="xl"
      className="suggest-mappings-modal"
      scrollable
    >
      <CModalHeader onClose={onClose}>
        <CModalTitle>Suggest Mappings</CModalTitle>
      </CModalHeader>
      <CModalBody>
        {/* Error Alert for Classes */}
        {errorClasses && (
          <CAlert color="danger" className="mb-3">
            {errorClasses}
          </CAlert>
        )}

        {/* Loading Spinner for Classes */}
        {isLoadingClasses ? (
          <div className="text-center py-4">
            <CSpinner color="primary" />
            <p className="mt-2 text-muted">Loading providers...</p>
          </div>
        ) : (
          <>
            {/* Source/Target Selection Row */}
            <CRow className="mb-4 align-items-end">
              <CCol md={5}>
                <CFormLabel htmlFor="source_class">Source Provider/Broker</CFormLabel>
                <CFormSelect id="source_class" value={sourceClass} onChange={handleSourceChange}>
                  <option value="" disabled>
                    Select source...
                  </option>
                  {classData.map((cls) => (
                    <option key={cls.class_name} value={cls.class_name}>
                      {cls.class_name} ({cls.class_type})
                    </option>
                  ))}
                </CFormSelect>
              </CCol>

              <CCol md={2} className="text-center pb-2">
                <CIcon icon={cilArrowRight} size="xl" />
              </CCol>

              <CCol md={5}>
                <CFormLabel htmlFor="target_class">Target Provider/Broker</CFormLabel>
                <CFormSelect
                  id="target_class"
                  value={targetClass}
                  onChange={handleTargetChange}
                  disabled={!sourceClass}
                >
                  <option value="" disabled>
                    Select target...
                  </option>
                  {classData
                    .filter((cls) => cls.class_name !== sourceClass)
                    .map((cls) => (
                      <option key={cls.class_name} value={cls.class_name}>
                        {cls.class_name} ({cls.class_type})
                      </option>
                    ))}
                </CFormSelect>
              </CCol>
            </CRow>

            {/* Filter Controls - only show when both source and target selected */}
            {sourceClass && targetClass && (
              <CRow className="mb-3 align-items-end">
                <CCol md={6}>
                  <CFormLabel>Search</CFormLabel>
                  <CFormInput
                    placeholder="Filter by symbol or name..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                  />
                </CCol>
                <CCol md={3}>
                  <CFormLabel>Minimum Confidence</CFormLabel>
                  <CFormSelect
                    value={minScoreFilter}
                    onChange={(e) => setMinScoreFilter(Number(e.target.value))}
                  >
                    <option value={30}>All Matches</option>
                    <option value={50}>Medium+ (50+)</option>
                    <option value={70}>High Only (70+)</option>
                  </CFormSelect>
                </CCol>
              </CRow>
            )}

            {/* Suggestions Section */}
            {sourceClass && targetClass && (
              <>
                {/* Suggestions Error */}
                {errorSuggestions && (
                  <CAlert color="danger" className="mb-3">
                    {errorSuggestions}
                  </CAlert>
                )}

                {/* Loading state for initial fetch */}
                {isLoadingSuggestions && suggestions.length === 0 ? (
                  <div className="text-center py-4">
                    <CSpinner color="primary" />
                    <p className="mt-2 text-muted">Loading suggestions...</p>
                  </div>
                ) : (
                  <>
                    {/* Results count */}
                    {totalCount !== null && (
                      <p className="text-muted mb-2">
                        Showing {suggestions.length} of {totalCount} suggestions
                      </p>
                    )}

                    {/* Suggestions Table */}
                    {suggestions.length > 0 ? (
                      <>
                        <CTable striped hover responsive>
                          <CTableHead>
                            <CTableRow>
                              <CTableHeaderCell style={{ width: '12%' }}>
                                Source Symbol
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '15%' }}>
                                Source Name
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '12%' }}>
                                Target Symbol
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '15%' }}>
                                Target Name
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '8%' }} className="text-center">
                                ID Match
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '8%' }} className="text-center">
                                Score
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '18%' }}>
                                Common Symbol
                              </CTableHeaderCell>
                              <CTableHeaderCell style={{ width: '12%' }} className="text-center">
                                Actions
                              </CTableHeaderCell>
                            </CTableRow>
                          </CTableHead>
                          <CTableBody>
                            {suggestions.map((item, index) => {
                              const idMatchStyle = getIdMatchBadgeStyle(item.id_match)
                              const scoreStyle = getScoreBadgeStyle(item.score)
                              const isCreating = creatingRows[index]
                              const isCreated = createdRows[index]
                              const commonSymbol = getCommonSymbol(item, index)
                              const pairCompletion = isPairCompletion(item)
                              const conflict = isConflict(item)
                              return (
                                <CTableRow
                                  key={`${item.source_symbol}-${item.target_symbol}-${index}`}
                                >
                                  <CTableDataCell>{item.source_symbol}</CTableDataCell>
                                  <CTableDataCell>{item.source_name || '-'}</CTableDataCell>
                                  <CTableDataCell>
                                    {item.target_symbol}
                                    {pairCompletion && (
                                      <CIcon
                                        icon={cilLink}
                                        className="text-info ms-1"
                                        size="sm"
                                        title={`Completes existing mapping to ${item.target_common_symbol}`}
                                      />
                                    )}
                                    {conflict && (
                                      <CIcon
                                        icon={cilWarning}
                                        className="text-warning ms-1"
                                        size="sm"
                                        title="Target already mapped to another symbol"
                                      />
                                    )}
                                  </CTableDataCell>
                                  <CTableDataCell>{item.target_name || '-'}</CTableDataCell>
                                  <CTableDataCell className="text-center">
                                    <CBadge
                                      color={idMatchStyle.color}
                                      textColor={idMatchStyle.textColor}
                                    >
                                      {item.id_match ? 'Yes' : 'No'}
                                    </CBadge>
                                  </CTableDataCell>
                                  <CTableDataCell className="text-center">
                                    <CBadge
                                      color={scoreStyle.color}
                                      textColor={scoreStyle.textColor}
                                    >
                                      {item.score.toFixed(1)}
                                    </CBadge>
                                  </CTableDataCell>
                                  <CTableDataCell>
                                    <CFormInput
                                      size="sm"
                                      value={commonSymbol || ''}
                                      onChange={(e) => handleSymbolChange(index, e.target.value)}
                                      disabled={isCreated || isCreating}
                                      placeholder="Enter common symbol..."
                                    />
                                  </CTableDataCell>
                                  <CTableDataCell className="text-center">
                                    {isCreated ? (
                                      <CBadge color="success" textColor="white">
                                        Created
                                      </CBadge>
                                    ) : (
                                      <CButton
                                        size="sm"
                                        color="primary"
                                        onClick={() => handleCreateMapping(item, index)}
                                        disabled={isCreating || !commonSymbol?.trim()}
                                      >
                                        {isCreating ? <CSpinner size="sm" /> : 'Create'}
                                      </CButton>
                                    )}
                                  </CTableDataCell>
                                </CTableRow>
                              )
                            })}
                          </CTableBody>
                        </CTable>

                        {/* Load More Button */}
                        {hasMore && (
                          <div className="text-center mt-3">
                            <CButton
                              color="primary"
                              variant="outline"
                              onClick={handleLoadMore}
                              disabled={isLoadingSuggestions}
                            >
                              {isLoadingSuggestions ? (
                                <>
                                  <CSpinner size="sm" className="me-2" />
                                  Loading...
                                </>
                              ) : (
                                'Load More'
                              )}
                            </CButton>
                          </div>
                        )}
                      </>
                    ) : (
                      !isLoadingSuggestions && (
                        <CAlert color="info">
                          No suggestions found for {sourceClass} → {targetClass}
                        </CAlert>
                      )
                    )}
                  </>
                )}
              </>
            )}
          </>
        )}
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={onClose}>
          Close
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

export default SuggestMappingsModal
