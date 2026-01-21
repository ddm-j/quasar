import React, { useState, useEffect } from 'react'
import PropTypes from 'prop-types'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CSpinner,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilSync, cilWarning, cilCheckCircle } from '@coreui/icons'
import { getRemapPreview, remapAssetMappings } from '../services/registry_api'

/**
 * RemapConfirmModal - Confirmation modal for re-mapping filtered asset mappings.
 * Shows a preview of the re-map operation impact and allows the user to confirm or cancel.
 */
const RemapConfirmModal = ({
  visible,
  onClose,
  onConfirm,
  providerFilter,
  providerClassType,
  assetClassFilter,
}) => {
  const [loading, setLoading] = useState(false)
  const [preview, setPreview] = useState(null)
  const [previewError, setPreviewError] = useState(null)
  const [remapping, setRemapping] = useState(false)
  const [remapResult, setRemapResult] = useState(null)

  // Fetch preview when modal becomes visible
  useEffect(() => {
    if (!visible) {
      // Reset state when modal closes
      setPreview(null)
      setPreviewError(null)
      setRemapping(false)
      setRemapResult(null)
      return
    }

    const fetchPreview = async () => {
      setLoading(true)
      setPreviewError(null)
      try {
        const params = {}
        if (providerFilter) {
          params.class_name = providerFilter
          params.class_type = providerClassType || 'provider'
        }
        if (assetClassFilter) {
          params.asset_class = assetClassFilter
        }
        const data = await getRemapPreview(params)
        setPreview(data)
      } catch (err) {
        setPreviewError(err.message || 'Failed to fetch preview')
      } finally {
        setLoading(false)
      }
    }

    fetchPreview()
  }, [visible, providerFilter, providerClassType, assetClassFilter])

  // Handle confirm button click - call remapAssetMappings and show result summary
  const handleConfirm = async () => {
    setRemapping(true)
    try {
      const params = {}
      if (providerFilter) {
        params.class_name = providerFilter
        params.class_type = providerClassType || 'provider'
      }
      if (assetClassFilter) {
        params.asset_class = assetClassFilter
      }
      const result = await remapAssetMappings(params)
      // Store result to show completion summary
      setRemapResult(result)
      // Pass the result to parent for toast notification and refresh
      onConfirm(result)
    } catch (err) {
      // Error handling will be added in T039
      console.error('Re-map failed:', err)
    } finally {
      setRemapping(false)
    }
  }

  // Handle closing after viewing result
  const handleClose = () => {
    onClose()
  }

  return (
    <CModal
      visible={visible}
      onClose={handleClose}
      backdrop="static"
      size="lg"
    >
      <CModalHeader>
        <CModalTitle>
          {remapResult ? (
            <>
              <CIcon icon={cilCheckCircle} className="me-2" style={{ color: 'var(--cui-success)' }} />
              Re-map Complete
            </>
          ) : (
            <>
              <CIcon icon={cilSync} className="me-2" />
              Confirm Re-map Operation
            </>
          )}
        </CModalTitle>
      </CModalHeader>
      <CModalBody>
        {/* Completion summary - shown after successful re-map */}
        {remapResult && (
          <div
            className="p-3 rounded mb-3"
            style={{ backgroundColor: 'var(--cui-success-bg-subtle)' }}
          >
            <div className="d-flex align-items-center mb-3">
              <CIcon
                icon={cilCheckCircle}
                size="xl"
                className="me-2"
                style={{ color: 'var(--cui-success)' }}
              />
              <span className="fw-semibold" style={{ color: 'var(--cui-success)' }}>
                Re-map operation completed successfully
              </span>
            </div>
            <div className="row text-center">
              <div className="col-4">
                <div className="fs-3 fw-bold text-danger">{remapResult.deleted_mappings}</div>
                <div className="small text-body-secondary">Deleted</div>
              </div>
              <div className="col-4">
                <div className="fs-3 fw-bold text-success">{remapResult.created_mappings}</div>
                <div className="small text-body-secondary">Created</div>
              </div>
              <div className="col-4">
                <div className="fs-3 fw-bold text-warning">{remapResult.skipped_mappings}</div>
                <div className="small text-body-secondary">Skipped</div>
              </div>
            </div>
            {remapResult.failed_mappings > 0 && (
              <div className="mt-3 text-center">
                <span className="text-danger fw-medium">
                  {remapResult.failed_mappings} mapping{remapResult.failed_mappings !== 1 ? 's' : ''} failed to create
                </span>
              </div>
            )}
            {remapResult.providers_affected && remapResult.providers_affected.length > 0 && (
              <div className="mt-3 small text-body-secondary text-center">
                Providers: {remapResult.providers_affected.join(', ')}
              </div>
            )}
          </div>
        )}

        {/* Preview/confirmation view - hidden after re-map completes */}
        {!remapResult && (
          <>
            <p className="text-body-secondary">
              This will delete existing mappings matching your filters and regenerate them.
            </p>
        <div className="mb-3">
          <strong>Filters Applied:</strong>
          <ul className="mb-0 mt-2">
            {providerFilter && (
              <li>Provider: {providerFilter} ({providerClassType || 'provider'})</li>
            )}
            {assetClassFilter && (
              <li>Asset Class: {assetClassFilter}</li>
            )}
            {!providerFilter && !assetClassFilter && (
              <li className="text-warning">No filters - all mappings will be affected</li>
            )}
          </ul>
        </div>

        {/* Loading state */}
        {loading && (
          <div className="text-center py-3">
            <CSpinner size="sm" className="me-2" />
            <span className="text-body-secondary">Fetching preview...</span>
          </div>
        )}

        {/* Preview error */}
        {previewError && (
          <div className="text-danger mb-3">
            Failed to load preview: {previewError}
          </div>
        )}

        {/* Preview data with filter summary and mappings count */}
        {preview && !loading && (
          <div className="mb-3">
            <strong>Impact Summary:</strong>
            <div
              className="mt-2 p-3 rounded"
              style={{ backgroundColor: 'var(--cui-tertiary-bg)' }}
            >
              {/* Mappings count - prominently displayed */}
              <div className="d-flex align-items-center mb-2">
                <span className="fs-4 fw-semibold text-warning me-2">
                  {preview.mappings_to_delete}
                </span>
                <span className="text-body-secondary">
                  mapping{preview.mappings_to_delete !== 1 ? 's' : ''} will be deleted and regenerated
                </span>
              </div>

              {/* Filter summary from server response */}
              {preview.filter_applied && Object.keys(preview.filter_applied).length > 0 && (
                <div className="small text-body-secondary">
                  <span className="fw-medium">Filters: </span>
                  {preview.filter_applied.class_name && (
                    <span className="me-2">
                      Provider: <span className="text-body">{preview.filter_applied.class_name}</span>
                    </span>
                  )}
                  {preview.filter_applied.asset_class && (
                    <span>
                      Asset Class: <span className="text-body">{preview.filter_applied.asset_class}</span>
                    </span>
                  )}
                </div>
              )}

              {/* Providers affected list */}
              {preview.providers_affected && preview.providers_affected.length > 0 && (
                <div className="mt-2 small text-body-secondary">
                  <span className="fw-medium">Providers affected: </span>
                  <span className="text-body">
                    {preview.providers_affected.join(', ')}
                  </span>
                </div>
              )}

              {/* Affected indices display with warning styling */}
              {preview.affected_indices && preview.affected_indices.length > 0 && (
                <div
                  className="mt-3 p-2 rounded d-flex align-items-start"
                  style={{ backgroundColor: 'var(--cui-warning-bg-subtle)' }}
                >
                  <CIcon
                    icon={cilWarning}
                    className="me-2 flex-shrink-0"
                    style={{ color: 'var(--cui-warning)' }}
                  />
                  <div>
                    <span className="fw-medium" style={{ color: 'var(--cui-warning)' }}>
                      Affected Indices:
                    </span>
                    <ul className="mb-0 mt-1 ps-3">
                      {preview.affected_indices.map((indexName) => (
                        <li key={indexName} className="text-body-secondary">
                          {indexName}
                        </li>
                      ))}
                    </ul>
                    <p className="mb-0 mt-2 small text-body-secondary">
                      These indices reference common symbols that will have their mappings regenerated.
                      Index composition will remain unchanged, but underlying asset mappings may differ.
                    </p>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
          </>
        )}
      </CModalBody>
      <CModalFooter>
        {remapResult ? (
          <CButton color="primary" onClick={handleClose}>
            Close
          </CButton>
        ) : (
          <>
            <CButton color="secondary" onClick={handleClose} disabled={remapping}>
              Cancel
            </CButton>
            <CButton
              color="warning"
              onClick={handleConfirm}
              disabled={loading || previewError || remapping}
            >
              {remapping ? (
                <>
                  <CSpinner size="sm" className="me-2" />
                  Re-mapping...
                </>
              ) : (
                'Confirm Re-map'
              )}
            </CButton>
          </>
        )}
      </CModalFooter>
    </CModal>
  )
}

RemapConfirmModal.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  onConfirm: PropTypes.func.isRequired,
  providerFilter: PropTypes.string,
  providerClassType: PropTypes.string,
  assetClassFilter: PropTypes.string,
}

RemapConfirmModal.defaultProps = {
  providerFilter: '',
  providerClassType: '',
  assetClassFilter: '',
}

export default RemapConfirmModal
