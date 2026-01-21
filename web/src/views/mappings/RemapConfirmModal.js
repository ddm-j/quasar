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
import { cilSync } from '@coreui/icons'
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

  // Fetch preview when modal becomes visible
  useEffect(() => {
    if (!visible) {
      // Reset state when modal closes
      setPreview(null)
      setPreviewError(null)
      setRemapping(false)
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

  // Handle confirm button click - call remapAssetMappings and close modal on success
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
      // Pass the result to parent and close the modal
      onConfirm(result)
      onClose()
    } catch (err) {
      // Error handling will be added in T039
      console.error('Re-map failed:', err)
    } finally {
      setRemapping(false)
    }
  }

  return (
    <CModal
      visible={visible}
      onClose={onClose}
      backdrop="static"
      size="lg"
    >
      <CModalHeader>
        <CModalTitle>
          <CIcon icon={cilSync} className="me-2" />
          Confirm Re-map Operation
        </CModalTitle>
      </CModalHeader>
      <CModalBody>
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
            </div>
          </div>
        )}
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={onClose} disabled={remapping}>
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
