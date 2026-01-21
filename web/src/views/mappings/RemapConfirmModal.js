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
import { getRemapPreview } from '../services/registry_api'

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

  // Fetch preview when modal becomes visible
  useEffect(() => {
    if (!visible) {
      // Reset state when modal closes
      setPreview(null)
      setPreviewError(null)
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

        {/* Preview data placeholder - will be expanded in T030 */}
        {preview && !loading && (
          <div className="mb-3">
            <strong>Preview:</strong>
            <p className="text-body-secondary mt-2 mb-0">
              {preview.mappings_to_delete} mapping(s) will be deleted and regenerated.
            </p>
          </div>
        )}
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={onClose}>
          Cancel
        </CButton>
        <CButton
          color="warning"
          onClick={onConfirm}
          disabled={loading || previewError}
        >
          Confirm Re-map
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
