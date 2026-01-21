import React from 'react'
import PropTypes from 'prop-types'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilSync } from '@coreui/icons'

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
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={onClose}>
          Cancel
        </CButton>
        <CButton color="warning" onClick={onConfirm}>
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
