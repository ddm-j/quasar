import React from 'react'
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
import { cilSync, cilInfo } from '@coreui/icons'

/**
 * RemapPromptModal - A simple Yes/No prompt that appears after changing crypto quote currency.
 * Asks the user if they want to re-map crypto asset mappings to reflect the new preference.
 */
const RemapPromptModal = ({
  visible,
  onClose,
  onConfirm,
  onDecline,
  isProcessing,
  className,
}) => {
  const handleDecline = () => {
    if (onDecline) {
      onDecline()
    }
    onClose()
  }

  const handleConfirm = () => {
    if (onConfirm) {
      onConfirm()
    }
  }

  return (
    <CModal
      visible={visible}
      onClose={isProcessing ? () => {} : handleDecline}
      backdrop="static"
      size="md"
    >
      <CModalHeader>
        <CModalTitle>
          <CIcon icon={cilSync} className="me-2" />
          Re-map Crypto Assets?
        </CModalTitle>
      </CModalHeader>
      <CModalBody>
        <div className="d-flex align-items-start mb-3">
          <CIcon icon={cilInfo} className="text-info me-3 mt-1 flex-shrink-0" size="lg" />
          <div>
            <p className="mb-2">
              You changed the preferred quote currency for <strong>{className}</strong>.
            </p>
            <p className="mb-0 text-body-secondary">
              Would you like to re-map crypto asset mappings to use the new quote currency preference?
              This will delete existing crypto mappings and regenerate them with the new preference.
            </p>
          </div>
        </div>
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={handleDecline} disabled={isProcessing}>
          No, Later
        </CButton>
        <CButton color="primary" onClick={handleConfirm} disabled={isProcessing}>
          {isProcessing ? <CSpinner size="sm" className="me-1" /> : null}
          {isProcessing ? 'Re-mapping...' : 'Yes, Re-map Now'}
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

RemapPromptModal.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  onConfirm: PropTypes.func.isRequired,
  onDecline: PropTypes.func,
  isProcessing: PropTypes.bool,
  className: PropTypes.string.isRequired,
}

RemapPromptModal.defaultProps = {
  onDecline: null,
  isProcessing: false,
}

export default RemapPromptModal
