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
import { cilSync, cilInfo, cilXCircle } from '@coreui/icons'

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
  error,
  onRetry,
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
          {error ? (
            <>
              <CIcon icon={cilXCircle} className="me-2" style={{ color: 'var(--cui-danger)' }} />
              Re-map Failed
            </>
          ) : (
            <>
              <CIcon icon={cilSync} className="me-2" />
              Re-map Crypto Assets?
            </>
          )}
        </CModalTitle>
      </CModalHeader>
      <CModalBody>
        {/* Error display - shown when re-map fails */}
        {error && (
          <div
            className="p-3 rounded mb-3"
            style={{
              backgroundColor: 'var(--cui-danger-bg-subtle)',
              color: 'var(--cui-danger-text-emphasis)',
            }}
          >
            <div className="d-flex align-items-center mb-2">
              <CIcon
                icon={cilXCircle}
                size="xl"
                className="me-2 flex-shrink-0"
                style={{ color: 'var(--cui-danger)' }}
              />
              <span className="fw-semibold" style={{ color: 'var(--cui-danger)' }}>
                Re-map operation failed
              </span>
            </div>
            <p className="mb-0">{error}</p>
            <p className="mb-0 mt-2 small">
              The operation has been rolled back. No mappings were modified.
            </p>
          </div>
        )}

        {/* Normal prompt view - shown when no error */}
        {!error && (
          <div className="d-flex align-items-start mb-3">
            <CIcon icon={cilInfo} className="text-info me-3 mt-1 flex-shrink-0" size="lg" />
            <div>
              <p className="mb-2">
                You changed the preferred quote currency for <strong>{className}</strong>.
              </p>
              <p className="mb-0 text-body-secondary">
                Would you like to re-map crypto asset mappings to use the new quote currency
                preference? This will delete existing crypto mappings and regenerate them with the
                new preference.
              </p>
            </div>
          </div>
        )}
      </CModalBody>
      <CModalFooter>
        {error ? (
          <>
            <CButton color="secondary" onClick={handleDecline} disabled={isProcessing}>
              Close
            </CButton>
            <CButton color="warning" onClick={onRetry || handleConfirm} disabled={isProcessing}>
              {isProcessing ? (
                <>
                  <CSpinner size="sm" className="me-2" />
                  Retrying...
                </>
              ) : (
                <>
                  <CIcon icon={cilSync} className="me-2" />
                  Retry
                </>
              )}
            </CButton>
          </>
        ) : (
          <>
            <CButton color="secondary" onClick={handleDecline} disabled={isProcessing}>
              No, Later
            </CButton>
            <CButton color="primary" onClick={handleConfirm} disabled={isProcessing}>
              {isProcessing ? <CSpinner size="sm" className="me-1" /> : null}
              {isProcessing ? 'Re-mapping...' : 'Yes, Re-map Now'}
            </CButton>
          </>
        )}
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
  error: PropTypes.string,
  onRetry: PropTypes.func,
}

RemapPromptModal.defaultProps = {
  onDecline: null,
  isProcessing: false,
  error: null,
  onRetry: null,
}

export default RemapPromptModal
