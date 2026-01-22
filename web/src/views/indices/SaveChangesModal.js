import React from 'react'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CSpinner,
  CAlert,
  CBadge,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilCheckCircle, cilWarning, cilPlus, cilMinus, cilPencil } from '@coreui/icons'
import { formatWeight } from '../../utils/formatting'

const SaveChangesModal = ({ visible, onClose, onConfirm, isSaving, changesSummary, indexName }) => {
  const { added = [], removed = [], weightChanges = [], totalWeight = 0 } = changesSummary || {}

  const hasChanges = added.length > 0 || removed.length > 0 || weightChanges.length > 0
  const totalWeightPercent = (totalWeight * 100).toFixed(1)
  const isWeightValid = Math.abs(totalWeight - 1) < 0.001 // Within 0.1% of 100%

  return (
    <CModal visible={visible} onClose={onClose} backdrop="static">
      <CModalHeader onClose={onClose}>
        <CModalTitle>
          <CIcon icon={cilCheckCircle} className="me-2" />
          Save Changes
        </CModalTitle>
      </CModalHeader>

      <CModalBody>
        <p>
          Save changes to <strong>&quot;{indexName}&quot;</strong>?
        </p>

        {!hasChanges ? (
          <CAlert color="info">No changes to save.</CAlert>
        ) : (
          <>
            {/* Added Members */}
            {added.length > 0 && (
              <div className="mb-3">
                <h6 className="text-success">
                  <CIcon icon={cilPlus} className="me-1" />
                  Members Added ({added.length})
                </h6>
                <ul className="mb-0 ps-4">
                  {added.slice(0, 10).map((symbol) => (
                    <li key={symbol}>{symbol}</li>
                  ))}
                  {added.length > 10 && (
                    <li className="text-muted">...and {added.length - 10} more</li>
                  )}
                </ul>
              </div>
            )}

            {/* Removed Members */}
            {removed.length > 0 && (
              <div className="mb-3">
                <h6 className="text-danger">
                  <CIcon icon={cilMinus} className="me-1" />
                  Members Removed ({removed.length})
                </h6>
                <ul className="mb-0 ps-4">
                  {removed.slice(0, 10).map((symbol) => (
                    <li key={symbol}>{symbol}</li>
                  ))}
                  {removed.length > 10 && (
                    <li className="text-muted">...and {removed.length - 10} more</li>
                  )}
                </ul>
              </div>
            )}

            {/* Weight Changes */}
            {weightChanges.length > 0 && (
              <div className="mb-3">
                <h6 className="text-info">
                  <CIcon icon={cilPencil} className="me-1" />
                  Weights Changed ({weightChanges.length})
                </h6>
                <ul className="mb-0 ps-4">
                  {weightChanges.slice(0, 10).map(({ symbol, old: oldWeight, new: newWeight }) => (
                    <li key={symbol}>
                      {symbol}: {formatWeight(oldWeight)} → {formatWeight(newWeight)}
                    </li>
                  ))}
                  {weightChanges.length > 10 && (
                    <li className="text-muted">...and {weightChanges.length - 10} more</li>
                  )}
                </ul>
              </div>
            )}

            {/* Total Weight Summary */}
            <div className="mt-3 pt-3 border-top">
              <div className="d-flex align-items-center justify-content-between">
                <span className="fw-semibold">Total Weight:</span>
                <span>
                  <CBadge color={isWeightValid ? 'success' : 'warning'}>
                    {totalWeightPercent}%
                  </CBadge>
                </span>
              </div>
              {!isWeightValid && totalWeight > 0 && (
                <CAlert color="warning" className="mt-2 mb-0 py-2">
                  <CIcon icon={cilWarning} className="me-1" />
                  Total weight is not 100%. Consider normalizing weights.
                </CAlert>
              )}
            </div>
          </>
        )}
      </CModalBody>

      <CModalFooter>
        <CButton color="secondary" onClick={onClose} disabled={isSaving}>
          Cancel
        </CButton>
        <CButton color="primary" onClick={onConfirm} disabled={isSaving || !hasChanges}>
          {isSaving ? (
            <>
              <CSpinner size="sm" className="me-1" />
              Saving...
            </>
          ) : (
            'Confirm Save'
          )}
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

export default SaveChangesModal
