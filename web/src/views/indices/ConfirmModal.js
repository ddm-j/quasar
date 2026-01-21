import React from 'react'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilWarning } from '@coreui/icons'

const ConfirmModal = ({
  visible,
  onClose,
  onConfirm,
  title = 'Confirm',
  message,
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  confirmColor = 'primary',
}) => {
  return (
    <CModal visible={visible} onClose={onClose} backdrop="static" size="sm">
      <CModalHeader onClose={onClose}>
        <CModalTitle>
          <CIcon icon={cilWarning} className="me-2 text-warning" />
          {title}
        </CModalTitle>
      </CModalHeader>
      <CModalBody>{message}</CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={onClose}>
          {cancelLabel}
        </CButton>
        <CButton color={confirmColor} onClick={onConfirm}>
          {confirmLabel}
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

export default ConfirmModal
