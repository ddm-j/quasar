import React, { useState } from 'react'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CSpinner,
  CAlert,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilWarning, cilTrash } from '@coreui/icons'

import { deleteUserIndex } from '../services/registry_api'

const DeleteIndexModal = ({ visible, onClose, onSuccess, indexName, pushToast }) => {
  const [isDeleting, setIsDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState(null)

  const handleDelete = async () => {
    if (!indexName) return

    setIsDeleting(true)
    setDeleteError(null)

    try {
      await deleteUserIndex(indexName)

      if (pushToast) {
        pushToast({
          title: 'Index Deleted',
          body: `Index "${indexName}" has been deleted.`,
          color: 'success',
        })
      }

      if (onSuccess) {
        onSuccess()
      }

      onClose()
    } catch (err) {
      setDeleteError(err.message || 'Failed to delete index.')
    } finally {
      setIsDeleting(false)
    }
  }

  const handleClose = () => {
    if (!isDeleting) {
      setDeleteError(null)
      onClose()
    }
  }

  return (
    <CModal visible={visible} onClose={handleClose} backdrop="static">
      <CModalHeader onClose={handleClose}>
        <CModalTitle className="text-danger">
          <CIcon icon={cilTrash} className="me-2" />
          Delete Index
        </CModalTitle>
      </CModalHeader>

      <CModalBody>
        {deleteError && (
          <CAlert color="danger" className="d-flex align-items-center">
            <CIcon icon={cilWarning} className="me-2" />
            {deleteError}
          </CAlert>
        )}

        <p>
          Are you sure you want to delete the index <strong>&quot;{indexName}&quot;</strong>?
        </p>
        <p className="text-danger mb-0">
          <CIcon icon={cilWarning} className="me-1" />
          This action cannot be undone. All members of this index will be removed.
        </p>
      </CModalBody>

      <CModalFooter>
        <CButton color="secondary" onClick={handleClose} disabled={isDeleting}>
          Cancel
        </CButton>
        <CButton color="danger" onClick={handleDelete} disabled={isDeleting}>
          {isDeleting ? (
            <>
              <CSpinner size="sm" className="me-1" />
              Deleting...
            </>
          ) : (
            <>
              <CIcon icon={cilTrash} className="me-1" />
              Delete Index
            </>
          )}
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

export default DeleteIndexModal
