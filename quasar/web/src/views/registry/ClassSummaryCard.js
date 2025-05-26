import React, { useState } from 'react'
import PropTypes from 'prop-types' // For prop type validation
import { 
  CCard,
  CCardBody, 
  CCardHeader, 
  CCardTitle, 
  CSpinner, 
  CButton,
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
 } from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
// Import icons you'll use in this card
import { 
  cilStorage, 
  cilCode, 
  cilCash, 
  cilLibraryBuilding, 
  cilWarning,
  cilTrash,
  cilSync, 
  cilCheckCircle } from '@coreui/icons'
import { updateAssetsForClass, deleteRegisteredClass } from '../services/registry_api'

// Helper to choose an icon based on class_type
const getIconForType = (classType) => {
  if (classType === 'provider') return cilStorage
  if (classType === 'broker') return cilLibraryBuilding
  return cilCode // Default icon
}
const getTypeTextForType = (classType) => {
    if (classType === 'provider') return 'Data Provider'
    if (classType === 'broker') return 'Broker'
    return 'Unknown Type' // Default text
}

const ClassSummaryCard = ({ class_summary, displayToast, onAssetsRefreshed }) => {
    // State to Manage Card Refreshing
    const [isRefreshing, setIsRefreshing] = useState(false)
    const [isDeleting, setIsDeleting] = useState(false)
    const [isDeleteModalVisible, setIsDeleteModalVisible] = useState(false)

    const handleRefresh = async () => {
        setIsRefreshing(true)
        // console.log(`Refreshing assets for: ${class_summary.class_name}`) // Keep for debugging if needed
        try {
            const stats = await updateAssetsForClass(class_summary.class_type, class_summary.class_name)
            // console.log(`Assets refreshed successfully for: ${class_summary.class_name}`, stats) // Keep for debugging

            let toastMessage = `Assets for ${class_summary.class_name} refreshed.`;
            if (stats) {
                toastMessage += ` Added: ${stats.added_symbols || 0}, Updated: ${stats.updated_symbols || 0}, Failed: ${stats.failed_symbols || 0}.`;
            }

            if (displayToast) {
                displayToast({
                    title: 'Assets Refreshed',
                    body: toastMessage,
                    color: 'success',
                    icon: cilCheckCircle,
                });
            }
            if (onAssetsRefreshed) {
                onAssetsRefreshed(); // This will trigger fetchClasses in Registry.js
            }
        } catch (error) {
            console.error(`Error refreshing assets for ${class_summary.class_name}:`, error)
            if (displayToast) {
                displayToast({
                    title: 'Refresh Failed',
                    body: error.message || `Failed to refresh assets for ${class_summary.class_name}.`,
                    color: 'danger',
                    icon: cilWarning,
                });
            }
        } finally {
            setIsRefreshing(false)
        }
    }

  const openDeleteModal = () => {
    setIsDeleteModalVisible(true)
  }

  const closeDeleteModal = () => {
    if (!isDeleting) { // Prevent closing if delete is in progress, or allow it
      setIsDeleteModalVisible(false)
    }
  }

  const handleDeleteConfirm = async () => {
    setIsDeleting(true)
    try {
      const result = await deleteRegisteredClass(class_summary.class_type, class_summary.class_name)
      if (displayToast) {
        displayToast({
          title: 'Deletion Successful',
          body: result.message || `Successfully deleted ${class_summary.class_name}.`,
          color: 'success',
          icon: cilCheckCircle,
        })
      }
      if (onAssetsRefreshed) {
        onAssetsRefreshed() // Refresh the list in Registry.js
      }
      setIsDeleteModalVisible(false) // Close modal on success
    } catch (error) {
      console.error(`Error deleting class ${class_summary.class_name}:`, error)
      if (displayToast) {
        displayToast({
          title: 'Deletion Failed',
          body: error.message || `Failed to delete ${class_summary.class_name}.`,
          color: 'danger',
          icon: cilWarning,
        })
      }
      // Optionally keep modal open on error, or close it:
      // setIsDeleteModalVisible(false);
    } finally {
      setIsDeleting(false)
    }
  }

  return (
    <>
      <CCard className="mb-4 h-100"> {/* h-100 for equal height cards in a row */}
        <CCardHeader className="d-flex justify-content-between align-items-center">
          {/* Card Title - Left Justified */}
          <CCardTitle as="h6" className="mb-0 text-truncate" title={class_summary.class_name}>
            {class_summary.class_name}
          </CCardTitle>

          {/* Card Icon - Right Justified */}
          <div className="d-flex align-items-center">
              <CButton
                  variant="ghost"
                  color="body"
                  size="sm"
                  onClick={handleRefresh}
                  disabled={isRefreshing}
                  className="p-1 me-2"
                  title="Refresh Assets"
              >
                  {isRefreshing ? (
                      <CSpinner size="sm" component="span" aria-hidden="true" color="success" title="Refreshing..."/>
                  ) : (
                      <CIcon icon={cilSync} className="sm" />
                  )}
              </CButton>
              <CButton
                variant="ghost"
                color="danger" // Make it red
                size="sm"
                onClick={openDeleteModal}
                disabled={isRefreshing || isDeleting} // Disable if refreshing or deleting
                className="p-1 me-2"
                title="Delete Class"
              >
                <CIcon icon={cilTrash} className="sm" />
              </CButton>
              <div
              className={`bg-${
                  class_summary.class_type === 'provider' ? 'info' : 'warning'
              } bg-opacity-25 text-${
                  class_summary.class_type === 'provider' ? 'info' : 'warning'
              } rounded p-2 ms-2`}
              >
              <CIcon icon={getIconForType(class_summary.class_type)} size="lg" />
              </div>
          </div>
        </CCardHeader>
        <CCardBody>
          <p className="text-body-secondary mb-1">
            <strong>Type:</strong> {getTypeTextForType(class_summary.class_type)}
          </p>
          <p className="text-body-secondary mb-1">
            <strong>Subtype:</strong> {class_summary.class_subtype || 'N/A'}
          </p>
          <p className="text-body-secondary mb-2">
            <strong>Uploaded:</strong> {new Date(class_summary.uploaded_at).toLocaleString()}
          </p>
          {/* <p className="text-body-secondary mb-2">
            <strong>Uploaded:</strong> {new Date(updatedAt).toLocaleString()}
          </p> */}
          <div className="d-flex align-items-center mt-3">
            <CIcon icon={cilCash} className="me-2 text-body-secondary" />
            <div className="fs-5 fw-semibold">{class_summary.asset_count} Assets</div>
          </div>
        </CCardBody>
      </CCard>
      <CModal visible={isDeleteModalVisible} onClose={closeDeleteModal} alignment="center">
        <CModalHeader onClose={closeDeleteModal}>
          <CModalTitle>Confirm Deletion</CModalTitle>
        </CModalHeader>
        <CModalBody>
          Are you sure you want to delete the class "{class_summary.class_name}" ({class_summary.class_type})?
          This action cannot be undone and will also delete its associated file and asset data.
        </CModalBody>
        <CModalFooter>
          <CButton color="secondary" onClick={closeDeleteModal} disabled={isDeleting}>
            Cancel
          </CButton>
          <CButton color="danger" onClick={handleDeleteConfirm} disabled={isDeleting}>
            {isDeleting ? <CSpinner size="sm" /> : 'Delete'}
          </CButton>
        </CModalFooter>
      </CModal>
    </>
  )
}

ClassSummaryCard.propTypes = {
  class_summary: PropTypes.object.isRequired,
  displayToast: PropTypes.func, // Add prop type for displayToast
  onAssetsRefreshed: PropTypes.func, // Add prop type for onAssetsRefreshed
}

export default ClassSummaryCard