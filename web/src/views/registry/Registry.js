import { React, useState, useEffect, useRef } from 'react'
import CIcon from '@coreui/icons-react'
import { 
  CCard, 
  CCardBody, 
  CCardHeader, 
  CCol, 
  CRow, 
  CSpinner, 
  CButton, 
  CToaster, // For toast notifications
  CToast,
  CToastHeader,
  CToastBody } from '@coreui/react-pro'
import { cilPlus, cilSync, cilLoopCircular, cilCheckCircle, cilWarning } from '@coreui/icons'
import ClassSummaryCard from './ClassSummaryCard'
import CodeUploadModal from './CodeUploadModal'
import { getRegisteredClasses, uploadCode, updateAllAssets } from '../services/registry_api'

const Registry = () => {
  const [classes, setClasses] = useState([])
  const [loading, setLoading] = useState(true)
  const [uploading, setUploading] = useState(false) // Specific loading for upload action
  const [refreshingAll, setRefreshingAll] = useState(false); // New state for "refresh all"
  const [error, setError] = useState(null)
  const [isUploadModalVisible, setIsUploadModalVisible] = useState(false)
  const [toastToShow, setToastToShow] = useState(null) // New state for the toast element
  const toaster = useRef(null) // Ref for CToaster

  const fetchClasses = async () => {
    try {
      const response = await getRegisteredClasses()
      setClasses(response)
      console.log('Registered classes:', response)
    } catch (error) {
      setError(null)
      console.error('Error fetching registered classes:', error)
      setError("Failed to fetch registered classes.")
      setClasses([])
    } finally {
      setLoading(false)
    }
  }

  // Fetch registered classes on component mount
  useEffect(() => {
    fetchClasses()
  }, [])

    // Helper function to add toasts
  const displayToast = (toastConfig) => {
    const newToast = (
      <CToast
        autohide={false}
        delay={5000}
        color={toastConfig.color || 'info'}
        // The 'visible' prop is managed by CToaster when using 'push'
      >
        <CToastHeader closeButton>
          {toastConfig.icon && <CIcon icon={toastConfig.icon} className="me-2" />}
          <strong className="me-auto">{toastConfig.title}</strong>
        </CToastHeader>
        <CToastBody>{toastConfig.body}</CToastBody>
      </CToast>
    )
    setToastToShow(newToast)
  }

  const handleRefreshComponents = () => {
    console.log('Refreshing components (re-fetching classes)...')
    fetchClasses()
  }

  const handleRefreshAllAssets = async () => {
    setRefreshingAll(true);
    displayToast({ title: 'Refreshing All Assets', body: 'Starting update for all registered classes...', color: 'info' });
    try {
      const results = await updateAllAssets(); // This returns a list of stats or a single message
      // console.log('Refresh all assets successful:', results); // For debugging

      // Process the results to give a summary toast
      // The backend returns a list of stats for each provider, or a single message if no providers.
      let successCount = 0;
      let failCount = 0;
      let totalAdded = 0;
      let totalUpdated = 0;
      let messageBody = '';

      if (Array.isArray(results)) {
        results.forEach(stat => {
          if (stat.status === 200 || stat.status === 204) { // 204 for "no symbols" is a success for that provider
            successCount++;
            totalAdded += stat.added_symbols || 0;
            totalUpdated += stat.updated_symbols || 0;
          } else {
            failCount++;
          }
        });
        messageBody = `${successCount} classes processed successfully, ${failCount} failed. Total assets added: ${totalAdded}, updated: ${totalUpdated}.`;
      } else if (results && results.message) { // Handle cases like "No registered providers found."
         messageBody = results.message;
      } else {
        messageBody = 'Asset refresh process completed.';
      }

      displayToast({
        title: 'Refresh All Complete',
        body: messageBody,
        color: failCount > 0 ? 'warning' : 'success',
        icon: failCount > 0 ? cilWarning : cilCheckCircle,
      });
      fetchClasses(); // Refresh the list to update counts on cards
    } catch (err) {
      console.error('Error refreshing all assets:', err);
      displayToast({
        title: 'Refresh All Failed',
        body: err.message || 'An error occurred while refreshing all assets.',
        color: 'danger',
        icon: cilWarning,
      });
    } finally {
      setRefreshingAll(false);
    }
  }

  const handleAddCode = () => {
    setIsUploadModalVisible(true) // Open the modal
  }

  const handleUploadModalClose = () => {
    if (!uploading) { // Prevent closing if an upload is in progress
      setIsUploadModalVisible(false)
    }
  }

  // Updated handleUploadSubmit
  const handleUploadSubmit = async ({ file, secrets, classType }) => {
    if (!file) {
      displayToast({ title: 'Upload Error', body: 'No file selected for upload.', color: 'danger', icon: cilWarning }); // Use displayToast
      return;
    }
    if (!classType) {
      displayToast({ title: 'Upload Error', body: 'No class type selected.', color: 'danger', icon: cilWarning }); // Use displayToast
      return;
    }

    setUploading(true)
    console.log(`Submitting code upload for ${classType}:`, { fileName: file.name, secretsCount: secrets.length });
    console.log('Secrets:', secrets)

    try {
      const responseData = await uploadCode(classType, file, secrets)
      console.log('Upload successful:', responseData)
      displayToast({ // Use displayToast
        title: 'Upload Success',
        body: responseData.message || responseData.status || 'Code uploaded successfully!',
        color: 'success',
        icon: cilCheckCircle,
      })
      fetchClasses()
      setIsUploadModalVisible(false)
    } catch (uploadError) {
      console.error('Upload failed:', uploadError)
      displayToast({ // Use displayToast
        title: 'Upload Failed',
        body: uploadError.message || 'An error occurred during upload.',
        color: 'danger',
        icon: cilWarning,
      })
    } finally {
      setUploading(false)
    }
  }

  return (
    <>
      <CToaster ref={toaster} push={toastToShow} placement="top-end" />

      <CRow className="g-4">
        <CCol xs={12} md={12} lg={12}>
          <CCard>
            <CCardHeader className="d-flex justify-content-between align-items-center">
              <h5>Registered Code</h5>
              <div className="d-flex align-items-center">
                <CButton
                  variant="ghost"
                  color="body"
                  size="sm"
                  onClick={handleAddCode}
                  className="p-1 me-2"
                  title="Add new code"
                  disabled={uploading || loading || refreshingAll}
                >
                  <CIcon icon={cilPlus} size="lg" />
                </CButton>
                <CButton
                  variant="ghost"
                  color="body"
                  size="sm"
                  onClick={handleRefreshAllAssets}
                  className="p-1 me-2"
                  title="Refresh all assets"
                  disabled={uploading || loading || refreshingAll}
                >
                  <CIcon icon={cilSync} size="lg" />
                </CButton>
                <CButton
                  variant="ghost"
                  color="body"
                  size="sm"
                  onClick={handleRefreshComponents}
                  className="p-1"
                  title="Refresh component list"
                  disabled={uploading || loading || refreshingAll}
                >
                  <CIcon icon={cilLoopCircular} size="lg" />
                </CButton>
              </div>
            </CCardHeader>
            <CCardBody>
              {loading ? (
                <div className="text-center">
                  <CSpinner color="primary" />
                </div>
              ) : error ? (
                <div className="text-danger text-center">{error}</div>
              ) : classes.length === 0 ? (
                <div className="text-center">No registered code found.</div>
              ) : (
                <CRow xs={{ cols: 1 }} sm={{ cols: 1 }} md={{ cols: 2 }} lg={{ cols: 3 }} xl={{ cols: 3 }} xxl={{cols: 4}} className="g-4">                {classes.map((class_summary_item) => (
                    // Each ClassSummaryCard is wrapped in its own CCol for grid layout
                    <CCol key={class_summary_item.class_name || class_summary_item.id /* Ensure unique key */}>
                      <ClassSummaryCard 
                        class_summary={class_summary_item}
                        displayToast={displayToast}
                        onAssetsRefreshed={fetchClasses} // Callback to refresh classes
                       />
                    </CCol>
                  ))}
                </CRow>
              )}
            </CCardBody>
          </CCard>
        </CCol>
      </CRow>
      <CodeUploadModal
        visible={isUploadModalVisible}
        onClose={handleUploadModalClose}
        onSubmit={handleUploadSubmit}
        isSubmitting={uploading}
      />
    </>
  )
}

export default Registry