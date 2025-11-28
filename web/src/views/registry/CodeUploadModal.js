import React, { useState } from 'react'
import PropTypes from 'prop-types'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CRow,
  CCol,
  CSpinner,
  CFormLabel,
  CFormInput,
  CFormSelect, // Import CFormSelect
  CAlert,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilPlus, cilTrash, cilCloudUpload, cilFile } from '@coreui/icons'

const CodeUploadModal = ({ visible, onClose, onSubmit, isSubmitting }) => {
  const [selectedFile, setSelectedFile] = useState(null)
  const [fileName, setFileName] = useState('')
  const [classType, setClassType] = useState('provider') // Add state for classType, default to 'provider'
  const [secrets, setSecrets] = useState([{ id: Date.now(), keyName: '', keyValue: '' }])
  const [error, setError] = useState('')

  const handleFileChange = (event) => {
    const file = event.target.files[0]
    if (file) {
      setSelectedFile(file)
      setFileName(file.name)
      setError('')
    }
  }

  const handleAddSecret = () => {
    setSecrets([...secrets, { id: Date.now(), keyName: '', keyValue: '' }])
  }

  const handleRemoveSecret = (id) => {
    setSecrets(secrets.filter((secret) => secret.id !== id))
  }

  const handleSecretChange = (id, field, value) => {
    setSecrets(
      secrets.map((secret) => (secret.id === id ? { ...secret, [field]: value } : secret)),
    )
  }

  const handleSubmit = () => {
    if (!selectedFile) {
      setError('Please select a file to upload.')
      return
    }
    if (!classType) {
      setError('Please select a class type.')
      return
    }
    for (const secret of secrets) {
      if (secret.keyValue && !secret.keyName) {
        setError(`Secret with value "${secret.keyValue.substring(0,20)}..." is missing a key name.`)
        return
      }
    }
    setError('')
    
    // Transform the secrets array into the desired object format
    const secretsObject = secrets.reduce((acc, secret) => {
      if (secret.keyName && secret.keyValue) { // Only include secrets that have both key and value
        acc[secret.keyName] = secret.keyValue;
      }
      return acc;
    }, {});

    onSubmit({ file: selectedFile, secrets: secretsObject, classType }) // Include classType
  }

  const handleCloseModal = () => {
    setSelectedFile(null)
    setFileName('')
    setSecrets([{ id: Date.now(), keyName: '', keyValue: '' }])
    setClassType('provider') // Reset classType
    setError('')
    onClose()
  }

  const handleDragOver = (event) => {
    event.preventDefault()
    event.currentTarget.classList.add('border-primary')
  }
  const handleDragLeave = (event) => {
    event.preventDefault()
    event.currentTarget.classList.remove('border-primary')
  }
  const handleDrop = (event) => {
    event.preventDefault()
    event.currentTarget.classList.remove('border-primary')
    const file = event.dataTransfer.files[0]
    if (file) {
      setSelectedFile(file)
      setFileName(file.name)
      setError('')
    }
  }

  return (
    <CModal size="lg" visible={visible} onClose={isSubmitting ? () => {} : handleCloseModal} backdrop="static">
      <CModalHeader>
        <CModalTitle>Upload Provider or Broker Class</CModalTitle>
      </CModalHeader>
      <CModalBody>
        {error && <CAlert color="danger">{error}</CAlert>}
        <CRow className="mb-3"> {/* Add a row for Class Type selection */}
          <CCol>
            <CFormLabel htmlFor="classTypeSelect">Class Type</CFormLabel>
            <CFormSelect
              id="classTypeSelect"
              value={classType}
              onChange={(e) => setClassType(e.target.value)}
              options={[
                { label: 'Select type...', value: '' },
                { label: 'Data Provider', value: 'provider' },
                { label: 'Broker', value: 'broker' },
                // Add other types if necessary
              ]}
              disabled={isSubmitting}
            />
          </CCol>
        </CRow>
        <CRow>
          {/* Left Side: File Upload */}
          <CCol md={6}>
            <h6>Python File</h6>
            <div
              className="border p-3 text-center"
              style={{ minHeight: '160px', cursor: 'pointer', borderStyle: 'dashed', borderWidth: '2px' }} // Adjusted minHeight
              onClick={() => !isSubmitting && document.getElementById('fileInput')?.click()}
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
            >
              <CFormInput
                type="file"
                id="fileInput"
                className="d-none"
                onChange={handleFileChange}
                accept=".py,.zip"
                disabled={isSubmitting}
              />
              {fileName ? (
                <>
                  <CIcon icon={cilFile} size="3xl" className="mb-2 text-success" />
                  <p>{fileName}</p>
                  {!isSubmitting && (
                    <CButton size="sm" color="link" onClick={() => document.getElementById('fileInput')?.click()}>
                      Change file
                    </CButton>
                  )}
                </>
              ) : (
                <>
                  <CIcon icon={cilCloudUpload} size="3xl" className="mb-2 text-body-secondary" />
                  <p className="text-body-secondary">Drag & drop file here, or click to browse.</p>
                  <small className="text-body-secondary">(.py or .zip files)</small>
                </>
              )}
            </div>
            <small className="form-text text-body-secondary mt-1">
              Upload a Python file (.py)
            </small>
          </CCol>

          {/* Right Side: Secrets Configuration */}
          <CCol md={6}>
            <h6>Secrets (Key-Value Pairs)</h6>
            {secrets.map((secret, index) => (
              <CRow key={secret.id} className="mb-2 align-items-center">
                <CCol xs={5}>
                  {index === 0 && <CFormLabel className="visually-hidden">Key</CFormLabel>}
                  <CFormInput
                    type="text"
                    placeholder="Secret Key"
                    value={secret.keyName}
                    onChange={(e) => handleSecretChange(secret.id, 'keyName', e.target.value)}
                    disabled={isSubmitting}
                  />
                </CCol>
                <CCol xs={5}>
                  {index === 0 && <CFormLabel className="visually-hidden">Value</CFormLabel>}
                  <CFormInput
                    type="password"
                    placeholder="Secret Value"
                    value={secret.keyValue}
                    onChange={(e) => handleSecretChange(secret.id, 'keyValue', e.target.value)}
                    disabled={isSubmitting}
                  />
                </CCol>
                <CCol xs={2} className="text-end">
                  {secrets.length > 1 && !isSubmitting && (
                    <CButton
                      color="danger"
                      variant="outline"
                      size="sm"
                      onClick={() => handleRemoveSecret(secret.id)}
                      title="Remove Secret"
                    >
                      <CIcon icon={cilTrash} />
                    </CButton>
                  )}
                </CCol>
              </CRow>
            ))}
            {!isSubmitting && (
              <CButton color="secondary" variant="outline" size="sm" onClick={handleAddSecret} className="mt-2">
                <CIcon icon={cilPlus} className="me-1" /> Add Secret
              </CButton>
            )}
          </CCol>
        </CRow>
      </CModalBody>
      <CModalFooter>
        <CButton color="secondary" onClick={handleCloseModal} disabled={isSubmitting}>
          Cancel
        </CButton>
        <CButton color="primary" onClick={handleSubmit} disabled={isSubmitting}>
          {isSubmitting ? <CSpinner size="sm" className="me-1" /> : null}
          Upload Code
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

CodeUploadModal.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  onSubmit: PropTypes.func.isRequired,
  isSubmitting: PropTypes.bool, // Prop to indicate if an upload is in progress
}

export default CodeUploadModal