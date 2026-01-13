import React, { useState, useEffect } from 'react';
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CForm,
  CFormInput,
  CFormLabel,
  CFormTextarea,
  CSpinner,
  CAlert,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilWarning } from '@coreui/icons';

import { createUserIndex } from '../services/registry_api';

const CreateIndexModal = ({ visible, onClose, onSuccess, pushToast }) => {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState(null);

  // Reset form when modal opens
  useEffect(() => {
    if (visible) {
      setName('');
      setDescription('');
      setSaveError(null);
    }
  }, [visible]);

  const handleSubmit = async (e) => {
    e.preventDefault();

    // Validate name
    const trimmedName = name.trim();
    if (!trimmedName) {
      setSaveError('Index name is required.');
      return;
    }

    setIsSaving(true);
    setSaveError(null);

    try {
      const data = { name: trimmedName };
      if (description.trim()) {
        data.description = description.trim();
      }

      await createUserIndex(data);

      if (pushToast) {
        pushToast({
          title: 'Index Created',
          body: `Index "${trimmedName}" has been created successfully.`,
          color: 'success',
        });
      }

      if (onSuccess) {
        onSuccess();
      }

      onClose();
    } catch (err) {
      setSaveError(err.message || 'Failed to create index.');
    } finally {
      setIsSaving(false);
    }
  };

  const handleClose = () => {
    if (!isSaving) {
      onClose();
    }
  };

  return (
    <CModal
      visible={visible}
      onClose={handleClose}
      backdrop="static"
    >
      <CModalHeader onClose={handleClose}>
        <CModalTitle>Create New Index</CModalTitle>
      </CModalHeader>

      <CForm onSubmit={handleSubmit}>
        <CModalBody>
          {saveError && (
            <CAlert color="danger" className="d-flex align-items-center">
              <CIcon icon={cilWarning} className="me-2" />
              {saveError}
            </CAlert>
          )}

          <div className="mb-3">
            <CFormLabel htmlFor="indexName">
              Name <span className="text-danger">*</span>
            </CFormLabel>
            <CFormInput
              id="indexName"
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Enter index name"
              disabled={isSaving}
              maxLength={100}
              required
            />
            <div className="form-text">
              A unique name for your index (max 100 characters).
            </div>
          </div>

          <div className="mb-3">
            <CFormLabel htmlFor="indexDescription">Description</CFormLabel>
            <CFormTextarea
              id="indexDescription"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional description"
              disabled={isSaving}
              rows={3}
            />
          </div>
        </CModalBody>

        <CModalFooter>
          <CButton
            color="secondary"
            onClick={handleClose}
            disabled={isSaving}
          >
            Cancel
          </CButton>
          <CButton
            color="primary"
            type="submit"
            disabled={isSaving || !name.trim()}
          >
            {isSaving ? (
              <>
                <CSpinner size="sm" className="me-1" />
                Creating...
              </>
            ) : (
              'Create Index'
            )}
          </CButton>
        </CModalFooter>
      </CForm>
    </CModal>
  );
};

export default CreateIndexModal;
