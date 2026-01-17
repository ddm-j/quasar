import React, { useState, useEffect } from 'react';
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CForm,
  CFormLabel,
  CFormInput,
  CSpinner,
  CAlert,
} from '@coreui/react-pro';

import { renameCommonSymbol } from '../services/registry_api';

const CommonSymbolRenameModal = ({ visible, onClose, onSuccess, commonSymbol }) => {
  const [newSymbol, setNewSymbol] = useState('');
  const [isRenaming, setIsRenaming] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (visible) {
      setNewSymbol('');
      setError(null);
    }
  }, [visible]);

  const handleRename = async () => {
    const trimmedNewSymbol = newSymbol.trim();

    if (!trimmedNewSymbol) {
      setError('New symbol name cannot be empty.');
      return;
    }

    if (trimmedNewSymbol === commonSymbol) {
      setError('New symbol name must be different from the current name.');
      return;
    }

    setIsRenaming(true);
    setError(null);

    try {
      const result = await renameCommonSymbol(commonSymbol, trimmedNewSymbol);
      if (onSuccess) {
        onSuccess(result);
      }
      onClose();
    } catch (err) {
      setError(err.message || 'Failed to rename symbol. Please try again.');
    } finally {
      setIsRenaming(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !isRenaming) {
      e.preventDefault();
      handleRename();
    }
  };

  return (
    <CModal visible={visible} onClose={onClose} backdrop="static">
      <CModalHeader onClose={onClose}>
        <CModalTitle>Rename Common Symbol</CModalTitle>
      </CModalHeader>
      <CForm onSubmit={(e) => e.preventDefault()}>
        <CModalBody>
          <div className="mb-3">
            <CFormLabel>Current Symbol</CFormLabel>
            <CFormInput type="text" value={commonSymbol || ''} disabled readOnly />
          </div>
          <div className="mb-3">
            <CFormLabel htmlFor="newSymbol">New Symbol</CFormLabel>
            <CFormInput
              id="newSymbol"
              type="text"
              value={newSymbol}
              onChange={(e) => setNewSymbol(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Enter new symbol name"
              disabled={isRenaming}
              autoFocus
            />
          </div>
          {error && (
            <CAlert color="danger" className="mb-0">
              {error}
            </CAlert>
          )}
        </CModalBody>
        <CModalFooter>
          <CButton color="secondary" onClick={onClose} disabled={isRenaming}>
            Cancel
          </CButton>
          <CButton
            color="primary"
            onClick={handleRename}
            disabled={isRenaming || !newSymbol.trim()}
          >
            {isRenaming ? (
              <>
                <CSpinner size="sm" className="me-2" />
                Renaming...
              </>
            ) : (
              'Rename'
            )}
          </CButton>
        </CModalFooter>
      </CForm>
    </CModal>
  );
};

export default CommonSymbolRenameModal;
