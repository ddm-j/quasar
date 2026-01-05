import React, { useState, useEffect } from 'react'
import PropTypes from 'prop-types'
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CFormSelect,
  CFormLabel,
  CRow,
  CCol,
  CSpinner,
  CAlert,
  CNav,
  CNavItem,
  CNavLink,
  CTabContent,
  CTabPane,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilSettings, cilLockLocked, cilChartLine } from '@coreui/icons'
import { getProviderConfig, updateProviderConfig, getAvailableQuoteCurrencies } from '../services/registry_api'

const ProviderConfigModal = ({ visible, onClose, classType, className, displayToast }) => {
  const [activeTab, setActiveTab] = useState('trading')
  const [config, setConfig] = useState({ crypto: { preferred_quote_currency: null } })
  const [availableCurrencies, setAvailableCurrencies] = useState([])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')

  // Load configuration and available currencies when modal opens
  useEffect(() => {
    if (visible && classType && className) {
      loadConfiguration()
      loadAvailableCurrencies()
    }
  }, [visible, classType, className])

  const loadConfiguration = async () => {
    setLoading(true)
    setError('')
    try {
      const response = await getProviderConfig(classType, className)
      setConfig(response.preferences || { crypto: { preferred_quote_currency: null } })
    } catch (err) {
      console.error('Failed to load provider configuration:', err)
      setError(`Failed to load configuration: ${err.message}`)
      // Set default empty config on error
      setConfig({ crypto: { preferred_quote_currency: null } })
    } finally {
      setLoading(false)
    }
  }

  const loadAvailableCurrencies = async () => {
    try {
      const response = await getAvailableQuoteCurrencies(classType, className)
      setAvailableCurrencies(response.available_quote_currencies || [])
    } catch (err) {
      console.error('Failed to load available quote currencies:', err)
      setAvailableCurrencies([])
    }
  }

  const handleSave = async () => {
    setSaving(true)
    setError('')
    try {
      const updateData = {
        crypto: config.crypto
      }
      const response = await updateProviderConfig(classType, className, updateData)

      if (displayToast) {
        displayToast({
          title: 'Configuration Updated',
          body: `Settings for ${className} have been saved successfully.`,
          color: 'success',
          icon: cilSettings,
        })
      }

      // Close modal on success
      handleClose()
    } catch (err) {
      console.error('Failed to update provider configuration:', err)
      setError(`Failed to save configuration: ${err.message}`)
    } finally {
      setSaving(false)
    }
  }

  const handleClose = () => {
    setError('')
    setActiveTab('trading')
    onClose()
  }

  const handlePreferenceChange = (field, value) => {
    setConfig(prevConfig => ({
      ...prevConfig,
      crypto: {
        ...prevConfig.crypto,
        [field]: value || null
      }
    }))
  }

  const hasChanges = () => {
    // Simple change detection - in a real app you'd do deep comparison
    return true // For now, always allow saving
  }

  return (
    <CModal size="lg" visible={visible} onClose={saving ? () => {} : handleClose} backdrop="static">
      <CModalHeader>
        <CModalTitle>
          <CIcon icon={cilSettings} className="me-2" />
          {className} Configuration
        </CModalTitle>
      </CModalHeader>

      <CModalBody>
        {error && <CAlert color="danger">{error}</CAlert>}

        {loading ? (
          <div className="text-center">
            <CSpinner color="primary" />
            <p className="mt-2">Loading configuration...</p>
          </div>
        ) : (
          <>
            <CNav variant="tabs" className="mb-3">
              <CNavItem>
                <CNavLink
                  active={activeTab === 'trading'}
                  onClick={() => setActiveTab('trading')}
                  className="d-flex align-items-center"
                >
                  <CIcon icon={cilChartLine} className="me-2" />
                  Trading Preferences
                </CNavLink>
              </CNavItem>
              <CNavItem>
                <CNavLink
                  active={activeTab === 'api'}
                  onClick={() => setActiveTab('api')}
                  className="d-flex align-items-center"
                >
                  <CIcon icon={cilLockLocked} className="me-2" />
                  API Secrets
                </CNavLink>
              </CNavItem>
            </CNav>

            <CTabContent>
              <CTabPane visible={activeTab === 'trading'}>
                <h6>Trading Preferences</h6>
                <p className="text-body-secondary mb-3">
                  Configure trading behavior for this provider.
                </p>

                <CRow className="mb-3">
                  <CCol md={6}>
                    <CFormLabel htmlFor="preferredQuoteCurrency">
                      Preferred Quote Currency
                    </CFormLabel>
                    <CFormSelect
                      id="preferredQuoteCurrency"
                      value={config.crypto?.preferred_quote_currency || ''}
                      onChange={(e) => handlePreferenceChange('preferred_quote_currency', e.target.value)}
                      disabled={saving}
                    >
                      <option value="">No preference</option>
                      {availableCurrencies.map(currency => (
                        <option key={currency} value={currency}>
                          {currency}
                        </option>
                      ))}
                    </CFormSelect>
                    <small className="form-text text-body-secondary">
                      For crypto pairs, prefer this quote currency when available (e.g., USDC over USDT).
                      {availableCurrencies.length === 0 && ' No crypto assets found for this provider.'}
                    </small>
                  </CCol>
                </CRow>
              </CTabPane>

              <CTabPane visible={activeTab === 'api'}>
                <h6>API Secrets</h6>
                <p className="text-body-secondary mb-3">
                  API credentials and secrets management.
                </p>

                <CAlert color="info">
                  <CIcon icon={cilSettings} className="me-2" />
                  API secret management will be available in a future update.
                </CAlert>

                <CRow className="mb-3">
                  <CCol>
                    <div className="p-3 border rounded bg-light">
                      <p className="mb-2 fw-semibold">Coming Soon</p>
                      <ul className="mb-0 text-body-secondary">
                        <li>Update API keys and secrets</li>
                        <li>Rotate credentials securely</li>
                        <li>View credential status</li>
                      </ul>
                    </div>
                  </CCol>
                </CRow>
              </CTabPane>
            </CTabContent>
          </>
        )}
      </CModalBody>

      <CModalFooter>
        <CButton color="secondary" onClick={handleClose} disabled={saving}>
          Cancel
        </CButton>
        <CButton
          color="primary"
          onClick={handleSave}
          disabled={saving || loading}
        >
          {saving ? <CSpinner size="sm" className="me-1" /> : null}
          {saving ? 'Saving...' : 'Save Changes'}
        </CButton>
      </CModalFooter>
    </CModal>
  )
}

ProviderConfigModal.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  classType: PropTypes.string.isRequired,
  className: PropTypes.string.isRequired,
  displayToast: PropTypes.func,
}

export default ProviderConfigModal