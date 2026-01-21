import React, { useState, useEffect, useRef } from 'react'
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
  CFormRange,
  CFormCheck,
  CFormInput,
  CInputGroup,
  CInputGroupText,
  CRow,
  CCol,
  CSpinner,
  CAlert,
  CNav,
  CNavItem,
  CNavLink,
  CTabContent,
  CTabPane,
  CBadge,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilSettings, cilLockLocked, cilChartLine, cilClock, cilStorage } from '@coreui/icons'
import { getProviderConfig, updateProviderConfig, getAvailableQuoteCurrencies, getSecretKeys, updateSecrets } from '../services/registry_api'
import RemapPromptModal from './RemapPromptModal'

// Default values for live provider scheduling
const DEFAULT_PRE_CLOSE_SECONDS = 30
const DEFAULT_POST_CLOSE_SECONDS = 5

// Default lookback days for historical providers
const DEFAULT_LOOKBACK_DAYS = 8000

// Default sync frequency for index providers
const DEFAULT_SYNC_FREQUENCY = '1w'

// Sync frequency options for IndexProviders
const SYNC_FREQUENCY_OPTIONS = [
  { value: '1d', label: 'Daily', description: 'Sync at midnight UTC every day' },
  { value: '1w', label: 'Weekly', description: 'Sync at midnight UTC every Monday' },
  { value: '1M', label: 'Monthly', description: 'Sync at midnight UTC on the 1st of each month' },
]

// Preset options for lookback period
const LOOKBACK_PRESETS = [
  { label: '1 month', value: 30 },
  { label: '3 months', value: 90 },
  { label: '1 year', value: 365 },
  { label: '3 years', value: 1095 },
  { label: '5 years', value: 1825 },
  { label: 'Max', value: 8000 },
]

const ProviderConfigModal = ({ visible, onClose, classType, className, classSubtype, displayToast }) => {
  // Determine which tabs should be visible based on class_subtype
  const showSchedulingTab = classSubtype === 'Historical' || classSubtype === 'Live' || classSubtype === 'IndexProvider'
  const showDataTab = classSubtype === 'Historical'

  const [activeTab, setActiveTab] = useState('trading')
  const [config, setConfig] = useState({
    crypto: { preferred_quote_currency: null },
    scheduling: {
      delay_hours: 0,
      pre_close_seconds: DEFAULT_PRE_CLOSE_SECONDS,
      post_close_seconds: DEFAULT_POST_CLOSE_SECONDS,
      sync_frequency: DEFAULT_SYNC_FREQUENCY
    },
    data: {
      lookback_days: DEFAULT_LOOKBACK_DAYS
    }
  })
  const [availableCurrencies, setAvailableCurrencies] = useState([])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')

  // API Secrets state
  const [secretKeys, setSecretKeys] = useState([])
  const [secretValues, setSecretValues] = useState({})
  const [loadingSecrets, setLoadingSecrets] = useState(false)
  const [savingSecrets, setSavingSecrets] = useState(false)
  const [secretsError, setSecretsError] = useState('')
  const [showConfirmDialog, setShowConfirmDialog] = useState(false)

  // Track original quote preference to detect changes for re-map prompt
  const [originalQuotePreference, setOriginalQuotePreference] = useState(null)
  // Control visibility of re-map prompt modal
  const [showRemapPrompt, setShowRemapPrompt] = useState(false)
  // Track if re-map is in progress
  const [isRemapping, setIsRemapping] = useState(false)

  // Ref to track which provider's secret keys have been loaded
  const secretKeysLoadedRef = useRef(null)

  // Load configuration and available currencies when modal opens
  useEffect(() => {
    if (visible && classType && className) {
      // Reset secret keys/values when provider changes to prevent stale data
      setSecretKeys([])
      setSecretValues({})
      secretKeysLoadedRef.current = null
      loadConfiguration()
      loadAvailableCurrencies()
    }
  }, [visible, classType, className])

  // Load secret keys when API tab is activated
  useEffect(() => {
    const providerKey = `${classType}:${className}`
    if (visible && activeTab === 'api' && classType && className && secretKeysLoadedRef.current !== providerKey) {
      secretKeysLoadedRef.current = providerKey
      loadSecretKeys()
    }
  }, [visible, activeTab, classType, className])

  const loadConfiguration = async () => {
    setLoading(true)
    setError('')
    try {
      const response = await getProviderConfig(classType, className)
      const prefs = response.preferences || {}
      const cryptoPrefs = prefs.crypto || { preferred_quote_currency: null }
      setConfig({
        crypto: cryptoPrefs,
        scheduling: {
          delay_hours: prefs.scheduling?.delay_hours ?? 0,
          pre_close_seconds: prefs.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS,
          post_close_seconds: prefs.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS,
          sync_frequency: prefs.scheduling?.sync_frequency ?? DEFAULT_SYNC_FREQUENCY
        },
        data: {
          lookback_days: prefs.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS
        }
      })
      // Track original quote preference for re-map detection
      setOriginalQuotePreference(cryptoPrefs.preferred_quote_currency)
    } catch (err) {
      console.error('Failed to load provider configuration:', err)
      setError(`Failed to load configuration: ${err.message}`)
      // Set default empty config on error
      setConfig({
        crypto: { preferred_quote_currency: null },
        scheduling: {
          delay_hours: 0,
          pre_close_seconds: DEFAULT_PRE_CLOSE_SECONDS,
          post_close_seconds: DEFAULT_POST_CLOSE_SECONDS,
          sync_frequency: DEFAULT_SYNC_FREQUENCY
        },
        data: {
          lookback_days: DEFAULT_LOOKBACK_DAYS
        }
      })
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

  const loadSecretKeys = async () => {
    setLoadingSecrets(true)
    setSecretsError('')
    try {
      const response = await getSecretKeys(classType, className)
      const keys = response.keys || []
      setSecretKeys(keys)
      // Initialize empty values for each key
      const initialValues = {}
      keys.forEach(key => {
        initialValues[key] = ''
      })
      setSecretValues(initialValues)
    } catch (err) {
      console.error('Failed to load secret keys:', err)
      setSecretsError(`Failed to load secret keys: ${err.message}`)
      setSecretKeys([])
    } finally {
      setLoadingSecrets(false)
    }
  }

  const handleSave = async () => {
    setSaving(true)
    setError('')
    try {
      // Build update data based on provider type - only include valid fields for this subtype
      const updateData = {
        crypto: config.crypto,
      }

      // Add scheduling fields based on provider subtype
      if (classSubtype === 'Historical') {
        updateData.scheduling = {
          delay_hours: config.scheduling.delay_hours
        }
        updateData.data = {
          lookback_days: config.data.lookback_days
        }
      } else if (classSubtype === 'Live') {
        updateData.scheduling = {
          pre_close_seconds: config.scheduling.pre_close_seconds,
          post_close_seconds: config.scheduling.post_close_seconds
        }
      } else if (classSubtype === 'IndexProvider') {
        updateData.scheduling = {
          sync_frequency: config.scheduling.sync_frequency
        }
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

      // Check if quote preference changed - if so, prompt for re-map
      if (hasQuotePreferenceChanged()) {
        setShowRemapPrompt(true)
        // Don't close yet - wait for user to respond to re-map prompt
      } else {
        // Close modal on success
        handleClose()
      }
    } catch (err) {
      console.error('Failed to update provider configuration:', err)
      setError(`Failed to save configuration: ${err.message}`)
    } finally {
      setSaving(false)
    }
  }

  const handleClose = () => {
    setError('')
    setSecretsError('')
    setActiveTab('trading')
    setSecretKeys([])
    setSecretValues({})
    setShowConfirmDialog(false)
    setOriginalQuotePreference(null)
    setShowRemapPrompt(false)
    setIsRemapping(false)
    secretKeysLoadedRef.current = null
    onClose()
  }

  const handleSecretChange = (key, value) => {
    setSecretValues(prev => ({
      ...prev,
      [key]: value
    }))
  }

  const handleUpdateSecretsClick = () => {
    // Check if all fields are filled (after trimming whitespace)
    const trimmedValues = {}
    secretKeys.forEach(key => {
      trimmedValues[key] = secretValues[key]?.trim() || ''
    })
    const emptyFields = secretKeys.filter(key => trimmedValues[key] === '')
    if (emptyFields.length > 0) {
      setSecretsError(`Please fill in all credential fields. Missing: ${emptyFields.join(', ')}`)
      return
    }
    // Update secretValues with trimmed values before confirmation
    setSecretValues(trimmedValues)
    // Show confirmation dialog
    setShowConfirmDialog(true)
  }

  const handleConfirmUpdateSecrets = async () => {
    setShowConfirmDialog(false)
    setSavingSecrets(true)
    setSecretsError('')
    try {
      await updateSecrets(classType, className, secretValues)
      if (displayToast) {
        displayToast({
          title: 'Credentials Updated',
          body: `API credentials for ${className} have been updated. The provider will be reloaded with new credentials.`,
          color: 'success',
          icon: cilLockLocked,
        })
      }
      // Clear the values after successful update (for security)
      const clearedValues = {}
      secretKeys.forEach(key => {
        clearedValues[key] = ''
      })
      setSecretValues(clearedValues)
    } catch (err) {
      console.error('Failed to update secrets:', err)
      setSecretsError(`Failed to update credentials: ${err.message}`)
    } finally {
      setSavingSecrets(false)
    }
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

  const handleSchedulingChange = (field, value) => {
    setConfig(prevConfig => ({
      ...prevConfig,
      scheduling: {
        ...prevConfig.scheduling,
        [field]: value
      }
    }))
  }

  const handleDataChange = (field, value) => {
    setConfig(prevConfig => ({
      ...prevConfig,
      data: {
        ...prevConfig.data,
        [field]: value
      }
    }))
  }

  // Helper to format pull time based on delay hours
  const formatPullTime = (delayHours) => {
    const hours = parseInt(delayHours, 10) || 0
    const formattedHour = hours.toString().padStart(2, '0')
    return `${formattedHour}:00 UTC`
  }

  const hasChanges = () => {
    // Simple change detection - in a real app you'd do deep comparison
    return true // For now, always allow saving
  }

  // Check if quote preference changed in a meaningful way (for re-map prompt)
  const hasQuotePreferenceChanged = () => {
    const currentPref = config.crypto?.preferred_quote_currency || null
    // Both null/empty means no change
    if (!originalQuotePreference && !currentPref) return false
    // One is set, other is not, or different values
    return originalQuotePreference !== currentPref
  }

  // Handler when user confirms re-map from the prompt modal
  const handleRemapConfirm = async () => {
    setIsRemapping(true)
    // T023 will wire this to actually call remapAssetMappings()
    // For now, just close and reset
    setIsRemapping(false)
    setShowRemapPrompt(false)
    handleClose()
  }

  // Handler when user declines re-map from the prompt modal
  const handleRemapDecline = () => {
    setShowRemapPrompt(false)
    handleClose()
  }

  // Handler to close the re-map prompt modal
  const handleRemapPromptClose = () => {
    setShowRemapPrompt(false)
    handleClose()
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
              {showSchedulingTab && (
                <CNavItem>
                  <CNavLink
                    active={activeTab === 'scheduling'}
                    onClick={() => setActiveTab('scheduling')}
                    className="d-flex align-items-center"
                  >
                    <CIcon icon={cilClock} className="me-2" />
                    Scheduling
                  </CNavLink>
                </CNavItem>
              )}
              {showDataTab && (
                <CNavItem>
                  <CNavLink
                    active={activeTab === 'data'}
                    onClick={() => setActiveTab('data')}
                    className="d-flex align-items-center"
                  >
                    <CIcon icon={cilStorage} className="me-2" />
                    Data
                  </CNavLink>
                </CNavItem>
              )}
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

              {showSchedulingTab && (
                <CTabPane visible={activeTab === 'scheduling'}>
                  <h6>Scheduling</h6>
                  <p className="text-body-secondary mb-3">
                    Configure when this provider collects data.
                  </p>

                  {classSubtype === 'Historical' && (
                    <>
                      <CRow className="mb-3">
                        <CCol>
                          <CAlert color="info">
                            <CIcon icon={cilClock} className="me-2" />
                            Configure the delay offset for daily data collection.
                            Historical providers pull data at midnight UTC by default.
                          </CAlert>
                        </CCol>
                      </CRow>

                      <CRow className="mb-4">
                        <CCol md={8}>
                          <CFormLabel htmlFor="delayHours">
                            Delay Hours
                          </CFormLabel>
                          <div className="d-flex align-items-center gap-3">
                            <CFormRange
                              id="delayHours"
                              min={0}
                              max={24}
                              step={1}
                              value={config.scheduling?.delay_hours || 0}
                              onChange={(e) => handleSchedulingChange('delay_hours', parseInt(e.target.value, 10))}
                              disabled={saving}
                              className="flex-grow-1"
                            />
                            <CBadge color="primary" className="px-3 py-2" style={{ minWidth: '50px' }}>
                              {config.scheduling?.delay_hours || 0}h
                            </CBadge>
                          </div>
                          <small className="form-text text-body-secondary">
                            Offset data collection from midnight UTC. Range: 0-24 hours.
                          </small>
                        </CCol>
                      </CRow>

                      <CRow className="mb-3">
                        <CCol md={8}>
                          <div className="p-3 border rounded" style={{ backgroundColor: 'var(--cui-body-secondary)' }}>
                            <strong>Pull Time Preview</strong>
                            <p className="mb-0 mt-2">
                              Daily data collection will run at{' '}
                              <CBadge color="success" className="ms-1">
                                {formatPullTime(config.scheduling?.delay_hours || 0)}
                              </CBadge>
                            </p>
                            <small className="text-body-secondary">
                              {config.scheduling?.delay_hours === 0
                                ? 'Data will be pulled immediately at midnight UTC.'
                                : `Data will be pulled ${config.scheduling?.delay_hours} hour${config.scheduling?.delay_hours !== 1 ? 's' : ''} after midnight UTC.`}
                            </small>
                          </div>
                        </CCol>
                      </CRow>
                    </>
                  )}

                  {classSubtype === 'Live' && (
                    <>
                      <CRow className="mb-3">
                        <CCol>
                          <CAlert color="info">
                            <CIcon icon={cilClock} className="me-2" />
                            Configure timing buffers for live data collection around bar close time.
                          </CAlert>
                        </CCol>
                      </CRow>

                      <CRow className="mb-4">
                        <CCol md={8}>
                          <CFormLabel htmlFor="preCloseSeconds">
                            Pre-Close Seconds
                          </CFormLabel>
                          <div className="d-flex align-items-center gap-3">
                            <CFormRange
                              id="preCloseSeconds"
                              min={0}
                              max={300}
                              step={5}
                              value={config.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS}
                              onChange={(e) => handleSchedulingChange('pre_close_seconds', parseInt(e.target.value, 10))}
                              disabled={saving}
                              className="flex-grow-1"
                            />
                            <CBadge color="primary" className="px-3 py-2" style={{ minWidth: '60px' }}>
                              {config.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS}s
                            </CBadge>
                          </div>
                          <small className="form-text text-body-secondary">
                            Start listening this many seconds before bar close. Range: 0-300 seconds.
                          </small>
                        </CCol>
                      </CRow>

                      <CRow className="mb-4">
                        <CCol md={8}>
                          <CFormLabel htmlFor="postCloseSeconds">
                            Post-Close Seconds
                          </CFormLabel>
                          <div className="d-flex align-items-center gap-3">
                            <CFormRange
                              id="postCloseSeconds"
                              min={0}
                              max={60}
                              step={1}
                              value={config.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS}
                              onChange={(e) => handleSchedulingChange('post_close_seconds', parseInt(e.target.value, 10))}
                              disabled={saving}
                              className="flex-grow-1"
                            />
                            <CBadge color="primary" className="px-3 py-2" style={{ minWidth: '60px' }}>
                              {config.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS}s
                            </CBadge>
                          </div>
                          <small className="form-text text-body-secondary">
                            Continue listening this many seconds after bar close to capture late data. Range: 0-60 seconds.
                          </small>
                        </CCol>
                      </CRow>

                      <CRow className="mb-3">
                        <CCol md={10}>
                          <div className="p-3 border rounded" style={{ backgroundColor: 'var(--cui-body-secondary)' }}>
                            <strong>Listening Window Preview</strong>
                            <div className="mt-3 position-relative" style={{ height: '60px' }}>
                              {/* Timeline visualization */}
                              <div
                                className="position-absolute bg-secondary"
                                style={{
                                  left: '0%',
                                  right: '0%',
                                  top: '25px',
                                  height: '4px',
                                  borderRadius: '2px'
                                }}
                              />

                              {/* Pre-close window (before bar close) */}
                              <div
                                className="position-absolute bg-info"
                                style={{
                                  left: `${Math.max(0, 50 - ((config.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS) / 300) * 45)}%`,
                                  width: `${((config.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS) / 300) * 45}%`,
                                  top: '20px',
                                  height: '14px',
                                  borderRadius: '4px 0 0 4px',
                                  opacity: 0.8
                                }}
                              />

                              {/* Bar close marker */}
                              <div
                                className="position-absolute bg-danger"
                                style={{
                                  left: '50%',
                                  top: '15px',
                                  width: '3px',
                                  height: '24px',
                                  marginLeft: '-1.5px',
                                  borderRadius: '2px'
                                }}
                              />

                              {/* Post-close window (after bar close) */}
                              <div
                                className="position-absolute bg-success"
                                style={{
                                  left: '50%',
                                  width: `${((config.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS) / 60) * 45}%`,
                                  top: '20px',
                                  height: '14px',
                                  borderRadius: '0 4px 4px 0',
                                  opacity: 0.8
                                }}
                              />

                              {/* Labels */}
                              <div className="position-absolute text-muted small" style={{ left: '0%', top: '45px' }}>
                                -{config.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS}s
                              </div>
                              <div className="position-absolute text-danger small fw-bold" style={{ left: '50%', top: '0px', transform: 'translateX(-50%)' }}>
                                Bar Close
                              </div>
                              <div className="position-absolute text-muted small" style={{ right: '0%', top: '45px' }}>
                                +{config.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS}s
                              </div>
                            </div>
                            <div className="mt-4 d-flex justify-content-between small text-body-secondary">
                              <span>
                                <span className="d-inline-block me-1" style={{ width: '12px', height: '12px', backgroundColor: 'var(--cui-info)', borderRadius: '2px', opacity: 0.8 }}></span>
                                Pre-close listening
                              </span>
                              <span>
                                <span className="d-inline-block me-1" style={{ width: '12px', height: '12px', backgroundColor: 'var(--cui-danger)', borderRadius: '2px' }}></span>
                                Bar close time
                              </span>
                              <span>
                                <span className="d-inline-block me-1" style={{ width: '12px', height: '12px', backgroundColor: 'var(--cui-success)', borderRadius: '2px', opacity: 0.8 }}></span>
                                Post-close listening
                              </span>
                            </div>
                            <p className="mb-0 mt-3 small">
                              Total listening window:{' '}
                              <CBadge color="success">
                                {(config.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS) + (config.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS)} seconds
                              </CBadge>
                            </p>
                          </div>
                        </CCol>
                      </CRow>
                    </>
                  )}

                  {classSubtype === 'IndexProvider' && (
                    <>
                      <CRow className="mb-3">
                        <CCol>
                          <CAlert color="info">
                            <CIcon icon={cilClock} className="me-2" />
                            Configure how often this IndexProvider automatically syncs its constituents.
                          </CAlert>
                        </CCol>
                      </CRow>

                      <CRow className="mb-4">
                        <CCol md={10}>
                          <CFormLabel className="fw-semibold">Sync Frequency</CFormLabel>
                          <p className="text-body-secondary small mb-3">
                            How often should this IndexProvider automatically fetch and sync its constituents?
                          </p>
                          <div className="d-flex flex-column gap-2 mb-3">
                            {SYNC_FREQUENCY_OPTIONS.map((option) => (
                              <CFormCheck
                                key={option.value}
                                type="radio"
                                name="syncFrequency"
                                id={`sync-frequency-${option.value}`}
                                label={
                                  <span>
                                    <strong>{option.label}</strong>
                                    <span className="text-body-secondary ms-2 small">
                                      - {option.description}
                                    </span>
                                  </span>
                                }
                                checked={(config.scheduling?.sync_frequency ?? DEFAULT_SYNC_FREQUENCY) === option.value}
                                onChange={() => handleSchedulingChange('sync_frequency', option.value)}
                                disabled={saving}
                              />
                            ))}
                          </div>
                        </CCol>
                      </CRow>

                      <CRow className="mb-3">
                        <CCol md={8}>
                          <div className="p-3 border rounded" style={{ backgroundColor: 'var(--cui-body-secondary)' }}>
                            <strong>Sync Schedule Preview</strong>
                            <p className="mb-0 mt-2">
                              Constituents will be synced{' '}
                              <CBadge color="success" className="ms-1">
                                {SYNC_FREQUENCY_OPTIONS.find(o => o.value === (config.scheduling?.sync_frequency ?? DEFAULT_SYNC_FREQUENCY))?.label || 'Weekly'}
                              </CBadge>
                            </p>
                            <small className="text-body-secondary">
                              {SYNC_FREQUENCY_OPTIONS.find(o => o.value === (config.scheduling?.sync_frequency ?? DEFAULT_SYNC_FREQUENCY))?.description || 'Sync at midnight UTC every Monday'}
                            </small>
                          </div>
                        </CCol>
                      </CRow>
                    </>
                  )}
                </CTabPane>
              )}

              {showDataTab && (
                <CTabPane visible={activeTab === 'data'}>
                  <h6>Data</h6>
                  <p className="text-body-secondary mb-3">
                    Configure historical data collection settings.
                  </p>

                  <CRow className="mb-3">
                    <CCol>
                      <CAlert color="info">
                        <CIcon icon={cilStorage} className="me-2" />
                        Configure how much historical data to fetch for new symbol subscriptions.
                        Existing subscriptions are not affected by changes to lookback period.
                      </CAlert>
                    </CCol>
                  </CRow>

                  <CRow className="mb-4">
                    <CCol md={10}>
                      <CFormLabel className="fw-semibold">Lookback Period</CFormLabel>
                      <p className="text-body-secondary small mb-3">
                        When subscribing to a new symbol, how much historical data should be fetched?
                      </p>
                      <div className="d-flex flex-wrap gap-3 mb-3">
                        {LOOKBACK_PRESETS.map((preset) => (
                          <CFormCheck
                            key={preset.value}
                            type="radio"
                            name="lookbackPreset"
                            id={`lookback-${preset.value}`}
                            label={preset.label}
                            checked={(config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS) === preset.value}
                            onChange={() => handleDataChange('lookback_days', preset.value)}
                            disabled={saving}
                          />
                        ))}
                        <CFormCheck
                          type="radio"
                          name="lookbackPreset"
                          id="lookback-custom"
                          label="Custom"
                          checked={!LOOKBACK_PRESETS.some(p => p.value === (config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS))}
                          onChange={() => {
                            // When switching to custom, keep current value if valid, otherwise set to a reasonable default
                            const currentValue = config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS
                            if (!LOOKBACK_PRESETS.some(p => p.value === currentValue)) {
                              // Already custom, keep value
                              return
                            }
                            // Switch to a custom value (default to 180 days as a reasonable non-preset value)
                            handleDataChange('lookback_days', 180)
                          }}
                          disabled={saving}
                        />
                      </div>
                      {!LOOKBACK_PRESETS.some(p => p.value === (config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS)) && (
                        <div className="mb-3">
                          <CFormLabel htmlFor="customLookbackDays" className="small">
                            Custom Lookback Days
                          </CFormLabel>
                          <CInputGroup style={{ maxWidth: '200px' }}>
                            <CFormInput
                              id="customLookbackDays"
                              type="number"
                              min={1}
                              max={8000}
                              value={config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS}
                              onChange={(e) => {
                                const value = parseInt(e.target.value, 10)
                                if (!isNaN(value)) {
                                  // Allow free typing without clamping
                                  handleDataChange('lookback_days', value)
                                }
                              }}
                              onBlur={(e) => {
                                const value = parseInt(e.target.value, 10)
                                if (!isNaN(value)) {
                                  // Clamp value to valid range on blur
                                  const clampedValue = Math.max(1, Math.min(8000, value))
                                  if (clampedValue !== value) {
                                    handleDataChange('lookback_days', clampedValue)
                                  }
                                }
                              }}
                              disabled={saving}
                              invalid={
                                config.data?.lookback_days !== undefined &&
                                (config.data.lookback_days < 1 || config.data.lookback_days > 8000)
                              }
                            />
                            <CInputGroupText>days</CInputGroupText>
                          </CInputGroup>
                          <small className="form-text text-body-secondary">
                            Enter a value between 1 and 8,000 days.
                          </small>
                        </div>
                      )}
                      <div className="p-3 border rounded" style={{ backgroundColor: 'var(--cui-body-secondary)' }}>
                        <p className="mb-0 text-body-secondary small">
                          Selected lookback:{' '}
                          <CBadge color="primary">
                            {config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS} days
                          </CBadge>
                          {' '}
                          ({LOOKBACK_PRESETS.find(p => p.value === (config.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS))?.label || 'Custom'})
                        </p>
                      </div>
                    </CCol>
                  </CRow>
                </CTabPane>
              )}

              <CTabPane visible={activeTab === 'api'}>
                <h6>API Secrets</h6>
                <p className="text-body-secondary mb-3">
                  Update API credentials for this provider. All credentials must be provided together.
                </p>

                {secretsError && <CAlert color="danger">{secretsError}</CAlert>}

                {loadingSecrets ? (
                  <div className="text-center py-4">
                    <CSpinner color="primary" />
                    <p className="mt-2 text-body-secondary">Loading credential fields...</p>
                  </div>
                ) : secretKeys.length === 0 ? (
                  <CAlert color="info">
                    <CIcon icon={cilLockLocked} className="me-2" />
                    No API credentials are configured for this provider.
                  </CAlert>
                ) : (
                  <>
                    <CAlert color="warning" className="mb-4">
                      <strong>Important:</strong> Updating credentials will replace all existing values.
                      You must provide values for all fields. The provider will be automatically reloaded
                      to use the new credentials.
                    </CAlert>

                    <CRow className="mb-4">
                      <CCol md={8}>
                        {secretKeys.map((key) => (
                          <div key={key} className="mb-3">
                            <CFormLabel htmlFor={`secret-${key}`}>
                              {key}
                            </CFormLabel>
                            <CFormInput
                              id={`secret-${key}`}
                              type="password"
                              placeholder={`Enter ${key}`}
                              value={secretValues[key] || ''}
                              onChange={(e) => handleSecretChange(key, e.target.value)}
                              disabled={savingSecrets}
                              autoComplete="off"
                            />
                          </div>
                        ))}
                      </CCol>
                    </CRow>

                    <CRow>
                      <CCol>
                        <CButton
                          color="warning"
                          onClick={handleUpdateSecretsClick}
                          disabled={savingSecrets || secretKeys.some(key => !secretValues[key])}
                        >
                          {savingSecrets ? <CSpinner size="sm" className="me-1" /> : null}
                          {savingSecrets ? 'Updating Credentials...' : 'Update Credentials'}
                        </CButton>
                      </CCol>
                    </CRow>
                  </>
                )}
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

      {/* Confirmation dialog for credential update */}
      <CModal
        visible={showConfirmDialog}
        onClose={() => setShowConfirmDialog(false)}
        backdrop="static"
        size="sm"
      >
        <CModalHeader>
          <CModalTitle>
            <CIcon icon={cilLockLocked} className="me-2" />
            Confirm Credential Update
          </CModalTitle>
        </CModalHeader>
        <CModalBody>
          <CAlert color="warning" className="mb-3">
            <strong>Warning:</strong> This action will replace all existing API credentials.
          </CAlert>
          <p className="mb-2">You are about to update the following credentials:</p>
          <ul className="mb-3">
            {secretKeys.map(key => (
              <li key={key}><code>{key}</code></li>
            ))}
          </ul>
          <p className="text-body-secondary small mb-0">
            The provider will be unloaded and will use the new credentials on next request.
          </p>
        </CModalBody>
        <CModalFooter>
          <CButton color="secondary" onClick={() => setShowConfirmDialog(false)}>
            Cancel
          </CButton>
          <CButton color="warning" onClick={handleConfirmUpdateSecrets}>
            Update Credentials
          </CButton>
        </CModalFooter>
      </CModal>

      {/* Re-map prompt modal for quote preference changes */}
      <RemapPromptModal
        visible={showRemapPrompt}
        onClose={handleRemapPromptClose}
        onConfirm={handleRemapConfirm}
        onDecline={handleRemapDecline}
        isProcessing={isRemapping}
        className={className}
      />
    </CModal>
  )
}

ProviderConfigModal.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  classType: PropTypes.string.isRequired,
  className: PropTypes.string.isRequired,
  classSubtype: PropTypes.string,
  displayToast: PropTypes.func,
}

export default ProviderConfigModal