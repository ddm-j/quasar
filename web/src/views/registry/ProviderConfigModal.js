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
  CFormRange,
  CFormCheck,
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
import { getProviderConfig, updateProviderConfig, getAvailableQuoteCurrencies } from '../services/registry_api'

// Default values for live provider scheduling
const DEFAULT_PRE_CLOSE_SECONDS = 30
const DEFAULT_POST_CLOSE_SECONDS = 5

// Default lookback days for historical providers
const DEFAULT_LOOKBACK_DAYS = 8000

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
  const showSchedulingTab = classSubtype === 'Historical' || classSubtype === 'Live'
  const showDataTab = classSubtype === 'Historical'

  const [activeTab, setActiveTab] = useState('trading')
  const [config, setConfig] = useState({
    crypto: { preferred_quote_currency: null },
    scheduling: {
      delay_hours: 0,
      pre_close_seconds: DEFAULT_PRE_CLOSE_SECONDS,
      post_close_seconds: DEFAULT_POST_CLOSE_SECONDS
    },
    data: {
      lookback_days: DEFAULT_LOOKBACK_DAYS
    }
  })
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
      const prefs = response.preferences || {}
      setConfig({
        crypto: prefs.crypto || { preferred_quote_currency: null },
        scheduling: {
          delay_hours: prefs.scheduling?.delay_hours ?? 0,
          pre_close_seconds: prefs.scheduling?.pre_close_seconds ?? DEFAULT_PRE_CLOSE_SECONDS,
          post_close_seconds: prefs.scheduling?.post_close_seconds ?? DEFAULT_POST_CLOSE_SECONDS
        },
        data: {
          lookback_days: prefs.data?.lookback_days ?? DEFAULT_LOOKBACK_DAYS
        }
      })
    } catch (err) {
      console.error('Failed to load provider configuration:', err)
      setError(`Failed to load configuration: ${err.message}`)
      // Set default empty config on error
      setConfig({
        crypto: { preferred_quote_currency: null },
        scheduling: {
          delay_hours: 0,
          pre_close_seconds: DEFAULT_PRE_CLOSE_SECONDS,
          post_close_seconds: DEFAULT_POST_CLOSE_SECONDS
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

  const handleSave = async () => {
    setSaving(true)
    setError('')
    try {
      const updateData = {
        crypto: config.crypto,
        scheduling: config.scheduling,
        data: config.data
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
                          <div className="p-3 border rounded bg-light">
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
                          <div className="p-3 border rounded bg-light">
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
                      </div>
                      <div className="p-3 border rounded bg-light">
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
  classSubtype: PropTypes.string,
  displayToast: PropTypes.func,
}

export default ProviderConfigModal