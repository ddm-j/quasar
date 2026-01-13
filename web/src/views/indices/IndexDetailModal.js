import React, { useState, useEffect, useMemo } from 'react';
import {
  CModal,
  CModalHeader,
  CModalTitle,
  CModalBody,
  CModalFooter,
  CButton,
  CSpinner,
  CAlert,
  CBadge,
  CNav,
  CNavItem,
  CNavLink,
  CTabContent,
  CTabPane,
  CTable,
  CTableHead,
  CTableRow,
  CTableHeaderCell,
  CTableBody,
  CTableDataCell,
  CFormInput,
  CFormLabel,
} from '@coreui/react-pro';
import CIcon from '@coreui/icons-react';
import { cilReload, cilWarning, cilPencil, cilTrash, cilHistory } from '@coreui/icons';
import { CChartPie } from '@coreui/react-chartjs';

import CommonSymbolDetailModal from '../mappings/CommonSymbolDetailModal';
import EditableMembersTable from './EditableMembersTable';
import DeleteIndexModal from './DeleteIndexModal';
import SaveChangesModal from './SaveChangesModal';
import ConfirmModal from './ConfirmModal';
import {
  getIndexDetail,
  getIndexMembers,
  getIndexHistory,
  updateAssetsForClass,
  updateUserIndexMembers,
} from '../services/registry_api';
import { formatDate, formatWeight } from '../../utils/formatting';

// Color palette for pie chart
const CHART_COLORS = [
  '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
  '#FF9F40', '#E7E9ED', '#7BC225', '#EE82EE', '#00CED1',
  '#FFD700', '#DC143C', '#00FA9A', '#8A2BE2', '#FF7F50',
  '#6495ED', '#DEB887', '#5F9EA0', '#D2691E', '#FF69B4',
];

const PIE_CHART_OPTIONS = {
  plugins: {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (context) => `${context.label}: ${context.parsed.toFixed(1)}%`,
      },
    },
  },
  maintainAspectRatio: true,
  aspectRatio: 1,
  responsive: true,
};

const IndexDetailModal = ({ visible, onClose, indexItem, onRefresh, pushToast }) => {
  // Tab state
  const [activeTab, setActiveTab] = useState('constituents');

  // Data state
  const [indexDetail, setIndexDetail] = useState(null);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);

  // Edit mode state
  const [isEditMode, setIsEditMode] = useState(false);
  const [editableMembers, setEditableMembers] = useState([]);
  const [originalMembers, setOriginalMembers] = useState([]);
  const [isSaving, setIsSaving] = useState(false);

  // Sub-modal state
  const [isDeleteModalVisible, setIsDeleteModalVisible] = useState(false);
  const [isSaveModalVisible, setIsSaveModalVisible] = useState(false);
  const [isSymbolDetailModalVisible, setIsSymbolDetailModalVisible] = useState(false);
  const [selectedCommonSymbol, setSelectedCommonSymbol] = useState(null);
  const [isDiscardModalVisible, setIsDiscardModalVisible] = useState(false);
  const [discardAction, setDiscardAction] = useState(null); // 'cancel' or 'close'

  // Historical view state
  const [asOfDate, setAsOfDate] = useState(null); // null = current, string = historical (YYYY-MM-DD)
  const [historicalMembers, setHistoricalMembers] = useState(null);
  const [isLoadingHistorical, setIsLoadingHistorical] = useState(false);

  // Timeline (history) state
  const [history, setHistory] = useState(null);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);

  // Reset state when modal opens/closes
  useEffect(() => {
    if (visible && indexItem) {
      fetchIndexDetail();
      setActiveTab('constituents');
      setIsEditMode(false);
      setEditableMembers([]);
      setOriginalMembers([]);
      setError(null);
      // Reset historical view
      setAsOfDate(null);
      setHistoricalMembers(null);
      // Reset timeline
      setHistory(null);
    }
  }, [visible, indexItem]);

  const fetchIndexDetail = async () => {
    if (!indexItem?.class_name) return;

    setLoading(true);
    setError(null);

    try {
      const data = await getIndexDetail(indexItem.class_name);
      setIndexDetail(data);
    } catch (err) {
      setError(err.message || 'Failed to fetch index details');
      setIndexDetail(null);
    } finally {
      setLoading(false);
    }
  };

  // Derived data - simple operations don't need useMemo
  const members = indexDetail?.members || [];
  const hasWeights = members.some((m) => m.weight != null);
  const weightedMembers = members.filter((m) => m.weight != null);
  const isUserIndex = indexItem?.index_type === 'UserIndex';

  // Historical view derived state
  const isViewingHistorical = asOfDate !== null;
  const displayMembers = isViewingHistorical ? (historicalMembers || []) : members;

  // Handle date change for historical view
  const handleDateChange = async (dateString) => {
    if (!dateString) {
      setAsOfDate(null);
      setHistoricalMembers(null);
      return;
    }

    setAsOfDate(dateString);
    setIsLoadingHistorical(true);

    try {
      // Convert date to end-of-day ISO 8601
      const asOfDateTime = `${dateString}T23:59:59Z`;
      const response = await getIndexMembers(indexItem.class_name, {
        as_of: asOfDateTime,
        limit: 500,
      });
      setHistoricalMembers(response.items);
    } catch (err) {
      console.error('Failed to fetch historical members:', err);
      if (pushToast) {
        pushToast({
          title: 'Error',
          body: err.message || 'Failed to fetch historical data',
          color: 'danger',
          icon: cilWarning,
        });
      }
      // Reset to current view on error
      setAsOfDate(null);
      setHistoricalMembers(null);
    } finally {
      setIsLoadingHistorical(false);
    }
  };

  // Fetch timeline history
  const fetchHistory = async () => {
    if (!indexItem?.class_name || history) return; // Don't refetch if already loaded

    setIsLoadingHistory(true);
    try {
      const response = await getIndexHistory(indexItem.class_name);
      setHistory(response.changes || []);
    } catch (err) {
      console.error('Failed to fetch history:', err);
      if (pushToast) {
        pushToast({
          title: 'Error',
          body: err.message || 'Failed to fetch history',
          color: 'danger',
          icon: cilWarning,
        });
      }
    } finally {
      setIsLoadingHistory(false);
    }
  };

  // Handle tab change (fetch history when timeline tab is selected)
  const handleTabChange = (tab) => {
    setActiveTab(tab);
    if (tab === 'timeline') {
      fetchHistory();
    }
  };

  // Check for unsaved changes
  const hasUnsavedChanges = () => {
    return JSON.stringify(editableMembers) !== JSON.stringify(originalMembers);
  };

  // Handle refresh for IndexProviders
  const handleRefresh = async () => {
    if (!indexItem) return;

    setRefreshing(true);
    setError(null);

    try {
      await updateAssetsForClass('provider', indexItem.class_name);
      await fetchIndexDetail();
      if (onRefresh) onRefresh();
    } catch (err) {
      setError(err.message || 'Failed to refresh index');
      if (pushToast) {
        pushToast({
          title: 'Refresh Failed',
          body: err.message || 'Failed to refresh index',
          color: 'danger',
          icon: cilWarning,
        });
      }
    } finally {
      setRefreshing(false);
    }
  };

  // Enter edit mode
  const handleEnterEditMode = () => {
    const editable = members.map((m) => ({
      id: m.id?.toString() || `existing_${m.common_symbol}`,
      common_symbol: m.common_symbol || m.effective_symbol,
      weight: m.weight,
      isNew: false,
      selectOption: null,
    }));
    setEditableMembers(editable);
    setOriginalMembers(editable.map((m) => ({ ...m })));
    setIsEditMode(true);
  };

  // Cancel edit mode - show confirmation if unsaved changes
  const handleCancelEdit = () => {
    if (hasUnsavedChanges()) {
      setDiscardAction('cancel');
      setIsDiscardModalVisible(true);
    } else {
      exitEditMode();
    }
  };

  // Exit edit mode without saving
  const exitEditMode = () => {
    setIsEditMode(false);
    setEditableMembers([]);
    setOriginalMembers([]);
  };

  // Handle discard confirmation
  const handleDiscardConfirm = () => {
    setIsDiscardModalVisible(false);
    if (discardAction === 'cancel') {
      exitEditMode();
    } else if (discardAction === 'close') {
      onClose();
    }
    setDiscardAction(null);
  };

  // Calculate changes summary for save modal
  const changesSummary = useMemo(() => {
    if (!isEditMode) return { added: [], removed: [], weightChanges: [], totalWeight: 0 };

    const originalMap = new Map(
      originalMembers.map((m) => [m.common_symbol, m.weight])
    );
    const editedMap = new Map(
      editableMembers
        .filter((m) => m.common_symbol)
        .map((m) => [m.common_symbol, m.weight])
    );

    const added = [];
    const removed = [];
    const weightChanges = [];

    // Find added and changed
    for (const [symbol, weight] of editedMap) {
      if (!originalMap.has(symbol)) {
        added.push(symbol);
      } else if (originalMap.get(symbol) !== weight) {
        weightChanges.push({
          symbol,
          old: originalMap.get(symbol),
          new: weight,
        });
      }
    }

    // Find removed
    for (const symbol of originalMap.keys()) {
      if (!editedMap.has(symbol)) {
        removed.push(symbol);
      }
    }

    const totalWeight = Array.from(editedMap.values())
      .filter((w) => w != null)
      .reduce((sum, w) => sum + w, 0);

    return { added, removed, weightChanges, totalWeight };
  }, [isEditMode, editableMembers, originalMembers]);

  // Calculate total weight for edit mode display
  const editTotalWeight = editableMembers
    .filter((m) => m.weight != null)
    .reduce((sum, m) => sum + m.weight, 0);

  // Handle save
  const handleSave = async () => {
    setIsSaving(true);
    setError(null);

    try {
      const membersPayload = editableMembers
        .filter((m) => m.common_symbol)
        .map((m) => ({
          common_symbol: m.common_symbol,
          weight: m.weight,
        }));

      await updateUserIndexMembers(indexItem.class_name, { members: membersPayload });

      if (pushToast) {
        pushToast({
          title: 'Index Updated',
          body: `Index "${indexItem.class_name}" has been updated successfully.`,
          color: 'success',
        });
      }

      setIsSaveModalVisible(false);
      setIsEditMode(false);
      await fetchIndexDetail();
      if (onRefresh) onRefresh();
    } catch (err) {
      setError(err.message || 'Failed to save changes');
      if (pushToast) {
        pushToast({
          title: 'Save Failed',
          body: err.message || 'Failed to save changes',
          color: 'danger',
          icon: cilWarning,
        });
      }
    } finally {
      setIsSaving(false);
    }
  };

  // Handle delete success
  const handleDeleteSuccess = () => {
    setIsDeleteModalVisible(false);
    onClose();
    if (onRefresh) onRefresh();
  };

  // Weight utilities
  const handleNormalize = () => {
    const withWeights = editableMembers.filter((m) => m.weight != null);
    const total = withWeights.reduce((sum, m) => sum + m.weight, 0);

    if (total === 0) {
      handleEqualWeight();
      return;
    }

    setEditableMembers((prev) =>
      prev.map((m) =>
        m.weight != null ? { ...m, weight: m.weight / total } : m
      )
    );
  };

  const handleEqualWeight = () => {
    const count = editableMembers.length;
    if (count === 0) return;
    const equalWeight = 1 / count;

    setEditableMembers((prev) =>
      prev.map((m) => ({ ...m, weight: equalWeight }))
    );
  };

  // Handle common symbol click (view mode) - opens symbol detail modal
  const handleCommonSymbolClick = (member) => {
    const symbol = member.mapped_common_symbol || member.common_symbol;
    if (symbol) {
      setSelectedCommonSymbol(symbol);
      setIsSymbolDetailModalVisible(true);
    }
  };

  // Handle modal close with unsaved changes check
  const handleClose = () => {
    if (isEditMode && hasUnsavedChanges()) {
      setDiscardAction('close');
      setIsDiscardModalVisible(true);
    } else {
      onClose();
    }
  };

  // Get badge color for index type
  const getTypeBadgeColor = (indexType) => {
    return indexType === 'IndexProvider' ? 'primary' : 'success';
  };

  // Prepare pie chart data with "Others" aggregation for <1% weights
  const pieChartData = useMemo(() => {
    if (!weightedMembers.length) return null;

    const significantMembers = weightedMembers.filter((m) => m.weight >= 0.01);
    const smallMembers = weightedMembers.filter((m) => m.weight < 0.01);
    const othersWeight = smallMembers.reduce((sum, m) => sum + m.weight, 0);

    const labels = significantMembers.map(
      (m) => m.effective_symbol || m.common_symbol || 'Unknown'
    );
    const data = significantMembers.map((m) => m.weight * 100);
    const colors = significantMembers.map((_, i) => CHART_COLORS[i % CHART_COLORS.length]);

    if (smallMembers.length > 0 && othersWeight > 0) {
      labels.push(`Others (${smallMembers.length})`);
      data.push(othersWeight * 100);
      colors.push('#9E9E9E');
    }

    return {
      labels,
      datasets: [{ data, backgroundColor: colors, hoverBackgroundColor: colors }],
    };
  }, [weightedMembers]);

  // Check for validation issues in edit mode
  const hasValidationErrors = useMemo(() => {
    if (!isEditMode) return false;
    const symbols = editableMembers
      .map((m) => m.common_symbol)
      .filter((s) => s !== null);
    const hasDuplicates = symbols.length !== new Set(symbols).size;
    const hasEmptyNewRows = editableMembers.some((m) => m.isNew && !m.common_symbol);
    return hasDuplicates || hasEmptyNewRows;
  }, [isEditMode, editableMembers]);

  return (
    <>
      <CModal
        visible={visible}
        onClose={handleClose}
        backdrop="static"
        size="lg"
        scrollable
      >
        <CModalHeader onClose={handleClose}>
          <CModalTitle className="d-flex align-items-center gap-2">
            <strong>{indexItem?.class_name || 'Index Details'}</strong>
            {indexItem?.index_type && (
              <CBadge color={getTypeBadgeColor(indexItem.index_type)}>
                {indexItem.index_type}
              </CBadge>
            )}
            {indexDetail?.index?.current_member_count !== undefined && (
              <small className="text-muted">
                {indexDetail.index.current_member_count} members
              </small>
            )}
            {isEditMode && (
              <CBadge color="warning" className="ms-2">
                Editing
              </CBadge>
            )}
          </CModalTitle>
        </CModalHeader>

        <CModalBody>
          {error && (
            <CAlert color="danger" className="d-flex align-items-center">
              <CIcon icon={cilWarning} className="me-2" />
              {error}
            </CAlert>
          )}

          {loading ? (
            <div className="text-center py-5">
              <CSpinner color="primary" />
              <p className="mt-2 text-muted">Loading index details...</p>
            </div>
          ) : (
            <>
              {/* Historical date selector */}
              {!isEditMode && (
                <div className="d-flex align-items-center gap-2 mb-3">
                  <CFormLabel className="mb-0 text-muted small">View as of:</CFormLabel>
                  <CFormInput
                    type="date"
                    value={asOfDate || ''}
                    onChange={(e) => handleDateChange(e.target.value)}
                    max={new Date().toISOString().split('T')[0]}
                    style={{ width: '160px' }}
                    size="sm"
                    disabled={isLoadingHistorical}
                  />
                  {isLoadingHistorical && <CSpinner size="sm" />}
                  {isViewingHistorical && !isLoadingHistorical && (
                    <CButton
                      color="secondary"
                      variant="outline"
                      size="sm"
                      onClick={() => handleDateChange(null)}
                    >
                      Reset to Current
                    </CButton>
                  )}
                </div>
              )}

              {/* Historical view banner */}
              {isViewingHistorical && !isLoadingHistorical && (
                <CAlert color="info" className="d-flex align-items-center py-2 mb-3">
                  <CIcon icon={cilHistory} className="me-2" />
                  <div>
                    <strong>Viewing index as of {formatDate(asOfDate, false)}</strong>
                    <span className="ms-2 text-muted">
                      ({displayMembers.length} members
                      {displayMembers.length !== members.length && (
                        <> • currently {members.length}</>
                      )}
                      )
                    </span>
                  </div>
                </CAlert>
              )}

              <CNav variant="tabs" className="mb-3">
                <CNavItem>
                  <CNavLink
                    active={activeTab === 'constituents'}
                    onClick={() => handleTabChange('constituents')}
                    style={{ cursor: 'pointer' }}
                  >
                    Constituents
                  </CNavLink>
                </CNavItem>
                {hasWeights && !isEditMode && (
                  <CNavItem>
                    <CNavLink
                      active={activeTab === 'weights'}
                      onClick={() => handleTabChange('weights')}
                      style={{ cursor: 'pointer' }}
                    >
                      Weights
                    </CNavLink>
                  </CNavItem>
                )}
                {!isEditMode && (
                  <CNavItem>
                    <CNavLink
                      active={activeTab === 'timeline'}
                      onClick={() => handleTabChange('timeline')}
                      style={{ cursor: 'pointer' }}
                    >
                      <CIcon icon={cilHistory} className="me-1" />
                      Timeline
                    </CNavLink>
                  </CNavItem>
                )}
              </CNav>

              <CTabContent>
                {/* Constituents Tab */}
                <CTabPane visible={activeTab === 'constituents'}>
                  {/* IndexProvider refresh button */}
                  {indexItem?.index_type === 'IndexProvider' && !isEditMode && (
                    <div className="mb-3">
                      <CButton
                        color="primary"
                        size="sm"
                        onClick={handleRefresh}
                        disabled={refreshing}
                      >
                        {refreshing ? (
                          <>
                            <CSpinner size="sm" className="me-1" />
                            Refreshing...
                          </>
                        ) : (
                          <>
                            <CIcon icon={cilReload} className="me-1" />
                            Refresh
                          </>
                        )}
                      </CButton>
                    </div>
                  )}

                  {/* Edit mode: EditableMembersTable */}
                  {isEditMode ? (
                    <>
                      <EditableMembersTable
                        members={editableMembers}
                        onChange={setEditableMembers}
                        disabled={isSaving}
                      />

                      {/* Weight summary and utilities */}
                      <div className="mt-3 pt-3 border-top d-flex align-items-center justify-content-between flex-wrap gap-2">
                        <div>
                          <span className="me-2">
                            Total Weight:{' '}
                            <CBadge
                              color={Math.abs(editTotalWeight - 1) < 0.001 ? 'success' : 'warning'}
                            >
                              {(editTotalWeight * 100).toFixed(1)}%
                            </CBadge>
                          </span>
                        </div>
                        <div className="d-flex gap-2">
                          <CButton
                            color="secondary"
                            variant="outline"
                            size="sm"
                            onClick={handleNormalize}
                            disabled={isSaving || editableMembers.length === 0}
                            title="Adjust weights to sum to 100%"
                          >
                            Normalize
                          </CButton>
                          <CButton
                            color="secondary"
                            variant="outline"
                            size="sm"
                            onClick={handleEqualWeight}
                            disabled={isSaving || editableMembers.length === 0}
                            title="Set all weights equal"
                          >
                            Equal Weight
                          </CButton>
                        </div>
                      </div>
                    </>
                  ) : (
                    /* View mode: Regular table with clickable common symbols */
                    <>
                      {displayMembers.length === 0 ? (
                        <CAlert color="info">
                          {isViewingHistorical
                            ? 'No members found at this point in time.'
                            : 'No members found in this index.'}
                        </CAlert>
                      ) : (
                        <>
                          <p className="text-muted small mb-2">
                            Click a common symbol to view its mappings.
                          </p>
                          <CTable striped hover responsive>
                            <CTableHead>
                              <CTableRow>
                                <CTableHeaderCell>Symbol</CTableHeaderCell>
                                <CTableHeaderCell>Common Symbol</CTableHeaderCell>
                                <CTableHeaderCell className="text-end">
                                  Weight
                                </CTableHeaderCell>
                                <CTableHeaderCell>Valid From</CTableHeaderCell>
                              </CTableRow>
                            </CTableHead>
                            <CTableBody>
                              {displayMembers.map((member, idx) => (
                                <CTableRow key={member.id || idx}>
                                  <CTableDataCell className="fw-semibold">
                                    {member.effective_symbol || member.common_symbol || '—'}
                                  </CTableDataCell>
                                  <CTableDataCell>
                                    {member.mapped_common_symbol || member.common_symbol ? (
                                      <span
                                        style={{
                                          cursor: 'pointer',
                                          color: 'var(--cui-link-color)',
                                          textDecoration: 'none',
                                        }}
                                        onClick={() => handleCommonSymbolClick(member)}
                                        onMouseEnter={(e) =>
                                          (e.target.style.textDecoration = 'underline')
                                        }
                                        onMouseLeave={(e) =>
                                          (e.target.style.textDecoration = 'none')
                                        }
                                      >
                                        {member.mapped_common_symbol || member.common_symbol}
                                      </span>
                                    ) : (
                                      <span className="text-muted">—</span>
                                    )}
                                  </CTableDataCell>
                                  <CTableDataCell className="text-end">
                                    {formatWeight(member.weight)}
                                  </CTableDataCell>
                                  <CTableDataCell>
                                    {formatDate(member.valid_from, false)}
                                  </CTableDataCell>
                                </CTableRow>
                              ))}
                            </CTableBody>
                          </CTable>
                        </>
                      )}
                    </>
                  )}
                </CTabPane>

                {/* Weights Tab (view mode only) */}
                {hasWeights && !isEditMode && (
                  <CTabPane visible={activeTab === 'weights'}>
                    {pieChartData && (
                      <div
                        style={{
                          width: '100%',
                          display: 'flex',
                          justifyContent: 'center',
                          marginBottom: '16px',
                        }}
                      >
                        <div
                          style={{
                            width: 'min(80%, calc(80vh - 200px))',
                            aspectRatio: '1',
                          }}
                        >
                          <CChartPie
                            data={pieChartData}
                            options={PIE_CHART_OPTIONS}
                          />
                        </div>
                      </div>
                    )}

                    <CTable striped hover responsive>
                      <CTableHead>
                        <CTableRow>
                          <CTableHeaderCell>Symbol</CTableHeaderCell>
                          <CTableHeaderCell className="text-end">Weight</CTableHeaderCell>
                        </CTableRow>
                      </CTableHead>
                      <CTableBody>
                        {weightedMembers.map((member, idx) => (
                          <CTableRow key={member.id || idx}>
                            <CTableDataCell className="fw-semibold">
                              {member.effective_symbol || member.common_symbol || '—'}
                            </CTableDataCell>
                            <CTableDataCell className="text-end">
                              {formatWeight(member.weight)}
                            </CTableDataCell>
                          </CTableRow>
                        ))}
                      </CTableBody>
                    </CTable>
                  </CTabPane>
                )}

                {/* Timeline Tab */}
                {!isEditMode && (
                  <CTabPane visible={activeTab === 'timeline'}>
                    {isLoadingHistory ? (
                      <div className="text-center py-4">
                        <CSpinner />
                        <div className="mt-2 text-muted">Loading timeline...</div>
                      </div>
                    ) : history && history.length > 0 ? (
                      <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
                        {history.map((change, idx) => (
                          <div key={idx} className="mb-3">
                            <div className="fw-bold text-muted small mb-1">
                              {formatDate(change.date, false)}
                            </div>
                            {change.events.map((event, eventIdx) => (
                              <div
                                key={eventIdx}
                                className={event.type === 'added' ? 'text-success' : 'text-danger'}
                              >
                                {event.type === 'added' ? '+' : '−'}{' '}
                                {event.symbol}
                                {event.weight != null && ` ${(event.weight * 100).toFixed(0)}%`}
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-muted text-center py-4">
                        No change history available for this index.
                      </div>
                    )}
                  </CTabPane>
                )}
              </CTabContent>
            </>
          )}
        </CModalBody>

        <CModalFooter>
          {isEditMode ? (
            /* Edit mode footer */
            <>
              <CButton
                color="secondary"
                onClick={handleCancelEdit}
                disabled={isSaving}
              >
                Cancel
              </CButton>
              <CButton
                color="primary"
                onClick={() => setIsSaveModalVisible(true)}
                disabled={isSaving || hasValidationErrors}
              >
                Save Changes
              </CButton>
            </>
          ) : (
            /* View mode footer */
            <>
              {isUserIndex && (
                <>
                  <CButton
                    color="danger"
                    variant="outline"
                    onClick={() => setIsDeleteModalVisible(true)}
                  >
                    <CIcon icon={cilTrash} className="me-1" />
                    Delete
                  </CButton>
                  <CButton color="primary" onClick={handleEnterEditMode}>
                    <CIcon icon={cilPencil} className="me-1" />
                    Edit
                  </CButton>
                </>
              )}
              <CButton color="secondary" onClick={handleClose}>
                Close
              </CButton>
            </>
          )}
        </CModalFooter>
      </CModal>

      {/* Common Symbol Detail Modal */}
      <CommonSymbolDetailModal
        visible={isSymbolDetailModalVisible}
        onClose={() => {
          setIsSymbolDetailModalVisible(false);
          setSelectedCommonSymbol(null);
        }}
        commonSymbol={selectedCommonSymbol}
      />

      {/* Delete Index Modal */}
      <DeleteIndexModal
        visible={isDeleteModalVisible}
        onClose={() => setIsDeleteModalVisible(false)}
        onSuccess={handleDeleteSuccess}
        indexName={indexItem?.class_name}
        pushToast={pushToast}
      />

      {/* Save Changes Modal */}
      <SaveChangesModal
        visible={isSaveModalVisible}
        onClose={() => setIsSaveModalVisible(false)}
        onConfirm={handleSave}
        isSaving={isSaving}
        changesSummary={changesSummary}
        indexName={indexItem?.class_name}
      />

      {/* Discard Changes Confirmation Modal */}
      <ConfirmModal
        visible={isDiscardModalVisible}
        onClose={() => setIsDiscardModalVisible(false)}
        onConfirm={handleDiscardConfirm}
        title="Discard Changes"
        message="You have unsaved changes. Are you sure you want to discard them?"
        confirmLabel="Discard"
        confirmColor="danger"
      />
    </>
  );
};

export default IndexDetailModal;
