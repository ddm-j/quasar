import React, { useState, useEffect } from 'react';
import {
    CModal, CModalHeader, CModalTitle, CModalBody, CModalFooter,
    CButton, CTable, CBadge, CSpinner, CAlert,
} from '@coreui/react-pro';
import { getAssetMappings } from '../services/registry_api';

const CommonSymbolDetailModal = ({ visible, commonSymbol, onClose }) => {
    // State
    const [mappings, setMappings] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    // Reset state when modal opens/closes or commonSymbol changes
    useEffect(() => {
        if (visible && commonSymbol) {
            fetchMappingsForSymbol();
        } else {
            setMappings([]);
            setError(null);
        }
    }, [visible, commonSymbol]);

    const fetchMappingsForSymbol = async () => {
        if (!commonSymbol) return;

        setLoading(true);
        setError(null);

        try {
            // Get all mappings and filter by common_symbol
            // In a real implementation, you might want a dedicated API endpoint
            // that filters server-side for better performance
            const allMappings = await getAssetMappings();
            const filteredMappings = allMappings.filter(mapping =>
                mapping.common_symbol === commonSymbol
            );
            setMappings(filteredMappings);
        } catch (err) {
            console.error('Error fetching mappings for symbol:', commonSymbol, err);
            setError(err.message || 'Failed to fetch mappings');
        } finally {
            setLoading(false);
        }
    };

    const getClassBadge = (class_type) => {
        switch (class_type) {
            case 'provider': return 'primary';
            case 'broker': return 'secondary';
            default: return 'primary';
        }
    };

    const getActiveBadge = (is_active) => {
        return is_active ? 'success' : 'danger';
    };

    return (
        <CModal visible={visible} onClose={onClose} size="lg">
            <CModalHeader onClose={onClose}>
                <CModalTitle>Common Symbol: {commonSymbol}</CModalTitle>
            </CModalHeader>
            <CModalBody>
                {loading && (
                    <div className="text-center">
                        <CSpinner color="primary" />
                        <p className="mt-2">Loading mappings...</p>
                    </div>
                )}

                {error && (
                    <CAlert color="danger">{error}</CAlert>
                )}

                {!loading && !error && mappings.length > 0 && (
                    <CTable striped hover responsive>
                        <thead>
                            <tr>
                                <th>Provider/Broker</th>
                                <th>Type</th>
                                <th>Symbol</th>
                                <th>Primary ID</th>
                                <th>Active</th>
                            </tr>
                        </thead>
                        <tbody>
                            {mappings.map((mapping, index) => (
                                <tr key={index}>
                                    <td>{mapping.class_name}</td>
                                    <td>
                                        <CBadge color={getClassBadge(mapping.class_type)}>
                                            {mapping.class_type.charAt(0).toUpperCase() + mapping.class_type.slice(1)}
                                        </CBadge>
                                    </td>
                                    <td>{mapping.class_symbol}</td>
                                    <td>{mapping.primary_id || '-'}</td>
                                    <td>
                                        <CBadge color={getActiveBadge(mapping.is_active)}>
                                            {mapping.is_active ? 'Yes' : 'No'}
                                        </CBadge>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </CTable>
                )}

                {!loading && !error && mappings.length === 0 && (
                    <p className="text-center text-muted">No mappings found for this symbol.</p>
                )}
            </CModalBody>
            <CModalFooter>
                <CButton color="secondary" onClick={onClose}>
                    Close
                </CButton>
            </CModalFooter>
        </CModal>
    );
};

export default CommonSymbolDetailModal;