// API base path - uses relative URL for proxy compatibility (dev: Vite proxy, prod: nginx/ALB)
const API_BASE = '/api/registry/';

// Normalize backend error payloads (objects/arrays) into a readable string
const formatErrorMessage = (data, status) => {
  if (!data) {
    return `HTTP error! status: ${status}`;
  }
  const detail = data.detail || data.error || data.message;
  if (Array.isArray(detail)) {
    return detail
      .map((item) => {
        if (typeof item === 'string') return item;
        if (item?.msg) return item.msg;
        if (item?.message) return item.message;
        return JSON.stringify(item);
      })
      .join('; ');
  }
  if (typeof detail === 'object') {
    if (detail?.msg) return detail.msg;
    if (detail?.message) return detail.message;
    return JSON.stringify(detail);
  }
  return detail || `HTTP error! status: ${status}`;
};

export const updateAssetsForClass = async (classType, className) => {
    const params = new URLSearchParams({
        class_type: classType,
        class_name: className
    });
    
    const response = await fetch(`${API_BASE}update-assets?${params.toString()}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();


    if (!response.ok) {
        // Use message from data if available, otherwise use a generic error
        const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}

export const updateAllAssets = async () => {
    const response = await fetch(`${API_BASE}update-all-assets`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();


    if (!response.ok) {
        // Use message from data if available, otherwise use a generic error
        const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}
export const getRegisteredClasses = async () => {
    const response = await fetch(`${API_BASE}classes/summary`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();

    if (!response.ok) {
        // Use message from data if available, otherwise use a generic error
        const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}
export const uploadCode = async (classType, file, secretsObject) => {
  const formData = new FormData();
  formData.append('file', file, file.name); // The third argument is the filename

  // Convert secrets array to a JSON string to send as a single field.
  const secretsString = JSON.stringify(secretsObject);
  formData.append('secrets', secretsString);

  const params = new URLSearchParams({
    class_type: classType
  });

  const response = await fetch(`${API_BASE}upload?${params.toString()}`, {
    method: 'POST',
    body: formData,
  });

  const responseData = await response.json();

  if (!response.ok) {
    // Use message from responseData if available, otherwise use a generic error
    const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return responseData;
};
export const deleteRegisteredClass = async (classType, className) => {
  const params = new URLSearchParams({
    class_type: classType,
    class_name: className
  });
  
  const response = await fetch(`${API_BASE}delete?${params.toString()}`, {
    method: 'DELETE',
    headers: {
      'Content-Type': 'application/json', // Optional for DELETE if no body, but good practice
    },
  });

  // DELETE requests might not always return a body, or might return an empty body on success (204 No Content)
  // Or they might return a JSON body with a success/error message.
  let responseData;
  const contentType = response.headers.get("content-type");
  if (contentType && contentType.indexOf("application/json") !== -1) {
    responseData = await response.json();
  } else {
    // If no JSON body, create a default object or use the status text
    responseData = { message: response.statusText, status: response.status };
  }

  if (!response.ok) {
    const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return responseData; // Contains { message: "...", class_name: "...", class_type: "..." } on success
                       // or { message: "...", ..., file_deletion_error: "..." } on partial success (207)
                       // or throws an error with { error: "..." }
};

export const getAssetMappings = async (params = {}) => {
  const queryParams = new URLSearchParams();

  // Add only defined parameters to the query string
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      queryParams.append(key, value);
    }
  });

  const queryString = queryParams.toString();
  const url = `${API_BASE}asset-mappings${queryString ? `?${queryString}` : ''}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data; // Now returns { items: [], total_items: X, limit: Y, offset: Z, page: P, total_pages: Q }
}

/**
 * Fetches asset mapping suggestions from the API.
 * @param {object} params - Query parameters.
 * @param {string} params.source_class - (Required) Provider/broker to suggest mappings for.
 * @param {string} [params.source_type] - Optional: 'provider' or 'broker'.
 * @param {string} [params.target_class] - Optional target provider/broker.
 * @param {string} [params.target_type] - Optional: 'provider' or 'broker'.
 * @param {string} [params.search] - Optional search filter.
 * @param {number} [params.min_score=30] - Minimum score threshold.
 * @param {number} [params.limit=50] - Max results (1-200).
 * @param {string} [params.cursor] - Pagination cursor from previous response.
 * @param {boolean} [params.include_total=false] - Include total count.
 * @returns {Promise<object>} - { items, total, limit, offset, next_cursor, has_more }
 */
export const getAssetMappingSuggestions = async (params = {}) => {
  const queryParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      queryParams.append(key, value);
    }
  });

  const queryString = queryParams.toString();
  const url = `${API_BASE}asset-mapping-suggestions${queryString ? `?${queryString}` : ''}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.detail || data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Creates one or more asset mappings.
 * Accepts a single mapping object or an array; always returns an array.
 * @param {object|object[]} mappingData - Mapping object or array of mappings.
 * @returns {Promise<object[]>} - The created mapping responses.
 */
export const createAssetMapping = async (mappingData) => {
  const payload = Array.isArray(mappingData) ? mappingData : [mappingData];

  const response = await fetch(`${API_BASE}asset-mappings`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(data, response.status);
    throw new Error(errorMessage);
  }

  return data;
}

/**
 * Updates an existing asset mapping.
 * @param {string} class_name - Class name (provider/broker name).
 * @param {string} class_type - Class type: 'provider' or 'broker'.
 * @param {string} class_symbol - Class-specific symbol.
 * @param {object} updateData - The update data (partial).
 * @param {string} [updateData.common_symbol] - Common symbol identifier.
 * @param {boolean} [updateData.is_active] - Whether the mapping is active.
 * @returns {Promise<object>} - The updated mapping response.
 */
export const updateAssetMapping = async (class_name, class_type, class_symbol, updateData) => {
  const params = new URLSearchParams({
    class_name: class_name,
    class_type: class_type,
    class_symbol: class_symbol
  });
  
  const response = await fetch(
    `${API_BASE}asset-mappings?${params.toString()}`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(updateData),
    }
  );

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.detail || data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data;
}

/**
 * Deletes an existing asset mapping.
 * @param {string} class_name - Class name (provider/broker name).
 * @param {string} class_type - Class type: 'provider' or 'broker'.
 * @param {string} class_symbol - Class-specific symbol.
 * @returns {Promise<void>} - Resolves on success, throws on error.
 */
export const deleteAssetMapping = async (class_name, class_type, class_symbol) => {
  const params = new URLSearchParams({
    class_name: class_name,
    class_type: class_type,
    class_symbol: class_symbol
  });
  
  const response = await fetch(
    `${API_BASE}asset-mappings?${params.toString()}`,
    {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
      },
    }
  );

  // DELETE requests might return 204 No Content (no body) or a JSON error
  // Check for 204 status FIRST before attempting to parse JSON
  if (response.status === 204) {
    // 204 No Content - successful deletion, no body
    return;
  }

  let responseData;
  const contentType = response.headers.get("content-type");
  if (contentType && contentType.indexOf("application/json") !== -1) {
    responseData = await response.json();
  } else {
    // If no JSON body, create a default object
    responseData = { message: response.statusText, status: response.status };
  }

  if (!response.ok) {
    const errorMessage = responseData.detail || responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return responseData;
}

/**
 * Fetches assets from the API with filtering, sorting, and pagination.
 * @param {object} params - The query parameters.
 * @param {number} [params.limit=25] - Number of items per page.
 * @param {number} [params.offset=0] - Starting index.
 * @param {string} [params.sort_by='class_name,symbol'] - Column(s) to sort by.
 * @param {string} [params.sort_order='asc'] - Sort order ('asc' or 'desc').
 * @param {string} [params.class_name_like] - Partial match for class_name.
 * @param {string} [params.class_type] - Exact match for class_type.
 * @param {string} [params.asset_class] - Exact match for asset_class.
 * @param {string} [params.base_currency_like] - Partial match for base_currency.
 * @param {string} [params.quote_currency_like] - Partial match for quote_currency.
 * @param {string} [params.country_like] - Partial match for country.
 * @param {string} [params.symbol_like] - Partial match for symbol.
 * @param {string} [params.name_like] - Partial match for name.
 * @param {string} [params.exchange_like] - Partial match for exchange.
 * @returns {Promise<object>} - The API response (e.g., { items: [], total_items: 0, ... }).
 */
export const getAssets = async (params = {}) => {
  const queryParams = new URLSearchParams();

  // Add only defined parameters to the query string
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      queryParams.append(key, value);
    }
  });

  const queryString = queryParams.toString();
  const url = `${API_BASE}assets${queryString ? `?${queryString}` : ''}`;

  console.log('Fetching assets with URL:', url); // For debugging

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
    console.error('Error fetching assets:', errorMessage, 'Response data:', data);
    throw new Error(errorMessage);
  }

  return data; // Expected format: { items: [], total_items: X, limit: Y, offset: Z, ... }
};

/**
 * Fetches provider configuration preferences.
 * @param {string} classType - Class type: 'provider' or 'broker'.
 * @param {string} className - Class name (provider/broker name).
 * @returns {Promise<object>} - Provider preferences response.
 */
export const getProviderConfig = async (classType, className) => {
  const params = new URLSearchParams({
    class_type: classType,
    class_name: className
  });

  const response = await fetch(`${API_BASE}config?${params.toString()}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.detail || data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Updates provider configuration preferences.
 * @param {string} classType - Class type: 'provider' or 'broker'.
 * @param {string} className - Class name (provider/broker name).
 * @param {object} config - Configuration update object.
 * @returns {Promise<object>} - Updated provider preferences response.
 */
export const updateProviderConfig = async (classType, className, config) => {
  const params = new URLSearchParams({
    class_type: classType,
    class_name: className
  });

  const response = await fetch(`${API_BASE}config?${params.toString()}`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(config),
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(data, response.status);
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Fetches available quote currencies for a provider's crypto assets.
 * @param {string} classType - Class type: 'provider' or 'broker'.
 * @param {string} className - Class name (provider/broker name).
 * @returns {Promise<object>} - Available quote currencies response.
 */
export const getAvailableQuoteCurrencies = async (classType, className) => {
  const params = new URLSearchParams({
    class_type: classType,
    class_name: className
  });

  const response = await fetch(`${API_BASE}config/available-quote-currencies?${params.toString()}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.detail || data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data;
};


/**
 * Fetches common symbols with server-side pagination, sorting, and filtering.
 * @param {object} params - Query parameters.
 * @param {number} [params.limit=25] - Number of items per page (max 100).
 * @param {number} [params.offset=0] - Starting index.
 * @param {string} [params.sort_by='common_symbol'] - Column to sort by.
 * @param {string} [params.sort_order='asc'] - Sort order ('asc' or 'desc').
 * @param {string} [params.common_symbol_like] - Partial match filter for common_symbol.
 * @returns {Promise<object>} - Common symbols response: { items, total_items, limit, offset, page, total_pages }
 */
export const getCommonSymbols = async (params = {}) => {
  const queryParams = new URLSearchParams();

  // Add only defined parameters to the query string
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      queryParams.append(key, value);
    }
  });

  const queryString = queryParams.toString();
  const url = `${API_BASE}common-symbols${queryString ? `?${queryString}` : ''}`;


  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
    console.error('Error fetching common symbols:', errorMessage, 'Response data:', data);
    throw new Error(errorMessage);
  }

  return data; // Expected format: { items: [], total_items: X, limit: Y, offset: Z, ... }
};

/**
 * Renames a common symbol, cascading to all asset mappings and index memberships.
 * @param {string} symbol - The current common symbol name.
 * @param {string} newSymbol - The new symbol name.
 * @returns {Promise<object>} - { old_symbol, new_symbol, asset_mappings_updated, index_memberships_updated }
 */
export const renameCommonSymbol = async (symbol, newSymbol) => {
  const response = await fetch(
    `${API_BASE}asset-mappings/common-symbol/${encodeURIComponent(symbol)}/rename`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ new_symbol: newSymbol }),
    }
  );

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.detail || data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Fetches asset mappings for a specific common symbol.
 * @param {string} commonSymbol - The common symbol to filter by.
 * @returns {Promise<object[]>} - Array of asset mapping objects.
 */
export const getAssetMappingsForSymbol = async (commonSymbol) => {
  const response = await fetch(`${API_BASE}asset-mappings/${encodeURIComponent(commonSymbol)}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = data.detail || data.error || data.message || `HTTP error! status: ${response.status}`;
    console.error('Error fetching asset mappings for symbol:', errorMessage, 'Response data:', data);
    throw new Error(errorMessage);
  }

  return data; // Expected format: [{ common_symbol, class_name, class_type, class_symbol, is_active }, ...]
};

/**
 * Fetches all indices with optional filtering.
 * @param {object} params - Query parameters.
 * @param {number} [params.limit=100] - Number of items per page.
 * @param {number} [params.offset=0] - Starting index.
 * @param {string} [params.index_type] - Filter by 'IndexProvider' or 'UserIndex'.
 * @param {string} [params.sort_by] - Column to sort by.
 * @param {string} [params.sort_order] - Sort order ('asc' or 'desc').
 * @returns {Promise<object>} - Indices response: { items, total_items, limit, offset }
 */
export const getIndices = async (params = {}) => {
  const queryParams = new URLSearchParams();

  // Default to large limit to fetch all indices for client-side pagination
  queryParams.append('limit', params.limit || 100);

  Object.entries(params).forEach(([key, value]) => {
    if (key !== 'limit' && value !== undefined && value !== null && String(value).trim() !== '') {
      queryParams.append(key, value);
    }
  });

  const url = `${API_BASE}indices?${queryParams.toString()}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(data, response.status);
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Fetches index detail with first 100 members.
 * @param {string} name - Index name.
 * @returns {Promise<object>} - Index detail response: { index, members }
 */
export const getIndexDetail = async (name) => {
  const response = await fetch(`${API_BASE}indices/${encodeURIComponent(name)}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(data, response.status);
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Fetches paginated members for an index.
 * @param {string} name - Index name.
 * @param {object} params - Query parameters.
 * @param {number} [params.limit=100] - Number of items per page.
 * @param {number} [params.offset=0] - Starting index.
 * @param {string} [params.sort_by] - Column to sort by.
 * @param {string} [params.sort_order] - Sort order ('asc' or 'desc').
 * @returns {Promise<object>} - Members response: { items, total_items, limit, offset }
 */
export const getIndexMembers = async (name, params = {}) => {
  const queryParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      queryParams.append(key, value);
    }
  });

  const queryString = queryParams.toString();
  const url = `${API_BASE}indices/${encodeURIComponent(name)}/members${queryString ? `?${queryString}` : ''}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(data, response.status);
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Fetches membership change history for an index (timeline view).
 * @param {string} name - Index name.
 * @returns {Promise<object>} - History response with changes grouped by date.
 */
export const getIndexHistory = async (name) => {
  const url = `${API_BASE}indices/${encodeURIComponent(name)}/history`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(data, response.status);
    throw new Error(errorMessage);
  }

  return data;
};

/**
 * Creates a new UserIndex.
 * @param {object} data - Index creation data.
 * @param {string} data.name - Index name (required, unique).
 * @param {string} [data.description] - Index description (optional).
 * @returns {Promise<object>} - Created IndexItem.
 */
export const createUserIndex = async (data) => {
  const response = await fetch(`${API_BASE}indices`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  });

  const responseData = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(responseData, response.status);
    throw new Error(errorMessage);
  }

  return responseData;
};

/**
 * Updates UserIndex members (full replacement).
 * @param {string} name - Index name.
 * @param {object} data - Members update data.
 * @param {object[]} data.members - Array of member objects.
 * @param {string} data.members[].common_symbol - Common symbol (required).
 * @param {number} [data.members[].weight] - Weight as decimal (optional, must be > 0).
 * @returns {Promise<object>} - Updated IndexMembersResponse.
 */
export const updateUserIndexMembers = async (name, data) => {
  const response = await fetch(`${API_BASE}indices/${encodeURIComponent(name)}/members`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  });

  const responseData = await response.json();

  if (!response.ok) {
    const errorMessage = formatErrorMessage(responseData, response.status);
    throw new Error(errorMessage);
  }

  return responseData;
};

/**
 * Deletes a UserIndex.
 * @param {string} name - Index name.
 * @returns {Promise<void>}
 */
export const deleteUserIndex = async (name) => {
  const response = await fetch(`${API_BASE}indices/${encodeURIComponent(name)}`, {
    method: 'DELETE',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  // Handle 204 No Content (successful deletion)
  if (response.status === 204) {
    return;
  }

  // If not 204, attempt to parse error response
  let responseData;
  const contentType = response.headers.get('content-type');
  if (contentType && contentType.indexOf('application/json') !== -1) {
    responseData = await response.json();
  } else {
    responseData = { message: response.statusText, status: response.status };
  }

  if (!response.ok) {
    const errorMessage = formatErrorMessage(responseData, response.status);
    throw new Error(errorMessage);
  }

  return responseData;
};