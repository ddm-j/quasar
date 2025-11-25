const BASE_URL = 'http://localhost:8080/' // API URL

export const updateAssetsForClass = async (classType, className) => {
    const response = await fetch(`${BASE_URL}internal/${encodeURIComponent(classType)}/${encodeURIComponent(className)}/update-assets`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();


    if (!response.ok) {
        // Use message from responseData if available, otherwise use a generic error
        const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}

export const updateAllAssets = async () => {
    const response = await fetch(`${BASE_URL}internal/update-all-assets`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();


    if (!response.ok) {
        // Use message from responseData if available, otherwise use a generic error
        const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
        throw new Error(errorMessage);
    }

    return data;
}
export const getRegisteredClasses = async () => {
    const response = await fetch(`${BASE_URL}internal/classes/summary`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        },
    });
    const data = await response.json();

    if (!response.ok) {
        // Use message from responseData if available, otherwise use a generic error
        const errorMessage = responseData.error || responseData.message || `HTTP error! status: ${response.status}`;
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

  const response = await fetch(`${BASE_URL}internal/${encodeURIComponent(classType)}/upload`, {
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
  const response = await fetch(`${BASE_URL}internal/delete/${encodeURIComponent(classType)}/${encodeURIComponent(className)}`, {
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

export const getAssetMappings = async () => {
  const response = await fetch(`${BASE_URL}internal/asset-mappings`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  });

  const data = await response.json();

  if (!response.ok) {
    // Use message from responseData if available, otherwise use a generic error
    const errorMessage = data.error || data.message || `HTTP error! status: ${response.status}`;
    throw new Error(errorMessage);
  }

  return data;
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
  const url = `${BASE_URL}internal/assets${queryString ? `?${queryString}` : ''}`;

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