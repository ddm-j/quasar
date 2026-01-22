// API base path - uses relative URL for proxy compatibility (dev: Vite proxy, prod: nginx/ALB)
const API_BASE = '/api/datahub/'

/**
 * Safely parse JSON response with Content-Type validation.
 * @param {Response} response - Fetch response object
 * @returns {Promise<object>} - Parsed JSON data
 * @throws {Error} - If response is not JSON or parsing fails
 */
const parseJSONResponse = async (response) => {
  const contentType = response.headers.get('content-type')

  // Read response as text first (response body can only be read once)
  const text = await response.text()

  if (!contentType || !contentType.includes('application/json')) {
    throw new Error(
      `Expected JSON response, got ${contentType || 'unknown content type'}. Response: ${text.substring(0, 200)}`,
    )
  }

  // Try to parse the text as JSON
  try {
    return JSON.parse(text)
  } catch (err) {
    throw new Error(
      `Failed to parse JSON response: ${err.message}. Response: ${text.substring(0, 200)}`,
    )
  }
}

/**
 * Validation helper functions
 */
const validateString = (value, name, required = true) => {
  if (required && (!value || typeof value !== 'string' || value.trim().length === 0)) {
    throw new Error(`${name} is required and must be a non-empty string`)
  }
  if (value && typeof value !== 'string') {
    throw new Error(`${name} must be a string`)
  }
  return value?.trim()
}

const validateEnum = (value, name, allowedValues, required = true) => {
  if (required && !value) {
    throw new Error(`${name} is required`)
  }
  if (value && !allowedValues.includes(value)) {
    throw new Error(`${name} must be one of: ${allowedValues.join(', ')}`)
  }
  return value
}

const validateNumber = (value, name, min, max, required = false) => {
  if (required && (value === undefined || value === null)) {
    throw new Error(`${name} is required`)
  }
  if (value !== undefined && value !== null) {
    const num = Number(value)
    if (isNaN(num)) {
      throw new Error(`${name} must be a number`)
    }
    if (min !== undefined && num < min) {
      throw new Error(`${name} must be at least ${min}`)
    }
    if (max !== undefined && num > max) {
      throw new Error(`${name} must be at most ${max}`)
    }
  }
  return value
}

/**
 * Search for symbols by common symbol, provider symbol, or asset name.
 * @param {string} query - Search query string
 * @param {object} options - Optional search parameters
 * @param {string} [options.data_type] - Filter by data type: 'historical' or 'live'
 * @param {string} [options.provider] - Filter by provider class name
 * @param {number} [options.limit=50] - Maximum number of results (1-200)
 * @returns {Promise<object>} - The API response with items array and total count
 */
export const searchSymbols = async (query, options = {}) => {
  // Validate inputs
  const validatedQuery = validateString(query, 'query', true)

  const validatedOptions = {}
  if (options.data_type !== undefined) {
    validatedOptions.data_type = validateEnum(
      options.data_type,
      'data_type',
      ['historical', 'live'],
      false,
    )
  }
  if (options.provider !== undefined) {
    validatedOptions.provider = validateString(options.provider, 'provider', false)
  }
  if (options.limit !== undefined) {
    validatedOptions.limit = validateNumber(options.limit, 'limit', 1, 200, false)
  }

  const params = new URLSearchParams({
    q: validatedQuery,
  })

  if (validatedOptions.data_type) {
    params.append('data_type', validatedOptions.data_type)
  }
  if (validatedOptions.provider) {
    params.append('provider', validatedOptions.provider)
  }
  if (validatedOptions.limit !== undefined) {
    params.append('limit', validatedOptions.limit.toString())
  }

  const response = await fetch(`${API_BASE}symbols/search?${params.toString()}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })

  let data
  try {
    data = await parseJSONResponse(response)
  } catch (err) {
    // If parsing fails, we still want to check response.ok for HTTP errors
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${err.message}`)
    }
    throw err
  }

  if (!response.ok) {
    const errorMessage =
      data.detail || data.error || data.message || `HTTP error! status: ${response.status}`
    throw new Error(errorMessage)
  }

  return data
}

/**
 * Retrieve OHLC data for a specific symbol/provider combination.
 * @param {string} provider - Provider class name
 * @param {string} symbol - Provider-specific symbol
 * @param {string} dataType - Data type: 'historical' or 'live'
 * @param {string} interval - Interval string
 * @param {object} options - Optional parameters
 * @param {number} [options.limit=500] - Number of bars to return (1-5000)
 * @param {string|number} [options.from_time] - Start time (ISO 8601 or Unix timestamp)
 * @param {string|number} [options.to_time] - End time (ISO 8601 or Unix timestamp)
 * @param {string} [options.order='desc'] - Order: 'asc' or 'desc'
 * @returns {Promise<object>} - The API response with bars array and metadata
 */
export const getOHLCData = async (provider, symbol, dataType, interval, options = {}) => {
  // Validate required inputs
  const validatedProvider = validateString(provider, 'provider', true)
  const validatedSymbol = validateString(symbol, 'symbol', true)
  const validatedDataType = validateEnum(dataType, 'data_type', ['historical', 'live'], true)
  const validatedInterval = validateString(interval, 'interval', true)

  // Validate optional inputs
  const validatedOptions = {}
  if (options.limit !== undefined) {
    validatedOptions.limit = validateNumber(options.limit, 'limit', 1, 5000, false)
  }
  if (options.order !== undefined) {
    validatedOptions.order = validateEnum(options.order, 'order', ['asc', 'desc'], false)
  }
  // from_time and to_time are validated by backend, but we ensure they're strings/numbers
  if (options.from_time !== undefined && options.from_time !== null) {
    validatedOptions.from_time = options.from_time
  }
  if (options.to_time !== undefined && options.to_time !== null) {
    validatedOptions.to_time = options.to_time
  }

  const params = new URLSearchParams({
    provider: validatedProvider,
    symbol: validatedSymbol,
    data_type: validatedDataType,
    interval: validatedInterval,
  })

  if (validatedOptions.limit !== undefined) {
    params.append('limit', validatedOptions.limit.toString())
  }
  if (validatedOptions.from_time !== undefined) {
    params.append('from', validatedOptions.from_time.toString())
  }
  if (validatedOptions.to_time !== undefined) {
    params.append('to', validatedOptions.to_time.toString())
  }
  if (validatedOptions.order) {
    params.append('order', validatedOptions.order)
  }

  const response = await fetch(`${API_BASE}data?${params.toString()}`, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })

  let data
  try {
    data = await parseJSONResponse(response)
  } catch (err) {
    // If parsing fails, we still want to check response.ok for HTTP errors
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${err.message}`)
    }
    throw err
  }

  if (!response.ok) {
    const errorMessage =
      data.detail || data.error || data.message || `HTTP error! status: ${response.status}`
    throw new Error(errorMessage)
  }

  return data
}

/**
 * Get detailed metadata for a specific symbol/provider combination.
 * @param {string} provider - Provider class name
 * @param {string} symbol - Provider-specific symbol
 * @param {string} [dataType] - Optional filter to 'historical' or 'live' (default: both)
 * @returns {Promise<object>} - The API response with metadata
 */
export const getSymbolMetadata = async (provider, symbol, dataType = null) => {
  // Validate required inputs
  const validatedProvider = validateString(provider, 'provider', true)
  const validatedSymbol = validateString(symbol, 'symbol', true)

  // Validate optional dataType
  if (dataType !== null && dataType !== undefined) {
    validateEnum(dataType, 'data_type', ['historical', 'live'], false)
  }

  const params = new URLSearchParams()

  if (dataType) {
    params.append('data_type', dataType)
  }

  const queryString = params.toString()
  const url = `${API_BASE}symbols/${encodeURIComponent(validatedProvider)}/${encodeURIComponent(validatedSymbol)}${queryString ? `?${queryString}` : ''}`

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
    },
  })

  let data
  try {
    data = await parseJSONResponse(response)
  } catch (err) {
    // If parsing fails, we still want to check response.ok for HTTP errors
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${err.message}`)
    }
    throw err
  }

  if (!response.ok) {
    const errorMessage =
      data.detail || data.error || data.message || `HTTP error! status: ${response.status}`
    throw new Error(errorMessage)
  }

  return data
}
