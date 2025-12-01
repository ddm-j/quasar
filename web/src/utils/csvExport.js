/**
 * CSV Export Utility Functions
 * 
 * Provides functions to generate and download OHLCV data as CSV files.
 */

/**
 * Format Unix timestamp to ISO 8601 date string for CSV
 * @param {number} timestamp - Unix timestamp in seconds
 * @returns {string} ISO 8601 formatted date string
 */
const formatTimestampForCSV = (timestamp) => {
  const date = new Date(timestamp * 1000) // Convert seconds to milliseconds
  return date.toISOString()
}

/**
 * Escape CSV value to prevent CSV injection and properly handle special characters.
 * This function:
 * 1. Prevents CSV injection by prefixing dangerous formula characters with a tab
 * 2. Properly escapes quotes and commas by quoting the value
 * 3. Handles newlines within values
 * 
 * CSV Injection Prevention: Values starting with =, +, -, @, or tab are prefixed with
 * a tab character to prevent Excel from interpreting them as formulas.
 * 
 * @param {string|number} value - The value to escape
 * @returns {string} Properly escaped CSV value
 */
const escapeCSVValue = (value) => {
  // Convert to string and handle null/undefined
  const str = value == null ? '' : String(value)
  
  // Check for CSV injection risk: values starting with =, +, -, @, or tab
  // These characters can be interpreted as formulas in Excel
  const dangerousChars = ['=', '+', '-', '@', '\t']
  const needsInjectionProtection = dangerousChars.some(char => str.startsWith(char))
  
  // Check if value needs quoting per RFC 4180 (contains comma, quote, or newline)
  const needsQuoting = str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')
  
  let escaped = str
  
  // If value needs injection protection, prefix with tab character
  // This prevents Excel from interpreting it as a formula while keeping it readable
  if (needsInjectionProtection) {
    escaped = `\t${escaped}`
  }
  
  // If value contains quotes, double them (RFC 4180 standard)
  // Do this after adding tab prefix so quotes in original value are handled
  if (escaped.includes('"')) {
    escaped = escaped.replace(/"/g, '""')
  }
  
  // If value needs quoting (per RFC 4180) or has injection protection, wrap in quotes
  // The tab prefix will be inside the quotes, which is correct
  if (needsQuoting || needsInjectionProtection) {
    escaped = `"${escaped}"`
  }
  
  return escaped
}

/**
 * Generate CSV content from OHLCV data
 * @param {Array} data - Array of OHLC bar objects with { time, open, high, low, close, volume }
 * @returns {string} CSV content as string
 */
export const generateCSV = (data) => {
  if (!data || data.length === 0) {
    return ''
  }

  // CSV Headers
  const headers = 'date,open,high,low,close,volume\n'

  // CSV Rows - sort by time ascending (oldest first) for logical CSV order
  const rows = data
    .sort((a, b) => a.time - b.time)
    .map(bar => {
      const datetime = formatTimestampForCSV(bar.time)
      // Escape all values to prevent CSV injection and handle special characters
      const open = escapeCSVValue(bar.open ?? '')
      const high = escapeCSVValue(bar.high ?? '')
      const low = escapeCSVValue(bar.low ?? '')
      const close = escapeCSVValue(bar.close ?? '')
      const volume = escapeCSVValue(bar.volume ?? '')
      
      return `${datetime},${open},${high},${low},${close},${volume}`
    })
    .join('\n')

  return headers + rows
}

/**
 * Generate a filename for the CSV download
 * @param {Object} symbol - Symbol object with common_symbol, provider, provider_symbol
 * @param {string} dataType - Data type: 'historical' or 'live'
 * @param {string} interval - Time interval string
 * @returns {string} Generated filename
 */
export const generateFilename = (symbol, dataType, interval) => {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5)
  const symbolName = symbol?.common_symbol || symbol?.provider_symbol || 'data'
  const sanitizedSymbol = symbolName.replace(/[^a-zA-Z0-9-_]/g, '_')
  const sanitizedInterval = (interval || '').replace(/[^a-zA-Z0-9-_]/g, '_')
  
  return `${sanitizedSymbol}_${dataType}_${sanitizedInterval}_${timestamp}.csv`
}

/**
 * Download data as CSV file
 * @param {Array} data - Array of OHLC bar objects
 * @param {Object} symbol - Symbol object
 * @param {string} dataType - Data type: 'historical' or 'live'
 * @param {string} interval - Time interval string
 */
export const downloadCSV = (data, symbol, dataType, interval) => {
  if (!data || data.length === 0) {
    console.warn('No data to download')
    return
  }

  // Generate CSV content
  const csv = generateCSV(data)
  if (!csv) {
    console.warn('Failed to generate CSV content')
    return
  }

  // Generate filename
  const filename = generateFilename(symbol, dataType, interval)

  // Create blob and download
  try {
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  } catch (err) {
    console.error('Error downloading CSV:', err)
    throw err
  }
}

