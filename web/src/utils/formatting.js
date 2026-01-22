/**
 * Format a date string for display.
 * @param {string|null} dateString - ISO date string
 * @param {boolean} includeTime - Whether to include time in output
 * @returns {string} Formatted date string or '—' if null
 */
export const formatDate = (dateString, includeTime = true) => {
  if (!dateString) return '—'
  const date = new Date(dateString)
  if (isNaN(date.getTime())) return '—'
  const options = {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  }
  if (includeTime) {
    options.hour = '2-digit'
    options.minute = '2-digit'
  }
  return date.toLocaleDateString('en-US', options)
}

/**
 * Format a weight (decimal 0-1) as a percentage string.
 * @param {number|null} weight - Weight as decimal (e.g., 0.15 for 15%)
 * @returns {string} Formatted percentage string or '—' if null
 */
export const formatWeight = (weight) => {
  if (weight === null || weight === undefined) return '—'
  return (weight * 100).toFixed(1) + '%'
}

/**
 * Format a weight for input field display (decimal to percentage number).
 * @param {number|null} weight - Weight as decimal (e.g., 0.15 for 15%)
 * @returns {string} Percentage value as string or empty string if null
 */
export const formatWeightForInput = (weight) => {
  if (weight === null || weight === undefined) return ''
  return (weight * 100).toFixed(1)
}
