/**
 * Returns badge color for class type.
 * @param {string} class_type - 'provider' or 'broker'
 * @returns {string} CoreUI badge color
 */
export const getClassBadge = (class_type) => {
  switch (class_type) {
    case 'provider':
      return 'primary'
    case 'broker':
      return 'secondary'
    default:
      return 'primary'
  }
}

/**
 * Returns badge color for active status.
 * @param {boolean} is_active
 * @returns {string} CoreUI badge color
 */
export const getActiveBadge = (is_active) => {
  return is_active ? 'success' : 'danger'
}
