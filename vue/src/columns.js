const KEY = 'apsis.runsList.columnWidths'

// default pixel widths, "arg" covers each "arg:<param>" column
const DEFAULT_WIDTHS = {
  job: 320,
  args: 260,
  arg: 160,
  run: 100,
  state: 72,
  group: 115,
  schedule: 150,
  start: 150,
  elapsed: 90,
  stop: 150,
  operations: 110,
}

export const MIN_WIDTH = 40

/**
 * Loads saved column widths, or none if storage is unavailable or invalid.
 *
 * @returns {Object<string, number>} widths by column name
 */
export function loadWidths() {
  try {
    const widths = JSON.parse(localStorage.getItem(KEY))
    return widths !== null && typeof widths === 'object' && !Array.isArray(widths) ? widths : {}
  } catch (e) {
    return {}
  }
}

/**
 * Saves column widths, ignoring storage errors.
 *
 * @param {Object<string, number>} widths - widths by column name
 */
export function saveWidths(widths) {
  try {
    localStorage.setItem(KEY, JSON.stringify(widths))
  } catch (e) {
    // storage is full or disabled
  }
}

/**
 * Returns the width of `column`, the saved one if valid else its default.
 *
 * @param {Object<string, number>} widths - saved widths by column name
 * @param {string} column - column name, e.g. "job" or "arg:date"
 */
export function columnWidth(widths, column) {
  const width = widths[column]
  if (Number.isFinite(width))
    return Math.max(MIN_WIDTH, width)
  return DEFAULT_WIDTHS[column.startsWith('arg:') ? 'arg' : column]
}

/**
 * Returns the width of a column dragged `dx` pixels from `start` wide.
 */
export function dragWidth(start, dx) {
  return Math.max(MIN_WIDTH, Math.round(start + dx))
}
