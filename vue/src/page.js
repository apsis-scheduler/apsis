/**
 * Sets the page title to `parts` followed by "apsis", skipping empty parts.
 *
 * @param {...(string | null | undefined)} parts - most specific first
 */
export function setTitle(...parts) {
  document.title = [...parts.filter(p => p), 'apsis'].join(' - ')
}
