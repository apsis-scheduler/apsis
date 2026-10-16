const FAVICON = '/static/favicon.svg'

/**
 * Sets the page title to `parts` followed by "apsis", skipping empty parts.
 *
 * @param {...(string | null | undefined)} parts - most specific first
 */
export function setTitle(...parts) {
  document.title = [...parts.filter(p => p), 'apsis'].join(' - ')
}

/**
 * Sets the page favicon to an svg element, or to the apsis icon if null.
 *
 * @param {SVGSVGElement | null} svg - must carry its own xmlns attribute
 */
export function setFavicon(svg) {
  document.querySelector('link[rel="icon"]').href = svg
    ? 'data:image/svg+xml,' + encodeURIComponent(svg.outerHTML)
    : FAVICON
}
