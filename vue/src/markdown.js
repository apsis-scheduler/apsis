import showdown from 'showdown'

const converter = new showdown.Converter()
const excerptCache = new Map()

/**
 * Renders the first paragraph of description Markdown to HTML, truncated
 * to 256 characters.  Results are cached by description.
 *
 * @param {string} src - description Markdown
 * @returns {string} HTML, or empty string for a blank description
 */
export function descriptionExcerptHtml(src) {
  let html = excerptCache.get(src)
  if (html === undefined) {
    let paragraph = src.split('\n\n')[0]
    if (paragraph.length > 256)
      paragraph = paragraph.substring(0, 256) + '…'
    html = paragraph.trim() === '' ? '' : converter.makeHtml(paragraph)
    excerptCache.set(src, html)
  }
  return html
}
