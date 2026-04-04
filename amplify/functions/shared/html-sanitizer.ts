/**
 * Server-side HTML sanitiser for newsletter body content.
 *
 * Implements an allowlist approach — strips everything not explicitly permitted.
 * No external dependencies; works in Node.js Lambda without a DOM.
 *
 * Allowed tags: p, br, strong, b, em, i, u, s, h2, h3, ul, ol, li, a, blockquote, hr, pre, code
 * Allowed attributes: href (a only, https:// required), target (a only)
 * All other tags and attributes are removed.
 * Script/style/iframe content is removed entirely including inner content.
 */

const VOID_TAGS = new Set(['br', 'hr', 'img', 'input', 'meta', 'link', 'area', 'base', 'col', 'embed', 'param', 'source', 'track', 'wbr']);

const ALLOWED_TAGS = new Set([
  'p', 'br', 'strong', 'b', 'em', 'i', 'u', 's', 'strike',
  'h2', 'h3', 'ul', 'ol', 'li',
  'a', 'blockquote', 'hr', 'pre', 'code', 'span',
]);

/** Tags whose entire content (opening tag + children + closing tag) should be removed */
const STRIP_CONTENT_TAGS = new Set(['script', 'style', 'iframe', 'object', 'embed', 'form', 'input', 'button']);

/**
 * Sanitises an HTML string for safe storage and rendering.
 * Safe to call on untrusted brand input before writing to DynamoDB.
 */
export function sanitizeHtml(html: string): string {
  if (!html || typeof html !== 'string') return '';

  let result = html;

  // 1. Remove entire content of dangerous tags
  for (const tag of STRIP_CONTENT_TAGS) {
    result = result.replace(new RegExp(`<${tag}[^>]*>[\\s\\S]*?<\\/${tag}>`, 'gi'), '');
    result = result.replace(new RegExp(`<${tag}[^>]*/?>`, 'gi'), '');
  }

  // 2. Remove HTML comments
  result = result.replace(/<!--[\s\S]*?-->/g, '');

  // 3. Process remaining tags
  result = result.replace(/<\/?([a-zA-Z][a-zA-Z0-9]*)((?:\s[^>]*)?)\/?>/g, (match, tagName: string, attrs: string) => {
    const tag = tagName.toLowerCase();

    if (!ALLOWED_TAGS.has(tag)) {
      // Unknown tag — strip the tag but keep inner content (except void tags)
      return VOID_TAGS.has(tag) ? '' : '';
    }

    // Closing tag — always allow if tag is in allowlist
    if (match.startsWith('</')) return `</${tag}>`;

    // Void tag — self-closing, no attributes needed for most
    if (tag === 'br') return '<br>';
    if (tag === 'hr') return '<hr>';

    // <a> tag — only allow href with https:// and target="_blank"
    if (tag === 'a') {
      const hrefMatch = /\s+href\s*=\s*["']([^"']*)["']/i.exec(attrs);
      const href = hrefMatch?.[1]?.trim() ?? '';
      if (!href.startsWith('https://') && !href.startsWith('http://')) return ''; // strip unsafe links
      // Force safe: https only, target blank, noopener
      const safeHref = href.startsWith('https://') ? href : href.replace(/^http:\/\//, 'https://');
      return `<a href="${escapeAttr(safeHref)}" target="_blank" rel="noopener noreferrer">`;
    }

    // All other allowed tags: strip all attributes
    return `<${tag}>`;
  });

  // 4. Strip any remaining inline event handlers (belt-and-suspenders)
  result = result.replace(/\s+on\w+\s*=\s*["'][^"']*["']/gi, '');
  result = result.replace(/\s+on\w+\s*=\s*[^\s>]*/gi, '');

  // 5. Strip javascript: protocol anywhere
  result = result.replace(/javascript\s*:/gi, '');

  // 6. Strip data: URIs
  result = result.replace(/data\s*:[^;>]*/gi, '');

  return result.trim();
}

function escapeAttr(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * Returns true if the string is safe (sanitisation produced no changes).
 * Use for logging/alerting when brands submit suspicious content.
 */
export function isHtmlClean(html: string): boolean {
  return sanitizeHtml(html) === html;
}
