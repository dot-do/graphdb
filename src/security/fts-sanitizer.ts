/**
 * FTS5 Query Sanitizer
 *
 * Sanitizes user input before using it in FTS5 MATCH clauses to prevent
 * SQL injection and query manipulation attacks while preserving legitimate
 * FTS5 query syntax.
 *
 * Supported FTS5 syntax (preserved):
 * - Boolean operators: AND, OR, NOT
 * - Prefix search: term*
 * - Phrase search: "term1 term2"
 * - Grouping: (term1 OR term2)
 *
 * Blocked/sanitized (for security):
 * - Column filter: column:term (prevents targeting specific columns)
 * - Negation at start: -term (use NOT instead)
 * - Proximity: NEAR/n (complex and rarely needed)
 * - Start of field: ^term
 * - SQL injection attempts
 *
 * @see https://www.sqlite.org/fts5.html#full_text_query_syntax
 */

/**
 * Error thrown when FTS query sanitization fails
 */
export class FtsSanitizationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FtsSanitizationError';
  }
}

/**
 * Maximum total query length
 */
const MAX_QUERY_LENGTH = 1000;

/**
 * Maximum number of search terms/tokens
 */
const MAX_TOKENS = 100;

/**
 * SQL keywords that should be stripped to prevent injection
 */
const SQL_KEYWORDS = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TABLE', 'UNION', 'EXEC', 'EXECUTE', 'CREATE', 'ALTER', 'TRUNCATE'];

/**
 * SQL comment patterns
 */
const SQL_COMMENTS = /--.*$|\/\*[\s\S]*?\*\//gm;

/**
 * Zero-width and invisible unicode characters
 * Includes:
 * - Zero-width space (U+200B)
 * - Zero-width non-joiner (U+200C)
 * - Zero-width joiner (U+200D)
 * - Zero-width no-break space (U+FEFF)
 * - Soft hyphen (U+00AD)
 * - RTL/LTR override and embedding characters (U+202A-U+202E)
 * - Pop directional formatting (U+202C)
 * - Right-to-left mark (U+200F)
 * - Left-to-right mark (U+200E)
 */
const ZERO_WIDTH_CHARS = /[\u200B-\u200F\u202A-\u202E\uFEFF\u00AD]/g;

/**
 * Null bytes
 */
const NULL_BYTES = /\x00/g;

/**
 * Common diacritics/accented character mappings to ASCII equivalents.
 * This enables searches to match content regardless of accent usage.
 */
const DIACRITICS_MAP: Record<string, string> = {
  'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a', 'ã': 'a', 'å': 'a', 'ā': 'a', 'ă': 'a', 'ą': 'a',
  'Á': 'A', 'À': 'A', 'Â': 'A', 'Ä': 'A', 'Ã': 'A', 'Å': 'A', 'Ā': 'A', 'Ă': 'A', 'Ą': 'A',
  'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e', 'ē': 'e', 'ė': 'e', 'ę': 'e', 'ě': 'e',
  'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E', 'Ē': 'E', 'Ė': 'E', 'Ę': 'E', 'Ě': 'E',
  'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i', 'ī': 'i', 'į': 'i', 'ı': 'i',
  'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I', 'Ī': 'I', 'Į': 'I', 'İ': 'I',
  'ó': 'o', 'ò': 'o', 'ô': 'o', 'ö': 'o', 'õ': 'o', 'ø': 'o', 'ō': 'o', 'ő': 'o',
  'Ó': 'O', 'Ò': 'O', 'Ô': 'O', 'Ö': 'O', 'Õ': 'O', 'Ø': 'O', 'Ō': 'O', 'Ő': 'O',
  'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u', 'ū': 'u', 'ů': 'u', 'ű': 'u', 'ų': 'u',
  'Ú': 'U', 'Ù': 'U', 'Û': 'U', 'Ü': 'U', 'Ū': 'U', 'Ů': 'U', 'Ű': 'U', 'Ų': 'U',
  'ý': 'y', 'ÿ': 'y', 'ŷ': 'y',
  'Ý': 'Y', 'Ÿ': 'Y', 'Ŷ': 'Y',
  'ñ': 'n', 'ń': 'n', 'ň': 'n', 'ņ': 'n',
  'Ñ': 'N', 'Ń': 'N', 'Ň': 'N', 'Ņ': 'N',
  'ç': 'c', 'ć': 'c', 'č': 'c', 'ĉ': 'c',
  'Ç': 'C', 'Ć': 'C', 'Č': 'C', 'Ĉ': 'C',
  'ß': 'ss',
  'ś': 's', 'š': 's', 'ş': 's',
  'Ś': 'S', 'Š': 'S', 'Ş': 'S',
  'ź': 'z', 'ž': 'z', 'ż': 'z',
  'Ź': 'Z', 'Ž': 'Z', 'Ż': 'Z',
  'ł': 'l', 'ľ': 'l', 'ļ': 'l',
  'Ł': 'L', 'Ľ': 'L', 'Ļ': 'L',
  'ř': 'r', 'ŕ': 'r',
  'Ř': 'R', 'Ŕ': 'R',
  'ť': 't', 'ţ': 't',
  'Ť': 'T', 'Ţ': 'T',
  'đ': 'd', 'ď': 'd',
  'Đ': 'D', 'Ď': 'D',
  'ğ': 'g', 'ĝ': 'g',
  'Ğ': 'G', 'Ĝ': 'G',
  'ĥ': 'h',
  'Ĥ': 'H',
  'ĵ': 'j',
  'Ĵ': 'J',
  'ķ': 'k',
  'Ķ': 'K',
  'æ': 'ae', 'Æ': 'AE',
  'œ': 'oe', 'Œ': 'OE',
  'ð': 'd', 'Ð': 'D',
  'þ': 'th', 'Þ': 'TH',
};

/**
 * Pattern matching all diacritics characters in our map
 */
const DIACRITICS_PATTERN = new RegExp(`[${Object.keys(DIACRITICS_MAP).join('')}]`, 'g');

/**
 * Normalizes diacritics/accented characters to ASCII equivalents
 */
function normalizeDiacritics(text: string): string {
  return text.replace(DIACRITICS_PATTERN, (char) => DIACRITICS_MAP[char] || char);
}

/**
 * Characters that are dangerous and should always be removed
 * (excludes FTS5 syntax chars like " * ( ) that we want to preserve)
 */
const DANGEROUS_CHARS = /['\{\}\[\];]/g;

/**
 * HTML-like tags pattern
 * Strips anything that looks like an HTML tag: <...>
 * This prevents XSS payloads in search queries
 */
const HTML_TAGS = /<[^>]*>/g;

/**
 * Event handler attributes pattern (for partially stripped HTML)
 * Matches common XSS event handlers
 */
const EVENT_HANDLERS = /\b(on\w+)\s*=/gi;

/**
 * Column filter pattern (e.g., column:term) - security risk
 */
const COLUMN_FILTER = /\b\w+:/g;

/**
 * NEAR proximity operator pattern (e.g., NEAR/5)
 */
const NEAR_PATTERN = /\bNEAR(\/\d+)?\b/gi;

/**
 * Start of field operator
 */
const START_OF_FIELD = /\^/g;

/**
 * Leading negation (e.g., -term at start or after space)
 */
const LEADING_NEGATION = /(?:^|\s)-(?=\w)/g;

/**
 * Sanitizes an FTS5 query string to prevent injection attacks while
 * preserving legitimate FTS5 query syntax.
 *
 * This function preserves:
 * - Double quotes for phrase search: "quick brown fox"
 * - Asterisks for prefix search: java*
 * - Parentheses for grouping: (web OR mobile)
 * - Boolean operators: AND, OR, NOT
 *
 * This function removes/sanitizes:
 * - Null bytes
 * - Zero-width and bidirectional unicode characters (RTL/LTR overrides)
 * - SQL comments (-- and /*)
 * - HTML tags and event handlers (XSS prevention)
 * - SQL injection keywords
 * - Column filters (column:term)
 * - NEAR proximity operator
 * - Start of field operator (^)
 * - Dangerous characters: ' { } [ ] ;
 *
 * @param query - The raw user input query string
 * @returns Sanitized query string safe for use in FTS5 MATCH
 */
export function sanitizeFtsQuery(query: string): string {
  if (!query || typeof query !== 'string') {
    return '';
  }

  let sanitized = query;

  // Step 1: Remove null bytes
  sanitized = sanitized.replace(NULL_BYTES, '');

  // Step 2: Remove zero-width and invisible unicode characters
  sanitized = sanitized.replace(ZERO_WIDTH_CHARS, '');

  // Step 2b: Normalize diacritics/accented characters to ASCII equivalents
  sanitized = normalizeDiacritics(sanitized);

  // Step 3: Remove SQL comments (-- and /* */)
  sanitized = sanitized.replace(SQL_COMMENTS, ' ');

  // Step 4: Remove HTML tags (XSS prevention)
  sanitized = sanitized.replace(HTML_TAGS, ' ');

  // Step 4b: Remove event handler patterns (XSS prevention)
  sanitized = sanitized.replace(EVENT_HANDLERS, ' ');

  // Step 6: Remove dangerous characters (but keep FTS5 syntax chars)
  sanitized = sanitized.replace(DANGEROUS_CHARS, ' ');

  // Step 7: Remove column filter pattern (security risk)
  sanitized = sanitized.replace(COLUMN_FILTER, ' ');

  // Step 8: Remove NEAR proximity operator
  sanitized = sanitized.replace(NEAR_PATTERN, ' ');

  // Step 9: Remove start of field operator
  sanitized = sanitized.replace(START_OF_FIELD, '');

  // Step 10: Convert leading negation to space (use NOT instead)
  sanitized = sanitized.replace(LEADING_NEGATION, ' ');

  // Step 11: Remove SQL keywords (but preserve FTS5 operators like AND, OR, NOT)
  for (const keyword of SQL_KEYWORDS) {
    const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
    sanitized = sanitized.replace(regex, ' ');
  }

  // Step 12: Balance quotes - ensure even number of double quotes
  const quoteCount = (sanitized.match(/"/g) || []).length;
  if (quoteCount % 2 !== 0) {
    // Remove all quotes if unbalanced (safer than trying to fix)
    sanitized = sanitized.replace(/"/g, ' ');
  }

  // Step 13: Balance parentheses
  let openParens = 0;
  let closeParens = 0;
  for (const char of sanitized) {
    if (char === '(') openParens++;
    if (char === ')') closeParens++;
  }
  if (openParens !== closeParens) {
    // Remove all parentheses if unbalanced
    sanitized = sanitized.replace(/[()]/g, ' ');
  }

  // Step 14: Normalize whitespace
  sanitized = sanitized.replace(/\s+/g, ' ').trim();

  // Step 15: Limit total query length
  if (sanitized.length > MAX_QUERY_LENGTH) {
    // Truncate at word boundary if possible
    const truncated = sanitized.slice(0, MAX_QUERY_LENGTH);
    const lastSpace = truncated.lastIndexOf(' ');
    if (lastSpace > MAX_QUERY_LENGTH * 0.8) {
      sanitized = truncated.slice(0, lastSpace);
    } else {
      sanitized = truncated;
    }
  }

  // Step 16: Limit number of tokens (rough approximation)
  const tokens = sanitized.split(/\s+/);
  if (tokens.length > MAX_TOKENS) {
    sanitized = tokens.slice(0, MAX_TOKENS).join(' ');
  }

  return sanitized;
}

/**
 * Validates that a query is safe for FTS5 MATCH.
 *
 * Returns true if the query passes all safety checks.
 * Use this for additional validation after sanitization.
 *
 * @param query - The query to validate
 * @returns true if query is safe, false otherwise
 */
export function isValidFtsQuery(query: string): boolean {
  if (!query || typeof query !== 'string') {
    return false;
  }

  // Check for null bytes
  if (NULL_BYTES.test(query)) {
    return false;
  }

  // Check for SQL comments
  if (/--/.test(query) || /\/\*/.test(query)) {
    return false;
  }

  // Check for unbalanced quotes
  const doubleQuotes = (query.match(/"/g) || []).length;
  const singleQuotes = (query.match(/'/g) || []).length;
  if (doubleQuotes % 2 !== 0 || singleQuotes % 2 !== 0) {
    return false;
  }

  // Check for dangerous FTS5 patterns
  if (/^\s*\*\s*$/.test(query)) {
    // Standalone asterisk matches everything
    return false;
  }

  // Check for column injection (word:word pattern)
  if (/\w+:\w+/.test(query)) {
    return false;
  }

  return true;
}
