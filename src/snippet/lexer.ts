/**
 * GraphDB Query Lexer/Tokenizer
 *
 * Tokenizes graph query language for snippet-based routing.
 * Must stay under 10KB minified for Cloudflare Snippets compatibility.
 *
 * Supported syntax:
 * - user:123.friends           # Single hop
 * - user:123.friends.posts     # Multi-hop
 * - user:123.friends[?age > 30] # Filter
 * - post:456 <- likes          # Reverse traversal
 * - user:123.friends*[depth <= 3] # Bounded recursion
 */

/**
 * Lexer error codes for programmatic error handling
 */
export const LexerErrorCode = {
  UNEXPECTED_CHARACTER: 'UNEXPECTED_CHARACTER',
  UNTERMINATED_STRING: 'UNTERMINATED_STRING',
  UNEXPECTED_TOKEN: 'UNEXPECTED_TOKEN',
} as const;

export type LexerErrorCodeType = (typeof LexerErrorCode)[keyof typeof LexerErrorCode];

/**
 * Position information for error context
 */
export interface LexerPosition {
  /** Byte offset from start of input */
  offset: number;
  /** Line number (1-indexed) */
  line: number;
  /** Column number (1-indexed) */
  column: number;
}

/**
 * Structured lexer error with full context
 *
 * All lexer errors provide:
 * - code: Machine-readable error code for programmatic handling
 * - message: Human-readable error description
 * - position: Line, column, and byte offset of the error
 * - source: Snippet of source code around the error
 */
export class LexerError extends Error {
  public readonly code: LexerErrorCodeType;
  public readonly position: LexerPosition;
  public readonly source: string;
  public readonly found?: string;
  public readonly expected?: string;

  constructor(
    code: LexerErrorCodeType,
    message: string,
    position: LexerPosition,
    source: string,
    found?: string,
    expected?: string
  ) {
    super(LexerError.formatMessage(code, message, position, source, found, expected));
    this.name = 'LexerError';
    this.code = code;
    this.position = position;
    this.source = source;
    if (found !== undefined) {
      this.found = found;
    }
    if (expected !== undefined) {
      this.expected = expected;
    }
  }

  /**
   * Format a consistent error message with full context
   */
  private static formatMessage(
    code: LexerErrorCodeType,
    message: string,
    position: LexerPosition,
    source: string,
    found?: string,
    expected?: string
  ): string {
    let msg = `[${code}] ${message} at line ${position.line}, column ${position.column}`;
    if (found !== undefined) {
      msg += ` (found: '${found}'`;
      if (expected !== undefined) {
        msg += `, expected: ${expected}`;
      }
      msg += ')';
    }
    if (source) {
      msg += `\n  Source: "${source}"`;
      // Add caret pointing to error position within the source snippet
      const caretOffset = Math.min(position.column - 1, source.length);
      msg += `\n          ${' '.repeat(caretOffset)}^`;
    }
    return msg;
  }

  /**
   * Convert to a plain object for JSON serialization
   */
  toJSON(): {
    code: LexerErrorCodeType;
    message: string;
    position: LexerPosition;
    source: string;
    found?: string;
    expected?: string;
  } {
    const result: {
      code: LexerErrorCodeType;
      message: string;
      position: LexerPosition;
      source: string;
      found?: string;
      expected?: string;
    } = {
      code: this.code,
      message: this.message,
      position: this.position,
      source: this.source,
    };
    if (this.found !== undefined) {
      result.found = this.found;
    }
    if (this.expected !== undefined) {
      result.expected = this.expected;
    }
    return result;
  }
}

/**
 * Calculate line and column from byte offset
 */
function getPositionFromOffset(input: string, offset: number): LexerPosition {
  let line = 1;
  let column = 1;
  for (let i = 0; i < offset && i < input.length; i++) {
    if (input[i] === '\n') {
      line++;
      column = 1;
    } else {
      column++;
    }
  }
  return { offset, line, column };
}

/**
 * Extract source snippet around error position (for context)
 */
function getSourceSnippet(input: string, offset: number, maxLength = 40): string {
  // Find line start
  let lineStart = offset;
  while (lineStart > 0 && input[lineStart - 1] !== '\n') {
    lineStart--;
  }
  // Find line end
  let lineEnd = offset;
  while (lineEnd < input.length && input[lineEnd] !== '\n') {
    lineEnd++;
  }
  const line = input.slice(lineStart, lineEnd);
  // Truncate if too long
  if (line.length > maxLength) {
    const start = Math.max(0, offset - lineStart - maxLength / 2);
    return '...' + line.slice(start, start + maxLength) + '...';
  }
  return line;
}

export enum TokenType {
  // Identifiers
  IDENTIFIER = 'IDENTIFIER',
  NUMBER = 'NUMBER',
  STRING = 'STRING',

  // Operators
  DOT = 'DOT',
  COLON = 'COLON',
  ARROW_LEFT = 'ARROW_LEFT',
  STAR = 'STAR',

  // Brackets
  LBRACKET = 'LBRACKET',
  RBRACKET = 'RBRACKET',
  LBRACE = 'LBRACE',
  RBRACE = 'RBRACE',
  LPAREN = 'LPAREN',
  RPAREN = 'RPAREN',

  // Filter operators
  QUESTION = 'QUESTION',
  GT = 'GT',
  LT = 'LT',
  GTE = 'GTE',
  LTE = 'LTE',
  EQ = 'EQ',
  NEQ = 'NEQ',

  // Keywords
  AND = 'AND',
  OR = 'OR',
  DEPTH = 'DEPTH',
  TRUE = 'TRUE',
  FALSE = 'FALSE',

  // Misc
  COMMA = 'COMMA',
  MINUS = 'MINUS',
  EOF = 'EOF',
}

export interface Token {
  type: TokenType;
  value: string;
  position: number;
}

/** Keywords mapping (case-insensitive) */
const KEYWORDS: Record<string, TokenType> = {
  and: TokenType.AND,
  or: TokenType.OR,
  depth: TokenType.DEPTH,
  true: TokenType.TRUE,
  false: TokenType.FALSE,
};

/** Single character tokens */
const SINGLE_CHAR_TOKENS: Record<string, TokenType> = {
  '.': TokenType.DOT,
  ':': TokenType.COLON,
  '*': TokenType.STAR,
  '[': TokenType.LBRACKET,
  ']': TokenType.RBRACKET,
  '{': TokenType.LBRACE,
  '}': TokenType.RBRACE,
  '(': TokenType.LPAREN,
  ')': TokenType.RPAREN,
  '?': TokenType.QUESTION,
  ',': TokenType.COMMA,
  '-': TokenType.MINUS,
};

function isWhitespace(c: string): boolean {
  return c === ' ' || c === '\t' || c === '\n' || c === '\r';
}

function isDigit(c: string): boolean {
  return c >= '0' && c <= '9';
}

function isAlpha(c: string): boolean {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_';
}

function isAlphaNumeric(c: string): boolean {
  return isAlpha(c) || isDigit(c);
}

/**
 * Tokenize a graph query string into tokens
 */
export function tokenize(query: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;

  function peek(offset = 0): string {
    return query[pos + offset] || '';
  }

  function advance(): string {
    return query[pos++] ?? '';
  }

  function skipWhitespace(): void {
    while (pos < query.length && isWhitespace(peek())) {
      pos++;
    }
  }

  function readString(quote: string): Token {
    const startPos = pos;
    pos++; // skip opening quote
    let value = '';

    while (pos < query.length) {
      const c = peek();
      if (c === '\\' && peek(1) === quote) {
        // Escaped quote
        pos += 2;
        value += quote;
      } else if (c === quote) {
        pos++; // skip closing quote
        return { type: TokenType.STRING, value, position: startPos };
      } else {
        value += advance();
      }
    }

    const position = getPositionFromOffset(query, startPos);
    const source = getSourceSnippet(query, startPos);
    throw new LexerError(
      LexerErrorCode.UNTERMINATED_STRING,
      'Unterminated string literal',
      position,
      source,
      quote === '"' ? 'end of input' : 'end of input',
      `closing ${quote}`
    );
  }

  function readNumber(): Token {
    const startPos = pos;
    let value = '';

    // Handle negative numbers
    if (peek() === '-') {
      value += advance();
    }

    // Integer part
    while (pos < query.length && isDigit(peek())) {
      value += advance();
    }

    // Decimal part
    if (peek() === '.' && isDigit(peek(1))) {
      value += advance(); // decimal point
      while (pos < query.length && isDigit(peek())) {
        value += advance();
      }
    }

    return { type: TokenType.NUMBER, value, position: startPos };
  }

  function readIdentifier(): Token {
    const startPos = pos;
    let value = '';

    while (pos < query.length && isAlphaNumeric(peek())) {
      value += advance();
    }

    // Check if it's a keyword (case-insensitive)
    const keyword = KEYWORDS[value.toLowerCase()];
    if (keyword) {
      return { type: keyword, value, position: startPos };
    }

    return { type: TokenType.IDENTIFIER, value, position: startPos };
  }

  while (pos < query.length) {
    skipWhitespace();

    if (pos >= query.length) break;

    const c = peek();
    const startPos = pos;

    // String literals
    if (c === '"' || c === "'") {
      tokens.push(readString(c));
      continue;
    }

    // Numbers (including negative)
    if (isDigit(c)) {
      tokens.push(readNumber());
      continue;
    }

    // Identifiers and keywords
    if (isAlpha(c)) {
      tokens.push(readIdentifier());
      continue;
    }

    // Two-character operators
    if (c === '<' && peek(1) === '-') {
      pos += 2;
      tokens.push({ type: TokenType.ARROW_LEFT, value: '<-', position: startPos });
      continue;
    }
    if (c === '<' && peek(1) === '=') {
      pos += 2;
      tokens.push({ type: TokenType.LTE, value: '<=', position: startPos });
      continue;
    }
    if (c === '>' && peek(1) === '=') {
      pos += 2;
      tokens.push({ type: TokenType.GTE, value: '>=', position: startPos });
      continue;
    }
    if (c === '!' && peek(1) === '=') {
      pos += 2;
      tokens.push({ type: TokenType.NEQ, value: '!=', position: startPos });
      continue;
    }

    // Single-character operators
    if (c === '<') {
      pos++;
      tokens.push({ type: TokenType.LT, value: '<', position: startPos });
      continue;
    }
    if (c === '>') {
      pos++;
      tokens.push({ type: TokenType.GT, value: '>', position: startPos });
      continue;
    }
    if (c === '=') {
      pos++;
      tokens.push({ type: TokenType.EQ, value: '=', position: startPos });
      continue;
    }

    // Other single-character tokens
    const singleCharType = SINGLE_CHAR_TOKENS[c];
    if (singleCharType !== undefined) {
      pos++;
      tokens.push({ type: singleCharType, value: c, position: startPos });
      continue;
    }

    // Invalid character
    const position = getPositionFromOffset(query, pos);
    const source = getSourceSnippet(query, pos);
    throw new LexerError(
      LexerErrorCode.UNEXPECTED_CHARACTER,
      'Unexpected character',
      position,
      source,
      c,
      'identifier, number, operator, or bracket'
    );
  }

  // Always end with EOF
  tokens.push({ type: TokenType.EOF, value: '', position: pos });

  return tokens;
}

export interface Lexer {
  peek(): Token;
  next(): Token;
  expect(type: TokenType): Token;
  match(type: TokenType): boolean;
  isAtEnd(): boolean;
  position(): number;
}

/**
 * Create a lexer for parsing tokens
 */
export function createLexer(query: string): Lexer {
  const tokens = tokenize(query);
  let index = 0;

  return {
    peek(): Token {
      return tokens[index] ?? tokens[tokens.length - 1] ?? { type: TokenType.EOF, value: '', position: query.length }; // Return EOF if past end
    },

    next(): Token {
      const token = this.peek();
      if (token.type !== TokenType.EOF) {
        index++;
      }
      return token;
    },

    expect(type: TokenType): Token {
      const token = this.peek();
      if (token.type !== type) {
        const position = getPositionFromOffset(query, token.position);
        const source = getSourceSnippet(query, token.position);
        throw new LexerError(
          LexerErrorCode.UNEXPECTED_TOKEN,
          'Unexpected token',
          position,
          source,
          token.type,
          type
        );
      }
      return this.next();
    },

    match(type: TokenType): boolean {
      if (this.peek().type === type) {
        this.next();
        return true;
      }
      return false;
    },

    isAtEnd(): boolean {
      return tokens[index]?.type === TokenType.EOF;
    },

    position(): number {
      return tokens[index]?.position ?? query.length;
    },
  };
}
