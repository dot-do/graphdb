/**
 * FTS Index - Full-Text Search for GraphDB
 *
 * Implements FTS5 (SQLite's built-in full-text search) for text search
 * on STRING values in the triples table.
 *
 * Key features:
 * - Automatic indexing via triggers for STRING type triples
 * - FTS5 query syntax support (phrases, prefixes, boolean operators)
 * - BM25 relevance ranking
 * - Snippet generation with highlighting
 * - Predicate filtering
 *
 * @see CLAUDE.md for architecture details
 * @see https://www.sqlite.org/fts5.html for FTS5 documentation
 */

import type { EntityId, Predicate } from '../core/types';
import { ObjectType, isPredicate } from '../core/types';
import { sanitizeFtsQuery } from '../security/fts-sanitizer.js';
import { querySql } from '../shard/sql-utils.js';

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Error codes for FTS query errors
 */
export const FTSErrorCode = {
  /** FTS5 query syntax error */
  SYNTAX_ERROR: 'FTS_SYNTAX_ERROR',
  /** FTS table not found (not initialized) */
  TABLE_NOT_FOUND: 'FTS_TABLE_NOT_FOUND',
  /** General FTS query error */
  QUERY_ERROR: 'FTS_QUERY_ERROR',
} as const;

export type FTSErrorCodeType = (typeof FTSErrorCode)[keyof typeof FTSErrorCode];

/**
 * Custom error class for FTS query errors
 *
 * Provides structured error information including:
 * - code: Machine-readable error code from FTSErrorCode
 * - message: Human-readable error description
 * - originalError: The underlying error that caused this (if any)
 *
 * @example
 * ```typescript
 * try {
 *   await searchFTS(sql, { query: 'test' });
 * } catch (error) {
 *   if (error instanceof FTSQueryError) {
 *     if (error.code === FTSErrorCode.TABLE_NOT_FOUND) {
 *       // Initialize FTS first
 *     } else if (error.code === FTSErrorCode.SYNTAX_ERROR) {
 *       // Invalid query syntax
 *     }
 *   }
 * }
 * ```
 */
export class FTSQueryError extends Error {
  public readonly code: FTSErrorCodeType;
  public readonly originalError: Error | undefined;

  constructor(code: FTSErrorCodeType, message: string, originalError?: Error) {
    super(message);
    this.name = 'FTSQueryError';
    this.code = code;
    this.originalError = originalError;

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    const ErrorWithStackTrace = Error as { captureStackTrace?: (target: object, constructor: Function) => void };
    if (ErrorWithStackTrace.captureStackTrace) {
      ErrorWithStackTrace.captureStackTrace(this, FTSQueryError);
    }
  }
}

// ============================================================================
// Schema Definitions
// ============================================================================

/**
 * FTS5 virtual table and sync triggers schema
 *
 * Creates:
 * - triples_fts: FTS5 virtual table for full-text search
 * - triples_ai: AFTER INSERT trigger to sync STRING triples to FTS
 * - triples_ad: AFTER DELETE trigger to remove from FTS
 * - triples_au: AFTER UPDATE trigger to update FTS
 *
 * The FTS table uses content sync mode where the content is stored
 * in the triples table and the FTS table just stores the index.
 */
export const FTS_SCHEMA = `
CREATE VIRTUAL TABLE IF NOT EXISTS triples_fts USING fts5(
  subject,
  predicate,
  content
);

-- Trigger: sync inserts of STRING type to FTS
CREATE TRIGGER IF NOT EXISTS triples_ai AFTER INSERT ON triples
WHEN NEW.obj_type = ${ObjectType.STRING}
BEGIN
  INSERT INTO triples_fts(rowid, subject, predicate, content)
  VALUES (NEW.id, NEW.subject, NEW.predicate, NEW.obj_string);
END;

-- Trigger: sync deletes of STRING type from FTS
CREATE TRIGGER IF NOT EXISTS triples_ad AFTER DELETE ON triples
WHEN OLD.obj_type = ${ObjectType.STRING}
BEGIN
  DELETE FROM triples_fts WHERE rowid = OLD.id;
END;

-- Trigger: sync updates of STRING type to FTS
CREATE TRIGGER IF NOT EXISTS triples_au AFTER UPDATE ON triples
WHEN OLD.obj_type = ${ObjectType.STRING} OR NEW.obj_type = ${ObjectType.STRING}
BEGIN
  DELETE FROM triples_fts WHERE rowid = OLD.id;
  INSERT INTO triples_fts(rowid, subject, predicate, content)
  SELECT NEW.id, NEW.subject, NEW.predicate, NEW.obj_string
  WHERE NEW.obj_type = ${ObjectType.STRING};
END;
`;

// ============================================================================
// Interfaces
// ============================================================================

/**
 * FTS query parameters
 */
export interface FTSQuery {
  /** FTS5 query string (supports phrases, prefixes, boolean operators) */
  query: string;
  /** Optional predicate filter - only search within this predicate */
  predicate?: Predicate;
  /** Maximum number of results to return */
  limit?: number;
  /** Number of results to skip (for pagination) */
  offset?: number;
}

/**
 * FTS search result
 */
export interface FTSResult {
  /** Subject entity ID */
  subject: EntityId;
  /** Predicate that matched */
  predicate: Predicate;
  /** Highlighted snippet of matched text */
  snippet: string;
  /** BM25 relevance score (more negative = more relevant) */
  rank: number;
}

// ============================================================================
// FTS Initialization
// ============================================================================

/**
 * Initialize FTS tables and triggers
 *
 * Creates the FTS5 virtual table and sync triggers if they don't exist.
 * This is idempotent - safe to call multiple times.
 *
 * Note: If data exists in the triples table before FTS is initialized,
 * the FTS index will be empty. Call rebuildFTS() to populate it.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 */
export function initializeFTS(sql: SqlStorage): void {
  // Parse statements properly - triggers contain semicolons inside their bodies
  // so we need to split on 'END;' for triggers and ';' for other statements
  const statements = parseSQLStatements(FTS_SCHEMA);

  for (const statement of statements) {
    try {
      sql.exec(statement);
    } catch (error) {
      // Ignore "already exists" errors for idempotency
      const message = error instanceof Error ? error.message : String(error);
      if (!message.includes('already exists')) {
        throw error;
      }
    }
  }
}

/**
 * Parse SQL statements that may contain triggers with embedded semicolons
 *
 * This handles the case where triggers contain multiple SQL statements
 * within BEGIN...END blocks.
 */
function parseSQLStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = '';
  let inTrigger = false;

  // Remove SQL comments (lines starting with --)
  const lines = sql.split('\n');
  const cleanedLines = lines.filter((line) => !line.trim().startsWith('--'));
  const cleanedSql = cleanedLines.join('\n');

  // Split by semicolon but track trigger context
  const tokens = cleanedSql.split(/;/);

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]!.trim();

    if (token.length === 0) {
      continue;
    }

    // Check if this starts a CREATE TRIGGER
    if (/CREATE\s+TRIGGER/i.test(token)) {
      inTrigger = true;
      current = token;
    } else if (inTrigger) {
      // Append to current trigger statement
      current += ';' + tokens[i];

      // Check if this ends the trigger (contains END)
      if (/\bEND\s*$/i.test(token)) {
        statements.push(current.trim());
        current = '';
        inTrigger = false;
      }
    } else {
      // Regular statement
      statements.push(token);
    }
  }

  // Handle any remaining statement
  if (current.trim().length > 0) {
    statements.push(current.trim());
  }

  return statements;
}

/**
 * Check if FTS is initialized
 *
 * @param sql - SqlStorage instance
 * @returns true if the FTS virtual table exists
 */
export function isFTSInitialized(sql: SqlStorage): boolean {
  try {
    const result = sql.exec(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='triples_fts'"
    );
    const rows = [...result];
    return rows.length > 0;
  } catch {
    return false;
  }
}

// ============================================================================
// FTS Search
// ============================================================================

/**
 * FTS row type from SQLite query
 */
interface FTSRow extends Record<string, unknown> {
  subject: string;
  predicate: string;
  snippet: string;
  rank: number;
}

/**
 * Search using FTS5
 *
 * Performs a full-text search against indexed STRING values.
 * Supports FTS5 query syntax:
 * - Simple words: `javascript`
 * - Phrases: `"quick brown fox"`
 * - Prefixes: `java*`
 * - Boolean: `javascript OR python`, `programming NOT tutorial`
 * - Grouping: `(web OR mobile) javascript`
 *
 * Results are sorted by BM25 relevance score (best matches first).
 *
 * @param sql - SqlStorage instance
 * @param query - FTS query parameters
 * @returns Array of search results with snippets and ranks
 */
export async function searchFTS(sql: SqlStorage, query: FTSQuery): Promise<FTSResult[]> {
  const { query: searchQuery, predicate, limit, offset } = query;

  // Handle empty query
  if (!searchQuery || searchQuery.trim() === '') {
    return [];
  }

  // Sanitize the query to prevent FTS5 injection attacks
  const sanitizedQuery = sanitizeFtsQuery(searchQuery);

  // If sanitization results in empty query, return empty results
  if (!sanitizedQuery || sanitizedQuery.trim() === '') {
    return [];
  }

  // Validate predicate at runtime to prevent injection via type bypass
  // While SQLite parameterized queries prevent classical SQL injection,
  // defense-in-depth requires validating all user input at API boundaries
  if (predicate !== undefined && !isPredicate(predicate)) {
    throw new FTSQueryError(
      FTSErrorCode.QUERY_ERROR,
      `Invalid predicate: "${predicate}". Predicates must be valid JS identifier-like names without colons or whitespace.`
    );
  }

  // Build the SQL query
  let sqlQuery: string;
  const params: (string | number)[] = [];

  if (predicate) {
    // Search with predicate filter
    // We use the MATCH syntax on the content column and filter by predicate
    sqlQuery = `
      SELECT
        subject,
        predicate,
        snippet(triples_fts, 2, '<b>', '</b>', '...', 32) as snippet,
        bm25(triples_fts) as rank
      FROM triples_fts
      WHERE triples_fts MATCH ?
        AND predicate = ?
      ORDER BY rank
    `;
    params.push(sanitizedQuery, predicate);
  } else {
    // Search across all predicates
    sqlQuery = `
      SELECT
        subject,
        predicate,
        snippet(triples_fts, 2, '<b>', '</b>', '...', 32) as snippet,
        bm25(triples_fts) as rank
      FROM triples_fts
      WHERE triples_fts MATCH ?
      ORDER BY rank
    `;
    params.push(sanitizedQuery);
  }

  // Add limit and offset
  const effectiveLimit = limit ?? 1000;
  const effectiveOffset = offset ?? 0;

  sqlQuery += ` LIMIT ? OFFSET ?`;
  params.push(effectiveLimit, effectiveOffset);

  // Execute query
  try {
    const rows = querySql<FTSRow>(sql, sqlQuery, ...params);

    return rows.map((row) => ({
      subject: row.subject as EntityId,
      predicate: row.predicate as Predicate,
      snippet: row.snippet,
      rank: row.rank,
    }));
  } catch (error) {
    // Categorize and throw FTSQueryError instead of swallowing errors
    const message = error instanceof Error ? error.message : String(error);
    const originalError = error instanceof Error ? error : new Error(String(error));

    // Check for table not found errors
    if (message.includes('no such table') || message.includes('triples_fts')) {
      throw new FTSQueryError(
        FTSErrorCode.TABLE_NOT_FOUND,
        'FTS table not initialized. Call initializeFTS() first.',
        originalError
      );
    }

    // Check for FTS5 syntax errors
    if (message.includes('fts5') || message.includes('syntax')) {
      throw new FTSQueryError(
        FTSErrorCode.SYNTAX_ERROR,
        `Invalid FTS5 query syntax: ${message}`,
        originalError
      );
    }

    // General query error
    throw new FTSQueryError(
      FTSErrorCode.QUERY_ERROR,
      `FTS query failed: ${message}`,
      originalError
    );
  }
}

// ============================================================================
// FTS Maintenance
// ============================================================================

/**
 * Rebuild the FTS index from existing triples
 *
 * This is useful when:
 * - FTS was initialized after data was already inserted
 * - The FTS index is out of sync with the triples table
 * - Recovery from corruption
 *
 * This operation clears the FTS index and repopulates it from all
 * STRING type triples in the triples table.
 *
 * @param sql - SqlStorage instance
 */
export async function rebuildFTS(sql: SqlStorage): Promise<void> {
  // First, ensure FTS is initialized
  if (!isFTSInitialized(sql)) {
    initializeFTS(sql);
  }

  // Clear the FTS index using DELETE (regular FTS5 table, not contentless)
  sql.exec('DELETE FROM triples_fts');

  // Repopulate from triples table
  // Use INSERT INTO ... SELECT to efficiently copy all STRING triples
  sql.exec(`
    INSERT INTO triples_fts(rowid, subject, predicate, content)
    SELECT id, subject, predicate, obj_string
    FROM triples
    WHERE obj_type = ?
  `, ObjectType.STRING);
}
