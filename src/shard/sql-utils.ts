/**
 * SQL Utility Functions for GraphDB
 *
 * Provides type-safe wrappers around SqlStorage operations to avoid
 * repetitive casting patterns like `[...result] as unknown as T[]`.
 *
 * @see CLAUDE.md for architecture details
 */

/**
 * Error thrown when SQL query result validation fails.
 * This indicates a mismatch between expected and actual row structure.
 */
export class SqlRowValidationError extends Error {
  constructor(
    message: string,
    public readonly rowIndex: number,
    public readonly expectedType: string,
    public readonly actualValue: unknown
  ) {
    super(message);
    this.name = 'SqlRowValidationError';
  }
}

/**
 * Type guard to check if a value is a plain object (row-like structure).
 * SqlStorage.exec() returns rows as plain objects with column names as keys.
 */
function isRowObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof ArrayBuffer) &&
    !(value instanceof Uint8Array)
  );
}

/**
 * Validates that a row from SqlStorage has the expected object structure.
 * This provides runtime safety for the type cast from SqlStorage results.
 *
 * @param row - The row returned from sql.exec()
 * @param rowIndex - Index of the row for error reporting
 * @throws SqlRowValidationError if the row is not a valid object
 */
function validateRow(row: unknown, rowIndex: number): asserts row is Record<string, unknown> {
  if (!isRowObject(row)) {
    throw new SqlRowValidationError(
      `SQL result row ${rowIndex} is not a valid row object. ` +
        `Expected object, got ${row === null ? 'null' : typeof row}`,
      rowIndex,
      'object',
      row
    );
  }
}

/**
 * Execute a SQL query and return typed results with runtime validation
 *
 * This is a convenience wrapper that handles the type casting pattern
 * commonly used with SqlStorage.exec() results. It validates that each
 * row is a proper object before returning, providing runtime safety.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param query - SQL query string
 * @param params - Query parameters (supports string, number, bigint, null, Uint8Array)
 * @returns Array of typed rows
 * @throws SqlRowValidationError if any row is not a valid object
 *
 * @example
 * ```typescript
 * interface UserRow {
 *   id: number;
 *   name: string;
 *   email: string;
 * }
 *
 * const users = querySql<UserRow>(sql, 'SELECT * FROM users WHERE active = ?', 1);
 * // users is typed as UserRow[]
 * ```
 */
export function querySql<T extends Record<string, unknown>>(
  sql: SqlStorage,
  query: string,
  ...params: SqlStorageValue[]
): T[] {
  const result = sql.exec(query, ...params);
  const rows: T[] = [];

  let index = 0;
  for (const row of result) {
    validateRow(row, index);
    rows.push(row as T);
    index++;
  }

  return rows;
}

/**
 * Execute a SQL query and return a single typed result or null
 *
 * Useful for queries that expect at most one result (e.g., lookups by primary key).
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param query - SQL query string
 * @param params - Query parameters
 * @returns Single typed row or null if no results
 * @throws SqlRowValidationError if the row is not a valid object
 *
 * @example
 * ```typescript
 * const user = querySqlOne<UserRow>(sql, 'SELECT * FROM users WHERE id = ?', 123);
 * if (user) {
 *   console.log(user.name);
 * }
 * ```
 */
export function querySqlOne<T extends Record<string, unknown>>(
  sql: SqlStorage,
  query: string,
  ...params: SqlStorageValue[]
): T | null {
  const rows = querySql<T>(sql, query, ...params);
  return rows.length > 0 ? rows[0]! : null;
}

/**
 * Type alias for values that can be passed to SqlStorage.exec()
 *
 * Matches the types accepted by Cloudflare's SqlStorage API.
 */
export type SqlStorageValue = string | number | bigint | null | Uint8Array | ArrayBuffer;
