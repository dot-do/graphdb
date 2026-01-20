/**
 * Type-Safe SQL Row Parsers
 *
 * Provides type-safe parsing of SQL query results instead of relying on
 * `any` types and direct casts. Returns Error objects for malformed rows
 * instead of throwing, allowing callers to handle errors gracefully.
 *
 * @see test/shard/row-parsers.test.ts for usage examples
 */

import {
  isEntityId,
  isPredicate,
  isTransactionId,
  isNamespace,
  type Namespace,
} from '../core/types.js';

// ============================================================================
// ERROR TYPES
// ============================================================================

/**
 * Error thrown when row parsing fails
 */
export class RowParseError extends Error {
  readonly name = 'RowParseError';
  readonly field?: string;
  readonly originalValue?: unknown;

  constructor(message: string, field?: string, originalValue?: unknown) {
    const fullMessage = field ? `${message}: field '${field}'` : message;
    super(fullMessage);
    if (field !== undefined) {
      this.field = field;
    }
    if (originalValue !== undefined) {
      this.originalValue = originalValue;
    }
  }
}

// ============================================================================
// TYPES
// ============================================================================

/**
 * Parsed triple row with typed fields
 *
 * Uses camelCase naming for TypeScript consistency while mapping
 * from snake_case SQL column names.
 */
export interface TripleRow {
  /** Optional auto-increment ID */
  id?: number;
  /** Subject entity URL */
  subject: string;
  /** Predicate field name */
  predicate: string;
  /** Object type enum value */
  objType: number;
  /** Reference value (for REF type) */
  objRef: string | null;
  /** String value */
  objString: string | null;
  /** Integer value (bigint for precision) */
  objInt64: bigint | null;
  /** Float value */
  objFloat64: number | null;
  /** Boolean value (0 or 1 from SQLite) */
  objBool: number | null;
  /** Timestamp value (bigint for precision) */
  objTimestamp: bigint | null;
  /** Latitude for GEO_POINT */
  objLat: number | null;
  /** Longitude for GEO_POINT */
  objLng: number | null;
  /** Binary data */
  objBinary: Uint8Array | null;
  /** Triple timestamp (bigint for precision) */
  timestamp: bigint;
  /** Transaction ID */
  txId: string;
}

/**
 * Parsed chunk row with typed fields
 */
export interface ChunkRow {
  /** Chunk ID (generated) */
  id: string;
  /** Namespace URL */
  namespace: Namespace;
  /** Number of triples in chunk */
  tripleCount: number;
  /** Minimum timestamp of triples in chunk */
  minTimestamp: number;
  /** Maximum timestamp of triples in chunk */
  maxTimestamp: number;
  /** GraphCol-encoded blob data */
  data: Uint8Array;
  /** Size of data in bytes */
  sizeBytes: number;
  /** Creation timestamp */
  createdAt: number;
}

/**
 * Options for parsing
 */
export interface ParseOptions {
  /** If true, reject rows with unknown columns */
  strict?: boolean;
}

// ============================================================================
// KNOWN COLUMN SETS
// ============================================================================

const TRIPLE_ROW_COLUMNS = new Set([
  'id',
  'subject',
  'predicate',
  'obj_type',
  'obj_ref',
  'obj_string',
  'obj_int64',
  'obj_float64',
  'obj_bool',
  'obj_timestamp',
  'obj_lat',
  'obj_lng',
  'obj_binary',
  'timestamp',
  'tx_id',
]);

const CHUNK_ROW_COLUMNS = new Set([
  'id',
  'namespace',
  'triple_count',
  'min_timestamp',
  'max_timestamp',
  'data',
  'size_bytes',
  'created_at',
]);

// ============================================================================
// TYPE GUARDS
// ============================================================================

/**
 * Check if value is a plain object (not null, not array)
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Check if value is a string
 */
function isString(value: unknown): value is string {
  return typeof value === 'string';
}

/**
 * Check if value is a number (not NaN)
 */
function isNumber(value: unknown): value is number {
  return typeof value === 'number' && !Number.isNaN(value);
}

/**
 * Check if value is a bigint
 */
function isBigInt(value: unknown): value is bigint {
  return typeof value === 'bigint';
}

/**
 * Check if value is a number or bigint (for SQLite integer columns)
 */
function isNumeric(value: unknown): value is number | bigint {
  return isNumber(value) || isBigInt(value);
}

/**
 * Convert value to bigint, handling both number and bigint inputs
 */
function toBigInt(value: number | bigint): bigint {
  return typeof value === 'bigint' ? value : BigInt(value);
}

/**
 * Convert value to number, handling both number and bigint inputs
 */
function toNumber(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value;
}

/**
 * Check if value is binary-like (Uint8Array or ArrayBuffer)
 */
function isBinaryLike(value: unknown): value is Uint8Array | ArrayBuffer {
  return value instanceof Uint8Array || value instanceof ArrayBuffer;
}

/**
 * Convert binary-like value to Uint8Array
 */
function toUint8Array(value: Uint8Array | ArrayBuffer): Uint8Array {
  return value instanceof ArrayBuffer ? new Uint8Array(value) : value;
}

// ============================================================================
// PARSERS
// ============================================================================

/**
 * Parse a raw SQL row into a typed TripleRow
 *
 * @param row - Raw row from SQL query (unknown type)
 * @param options - Parse options
 * @returns TripleRow or RowParseError
 */
export function parseTripleRow(
  row: unknown,
  options?: ParseOptions
): TripleRow | RowParseError {
  // Validate input is object
  if (!isPlainObject(row)) {
    return new RowParseError('Row must be a plain object', undefined, row);
  }

  // Check for unknown columns in strict mode
  if (options?.strict) {
    for (const key of Object.keys(row)) {
      if (!TRIPLE_ROW_COLUMNS.has(key)) {
        return new RowParseError(
          `Unknown column in strict mode`,
          key,
          row
        );
      }
    }
  }

  // Validate required fields - basic type checks
  if (!isString(row['subject'])) {
    return new RowParseError('Missing or invalid required field', 'subject', row);
  }

  if (!isString(row['predicate'])) {
    return new RowParseError('Missing or invalid required field', 'predicate', row);
  }

  if (!isNumber(row['obj_type'])) {
    return new RowParseError('Missing or invalid required field', 'obj_type', row);
  }

  if (!isNumeric(row['timestamp'])) {
    return new RowParseError('Missing or invalid required field', 'timestamp', row);
  }

  if (!isString(row['tx_id'])) {
    return new RowParseError('Missing or invalid required field', 'tx_id', row);
  }

  // Validate branded types - ensure values conform to expected formats
  if (!isEntityId(row['subject'])) {
    return new RowParseError(
      'Invalid subject format (must be valid http/https URL)',
      'subject',
      row
    );
  }

  if (!isPredicate(row['predicate'])) {
    return new RowParseError(
      'Invalid predicate format (must be valid JS identifier without colons)',
      'predicate',
      row
    );
  }

  if (!isTransactionId(row['tx_id'])) {
    return new RowParseError(
      'Invalid tx_id format (must be 26-character ULID)',
      'tx_id',
      row
    );
  }

  // Parse optional id
  let id: number | undefined;
  if (row['id'] !== undefined && row['id'] !== null) {
    if (!isNumber(row['id'])) {
      return new RowParseError('Invalid field type', 'id', row);
    }
    id = row['id'];
  }

  // Parse optional string fields
  // obj_ref must be a valid EntityId when present (it's a reference to another entity)
  let objRef: string | null = null;
  if (row['obj_ref'] !== null && row['obj_ref'] !== undefined) {
    if (!isString(row['obj_ref'])) {
      return new RowParseError('Invalid field type (expected string)', 'obj_ref', row);
    }
    if (!isEntityId(row['obj_ref'])) {
      return new RowParseError(
        'Invalid obj_ref format (must be valid http/https URL)',
        'obj_ref',
        row
      );
    }
    objRef = row['obj_ref'];
  }

  const objString = row['obj_string'] === null || row['obj_string'] === undefined
    ? null
    : isString(row['obj_string'])
      ? row['obj_string']
      : null;

  // Parse optional numeric fields
  const objInt64 = row['obj_int64'] === null || row['obj_int64'] === undefined
    ? null
    : isNumeric(row['obj_int64'])
      ? toBigInt(row['obj_int64'])
      : null;

  const objFloat64 = row['obj_float64'] === null || row['obj_float64'] === undefined
    ? null
    : isNumber(row['obj_float64'])
      ? row['obj_float64']
      : null;

  const objBool = row['obj_bool'] === null || row['obj_bool'] === undefined
    ? null
    : isNumber(row['obj_bool'])
      ? row['obj_bool']
      : null;

  const objTimestamp = row['obj_timestamp'] === null || row['obj_timestamp'] === undefined
    ? null
    : isNumeric(row['obj_timestamp'])
      ? toBigInt(row['obj_timestamp'])
      : null;

  const objLat = row['obj_lat'] === null || row['obj_lat'] === undefined
    ? null
    : isNumber(row['obj_lat'])
      ? row['obj_lat']
      : null;

  const objLng = row['obj_lng'] === null || row['obj_lng'] === undefined
    ? null
    : isNumber(row['obj_lng'])
      ? row['obj_lng']
      : null;

  // Parse optional binary field
  const objBinary = row['obj_binary'] === null || row['obj_binary'] === undefined
    ? null
    : isBinaryLike(row['obj_binary'])
      ? toUint8Array(row['obj_binary'])
      : null;

  // Build result
  const result: TripleRow = {
    subject: row['subject'],
    predicate: row['predicate'],
    objType: row['obj_type'],
    objRef,
    objString,
    objInt64,
    objFloat64,
    objBool,
    objTimestamp,
    objLat,
    objLng,
    objBinary,
    timestamp: toBigInt(row['timestamp']),
    txId: row['tx_id'],
  };

  if (id !== undefined) {
    result.id = id;
  }

  return result;
}

/**
 * Parse a raw SQL row into a typed ChunkRow
 *
 * @param row - Raw row from SQL query (unknown type)
 * @param options - Parse options
 * @returns ChunkRow or RowParseError
 */
export function parseChunkRow(
  row: unknown,
  options?: ParseOptions
): ChunkRow | RowParseError {
  // Validate input is object
  if (!isPlainObject(row)) {
    return new RowParseError('Row must be a plain object', undefined, row);
  }

  // Check for unknown columns in strict mode
  if (options?.strict) {
    for (const key of Object.keys(row)) {
      if (!CHUNK_ROW_COLUMNS.has(key)) {
        return new RowParseError(
          `Unknown column in strict mode`,
          key,
          row
        );
      }
    }
  }

  // Validate required fields
  if (!isString(row['id'])) {
    return new RowParseError('Missing or invalid required field', 'id', row);
  }

  if (!isString(row['namespace'])) {
    return new RowParseError('Missing or invalid required field', 'namespace', row);
  }

  // Validate namespace is a valid URL with http/https protocol
  if (!isNamespace(row['namespace'])) {
    return new RowParseError(
      'Invalid namespace format (must be valid http/https URL)',
      'namespace',
      row
    );
  }

  if (!isNumeric(row['triple_count'])) {
    return new RowParseError('Missing or invalid required field', 'triple_count', row);
  }

  if (!isNumeric(row['min_timestamp'])) {
    return new RowParseError('Missing or invalid required field', 'min_timestamp', row);
  }

  if (!isNumeric(row['max_timestamp'])) {
    return new RowParseError('Missing or invalid required field', 'max_timestamp', row);
  }

  if (!isBinaryLike(row['data'])) {
    return new RowParseError('Missing or invalid required field', 'data', row);
  }

  if (!isNumeric(row['size_bytes'])) {
    return new RowParseError('Missing or invalid required field', 'size_bytes', row);
  }

  if (!isNumeric(row['created_at'])) {
    return new RowParseError('Missing or invalid required field', 'created_at', row);
  }

  // Build result with normalized types
  return {
    id: row['id'],
    namespace: row['namespace'] as Namespace,
    tripleCount: toNumber(row['triple_count']),
    minTimestamp: toNumber(row['min_timestamp']),
    maxTimestamp: toNumber(row['max_timestamp']),
    data: toUint8Array(row['data']),
    sizeBytes: toNumber(row['size_bytes']),
    createdAt: toNumber(row['created_at']),
  };
}

// ============================================================================
// UTILITIES
// ============================================================================

/**
 * Assert that a parse result is not an error, throwing if it is
 *
 * @param result - Parse result
 * @returns The successfully parsed row
 * @throws RowParseError if parsing failed
 */
export function assertTripleRow(result: TripleRow | RowParseError): TripleRow {
  if (result instanceof RowParseError) {
    throw result;
  }
  return result;
}

/**
 * Assert that a parse result is not an error, throwing if it is
 *
 * @param result - Parse result
 * @returns The successfully parsed row
 * @throws RowParseError if parsing failed
 */
export function assertChunkRow(result: ChunkRow | RowParseError): ChunkRow {
  if (result instanceof RowParseError) {
    throw result;
  }
  return result;
}

/**
 * Parse multiple rows, returning only successful parses
 *
 * @param rows - Array of raw rows
 * @returns Array of successfully parsed TripleRows
 */
export function parseTripleRows(rows: unknown[]): TripleRow[] {
  const results: TripleRow[] = [];
  for (const row of rows) {
    const parsed = parseTripleRow(row);
    if (!(parsed instanceof RowParseError)) {
      results.push(parsed);
    }
  }
  return results;
}

/**
 * Parse multiple rows, returning only successful parses
 *
 * @param rows - Array of raw rows
 * @returns Array of successfully parsed ChunkRows
 */
export function parseChunkRows(rows: unknown[]): ChunkRow[] {
  const results: ChunkRow[] = [];
  for (const row of rows) {
    const parsed = parseChunkRow(row);
    if (!(parsed instanceof RowParseError)) {
      results.push(parsed);
    }
  }
  return results;
}
