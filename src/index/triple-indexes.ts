/**
 * Triple Index Query Operations for GraphDB
 *
 * @deprecated This module uses individual SQLite rows which is NOT cost-efficient.
 * On Cloudflare DO, a 1KB row costs the SAME as a 2MB BLOB.
 * The 'triples' table has been removed in BLOB-only architecture (schema v3).
 *
 * This module is kept only for backwards compatibility and reference.
 * All queries should now use ChunkStore which scans GraphCol BLOB chunks.
 *
 * Provides efficient query helpers for the three main index access patterns:
 * - SPO Index: Forward traversal (get all predicates/values for entity X)
 * - POS Index: Predicate queries (get all entities with predicate P having value V)
 * - OSP Index: Reverse lookups (who references entity X?)
 *
 * The SQLite indexes are already created in schema.ts. This module provides
 * the query interface that leverages those indexes efficiently.
 *
 * @see CLAUDE.md for architecture details
 * @see src/shard/chunk-store.ts for the cost-optimized BLOB-only implementation
 */

import type { EntityId, Predicate, TransactionId } from '../core/types';
import { ObjectType } from '../core/types';
import type { Triple, TypedObject } from '../core/triple';
import { encodeString, decodeString, toBase64, fromBase64 } from '../core/index';
import { sqlValueToTypedObject, getValueColumn, inferObjectTypeFromValue, type SqlRowInput } from '../core/type-converters';
import { querySql } from '../shard/sql-utils.js';

// ============================================================================
// Query Interfaces
// ============================================================================

/**
 * SPO Index Query - Forward traversal
 * "Get all predicates and values for entity X"
 */
export interface SPOQuery {
  /** The subject entity to query */
  subject: EntityId;
  /** Optional predicate filter */
  predicate?: Predicate;
  /** Maximum number of results to return */
  limit?: number;
  /** Pagination cursor from previous query */
  cursor?: string;
}

/**
 * POS Index Query - Predicate queries
 * "Get all entities with predicate P having value V"
 */
export interface POSQuery {
  /** The predicate to query */
  predicate: Predicate;
  /** Optional value filter */
  value?: unknown;
  /** Comparison operator for value filter */
  valueOp?: '=' | '>' | '<' | '>=' | '<=';
  /** Maximum number of results to return */
  limit?: number;
  /** Pagination cursor from previous query */
  cursor?: string;
}

/**
 * OSP Index Query - Reverse lookups
 * "Who references entity X?"
 */
export interface OSPQuery {
  /** The target entity being referenced */
  objectRef: EntityId;
  /** Optional source entity filter */
  subject?: EntityId;
  /** Optional predicate filter */
  predicate?: Predicate;
  /** Maximum number of results to return */
  limit?: number;
  /** Pagination cursor from previous query */
  cursor?: string;
}

/**
 * Query result with pagination support
 */
export interface QueryResult {
  /** The matching triples */
  triples: Triple[];
  /** Cursor for fetching the next page */
  cursor?: string;
  /** Whether there are more results available */
  hasMore: boolean;
}

// ============================================================================
// Row to Triple Conversion
// ============================================================================

/**
 * Database row type from SQLite query
 */
interface TripleRow extends Record<string, unknown> {
  id: number;
  subject: string;
  predicate: string;
  obj_type: number;
  obj_ref: string | null;
  obj_string: string | null;
  obj_int64: number | bigint | null;
  obj_float64: number | null;
  obj_bool: number | null;
  obj_timestamp: number | bigint | null;
  obj_lat: number | null;
  obj_lng: number | null;
  obj_binary: Uint8Array | null;
  timestamp: number | bigint;
  tx_id: string;
}

/**
 * Convert a database row to a TypedObject
 * Uses consolidated type-converters module for conversion
 */
function rowToTypedObject(row: TripleRow): TypedObject {
  // Map TripleRow to SqlRowInput for the converter
  const sqlRow: SqlRowInput = {
    obj_type: row.obj_type,
    obj_ref: row.obj_ref,
    obj_string: row.obj_string,
    obj_int64: row.obj_int64,
    obj_float64: row.obj_float64,
    obj_bool: row.obj_bool,
    obj_timestamp: row.obj_timestamp,
    obj_lat: row.obj_lat,
    obj_lng: row.obj_lng,
    obj_binary: row.obj_binary,
  };
  return sqlValueToTypedObject(sqlRow);
}

/**
 * Convert a database row to a Triple
 */
function rowToTriple(row: TripleRow): Triple {
  return {
    subject: row.subject as EntityId,
    predicate: row.predicate as Predicate,
    object: rowToTypedObject(row),
    timestamp: typeof row.timestamp === 'bigint' ? row.timestamp : BigInt(row.timestamp),
    txId: row.tx_id as TransactionId,
  };
}

// ============================================================================
// Cursor Encoding/Decoding
// ============================================================================

/**
 * Encode a cursor for pagination
 * Workers-compatible: uses TextEncoder + base64 instead of Buffer
 */
function encodeCursor(lastId: number): string {
  return toBase64(encodeString(String(lastId)));
}

/**
 * Decode a cursor for pagination
 * Workers-compatible: uses base64 + TextDecoder instead of Buffer
 */
function decodeCursor(cursor: string): number {
  return parseInt(decodeString(fromBase64(cursor)), 10);
}

// ============================================================================
// SPO Index Queries
// ============================================================================

/**
 * Query using SPO index - Forward traversal
 *
 * Retrieves all triples for a given subject, optionally filtered by predicate.
 * Uses idx_spo index for efficient lookup.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param query - SPO query parameters
 * @returns Query result with triples and pagination info
 */
export async function querySPO(sql: SqlStorage, query: SPOQuery): Promise<QueryResult> {
  const { subject, predicate, limit, cursor } = query;

  // Build query
  let sqlQuery: string;
  const params: (string | number)[] = [];

  if (predicate) {
    sqlQuery = `SELECT * FROM triples WHERE subject = ? AND predicate = ?`;
    params.push(subject, predicate);
  } else {
    sqlQuery = `SELECT * FROM triples WHERE subject = ?`;
    params.push(subject);
  }

  // Add cursor condition
  if (cursor) {
    const lastId = decodeCursor(cursor);
    sqlQuery += ` AND id > ?`;
    params.push(lastId);
  }

  // Add ordering and limit
  sqlQuery += ` ORDER BY id ASC`;
  const actualLimit = limit ?? 1000;
  sqlQuery += ` LIMIT ?`;
  params.push(actualLimit + 1); // Fetch one extra to check hasMore

  // Execute query
  const rows = querySql<TripleRow>(sql, sqlQuery, ...params);

  // Check if there are more results
  const hasMore = rows.length > actualLimit;
  const resultRows = hasMore ? rows.slice(0, actualLimit) : rows;

  // Convert to triples
  const triples = resultRows.map(rowToTriple);

  // Create cursor for next page and build result
  if (hasMore && resultRows.length > 0) {
    const lastRow = resultRows[resultRows.length - 1]!;
    return {
      triples,
      cursor: encodeCursor(lastRow.id),
      hasMore,
    };
  }

  return {
    triples,
    hasMore,
  };
}

// ============================================================================
// POS Index Queries
// ============================================================================

// Helper functions getValueColumn and inferObjectTypeFromValue
// are now imported from '../core/type-converters'

/**
 * Query using POS index - Predicate queries
 *
 * Retrieves all triples with a given predicate, optionally filtered by value.
 * Uses idx_pos index for efficient lookup.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param query - POS query parameters
 * @returns Query result with triples and pagination info
 */
export async function queryPOS(sql: SqlStorage, query: POSQuery): Promise<QueryResult> {
  const { predicate, value, valueOp, limit, cursor } = query;

  // Build query
  let sqlQuery = `SELECT * FROM triples WHERE predicate = ?`;
  const params: (string | number | bigint)[] = [predicate];

  // Add value filter if specified
  if (value !== undefined && valueOp) {
    const valueType = inferObjectTypeFromValue(value);
    const valueColumn = getValueColumn(valueType);

    sqlQuery += ` AND obj_type = ?`;
    params.push(valueType);

    sqlQuery += ` AND ${valueColumn} ${valueOp} ?`;
    params.push(value as string | number | bigint);
  }

  // Add cursor condition
  if (cursor) {
    const lastId = decodeCursor(cursor);
    sqlQuery += ` AND id > ?`;
    params.push(lastId);
  }

  // Add ordering and limit
  sqlQuery += ` ORDER BY id ASC`;
  const actualLimit = limit ?? 1000;
  sqlQuery += ` LIMIT ?`;
  params.push(actualLimit + 1);

  // Execute query
  const rows = querySql<TripleRow>(sql, sqlQuery, ...params);

  // Check if there are more results
  const hasMore = rows.length > actualLimit;
  const resultRows = hasMore ? rows.slice(0, actualLimit) : rows;

  // Convert to triples
  const triples = resultRows.map(rowToTriple);

  // Create cursor for next page and build result
  if (hasMore && resultRows.length > 0) {
    const lastRow = resultRows[resultRows.length - 1]!;
    return {
      triples,
      cursor: encodeCursor(lastRow.id),
      hasMore,
    };
  }

  return {
    triples,
    hasMore,
  };
}

// ============================================================================
// OSP Index Queries
// ============================================================================

/**
 * Query using OSP index - Reverse lookups
 *
 * Retrieves all triples that reference a given target entity.
 * Uses idx_osp partial index for efficient lookup (only for REF type).
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param query - OSP query parameters
 * @returns Query result with triples and pagination info
 */
export async function queryOSP(sql: SqlStorage, query: OSPQuery): Promise<QueryResult> {
  const { objectRef, subject, predicate, limit, cursor } = query;

  // Build query - OSP index only works with REF type
  let sqlQuery = `SELECT * FROM triples WHERE obj_ref = ? AND obj_type = ?`;
  const params: (string | number)[] = [objectRef, ObjectType.REF];

  // Add optional filters
  if (subject) {
    sqlQuery += ` AND subject = ?`;
    params.push(subject);
  }

  if (predicate) {
    sqlQuery += ` AND predicate = ?`;
    params.push(predicate);
  }

  // Add cursor condition
  if (cursor) {
    const lastId = decodeCursor(cursor);
    sqlQuery += ` AND id > ?`;
    params.push(lastId);
  }

  // Add ordering and limit
  sqlQuery += ` ORDER BY id ASC`;
  const actualLimit = limit ?? 1000;
  sqlQuery += ` LIMIT ?`;
  params.push(actualLimit + 1);

  // Execute query
  const rows = querySql<TripleRow>(sql, sqlQuery, ...params);

  // Check if there are more results
  const hasMore = rows.length > actualLimit;
  const resultRows = hasMore ? rows.slice(0, actualLimit) : rows;

  // Convert to triples
  const triples = resultRows.map(rowToTriple);

  // Create cursor for next page and build result
  if (hasMore && resultRows.length > 0) {
    const lastRow = resultRows[resultRows.length - 1]!;
    return {
      triples,
      cursor: encodeCursor(lastRow.id),
      hasMore,
    };
  }

  return {
    triples,
    hasMore,
  };
}

// ============================================================================
// Batch Operations
// ============================================================================

/**
 * Batch query using SPO index for multiple subjects
 *
 * Efficiently retrieves all triples for multiple subjects in a single query.
 * Returns a Map keyed by subject EntityId.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param subjects - Array of subject EntityIds to query
 * @returns Map of subject to their triples
 */
export async function batchQuerySPO(sql: SqlStorage, subjects: EntityId[]): Promise<Map<string, Triple[]>> {
  const result = new Map<string, Triple[]>();

  // Initialize empty arrays for all subjects
  for (const subject of subjects) {
    result.set(subject, []);
  }

  if (subjects.length === 0) {
    return result;
  }

  // Build query with IN clause
  const placeholders = subjects.map(() => '?').join(', ');
  const sqlQuery = `SELECT * FROM triples WHERE subject IN (${placeholders}) ORDER BY subject, id`;

  // Execute query
  const rows = querySql<TripleRow>(sql, sqlQuery, ...subjects);

  // Group triples by subject
  for (const row of rows) {
    const triple = rowToTriple(row);
    const existing = result.get(triple.subject) ?? [];
    existing.push(triple);
    result.set(triple.subject, existing);
  }

  return result;
}

/**
 * Batch query using OSP index for multiple target entities
 *
 * Efficiently retrieves all incoming references for multiple targets in a single query.
 * Returns a Map keyed by target EntityId.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param targets - Array of target EntityIds to query
 * @returns Map of target to triples that reference them
 */
export async function batchQueryOSP(sql: SqlStorage, targets: EntityId[]): Promise<Map<string, Triple[]>> {
  const result = new Map<string, Triple[]>();

  // Initialize empty arrays for all targets
  for (const target of targets) {
    result.set(target, []);
  }

  if (targets.length === 0) {
    return result;
  }

  // Build query with IN clause - only for REF type
  const placeholders = targets.map(() => '?').join(', ');
  const sqlQuery = `SELECT * FROM triples WHERE obj_ref IN (${placeholders}) AND obj_type = ? ORDER BY obj_ref, id`;

  // Execute query
  const rows = querySql<TripleRow>(sql, sqlQuery, ...targets, ObjectType.REF);

  // Group triples by target (obj_ref)
  for (const row of rows) {
    const triple = rowToTriple(row);
    // We know it's a REF type because we filtered by obj_type = ObjectType.REF
    if (triple.object.type === ObjectType.REF) {
      const targetRef = triple.object.value;
      if (targetRef) {
        const existing = result.get(targetRef) ?? [];
        existing.push(triple);
        result.set(targetRef, existing);
      }
    }
  }

  return result;
}
