/**
 * Triple CRUD Operations for GraphDB Shard DO
 *
 * ============================================================================
 * HYBRID ARCHITECTURE - When to use crud.ts vs ChunkStore
 * ============================================================================
 *
 * This module provides individual triple row operations in the `triples` table.
 * It works alongside ChunkStore in a hybrid architecture:
 *
 * **crud.ts (this module) - Use for:**
 *   - Index Building: FTS, Geo, Vector indexes require individual triple access
 *   - Development/Testing: Small-scale testing without BLOB encoding overhead
 *   - Query-heavy workloads: When you need indexed lookups (SPO, POS, OSP)
 *   - Real-time updates: Individual triple updates without chunk rewriting
 *
 * **ChunkStore (chunk-store.ts) - Use for:**
 *   - Production bulk storage: High-volume data with cost optimization
 *   - Append-only workloads: Log-style data where chunks rarely change
 *   - Cold storage: Archival data that's infrequently queried
 *
 * **Cost Trade-offs:**
 *   On Cloudflare DO, a 1KB row costs the SAME as a 2MB BLOB for storage ops.
 *   - ChunkStore: 1 op per ~50,000 triples (cost-efficient for bulk)
 *   - crud.ts: 1 op per triple (enables indexes, real-time queries)
 *
 * **Schema v4 re-added the triples table specifically for index integration.**
 * The triples table is NOT deprecated - it serves a different purpose than chunks.
 *
 * ============================================================================
 *
 * Implements the TripleStore interface for CRUD operations on triples.
 * Supports MVCC (Multi-Version Concurrency Control) via timestamp ordering.
 * Uses soft deletes with tombstones (NULL type) for delete operations.
 *
 * @see CLAUDE.md for architecture details
 * @see chunk-store.ts for bulk storage with BLOB-optimized architecture
 * @see schema.ts for the hybrid schema (chunks + triples + index tables)
 */

import type { Triple, TypedObject } from '../core/triple.js';
import type { EntityId, Predicate, TransactionId } from '../core/types.js';
import { ObjectType, isEntityId } from '../core/types.js';
import {
  parseTripleRow,
  RowParseError,
  type TripleRow as ParsedTripleRow,
} from './row-parsers.js';

/**
 * TripleStore interface for CRUD operations
 *
 * All operations support MVCC:
 * - Updates create new versions (old versions preserved)
 * - Deletes create tombstones (soft delete)
 * - getLatestTriple returns the most recent version
 */
export interface TripleStore {
  // Create
  insertTriple(triple: Triple): Promise<void>;
  insertTriples(triples: Triple[]): Promise<void>;

  // Read
  getTriple(subject: EntityId, predicate: Predicate): Promise<Triple | null>;
  getTriples(subject: EntityId): Promise<Triple[]>;
  getTriplesByPredicate(predicate: Predicate): Promise<Triple[]>;

  /**
   * Get triples for multiple subjects in a single query (batch operation).
   * Returns a Map from subject ID to array of triples for that subject.
   * This avoids the N+1 query pattern when looking up many entities.
   *
   * @param subjects Array of subject EntityIds to query
   * @returns Map of subject ID to its triples (latest version of each predicate)
   */
  getTriplesForMultipleSubjects(subjects: EntityId[]): Promise<Map<EntityId, Triple[]>>;

  // Update (insert new version, old stays for MVCC)
  updateTriple(
    subject: EntityId,
    predicate: Predicate,
    newValue: TypedObject,
    txId: TransactionId
  ): Promise<void>;

  // Delete (soft delete via tombstone)
  deleteTriple(subject: EntityId, predicate: Predicate, txId: TransactionId): Promise<void>;
  deleteEntity(subject: EntityId, txId: TransactionId): Promise<void>;

  // Query helpers
  exists(subject: EntityId): Promise<boolean>;
  getLatestTriple(subject: EntityId, predicate: Predicate): Promise<Triple | null>;
}

/**
 * Row representation of a triple in SQLite
 */
export interface TripleRow {
  id?: number;
  subject: string;
  predicate: string;
  obj_type: number;
  obj_ref?: string | null;
  obj_string?: string | null;
  obj_int64?: number | bigint | null;
  obj_float64?: number | null;
  obj_bool?: number | null;
  obj_timestamp?: number | bigint | null;
  obj_lat?: number | null;
  obj_lng?: number | null;
  obj_binary?: Uint8Array | null;
  timestamp: number | bigint;
  tx_id: string;
}

/**
 * Convert a Triple to a database row format
 *
 * Maps TypedObject fields to the appropriate SQLite columns.
 * Note: SQLite INTEGER type is 64-bit and handles bigint correctly.
 * We preserve bigint values to avoid precision loss for timestamps
 * beyond Number.MAX_SAFE_INTEGER (2^53 - 1).
 *
 * @param triple The Triple to convert
 * @returns Row object ready for SQL insertion
 */
export function tripleToRow(triple: Triple): Record<string, unknown> {
  // Convert BigInt timestamp to Number for SQL binding
  // Cloudflare DO SQLite doesn't support BigInt parameters directly
  // Note: This is safe for timestamps up to year 275760 (Number.MAX_SAFE_INTEGER)
  const timestamp = Number(triple.timestamp);

  const row: Record<string, unknown> = {
    subject: triple.subject,
    predicate: triple.predicate,
    obj_type: triple.object.type,
    timestamp,
    tx_id: triple.txId,
  };

  switch (triple.object.type) {
    case ObjectType.NULL:
      // No value columns needed
      break;

    case ObjectType.BOOL:
      row['obj_bool'] = triple.object.value ? 1 : 0;
      break;

    case ObjectType.INT32:
    case ObjectType.INT64:
      // Convert BigInt to Number for SQLite
      row['obj_int64'] = triple.object.value !== undefined
        ? Number(triple.object.value)
        : null;
      break;

    case ObjectType.FLOAT64:
      row['obj_float64'] = triple.object.value;
      break;

    case ObjectType.STRING:
      row['obj_string'] = triple.object.value;
      break;

    case ObjectType.BINARY:
      row['obj_binary'] = triple.object.value;
      break;

    case ObjectType.TIMESTAMP:
      // Convert BigInt to Number for SQLite (Cloudflare DO doesn't support BigInt params)
      row['obj_timestamp'] = triple.object.value !== undefined
        ? Number(triple.object.value)
        : null;
      break;

    case ObjectType.DATE:
      // Store date as integer (days since epoch)
      row['obj_int64'] = triple.object.value;
      break;

    case ObjectType.DURATION:
      // Store duration as string
      row['obj_string'] = triple.object.value;
      break;

    case ObjectType.REF:
      row['obj_ref'] = triple.object.value;
      break;

    case ObjectType.REF_ARRAY:
      // Store as JSON in binary column
      row['obj_binary'] = new TextEncoder().encode(JSON.stringify(triple.object.value));
      break;

    case ObjectType.JSON:
      // Store as JSON in binary column
      row['obj_binary'] = new TextEncoder().encode(JSON.stringify(triple.object.value));
      break;

    case ObjectType.GEO_POINT:
      row['obj_lat'] = triple.object.value?.lat;
      row['obj_lng'] = triple.object.value?.lng;
      break;

    case ObjectType.GEO_POLYGON:
      // Store as JSON in binary column
      row['obj_binary'] = new TextEncoder().encode(JSON.stringify(triple.object.value));
      break;

    case ObjectType.GEO_LINESTRING:
      // Store as JSON in binary column
      row['obj_binary'] = new TextEncoder().encode(JSON.stringify(triple.object.value));
      break;

    case ObjectType.URL:
      row['obj_string'] = triple.object.value;
      break;
  }

  return row;
}

/**
 * Convert a database row to a Triple using type-safe parsing
 *
 * Reconstructs the TypedObject from the appropriate SQLite columns.
 * Uses the type-safe parser to validate row structure before conversion.
 *
 * @param row The database row to convert
 * @returns A Triple object
 * @throws RowParseError if the row is malformed
 */
export function rowToTriple(row: Record<string, unknown>): Triple {
  // Parse the row with type-safe parser
  const parsed = parseTripleRow(row);
  if (parsed instanceof RowParseError) {
    throw parsed;
  }

  // Convert ParsedTripleRow to Triple
  return parsedRowToTriple(parsed);
}

/**
 * Internal function to convert a validated ParsedTripleRow to a Triple
 *
 * This function assumes the row has already been validated by parseTripleRow.
 *
 * @param row The validated ParsedTripleRow
 * @returns A Triple object
 */
function parsedRowToTriple(row: ParsedTripleRow): Triple {
  const objType = row.objType;
  let object: TypedObject;

  switch (objType) {
    case ObjectType.NULL:
      object = { type: ObjectType.NULL };
      break;

    case ObjectType.BOOL:
      object = { type: ObjectType.BOOL, value: row.objBool === 1 };
      break;

    case ObjectType.INT32:
      // Row parser already converts to bigint
      object = { type: ObjectType.INT32, value: row.objInt64! };
      break;

    case ObjectType.INT64:
      // Row parser already converts to bigint
      object = { type: ObjectType.INT64, value: row.objInt64! };
      break;

    case ObjectType.FLOAT64:
      object = { type: ObjectType.FLOAT64, value: row.objFloat64! };
      break;

    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: row.objString! };
      break;

    case ObjectType.BINARY:
      // Row parser already converts to Uint8Array
      object = { type: ObjectType.BINARY, value: row.objBinary! };
      break;

    case ObjectType.TIMESTAMP:
      // Row parser already converts to bigint
      object = { type: ObjectType.TIMESTAMP, value: row.objTimestamp! };
      break;

    case ObjectType.DATE:
      object = { type: ObjectType.DATE, value: Number(row.objInt64) };
      break;

    case ObjectType.DURATION:
      object = { type: ObjectType.DURATION, value: row.objString! };
      break;

    case ObjectType.REF:
      object = { type: ObjectType.REF, value: row.objRef as EntityId };
      break;

    case ObjectType.REF_ARRAY: {
      const jsonStr = new TextDecoder().decode(row.objBinary!);
      const parsed = JSON.parse(jsonStr) as unknown;
      // Validate that the parsed value is an array of valid EntityIds
      if (!Array.isArray(parsed)) {
        throw new RowParseError('REF_ARRAY value must be an array', 'obj_binary');
      }
      const refs: EntityId[] = [];
      for (const item of parsed) {
        if (typeof item !== 'string' || !isEntityId(item)) {
          throw new RowParseError(
            'REF_ARRAY items must be valid EntityIds (http/https URLs)',
            'obj_binary'
          );
        }
        refs.push(item as EntityId);
      }
      object = { type: ObjectType.REF_ARRAY, value: refs };
      break;
    }

    case ObjectType.JSON: {
      const jsonStr = new TextDecoder().decode(row.objBinary!);
      object = { type: ObjectType.JSON, value: JSON.parse(jsonStr) };
      break;
    }

    case ObjectType.GEO_POINT:
      object = {
        type: ObjectType.GEO_POINT,
        value: {
          lat: row.objLat!,
          lng: row.objLng!,
        },
      };
      break;

    case ObjectType.GEO_POLYGON: {
      const jsonStr = new TextDecoder().decode(row.objBinary!);
      object = { type: ObjectType.GEO_POLYGON, value: JSON.parse(jsonStr) };
      break;
    }

    case ObjectType.GEO_LINESTRING: {
      const jsonStr = new TextDecoder().decode(row.objBinary!);
      object = { type: ObjectType.GEO_LINESTRING, value: JSON.parse(jsonStr) };
      break;
    }

    case ObjectType.URL:
      object = { type: ObjectType.URL, value: row.objString! };
      break;

    default:
      object = { type: ObjectType.NULL };
      break;
  }

  return {
    subject: row.subject as EntityId,
    predicate: row.predicate as Predicate,
    object,
    timestamp: row.timestamp,
    txId: row.txId as TransactionId,
  };
}

/**
 * Create a TripleStore from a SqlStorage instance
 *
 * @param sql SqlStorage instance from DurableObjectState
 * @returns TripleStore implementation
 */
export function createTripleStore(sql: SqlStorage): TripleStore {
  return {
    async insertTriple(triple: Triple): Promise<void> {
      const row = tripleToRow(triple);

      sql.exec(
        `INSERT INTO triples (subject, predicate, obj_type, obj_ref, obj_string, obj_int64, obj_float64, obj_bool, obj_timestamp, obj_lat, obj_lng, obj_binary, timestamp, tx_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        row['subject'],
        row['predicate'],
        row['obj_type'],
        row['obj_ref'] ?? null,
        row['obj_string'] ?? null,
        row['obj_int64'] ?? null,
        row['obj_float64'] ?? null,
        row['obj_bool'] ?? null,
        row['obj_timestamp'] ?? null,
        row['obj_lat'] ?? null,
        row['obj_lng'] ?? null,
        row['obj_binary'] ?? null,
        row['timestamp'],
        row['tx_id']
      );
    },

    async insertTriples(triples: Triple[]): Promise<void> {
      if (triples.length === 0) {
        return;
      }

      // For a single triple, use the individual insert method
      if (triples.length === 1) {
        return this.insertTriple(triples[0]!);
      }

      // Batch insert using a single SQL statement with multiple VALUES clauses
      // This provides atomicity (all-or-nothing) and better performance
      const rows = triples.map(tripleToRow);

      // Build the VALUES clause with placeholders
      // Each row has 14 columns
      const placeholderRow = '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
      const placeholders = rows.map(() => placeholderRow).join(', ');

      // Flatten all values into a single array for binding
      const values: unknown[] = [];
      for (const row of rows) {
        values.push(
          row['subject'],
          row['predicate'],
          row['obj_type'],
          row['obj_ref'] ?? null,
          row['obj_string'] ?? null,
          row['obj_int64'] ?? null,
          row['obj_float64'] ?? null,
          row['obj_bool'] ?? null,
          row['obj_timestamp'] ?? null,
          row['obj_lat'] ?? null,
          row['obj_lng'] ?? null,
          row['obj_binary'] ?? null,
          row['timestamp'],
          row['tx_id']
        );
      }

      sql.exec(
        `INSERT INTO triples (subject, predicate, obj_type, obj_ref, obj_string, obj_int64, obj_float64, obj_bool, obj_timestamp, obj_lat, obj_lng, obj_binary, timestamp, tx_id)
         VALUES ${placeholders}`,
        ...values
      );
    },

    async getTriple(subject: EntityId, predicate: Predicate): Promise<Triple | null> {
      return this.getLatestTriple(subject, predicate);
    },

    async getTriples(subject: EntityId): Promise<Triple[]> {
      // Get latest version of each predicate for the subject
      const result = sql.exec(
        `SELECT t1.* FROM triples t1
         INNER JOIN (
           SELECT subject, predicate, MAX(timestamp) as max_ts
           FROM triples
           WHERE subject = ?
           GROUP BY subject, predicate
         ) t2 ON t1.subject = t2.subject AND t1.predicate = t2.predicate AND t1.timestamp = t2.max_ts`,
        subject
      );

      return [...result].map(rowToTriple);
    },

    async getTriplesByPredicate(predicate: Predicate): Promise<Triple[]> {
      // Get latest version of each subject for the predicate
      const result = sql.exec(
        `SELECT t1.* FROM triples t1
         INNER JOIN (
           SELECT subject, predicate, MAX(timestamp) as max_ts
           FROM triples
           WHERE predicate = ?
           GROUP BY subject, predicate
         ) t2 ON t1.subject = t2.subject AND t1.predicate = t2.predicate AND t1.timestamp = t2.max_ts`,
        predicate
      );

      return [...result].map(rowToTriple);
    },

    async getTriplesForMultipleSubjects(subjects: EntityId[]): Promise<Map<EntityId, Triple[]>> {
      const resultMap = new Map<EntityId, Triple[]>();

      if (subjects.length === 0) {
        return resultMap;
      }

      // For a single subject, delegate to the existing method
      if (subjects.length === 1) {
        const triples = await this.getTriples(subjects[0]!);
        if (triples.length > 0) {
          resultMap.set(subjects[0]!, triples);
        }
        return resultMap;
      }

      // Build IN clause with placeholders for batch query
      const placeholders = subjects.map(() => '?').join(', ');

      // Get latest version of each predicate for all subjects in a single query
      const result = sql.exec(
        `SELECT t1.* FROM triples t1
         INNER JOIN (
           SELECT subject, predicate, MAX(timestamp) as max_ts
           FROM triples
           WHERE subject IN (${placeholders})
           GROUP BY subject, predicate
         ) t2 ON t1.subject = t2.subject AND t1.predicate = t2.predicate AND t1.timestamp = t2.max_ts`,
        ...subjects
      );

      // Group triples by subject
      for (const row of result) {
        const triple = rowToTriple(row as Record<string, unknown>);
        const existingTriples = resultMap.get(triple.subject) || [];
        existingTriples.push(triple);
        resultMap.set(triple.subject, existingTriples);
      }

      return resultMap;
    },

    async updateTriple(
      subject: EntityId,
      predicate: Predicate,
      newValue: TypedObject,
      txId: TransactionId
    ): Promise<void> {
      // MVCC: insert new version, old version stays
      // Get current max timestamp for this subject/predicate to ensure new version is later
      const currentLatest = await this.getLatestTriple(subject, predicate);
      const currentTimestamp = currentLatest ? Number(currentLatest.timestamp) : 0;
      const newTimestamp = Math.max(Date.now(), currentTimestamp + 1);

      const triple: Triple = {
        subject,
        predicate,
        object: newValue,
        timestamp: BigInt(newTimestamp),
        txId,
      };

      await this.insertTriple(triple);
    },

    async deleteTriple(subject: EntityId, predicate: Predicate, txId: TransactionId): Promise<void> {
      // Soft delete: insert a tombstone (NULL type)
      // Ensure tombstone has a later timestamp than the current latest
      const currentLatest = await this.getLatestTriple(subject, predicate);
      const currentTimestamp = currentLatest ? Number(currentLatest.timestamp) : 0;
      const newTimestamp = Math.max(Date.now(), currentTimestamp + 1);

      const tombstone: Triple = {
        subject,
        predicate,
        object: { type: ObjectType.NULL },
        timestamp: BigInt(newTimestamp),
        txId,
      };

      await this.insertTriple(tombstone);
    },

    async deleteEntity(subject: EntityId, txId: TransactionId): Promise<void> {
      // Get all unique predicates for this subject
      const result = sql.exec(
        `SELECT DISTINCT predicate FROM triples WHERE subject = ?`,
        subject
      );

      const predicates = [...result].map((row) => row['predicate'] as string);

      // Create tombstones for each predicate
      for (const predicate of predicates) {
        await this.deleteTriple(subject, predicate as Predicate, txId);
      }
    },

    async exists(subject: EntityId): Promise<boolean> {
      // An entity exists if it has at least one non-tombstone triple
      // We need to check the latest version of each predicate
      const triples = await this.getTriples(subject);

      // Filter out tombstones (NULL type)
      const nonTombstones = triples.filter((t) => t.object.type !== ObjectType.NULL);

      return nonTombstones.length > 0;
    },

    async getLatestTriple(subject: EntityId, predicate: Predicate): Promise<Triple | null> {
      const result = sql.exec(
        `SELECT * FROM triples
         WHERE subject = ? AND predicate = ?
         ORDER BY timestamp DESC
         LIMIT 1`,
        subject,
        predicate
      );

      const rows = [...result];
      if (rows.length === 0) {
        return null;
      }

      return rowToTriple(rows[0] as Record<string, unknown>);
    },
  };
}
