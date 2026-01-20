/**
 * ChunkStore - BLOB-Optimized Bulk Storage
 *
 * ============================================================================
 * HYBRID ARCHITECTURE - Relationship to crud.ts
 * ============================================================================
 *
 * GraphDB uses a hybrid storage strategy with two complementary stores:
 *
 * **ChunkStore (this module) - Bulk storage:**
 *   - Stores triples in 2MB GraphCol-encoded BLOB chunks
 *   - Optimized for high-volume, append-only workloads
 *   - 1 storage op per ~50,000 triples (massive cost savings)
 *   - Best for: Production bulk ingestion, archival, cold storage
 *
 * **TripleStore (crud.ts) - Indexed storage:**
 *   - Stores triples as individual SQLite rows
 *   - Enables FTS, Geo, Vector, and SPO/POS/OSP indexes
 *   - 1 storage op per triple (enables rich querying)
 *   - Best for: Index building, real-time queries, development
 *
 * **Typical Production Flow:**
 *   1. Ingest bulk data via ChunkStore (cost-efficient)
 *   2. Index selected triples via TripleStore (enables queries)
 *   3. Query uses indexes from triples table, bulk data from chunks
 *
 * **Cost Trade-offs:**
 *   On Cloudflare DO, a 1KB row costs the SAME as a 2MB BLOB.
 *   Individual rows are NOT faster - they're the same cost but enable indexing.
 *
 * ============================================================================
 *
 * Architecture:
 *   Write Request -> In-Memory Buffer -> Flush to 2MB BLOB (only SQLite operation)
 *
 * The ChunkStore maintains:
 * - An in-memory buffer of triples (no SQLite writes until flush)
 * - Flush creates a single GraphCol-encoded BLOB in the chunks table
 * - Query scans buffer first, then chunks
 *
 * @see CLAUDE.md for architecture details
 * @see crud.ts for indexed triple storage with FTS/Geo/Vector support
 * @see schema.ts for the hybrid schema (chunks + triples + index tables)
 */

import type { Triple } from '../core/triple.js';
import type { EntityId, Namespace } from '../core/types.js';
import { encodeGraphCol, decodeGraphCol } from '../storage/graphcol.js';

// ============================================================================
// CONSTANTS
// ============================================================================

/**
 * Target buffer size for optimal 2MB chunks
 * ~50,000 triples typically encode to ~2MB in GraphCol format
 */
export const TARGET_BUFFER_SIZE = 50_000;

/**
 * Minimum chunk size considered for compaction
 * Chunks smaller than this are candidates for merging
 */
export const MIN_CHUNK_SIZE_FOR_COMPACTION = 10_000;

/**
 * Minimum number of small chunks before compaction is triggered
 */
export const MIN_CHUNKS_FOR_COMPACTION = 3;

// ============================================================================
// TYPES
// ============================================================================

/**
 * Chunk metadata returned by listChunks
 */
export interface ChunkMetadata {
  id: string;
  namespace: Namespace;
  tripleCount: number;
  minTimestamp: number;
  maxTimestamp: number;
  sizeBytes: number;
  createdAt: number;
}

/**
 * Statistics about the store
 */
export interface ChunkStoreStats {
  /** Number of triples in the in-memory buffer */
  bufferSize: number;
  /** Number of BLOB chunks in SQLite */
  chunkCount: number;
  /** Total triples stored in chunks */
  totalTriplesInChunks: number;
  /** Total storage bytes used by chunks */
  totalStorageBytes: number;
}

/**
 * ChunkStore interface - BLOB-only architecture
 *
 * The store maintains an in-memory buffer and flushes to 2MB BLOBs.
 * NO individual SQLite rows for triples.
 */
export interface ChunkStore {
  /**
   * The in-memory buffer of triples (NOT stored in SQLite)
   *
   * This buffer accumulates writes until flush() is called.
   * On query, both buffer and chunks are searched.
   */
  buffer: Triple[];

  /**
   * Write triples to the in-memory buffer
   *
   * This is a synchronous operation - no SQLite writes occur.
   * Call flush() when buffer reaches TARGET_BUFFER_SIZE.
   */
  write(triples: Triple[]): void;

  /**
   * Flush the buffer to a 2MB BLOB chunk
   *
   * Encodes all buffered triples to GraphCol format and stores
   * as a single BLOB in the chunks table. Clears the buffer.
   *
   * @returns Chunk ID, or null if buffer was empty
   */
  flush(): Promise<string | null>;

  /**
   * Force flush the buffer regardless of size
   *
   * Used for hibernation/shutdown to persist all buffered data.
   *
   * @returns Chunk ID, or null if buffer was empty
   */
  forceFlush(): Promise<string | null>;

  /**
   * Query triples by subject
   *
   * Searches both the in-memory buffer AND all chunks.
   * Buffer data takes precedence (newer) over chunk data for same predicate.
   *
   * @param subject - EntityId to query
   * @returns Array of matching triples (newest version of each predicate)
   */
  query(subject: EntityId): Promise<Triple[]>;

  /**
   * Get store statistics
   */
  getStats(): Promise<ChunkStoreStats>;

  /**
   * List all chunks with metadata
   */
  listChunks(): Promise<ChunkMetadata[]>;

  /**
   * Delete a chunk by ID
   */
  deleteChunk(chunkId: string): Promise<void>;

  /**
   * Read all triples from a specific chunk
   *
   * @param chunkId - The chunk ID to read
   * @returns Array of triples from the chunk, or empty array if not found
   */
  readChunk(chunkId: string): Promise<Triple[]>;

  /**
   * Compact small chunks into larger ones
   *
   * Merges chunks smaller than MIN_CHUNK_SIZE_FOR_COMPACTION into
   * new optimally-sized chunks. Returns the number of chunks compacted.
   *
   * Background compaction helps maintain cost efficiency by reducing
   * the number of chunk rows while preserving all data.
   *
   * @returns Number of source chunks that were compacted
   */
  compact(): Promise<number>;
}

// ============================================================================
// IMPLEMENTATION
// ============================================================================

/**
 * Generate a unique chunk ID
 */
function generateChunkId(): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 8);
  return `chunk_${timestamp}_${random}`;
}

/**
 * Create a ChunkStore for BLOB-only triple storage
 *
 * @param sql - SqlStorage instance from DurableObjectState
 * @param namespace - Namespace for this store
 * @returns ChunkStore instance
 */
export function createChunkStore(sql: SqlStorage, namespace: Namespace): ChunkStore {
  // In-memory buffer - NO SQLite until flush
  const buffer: Triple[] = [];

  /**
   * Write triples to in-memory buffer (not SQLite)
   *
   * Optimized for batch inserts:
   * - Uses Array.prototype.push.apply for small batches (faster for <10000)
   * - Uses for-loop for large batches (avoids stack overflow)
   * - O(n) time complexity for n triples
   */
  function write(triples: Triple[]): void {
    if (triples.length === 0) {
      return;
    }

    // For large arrays, spread operator can cause stack overflow
    // Use for-loop with pre-allocation hint for large batches
    if (triples.length > 10000) {
      // Pre-extend the buffer capacity hint (V8 optimization)
      const startLen = buffer.length;
      buffer.length = startLen + triples.length;
      for (let i = 0; i < triples.length; i++) {
        buffer[startLen + i] = triples[i]!;
      }
    } else {
      // For smaller batches, push.apply is faster than spread
      // and doesn't have stack overflow issues
      Array.prototype.push.apply(buffer, triples);
    }
  }

  /**
   * Internal flush implementation
   */
  async function flushInternal(): Promise<string | null> {
    if (buffer.length === 0) {
      return null;
    }

    // Encode triples to GraphCol BLOB
    const encoded = encodeGraphCol(buffer, namespace);

    // Calculate min/max timestamps
    let minTs = buffer[0]!.timestamp;
    let maxTs = buffer[0]!.timestamp;
    for (let i = 1; i < buffer.length; i++) {
      if (buffer[i]!.timestamp < minTs) minTs = buffer[i]!.timestamp;
      if (buffer[i]!.timestamp > maxTs) maxTs = buffer[i]!.timestamp;
    }

    // Generate chunk ID
    const chunkId = generateChunkId();
    const now = Date.now();

    // Store as single BLOB - the ONLY SQLite write operation
    sql.exec(
      `INSERT INTO chunks (id, namespace, triple_count, min_timestamp, max_timestamp, data, size_bytes, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      chunkId,
      namespace,
      buffer.length,
      Number(minTs),
      Number(maxTs),
      encoded,
      encoded.length,
      now
    );

    // Clear the buffer
    buffer.length = 0;

    return chunkId;
  }

  /**
   * Flush buffer to BLOB chunk
   */
  async function flush(): Promise<string | null> {
    return flushInternal();
  }

  /**
   * Force flush (for hibernation)
   */
  async function forceFlush(): Promise<string | null> {
    return flushInternal();
  }

  /**
   * Query triples by subject
   *
   * Searches buffer first (newer data), then chunks.
   * Returns the newest version of each predicate.
   */
  async function query(subject: EntityId): Promise<Triple[]> {
    // Map to track newest version of each predicate
    const resultsByPredicate = new Map<string, Triple>();

    // First, search the buffer (newest data)
    for (const triple of buffer) {
      if (triple.subject === subject) {
        const existing = resultsByPredicate.get(triple.predicate);
        if (!existing || triple.timestamp > existing.timestamp) {
          resultsByPredicate.set(triple.predicate, triple);
        }
      }
    }

    // Then search all chunks
    const chunksResult = sql.exec(
      `SELECT data FROM chunks WHERE namespace = ? ORDER BY created_at DESC`,
      namespace
    );

    for (const row of chunksResult) {
      const data = row['data'] as ArrayBuffer;
      const triples = decodeGraphCol(new Uint8Array(data));

      for (const triple of triples) {
        if (triple.subject === subject) {
          const existing = resultsByPredicate.get(triple.predicate);
          // Only add if we don't have a newer version from buffer or previous chunks
          if (!existing || triple.timestamp > existing.timestamp) {
            resultsByPredicate.set(triple.predicate, triple);
          }
        }
      }
    }

    return Array.from(resultsByPredicate.values());
  }

  /**
   * Get store statistics
   */
  async function getStats(): Promise<ChunkStoreStats> {
    const statsResult = sql.exec(
      `SELECT COUNT(*) as chunk_count,
              COALESCE(SUM(triple_count), 0) as total_triples,
              COALESCE(SUM(size_bytes), 0) as total_bytes
       FROM chunks WHERE namespace = ?`,
      namespace
    );

    const stats = [...statsResult][0] ?? { chunk_count: 0, total_triples: 0, total_bytes: 0 };

    return {
      bufferSize: buffer.length,
      chunkCount: Number(stats['chunk_count']),
      totalTriplesInChunks: Number(stats['total_triples']),
      totalStorageBytes: Number(stats['total_bytes']),
    };
  }

  /**
   * List all chunks with metadata
   */
  async function listChunks(): Promise<ChunkMetadata[]> {
    const result = sql.exec(
      `SELECT id, namespace, triple_count, min_timestamp, max_timestamp, size_bytes, created_at
       FROM chunks WHERE namespace = ? ORDER BY created_at DESC`,
      namespace
    );

    const chunks: ChunkMetadata[] = [];
    for (const row of result) {
      chunks.push({
        id: row['id'] as string,
        namespace: row['namespace'] as Namespace,
        tripleCount: row['triple_count'] as number,
        minTimestamp: row['min_timestamp'] as number,
        maxTimestamp: row['max_timestamp'] as number,
        sizeBytes: row['size_bytes'] as number,
        createdAt: row['created_at'] as number,
      });
    }

    return chunks;
  }

  /**
   * Delete a chunk by ID
   */
  async function deleteChunk(chunkId: string): Promise<void> {
    sql.exec(`DELETE FROM chunks WHERE id = ?`, chunkId);
  }

  /**
   * Read all triples from a specific chunk
   */
  async function readChunk(chunkId: string): Promise<Triple[]> {
    const result = sql.exec(
      `SELECT data FROM chunks WHERE id = ?`,
      chunkId
    );

    const rows = [...result];
    if (rows.length === 0) {
      return [];
    }

    const data = rows[0]!['data'] as ArrayBuffer;
    return decodeGraphCol(new Uint8Array(data));
  }

  /**
   * Compact small chunks into larger ones
   *
   * Finds chunks smaller than MIN_CHUNK_SIZE_FOR_COMPACTION and merges
   * them into new optimally-sized chunks. Old chunks are deleted after
   * successful merge.
   */
  async function compact(): Promise<number> {
    // Find small chunks eligible for compaction
    const smallChunksResult = sql.exec(
      `SELECT id, data, triple_count FROM chunks
       WHERE namespace = ? AND triple_count < ?
       ORDER BY created_at ASC`,
      namespace,
      MIN_CHUNK_SIZE_FOR_COMPACTION
    );

    const smallChunks = [...smallChunksResult];

    // Need at least MIN_CHUNKS_FOR_COMPACTION small chunks to compact
    if (smallChunks.length < MIN_CHUNKS_FOR_COMPACTION) {
      return 0;
    }

    // Decode all small chunks and merge their triples
    const allTriples: Triple[] = [];
    const chunkIdsToDelete: string[] = [];

    for (const chunk of smallChunks) {
      const data = chunk['data'] as ArrayBuffer;
      const triples = decodeGraphCol(new Uint8Array(data));
      allTriples.push(...triples);
      chunkIdsToDelete.push(chunk['id'] as string);
    }

    // Sort by timestamp to maintain order
    allTriples.sort((a, b) => Number(a.timestamp - b.timestamp));

    // Create new optimally-sized chunks
    const newChunkIds: string[] = [];
    for (let i = 0; i < allTriples.length; i += TARGET_BUFFER_SIZE) {
      const chunkTriples = allTriples.slice(i, i + TARGET_BUFFER_SIZE);

      if (chunkTriples.length === 0) continue;

      // Encode to GraphCol BLOB
      const encoded = encodeGraphCol(chunkTriples, namespace);

      // Calculate min/max timestamps
      let minTs = chunkTriples[0]!.timestamp;
      let maxTs = chunkTriples[0]!.timestamp;
      for (let j = 1; j < chunkTriples.length; j++) {
        if (chunkTriples[j]!.timestamp < minTs) minTs = chunkTriples[j]!.timestamp;
        if (chunkTriples[j]!.timestamp > maxTs) maxTs = chunkTriples[j]!.timestamp;
      }

      // Generate chunk ID and store
      const chunkId = generateChunkId();
      const now = Date.now();

      sql.exec(
        `INSERT INTO chunks (id, namespace, triple_count, min_timestamp, max_timestamp, data, size_bytes, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        chunkId,
        namespace,
        chunkTriples.length,
        Number(minTs),
        Number(maxTs),
        encoded,
        encoded.length,
        now
      );

      newChunkIds.push(chunkId);
    }

    // Delete old chunks after successful merge
    for (const chunkId of chunkIdsToDelete) {
      sql.exec(`DELETE FROM chunks WHERE id = ?`, chunkId);
    }

    return chunkIdsToDelete.length;
  }

  return {
    buffer,
    write,
    flush,
    forceFlush,
    query,
    getStats,
    listChunks,
    deleteChunk,
    readChunk,
    compact,
  };
}

// ============================================================================
// LEGACY EXPORTS (deprecated, kept for backwards compatibility)
// ============================================================================

/**
 * @deprecated The triples table has been removed in favor of BLOB-only architecture.
 */
export const HOT_ROW_AGE_MS = 60 * 60 * 1000;

/**
 * @deprecated The triples table has been removed in favor of BLOB-only architecture.
 */
export const MIN_ROWS_FOR_COMPACTION = 100;

/**
 * @deprecated Use TARGET_BUFFER_SIZE instead.
 */
export const MAX_TRIPLES_PER_CHUNK = TARGET_BUFFER_SIZE;

/**
 * Initialize the chunks schema
 *
 * @deprecated Use initializeSchema from schema.ts instead
 */
export function initializeChunksSchema(_sql: SqlStorage): void {
  // No-op: Schema is now handled by schema.ts
  // Kept for backwards compatibility
}
