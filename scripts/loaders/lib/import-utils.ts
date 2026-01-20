/**
 * GraphDB Import Utilities
 *
 * Shared utilities for dataset loaders that import data into GraphDB.
 * Provides triple batching, encoding, TSV parsing, gzip decompression,
 * bloom filter generation, and manifest management.
 *
 * @packageDocumentation
 */

import type { Triple, TypedObject } from '../../../src/core/triple';
import type { EntityId, Predicate, TransactionId, Namespace } from '../../../src/core/types';
import { ObjectType, createEntityId, createPredicate, createTransactionId, createNamespace } from '../../../src/core/types';
import { encodeGraphCol } from '../../../src/storage/graphcol';
import {
  createBloomFilter,
  addToFilter,
  addManyToFilter,
  serializeFilter,
  deserializeFilter as bloomDeserializeFilter,
  type BloomFilter,
  type SerializedFilter,
} from '../../../src/snippet/bloom';

// ============================================================================
// Types
// ============================================================================

/**
 * Statistics for a batch encoder
 */
export interface BatchEncoderStats {
  /** Number of chunks written */
  chunks: number;
  /** Total number of triples written */
  triples: number;
  /** Total bytes written */
  bytes: number;
}

/**
 * BatchEncoder interface for accumulating and encoding triples
 */
export interface BatchEncoder {
  /** Add a triple to the current batch */
  add(triple: Triple): void;
  /** Flush the current batch to R2 storage */
  flush(): Promise<void>;
  /** Get current statistics */
  getStats(): BatchEncoderStats;
  /** Get all chunk info for manifest generation */
  getChunkInfos(): ChunkInfo[];
  /** Finalize and write manifest */
  finalize(): Promise<ChunkManifest>;
}

/**
 * Options for creating a batch encoder
 */
export interface BatchEncoderOptions {
  /** Number of triples per batch (default: 10000) */
  batchSize?: number;
  /** Whether to generate bloom filters for chunks (default: true) */
  generateBloom?: boolean;
  /** Target false positive rate for bloom filters (default: 0.01) */
  bloomFpr?: number;
}

/**
 * Information about a written chunk
 */
export interface ChunkInfo {
  /** Unique chunk identifier */
  id: string;
  /** Number of triples in the chunk */
  tripleCount: number;
  /** Minimum timestamp in the chunk */
  minTime: bigint;
  /** Maximum timestamp in the chunk */
  maxTime: bigint;
  /** Size in bytes */
  bytes: number;
  /** Path in R2 */
  path: string;
  /** Bloom filter for the chunk (if generated) */
  bloom?: SerializedFilter;
}

/**
 * Manifest for a namespace's chunks
 */
export interface ChunkManifest {
  /** Namespace URL */
  namespace: string;
  /** List of chunks */
  chunks: {
    id: string;
    tripleCount: number;
    minTime: bigint;
    maxTime: bigint;
    bytes: number;
    path: string;
  }[];
  /** Total triples across all chunks */
  totalTriples: number;
  /** Manifest creation timestamp */
  createdAt: string;
  /** Version for cache invalidation */
  version: string;
  /** Combined bloom filter for all entities */
  combinedBloom?: SerializedFilter;
}

/**
 * TSV column definition for parsing
 */
export interface TsvColumnDef {
  /** Column name */
  name: string;
  /** Optional type hint */
  type?: 'string' | 'number' | 'boolean' | 'bigint';
}

// ============================================================================
// TSV Streaming Parser
// ============================================================================

/**
 * Create a streaming TSV parser transform stream
 *
 * Parses tab-separated values into records. Handles:
 * - Header row detection
 * - Quoted fields
 * - Line continuation
 * - UTF-8 encoding
 *
 * @param options Optional column definitions for type conversion
 * @returns TransformStream that converts Uint8Array chunks to records
 *
 * @example
 * ```typescript
 * const response = await fetch('data.tsv');
 * const records = response.body
 *   .pipeThrough(createTsvParser())
 *   .getReader();
 *
 * let result = await records.read();
 * while (!result.done) {
 *   console.log(result.value); // { col1: 'val1', col2: 'val2' }
 *   result = await records.read();
 * }
 * ```
 */
export function createTsvParser(options?: {
  columns?: TsvColumnDef[];
  hasHeader?: boolean;
  delimiter?: string;
}): TransformStream<Uint8Array, Record<string, string>> {
  const delimiter = options?.delimiter ?? '\t';
  const hasHeader = options?.hasHeader ?? true;

  let headers: string[] | null = options?.columns?.map((c) => c.name) ?? null;
  let buffer = '';
  const decoder = new TextDecoder('utf-8');

  return new TransformStream<Uint8Array, Record<string, string>>({
    transform(chunk, controller) {
      buffer += decoder.decode(chunk, { stream: true });

      // Process complete lines
      const lines = buffer.split('\n');
      // Keep the last potentially incomplete line in buffer
      buffer = lines.pop() ?? '';

      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;

        const fields = parseTsvLine(trimmed, delimiter);

        // First line is header if hasHeader is true and no predefined columns
        if (hasHeader && headers === null) {
          headers = fields;
          continue;
        }

        // If no headers defined yet, generate column names
        if (headers === null) {
          // Use numeric indices as keys
          headers = fields.map((_, i) => `col${i}`);
          // Note: we fall through to process this first line as data
        }

        // Build record
        const record: Record<string, string> = {};
        for (let i = 0; i < headers.length && i < fields.length; i++) {
          record[headers[i]] = fields[i];
        }

        controller.enqueue(record);
      }
    },

    flush(controller) {
      // Process any remaining data in buffer
      if (buffer.trim()) {
        const fields = parseTsvLine(buffer.trim(), delimiter);

        if (headers === null) {
          headers = fields.map((_, i) => `col${i}`);
        }

        if (headers.length > 0 && fields.length > 0) {
          const record: Record<string, string> = {};
          for (let i = 0; i < headers.length && i < fields.length; i++) {
            record[headers[i]] = fields[i];
          }
          controller.enqueue(record);
        }
      }
    },
  });
}

/**
 * Parse a single TSV line, handling quoted fields
 */
function parseTsvLine(line: string, delimiter: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;
  let i = 0;

  while (i < line.length) {
    const char = line[i];

    if (inQuotes) {
      if (char === '"') {
        // Check for escaped quote
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      current += char;
      i++;
    } else {
      if (char === '"') {
        inQuotes = true;
        i++;
        continue;
      }
      if (char === delimiter) {
        fields.push(current);
        current = '';
        i++;
        continue;
      }
      current += char;
      i++;
    }
  }

  fields.push(current);
  return fields;
}

// ============================================================================
// Gzip Streaming Decompression
// ============================================================================

/**
 * Create a streaming gzip decompressor transform stream
 *
 * Uses the native DecompressionStream API available in modern runtimes
 * (Cloudflare Workers, Node 18+, Deno, modern browsers).
 *
 * @returns TransformStream that decompresses gzip data
 *
 * @example
 * ```typescript
 * const response = await fetch('data.tsv.gz');
 * const decompressed = response.body
 *   .pipeThrough(createGzipDecompressor())
 *   .pipeThrough(createTsvParser());
 * ```
 */
export function createGzipDecompressor(): TransformStream<Uint8Array, Uint8Array> {
  // Use native DecompressionStream
  return new DecompressionStream('gzip');
}

// ============================================================================
// Bloom Filter for Chunk
// ============================================================================

/**
 * Create a bloom filter for a chunk's entities
 *
 * Generates a bloom filter containing all unique subject and object REF
 * entity IDs from the triples. This enables efficient negative lookups
 * at the edge layer.
 *
 * @param triples Array of triples to include in the bloom filter
 * @param options Optional bloom filter configuration
 * @returns Uint8Array containing the serialized bloom filter
 *
 * @example
 * ```typescript
 * const bloom = createChunkBloom(triples);
 * // Store bloom with chunk metadata
 * await r2.put(`${chunkPath}.bloom`, bloom);
 * ```
 */
export function createChunkBloom(
  triples: Triple[],
  options?: {
    targetFpr?: number;
    maxSizeBytes?: number;
  }
): Uint8Array {
  // Collect unique entity IDs (subjects and REF objects)
  const entityIds = new Set<string>();

  for (const triple of triples) {
    entityIds.add(triple.subject);

    if (triple.object.type === ObjectType.REF) {
      entityIds.add(triple.object.value);
    } else if (triple.object.type === ObjectType.REF_ARRAY) {
      for (const ref of triple.object.value) {
        entityIds.add(ref);
      }
    }
  }

  // Create bloom filter sized for the entities
  const capacity = Math.max(entityIds.size, 100); // Minimum 100 capacity
  const filter = createBloomFilter({
    capacity,
    targetFpr: options?.targetFpr ?? 0.01,
    maxSizeBytes: options?.maxSizeBytes ?? 16 * 1024,
  });

  // Add all entity IDs
  addManyToFilter(filter, Array.from(entityIds));

  // Return serialized filter as bytes
  const serialized = serializeFilter(filter);
  const encoder = new TextEncoder();
  return encoder.encode(JSON.stringify(serialized));
}

/**
 * Create a serialized bloom filter object for a chunk
 *
 * Similar to createChunkBloom but returns a SerializedFilter object
 * for embedding in chunk metadata.
 */
export function createChunkBloomSerialized(
  triples: Triple[],
  options?: {
    targetFpr?: number;
    maxSizeBytes?: number;
  }
): SerializedFilter {
  // Collect unique entity IDs
  const entityIds = new Set<string>();

  for (const triple of triples) {
    entityIds.add(triple.subject);

    if (triple.object.type === ObjectType.REF) {
      entityIds.add(triple.object.value);
    } else if (triple.object.type === ObjectType.REF_ARRAY) {
      for (const ref of triple.object.value) {
        entityIds.add(ref);
      }
    }
  }

  const capacity = Math.max(entityIds.size, 100);
  const filter = createBloomFilter({
    capacity,
    targetFpr: options?.targetFpr ?? 0.01,
    maxSizeBytes: options?.maxSizeBytes ?? 16 * 1024,
  });

  addManyToFilter(filter, Array.from(entityIds));

  return serializeFilter(filter);
}

// ============================================================================
// Manifest Generation
// ============================================================================

/**
 * Generate a manifest for a namespace's chunks
 *
 * Creates a manifest document that describes all chunks in a namespace,
 * enabling efficient chunk discovery and query planning.
 *
 * @param namespace The namespace URL
 * @param chunks Array of chunk information
 * @param options Optional manifest options
 * @returns ChunkManifest document
 *
 * @example
 * ```typescript
 * const manifest = generateManifest('https://imdb.com/movies/', chunks);
 * await r2.put('https://imdb.com/movies/_manifest.json', JSON.stringify(manifest));
 * ```
 */
export function generateManifest(
  namespace: string,
  chunks: ChunkInfo[],
  options?: {
    combinedBloom?: SerializedFilter;
    version?: string;
  }
): ChunkManifest {
  const totalTriples = chunks.reduce((sum, c) => sum + c.tripleCount, 0);

  return {
    namespace,
    chunks: chunks.map((c) => ({
      id: c.id,
      tripleCount: c.tripleCount,
      minTime: c.minTime,
      maxTime: c.maxTime,
      bytes: c.bytes,
      path: c.path,
    })),
    totalTriples,
    createdAt: new Date().toISOString(),
    version: options?.version ?? generateVersion(),
    combinedBloom: options?.combinedBloom,
  };
}

/**
 * Generate a version string for cache invalidation
 */
function generateVersion(): string {
  return `v${Date.now().toString(36)}`;
}

/**
 * Serialize a manifest to JSON with bigint support
 */
export function serializeManifest(manifest: ChunkManifest): string {
  return JSON.stringify(manifest, (key, value) => {
    if (typeof value === 'bigint') {
      return value.toString();
    }
    return value;
  }, 2);
}

/**
 * Deserialize a manifest from JSON with bigint support
 */
export function deserializeManifest(json: string): ChunkManifest {
  const parsed = JSON.parse(json);

  return {
    ...parsed,
    chunks: parsed.chunks.map((c: { minTime: string; maxTime: string }) => ({
      ...c,
      minTime: BigInt(c.minTime),
      maxTime: BigInt(c.maxTime),
    })),
  };
}

// ============================================================================
// Batch Encoder
// ============================================================================

/**
 * Create a batch encoder for streaming triples to R2
 *
 * Accumulates triples into batches and writes them as GraphCol-encoded
 * chunks to R2 storage. Tracks statistics and generates chunk metadata.
 *
 * @param r2 R2 bucket binding
 * @param namespace Namespace URL for the data
 * @param options Optional encoder configuration
 * @returns BatchEncoder instance
 *
 * @example
 * ```typescript
 * const encoder = createBatchEncoder(env.DATASETS, 'https://imdb.com/movies/');
 *
 * for (const record of records) {
 *   encoder.add(recordToTriple(record));
 * }
 *
 * await encoder.flush();
 * const manifest = await encoder.finalize();
 * ```
 */
export function createBatchEncoder(
  r2: R2Bucket,
  namespace: string,
  options?: BatchEncoderOptions
): BatchEncoder {
  const batchSize = options?.batchSize ?? 10000;
  const generateBloom = options?.generateBloom ?? true;
  const bloomFpr = options?.bloomFpr ?? 0.01;

  // Validate namespace is a valid URL
  const ns = createNamespace(namespace);

  // Internal state
  let buffer: Triple[] = [];
  let stats: BatchEncoderStats = { chunks: 0, triples: 0, bytes: 0 };
  const chunkInfos: ChunkInfo[] = [];
  const allEntityIds = new Set<string>();

  // Generate chunk path from namespace
  function getChunkPath(chunkId: string): string {
    const url = new URL(namespace);
    const domainParts = url.hostname.split('.');
    const reversedDomain = domainParts.reverse().map((part) => `.${part}`).join('/');
    const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
    const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';

    return `${reversedDomain}${pathStr}/_chunks/${chunkId}.gcol`;
  }

  // Generate unique chunk ID
  function generateChunkId(): string {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 8);
    return `${timestamp.toString(36)}-${random}`;
  }

  // Collect entity IDs from triples
  function collectEntityIds(triples: Triple[]): void {
    for (const triple of triples) {
      allEntityIds.add(triple.subject);
      if (triple.object.type === ObjectType.REF) {
        allEntityIds.add(triple.object.value);
      } else if (triple.object.type === ObjectType.REF_ARRAY) {
        for (const ref of triple.object.value) {
          allEntityIds.add(ref);
        }
      }
    }
  }

  return {
    add(triple: Triple): void {
      buffer.push(triple);
    },

    async flush(): Promise<void> {
      if (buffer.length === 0) {
        return;
      }

      // Encode the batch
      const triplesToEncode = buffer;
      buffer = [];

      const encoded = encodeGraphCol(triplesToEncode, ns);
      const chunkId = generateChunkId();
      const path = getChunkPath(chunkId);

      // Calculate time range
      let minTime = triplesToEncode[0].timestamp;
      let maxTime = triplesToEncode[0].timestamp;
      for (const t of triplesToEncode) {
        if (t.timestamp < minTime) minTime = t.timestamp;
        if (t.timestamp > maxTime) maxTime = t.timestamp;
      }

      // Collect entity IDs for combined bloom
      collectEntityIds(triplesToEncode);

      // Generate chunk bloom filter if enabled
      let bloom: SerializedFilter | undefined;
      if (generateBloom) {
        bloom = createChunkBloomSerialized(triplesToEncode, { targetFpr: bloomFpr });
      }

      // Write to R2
      await r2.put(path, encoded);

      // Record chunk info
      const chunkInfo: ChunkInfo = {
        id: chunkId,
        tripleCount: triplesToEncode.length,
        minTime,
        maxTime,
        bytes: encoded.length,
        path,
        bloom,
      };
      chunkInfos.push(chunkInfo);

      // Update stats
      stats.chunks++;
      stats.triples += triplesToEncode.length;
      stats.bytes += encoded.length;
    },

    getStats(): BatchEncoderStats {
      return { ...stats };
    },

    getChunkInfos(): ChunkInfo[] {
      return [...chunkInfos];
    },

    async finalize(): Promise<ChunkManifest> {
      // Flush any remaining triples
      await this.flush();

      // Generate combined bloom filter for all entities
      let combinedBloom: SerializedFilter | undefined;
      if (generateBloom && allEntityIds.size > 0) {
        const filter = createBloomFilter({
          capacity: allEntityIds.size,
          targetFpr: bloomFpr,
          maxSizeBytes: 64 * 1024, // 64KB for combined filter
        });
        addManyToFilter(filter, Array.from(allEntityIds));
        combinedBloom = serializeFilter(filter);
      }

      // Generate manifest
      const manifest = generateManifest(namespace, chunkInfos, { combinedBloom });

      // Write manifest to R2
      const url = new URL(namespace);
      const domainParts = url.hostname.split('.');
      const reversedDomain = domainParts.reverse().map((part) => `.${part}`).join('/');
      const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
      const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';
      const manifestPath = `${reversedDomain}${pathStr}/_manifest.json`;

      await r2.put(manifestPath, serializeManifest(manifest));

      return manifest;
    },
  };
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Generate a ULID-style transaction ID
 *
 * Creates a lexicographically sortable unique identifier with embedded
 * timestamp for transaction ordering.
 */
export function generateTxId(): TransactionId {
  const timestamp = Date.now();
  const random = crypto.getRandomValues(new Uint8Array(10));

  // Encode timestamp (48 bits) in Crockford Base32
  const CHARS = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  let result = '';

  // First 10 chars: timestamp
  let t = timestamp;
  for (let i = 0; i < 10; i++) {
    result = CHARS[t & 31] + result;
    t = Math.floor(t / 32);
  }

  // Last 16 chars: random
  for (let i = 0; i < 16; i++) {
    const idx = i < 10 ? random[i] & 31 : (random[i - 10] >> 3) & 31;
    result += CHARS[idx];
  }

  return createTransactionId(result);
}

/**
 * Create a triple from basic components
 *
 * Helper function for creating triples with sensible defaults.
 */
export function makeTriple(
  subject: string,
  predicate: string,
  value: unknown,
  objectType?: ObjectType,
  options?: {
    timestamp?: bigint;
    txId?: TransactionId;
  }
): Triple {
  const subjectId = createEntityId(subject);
  const predicateName = createPredicate(predicate);
  const timestamp = options?.timestamp ?? BigInt(Date.now());
  const txId = options?.txId ?? generateTxId();

  // Infer object type from value if not specified
  let object: TypedObject;

  if (objectType !== undefined) {
    object = createTypedObject(value, objectType);
  } else {
    object = inferTypedObject(value);
  }

  return {
    subject: subjectId,
    predicate: predicateName,
    object,
    timestamp,
    txId,
  };
}

/**
 * Create a TypedObject with explicit type
 */
function createTypedObject(value: unknown, type: ObjectType): TypedObject {
  switch (type) {
    case ObjectType.NULL:
      return { type: ObjectType.NULL };
    case ObjectType.BOOL:
      return { type: ObjectType.BOOL, value: Boolean(value) };
    case ObjectType.INT32:
      return { type: ObjectType.INT32, value: BigInt(value as number) };
    case ObjectType.INT64:
      return { type: ObjectType.INT64, value: BigInt(value as number) };
    case ObjectType.FLOAT64:
      return { type: ObjectType.FLOAT64, value: Number(value) };
    case ObjectType.STRING:
      return { type: ObjectType.STRING, value: String(value) };
    case ObjectType.BINARY:
      return { type: ObjectType.BINARY, value: value as Uint8Array };
    case ObjectType.TIMESTAMP:
      return { type: ObjectType.TIMESTAMP, value: BigInt(value as number) };
    case ObjectType.DATE:
      return { type: ObjectType.DATE, value: Number(value) };
    case ObjectType.DURATION:
      return { type: ObjectType.DURATION, value: String(value) };
    case ObjectType.REF:
      return { type: ObjectType.REF, value: createEntityId(String(value)) };
    case ObjectType.REF_ARRAY:
      return {
        type: ObjectType.REF_ARRAY,
        value: (value as string[]).map((v) => createEntityId(v)),
      };
    case ObjectType.JSON:
      return { type: ObjectType.JSON, value };
    case ObjectType.GEO_POINT:
      return { type: ObjectType.GEO_POINT, value: value as { lat: number; lng: number } };
    case ObjectType.GEO_POLYGON:
      return { type: ObjectType.GEO_POLYGON, value: value as { exterior: { lat: number; lng: number }[]; holes?: { lat: number; lng: number }[][] } };
    case ObjectType.GEO_LINESTRING:
      return { type: ObjectType.GEO_LINESTRING, value: value as { points: { lat: number; lng: number }[] } };
    case ObjectType.URL:
      return { type: ObjectType.URL, value: String(value) };
    default:
      return { type: ObjectType.STRING, value: String(value) };
  }
}

/**
 * Infer TypedObject from JavaScript value
 */
function inferTypedObject(value: unknown): TypedObject {
  if (value === null || value === undefined) {
    return { type: ObjectType.NULL };
  }

  if (typeof value === 'boolean') {
    return { type: ObjectType.BOOL, value };
  }

  if (typeof value === 'bigint') {
    return { type: ObjectType.INT64, value };
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { type: ObjectType.INT64, value: BigInt(value) };
    }
    return { type: ObjectType.FLOAT64, value };
  }

  if (typeof value === 'string') {
    // Check if it's a URL that could be a reference
    if (value.startsWith('http://') || value.startsWith('https://')) {
      try {
        return { type: ObjectType.REF, value: createEntityId(value) };
      } catch {
        return { type: ObjectType.STRING, value };
      }
    }
    return { type: ObjectType.STRING, value };
  }

  if (value instanceof Uint8Array) {
    return { type: ObjectType.BINARY, value };
  }

  if (value instanceof Date) {
    return { type: ObjectType.TIMESTAMP, value: BigInt(value.getTime()) };
  }

  if (Array.isArray(value)) {
    // Check if all elements are URL strings (REF_ARRAY)
    if (value.every((v) => typeof v === 'string' && (v.startsWith('http://') || v.startsWith('https://')))) {
      try {
        return {
          type: ObjectType.REF_ARRAY,
          value: value.map((v) => createEntityId(v)),
        };
      } catch {
        return { type: ObjectType.JSON, value };
      }
    }
    return { type: ObjectType.JSON, value };
  }

  // Check for GeoPoint
  if (typeof value === 'object' && value !== null) {
    const obj = value as Record<string, unknown>;
    if ('lat' in obj && 'lng' in obj && typeof obj.lat === 'number' && typeof obj.lng === 'number') {
      return { type: ObjectType.GEO_POINT, value: obj as { lat: number; lng: number } };
    }
  }

  // Default to JSON for complex objects
  return { type: ObjectType.JSON, value };
}

/**
 * Parse a record from TSV into triples
 *
 * Generic helper for converting TSV records to triples. The caller
 * provides a mapping function for customizing the conversion.
 */
export function parseRecordToTriples<T extends Record<string, string>>(
  record: T,
  options: {
    /** Function to generate subject ID from record */
    getSubject: (record: T) => string;
    /** Function to map field names to predicates (return null to skip) */
    mapPredicate?: (field: string) => string | null;
    /** Function to map field values to typed objects */
    mapValue?: (field: string, value: string) => TypedObject | null;
    /** Fields to skip */
    skipFields?: string[];
    /** Transaction ID (generated if not provided) */
    txId?: TransactionId;
    /** Timestamp (generated if not provided) */
    timestamp?: bigint;
  }
): Triple[] {
  const triples: Triple[] = [];
  const subject = createEntityId(options.getSubject(record));
  const txId = options.txId ?? generateTxId();
  const timestamp = options.timestamp ?? BigInt(Date.now());
  const skipFields = new Set(options.skipFields ?? []);

  for (const [field, value] of Object.entries(record)) {
    // Skip empty values and excluded fields
    if (!value || skipFields.has(field)) {
      continue;
    }

    // Map field to predicate (null means skip this field)
    let predicateName: string | null;
    if (options.mapPredicate) {
      predicateName = options.mapPredicate(field);
      if (predicateName === null) {
        continue; // Explicitly skipped by mapper
      }
    } else {
      predicateName = field;
    }

    // Validate predicate doesn't contain colons
    if (predicateName.includes(':')) {
      continue;
    }

    const predicate = createPredicate(predicateName);

    // Map value to typed object
    let object: TypedObject;
    if (options.mapValue) {
      const mapped = options.mapValue(field, value);
      if (mapped === null) {
        continue;
      }
      object = mapped;
    } else {
      object = { type: ObjectType.STRING, value };
    }

    triples.push({
      subject,
      predicate,
      object,
      timestamp,
      txId,
    });
  }

  return triples;
}

// ============================================================================
// Streaming Import Types
// ============================================================================

/**
 * State for StreamingLineReader - can be persisted and restored
 */
export interface LineReaderState {
  /** Total bytes processed so far */
  bytesProcessed: number;
  /** Total lines emitted so far */
  linesEmitted: number;
  /** Partial line carried over from previous chunk */
  partialLine: string;
}

/**
 * State for BatchedTripleWriter - can be persisted and restored
 */
export interface BatchWriterState {
  /** Number of triples written to storage */
  triplesWritten: number;
  /** Number of chunks uploaded */
  chunksUploaded: number;
  /** Total bytes uploaded */
  bytesUploaded: number;
  /** Chunk infos for manifest */
  chunkInfos: ChunkInfo[];
  /** Serialized bloom filter state */
  bloomState: SerializedFilter;
}

/**
 * Result from BatchedTripleWriter.finalize()
 */
export interface WriterResult {
  /** Total triples written */
  totalTriples: number;
  /** Total chunks uploaded */
  totalChunks: number;
  /** Total bytes uploaded */
  totalBytes: number;
  /** Chunk manifest */
  chunks: ChunkInfo[];
  /** Combined bloom filter */
  combinedBloom: SerializedFilter;
}

/**
 * Import checkpoint for resumability
 */
export interface ImportCheckpoint {
  /** Unique job identifier */
  jobId: string;
  /** Source URL being imported */
  sourceUrl: string;
  /** Current byte offset in source */
  byteOffset: number;
  /** Total bytes in source (if known) */
  totalBytes?: number;
  /** Lines processed so far */
  linesProcessed: number;
  /** Triples written so far */
  triplesWritten: number;
  /** Line reader state */
  lineReaderState: LineReaderState;
  /** Batch writer state */
  batchWriterState: BatchWriterState;
  /** Timestamp of last checkpoint */
  checkpointedAt: string;
  /** Custom metadata */
  metadata?: Record<string, unknown>;
}

/**
 * Result from a range fetch operation
 */
export interface RangeFetchResult {
  /** The fetched data chunk */
  data: Uint8Array;
  /** Start byte offset */
  start: number;
  /** End byte offset (exclusive) */
  end: number;
  /** Total size of the resource (if known) */
  totalSize?: number;
  /** Whether this is the last chunk */
  isLast: boolean;
}

/**
 * Configuration for streaming import
 */
export interface StreamingImportConfig {
  /** Source URL to import from */
  url: string;
  /** Data format */
  format: 'tsv' | 'ndjson';
  /** Whether source is gzipped */
  gzipped: boolean;
  /** Target namespace for the data */
  namespace: string;
  /** Whether to use HTTP Range requests (for non-streaming sources) */
  useRangeRequests?: boolean;
  /** Transform function to convert records to triples */
  transform: (record: Record<string, unknown>, txId: TransactionId) => Triple[];
}

/**
 * Result from streaming import
 */
export interface StreamingImportResult {
  /** Whether import completed successfully */
  success: boolean;
  /** Total triples imported */
  triplesImported: number;
  /** Total chunks created */
  chunksCreated: number;
  /** Total bytes written */
  bytesWritten: number;
  /** Generated manifest */
  manifest: ChunkManifest;
  /** Error message if failed */
  error?: string;
}

// ============================================================================
// StreamingLineReader
// ============================================================================

/**
 * StreamingLineReader interface for memory-efficient line processing
 */
export interface StreamingLineReader {
  /** Process a chunk of data and yield complete lines */
  processChunk(chunk: Uint8Array): AsyncGenerator<string>;
  /** Get current state for checkpointing */
  getState(): LineReaderState;
  /** Restore from a saved state */
  restoreState(state: LineReaderState): void;
  /** Flush any remaining partial line */
  flush(): string | null;
}

/**
 * Create a streaming line reader that processes chunks without loading full text
 *
 * Key features:
 * - Only keeps 1 incomplete line in memory (< 64KB typical)
 * - Tracks byte and line counts for checkpointing
 * - Can be paused and resumed with state persistence
 *
 * @param options Configuration options
 * @returns StreamingLineReader instance
 *
 * @example
 * ```typescript
 * const lineReader = createStreamingLineReader();
 *
 * for await (const chunk of fetchChunks(url)) {
 *   for await (const line of lineReader.processChunk(chunk)) {
 *     const record = JSON.parse(line);
 *     // process record...
 *   }
 * }
 *
 * // Handle any remaining partial line
 * const remaining = lineReader.flush();
 * if (remaining) {
 *   const record = JSON.parse(remaining);
 * }
 * ```
 */
export function createStreamingLineReader(options?: {
  /** Maximum buffer size for partial lines (default 64KB) */
  maxBufferSize?: number;
}): StreamingLineReader {
  const maxBufferSize = options?.maxBufferSize ?? 64 * 1024;
  const decoder = new TextDecoder('utf-8');

  let state: LineReaderState = {
    bytesProcessed: 0,
    linesEmitted: 0,
    partialLine: '',
  };

  return {
    async *processChunk(chunk: Uint8Array): AsyncGenerator<string> {
      // Decode chunk to text
      const text = decoder.decode(chunk, { stream: true });
      state.bytesProcessed += chunk.byteLength;

      // Combine with any partial line from previous chunk
      const fullText = state.partialLine + text;

      // Split into lines
      const lines = fullText.split('\n');

      // Last element is either empty (if text ended with \n) or partial
      state.partialLine = lines.pop() ?? '';

      // Check buffer size limit
      if (state.partialLine.length > maxBufferSize) {
        console.warn(
          `[StreamingLineReader] Partial line exceeds ${maxBufferSize} bytes, truncating`
        );
        state.partialLine = state.partialLine.slice(-maxBufferSize);
      }

      // Yield complete lines
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed) {
          state.linesEmitted++;
          yield trimmed;
        }
      }
    },

    getState(): LineReaderState {
      return { ...state };
    },

    restoreState(savedState: LineReaderState): void {
      state = { ...savedState };
    },

    flush(): string | null {
      if (state.partialLine.trim()) {
        const line = state.partialLine.trim();
        state.partialLine = '';
        state.linesEmitted++;
        return line;
      }
      return null;
    },
  };
}

// ============================================================================
// BatchedTripleWriter
// ============================================================================

/**
 * BatchedTripleWriter interface for memory-efficient triple batching
 */
export interface BatchedTripleWriter {
  /** Add a single triple to the batch */
  addTriple(triple: Triple): Promise<void>;
  /** Add multiple triples to the batch */
  addTriples(triples: Triple[]): Promise<void>;
  /** Manually flush current batch to storage */
  flush(): Promise<string | null>;
  /** Check if writer is backpressured (too many pending writes) */
  isBackpressured(): boolean;
  /** Finalize writing and return results */
  finalize(): Promise<WriterResult>;
  /** Get current state for checkpointing */
  getState(): BatchWriterState;
  /** Restore from a saved state */
  restoreState(state: BatchWriterState): void;
}

/**
 * Create a batched triple writer with streaming bloom filter
 *
 * Key features:
 * - Batches triples before flushing (default 10K per batch)
 * - Uses streaming bloom filter instead of unbounded Set
 * - Implements backpressure to prevent memory buildup
 * - Can be checkpointed and resumed
 *
 * @param r2 R2 bucket for storage
 * @param namespace Target namespace URL
 * @param options Configuration options
 * @returns BatchedTripleWriter instance
 *
 * @example
 * ```typescript
 * const writer = createBatchedTripleWriter(env.DATASETS, 'https://example.org/data/');
 *
 * for (const record of records) {
 *   const triples = transformRecord(record);
 *   await writer.addTriples(triples);
 *
 *   // Respect backpressure
 *   while (writer.isBackpressured()) {
 *     await new Promise(resolve => setTimeout(resolve, 100));
 *   }
 * }
 *
 * const result = await writer.finalize();
 * ```
 */
export function createBatchedTripleWriter(
  r2: R2Bucket,
  namespace: string,
  options?: {
    /** Number of triples per batch (default 10000) */
    batchSize?: number;
    /** Maximum pending batches before backpressure (default 2) */
    maxPendingBatches?: number;
    /** Bloom filter capacity (default 1,000,000) */
    bloomCapacity?: number;
    /** Target false positive rate for bloom filter (default 0.01) */
    bloomFpr?: number;
  }
): BatchedTripleWriter {
  const batchSize = options?.batchSize ?? 10000;
  const maxPendingBatches = options?.maxPendingBatches ?? 2;
  const bloomCapacity = options?.bloomCapacity ?? 1_000_000;
  const bloomFpr = options?.bloomFpr ?? 0.01;

  // Validate namespace
  const ns = createNamespace(namespace);

  // Internal state
  let buffer: Triple[] = [];
  let pendingWrites = 0;
  let triplesWritten = 0;
  let chunksUploaded = 0;
  let bytesUploaded = 0;
  const chunkInfos: ChunkInfo[] = [];

  // Streaming bloom filter - no unbounded Set!
  let bloomFilter = createBloomFilter({
    capacity: bloomCapacity,
    targetFpr: bloomFpr,
    maxSizeBytes: 64 * 1024, // 64KB max
  });

  // Generate chunk path from namespace
  function getChunkPath(chunkId: string): string {
    const url = new URL(namespace);
    const domainParts = url.hostname.split('.');
    const reversedDomain = domainParts.reverse().map((part) => `.${part}`).join('/');
    const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
    const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';

    return `${reversedDomain}${pathStr}/_chunks/${chunkId}.gcol`;
  }

  // Generate unique chunk ID
  function generateChunkId(): string {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 8);
    return `${timestamp.toString(36)}-${random}`;
  }

  // Add entity IDs to bloom filter (streaming, no Set)
  function addToBloom(triples: Triple[]): void {
    for (const triple of triples) {
      addToFilter(bloomFilter, triple.subject);
      if (triple.object.type === ObjectType.REF) {
        addToFilter(bloomFilter, triple.object.value);
      } else if (triple.object.type === ObjectType.REF_ARRAY) {
        for (const ref of triple.object.value) {
          addToFilter(bloomFilter, ref);
        }
      }
    }
  }

  // Flush buffer to R2
  async function doFlush(): Promise<string | null> {
    if (buffer.length === 0) {
      return null;
    }

    const triplesToEncode = buffer;
    buffer = [];

    const encoded = encodeGraphCol(triplesToEncode, ns);
    const chunkId = generateChunkId();
    const path = getChunkPath(chunkId);

    // Calculate time range
    let minTime = triplesToEncode[0].timestamp;
    let maxTime = triplesToEncode[0].timestamp;
    for (const t of triplesToEncode) {
      if (t.timestamp < minTime) minTime = t.timestamp;
      if (t.timestamp > maxTime) maxTime = t.timestamp;
    }

    // Add to bloom filter (streaming)
    addToBloom(triplesToEncode);

    // Generate chunk bloom filter
    const chunkBloom = createChunkBloomSerialized(triplesToEncode, { targetFpr: bloomFpr });

    pendingWrites++;
    try {
      // Write to R2
      await r2.put(path, encoded);

      // Record chunk info
      const chunkInfo: ChunkInfo = {
        id: chunkId,
        tripleCount: triplesToEncode.length,
        minTime,
        maxTime,
        bytes: encoded.length,
        path,
        bloom: chunkBloom,
      };
      chunkInfos.push(chunkInfo);

      // Update stats
      chunksUploaded++;
      triplesWritten += triplesToEncode.length;
      bytesUploaded += encoded.length;

      return path;
    } finally {
      pendingWrites--;
    }
  }

  return {
    async addTriple(triple: Triple): Promise<void> {
      buffer.push(triple);
      if (buffer.length >= batchSize) {
        await doFlush();
      }
    },

    async addTriples(triples: Triple[]): Promise<void> {
      for (const triple of triples) {
        buffer.push(triple);
        if (buffer.length >= batchSize) {
          await doFlush();
        }
      }
    },

    async flush(): Promise<string | null> {
      return doFlush();
    },

    isBackpressured(): boolean {
      return pendingWrites >= maxPendingBatches;
    },

    async finalize(): Promise<WriterResult> {
      // Flush any remaining triples
      await doFlush();

      // Wait for pending writes
      while (pendingWrites > 0) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }

      return {
        totalTriples: triplesWritten,
        totalChunks: chunksUploaded,
        totalBytes: bytesUploaded,
        chunks: chunkInfos,
        combinedBloom: serializeFilter(bloomFilter),
      };
    },

    getState(): BatchWriterState {
      return {
        triplesWritten,
        chunksUploaded,
        bytesUploaded,
        chunkInfos: [...chunkInfos],
        bloomState: serializeFilter(bloomFilter),
      };
    },

    restoreState(state: BatchWriterState): void {
      triplesWritten = state.triplesWritten;
      chunksUploaded = state.chunksUploaded;
      bytesUploaded = state.bytesUploaded;
      chunkInfos.length = 0;
      chunkInfos.push(...state.chunkInfos);
      bloomFilter = deserializeFilter(state.bloomState);
    },
  };
}

// ============================================================================
// ResumableImportState
// ============================================================================

/**
 * ResumableImportState interface for checkpoint management
 */
export interface ResumableImportState {
  /** Load a checkpoint for a job */
  loadCheckpoint(jobId: string): Promise<ImportCheckpoint | null>;
  /** Save a checkpoint */
  saveCheckpoint(checkpoint: ImportCheckpoint): Promise<void>;
  /** Update an existing checkpoint with partial data */
  updateCheckpoint(jobId: string, updates: Partial<ImportCheckpoint>): Promise<void>;
  /** Delete a checkpoint (after successful completion) */
  deleteCheckpoint(jobId: string): Promise<void>;
  /** List all active checkpoints */
  listCheckpoints(): Promise<string[]>;
}

/**
 * Create a resumable import state manager using Durable Object storage
 *
 * Key features:
 * - Persists checkpoints to DO storage for durability
 * - Enables resume after timeout/restart
 * - Minimal overhead (~1KB per checkpoint)
 *
 * @param storage Durable Object storage
 * @returns ResumableImportState instance
 *
 * @example
 * ```typescript
 * const importState = createResumableImportState(this.state.storage);
 *
 * // Check for existing checkpoint
 * const checkpoint = await importState.loadCheckpoint('wiktionary-load');
 * if (checkpoint) {
 *   // Resume from checkpoint
 *   lineReader.restoreState(checkpoint.lineReaderState);
 *   writer.restoreState(checkpoint.batchWriterState);
 *   startOffset = checkpoint.byteOffset;
 * }
 *
 * // Save checkpoint periodically
 * await importState.saveCheckpoint({
 *   jobId: 'wiktionary-load',
 *   byteOffset: currentOffset,
 *   // ... other state
 * });
 * ```
 */
export function createResumableImportState(
  storage: DurableObjectStorage
): ResumableImportState {
  const CHECKPOINT_PREFIX = 'checkpoint:';

  return {
    async loadCheckpoint(jobId: string): Promise<ImportCheckpoint | null> {
      const key = `${CHECKPOINT_PREFIX}${jobId}`;
      const checkpoint = await storage.get<ImportCheckpoint>(key);
      return checkpoint ?? null;
    },

    async saveCheckpoint(checkpoint: ImportCheckpoint): Promise<void> {
      const key = `${CHECKPOINT_PREFIX}${checkpoint.jobId}`;
      checkpoint.checkpointedAt = new Date().toISOString();
      await storage.put(key, checkpoint);
    },

    async updateCheckpoint(jobId: string, updates: Partial<ImportCheckpoint>): Promise<void> {
      const key = `${CHECKPOINT_PREFIX}${jobId}`;
      const existing = await storage.get<ImportCheckpoint>(key);
      if (existing) {
        const updated = { ...existing, ...updates, checkpointedAt: new Date().toISOString() };
        await storage.put(key, updated);
      }
    },

    async deleteCheckpoint(jobId: string): Promise<void> {
      const key = `${CHECKPOINT_PREFIX}${jobId}`;
      await storage.delete(key);
    },

    async listCheckpoints(): Promise<string[]> {
      const entries = await storage.list({ prefix: CHECKPOINT_PREFIX });
      return Array.from(entries.keys()).map((key) => key.replace(CHECKPOINT_PREFIX, ''));
    },
  };
}

// ============================================================================
// RangeFetcher
// ============================================================================

/**
 * RangeFetcher interface for chunked HTTP downloads
 */
export interface RangeFetcher {
  /** Fetch a specific byte range */
  fetchRange(start: number, end?: number): Promise<RangeFetchResult>;
  /** Async generator that yields chunks from a starting offset */
  chunks(startOffset?: number): AsyncGenerator<RangeFetchResult>;
  /** Get total size of the resource (may require a HEAD request) */
  getTotalSize(): Promise<number | null>;
}

/**
 * Create a range fetcher for chunked HTTP downloads
 *
 * Key features:
 * - Uses HTTP Range requests for incremental downloading
 * - Configurable chunk size (default 10MB, not 50MB for safety)
 * - Exponential backoff retry on failures
 * - Yields chunks as async generator
 *
 * @param url Source URL
 * @param options Configuration options
 * @returns RangeFetcher instance
 *
 * @example
 * ```typescript
 * const fetcher = createRangeFetcher(WIKTIONARY_URL, { chunkSize: 10 * 1024 * 1024 });
 *
 * // Resume from checkpoint
 * const checkpoint = await importState.loadCheckpoint('wiktionary');
 * const startOffset = checkpoint?.byteOffset ?? 0;
 *
 * for await (const { data, start, end, isLast } of fetcher.chunks(startOffset)) {
 *   for await (const line of lineReader.processChunk(data)) {
 *     // process line...
 *   }
 *
 *   // Save checkpoint after each chunk
 *   await importState.updateCheckpoint('wiktionary', { byteOffset: end });
 * }
 * ```
 */
export function createRangeFetcher(
  url: string,
  options?: {
    /** Chunk size in bytes (default 10MB) */
    chunkSize?: number;
    /** Maximum retry attempts (default 3) */
    maxRetries?: number;
    /** Base delay for exponential backoff in ms (default 1000) */
    baseDelayMs?: number;
  }
): RangeFetcher {
  const chunkSize = options?.chunkSize ?? 10 * 1024 * 1024; // 10MB default
  const maxRetries = options?.maxRetries ?? 3;
  const baseDelayMs = options?.baseDelayMs ?? 1000;

  let cachedTotalSize: number | null = null;

  async function fetchWithRetry(
    start: number,
    end?: number
  ): Promise<{ response: Response; actualEnd: number; totalSize?: number }> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const rangeHeader = end !== undefined ? `bytes=${start}-${end}` : `bytes=${start}-`;

        const response = await fetch(url, {
          headers: { Range: rangeHeader },
        });

        if (response.status === 206) {
          // Partial content - parse Content-Range header
          const contentRange = response.headers.get('Content-Range');
          let totalSize: number | undefined;
          let actualEnd = end ?? start + chunkSize - 1;

          if (contentRange) {
            // Format: bytes start-end/total or bytes start-end/*
            const match = contentRange.match(/bytes (\d+)-(\d+)\/(\d+|\*)/);
            if (match) {
              actualEnd = parseInt(match[2], 10);
              if (match[3] !== '*') {
                totalSize = parseInt(match[3], 10);
                cachedTotalSize = totalSize;
              }
            }
          }

          return { response, actualEnd, totalSize };
        } else if (response.status === 200) {
          // Server doesn't support range requests - return full response
          const contentLength = response.headers.get('Content-Length');
          const totalSize = contentLength ? parseInt(contentLength, 10) : undefined;
          if (totalSize) cachedTotalSize = totalSize;

          return {
            response,
            actualEnd: totalSize ? totalSize - 1 : start + chunkSize - 1,
            totalSize,
          };
        } else if (response.status === 416) {
          // Range not satisfiable - we're past the end
          return {
            response: new Response(null, { status: 200 }),
            actualEnd: start,
            totalSize: cachedTotalSize ?? undefined,
          };
        } else {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < maxRetries - 1) {
          const delay = baseDelayMs * Math.pow(2, attempt);
          console.warn(`[RangeFetcher] Retry ${attempt + 1}/${maxRetries} after ${delay}ms: ${lastError.message}`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError ?? new Error('Failed to fetch range');
  }

  return {
    async fetchRange(start: number, end?: number): Promise<RangeFetchResult> {
      const requestEnd = end ?? start + chunkSize - 1;
      const { response, actualEnd, totalSize } = await fetchWithRetry(start, requestEnd);

      const data = new Uint8Array(await response.arrayBuffer());

      return {
        data,
        start,
        end: actualEnd + 1, // Make end exclusive
        totalSize,
        isLast: totalSize !== undefined && actualEnd >= totalSize - 1,
      };
    },

    async *chunks(startOffset: number = 0): AsyncGenerator<RangeFetchResult> {
      let currentOffset = startOffset;

      while (true) {
        const requestEnd = currentOffset + chunkSize - 1;
        const { response, actualEnd, totalSize } = await fetchWithRetry(currentOffset, requestEnd);

        const data = new Uint8Array(await response.arrayBuffer());

        if (data.length === 0) {
          // No more data
          break;
        }

        const isLast = totalSize !== undefined && actualEnd >= totalSize - 1;

        yield {
          data,
          start: currentOffset,
          end: actualEnd + 1,
          totalSize,
          isLast,
        };

        if (isLast) {
          break;
        }

        currentOffset = actualEnd + 1;
      }
    },

    async getTotalSize(): Promise<number | null> {
      if (cachedTotalSize !== null) {
        return cachedTotalSize;
      }

      try {
        const response = await fetch(url, { method: 'HEAD' });
        const contentLength = response.headers.get('Content-Length');
        if (contentLength) {
          cachedTotalSize = parseInt(contentLength, 10);
          return cachedTotalSize;
        }
      } catch {
        // HEAD request failed, try range request
        try {
          const { totalSize } = await fetchWithRetry(0, 0);
          if (totalSize) {
            cachedTotalSize = totalSize;
            return cachedTotalSize;
          }
        } catch {
          // Ignore
        }
      }

      return null;
    },
  };
}

// ============================================================================
// Streaming Import Orchestrator
// ============================================================================

/**
 * Create a streaming import pipeline
 *
 * Orchestrates:
 * - RangeFetcher (if useRangeRequests) or streaming fetch
 * - Optional gzip decompression
 * - StreamingLineReader for line parsing
 * - Record transformation
 * - BatchedTripleWriter for chunked output
 * - ResumableImportState for checkpointing
 *
 * @param r2 R2 bucket for storage
 * @param storage Durable Object storage for checkpoints
 * @param config Import configuration
 * @returns Promise resolving to import result
 *
 * @example
 * ```typescript
 * const result = await createStreamingImport(env.DATASETS, this.state.storage, {
 *   url: 'https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl',
 *   format: 'ndjson',
 *   gzipped: false,
 *   namespace: 'https://wiktionary.org/',
 *   useRangeRequests: true,
 *   transform: (record, txId) => generateWordTriples(record, txId),
 * });
 * ```
 */
export async function createStreamingImport(
  r2: R2Bucket,
  storage: DurableObjectStorage,
  config: StreamingImportConfig
): Promise<StreamingImportResult> {
  const jobId = `import-${config.namespace.replace(/[^a-z0-9]/gi, '-')}`;

  // Initialize components
  const importState = createResumableImportState(storage);
  const lineReader = createStreamingLineReader();
  const writer = createBatchedTripleWriter(r2, config.namespace);

  // Check for existing checkpoint
  let checkpoint = await importState.loadCheckpoint(jobId);
  let startOffset = 0;

  if (checkpoint) {
    console.log(`[StreamingImport] Resuming from checkpoint at byte ${checkpoint.byteOffset}`);
    lineReader.restoreState(checkpoint.lineReaderState);
    writer.restoreState(checkpoint.batchWriterState);
    startOffset = checkpoint.byteOffset;
  }

  const txId = generateTxId();
  let linesProcessed = checkpoint?.linesProcessed ?? 0;
  let triplesWritten = checkpoint?.triplesWritten ?? 0;

  try {
    if (config.useRangeRequests) {
      // Use range fetcher for chunked downloading
      const fetcher = createRangeFetcher(config.url);
      const totalSize = await fetcher.getTotalSize();

      for await (const { data, end, isLast } of fetcher.chunks(startOffset)) {
        // Decompress if needed
        let processData = data;
        if (config.gzipped) {
          const decompressed = new DecompressionStream('gzip');
          const writer = decompressed.writable.getWriter();
          const reader = decompressed.readable.getReader();

          writer.write(data);
          writer.close();

          const chunks: Uint8Array[] = [];
          let result = await reader.read();
          while (!result.done) {
            chunks.push(result.value);
            result = await reader.read();
          }

          // Concatenate chunks
          const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
          processData = new Uint8Array(totalLength);
          let offset = 0;
          for (const chunk of chunks) {
            processData.set(chunk, offset);
            offset += chunk.length;
          }
        }

        // Process lines
        for await (const line of lineReader.processChunk(processData)) {
          linesProcessed++;

          // Parse based on format
          let record: Record<string, unknown>;
          try {
            if (config.format === 'ndjson') {
              record = JSON.parse(line);
            } else {
              // TSV - split by tab
              const fields = line.split('\t');
              record = {};
              fields.forEach((field, i) => {
                record[`col${i}`] = field;
              });
            }
          } catch {
            // Skip malformed lines
            continue;
          }

          // Transform to triples
          const triples = config.transform(record, txId);
          await writer.addTriples(triples);
          triplesWritten += triples.length;

          // Respect backpressure
          while (writer.isBackpressured()) {
            await new Promise((resolve) => setTimeout(resolve, 50));
          }
        }

        // Save checkpoint after each chunk
        await importState.saveCheckpoint({
          jobId,
          sourceUrl: config.url,
          byteOffset: end,
          totalBytes: totalSize ?? undefined,
          linesProcessed,
          triplesWritten,
          lineReaderState: lineReader.getState(),
          batchWriterState: writer.getState(),
          checkpointedAt: new Date().toISOString(),
        });

        console.log(
          `[StreamingImport] Checkpoint: ${end} bytes, ${linesProcessed} lines, ${triplesWritten} triples`
        );

        if (isLast) break;
      }
    } else {
      // Use streaming fetch
      const response = await fetch(config.url);
      if (!response.ok || !response.body) {
        throw new Error(`Failed to fetch ${config.url}: ${response.status}`);
      }

      let stream: ReadableStream<Uint8Array> = response.body;

      // Decompress if needed
      if (config.gzipped) {
        stream = stream.pipeThrough(new DecompressionStream('gzip'));
      }

      const reader = stream.getReader();
      let bytesRead = 0;

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          bytesRead += value.byteLength;

          // Process lines
          for await (const line of lineReader.processChunk(value)) {
            linesProcessed++;

            // Parse based on format
            let record: Record<string, unknown>;
            try {
              if (config.format === 'ndjson') {
                record = JSON.parse(line);
              } else {
                const fields = line.split('\t');
                record = {};
                fields.forEach((field, i) => {
                  record[`col${i}`] = field;
                });
              }
            } catch {
              continue;
            }

            // Transform to triples
            const triples = config.transform(record, txId);
            await writer.addTriples(triples);
            triplesWritten += triples.length;

            // Respect backpressure
            while (writer.isBackpressured()) {
              await new Promise((resolve) => setTimeout(resolve, 50));
            }
          }

          // Periodic checkpoint (every ~50K lines)
          if (linesProcessed % 50000 < 100) {
            await importState.saveCheckpoint({
              jobId,
              sourceUrl: config.url,
              byteOffset: bytesRead,
              linesProcessed,
              triplesWritten,
              lineReaderState: lineReader.getState(),
              batchWriterState: writer.getState(),
              checkpointedAt: new Date().toISOString(),
            });
          }
        }
      } finally {
        reader.releaseLock();
      }
    }

    // Flush any remaining partial line
    const remaining = lineReader.flush();
    if (remaining) {
      linesProcessed++;
      try {
        let record: Record<string, unknown>;
        if (config.format === 'ndjson') {
          record = JSON.parse(remaining);
        } else {
          const fields = remaining.split('\t');
          record = {};
          fields.forEach((field, i) => {
            record[`col${i}`] = field;
          });
        }
        const triples = config.transform(record, txId);
        await writer.addTriples(triples);
        triplesWritten += triples.length;
      } catch {
        // Skip malformed final line
      }
    }

    // Finalize
    const result = await writer.finalize();

    // Generate manifest
    const manifest = generateManifest(config.namespace, result.chunks, {
      combinedBloom: result.combinedBloom,
    });

    // Write manifest to R2
    const url = new URL(config.namespace);
    const domainParts = url.hostname.split('.');
    const reversedDomain = domainParts.reverse().map((part) => `.${part}`).join('/');
    const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
    const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';
    const manifestPath = `${reversedDomain}${pathStr}/_manifest.json`;

    await r2.put(manifestPath, serializeManifest(manifest));

    // Delete checkpoint on success
    await importState.deleteCheckpoint(jobId);

    console.log(
      `[StreamingImport] Complete: ${linesProcessed} lines, ${triplesWritten} triples, ${result.totalChunks} chunks`
    );

    return {
      success: true,
      triplesImported: triplesWritten,
      chunksCreated: result.totalChunks,
      bytesWritten: result.totalBytes,
      manifest,
    };
  } catch (error) {
    // Save final checkpoint on error
    await importState.saveCheckpoint({
      jobId,
      sourceUrl: config.url,
      byteOffset: startOffset,
      linesProcessed,
      triplesWritten,
      lineReaderState: lineReader.getState(),
      batchWriterState: writer.getState(),
      checkpointedAt: new Date().toISOString(),
      metadata: {
        error: error instanceof Error ? error.message : String(error),
      },
    });

    return {
      success: false,
      triplesImported: triplesWritten,
      chunksCreated: writer.getState().chunksUploaded,
      bytesWritten: writer.getState().bytesUploaded,
      manifest: generateManifest(config.namespace, writer.getState().chunkInfos),
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

// Helper: deserialize bloom filter from state
function deserializeFilter(state: SerializedFilter): BloomFilter {
  return bloomDeserializeFilter(state);
}
