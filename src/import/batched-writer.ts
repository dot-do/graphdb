/**
 * BatchedTripleWriter - Batch triples for memory-efficient storage
 *
 * Key features:
 * - Batches triples before flushing (default 10K per batch)
 * - Uses streaming bloom filter instead of unbounded Set
 * - Implements backpressure to prevent memory buildup
 * - Can be checkpointed and resumed
 *
 * @packageDocumentation
 */

import type { Triple } from '../core/triple';
import { ObjectType, createNamespace } from '../core/types';
import { encodeGraphCol } from '../storage/graphcol';
import {
  createBloomFilter,
  addToFilter,
  serializeFilter,
  deserializeFilter,
  type SerializedFilter,
} from '../snippet/bloom';

// ============================================================================
// Types
// ============================================================================

/**
 * Information about a written chunk from the import process
 */
export interface ImportChunkInfo {
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
  chunkInfos: ImportChunkInfo[];
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
  chunks: ImportChunkInfo[];
  /** Combined bloom filter */
  combinedBloom: SerializedFilter;
}

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
 * Options for creating a BatchedTripleWriter
 */
export interface BatchedTripleWriterOptions {
  /** Number of triples per batch (default 10000) */
  batchSize?: number;
  /** Maximum pending batches before backpressure (default 2) */
  maxPendingBatches?: number;
  /** Bloom filter capacity (default 1,000,000) */
  bloomCapacity?: number;
  /** Target false positive rate for bloom filter (default 0.01) */
  bloomFpr?: number;
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Create a bloom filter for a chunk's entities
 */
function createChunkBloomSerialized(
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

  for (const id of entityIds) {
    addToFilter(filter, id);
  }

  return serializeFilter(filter);
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
  options?: BatchedTripleWriterOptions
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
  const chunkInfos: ImportChunkInfo[] = [];

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
    let minTime = triplesToEncode[0]!.timestamp;
    let maxTime = triplesToEncode[0]!.timestamp;
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
      const chunkInfo: ImportChunkInfo = {
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
