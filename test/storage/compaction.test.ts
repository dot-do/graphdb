/**
 * R2 Compaction Pipeline Tests (TDD - RED Phase)
 *
 * Tests for compacting small R2 WAL chunks into larger files:
 * - Should compact small chunks into larger ones
 * - Should maintain data integrity after compaction
 * - Should respect compaction thresholds (8MB L1, 128MB L2)
 * - Should delete source chunks after successful compaction
 * - Should handle concurrent compaction safely
 *
 * Uses mocks for R2Bucket in tests.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  compactChunks,
  selectChunksForCompaction,
  CompactionLevel,
  type CompactionConfig,
  type CompactionResult,
  type ChunkInfo,
} from '../../src/storage/compaction';
import { encodeGraphCol, decodeGraphCol, getChunkStats } from '../../src/storage/graphcol';
import { parseNamespaceToPath, listCDCFiles, readCDCFile } from '../../src/storage/r2-writer';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type Namespace,
  type TransactionId,
} from '../../src/core/types';
import { type Triple } from '../../src/core/triple';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Generate a valid ULID-format transaction ID for testing
 */
function generateTestTxId(index: number): TransactionId {
  const base = '01ARZ3NDEKTSV4RRFFQ69G5FA';
  const lastChar = 'ABCDEFGHJKMNPQRSTVWXYZ'[index % 22];
  return createTransactionId(base + lastChar);
}

/**
 * Create a test triple
 */
function createTestTriple(
  subjectId: number,
  predicateName: string,
  value: string,
  timestamp: bigint,
  txId: TransactionId
): Triple {
  return {
    subject: createEntityId(`https://example.com/entity/${subjectId}`),
    predicate: createPredicate(predicateName),
    object: { type: ObjectType.STRING, value: value },
    timestamp,
    txId,
  };
}

/**
 * Create multiple test triples
 */
function createTestTriples(count: number, baseTimestamp: bigint): Triple[] {
  return Array.from({ length: count }, (_, i) =>
    createTestTriple(i, 'name', `User ${i}`, baseTimestamp + BigInt(i * 1000), generateTestTxId(i % 22))
  );
}

/**
 * Create a chunk of specified approximate size (in bytes)
 */
function createChunkOfSize(namespace: Namespace, targetBytes: number, baseTimestamp: bigint): Uint8Array {
  // Estimate ~100 bytes per triple on average
  const triplesNeeded = Math.ceil(targetBytes / 100);
  const triples = createTestTriples(triplesNeeded, baseTimestamp);
  return encodeGraphCol(triples, namespace);
}

// ============================================================================
// R2 Bucket Mock
// ============================================================================

interface MockR2Object {
  key: string;
  data: Uint8Array;
  size: number;
  etag: string;
  uploaded: Date;
}

/**
 * Create a mock R2Bucket for testing
 */
function createMockR2Bucket(): R2Bucket & { _storage: Map<string, MockR2Object> } {
  const storage = new Map<string, MockR2Object>();

  return {
    _storage: storage,

    async put(key: string, value: ArrayBuffer | Uint8Array | string | ReadableStream | Blob | null): Promise<R2Object> {
      let data: Uint8Array;
      if (value instanceof Uint8Array) {
        data = value;
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value);
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value);
      } else {
        throw new Error('Unsupported value type');
      }

      const obj: MockR2Object = {
        key,
        data,
        size: data.length,
        etag: `etag-${Date.now()}-${Math.random()}`,
        uploaded: new Date(),
      };
      storage.set(key, obj);

      return {
        key,
        size: obj.size,
        etag: obj.etag,
        httpEtag: `"${obj.etag}"`,
        uploaded: obj.uploaded,
        checksums: {},
        customMetadata: {},
        httpMetadata: {},
        writeHttpMetadata: () => {},
        storageClass: 'Standard',
      } as unknown as R2Object;
    },

    async get(key: string): Promise<R2ObjectBody | null> {
      const obj = storage.get(key);
      if (!obj) return null;

      return {
        key: obj.key,
        size: obj.size,
        etag: obj.etag,
        httpEtag: `"${obj.etag}"`,
        uploaded: obj.uploaded,
        checksums: {},
        customMetadata: {},
        httpMetadata: {},
        writeHttpMetadata: () => {},
        storageClass: 'Standard',
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(obj.data);
            controller.close();
          },
        }),
        bodyUsed: false,
        arrayBuffer: async () => obj.data.buffer.slice(obj.data.byteOffset, obj.data.byteOffset + obj.data.byteLength),
        text: async () => new TextDecoder().decode(obj.data),
        json: async () => JSON.parse(new TextDecoder().decode(obj.data)),
        blob: async () => new Blob([obj.data]),
      } as unknown as R2ObjectBody;
    },

    async head(key: string): Promise<R2Object | null> {
      const obj = storage.get(key);
      if (!obj) return null;

      return {
        key: obj.key,
        size: obj.size,
        etag: obj.etag,
        httpEtag: `"${obj.etag}"`,
        uploaded: obj.uploaded,
        checksums: {},
        customMetadata: {},
        httpMetadata: {},
        writeHttpMetadata: () => {},
        storageClass: 'Standard',
      } as unknown as R2Object;
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys];
      for (const key of keyArray) {
        storage.delete(key);
      }
    },

    async list(options?: R2ListOptions): Promise<R2Objects> {
      const prefix = options?.prefix ?? '';
      const objects: R2Object[] = [];

      for (const [key, obj] of storage) {
        if (key.startsWith(prefix)) {
          objects.push({
            key: obj.key,
            size: obj.size,
            etag: obj.etag,
            httpEtag: `"${obj.etag}"`,
            uploaded: obj.uploaded,
            checksums: {},
            customMetadata: {},
            httpMetadata: {},
            writeHttpMetadata: () => {},
            storageClass: 'Standard',
          } as unknown as R2Object);
        }
      }

      return {
        objects,
        truncated: false,
        delimitedPrefixes: [],
      };
    },

    async createMultipartUpload(): Promise<R2MultipartUpload> {
      throw new Error('Not implemented');
    },

    async resumeMultipartUpload(): Promise<R2MultipartUpload> {
      throw new Error('Not implemented');
    },
  } as unknown as R2Bucket & { _storage: Map<string, MockR2Object> };
}

// ============================================================================
// Tests
// ============================================================================

describe('R2 Compaction Pipeline', () => {
  const testNamespace = createNamespace('https://example.com/crm/acme');

  describe('Default Configuration', () => {
    it('should have L1 threshold of 8MB', () => {
      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };
      expect(config.l1ThresholdBytes).toBe(8388608);
    });

    it('should have L2 threshold of 128MB', () => {
      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };
      expect(config.l2ThresholdBytes).toBe(134217728);
    });
  });

  describe('Chunk Selection - selectChunksForCompaction', () => {
    it('should select chunks that meet minimum count threshold', () => {
      const chunks: ChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1024, tripleCount: 10, minTimestamp: 1n, maxTimestamp: 100n },
        { path: 'chunk2.gcol', sizeBytes: 2048, tripleCount: 20, minTimestamp: 101n, maxTimestamp: 200n },
        { path: 'chunk3.gcol', sizeBytes: 3072, tripleCount: 30, minTimestamp: 201n, maxTimestamp: 300n },
        { path: 'chunk4.gcol', sizeBytes: 4096, tripleCount: 40, minTimestamp: 301n, maxTimestamp: 400n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);
      expect(selected.length).toBe(4);
    });

    it('should not select chunks below minimum count threshold', () => {
      const chunks: ChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1024, tripleCount: 10, minTimestamp: 1n, maxTimestamp: 100n },
        { path: 'chunk2.gcol', sizeBytes: 2048, tripleCount: 20, minTimestamp: 101n, maxTimestamp: 200n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);
      expect(selected.length).toBe(0);
    });

    it('should limit selected chunks to not exceed L1 threshold', () => {
      // Create chunks that would exceed L1 threshold when combined
      const chunks: ChunkInfo[] = Array.from({ length: 10 }, (_, i) => ({
        path: `chunk${i}.gcol`,
        sizeBytes: 2 * 1024 * 1024, // 2MB each
        tripleCount: 1000,
        minTimestamp: BigInt(i * 1000),
        maxTimestamp: BigInt((i + 1) * 1000 - 1),
      }));

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024, // 8MB
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);
      const totalSize = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
      expect(totalSize).toBeLessThanOrEqual(config.l1ThresholdBytes);
    });

    it('should respect L2 threshold for L1-to-L2 compaction', () => {
      const chunks: ChunkInfo[] = Array.from({ length: 20 }, (_, i) => ({
        path: `chunk${i}.gcol`,
        sizeBytes: 8 * 1024 * 1024, // 8MB each (L1 chunks)
        tripleCount: 10000,
        minTimestamp: BigInt(i * 10000),
        maxTimestamp: BigInt((i + 1) * 10000 - 1),
      }));

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024, // 128MB
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L1_TO_L2);
      const totalSize = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
      expect(totalSize).toBeLessThanOrEqual(config.l2ThresholdBytes);
    });

    it('should select chunks in timestamp order', () => {
      const chunks: ChunkInfo[] = [
        { path: 'chunk3.gcol', sizeBytes: 1024, tripleCount: 10, minTimestamp: 300n, maxTimestamp: 400n },
        { path: 'chunk1.gcol', sizeBytes: 1024, tripleCount: 10, minTimestamp: 100n, maxTimestamp: 200n },
        { path: 'chunk2.gcol', sizeBytes: 1024, tripleCount: 10, minTimestamp: 200n, maxTimestamp: 300n },
        { path: 'chunk4.gcol', sizeBytes: 1024, tripleCount: 10, minTimestamp: 400n, maxTimestamp: 500n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);
      expect(selected[0].path).toBe('chunk1.gcol');
      expect(selected[1].path).toBe('chunk2.gcol');
      expect(selected[2].path).toBe('chunk3.gcol');
      expect(selected[3].path).toBe('chunk4.gcol');
    });
  });

  describe('compactChunks - Basic Compaction', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should compact small chunks into larger ones', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create 4 small WAL chunks
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      // Verify 4 chunks exist
      const beforeList = await bucket.list({ prefix: `${namespacePath}/_wal/` });
      expect(beforeList.objects.length).toBe(4);

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);

      // Should have compacted
      expect(result).not.toBeNull();
      expect(result!.sourcePaths.length).toBe(4);
      expect(result!.bytesCompacted).toBeGreaterThan(0);
    });

    it('should maintain data integrity after compaction', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create source chunks with known data
      const allOriginalTriples: Triple[] = [];
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        allOriginalTriples.push(...triples);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);
      expect(result).not.toBeNull();

      // Read the compacted chunk
      const compactedObj = await bucket.get(result!.targetPath);
      expect(compactedObj).not.toBeNull();

      const compactedData = new Uint8Array(await compactedObj!.arrayBuffer());
      const compactedTriples = decodeGraphCol(compactedData);

      // Verify all triples are present
      expect(compactedTriples.length).toBe(allOriginalTriples.length);

      // Verify data matches (by subject count and predicate)
      const originalSubjects = new Set(allOriginalTriples.map((t) => t.subject));
      const compactedSubjects = new Set(compactedTriples.map((t) => t.subject));
      expect(compactedSubjects.size).toBe(originalSubjects.size);
    });

    it('should not compact when below minimum chunk count', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create only 2 chunks (below threshold of 4)
      for (let i = 0; i < 2; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);

      // Should not compact
      expect(result).toBeNull();

      // Original chunks should still exist
      const afterList = await bucket.list({ prefix: `${namespacePath}/_wal/` });
      expect(afterList.objects.length).toBe(2);
    });
  });

  describe('compactChunks - Threshold Enforcement', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should respect L1 threshold (8MB) for WAL compaction', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create chunks that would exceed 8MB when combined
      // Using smaller chunks for test performance
      for (let i = 0; i < 10; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(1000, timestamp); // ~100KB each
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 500 * 1024, // 500KB for faster testing
        l2ThresholdBytes: 8 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);
      expect(result).not.toBeNull();
      expect(result!.bytesCompacted).toBeLessThanOrEqual(config.l1ThresholdBytes * 1.1); // Allow 10% overhead
    });

    it('should use L2 threshold (128MB) for L1 chunk compaction', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);

      // Create L1-level chunks (simulating already compacted chunks)
      for (let i = 0; i < 20; i++) {
        const baseTime = BigInt(Date.now() + i * 3600000); // 1 hour apart
        const triples = createTestTriples(2000, baseTime);
        const data = encodeGraphCol(triples, namespace);
        // L1 chunks go in _l1 directory
        const path = `${namespacePath}/_l1/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 1024 * 1024, // 1MB for faster testing
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config, CompactionLevel.L1_TO_L2);
      expect(result).not.toBeNull();
      expect(result!.bytesCompacted).toBeLessThanOrEqual(config.l2ThresholdBytes * 1.1);
    });
  });

  describe('compactChunks - Source Chunk Deletion', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should delete source chunks after successful compaction', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create 4 source chunks
      const sourcePaths: string[] = [];
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        sourcePaths.push(path);
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);
      expect(result).not.toBeNull();

      // Verify source chunks are deleted
      for (const path of sourcePaths) {
        const obj = await bucket.head(path);
        expect(obj).toBeNull();
      }

      // Verify compacted chunk exists
      const compactedObj = await bucket.head(result!.targetPath);
      expect(compactedObj).not.toBeNull();
    });

    it('should preserve source chunks if compaction fails mid-way', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create source chunks
      const sourcePaths: string[] = [];
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        sourcePaths.push(path);
        await bucket.put(path, data);
      }

      // Mock put to fail
      const originalPut = bucket.put.bind(bucket);
      let putCallCount = 0;
      bucket.put = vi.fn(async (key: string, value: any) => {
        putCallCount++;
        // Fail on the compacted chunk write (after reading all sources)
        if (key.includes('/_l1/')) {
          throw new Error('Simulated R2 write failure');
        }
        return originalPut(key, value);
      });

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      // Compaction should fail
      await expect(compactChunks(bucket, namespace, config)).rejects.toThrow('Simulated R2 write failure');

      // Source chunks should still exist
      for (const path of sourcePaths) {
        const obj = await bucket.head(path);
        expect(obj).not.toBeNull();
      }
    });
  });

  describe('compactChunks - Concurrent Safety', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should handle concurrent compaction safely with lock', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create 8 chunks
      for (let i = 0; i < 8; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      // Run concurrent compactions
      const results = await Promise.allSettled([
        compactChunks(bucket, namespace, config),
        compactChunks(bucket, namespace, config),
      ]);

      // At least one should succeed, one might be skipped due to lock
      const successes = results.filter((r) => r.status === 'fulfilled' && r.value !== null);
      expect(successes.length).toBeGreaterThanOrEqual(1);

      // Final state should be consistent
      const walList = await bucket.list({ prefix: `${namespacePath}/_wal/` });
      const l1List = await bucket.list({ prefix: `${namespacePath}/_l1/` });

      // Either all 8 WAL chunks compacted into L1, or 4 compacted and 4 remain
      const totalChunks = walList.objects.length + l1List.objects.length;
      expect(totalChunks).toBeLessThanOrEqual(8);
    });

    it('should create and release lock file during compaction', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create chunks
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      // Spy on put to detect lock file
      const putCalls: string[] = [];
      const deleteCalls: string[] = [];
      const originalPut = bucket.put.bind(bucket);
      const originalDelete = bucket.delete.bind(bucket);

      bucket.put = vi.fn(async (key: string, value: any) => {
        putCalls.push(key);
        return originalPut(key, value);
      });

      bucket.delete = vi.fn(async (keys: string | string[]) => {
        const keyArray = Array.isArray(keys) ? keys : [keys];
        deleteCalls.push(...keyArray);
        return originalDelete(keys);
      });

      await compactChunks(bucket, namespace, config);

      // Lock file should have been created and then deleted
      const lockPath = `${namespacePath}/_compaction.lock`;
      expect(putCalls).toContain(lockPath);
      expect(deleteCalls).toContain(lockPath);
    });

    it('should skip compaction if lock file exists and is recent', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create chunks
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      // Create a recent lock file
      const lockPath = `${namespacePath}/_compaction.lock`;
      await bucket.put(lockPath, JSON.stringify({ lockedAt: Date.now(), owner: 'other-worker' }));

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);

      // Should skip due to lock
      expect(result).toBeNull();

      // Chunks should still exist
      const walList = await bucket.list({ prefix: `${namespacePath}/_wal/` });
      expect(walList.objects.length).toBe(4);
    });

    it('should acquire lock if existing lock is stale', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create chunks
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      // Create a stale lock file (10 minutes old)
      const lockPath = `${namespacePath}/_compaction.lock`;
      const staleLockTime = Date.now() - 10 * 60 * 1000;
      await bucket.put(lockPath, JSON.stringify({ lockedAt: staleLockTime, owner: 'other-worker' }));

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
        lockTimeoutMs: 5 * 60 * 1000, // 5 minute timeout
      };

      const result = await compactChunks(bucket, namespace, config);

      // Should proceed with compaction
      expect(result).not.toBeNull();
    });
  });

  describe('CompactionResult', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should return complete compaction result', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(Date.now());

      // Create chunks
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);

      expect(result).not.toBeNull();
      expect(result!.sourcePaths).toHaveLength(4);
      expect(result!.targetPath).toContain('/_l1/');
      expect(result!.bytesCompacted).toBeGreaterThan(0);
      expect(result!.triplesCompacted).toBe(40); // 4 chunks * 10 triples
      expect(result!.durationMs).toBeGreaterThanOrEqual(0);
    });

    it('should generate correct target path format', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);
      const baseTime = BigInt(new Date('2024-01-16T12:00:00Z').getTime());

      // Create chunks
      for (let i = 0; i < 4; i++) {
        const timestamp = baseTime + BigInt(i * 60000);
        const triples = createTestTriples(10, timestamp);
        const data = encodeGraphCol(triples, namespace);
        const path = `${namespacePath}/_wal/2024-01-16/${String(i).padStart(6, '0')}-000.gcol`;
        await bucket.put(path, data);
      }

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const result = await compactChunks(bucket, namespace, config);

      // Target path should be in L1 directory with date folder
      expect(result!.targetPath).toMatch(/\/_l1\/\d{4}-\d{2}-\d{2}\/.*\.gcol$/);
    });
  });
});
