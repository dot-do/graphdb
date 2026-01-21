/**
 * R2 CDC Writer Tests (RED first, then GREEN)
 *
 * Tests for:
 * - getCDCPath generates correct URL hierarchy paths
 * - R2Writer batches events
 * - R2Writer flushes on interval
 * - listCDCFiles finds files in time range
 * - readCDCFile decodes GraphCol format
 *
 * Uses mocks for R2Bucket in tests.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createR2Writer,
  getCDCPath,
  listCDCFiles,
  readCDCFile,
  parseNamespaceToPath,
  formatDatePath,
  generateSequence,
  parseCDCPath,
  type R2Writer,
  type R2WriterConfig,
} from '../../src/storage/r2-writer';
import { encodeGraphCol, decodeGraphCol } from '../../src/storage/graphcol';
import { type CDCEvent } from '../../src/storage/cdc-types';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type Namespace,
  type TransactionId,
} from '../../src/core/types';
import { type Triple, type TypedObject } from '../../src/core/triple';

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
 * Create a test CDC event
 */
function createTestCDCEvent(
  subjectId: number,
  value: string,
  timestamp: bigint
): CDCEvent {
  return {
    type: 'insert',
    triple: createTestTriple(subjectId, 'name', value, timestamp, generateTestTxId(subjectId % 22)),
    timestamp,
  };
}

/**
 * Create multiple test CDC events
 */
function createTestCDCEvents(count: number, baseTimestamp: bigint): CDCEvent[] {
  return Array.from({ length: count }, (_, i) =>
    createTestCDCEvent(i, `User ${i}`, baseTimestamp + BigInt(i * 1000))
  );
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

describe('R2 CDC Writer', () => {
  const testNamespace = createNamespace('https://example.com/crm/acme');

  describe('Path Generation - getCDCPath', () => {
    it('should generate correct URL hierarchy path', () => {
      const timestamp = BigInt(new Date('2024-01-16T12:30:00Z').getTime());
      const path = getCDCPath(testNamespace, timestamp);

      // Should start with reversed domain hierarchy
      expect(path).toMatch(/^\.com\/\.example/);
      // Should contain the path segments
      expect(path).toContain('/crm/acme/');
      // Should have _wal directory
      expect(path).toContain('/_wal/');
      // Should have date directory
      expect(path).toContain('/2024-01-16/');
      // Should end with .gcol
      expect(path).toMatch(/\.gcol$/);
    });

    it('should use reversed domain hierarchy for TLD-first ordering', () => {
      const namespace = createNamespace('https://subdomain.example.com/path');
      const timestamp = BigInt(Date.now());
      const path = getCDCPath(namespace, timestamp);

      // .com comes first, then .example, then .subdomain
      expect(path).toMatch(/^\.com\/\.example\/\.subdomain/);
    });

    it('should handle namespace with no path', () => {
      const namespace = createNamespace('https://example.com');
      const timestamp = BigInt(Date.now());
      const path = getCDCPath(namespace, timestamp);

      expect(path).toMatch(/^\.com\/\.example\/_wal/);
    });

    it('should generate sortable sequence numbers', () => {
      // Early morning
      const morning = BigInt(new Date('2024-01-16T02:00:00Z').getTime());
      const morningPath = getCDCPath(testNamespace, morning);

      // Late afternoon
      const afternoon = BigInt(new Date('2024-01-16T16:00:00Z').getTime());
      const afternoonPath = getCDCPath(testNamespace, afternoon);

      // Paths should be sortable (later time = later in sort order)
      expect(afternoonPath > morningPath).toBe(true);
    });

    it('should generate different paths for different days', () => {
      const day1 = BigInt(new Date('2024-01-15T12:00:00Z').getTime());
      const day2 = BigInt(new Date('2024-01-16T12:00:00Z').getTime());

      const path1 = getCDCPath(testNamespace, day1);
      const path2 = getCDCPath(testNamespace, day2);

      expect(path1).toContain('2024-01-15');
      expect(path2).toContain('2024-01-16');
      expect(path1).not.toBe(path2);
    });
  });

  describe('Path Utilities', () => {
    it('parseNamespaceToPath should reverse domain correctly', () => {
      const namespace = createNamespace('https://example.com/foo/bar');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example/foo/bar');
    });

    it('parseNamespaceToPath should handle subdomains', () => {
      const namespace = createNamespace('https://api.staging.example.com/v1');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example/.staging/.api/v1');
    });

    it('formatDatePath should format dates correctly', () => {
      const timestamp = BigInt(new Date('2024-01-16T12:00:00Z').getTime());
      const dateStr = formatDatePath(timestamp);

      expect(dateStr).toBe('2024-01-16');
    });

    it('formatDatePath should pad month and day', () => {
      const timestamp = BigInt(new Date('2024-03-05T12:00:00Z').getTime());
      const dateStr = formatDatePath(timestamp);

      expect(dateStr).toBe('2024-03-05');
    });

    it('generateSequence should generate HHMMSS-mmm format', () => {
      const timestamp = BigInt(new Date('2024-01-16T08:30:45.123Z').getTime());
      const seq = generateSequence(timestamp);

      expect(seq).toBe('083045-123');
    });

    it('generateSequence should increase through the day', () => {
      const morning = generateSequence(BigInt(new Date('2024-01-16T00:00:00Z').getTime()));
      const midday = generateSequence(BigInt(new Date('2024-01-16T12:00:00Z').getTime()));
      const evening = generateSequence(BigInt(new Date('2024-01-16T20:00:00Z').getTime()));

      // String comparison works for HHMMSS format (lexicographically sortable)
      expect(morning < midday).toBe(true);
      expect(midday < evening).toBe(true);
    });

    it('parseCDCPath should extract date and sequence', () => {
      // New format with HHMMSS-mmm
      const path = '.com/.example/foo/_wal/2024-01-16/123045-123.gcol';
      const parsed = parseCDCPath(path);

      expect(parsed).toEqual({ date: '2024-01-16', sequence: '123045-123' });
    });

    it('parseCDCPath should handle old 3-digit sequence format', () => {
      const path = '.com/.example/foo/_wal/2024-01-16/042.gcol';
      const parsed = parseCDCPath(path);

      expect(parsed).toEqual({ date: '2024-01-16', sequence: '042' });
    });

    it('parseCDCPath should return null for invalid paths', () => {
      expect(parseCDCPath('invalid/path')).toBeNull();
      expect(parseCDCPath('/foo/bar.txt')).toBeNull();
    });
  });

  describe('R2Writer - Batching', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.restoreAllMocks();
      vi.useRealTimers();
    });

    it('should batch events before writing', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0, // Disable interval flushing
        maxBatchSize: 10,
      });

      const events = createTestCDCEvents(5, BigInt(Date.now()));
      await writer.write(events);

      // Should not have written yet (batch not full)
      expect(bucket._storage.size).toBe(0);

      // Clean up
      writer.close();
    });

    it('should auto-flush when batch size reached', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0, // Disable interval flushing
        maxBatchSize: 10,
      });

      const events = createTestCDCEvents(15, BigInt(Date.now()));
      await writer.write(events);

      // Should have flushed (batch size exceeded)
      expect(bucket._storage.size).toBe(1);

      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(15);
      expect(stats.flushCount).toBe(1);

      writer.close();
    });

    it('should flush on explicit flush call', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxBatchSize: 1000,
      });

      const events = createTestCDCEvents(5, BigInt(Date.now()));
      await writer.write(events);

      expect(bucket._storage.size).toBe(0);

      await writer.flush();

      expect(bucket._storage.size).toBe(1);

      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(5);
      expect(stats.flushCount).toBe(1);

      writer.close();
    });

    it('should accumulate events across multiple writes', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxBatchSize: 1000,
      });

      const baseTime = BigInt(Date.now());

      await writer.write(createTestCDCEvents(3, baseTime));
      await writer.write(createTestCDCEvents(4, baseTime + BigInt(10000)));
      await writer.write(createTestCDCEvents(5, baseTime + BigInt(20000)));

      await writer.flush();

      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(12); // 3 + 4 + 5

      writer.close();
    });

    it('should handle empty event arrays', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxBatchSize: 1000,
      });

      await writer.write([]);
      await writer.flush();

      expect(bucket._storage.size).toBe(0);

      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(0);
      expect(stats.flushCount).toBe(0);

      writer.close();
    });
  });

  describe('R2Writer - Interval Flushing', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.restoreAllMocks();
      vi.useRealTimers();
    });

    it('should flush on interval', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 100,
        maxBatchSize: 1000,
      });

      const events = createTestCDCEvents(5, BigInt(Date.now()));
      await writer.write(events);

      expect(bucket._storage.size).toBe(0);

      // Advance time past the flush interval
      await vi.advanceTimersByTimeAsync(150);

      expect(bucket._storage.size).toBe(1);

      writer.close();
    });

    it('should use default flush interval of 100ms', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        // No flushIntervalMs specified
      });

      const events = createTestCDCEvents(5, BigInt(Date.now()));
      await writer.write(events);

      expect(bucket._storage.size).toBe(0);

      // Advance time past default 100ms
      await vi.advanceTimersByTimeAsync(150);

      expect(bucket._storage.size).toBe(1);

      writer.close();
    });

    it('should stop interval timer on close', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 100,
      });

      const events = createTestCDCEvents(5, BigInt(Date.now()));
      await writer.write(events);

      writer.close();

      // Advance time - should not flush since writer is closed
      await vi.advanceTimersByTimeAsync(200);

      // Nothing written because closed before flush
      expect(bucket._storage.size).toBe(0);
    });

    it('should throw error when writing to closed writer', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
      });

      writer.close();

      await expect(writer.write(createTestCDCEvents(1, BigInt(Date.now())))).rejects.toThrow(
        'R2Writer is closed'
      );
    });
  });

  describe('R2Writer - Statistics', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should track events written', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
      });

      await writer.write(createTestCDCEvents(10, BigInt(Date.now())));
      await writer.flush();

      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(10);

      writer.close();
    });

    it('should track bytes written', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
      });

      await writer.write(createTestCDCEvents(10, BigInt(Date.now())));
      await writer.flush();

      const stats = writer.getStats();
      expect(stats.bytesWritten).toBeGreaterThan(0);

      writer.close();
    });

    it('should track flush count', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));
      await writer.flush();
      await writer.write(createTestCDCEvents(5, BigInt(Date.now()) + BigInt(10000)));
      await writer.flush();
      await writer.write(createTestCDCEvents(5, BigInt(Date.now()) + BigInt(20000)));
      await writer.flush();

      const stats = writer.getStats();
      expect(stats.flushCount).toBe(3);

      writer.close();
    });

    it('should return copy of stats (immutable)', async () => {
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
      });

      const stats1 = writer.getStats();
      stats1.eventsWritten = 9999; // Try to mutate

      const stats2 = writer.getStats();
      expect(stats2.eventsWritten).toBe(0); // Should be unchanged

      writer.close();
    });
  });

  describe('listCDCFiles', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should list all CDC files for namespace', async () => {
      // Create some CDC files
      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
      });

      const baseTime = BigInt(new Date('2024-01-15T12:00:00Z').getTime());
      await writer.write(createTestCDCEvents(5, baseTime));
      await writer.flush();

      const files = await listCDCFiles(bucket, testNamespace);

      expect(files.length).toBe(1);
      expect(files[0]).toMatch(/\.gcol$/);

      writer.close();
    });

    it('should filter files by time range', async () => {
      // Pre-populate storage with files from different dates
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);

      // Add files for different dates (using new sequence format)
      const dates = ['2024-01-14', '2024-01-15', '2024-01-16', '2024-01-17'];
      for (const date of dates) {
        const path = `${namespacePath}/_wal/${date}/120000-000.gcol`;
        // Create minimal valid GraphCol data
        const triples: Triple[] = [
          createTestTriple(1, 'name', 'Test', BigInt(Date.now()), generateTestTxId(0)),
        ];
        const data = encodeGraphCol(triples, namespace);
        await bucket.put(path, data);
      }

      // Filter to Jan 15-16 (startTime inclusive, endTime exclusive)
      // So Jan 15 and Jan 16 should be included, Jan 17 excluded
      const startTime = BigInt(new Date('2024-01-15T00:00:00Z').getTime());
      const endTime = BigInt(new Date('2024-01-17T00:00:00Z').getTime());

      const files = await listCDCFiles(bucket, namespace, { startTime, endTime });

      expect(files.length).toBe(2);
      expect(files.some((f) => f.includes('2024-01-15'))).toBe(true);
      expect(files.some((f) => f.includes('2024-01-16'))).toBe(true);
      expect(files.some((f) => f.includes('2024-01-17'))).toBe(false);
    });

    it('should return sorted file list', async () => {
      const namespace = testNamespace;
      const namespacePath = parseNamespaceToPath(namespace);

      // Add files out of order (using new sequence format)
      const paths = [
        `${namespacePath}/_wal/2024-01-16/160000-000.gcol`,
        `${namespacePath}/_wal/2024-01-14/040000-000.gcol`,
        `${namespacePath}/_wal/2024-01-15/080000-000.gcol`,
      ];

      for (const path of paths) {
        const triples: Triple[] = [
          createTestTriple(1, 'name', 'Test', BigInt(Date.now()), generateTestTxId(0)),
        ];
        await bucket.put(path, encodeGraphCol(triples, namespace));
      }

      const files = await listCDCFiles(bucket, namespace);

      expect(files.length).toBe(3);
      // Should be sorted by date, then sequence
      expect(files[0]).toContain('2024-01-14');
      expect(files[1]).toContain('2024-01-15');
      expect(files[2]).toContain('2024-01-16');
    });

    it('should return empty array when no files exist', async () => {
      const files = await listCDCFiles(bucket, testNamespace);
      expect(files).toEqual([]);
    });
  });

  describe('readCDCFile', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should decode GraphCol format correctly', async () => {
      const namespace = testNamespace;
      const timestamp = BigInt(Date.now());

      // Write events
      const writer = createR2Writer({
        bucket,
        namespace,
        flushIntervalMs: 0,
      });

      const events = createTestCDCEvents(5, timestamp);
      await writer.write(events);
      await writer.flush();

      writer.close();

      // List and read the file
      const files = await listCDCFiles(bucket, namespace);
      expect(files.length).toBe(1);

      const readEvents = await readCDCFile(bucket, files[0]);

      expect(readEvents.length).toBe(5);
      // Verify event data was preserved
      expect(readEvents[0].triple.object.value).toBe('User 0');
      expect(readEvents[4].triple.object.value).toBe('User 4');
    });

    it('should throw error for non-existent file', async () => {
      await expect(
        readCDCFile(bucket, '.com/.example/crm/acme/_wal/2024-01-16/001.gcol')
      ).rejects.toThrow('CDC file not found');
    });

    it('should reconstruct events as insert type', async () => {
      const namespace = testNamespace;

      // Write mixed event types (though all stored as triples)
      const events: CDCEvent[] = [
        { type: 'insert', triple: createTestTriple(1, 'name', 'Alice', BigInt(Date.now()), generateTestTxId(0)), timestamp: BigInt(Date.now()) },
        { type: 'update', triple: createTestTriple(2, 'name', 'Bob', BigInt(Date.now()), generateTestTxId(1)), timestamp: BigInt(Date.now()) },
        { type: 'delete', triple: createTestTriple(3, 'name', 'Charlie', BigInt(Date.now()), generateTestTxId(2)), timestamp: BigInt(Date.now()) },
      ];

      const writer = createR2Writer({
        bucket,
        namespace,
        flushIntervalMs: 0,
      });

      await writer.write(events);
      await writer.flush();
      writer.close();

      const files = await listCDCFiles(bucket, namespace);
      const readEvents = await readCDCFile(bucket, files[0]);

      // Note: Currently all events are reconstructed as 'insert' since GraphCol
      // doesn't store event type. For full CDC fidelity, this would need enhancement.
      expect(readEvents.every((e) => e.type === 'insert')).toBe(true);
      expect(readEvents.length).toBe(3);
    });

    it('should preserve triple data through round-trip', async () => {
      const namespace = testNamespace;
      const timestamp = BigInt(Date.now());
      const txId = generateTestTxId(0);

      const originalTriple: Triple = {
        subject: createEntityId('https://example.com/user/123'),
        predicate: createPredicate('email'),
        object: { type: ObjectType.STRING, value: 'test@example.com' },
        timestamp,
        txId,
      };

      const events: CDCEvent[] = [{ type: 'insert', triple: originalTriple, timestamp }];

      const writer = createR2Writer({
        bucket,
        namespace,
        flushIntervalMs: 0,
      });

      await writer.write(events);
      await writer.flush();
      writer.close();

      const files = await listCDCFiles(bucket, namespace);
      const readEvents = await readCDCFile(bucket, files[0]);

      expect(readEvents[0].triple.subject).toBe(originalTriple.subject);
      expect(readEvents[0].triple.predicate).toBe(originalTriple.predicate);
      expect(readEvents[0].triple.object.type).toBe(originalTriple.object.type);
      expect(readEvents[0].triple.object.value).toBe(originalTriple.object.value);
      expect(readEvents[0].triple.timestamp).toBe(originalTriple.timestamp);
      expect(readEvents[0].triple.txId).toBe(originalTriple.txId);
    });
  });

  describe('Integration', () => {
    let bucket: R2Bucket & { _storage: Map<string, MockR2Object> };

    beforeEach(() => {
      bucket = createMockR2Bucket();
    });

    it('should support full write-list-read cycle', async () => {
      const namespace = testNamespace;
      const baseTime = BigInt(Date.now());

      // Write events
      const writer = createR2Writer({
        bucket,
        namespace,
        flushIntervalMs: 0,
        maxBatchSize: 100,
      });

      for (let batch = 0; batch < 3; batch++) {
        const events = createTestCDCEvents(10, baseTime + BigInt(batch * 100000));
        await writer.write(events);
        await writer.flush();
      }

      writer.close();

      // List files
      const files = await listCDCFiles(bucket, namespace);
      expect(files.length).toBe(3);

      // Read all events
      let totalEvents = 0;
      for (const file of files) {
        const events = await readCDCFile(bucket, file);
        totalEvents += events.length;
      }

      expect(totalEvents).toBe(30); // 3 batches * 10 events
    });

    it('should handle multiple namespaces independently', async () => {
      const namespace1 = createNamespace('https://example.com/tenant/a');
      const namespace2 = createNamespace('https://example.com/tenant/b');
      const baseTime = BigInt(Date.now());

      // Write to namespace 1
      const writer1 = createR2Writer({
        bucket,
        namespace: namespace1,
        flushIntervalMs: 0,
      });
      await writer1.write(createTestCDCEvents(5, baseTime));
      await writer1.flush();
      writer1.close();

      // Write to namespace 2
      const writer2 = createR2Writer({
        bucket,
        namespace: namespace2,
        flushIntervalMs: 0,
      });
      await writer2.write(createTestCDCEvents(10, baseTime));
      await writer2.flush();
      writer2.close();

      // List each namespace
      const files1 = await listCDCFiles(bucket, namespace1);
      const files2 = await listCDCFiles(bucket, namespace2);

      expect(files1.length).toBe(1);
      expect(files2.length).toBe(1);

      // Read and verify
      const events1 = await readCDCFile(bucket, files1[0]);
      const events2 = await readCDCFile(bucket, files2[0]);

      expect(events1.length).toBe(5);
      expect(events2.length).toBe(10);
    });
  });
});
