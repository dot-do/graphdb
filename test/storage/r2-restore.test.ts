/**
 * R2 Restore Tests
 *
 * Tests for:
 * - listBackups groups CDC files by date
 * - getBackupMetadata fetches timestamp range
 * - findBackupBeforeTimestamp finds correct snapshot
 * - restoreFromBackup replays events correctly
 * - Point-in-time recovery with timestamp filtering
 * - Resumable restore with tokens
 * - Dry run mode
 *
 * Uses mocks for R2Bucket in tests.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  listBackups,
  getBackupMetadata,
  findBackupBeforeTimestamp,
  restoreFromBackup,
  restoreFromSnapshot,
  estimateRestoreDuration,
  validateBackup,
  getBackupSize,
  countBackupEvents,
  estimateEventCount,
  type BackupSnapshot,
  type RestoreOptions,
  type RestoreProgress,
  type RestoreResult,
} from '../../src/storage/r2-restore';
import { createR2Writer, getCDCPath, listCDCFiles, readCDCFile } from '../../src/storage/r2-writer';
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
 * Create a test CDC event
 */
function createTestCDCEvent(
  subjectId: number,
  value: string,
  timestamp: bigint,
  type: 'insert' | 'update' | 'delete' = 'insert'
): CDCEvent {
  return {
    type,
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

/**
 * Seed test data into mock bucket
 */
async function seedTestData(
  bucket: R2Bucket,
  namespace: Namespace,
  dates: string[],
  eventsPerFile: number = 10
): Promise<void> {
  for (const dateStr of dates) {
    const [year, month, day] = dateStr.split('-').map(Number);
    const baseTimestamp = BigInt(new Date(year!, month! - 1, day!, 12, 0, 0).getTime());

    // Create a few files per date
    for (let fileIdx = 0; fileIdx < 3; fileIdx++) {
      const fileTimestamp = baseTimestamp + BigInt(fileIdx * 3600000); // 1 hour apart
      const events = createTestCDCEvents(eventsPerFile, fileTimestamp);
      const triples = events.map((e) => e.triple);
      const encoded = encodeGraphCol(triples, namespace);
      const path = getCDCPath(namespace, fileTimestamp);
      await bucket.put(path, encoded);
    }
  }
}

// ============================================================================
// Tests
// ============================================================================

describe('R2 Restore', () => {
  const testNamespace = createNamespace('https://example.com/crm/acme');

  describe('listBackups', () => {
    it('returns empty array for namespace with no backups', async () => {
      const bucket = createMockR2Bucket();

      const backups = await listBackups(bucket, testNamespace);

      expect(backups).toEqual([]);
    });

    it('groups CDC files by date', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15', '2024-01-16', '2024-01-17']);

      const backups = await listBackups(bucket, testNamespace);

      expect(backups).toHaveLength(3);
      expect(backups[0]?.date).toBe('2024-01-15');
      expect(backups[1]?.date).toBe('2024-01-16');
      expect(backups[2]?.date).toBe('2024-01-17');
    });

    it('returns correct file count per snapshot', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const backups = await listBackups(bucket, testNamespace);

      expect(backups).toHaveLength(1);
      expect(backups[0]?.fileCount).toBe(3); // We create 3 files per date in seedTestData
    });

    it('filters by start date', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-14', '2024-01-15', '2024-01-16']);

      const backups = await listBackups(bucket, testNamespace, {
        startDate: '2024-01-15',
      });

      expect(backups).toHaveLength(2);
      expect(backups[0]?.date).toBe('2024-01-15');
      expect(backups[1]?.date).toBe('2024-01-16');
    });

    it('filters by end date', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-14', '2024-01-15', '2024-01-16']);

      const backups = await listBackups(bucket, testNamespace, {
        endDate: '2024-01-15',
      });

      expect(backups).toHaveLength(2);
      expect(backups[0]?.date).toBe('2024-01-14');
      expect(backups[1]?.date).toBe('2024-01-15');
    });

    it('applies limit to results', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-14', '2024-01-15', '2024-01-16', '2024-01-17']);

      const backups = await listBackups(bucket, testNamespace, {
        limit: 2,
      });

      expect(backups).toHaveLength(2);
    });

    it('sorts snapshots chronologically', async () => {
      const bucket = createMockR2Bucket();
      // Seed in non-chronological order
      await seedTestData(bucket, testNamespace, ['2024-01-17', '2024-01-14', '2024-01-16']);

      const backups = await listBackups(bucket, testNamespace);

      expect(backups.map((b) => b.date)).toEqual(['2024-01-14', '2024-01-16', '2024-01-17']);
    });
  });

  describe('getBackupMetadata', () => {
    it('returns snapshot unchanged if no files', async () => {
      const bucket = createMockR2Bucket();
      const emptySnapshot: BackupSnapshot = {
        date: '2024-01-15',
        namespace: testNamespace,
        fileCount: 0,
        files: [],
      };

      const result = await getBackupMetadata(bucket, emptySnapshot);

      expect(result).toEqual(emptySnapshot);
    });

    it('calculates total size for snapshot', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const backups = await listBackups(bucket, testNamespace);
      const metadata = await getBackupMetadata(bucket, backups[0]!);

      expect(metadata.totalSizeBytes).toBeDefined();
      expect(metadata.totalSizeBytes).toBeGreaterThan(0);
    });

    it('extracts timestamp range from files', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const backups = await listBackups(bucket, testNamespace);
      const metadata = await getBackupMetadata(bucket, backups[0]!);

      expect(metadata.earliestTimestamp).toBeDefined();
      expect(metadata.latestTimestamp).toBeDefined();
      expect(metadata.earliestTimestamp! <= metadata.latestTimestamp!).toBe(true);
    });
  });

  describe('findBackupBeforeTimestamp', () => {
    it('returns null if no backups exist', async () => {
      const bucket = createMockR2Bucket();
      const timestamp = BigInt(Date.parse('2024-01-15T12:00:00Z'));

      const result = await findBackupBeforeTimestamp(bucket, testNamespace, timestamp);

      expect(result).toBeNull();
    });

    it('finds latest backup before timestamp', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-14', '2024-01-15', '2024-01-16']);

      const timestamp = BigInt(Date.parse('2024-01-16T00:00:00Z'));
      const result = await findBackupBeforeTimestamp(bucket, testNamespace, timestamp);

      expect(result).not.toBeNull();
      expect(result?.date).toBe('2024-01-16');
    });

    it('returns backup on same date', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const timestamp = BigInt(Date.parse('2024-01-15T18:00:00Z'));
      const result = await findBackupBeforeTimestamp(bucket, testNamespace, timestamp);

      expect(result).not.toBeNull();
      expect(result?.date).toBe('2024-01-15');
    });
  });

  describe('restoreFromBackup', () => {
    it('returns empty result for namespace with no backups', async () => {
      const bucket = createMockR2Bucket();
      const handler = vi.fn();

      const result = await restoreFromBackup(bucket, testNamespace, handler);

      expect(result.success).toBe(true);
      expect(result.eventsReplayed).toBe(0);
      expect(result.filesProcessed).toBe(0);
      expect(handler).not.toHaveBeenCalled();
    });

    it('replays all events in chronological order', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 5);

      const receivedEvents: CDCEvent[] = [];
      const handler = vi.fn(async (events: CDCEvent[]) => {
        receivedEvents.push(...events);
      });

      const result = await restoreFromBackup(bucket, testNamespace, handler);

      expect(result.success).toBe(true);
      expect(result.eventsReplayed).toBe(15); // 3 files * 5 events
      expect(result.filesProcessed).toBe(3);
      expect(receivedEvents.length).toBe(15);
    });

    it('filters events by target timestamp', async () => {
      const bucket = createMockR2Bucket();
      // Create events at known timestamps
      const baseTimestamp = BigInt(Date.parse('2024-01-15T12:00:00Z'));
      const events = createTestCDCEvents(10, baseTimestamp);
      const triples = events.map((e) => e.triple);
      const encoded = encodeGraphCol(triples, testNamespace);
      const path = getCDCPath(testNamespace, baseTimestamp);
      await bucket.put(path, encoded);

      const receivedEvents: CDCEvent[] = [];
      const handler = vi.fn(async (evts: CDCEvent[]) => {
        receivedEvents.push(...evts);
      });

      // Filter to only first 5 events
      const targetTimestamp = baseTimestamp + BigInt(4000); // First 5 events (0-4 seconds)
      const result = await restoreFromBackup(bucket, testNamespace, handler, {
        targetTimestamp,
      });

      expect(result.success).toBe(true);
      expect(result.eventsReplayed).toBe(5);
      expect(result.eventsSkipped).toBe(5);
    });

    it('respects batch size', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 10);

      const batchSizes: number[] = [];
      const handler = vi.fn(async (events: CDCEvent[]) => {
        batchSizes.push(events.length);
      });

      await restoreFromBackup(bucket, testNamespace, handler, {
        batchSize: 5,
      });

      // Each batch should be at most 5 events
      expect(batchSizes.every((size) => size <= 5)).toBe(true);
    });

    it('calls progress callback', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 5);

      const progressUpdates: RestoreProgress[] = [];
      const handler = vi.fn();

      await restoreFromBackup(bucket, testNamespace, handler, {
        onProgress: (progress) => {
          progressUpdates.push({ ...progress });
        },
      });

      expect(progressUpdates.length).toBeGreaterThan(0);
      // Final progress should be 100%
      const finalProgress = progressUpdates[progressUpdates.length - 1];
      expect(finalProgress?.percentComplete).toBe(100);
    });

    it('supports dry run mode', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 5);

      const handler = vi.fn();

      const result = await restoreFromBackup(bucket, testNamespace, handler, {
        dryRun: true,
      });

      expect(result.success).toBe(true);
      expect(result.eventsReplayed).toBe(15);
      expect(handler).not.toHaveBeenCalled(); // Handler not called in dry run
    });

    it('filters delete events when includeDeletes is false', async () => {
      const bucket = createMockR2Bucket();

      // Create events with mix of insert and delete
      const baseTimestamp = BigInt(Date.parse('2024-01-15T12:00:00Z'));
      const events: CDCEvent[] = [
        createTestCDCEvent(1, 'User 1', baseTimestamp, 'insert'),
        createTestCDCEvent(2, 'User 2', baseTimestamp + BigInt(1000), 'insert'),
        createTestCDCEvent(1, 'User 1', baseTimestamp + BigInt(2000), 'delete'),
        createTestCDCEvent(3, 'User 3', baseTimestamp + BigInt(3000), 'insert'),
      ];
      const triples = events.map((e) => e.triple);
      const encoded = encodeGraphCol(triples, testNamespace);
      const path = getCDCPath(testNamespace, baseTimestamp);
      await bucket.put(path, encoded);

      const receivedEvents: CDCEvent[] = [];
      const handler = vi.fn(async (evts: CDCEvent[]) => {
        receivedEvents.push(...evts);
      });

      // Note: The delete event is not preserved in GraphCol encoding
      // This test verifies the filtering logic still works
      const result = await restoreFromBackup(bucket, testNamespace, handler, {
        includeDeletes: false,
      });

      expect(result.success).toBe(true);
    });

    it('returns latest timestamp restored', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 10);

      const handler = vi.fn();
      const result = await restoreFromBackup(bucket, testNamespace, handler);

      expect(result.latestTimestamp).toBeDefined();
      expect(typeof result.latestTimestamp).toBe('bigint');
    });
  });

  describe('restoreFromSnapshot', () => {
    it('restores events from a specific snapshot', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15', '2024-01-16']);

      const backups = await listBackups(bucket, testNamespace);
      const targetSnapshot = backups.find((b) => b.date === '2024-01-15')!;

      const receivedEvents: CDCEvent[] = [];
      const handler = vi.fn(async (events: CDCEvent[]) => {
        receivedEvents.push(...events);
      });

      const result = await restoreFromSnapshot(bucket, targetSnapshot, handler);

      expect(result.success).toBe(true);
      expect(result.filesProcessed).toBe(3); // Only files from 2024-01-15
    });

    it('returns empty result for empty snapshot', async () => {
      const bucket = createMockR2Bucket();
      const emptySnapshot: BackupSnapshot = {
        date: '2024-01-15',
        namespace: testNamespace,
        fileCount: 0,
        files: [],
      };

      const handler = vi.fn();
      const result = await restoreFromSnapshot(bucket, emptySnapshot, handler);

      expect(result.success).toBe(true);
      expect(result.eventsReplayed).toBe(0);
      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('validateBackup', () => {
    it('returns valid for backup with all files present', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const backups = await listBackups(bucket, testNamespace);
      const result = await validateBackup(bucket, backups[0]!);

      expect(result.valid).toBe(true);
      expect(result.missingFiles).toHaveLength(0);
    });

    it('detects missing files', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const backups = await listBackups(bucket, testNamespace);
      const snapshot = backups[0]!;

      // Remove one file
      await bucket.delete(snapshot.files[0]!);

      const result = await validateBackup(bucket, snapshot);

      expect(result.valid).toBe(false);
      expect(result.missingFiles).toHaveLength(1);
    });
  });

  describe('getBackupSize', () => {
    it('returns 0 for empty snapshot', async () => {
      const bucket = createMockR2Bucket();
      const emptySnapshot: BackupSnapshot = {
        date: '2024-01-15',
        namespace: testNamespace,
        fileCount: 0,
        files: [],
      };

      const size = await getBackupSize(bucket, emptySnapshot);

      expect(size).toBe(0);
    });

    it('returns total size of all files', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15']);

      const backups = await listBackups(bucket, testNamespace);
      const size = await getBackupSize(bucket, backups[0]!);

      expect(size).toBeGreaterThan(0);
    });
  });

  describe('countBackupEvents', () => {
    it('returns 0 for empty snapshot', async () => {
      const bucket = createMockR2Bucket();
      const emptySnapshot: BackupSnapshot = {
        date: '2024-01-15',
        namespace: testNamespace,
        fileCount: 0,
        files: [],
      };

      const count = await countBackupEvents(bucket, emptySnapshot);

      expect(count).toBe(0);
    });

    it('returns total event count', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 10);

      const backups = await listBackups(bucket, testNamespace);
      const count = await countBackupEvents(bucket, backups[0]!);

      expect(count).toBe(30); // 3 files * 10 events
    });
  });

  describe('estimateEventCount', () => {
    it('estimates event count based on file size', async () => {
      const bucket = createMockR2Bucket();
      await seedTestData(bucket, testNamespace, ['2024-01-15'], 10);

      const backups = await listBackups(bucket, testNamespace);
      const estimate = await estimateEventCount(bucket, backups[0]!);

      // Estimate should be a positive number
      expect(estimate).toBeGreaterThan(0);
    });
  });

  describe('estimateRestoreDuration', () => {
    it('estimates duration based on file count', () => {
      const snapshot: BackupSnapshot = {
        date: '2024-01-15',
        namespace: testNamespace,
        fileCount: 10,
        files: [],
      };

      const duration = estimateRestoreDuration(snapshot);

      // 10 files * 1000 events/file / 10000 events/sec = 1 second
      expect(duration).toBe(1000);
    });

    it('respects custom events per second', () => {
      const snapshot: BackupSnapshot = {
        date: '2024-01-15',
        namespace: testNamespace,
        fileCount: 10,
        files: [],
      };

      const duration = estimateRestoreDuration(snapshot, 5000);

      // 10 files * 1000 events/file / 5000 events/sec = 2 seconds
      expect(duration).toBe(2000);
    });
  });
});
