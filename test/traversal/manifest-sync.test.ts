/**
 * ManifestSync Tests
 *
 * Tests for the sync protocol between R2 and DO storage.
 * Following TDD approach - write tests first, then implement.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ManifestSync,
  type ManifestSyncConfig,
  type SyncResult,
  type SyncStatus,
} from '../../src/traversal/manifest-sync.js';
import { ManifestStore, type R2Manifest, type EntityIndexEntry } from '../../src/traversal/manifest-store.js';

// ============================================================================
// Mock DurableObjectStorage
// ============================================================================

function createMockStorage(): DurableObjectStorage {
  const store = new Map<string, unknown>();

  return {
    // Support both single key and batch get (array of keys)
    get: vi.fn(async <T>(keyOrKeys: string | string[]): Promise<T | Map<string, T> | undefined> => {
      if (Array.isArray(keyOrKeys)) {
        // Batch get - return Map
        const result = new Map<string, T>();
        for (const key of keyOrKeys) {
          const value = store.get(key);
          if (value !== undefined) {
            result.set(key, value as T);
          }
        }
        return result;
      }
      // Single key get
      return store.get(keyOrKeys) as T | undefined;
    }),
    // Support both single put and batch put (Map or Object)
    put: vi.fn(async <T>(keyOrMap: string | Map<string, T> | Record<string, T>, value?: T): Promise<void> => {
      if (keyOrMap instanceof Map) {
        // Batch put with Map
        for (const [k, v] of keyOrMap) {
          store.set(k, v);
        }
      } else if (typeof keyOrMap === 'object' && keyOrMap !== null) {
        // Batch put with Object (from Object.fromEntries)
        for (const [k, v] of Object.entries(keyOrMap)) {
          store.set(k, v);
        }
      } else {
        // Single put
        store.set(keyOrMap, value);
      }
    }),
    delete: vi.fn(async (key: string): Promise<boolean> => {
      return store.delete(key);
    }),
    list: vi.fn(async <T>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>> => {
      const result = new Map<string, T>();
      let count = 0;
      for (const [key, value] of store) {
        if (options?.limit && count >= options.limit) break;
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value as T);
          count++;
        }
      }
      return result;
    }),
    // Add other methods as stubs (not used in tests)
    deleteAll: vi.fn(),
    deleteAlarm: vi.fn(),
    getAlarm: vi.fn(),
    setAlarm: vi.fn(),
    sync: vi.fn(),
    transaction: vi.fn(),
    transactionSync: vi.fn(),
    getCurrentBookmark: vi.fn(),
    getBookmarkForTime: vi.fn(),
    onNextSessionRestoreBookmark: vi.fn(),
    sql: {} as DurableObjectStorage['sql'],
  } as unknown as DurableObjectStorage;
}

// ============================================================================
// Mock R2 Bucket
// ============================================================================

function createMockR2Bucket(): R2Bucket & { _store: Map<string, string> } {
  const store = new Map<string, string>();

  return {
    _store: store,
    get: vi.fn(async (key: string): Promise<R2ObjectBody | null> => {
      const content = store.get(key);
      if (!content) return null;

      return {
        key,
        version: '1',
        size: content.length,
        etag: 'mock-etag',
        httpEtag: '"mock-etag"',
        checksums: {},
        uploaded: new Date(),
        httpMetadata: {},
        customMetadata: {},
        storageClass: 'Standard',
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(content));
            controller.close();
          },
        }),
        bodyUsed: false,
        arrayBuffer: async () => new TextEncoder().encode(content).buffer as ArrayBuffer,
        text: async () => content,
        json: async () => JSON.parse(content),
        blob: async () => new Blob([content]),
        writeHttpMetadata: () => {},
      } as R2ObjectBody;
    }),
    put: vi.fn(async (key: string, value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob | null) => {
      if (typeof value === 'string') {
        store.set(key, value);
      }
      return {
        key,
        version: '1',
        size: typeof value === 'string' ? value.length : 0,
        etag: 'mock-etag',
        httpEtag: '"mock-etag"',
        checksums: {},
        uploaded: new Date(),
        httpMetadata: {},
        customMetadata: {},
        storageClass: 'Standard',
      } as R2Object;
    }),
    delete: vi.fn(async () => {}),
    head: vi.fn(async () => null),
    list: vi.fn(async () => ({
      objects: [],
      truncated: false,
      delimitedPrefixes: [],
    })),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
  } as unknown as R2Bucket & { _store: Map<string, string> };
}

// ============================================================================
// Test Data
// ============================================================================

const testNamespace = 'https://imdb.com/title/';

const sampleEntityIndex: EntityIndexEntry[] = [
  {
    entityId: 'https://imdb.com/title/tt0000001',
    filePath: '.com/.imdb/title/_chunks/chunk-001.gcol',
    byteOffset: 0,
    byteLength: 512,
  },
  {
    entityId: 'https://imdb.com/title/tt0000002',
    filePath: '.com/.imdb/title/_chunks/chunk-001.gcol',
    byteOffset: 512,
    byteLength: 256,
  },
];

const sampleR2Manifest: R2Manifest = {
  namespace: testNamespace,
  version: 'v1.0.0',
  files: [
    {
      path: '.com/.imdb/title/_chunks/chunk-001.gcol',
      footerOffset: 1024,
      footerSize: 256,
      entityCount: 1000,
    },
  ],
  entityIndex: sampleEntityIndex,
  createdAt: Date.now(),
};

const updatedR2Manifest: R2Manifest = {
  namespace: testNamespace,
  version: 'v2.0.0',
  files: [
    {
      path: '.com/.imdb/title/_chunks/chunk-001.gcol',
      footerOffset: 1024,
      footerSize: 256,
      entityCount: 1500,
    },
    {
      path: '.com/.imdb/title/_chunks/chunk-002.gcol',
      footerOffset: 2048,
      footerSize: 512,
      entityCount: 500,
    },
  ],
  entityIndex: [
    ...sampleEntityIndex,
    {
      entityId: 'https://imdb.com/title/tt0000003',
      filePath: '.com/.imdb/title/_chunks/chunk-002.gcol',
      byteOffset: 0,
      byteLength: 384,
    },
  ],
  createdAt: Date.now(),
};

// ============================================================================
// Tests
// ============================================================================

describe('ManifestSync', () => {
  let doStorage: DurableObjectStorage;
  let r2Bucket: R2Bucket & { _store: Map<string, string> };
  let manifestStore: ManifestStore;
  let manifestSync: ManifestSync;

  beforeEach(() => {
    doStorage = createMockStorage();
    r2Bucket = createMockR2Bucket();
    manifestStore = new ManifestStore(doStorage);
    manifestSync = new ManifestSync({
      r2: r2Bucket,
      store: manifestStore,
      namespace: testNamespace,
    });
  });

  // --------------------------------------------------------------------------
  // needsSync Tests
  // --------------------------------------------------------------------------

  describe('needsSync', () => {
    it('returns true when DO storage is empty', async () => {
      // Put manifest in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));

      const needsSync = await manifestSync.needsSync();

      expect(needsSync).toBe(true);
    });

    it('returns true when versions differ', async () => {
      // Put manifest in R2 with v2.0.0
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(updatedR2Manifest));

      // Put older manifest in DO storage with v1.0.0
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const needsSync = await manifestSync.needsSync();

      expect(needsSync).toBe(true);
    });

    it('returns false when versions match', async () => {
      // Put same manifest in R2 and DO
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const needsSync = await manifestSync.needsSync();

      expect(needsSync).toBe(false);
    });

    it('returns false when R2 has no manifest', async () => {
      // R2 is empty, DO is empty
      const needsSync = await manifestSync.needsSync();

      expect(needsSync).toBe(false);
    });
  });

  // --------------------------------------------------------------------------
  // syncFromR2 Tests
  // --------------------------------------------------------------------------

  describe('syncFromR2', () => {
    it('populates DO storage from R2 manifest', async () => {
      // Put manifest in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));

      const result = await manifestSync.syncFromR2();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('from_r2');
      expect(result.entriesUpdated).toBe(sampleEntityIndex.length);
      expect(result.conflicts).toBe(0);

      // Verify DO storage was populated
      const manifest = await manifestStore.getManifest(testNamespace);
      expect(manifest).not.toBeNull();
      expect(manifest?.version).toBe('v1.0.0');

      // Verify entity index was populated
      const entry = await manifestStore.lookupEntity('https://imdb.com/title/tt0000001');
      expect(entry).not.toBeNull();
      expect(entry?.filePath).toBe('.com/.imdb/title/_chunks/chunk-001.gcol');
    });

    it('returns success with 0 entries when R2 has no manifest', async () => {
      const result = await manifestSync.syncFromR2();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('from_r2');
      expect(result.entriesUpdated).toBe(0);
    });

    it('returns error on R2 fetch failure', async () => {
      // Make R2 throw an error
      vi.mocked(r2Bucket.get).mockRejectedValueOnce(new Error('R2 connection failed'));

      const result = await manifestSync.syncFromR2();

      expect(result.success).toBe(false);
      expect(result.error).toContain('R2 connection failed');
      expect(result.errorCode).toBe('R2_FETCH_FAILED');
    });
  });

  // --------------------------------------------------------------------------
  // syncToR2 Tests
  // --------------------------------------------------------------------------

  describe('syncToR2', () => {
    it('writes DO storage to R2 bucket', async () => {
      // Populate DO storage
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const result = await manifestSync.syncToR2();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('to_r2');
      expect(result.entriesUpdated).toBe(sampleEntityIndex.length);

      // Verify R2 was written
      const r2Path = '.com/.imdb/title/_manifest.json';
      expect(r2Bucket._store.has(r2Path)).toBe(true);

      const r2Content = JSON.parse(r2Bucket._store.get(r2Path)!);
      expect(r2Content.namespace).toBe(testNamespace);
      expect(r2Content.entityIndex.length).toBe(sampleEntityIndex.length);
    });

    it('returns success with 0 entries when DO storage is empty', async () => {
      const result = await manifestSync.syncToR2();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('to_r2');
      expect(result.entriesUpdated).toBe(0);
    });

    it('logs error but returns failure on R2 write failure', async () => {
      // Populate DO storage
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      // Make R2 throw an error
      vi.mocked(r2Bucket.put).mockRejectedValueOnce(new Error('R2 write failed'));

      // Spy on console.error
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      const result = await manifestSync.syncToR2();

      expect(result.success).toBe(false);
      expect(result.error).toContain('R2 write failed');
      expect(result.errorCode).toBe('R2_WRITE_FAILED');
      expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('R2 write failed'));

      consoleSpy.mockRestore();
    });
  });

  // --------------------------------------------------------------------------
  // fullSync Tests
  // --------------------------------------------------------------------------

  describe('fullSync', () => {
    it('syncs from R2 when DO is empty and R2 has data', async () => {
      // Put manifest in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));

      const result = await manifestSync.fullSync();

      expect(result.success).toBe(true);
      expect(result.entriesUpdated).toBe(sampleEntityIndex.length);

      // Verify DO was populated
      const manifest = await manifestStore.getManifest(testNamespace);
      expect(manifest?.version).toBe('v1.0.0');
    });

    it('syncs to R2 when DO has data and R2 is empty', async () => {
      // Populate DO storage
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const result = await manifestSync.fullSync();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('to_r2');

      // Verify R2 was populated
      const r2Path = '.com/.imdb/title/_manifest.json';
      expect(r2Bucket._store.has(r2Path)).toBe(true);
    });

    it('resolves conflicts with R2 winning (source of truth)', async () => {
      // Put older manifest in DO (v1.0.0)
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      // Put newer manifest in R2 (v2.0.0)
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(updatedR2Manifest));

      const result = await manifestSync.fullSync();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('bidirectional');
      expect(result.conflicts).toBe(1);

      // Verify DO was updated to R2 version
      const manifest = await manifestStore.getManifest(testNamespace);
      expect(manifest?.version).toBe('v2.0.0');

      // Verify new entity is available
      const entry = await manifestStore.lookupEntity('https://imdb.com/title/tt0000003');
      expect(entry).not.toBeNull();
    });

    it('returns no changes when versions match', async () => {
      // Put same manifest in both
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const result = await manifestSync.fullSync();

      expect(result.success).toBe(true);
      expect(result.direction).toBe('bidirectional');
      expect(result.entriesUpdated).toBe(0);
      expect(result.conflicts).toBe(0);
    });
  });

  // --------------------------------------------------------------------------
  // getSyncStatus Tests
  // --------------------------------------------------------------------------

  describe('getSyncStatus', () => {
    it('returns initial state with null values', () => {
      const status = manifestSync.getSyncStatus();

      expect(status.lastSyncTime).toBeNull();
      expect(status.doVersion).toBeNull();
      expect(status.r2Version).toBeNull();
      expect(status.isStale).toBe(false);
      expect(status.syncInProgress).toBe(false);
    });

    it('returns cached R2 version after sync', async () => {
      // Put manifest in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));

      // Sync from R2
      await manifestSync.syncFromR2();

      const status = manifestSync.getSyncStatus();

      expect(status.lastSyncTime).not.toBeNull();
      expect(status.r2Version).toBe('v1.0.0');
    });
  });

  describe('getSyncStatusAsync', () => {
    it('returns correct state with fresh data', async () => {
      // Put manifest in R2 and DO with same version
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const status = await manifestSync.getSyncStatusAsync();

      expect(status.doVersion).toBe('v1.0.0');
      expect(status.r2Version).toBe('v1.0.0');
      expect(status.isStale).toBe(false);
    });

    it('returns isStale=true when versions differ', async () => {
      // Put older manifest in DO
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      // Put newer manifest in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(updatedR2Manifest));

      const status = await manifestSync.getSyncStatusAsync();

      expect(status.doVersion).toBe('v1.0.0');
      expect(status.r2Version).toBe('v2.0.0');
      expect(status.isStale).toBe(true);
    });

    it('returns isStale=true when DO is empty but R2 has data', async () => {
      // Put manifest only in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));

      const status = await manifestSync.getSyncStatusAsync();

      expect(status.doVersion).toBeNull();
      expect(status.r2Version).toBe('v1.0.0');
      expect(status.isStale).toBe(true);
    });
  });

  // --------------------------------------------------------------------------
  // Edge Cases
  // --------------------------------------------------------------------------

  describe('edge cases', () => {
    it('handles invalid JSON in R2 manifest', async () => {
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, 'not valid json');

      const result = await manifestSync.syncFromR2();

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
    });

    it('handles concurrent syncs gracefully', async () => {
      // Put manifest in R2
      const r2Path = '.com/.imdb/title/_manifest.json';
      r2Bucket._store.set(r2Path, JSON.stringify(sampleR2Manifest));

      // Start two syncs concurrently
      const [result1, result2] = await Promise.all([
        manifestSync.syncFromR2(),
        manifestSync.syncFromR2(),
      ]);

      expect(result1.success).toBe(true);
      expect(result2.success).toBe(true);

      // Verify final state is consistent
      const manifest = await manifestStore.getManifest(testNamespace);
      expect(manifest?.version).toBe('v1.0.0');
    });
  });
});
