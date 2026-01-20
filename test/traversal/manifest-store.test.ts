/**
 * ManifestStore Tests
 *
 * Tests for DO-local manifest storage for zero-RTT lookups.
 * Following TDD approach - write tests first, then implement.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ManifestStore,
  ManifestStoreError,
  ManifestStoreErrorCode,
  type ManifestFile,
  type EntityIndexEntry,
  type R2Manifest,
  type CacheStats,
} from '../../src/traversal/manifest-store.js';

// ============================================================================
// Mock DurableObjectStorage
// ============================================================================

function createMockStorage(): DurableObjectStorage {
  const store = new Map<string, unknown>();

  return {
    get: vi.fn(async <T>(keyOrKeys: string | string[]): Promise<T | Map<string, T> | undefined> => {
      // Handle batch get (array of keys)
      if (Array.isArray(keyOrKeys)) {
        const result = new Map<string, T>();
        for (const key of keyOrKeys) {
          const value = store.get(key);
          if (value !== undefined) {
            result.set(key, value as T);
          }
        }
        return result as Map<string, T>;
      }
      // Single key get
      return store.get(keyOrKeys) as T | undefined;
    }),
    put: vi.fn(async <T>(keyOrEntries: string | Record<string, T>, value?: T): Promise<void> => {
      // Handle batch put (object of key-value pairs)
      if (typeof keyOrEntries === 'object') {
        for (const [k, v] of Object.entries(keyOrEntries)) {
          store.set(k, v);
        }
        return;
      }
      // Single key put
      store.set(keyOrEntries, value);
    }),
    delete: vi.fn(async (keyOrKeys: string | string[]): Promise<boolean> => {
      if (Array.isArray(keyOrKeys)) {
        let deleted = false;
        for (const key of keyOrKeys) {
          if (store.delete(key)) deleted = true;
        }
        return deleted;
      }
      return store.delete(keyOrKeys);
    }),
    list: vi.fn(
      async (options?: { prefix?: string; limit?: number }): Promise<Map<string, unknown>> => {
        const result = new Map<string, unknown>();
        let count = 0;
        for (const [key, value] of store) {
          if (options?.limit && count >= options.limit) break;
          if (!options?.prefix || key.startsWith(options.prefix)) {
            result.set(key, value);
            count++;
          }
        }
        return result;
      }
    ),
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
// Test Data
// ============================================================================

const sampleManifest: ManifestFile = {
  namespace: 'https://imdb.com/title/',
  path: '.com/.imdb/title/_chunks/chunk-001.gcol',
  footerOffset: 1024,
  footerSize: 256,
  entityCount: 1000,
  version: 'v1.0.0',
  updatedAt: Date.now(),
};

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
  {
    entityId: 'https://imdb.com/title/tt0000003',
    filePath: '.com/.imdb/title/_chunks/chunk-002.gcol',
    byteOffset: 0,
    byteLength: 384,
  },
];

const sampleR2Manifest: R2Manifest = {
  namespace: 'https://imdb.com/title/',
  version: 'v1.0.0',
  files: [
    {
      path: '.com/.imdb/title/_chunks/chunk-001.gcol',
      footerOffset: 1024,
      footerSize: 256,
      entityCount: 1000,
    },
    {
      path: '.com/.imdb/title/_chunks/chunk-002.gcol',
      footerOffset: 2048,
      footerSize: 512,
      entityCount: 500,
    },
  ],
  entityIndex: sampleEntityIndex,
  createdAt: Date.now(),
};

// ============================================================================
// Tests
// ============================================================================

describe('ManifestStore', () => {
  let storage: DurableObjectStorage;
  let manifestStore: ManifestStore;

  beforeEach(() => {
    storage = createMockStorage();
    manifestStore = new ManifestStore(storage);
  });

  // --------------------------------------------------------------------------
  // Manifest CRUD
  // --------------------------------------------------------------------------

  describe('manifest operations', () => {
    it('putManifest/getManifest round-trip', async () => {
      // Put a manifest
      await manifestStore.putManifest(sampleManifest);

      // Get it back
      const retrieved = await manifestStore.getManifest(sampleManifest.namespace);

      expect(retrieved).not.toBeNull();
      expect(retrieved?.namespace).toBe(sampleManifest.namespace);
      expect(retrieved?.path).toBe(sampleManifest.path);
      expect(retrieved?.footerOffset).toBe(sampleManifest.footerOffset);
      expect(retrieved?.footerSize).toBe(sampleManifest.footerSize);
      expect(retrieved?.entityCount).toBe(sampleManifest.entityCount);
      expect(retrieved?.version).toBe(sampleManifest.version);
    });

    it('getManifest returns null for missing namespace', async () => {
      const retrieved = await manifestStore.getManifest('https://nonexistent.com/');
      expect(retrieved).toBeNull();
    });

    it('listManifests returns all stored manifests', async () => {
      const manifest2: ManifestFile = {
        namespace: 'https://wiktionary.org/entries/',
        path: '.org/.wiktionary/entries/_chunks/chunk-001.gcol',
        footerOffset: 512,
        footerSize: 128,
        entityCount: 500,
        version: 'v2.0.0',
        updatedAt: Date.now(),
      };

      await manifestStore.putManifest(sampleManifest);
      await manifestStore.putManifest(manifest2);

      const manifests = await manifestStore.listManifests();

      expect(manifests).toHaveLength(2);
      expect(manifests.map((m) => m.namespace)).toContain(sampleManifest.namespace);
      expect(manifests.map((m) => m.namespace)).toContain(manifest2.namespace);
    });

    it('listManifestsForNamespace returns only manifests for that namespace', async () => {
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const manifests = await manifestStore.listManifestsForNamespace(sampleR2Manifest.namespace);

      expect(manifests).toHaveLength(2);
      expect(manifests.every((m) => m.namespace === sampleR2Manifest.namespace)).toBe(true);
    });

    it('deleteManifest removes the manifest', async () => {
      await manifestStore.putManifest(sampleManifest);

      const deleted = await manifestStore.deleteManifest(
        sampleManifest.namespace,
        sampleManifest.path
      );
      expect(deleted).toBe(true);

      const retrieved = await manifestStore.getManifest(sampleManifest.namespace);
      expect(retrieved).toBeNull();
    });
  });

  // --------------------------------------------------------------------------
  // Entity Index Operations
  // --------------------------------------------------------------------------

  describe('entity index operations', () => {
    it('loadEntityIndex returns Map with entries', async () => {
      // Store entity index
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      // Load it back
      const index = await manifestStore.loadEntityIndex(sampleManifest.namespace);

      expect(index.size).toBe(sampleEntityIndex.length);
      expect(index.get('https://imdb.com/title/tt0000001')).toEqual(sampleEntityIndex[0]);
      expect(index.get('https://imdb.com/title/tt0000002')).toEqual(sampleEntityIndex[1]);
      expect(index.get('https://imdb.com/title/tt0000003')).toEqual(sampleEntityIndex[2]);
    });

    it('loadEntityIndex returns empty Map for missing namespace', async () => {
      const index = await manifestStore.loadEntityIndex('https://nonexistent.com/');
      expect(index.size).toBe(0);
    });

    it('lookupEntity finds entry from loaded index', async () => {
      // Store entity index
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      // Lookup an entity
      const entry = await manifestStore.lookupEntity('https://imdb.com/title/tt0000001');

      expect(entry).not.toBeNull();
      expect(entry?.entityId).toBe('https://imdb.com/title/tt0000001');
      expect(entry?.filePath).toBe('.com/.imdb/title/_chunks/chunk-001.gcol');
      expect(entry?.byteOffset).toBe(0);
      expect(entry?.byteLength).toBe(512);
    });

    it('lookupEntity returns null for missing entity', async () => {
      // Store entity index
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      // Lookup non-existent entity
      const entry = await manifestStore.lookupEntity('https://imdb.com/title/tt9999999');

      expect(entry).toBeNull();
    });

    it('lookupEntity loads index from storage on first call', async () => {
      // Store entity index
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      // Create a new ManifestStore instance (simulating a fresh DO)
      const freshStore = new ManifestStore(storage);

      // Lookup should still work (loads from storage)
      const entry = await freshStore.lookupEntity('https://imdb.com/title/tt0000002');

      expect(entry).not.toBeNull();
      expect(entry?.entityId).toBe('https://imdb.com/title/tt0000002');
    });

    it('deleteEntityIndex removes all entities for namespace', async () => {
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      await manifestStore.deleteEntityIndex(sampleManifest.namespace);

      const index = await manifestStore.loadEntityIndex(sampleManifest.namespace);
      expect(index.size).toBe(0);
    });
  });

  // --------------------------------------------------------------------------
  // Version/Staleness Check
  // --------------------------------------------------------------------------

  describe('staleness check', () => {
    it('isStale returns true when versions differ', async () => {
      await manifestStore.putManifest(sampleManifest);

      const stale = await manifestStore.isStale(sampleManifest.namespace, 'v2.0.0');

      expect(stale).toBe(true);
    });

    it('isStale returns false when versions match', async () => {
      await manifestStore.putManifest(sampleManifest);

      const stale = await manifestStore.isStale(sampleManifest.namespace, sampleManifest.version);

      expect(stale).toBe(false);
    });

    it('isStale returns true when namespace not found', async () => {
      const stale = await manifestStore.isStale('https://nonexistent.com/', 'v1.0.0');

      expect(stale).toBe(true);
    });
  });

  // --------------------------------------------------------------------------
  // Import/Export from R2 Manifest
  // --------------------------------------------------------------------------

  describe('import/export from R2 manifest', () => {
    it('importFromR2Manifest populates storage with all files', async () => {
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      // Check manifests were created for each file (now properly stores both)
      const manifests = await manifestStore.listManifests();
      expect(manifests.length).toBe(2);

      // Check entity index was populated
      const entry = await manifestStore.lookupEntity('https://imdb.com/title/tt0000001');
      expect(entry).not.toBeNull();
      expect(entry?.filePath).toBe('.com/.imdb/title/_chunks/chunk-001.gcol');
    });

    it('exportToR2Manifest reconstructs manifest structure', async () => {
      // Import first
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      // Export
      const exported = await manifestStore.exportToR2Manifest();

      expect(exported.namespace).toBe(sampleR2Manifest.namespace);
      expect(exported.version).toBe(sampleR2Manifest.version);
      expect(exported.files).toHaveLength(2);
      expect(exported.entityIndex).toBeDefined();
      expect(exported.entityIndex.length).toBe(sampleEntityIndex.length);
    });

    it('exportToR2Manifest accepts namespace parameter', async () => {
      await manifestStore.importFromR2Manifest(sampleR2Manifest);

      const exported = await manifestStore.exportToR2Manifest(sampleR2Manifest.namespace);

      expect(exported.namespace).toBe(sampleR2Manifest.namespace);
      expect(exported.files).toHaveLength(2);
    });
  });

  // --------------------------------------------------------------------------
  // In-Memory Caching
  // --------------------------------------------------------------------------

  describe('in-memory caching', () => {
    it('entity index is cached in memory after first load', async () => {
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      // First lookup (loads from storage)
      await manifestStore.lookupEntity('https://imdb.com/title/tt0000001');

      // Clear the mock call count
      vi.mocked(storage.list).mockClear();
      vi.mocked(storage.get).mockClear();

      // Second lookup (should use cache)
      await manifestStore.lookupEntity('https://imdb.com/title/tt0000002');

      // storage.get should not have been called again (using in-memory cache)
      expect(storage.get).not.toHaveBeenCalled();
    });

    it('getCacheStats returns correct statistics', async () => {
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);
      await manifestStore.loadEntityIndex(sampleManifest.namespace);

      const stats: CacheStats = manifestStore.getCacheStats();

      expect(stats.cachedNamespaces).toBe(1);
      expect(stats.totalCachedEntities).toBe(3);
      expect(stats.entitiesPerNamespace[sampleManifest.namespace]).toBe(3);
    });

    it('LRU eviction removes oldest namespace when limit reached', async () => {
      // Create store with small limit
      const smallStore = new ManifestStore(storage, { maxCachedNamespaces: 2 });

      const ns1 = 'https://ns1.com/';
      const ns2 = 'https://ns2.com/';
      const ns3 = 'https://ns3.com/';

      const entries1: EntityIndexEntry[] = [
        { entityId: 'https://ns1.com/e1', filePath: 'f1', byteOffset: 0, byteLength: 100 },
      ];
      const entries2: EntityIndexEntry[] = [
        { entityId: 'https://ns2.com/e1', filePath: 'f2', byteOffset: 0, byteLength: 100 },
      ];
      const entries3: EntityIndexEntry[] = [
        { entityId: 'https://ns3.com/e1', filePath: 'f3', byteOffset: 0, byteLength: 100 },
      ];

      await smallStore.putEntityIndex(ns1, entries1);
      await smallStore.putEntityIndex(ns2, entries2);

      // Both should be loaded
      expect(smallStore.isNamespaceLoaded(ns1)).toBe(true);
      expect(smallStore.isNamespaceLoaded(ns2)).toBe(true);

      // Add third namespace - should evict ns1 (LRU)
      await smallStore.putEntityIndex(ns3, entries3);

      expect(smallStore.isNamespaceLoaded(ns1)).toBe(false);
      expect(smallStore.isNamespaceLoaded(ns2)).toBe(true);
      expect(smallStore.isNamespaceLoaded(ns3)).toBe(true);
    });
  });

  // --------------------------------------------------------------------------
  // Error Handling
  // --------------------------------------------------------------------------

  describe('error handling', () => {
    it('wraps storage errors in ManifestStoreError', async () => {
      // Create a storage mock that throws
      const failingStorage = {
        ...createMockStorage(),
        put: vi.fn().mockRejectedValue(new Error('Storage quota exceeded')),
      } as unknown as DurableObjectStorage;

      const failingStore = new ManifestStore(failingStorage);

      await expect(failingStore.putManifest(sampleManifest)).rejects.toThrow(ManifestStoreError);
      await expect(failingStore.putManifest(sampleManifest)).rejects.toMatchObject({
        code: ManifestStoreErrorCode.STORAGE_WRITE_FAILED,
      });
    });

    it('preserves original error as cause', async () => {
      const originalError = new Error('Connection timeout');
      const failingStorage = {
        ...createMockStorage(),
        list: vi.fn().mockRejectedValue(originalError),
      } as unknown as DurableObjectStorage;

      const failingStore = new ManifestStore(failingStorage);

      try {
        await failingStore.listManifests();
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ManifestStoreError);
        expect((error as ManifestStoreError).cause).toBe(originalError);
      }
    });
  });

  // --------------------------------------------------------------------------
  // Edge Cases
  // --------------------------------------------------------------------------

  describe('edge cases', () => {
    it('handles empty entity index', async () => {
      await manifestStore.putEntityIndex(sampleManifest.namespace, []);

      const index = await manifestStore.loadEntityIndex(sampleManifest.namespace);
      expect(index.size).toBe(0);
    });

    it('overwrites existing manifest on put', async () => {
      await manifestStore.putManifest(sampleManifest);

      const updatedManifest: ManifestFile = {
        ...sampleManifest,
        version: 'v2.0.0',
        entityCount: 2000,
      };
      await manifestStore.putManifest(updatedManifest);

      const retrieved = await manifestStore.getManifest(sampleManifest.namespace);
      expect(retrieved?.version).toBe('v2.0.0');
      expect(retrieved?.entityCount).toBe(2000);
    });

    it('handles multiple namespaces independently', async () => {
      const ns1 = 'https://imdb.com/title/';
      const ns2 = 'https://imdb.com/name/';

      const entries1: EntityIndexEntry[] = [
        {
          entityId: 'https://imdb.com/title/tt0000001',
          filePath: 'chunk1.gcol',
          byteOffset: 0,
          byteLength: 100,
        },
      ];

      const entries2: EntityIndexEntry[] = [
        {
          entityId: 'https://imdb.com/name/nm0000001',
          filePath: 'chunk2.gcol',
          byteOffset: 0,
          byteLength: 200,
        },
      ];

      await manifestStore.putEntityIndex(ns1, entries1);
      await manifestStore.putEntityIndex(ns2, entries2);

      const index1 = await manifestStore.loadEntityIndex(ns1);
      const index2 = await manifestStore.loadEntityIndex(ns2);

      expect(index1.size).toBe(1);
      expect(index2.size).toBe(1);
      expect(index1.has('https://imdb.com/title/tt0000001')).toBe(true);
      expect(index2.has('https://imdb.com/name/nm0000001')).toBe(true);
    });

    it('validates manifest data on read', async () => {
      // Directly insert invalid data into storage
      const invalidStorage = createMockStorage();
      await invalidStorage.put('manifest:https://test.com/:path1', { invalid: 'data' });

      const store = new ManifestStore(invalidStorage);
      const result = await store.getManifest('https://test.com/');

      // Should return null for invalid data rather than crash
      expect(result).toBeNull();
    });

    it('clearMemoryCache clears both caches', async () => {
      await manifestStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);
      await manifestStore.loadEntityIndex(sampleManifest.namespace);

      expect(manifestStore.isNamespaceLoaded(sampleManifest.namespace)).toBe(true);

      manifestStore.clearMemoryCache();

      expect(manifestStore.isNamespaceLoaded(sampleManifest.namespace)).toBe(false);
    });
  });

  // --------------------------------------------------------------------------
  // Configuration
  // --------------------------------------------------------------------------

  describe('configuration', () => {
    it('respects maxEntitiesPerNamespace config', async () => {
      const limitedStore = new ManifestStore(storage, { maxEntitiesPerNamespace: 2 });

      await limitedStore.putEntityIndex(sampleManifest.namespace, sampleEntityIndex);

      const stats = limitedStore.getCacheStats();
      // Should be truncated to 2 entries
      expect(stats.totalCachedEntities).toBe(2);
    });

    it('respects maxCachedNamespaces config', async () => {
      const limitedStore = new ManifestStore(storage, { maxCachedNamespaces: 1 });

      const ns1 = 'https://ns1.com/';
      const ns2 = 'https://ns2.com/';

      await limitedStore.putEntityIndex(ns1, [
        { entityId: 'https://ns1.com/e1', filePath: 'f1', byteOffset: 0, byteLength: 100 },
      ]);
      await limitedStore.putEntityIndex(ns2, [
        { entityId: 'https://ns2.com/e1', filePath: 'f2', byteOffset: 0, byteLength: 100 },
      ]);

      const stats = limitedStore.getCacheStats();
      expect(stats.cachedNamespaces).toBe(1);
    });
  });
});
