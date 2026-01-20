/**
 * Cache Invalidation Tests for GraphDB
 *
 * TDD RED phase - tests for cache invalidation/purge logic.
 * Following the design from pocs-s0ks:
 * - Purge cache on chunk updates
 * - Purge cache on compaction
 * - Batch invalidation for efficiency
 *
 * Key scenarios:
 * - Single chunk invalidation
 * - Namespace-wide invalidation
 * - Compaction-triggered invalidation (old chunks removed)
 * - Manifest invalidation on chunk list changes
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  type InvalidationConfig,
  type InvalidationResult,
  type CompactionInvalidationEvent,
  CacheInvalidator,
  createInvalidationTags,
  createNamespaceTags,
  createChunkTag,
} from '../../src/cache/cache-invalidation';
import { type Namespace, createNamespace } from '../../src/core/types';

// Mock the Cloudflare Cache API
const mockCache = {
  match: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
};

vi.stubGlobal('caches', {
  default: mockCache,
});

describe('CacheInvalidator', () => {
  let invalidator: CacheInvalidator;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    invalidator = new CacheInvalidator();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('invalidateChunk', () => {
    it('should delete a specific chunk from cache', async () => {
      mockCache.delete.mockResolvedValueOnce(true);

      const result = await invalidator.invalidateChunk(testNamespace, 'chunk-001');

      expect(result.success).toBe(true);
      expect(result.invalidatedCount).toBe(1);
      expect(mockCache.delete).toHaveBeenCalledTimes(1);
    });

    it('should return success=true even if chunk was not in cache', async () => {
      mockCache.delete.mockResolvedValueOnce(false);

      const result = await invalidator.invalidateChunk(testNamespace, 'chunk-001');

      expect(result.success).toBe(true);
      expect(result.invalidatedCount).toBe(0);
    });

    it('should include chunk ID in invalidated list', async () => {
      mockCache.delete.mockResolvedValueOnce(true);

      const result = await invalidator.invalidateChunk(testNamespace, 'chunk-001');

      expect(result.invalidatedKeys).toContain('chunk-001');
    });
  });

  describe('invalidateChunks (batch)', () => {
    it('should invalidate multiple chunks in one call', async () => {
      const chunkIds = ['chunk-001', 'chunk-002', 'chunk-003'];
      mockCache.delete.mockResolvedValue(true);

      const result = await invalidator.invalidateChunks(testNamespace, chunkIds);

      expect(result.success).toBe(true);
      expect(result.invalidatedCount).toBe(3);
      expect(mockCache.delete).toHaveBeenCalledTimes(3);
    });

    it('should continue invalidating even if some fail', async () => {
      const chunkIds = ['chunk-001', 'chunk-002', 'chunk-003'];
      mockCache.delete
        .mockResolvedValueOnce(true)
        .mockRejectedValueOnce(new Error('Delete failed'))
        .mockResolvedValueOnce(true);

      const result = await invalidator.invalidateChunks(testNamespace, chunkIds);

      expect(result.success).toBe(false); // Some failures
      expect(result.invalidatedCount).toBe(2);
      expect(result.errors).toHaveLength(1);
    });

    it('should handle empty chunk list gracefully', async () => {
      const result = await invalidator.invalidateChunks(testNamespace, []);

      expect(result.success).toBe(true);
      expect(result.invalidatedCount).toBe(0);
    });
  });

  describe('invalidateNamespace', () => {
    it('should invalidate manifest for namespace', async () => {
      mockCache.delete.mockResolvedValue(true);

      const result = await invalidator.invalidateNamespace(testNamespace);

      expect(result.success).toBe(true);
      // Should at least delete the manifest
      expect(mockCache.delete).toHaveBeenCalled();
    });

    it('should optionally invalidate all known chunks', async () => {
      mockCache.delete.mockResolvedValue(true);

      const chunkIds = ['chunk-001', 'chunk-002'];
      const result = await invalidator.invalidateNamespace(testNamespace, {
        includeChunks: chunkIds,
      });

      expect(result.success).toBe(true);
      // Manifest + 2 chunks
      expect(mockCache.delete).toHaveBeenCalledTimes(3);
    });
  });

  describe('onCompaction', () => {
    it('should invalidate old chunks that were compacted', async () => {
      mockCache.delete.mockResolvedValue(true);

      const event: CompactionInvalidationEvent = {
        namespace: testNamespace,
        sourceChunks: ['chunk-001', 'chunk-002', 'chunk-003'],
        targetChunk: 'chunk-compacted-001',
        timestamp: Date.now(),
      };

      const result = await invalidator.onCompaction(event);

      expect(result.success).toBe(true);
      // Should invalidate all source chunks (they no longer exist)
      expect(result.invalidatedCount).toBeGreaterThanOrEqual(3);
    });

    it('should invalidate the manifest after compaction', async () => {
      mockCache.delete.mockResolvedValue(true);

      const event: CompactionInvalidationEvent = {
        namespace: testNamespace,
        sourceChunks: ['chunk-001'],
        targetChunk: 'chunk-compacted-001',
        timestamp: Date.now(),
      };

      await invalidator.onCompaction(event);

      // Verify manifest cache key was deleted
      const deleteCalls = mockCache.delete.mock.calls;
      const manifestDeleted = deleteCalls.some(
        (call) => call[0].url && call[0].url.includes('manifest')
      );
      expect(manifestDeleted).toBe(true);
    });

    it('should track compaction events for metrics', async () => {
      mockCache.delete.mockResolvedValue(true);
      const metricsInvalidator = new CacheInvalidator({ trackMetrics: true });

      const event: CompactionInvalidationEvent = {
        namespace: testNamespace,
        sourceChunks: ['chunk-001'],
        targetChunk: 'chunk-compacted-001',
        timestamp: Date.now(),
      };

      await metricsInvalidator.onCompaction(event);

      const metrics = metricsInvalidator.getMetrics();
      expect(metrics.compactionEvents).toBe(1);
    });
  });
});

describe('Cache Tag Generation', () => {
  const testNamespace = createNamespace('https://example.com/graphdb/');

  describe('createChunkTag', () => {
    it('should create a unique tag for a chunk', () => {
      const tag = createChunkTag(testNamespace, 'chunk-001');

      expect(tag).toContain('chunk-001');
      expect(typeof tag).toBe('string');
    });

    it('should create different tags for different chunks', () => {
      const tag1 = createChunkTag(testNamespace, 'chunk-001');
      const tag2 = createChunkTag(testNamespace, 'chunk-002');

      expect(tag1).not.toBe(tag2);
    });

    it('should include namespace in tag for isolation', () => {
      const ns1 = createNamespace('https://example.com/ns1/');
      const ns2 = createNamespace('https://example.com/ns2/');

      const tag1 = createChunkTag(ns1, 'chunk-001');
      const tag2 = createChunkTag(ns2, 'chunk-001');

      expect(tag1).not.toBe(tag2);
    });
  });

  describe('createNamespaceTags', () => {
    it('should create tags for namespace-level invalidation', () => {
      const tags = createNamespaceTags(testNamespace);

      expect(Array.isArray(tags)).toBe(true);
      expect(tags.length).toBeGreaterThan(0);
    });

    it('should include host-level tag for broad invalidation', () => {
      const tags = createNamespaceTags(testNamespace);

      const hasHostTag = tags.some((tag) => tag.startsWith('host:'));
      expect(hasHostTag).toBe(true);
    });
  });

  describe('createInvalidationTags', () => {
    it('should create tags for a specific chunk', () => {
      const tags = createInvalidationTags(testNamespace, 'chunk-001');

      expect(tags).toContain(createChunkTag(testNamespace, 'chunk-001'));
    });

    it('should include namespace tags for broader invalidation', () => {
      const tags = createInvalidationTags(testNamespace, 'chunk-001');
      const nsTags = createNamespaceTags(testNamespace);

      for (const nsTag of nsTags) {
        expect(tags).toContain(nsTag);
      }
    });
  });
});

describe('Invalidation Configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should accept custom configuration', () => {
    const config: InvalidationConfig = {
      batchSize: 100,
      retryAttempts: 3,
      trackMetrics: true,
    };

    const invalidator = new CacheInvalidator(config);

    expect(invalidator.config.batchSize).toBe(100);
    expect(invalidator.config.retryAttempts).toBe(3);
  });

  it('should use default values when not specified', () => {
    const invalidator = new CacheInvalidator();

    expect(invalidator.config.batchSize).toBeGreaterThan(0);
    expect(invalidator.config.retryAttempts).toBeGreaterThanOrEqual(0);
  });
});

describe('Invalidation Error Handling', () => {
  let invalidator: CacheInvalidator;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    invalidator = new CacheInvalidator();
  });

  it('should handle cache API errors gracefully', async () => {
    mockCache.delete.mockRejectedValueOnce(new Error('Cache unavailable'));

    const result = await invalidator.invalidateChunk(testNamespace, 'chunk-001');

    expect(result.success).toBe(false);
    expect(result.errors).toHaveLength(1);
    expect(result.errors![0]).toContain('Cache unavailable');
  });

  it('should retry on transient failures if configured', async () => {
    const invalidatorWithRetry = new CacheInvalidator({ retryAttempts: 2 });

    mockCache.delete
      .mockRejectedValueOnce(new Error('Temporary error'))
      .mockResolvedValueOnce(true);

    const result = await invalidatorWithRetry.invalidateChunk(testNamespace, 'chunk-001');

    expect(result.success).toBe(true);
    expect(mockCache.delete).toHaveBeenCalledTimes(2);
  });
});

describe('Invalidation Metrics', () => {
  let invalidator: CacheInvalidator;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    invalidator = new CacheInvalidator({ trackMetrics: true });
  });

  it('should track total invalidations', async () => {
    mockCache.delete.mockResolvedValue(true);

    await invalidator.invalidateChunk(testNamespace, 'chunk-001');
    await invalidator.invalidateChunk(testNamespace, 'chunk-002');

    const metrics = invalidator.getMetrics();
    expect(metrics.totalInvalidations).toBe(2);
  });

  it('should track failed invalidations', async () => {
    mockCache.delete
      .mockResolvedValueOnce(true)
      .mockRejectedValueOnce(new Error('Failed'));

    await invalidator.invalidateChunk(testNamespace, 'chunk-001');
    await invalidator.invalidateChunk(testNamespace, 'chunk-002');

    const metrics = invalidator.getMetrics();
    expect(metrics.failedInvalidations).toBe(1);
  });

  it('should track namespace invalidations separately', async () => {
    mockCache.delete.mockResolvedValue(true);

    await invalidator.invalidateNamespace(testNamespace);

    const metrics = invalidator.getMetrics();
    expect(metrics.namespaceInvalidations).toBe(1);
  });

  it('should reset metrics when requested', async () => {
    mockCache.delete.mockResolvedValue(true);

    await invalidator.invalidateChunk(testNamespace, 'chunk-001');
    invalidator.resetMetrics();

    const metrics = invalidator.getMetrics();
    expect(metrics.totalInvalidations).toBe(0);
  });
});

describe('Compaction Integration', () => {
  let invalidator: CacheInvalidator;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    invalidator = new CacheInvalidator();
  });

  it('should handle L0 to L1 compaction invalidation', async () => {
    mockCache.delete.mockResolvedValue(true);

    const event: CompactionInvalidationEvent = {
      namespace: testNamespace,
      sourceChunks: ['wal-001', 'wal-002', 'wal-003', 'wal-004', 'wal-005'],
      targetChunk: 'l1-compacted-001',
      timestamp: Date.now(),
      level: 'L0_TO_L1',
    };

    const result = await invalidator.onCompaction(event);

    expect(result.success).toBe(true);
    // All source WAL chunks should be invalidated
    expect(result.invalidatedKeys).toContain('wal-001');
    expect(result.invalidatedKeys).toContain('wal-005');
  });

  it('should handle L1 to L2 compaction invalidation', async () => {
    mockCache.delete.mockResolvedValue(true);

    const event: CompactionInvalidationEvent = {
      namespace: testNamespace,
      sourceChunks: ['l1-001', 'l1-002'],
      targetChunk: 'l2-compacted-001',
      timestamp: Date.now(),
      level: 'L1_TO_L2',
    };

    const result = await invalidator.onCompaction(event);

    expect(result.success).toBe(true);
    expect(result.invalidatedKeys).toContain('l1-001');
    expect(result.invalidatedKeys).toContain('l1-002');
  });
});
