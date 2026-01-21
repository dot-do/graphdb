/**
 * Advanced Caching Tests for Broker
 *
 * TDD RED phase - tests for advanced caching patterns.
 * These tests define expected behavior for:
 * - Stale-while-revalidate pattern
 * - Cache warming strategies
 * - TTL-based invalidation
 * - Cache coherence across broker instances
 * - Optimistic caching with validation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  BrokerEdgeCache,
  type CacheableRequest,
  type CacheableResponse,
  type CachedResponse,
  createCacheTagsForQuery,
  shouldCacheResponse,
} from '../../src/broker/edge-cache';
import { createNamespace, type Namespace } from '../../src/core/types';

// Mock Cloudflare Cache API
const mockCache = {
  match: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
};

vi.stubGlobal('caches', {
  default: mockCache,
});

describe('Stale-While-Revalidate', () => {
  let brokerCache: BrokerEdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    brokerCache = new BrokerEdgeCache();
  });

  describe('Serving stale content', () => {
    it('should return stale data immediately while revalidating in background', async () => {
      const staleData = { $id: 'https://example.com/1', name: 'Stale Name', version: 1 };
      const freshData = { $id: 'https://example.com/1', name: 'Fresh Name', version: 2 };

      const staleTimestamp = Date.now() - 120000; // 2 minutes ago
      const staleCachedResponse = new Response(JSON.stringify(staleData), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Timestamp': staleTimestamp.toString(),
          'Cache-Control': 'max-age=60, stale-while-revalidate=300',
        },
      });

      mockCache.match.mockResolvedValueOnce(staleCachedResponse);

      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/1',
        cacheKey: 'query-swr-1',
      };

      // Mock the revalidation fetch
      const revalidateFn = vi.fn(async () => freshData);

      const result = await brokerCache.checkCacheWithSWR(request, revalidateFn);

      // Should return stale data immediately
      expect(result?.data).toEqual(staleData);
      expect(result?.metadata.isStale).toBe(true);

      // Wait for background revalidation
      await new Promise(resolve => setTimeout(resolve, 50));

      // Cache should have been updated with fresh data
      expect(mockCache.put).toHaveBeenCalled();
      expect(revalidateFn).toHaveBeenCalled();
    });

    it('should return fresh data when cache is not stale', async () => {
      const freshData = { $id: 'https://example.com/1', name: 'Fresh Name' };

      const freshTimestamp = Date.now() - 10000; // 10 seconds ago
      const freshCachedResponse = new Response(JSON.stringify(freshData), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Timestamp': freshTimestamp.toString(),
          'Cache-Control': 'max-age=60, stale-while-revalidate=300',
        },
      });

      mockCache.match.mockResolvedValueOnce(freshCachedResponse);

      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/1',
        cacheKey: 'query-fresh-1',
      };

      const revalidateFn = vi.fn();

      const result = await brokerCache.checkCacheWithSWR(request, revalidateFn);

      // Should return fresh data
      expect(result?.data).toEqual(freshData);
      expect(result?.metadata.isStale).toBe(false);

      // Should NOT trigger revalidation
      expect(revalidateFn).not.toHaveBeenCalled();
    });

    it('should fetch fresh data when cache entry is beyond SWR window', async () => {
      const veryStaleData = { $id: 'https://example.com/1', name: 'Very Stale' };
      const freshData = { $id: 'https://example.com/1', name: 'Fresh' };

      const veryStaleTimestamp = Date.now() - 600000; // 10 minutes ago (beyond SWR window)
      const staleCachedResponse = new Response(JSON.stringify(veryStaleData), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Timestamp': veryStaleTimestamp.toString(),
          'Cache-Control': 'max-age=60, stale-while-revalidate=300',
        },
      });

      mockCache.match.mockResolvedValueOnce(staleCachedResponse);

      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/1',
        cacheKey: 'query-very-stale-1',
      };

      const revalidateFn = vi.fn(async () => freshData);

      const result = await brokerCache.checkCacheWithSWR(request, revalidateFn);

      // Should NOT return stale data (too old)
      expect(result?.data).toEqual(freshData);
      expect(result?.metadata.isStale).toBe(false);

      // Should have fetched fresh data synchronously
      expect(revalidateFn).toHaveBeenCalled();
    });
  });

  describe('Revalidation handling', () => {
    it('should update cache on successful revalidation', async () => {
      const staleData = { $id: 'https://example.com/1', value: 'stale' };
      const freshData = { $id: 'https://example.com/1', value: 'fresh' };

      const staleTimestamp = Date.now() - 90000; // 1.5 minutes ago
      mockCache.match.mockResolvedValueOnce(
        new Response(JSON.stringify(staleData), {
          headers: {
            'X-Cache-Timestamp': staleTimestamp.toString(),
            'Cache-Control': 'max-age=60, stale-while-revalidate=300',
          },
        })
      );

      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/1',
        cacheKey: 'query-revalidate',
      };

      const revalidateFn = vi.fn(async () => freshData);

      await brokerCache.checkCacheWithSWR(request, revalidateFn);

      // Wait for background revalidation
      await new Promise(resolve => setTimeout(resolve, 100));

      // Cache should have been updated
      expect(mockCache.put).toHaveBeenCalled();
      const [, response] = mockCache.put.mock.calls[0];
      const cachedData = await response.json();
      expect(cachedData).toEqual(freshData);
    });

    it('should keep stale cache on revalidation failure', async () => {
      const staleData = { $id: 'https://example.com/1', value: 'stale' };

      const staleTimestamp = Date.now() - 90000;
      mockCache.match.mockResolvedValueOnce(
        new Response(JSON.stringify(staleData), {
          headers: {
            'X-Cache-Timestamp': staleTimestamp.toString(),
            'Cache-Control': 'max-age=60, stale-while-revalidate=300',
          },
        })
      );

      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/1',
        cacheKey: 'query-fail-revalidate',
      };

      const revalidateFn = vi.fn(async () => {
        throw new Error('Revalidation failed');
      });

      const result = await brokerCache.checkCacheWithSWR(request, revalidateFn);

      // Should still return stale data
      expect(result?.data).toEqual(staleData);

      // Wait for background revalidation attempt
      await new Promise(resolve => setTimeout(resolve, 100));

      // Cache should NOT have been updated (revalidation failed)
      expect(mockCache.put).not.toHaveBeenCalled();
    });
  });
});

describe('Cache Warming', () => {
  let brokerCache: BrokerEdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    brokerCache = new BrokerEdgeCache();
  });

  describe('Preemptive warming', () => {
    it('should warm cache with predicted queries', async () => {
      const queries = [
        'https://example.com/api/user/1',
        'https://example.com/api/user/2',
        'https://example.com/api/user/3',
      ];

      const fetchFn = vi.fn(async (query: string) => ({
        $id: query,
        name: `User for ${query}`,
      }));

      mockCache.put.mockResolvedValue(undefined);

      await brokerCache.warmCache(testNamespace, queries, fetchFn);

      // All queries should have been fetched
      expect(fetchFn).toHaveBeenCalledTimes(3);

      // All results should have been cached
      expect(mockCache.put).toHaveBeenCalledTimes(3);
    });

    it('should skip warming for queries already in cache', async () => {
      const queries = [
        'https://example.com/api/user/1',
        'https://example.com/api/user/2',
      ];

      // First query is already cached
      mockCache.match.mockImplementation(async (request: Request) => {
        if (request.url.includes('user/1')) {
          return new Response(JSON.stringify({ $id: 'user/1' }), { status: 200 });
        }
        return undefined;
      });

      const fetchFn = vi.fn(async (query: string) => ({
        $id: query,
        name: `User for ${query}`,
      }));

      mockCache.put.mockResolvedValue(undefined);

      await brokerCache.warmCache(testNamespace, queries, fetchFn, { skipCached: true });

      // Only the uncached query should have been fetched
      expect(fetchFn).toHaveBeenCalledTimes(1);
      expect(fetchFn).toHaveBeenCalledWith('https://example.com/api/user/2');
    });

    it('should batch warming requests efficiently', async () => {
      const queries = Array.from({ length: 100 }, (_, i) =>
        `https://example.com/api/entity/${i}`
      );

      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const fetchFn = vi.fn(async (query: string) => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        await new Promise(resolve => setTimeout(resolve, 10));
        currentConcurrent--;
        return { $id: query };
      });

      mockCache.put.mockResolvedValue(undefined);
      mockCache.match.mockResolvedValue(undefined);

      await brokerCache.warmCache(testNamespace, queries, fetchFn, { maxConcurrency: 10 });

      // Should not exceed max concurrency
      expect(maxConcurrent).toBeLessThanOrEqual(10);
    });
  });

  describe('Conditional warming', () => {
    it('should warm cache based on access patterns', async () => {
      const accessLog = [
        { query: 'https://example.com/api/popular', count: 100 },
        { query: 'https://example.com/api/medium', count: 50 },
        { query: 'https://example.com/api/rare', count: 5 },
      ];

      const fetchFn = vi.fn(async (query: string) => ({ $id: query }));
      mockCache.put.mockResolvedValue(undefined);
      mockCache.match.mockResolvedValue(undefined);

      await brokerCache.warmCacheByAccessPattern(testNamespace, accessLog, fetchFn, {
        minAccessCount: 20,
      });

      // Should only warm frequently accessed queries
      expect(fetchFn).toHaveBeenCalledTimes(2);
      expect(fetchFn).toHaveBeenCalledWith('https://example.com/api/popular');
      expect(fetchFn).toHaveBeenCalledWith('https://example.com/api/medium');
      expect(fetchFn).not.toHaveBeenCalledWith('https://example.com/api/rare');
    });
  });
});

describe('TTL-Based Invalidation', () => {
  let brokerCache: BrokerEdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    brokerCache = new BrokerEdgeCache();
  });

  describe('TTL configuration', () => {
    it('should set correct TTL based on response type', async () => {
      mockCache.put.mockResolvedValue(undefined);

      const staticRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/static-data',
        cacheKey: 'static',
        responseType: 'static',
      };

      const dynamicRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/dynamic-data',
        cacheKey: 'dynamic',
        responseType: 'dynamic',
      };

      await brokerCache.populateCache(staticRequest, { data: 'static' });
      await brokerCache.populateCache(dynamicRequest, { data: 'dynamic' });

      const [, staticResponse] = mockCache.put.mock.calls[0];
      const [, dynamicResponse] = mockCache.put.mock.calls[1];

      const staticCacheControl = staticResponse.headers.get('Cache-Control');
      const dynamicCacheControl = dynamicResponse.headers.get('Cache-Control');

      // Static data should have longer TTL
      expect(staticCacheControl).toContain('max-age=3600'); // 1 hour
      expect(dynamicCacheControl).toContain('max-age=300'); // 5 minutes
    });

    it('should respect per-entity TTL hints', async () => {
      mockCache.put.mockResolvedValue(undefined);

      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/entity',
        cacheKey: 'entity-ttl',
        ttl: 120, // 2 minutes
      };

      await brokerCache.populateCache(request, { data: 'test' });

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      expect(cacheControl).toContain('max-age=120');
    });
  });

  describe('Invalidation on mutation', () => {
    it('should invalidate related cache entries on mutation', async () => {
      mockCache.delete.mockResolvedValue(true);

      const mutation = {
        type: 'mutation',
        entityId: 'https://example.com/api/user/123',
        operation: 'update',
        affectedTags: ['entity:user/123', 'ns:example.com/api/'],
      };

      const result = await brokerCache.invalidateOnMutation(mutation);

      expect(result.success).toBe(true);
      expect(result.invalidatedCount).toBeGreaterThan(0);
    });

    it('should cascade invalidation to dependent queries', async () => {
      mockCache.delete.mockResolvedValue(true);

      // When user/123 is mutated, queries that depend on it should be invalidated
      const mutation = {
        type: 'mutation',
        entityId: 'https://example.com/api/user/123',
        operation: 'update',
        cascadeInvalidation: true,
      };

      const dependentQueries = [
        'https://example.com/api/user/123.friends',
        'https://example.com/api/user/123.posts',
        'https://example.com/api/users?limit=10', // List that includes this user
      ];

      await brokerCache.invalidateOnMutation(mutation);

      // All dependent queries should have been invalidated
      for (const query of dependentQueries) {
        expect(mockCache.delete).toHaveBeenCalledWith(
          expect.objectContaining({
            url: expect.stringContaining(query),
          })
        );
      }
    });
  });
});

describe('Cache Coherence', () => {
  describe('Cross-instance coherence', () => {
    it('should propagate invalidation to all broker instances', async () => {
      const broadcastFn = vi.fn();
      const brokerCache = new BrokerEdgeCache({
        onInvalidation: broadcastFn,
      });

      mockCache.delete.mockResolvedValue(true);

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/1',
        cacheKey: 'coherence-test',
        cacheTags: ['tag1', 'tag2'],
      };

      await brokerCache.invalidateEntry(request);

      // Should broadcast invalidation event
      expect(broadcastFn).toHaveBeenCalledWith({
        type: 'invalidation',
        cacheKey: 'coherence-test',
        tags: ['tag1', 'tag2'],
      });
    });

    it('should handle invalidation from other instances', async () => {
      const brokerCache = new BrokerEdgeCache();

      mockCache.delete.mockResolvedValue(true);

      const invalidationEvent = {
        type: 'invalidation',
        cacheKey: 'remote-key',
        tags: ['remote-tag'],
        sourceInstance: 'broker-2',
      };

      await brokerCache.handleRemoteInvalidation(invalidationEvent);

      // Should delete local cache entry
      expect(mockCache.delete).toHaveBeenCalled();
    });
  });

  describe('Version-based coherence', () => {
    it('should validate cache entries against version', async () => {
      const cachedData = {
        $id: 'https://example.com/1',
        name: 'Cached',
        _version: 5,
      };

      mockCache.match.mockResolvedValueOnce(
        new Response(JSON.stringify(cachedData), {
          headers: {
            'X-Version': '5',
          },
        })
      );

      const brokerCache = new BrokerEdgeCache();

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/1',
        cacheKey: 'version-check',
        expectedVersion: 6, // Newer version expected
      };

      const result = await brokerCache.checkCache(request);

      // Should return null (cache is outdated)
      expect(result).toBeNull();
    });

    it('should return cached data when version matches', async () => {
      const cachedData = {
        $id: 'https://example.com/1',
        name: 'Cached',
        _version: 5,
      };

      mockCache.match.mockResolvedValueOnce(
        new Response(JSON.stringify(cachedData), {
          headers: {
            'X-Version': '5',
          },
        })
      );

      const brokerCache = new BrokerEdgeCache();

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/1',
        cacheKey: 'version-match',
        expectedVersion: 5, // Same version
      };

      const result = await brokerCache.checkCache(request);

      expect(result?.data).toEqual(cachedData);
    });
  });
});

describe('Optimistic Caching', () => {
  describe('Optimistic updates', () => {
    it('should cache optimistic response before confirmation', async () => {
      const brokerCache = new BrokerEdgeCache();

      mockCache.put.mockResolvedValue(undefined);

      const optimisticData = {
        $id: 'https://example.com/new-entity',
        name: 'New Entity',
        _optimistic: true,
      };

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/new-entity',
        cacheKey: 'optimistic-create',
      };

      await brokerCache.populateOptimistic(request, optimisticData);

      expect(mockCache.put).toHaveBeenCalled();
      const [, response] = mockCache.put.mock.calls[0];
      const headers = response.headers;

      // Should be marked as optimistic
      expect(headers.get('X-Optimistic')).toBe('true');
    });

    it('should confirm optimistic cache on success', async () => {
      const brokerCache = new BrokerEdgeCache();

      mockCache.put.mockResolvedValue(undefined);

      const confirmedData = {
        $id: 'https://example.com/new-entity',
        name: 'New Entity',
        _version: 1,
      };

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/new-entity',
        cacheKey: 'optimistic-confirm',
      };

      await brokerCache.confirmOptimistic(request, confirmedData);

      expect(mockCache.put).toHaveBeenCalled();
      const [, response] = mockCache.put.mock.calls[0];
      const headers = response.headers;

      // Should no longer be marked as optimistic
      expect(headers.get('X-Optimistic')).toBeNull();
    });

    it('should rollback optimistic cache on failure', async () => {
      const brokerCache = new BrokerEdgeCache();

      mockCache.delete.mockResolvedValue(true);

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/new-entity',
        cacheKey: 'optimistic-rollback',
      };

      await brokerCache.rollbackOptimistic(request);

      expect(mockCache.delete).toHaveBeenCalled();
    });
  });

  describe('Optimistic conflict resolution', () => {
    it('should detect conflicts between optimistic and confirmed data', async () => {
      const brokerCache = new BrokerEdgeCache();

      const optimisticData = {
        $id: 'https://example.com/entity',
        name: 'Optimistic Name',
        _version: 1,
        _optimistic: true,
      };

      const serverData = {
        $id: 'https://example.com/entity',
        name: 'Server Name', // Different value
        _version: 2, // Server has newer version
      };

      mockCache.match.mockResolvedValueOnce(
        new Response(JSON.stringify(optimisticData), {
          headers: { 'X-Optimistic': 'true' },
        })
      );

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/entity',
        cacheKey: 'conflict-detect',
      };

      const conflict = await brokerCache.checkConflict(request, serverData);

      expect(conflict.hasConflict).toBe(true);
      expect(conflict.optimisticValue).toBe('Optimistic Name');
      expect(conflict.serverValue).toBe('Server Name');
    });

    it('should resolve conflicts using specified strategy', async () => {
      const brokerCache = new BrokerEdgeCache();

      mockCache.put.mockResolvedValue(undefined);

      const optimisticData = {
        $id: 'https://example.com/entity',
        name: 'Optimistic Name',
        _version: 1,
      };

      const serverData = {
        $id: 'https://example.com/entity',
        name: 'Server Name',
        _version: 2,
      };

      const request: CacheableRequest = {
        type: 'query',
        namespace: createNamespace('https://example.com/api/'),
        query: 'https://example.com/api/entity',
        cacheKey: 'conflict-resolve',
      };

      // Use 'server-wins' strategy
      await brokerCache.resolveConflict(request, optimisticData, serverData, 'server-wins');

      const [, response] = mockCache.put.mock.calls[0];
      const cachedData = await response.json();

      expect(cachedData.name).toBe('Server Name');
    });
  });
});

describe('Cache Metrics', () => {
  describe('Hit/miss tracking', () => {
    it('should track cache hit rate', async () => {
      const brokerCache = new BrokerEdgeCache();

      const namespace = createNamespace('https://example.com/api/');

      // Simulate some hits and misses
      mockCache.match
        .mockResolvedValueOnce(new Response(JSON.stringify({ data: 1 })))
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(new Response(JSON.stringify({ data: 2 })))
        .mockResolvedValueOnce(new Response(JSON.stringify({ data: 3 })))
        .mockResolvedValueOnce(undefined);

      for (let i = 0; i < 5; i++) {
        await brokerCache.checkCache({
          type: 'query',
          namespace,
          query: `https://example.com/api/${i}`,
          cacheKey: `query-${i}`,
        });
      }

      const metrics = brokerCache.getMetrics();

      expect(metrics.totalRequests).toBe(5);
      expect(metrics.hits).toBe(3);
      expect(metrics.misses).toBe(2);
      expect(metrics.hitRate).toBeCloseTo(0.6);
    });

    it('should track cache size', async () => {
      const brokerCache = new BrokerEdgeCache();

      mockCache.put.mockResolvedValue(undefined);

      const namespace = createNamespace('https://example.com/api/');

      for (let i = 0; i < 10; i++) {
        await brokerCache.populateCache(
          {
            type: 'query',
            namespace,
            query: `https://example.com/api/${i}`,
            cacheKey: `query-${i}`,
          },
          { data: 'x'.repeat(1000) } // 1KB each
        );
      }

      const metrics = brokerCache.getMetrics();

      expect(metrics.entriesCount).toBe(10);
      expect(metrics.approximateSize).toBeGreaterThan(10000);
    });
  });
});
