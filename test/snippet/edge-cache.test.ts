/**
 * Edge Cache Tests for Snippet Layer
 *
 * TDD RED phase - tests for edge caching bloom filters and index segments.
 * Edge cache is FREE on Cloudflare, making it critical for cost optimization.
 *
 * Key concepts:
 * - Uses caches.default for edge cache access
 * - Cache keys include namespace + version for proper invalidation
 * - Bloom filters cached with short TTL (updates frequently)
 * - Index segments cached with longer TTL (1 hour)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  type EdgeCacheConfig,
  type CachedBloomFilter,
  type CachedIndexSegment,
  EdgeCache,
  createEdgeCacheKey,
  parseEdgeCacheKey,
  DEFAULT_BLOOM_TTL,
  DEFAULT_SEGMENT_TTL,
} from '../../src/snippet/edge-cache';
import { type SerializedFilter, createBloomFilter, serializeFilter, addToFilter } from '../../src/snippet/bloom';
import { type Namespace, createNamespace } from '../../src/core/types';

// Mock the Cloudflare Cache API
const mockCache = {
  match: vi.fn(),
  put: vi.fn(),
  delete: vi.fn(),
};

// Mock caches.default
vi.stubGlobal('caches', {
  default: mockCache,
});

describe('EdgeCache for Bloom Filters', () => {
  let edgeCache: EdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');
  const testVersion = 'v1-abc123';

  beforeEach(() => {
    vi.clearAllMocks();
    edgeCache = new EdgeCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('getBloomFilter', () => {
    it('should return null on cache miss', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await edgeCache.getBloomFilter(testNamespace, testVersion);

      expect(result).toBeNull();
      expect(mockCache.match).toHaveBeenCalledTimes(1);
    });

    it('should return cached bloom filter on cache hit', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      const cachedResponse = new Response(JSON.stringify(serialized), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Version': testVersion,
          'X-Cache-Timestamp': Date.now().toString(),
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await edgeCache.getBloomFilter(testNamespace, testVersion);

      expect(result).not.toBeNull();
      expect(result?.filter).toBeDefined();
      expect(result?.version).toBe(testVersion);
    });

    it('should return null if cached version does not match requested version', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const serialized = serializeFilter(filter);

      const cachedResponse = new Response(JSON.stringify(serialized), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Version': 'old-version',
          'X-Cache-Timestamp': Date.now().toString(),
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await edgeCache.getBloomFilter(testNamespace, testVersion);

      expect(result).toBeNull();
    });

    it('should use correct cache key format including namespace and version', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      await edgeCache.getBloomFilter(testNamespace, testVersion);

      const calledUrl = mockCache.match.mock.calls[0][0];
      expect(calledUrl).toBeInstanceOf(Request);
      expect(calledUrl.url).toContain('bloom');
      expect(calledUrl.url).toContain(encodeURIComponent(testNamespace));
    });
  });

  describe('putBloomFilter', () => {
    it('should cache bloom filter in edge cache on first request', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await edgeCache.putBloomFilter(testNamespace, testVersion, serialized);

      expect(mockCache.put).toHaveBeenCalledTimes(1);

      const [request, response] = mockCache.put.mock.calls[0];
      expect(request).toBeInstanceOf(Request);
      expect(response).toBeInstanceOf(Response);

      // Verify Cache-Control headers for TTL
      const cacheControl = response.headers.get('Cache-Control');
      expect(cacheControl).toContain('max-age=');
    });

    it('should include version in cache metadata', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await edgeCache.putBloomFilter(testNamespace, testVersion, serialized);

      const [, response] = mockCache.put.mock.calls[0];
      expect(response.headers.get('X-Cache-Version')).toBe(testVersion);
    });

    it('should set appropriate TTL for bloom filters', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await edgeCache.putBloomFilter(testNamespace, testVersion, serialized);

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      // Default bloom TTL should be relatively short (e.g., 5 minutes)
      expect(cacheControl).toContain(`max-age=${DEFAULT_BLOOM_TTL}`);
    });

    it('should allow custom TTL override', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const serialized = serializeFilter(filter);
      const customTtl = 600; // 10 minutes

      mockCache.put.mockResolvedValueOnce(undefined);

      await edgeCache.putBloomFilter(testNamespace, testVersion, serialized, { ttl: customTtl });

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      expect(cacheControl).toContain(`max-age=${customTtl}`);
    });
  });

  describe('invalidateBloomFilter', () => {
    it('should delete bloom filter from edge cache', async () => {
      mockCache.delete.mockResolvedValueOnce(true);

      const result = await edgeCache.invalidateBloomFilter(testNamespace);

      expect(result).toBe(true);
      expect(mockCache.delete).toHaveBeenCalledTimes(1);
    });

    it('should return false if cache entry did not exist', async () => {
      mockCache.delete.mockResolvedValueOnce(false);

      const result = await edgeCache.invalidateBloomFilter(testNamespace);

      expect(result).toBe(false);
    });
  });
});

describe('EdgeCache for Index Segments', () => {
  let edgeCache: EdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');
  const testSegmentId = 'segment-001';
  const testVersion = 'v1-xyz789';

  beforeEach(() => {
    vi.clearAllMocks();
    edgeCache = new EdgeCache();
  });

  describe('getIndexSegment', () => {
    it('should return null on cache miss', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await edgeCache.getIndexSegment(testNamespace, testSegmentId, testVersion);

      expect(result).toBeNull();
      expect(mockCache.match).toHaveBeenCalledTimes(1);
    });

    it('should return cached index segment on cache hit', async () => {
      const segmentData = {
        id: testSegmentId,
        entries: [
          { key: 'subject1', positions: [0, 100, 200] },
          { key: 'subject2', positions: [300, 400] },
        ],
        minKey: 'subject1',
        maxKey: 'subject2',
      };

      const cachedResponse = new Response(JSON.stringify(segmentData), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Version': testVersion,
          'X-Segment-Id': testSegmentId,
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await edgeCache.getIndexSegment(testNamespace, testSegmentId, testVersion);

      expect(result).not.toBeNull();
      expect(result?.id).toBe(testSegmentId);
      expect(result?.entries).toHaveLength(2);
    });

    it('should fall back to R2 on cache miss when fallback provided', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const mockR2Fallback = vi.fn().mockResolvedValue({
        id: testSegmentId,
        entries: [{ key: 'subject1', positions: [0] }],
      });

      const result = await edgeCache.getIndexSegment(
        testNamespace,
        testSegmentId,
        testVersion,
        { r2Fallback: mockR2Fallback }
      );

      expect(result).not.toBeNull();
      expect(mockR2Fallback).toHaveBeenCalledWith(testNamespace, testSegmentId);
    });

    it('should populate cache after R2 fallback', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);
      mockCache.put.mockResolvedValueOnce(undefined);

      const segmentFromR2 = {
        id: testSegmentId,
        entries: [{ key: 'subject1', positions: [0] }],
      };

      const mockR2Fallback = vi.fn().mockResolvedValue(segmentFromR2);

      await edgeCache.getIndexSegment(
        testNamespace,
        testSegmentId,
        testVersion,
        { r2Fallback: mockR2Fallback, cacheOnMiss: true }
      );

      expect(mockCache.put).toHaveBeenCalledTimes(1);
    });
  });

  describe('putIndexSegment', () => {
    it('should cache index segments with appropriate TTL', async () => {
      const segmentData = {
        id: testSegmentId,
        entries: [{ key: 'subject1', positions: [0, 100] }],
      };

      mockCache.put.mockResolvedValueOnce(undefined);

      await edgeCache.putIndexSegment(testNamespace, testSegmentId, testVersion, segmentData);

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      // Index segments should have longer TTL (1 hour default)
      expect(cacheControl).toContain(`max-age=${DEFAULT_SEGMENT_TTL}`);
    });

    it('should include segment ID in cache key', async () => {
      const segmentData = {
        id: testSegmentId,
        entries: [],
      };

      mockCache.put.mockResolvedValueOnce(undefined);

      await edgeCache.putIndexSegment(testNamespace, testSegmentId, testVersion, segmentData);

      const [request] = mockCache.put.mock.calls[0];
      expect(request.url).toContain(testSegmentId);
    });
  });

  describe('invalidateIndexSegment', () => {
    it('should delete specific segment from edge cache', async () => {
      mockCache.delete.mockResolvedValueOnce(true);

      const result = await edgeCache.invalidateIndexSegment(testNamespace, testSegmentId);

      expect(result).toBe(true);
      expect(mockCache.delete).toHaveBeenCalledTimes(1);
    });
  });
});

describe('Cache Key Generation', () => {
  it('should create deterministic cache keys', () => {
    const namespace = createNamespace('https://example.com/api/');

    const key1 = createEdgeCacheKey('bloom', namespace);
    const key2 = createEdgeCacheKey('bloom', namespace);

    expect(key1).toBe(key2);
  });

  it('should create different keys for different namespaces', () => {
    const ns1 = createNamespace('https://example.com/api/');
    const ns2 = createNamespace('https://other.com/api/');

    const key1 = createEdgeCacheKey('bloom', ns1);
    const key2 = createEdgeCacheKey('bloom', ns2);

    expect(key1).not.toBe(key2);
  });

  it('should create different keys for different resource types', () => {
    const namespace = createNamespace('https://example.com/api/');

    const bloomKey = createEdgeCacheKey('bloom', namespace);
    const segmentKey = createEdgeCacheKey('segment', namespace, 'seg-001');

    expect(bloomKey).not.toBe(segmentKey);
  });

  it('should include segment ID in segment cache keys', () => {
    const namespace = createNamespace('https://example.com/api/');

    const key = createEdgeCacheKey('segment', namespace, 'segment-001');

    expect(key).toContain('segment-001');
  });

  it('should produce valid URL format for cache keys', () => {
    const namespace = createNamespace('https://example.com/api/');

    const key = createEdgeCacheKey('bloom', namespace);

    // Should be a valid URL that can be used with cache API
    expect(() => new URL(key)).not.toThrow();
  });

  it('should parse cache keys back to components', () => {
    const namespace = createNamespace('https://example.com/api/');
    const segmentId = 'segment-001';

    const key = createEdgeCacheKey('segment', namespace, segmentId);
    const parsed = parseEdgeCacheKey(key);

    expect(parsed.type).toBe('segment');
    expect(parsed.namespace).toBe(namespace);
    expect(parsed.segmentId).toBe(segmentId);
  });
});

describe('Cache-Control Headers', () => {
  let edgeCache: EdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    edgeCache = new EdgeCache();
  });

  it('should set s-maxage for CDN caching', async () => {
    const filter = createBloomFilter({ capacity: 1000 });
    const serialized = serializeFilter(filter);

    mockCache.put.mockResolvedValueOnce(undefined);

    await edgeCache.putBloomFilter(testNamespace, 'v1', serialized);

    const [, response] = mockCache.put.mock.calls[0];
    const cacheControl = response.headers.get('Cache-Control');

    expect(cacheControl).toContain('s-maxage=');
  });

  it('should set public directive for edge caching', async () => {
    const filter = createBloomFilter({ capacity: 1000 });
    const serialized = serializeFilter(filter);

    mockCache.put.mockResolvedValueOnce(undefined);

    await edgeCache.putBloomFilter(testNamespace, 'v1', serialized);

    const [, response] = mockCache.put.mock.calls[0];
    const cacheControl = response.headers.get('Cache-Control');

    expect(cacheControl).toContain('public');
  });

  it('should support stale-while-revalidate for performance', async () => {
    const filter = createBloomFilter({ capacity: 1000 });
    const serialized = serializeFilter(filter);

    mockCache.put.mockResolvedValueOnce(undefined);

    await edgeCache.putBloomFilter(testNamespace, 'v1', serialized, {
      staleWhileRevalidate: 60,
    });

    const [, response] = mockCache.put.mock.calls[0];
    const cacheControl = response.headers.get('Cache-Control');

    expect(cacheControl).toContain('stale-while-revalidate=60');
  });
});

describe('Edge Cache Error Handling', () => {
  let edgeCache: EdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    edgeCache = new EdgeCache();
  });

  it('should handle cache API errors gracefully for get', async () => {
    mockCache.match.mockRejectedValueOnce(new Error('Cache unavailable'));

    // Should not throw, should return null
    const result = await edgeCache.getBloomFilter(testNamespace, 'v1');

    expect(result).toBeNull();
  });

  it('should handle cache API errors gracefully for put', async () => {
    const filter = createBloomFilter({ capacity: 1000 });
    const serialized = serializeFilter(filter);

    mockCache.put.mockRejectedValueOnce(new Error('Cache unavailable'));

    // Should not throw
    await expect(
      edgeCache.putBloomFilter(testNamespace, 'v1', serialized)
    ).resolves.not.toThrow();
  });

  it('should handle malformed cached data', async () => {
    const malformedResponse = new Response('not json', {
      headers: {
        'Content-Type': 'application/json',
        'X-Cache-Version': 'v1',
      },
    });

    mockCache.match.mockResolvedValueOnce(malformedResponse);

    const result = await edgeCache.getBloomFilter(testNamespace, 'v1');

    expect(result).toBeNull();
  });
});

describe('EdgeCache with Custom Configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should accept custom cache configuration', () => {
    const config: EdgeCacheConfig = {
      bloomTtl: 120, // 2 minutes
      segmentTtl: 7200, // 2 hours
      cacheKeyPrefix: 'graphdb-prod',
    };

    const edgeCache = new EdgeCache(config);

    expect(edgeCache.config.bloomTtl).toBe(120);
    expect(edgeCache.config.segmentTtl).toBe(7200);
    expect(edgeCache.config.cacheKeyPrefix).toBe('graphdb-prod');
  });

  it('should use custom prefix in cache keys', async () => {
    const config: EdgeCacheConfig = {
      cacheKeyPrefix: 'custom-prefix',
    };

    const edgeCache = new EdgeCache(config);
    const namespace = createNamespace('https://example.com/api/');

    mockCache.match.mockResolvedValueOnce(undefined);

    await edgeCache.getBloomFilter(namespace, 'v1');

    const [request] = mockCache.match.mock.calls[0];
    expect(request.url).toContain('custom-prefix');
  });
});
