/**
 * Bloom Filter Edge Cache Integration Tests
 *
 * TDD RED phase - tests for wiring edge cache to bloom filter layer.
 * Bloom filters should be cached at edge with immutable headers since they rarely change.
 *
 * Key requirements:
 * - Bloom filters cached with immutable headers (1 year TTL)
 * - Cache-Control: public, max-age=31536000, immutable
 * - Content-addressed caching using filter version/hash
 * - BloomRouter integrates EdgeCache with bloom filter operations
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  type ImmutableBloomCacheConfig,
  type BloomCacheEntry,
  ImmutableBloomCache,
  createBloomCacheKey,
  generateBloomCacheHeaders,
  DEFAULT_IMMUTABLE_MAX_AGE,
} from '../../src/snippet/bloom-cache';
import {
  BloomRouter,
  type BloomRouterConfig,
  type BloomRouteResult,
} from '../../src/snippet/bloom-router';
import {
  createBloomFilter,
  addToFilter,
  mightExist,
  serializeFilter,
  deserializeFilter,
  type SerializedFilter,
} from '../../src/snippet/bloom';
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

describe('ImmutableBloomCache', () => {
  let bloomCache: ImmutableBloomCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    bloomCache = new ImmutableBloomCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('getFilter', () => {
    it('should return null on cache miss', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await bloomCache.getFilter(testNamespace, 'v1-abc123');

      expect(result).toBeNull();
      expect(mockCache.match).toHaveBeenCalledTimes(1);
    });

    it('should return cached bloom filter on cache hit', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: 'v1-abc123' });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      const cachedResponse = new Response(JSON.stringify(serialized), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=31536000, immutable',
          'X-Bloom-Version': 'v1-abc123',
          'X-Cache-Timestamp': Date.now().toString(),
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await bloomCache.getFilter(testNamespace, 'v1-abc123');

      expect(result).not.toBeNull();
      expect(result?.filter).toBeDefined();
      expect(result?.version).toBe('v1-abc123');
    });

    it('should use content-addressed cache key', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      await bloomCache.getFilter(testNamespace, 'v1-abc123');

      const [request] = mockCache.match.mock.calls[0];
      expect(request).toBeInstanceOf(Request);
      // Key should include both namespace and version
      expect(request.url).toContain('bloom');
      expect(request.url).toContain(encodeURIComponent(testNamespace));
      expect(request.url).toContain('v1-abc123');
    });
  });

  describe('putFilter', () => {
    it('should cache bloom filter with immutable headers', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: 'v1-abc123' });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await bloomCache.putFilter(testNamespace, 'v1-abc123', serialized);

      expect(mockCache.put).toHaveBeenCalledTimes(1);

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      expect(cacheControl).toContain('public');
      expect(cacheControl).toContain('max-age=31536000');
      expect(cacheControl).toContain('immutable');
    });

    it('should include bloom version in response headers', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: 'v1-abc123' });
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await bloomCache.putFilter(testNamespace, 'v1-abc123', serialized);

      const [, response] = mockCache.put.mock.calls[0];
      expect(response.headers.get('X-Bloom-Version')).toBe('v1-abc123');
    });

    it('should set JSON content type', async () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await bloomCache.putFilter(testNamespace, 'v1', serialized);

      const [, response] = mockCache.put.mock.calls[0];
      expect(response.headers.get('Content-Type')).toBe('application/json');
    });
  });

  describe('invalidateFilter', () => {
    it('should delete specific version from cache', async () => {
      mockCache.delete.mockResolvedValueOnce(true);

      const result = await bloomCache.invalidateFilter(testNamespace, 'v1-abc123');

      expect(result).toBe(true);
      expect(mockCache.delete).toHaveBeenCalledTimes(1);
    });
  });
});

describe('generateBloomCacheHeaders', () => {
  it('should generate immutable cache headers', () => {
    const headers = generateBloomCacheHeaders('v1-abc123');

    expect(headers['Cache-Control']).toContain('public');
    expect(headers['Cache-Control']).toContain('max-age=31536000');
    expect(headers['Cache-Control']).toContain('immutable');
    expect(headers['X-Bloom-Version']).toBe('v1-abc123');
    expect(headers['Content-Type']).toBe('application/json');
  });

  it('should allow custom max-age override', () => {
    const headers = generateBloomCacheHeaders('v1', { maxAge: 86400 });

    expect(headers['Cache-Control']).toContain('max-age=86400');
    expect(headers['Cache-Control']).toContain('immutable');
  });

  it('should allow disabling immutable directive', () => {
    const headers = generateBloomCacheHeaders('v1', { immutable: false });

    expect(headers['Cache-Control']).not.toContain('immutable');
  });
});

describe('createBloomCacheKey', () => {
  it('should create deterministic cache keys', () => {
    const namespace = createNamespace('https://example.com/api/');

    const key1 = createBloomCacheKey(namespace, 'v1-abc123');
    const key2 = createBloomCacheKey(namespace, 'v1-abc123');

    expect(key1).toBe(key2);
  });

  it('should create different keys for different versions', () => {
    const namespace = createNamespace('https://example.com/api/');

    const key1 = createBloomCacheKey(namespace, 'v1');
    const key2 = createBloomCacheKey(namespace, 'v2');

    expect(key1).not.toBe(key2);
  });

  it('should produce valid URLs', () => {
    const namespace = createNamespace('https://example.com/api/');
    const key = createBloomCacheKey(namespace, 'v1');

    expect(() => new URL(key)).not.toThrow();
  });
});

describe('BloomRouter', () => {
  let bloomRouter: BloomRouter;
  const testNamespace = createNamespace('https://example.com/api/');
  const testVersion = 'v1-abc123';

  beforeEach(() => {
    vi.clearAllMocks();
    bloomRouter = new BloomRouter();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('checkEntity', () => {
    it('should check entity against cached bloom filter', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: testVersion });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      const cachedResponse = new Response(JSON.stringify(serialized), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=31536000, immutable',
          'X-Bloom-Version': testVersion,
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await bloomRouter.checkEntity(
        testNamespace,
        testVersion,
        'https://example.com/api/entity/123'
      );

      expect(result.mightExist).toBe(true);
      expect(result.cacheHit).toBe(true);
    });

    it('should return false for non-existent entity', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: testVersion });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      const cachedResponse = new Response(JSON.stringify(serialized), {
        headers: {
          'Content-Type': 'application/json',
          'X-Bloom-Version': testVersion,
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await bloomRouter.checkEntity(
        testNamespace,
        testVersion,
        'https://example.com/api/entity/999'
      );

      expect(result.mightExist).toBe(false);
      expect(result.cacheHit).toBe(true);
    });

    it('should indicate cache miss when filter not cached', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await bloomRouter.checkEntity(
        testNamespace,
        testVersion,
        'https://example.com/api/entity/123'
      );

      expect(result.cacheHit).toBe(false);
      // When cache misses, we must assume entity might exist
      expect(result.mightExist).toBe(true);
    });
  });

  describe('cacheFilter', () => {
    it('should cache bloom filter with immutable headers', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: testVersion });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      mockCache.put.mockResolvedValueOnce(undefined);

      await bloomRouter.cacheFilter(testNamespace, testVersion, serialized);

      expect(mockCache.put).toHaveBeenCalledTimes(1);

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      expect(cacheControl).toContain('immutable');
    });
  });

  describe('generateResponseHeaders', () => {
    it('should generate Cache-Control headers for bloom filter responses', () => {
      const headers = bloomRouter.generateResponseHeaders(testVersion);

      expect(headers['Cache-Control']).toContain('public');
      expect(headers['Cache-Control']).toContain('max-age=31536000');
      expect(headers['Cache-Control']).toContain('immutable');
    });
  });

  describe('with fallback loader', () => {
    it('should load filter from fallback on cache miss', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: testVersion });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      // First call - cache miss
      mockCache.match.mockResolvedValueOnce(undefined);
      // Second call after caching - still need to handle put
      mockCache.put.mockResolvedValueOnce(undefined);

      const mockLoader = vi.fn().mockResolvedValue(serialized);

      const routerWithLoader = new BloomRouter({
        filterLoader: mockLoader,
        cacheOnLoad: true,
      });

      const result = await routerWithLoader.checkEntity(
        testNamespace,
        testVersion,
        'https://example.com/api/entity/123'
      );

      expect(mockLoader).toHaveBeenCalledWith(testNamespace, testVersion);
      expect(result.mightExist).toBe(true);
      expect(mockCache.put).toHaveBeenCalledTimes(1);
    });

    it('should not call loader on cache hit', async () => {
      const filter = createBloomFilter({ capacity: 1000, version: testVersion });
      addToFilter(filter, 'https://example.com/api/entity/123');
      const serialized = serializeFilter(filter);

      const cachedResponse = new Response(JSON.stringify(serialized), {
        headers: {
          'Content-Type': 'application/json',
          'X-Bloom-Version': testVersion,
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const mockLoader = vi.fn();

      const routerWithLoader = new BloomRouter({
        filterLoader: mockLoader,
      });

      await routerWithLoader.checkEntity(
        testNamespace,
        testVersion,
        'https://example.com/api/entity/123'
      );

      expect(mockLoader).not.toHaveBeenCalled();
    });
  });
});

describe('BloomRouter Response Headers Integration', () => {
  let bloomRouter: BloomRouter;
  const testNamespace = createNamespace('https://example.com/api/');
  const testVersion = 'v1-abc123';

  beforeEach(() => {
    vi.clearAllMocks();
    bloomRouter = new BloomRouter();
  });

  it('should include headers in check result for client responses', async () => {
    const filter = createBloomFilter({ capacity: 1000, version: testVersion });
    addToFilter(filter, 'https://example.com/api/entity/123');
    const serialized = serializeFilter(filter);

    const cachedResponse = new Response(JSON.stringify(serialized), {
      headers: {
        'Content-Type': 'application/json',
        'X-Bloom-Version': testVersion,
      },
    });

    mockCache.match.mockResolvedValueOnce(cachedResponse);

    const result = await bloomRouter.checkEntity(
      testNamespace,
      testVersion,
      'https://example.com/api/entity/123'
    );

    expect(result.headers).toBeDefined();
    expect(result.headers?.['Cache-Control']).toContain('immutable');
  });
});

describe('Edge Cache Error Handling', () => {
  let bloomRouter: BloomRouter;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    bloomRouter = new BloomRouter();
  });

  it('should handle cache API errors gracefully', async () => {
    mockCache.match.mockRejectedValueOnce(new Error('Cache unavailable'));

    const result = await bloomRouter.checkEntity(
      testNamespace,
      'v1',
      'https://example.com/api/entity/123'
    );

    // Should not throw, and should assume entity might exist
    expect(result.mightExist).toBe(true);
    expect(result.cacheHit).toBe(false);
  });

  it('should handle malformed cached data', async () => {
    const malformedResponse = new Response('not json', {
      headers: {
        'Content-Type': 'application/json',
        'X-Bloom-Version': 'v1',
      },
    });

    mockCache.match.mockResolvedValueOnce(malformedResponse);

    const result = await bloomRouter.checkEntity(
      testNamespace,
      'v1',
      'https://example.com/api/entity/123'
    );

    // Should handle gracefully and assume entity might exist
    expect(result.mightExist).toBe(true);
    expect(result.cacheHit).toBe(false);
  });
});
