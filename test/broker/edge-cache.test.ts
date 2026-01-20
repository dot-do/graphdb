/**
 * Edge Cache Tests for Broker DO
 *
 * TDD RED phase - tests for broker edge caching integration.
 * The broker should check edge cache before hitting DOs and populate cache after responses.
 *
 * Key concepts:
 * - Check edge cache before DO requests (reduce DO invocations)
 * - Populate edge cache after DO response
 * - Respect cache tags for selective invalidation
 * - Version-based cache busting
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  type BrokerCacheConfig,
  type CacheableRequest,
  type CacheableResponse,
  BrokerEdgeCache,
  createCacheTagsForNamespace,
  createCacheTagsForQuery,
  shouldCacheResponse,
  extractCacheableRequest,
} from '../../src/broker/edge-cache';
import { type Namespace, createNamespace, createEntityId } from '../../src/core/types';

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

describe('BrokerEdgeCache', () => {
  let brokerCache: BrokerEdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    brokerCache = new BrokerEdgeCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('checkCache', () => {
    it('should check edge cache before hitting DO', async () => {
      const cacheableRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        cacheKey: 'query-abc123',
      };

      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await brokerCache.checkCache(cacheableRequest);

      expect(mockCache.match).toHaveBeenCalledTimes(1);
      expect(result).toBeNull();
    });

    it('should return cached response on cache hit', async () => {
      const cacheableRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        cacheKey: 'query-abc123',
      };

      const cachedData = {
        $id: 'https://example.com/api/users/123',
        name: 'John Doe',
        age: 30,
      };

      const cachedResponse = new Response(JSON.stringify(cachedData), {
        headers: {
          'Content-Type': 'application/json',
          'X-Cache-Hit': 'true',
          'X-Cache-Age': '60',
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await brokerCache.checkCache(cacheableRequest);

      expect(result).not.toBeNull();
      expect(result?.data).toEqual(cachedData);
      expect(result?.metadata.cacheHit).toBe(true);
    });

    it('should skip cache for mutation requests', async () => {
      const mutationRequest: CacheableRequest = {
        type: 'mutation',
        namespace: testNamespace,
        query: 'MUTATE https://example.com/api/users/123 SET name = "Jane"',
      };

      const result = await brokerCache.checkCache(mutationRequest);

      expect(mockCache.match).not.toHaveBeenCalled();
      expect(result).toBeNull();
    });

    it('should use request-specific cache key', async () => {
      const cacheableRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123.friends',
        cacheKey: 'query-specific-key',
      };

      mockCache.match.mockResolvedValueOnce(undefined);

      await brokerCache.checkCache(cacheableRequest);

      const [request] = mockCache.match.mock.calls[0];
      expect(request.url).toContain('query-specific-key');
    });
  });

  describe('populateCache', () => {
    it('should populate edge cache after DO response', async () => {
      const cacheableRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        cacheKey: 'query-abc123',
      };

      const responseData = {
        $id: 'https://example.com/api/users/123',
        name: 'John Doe',
      };

      mockCache.put.mockResolvedValueOnce(undefined);

      await brokerCache.populateCache(cacheableRequest, responseData);

      expect(mockCache.put).toHaveBeenCalledTimes(1);

      const [request, response] = mockCache.put.mock.calls[0];
      expect(request).toBeInstanceOf(Request);
      expect(response).toBeInstanceOf(Response);
    });

    it('should not cache mutation responses', async () => {
      const mutationRequest: CacheableRequest = {
        type: 'mutation',
        namespace: testNamespace,
        query: 'UPDATE something',
      };

      await brokerCache.populateCache(mutationRequest, { success: true });

      expect(mockCache.put).not.toHaveBeenCalled();
    });

    it('should include cache tags in response headers', async () => {
      const cacheableRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        cacheKey: 'query-abc123',
        cacheTags: ['namespace:example.com', 'entity:users/123'],
      };

      mockCache.put.mockResolvedValueOnce(undefined);

      await brokerCache.populateCache(cacheableRequest, { data: 'test' });

      const [, response] = mockCache.put.mock.calls[0];
      const cacheTags = response.headers.get('Cache-Tag');

      expect(cacheTags).toContain('namespace:example.com');
      expect(cacheTags).toContain('entity:users/123');
    });

    it('should set appropriate TTL based on query type', async () => {
      const cacheableRequest: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        cacheKey: 'query-abc123',
        ttl: 300, // 5 minutes
      };

      mockCache.put.mockResolvedValueOnce(undefined);

      await brokerCache.populateCache(cacheableRequest, { data: 'test' });

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');

      expect(cacheControl).toContain('max-age=300');
    });
  });

  describe('invalidateByTags', () => {
    it('should respect cache tags for selective invalidation', async () => {
      const tags = ['namespace:example.com', 'entity:users/123'];

      mockCache.delete.mockResolvedValue(true);

      const result = await brokerCache.invalidateByTags(tags);

      expect(result.invalidatedCount).toBeGreaterThan(0);
    });

    it('should invalidate all entries for a namespace', async () => {
      const namespace = createNamespace('https://example.com/api/');

      mockCache.delete.mockResolvedValue(true);

      const result = await brokerCache.invalidateNamespace(namespace);

      expect(mockCache.delete).toHaveBeenCalled();
      expect(result.success).toBe(true);
    });
  });

  describe('conditional caching', () => {
    it('should cache GET-like queries', async () => {
      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        cacheKey: 'query-get',
      };

      expect(brokerCache.shouldCache(request)).toBe(true);
    });

    it('should not cache queries with real-time requirements', async () => {
      const request: CacheableRequest = {
        type: 'subscription',
        namespace: testNamespace,
        query: 'SUBSCRIBE https://example.com/api/users/123',
      };

      expect(brokerCache.shouldCache(request)).toBe(false);
    });

    it('should respect no-cache directive in request', async () => {
      const request: CacheableRequest = {
        type: 'query',
        namespace: testNamespace,
        query: 'https://example.com/api/users/123',
        noCache: true,
      };

      expect(brokerCache.shouldCache(request)).toBe(false);
    });
  });
});

describe('Cache Tag Generation', () => {
  describe('createCacheTagsForNamespace', () => {
    it('should create namespace-based cache tags', () => {
      const namespace = createNamespace('https://example.com/api/');
      const tags = createCacheTagsForNamespace(namespace);

      expect(tags).toContain('ns:example.com/api/');
      expect(tags.length).toBeGreaterThan(0);
    });

    it('should include host-level tag for broader invalidation', () => {
      const namespace = createNamespace('https://example.com/api/');
      const tags = createCacheTagsForNamespace(namespace);

      expect(tags.some((t) => t.includes('host:example.com'))).toBe(true);
    });
  });

  describe('createCacheTagsForQuery', () => {
    it('should include entity-specific tags when entity ID is present', () => {
      const namespace = createNamespace('https://example.com/api/');
      const query = 'https://example.com/api/users/123';

      const tags = createCacheTagsForQuery(namespace, query);

      expect(tags.some((t) => t.includes('entity:'))).toBe(true);
    });

    it('should include property tags for traversal queries', () => {
      const namespace = createNamespace('https://example.com/api/');
      const query = 'https://example.com/api/users/123.friends.name';

      const tags = createCacheTagsForQuery(namespace, query);

      expect(tags.some((t) => t.includes('prop:friends'))).toBe(true);
      expect(tags.some((t) => t.includes('prop:name'))).toBe(true);
    });

    it('should include namespace tag', () => {
      const namespace = createNamespace('https://example.com/api/');
      const query = 'https://example.com/api/users/123';

      const tags = createCacheTagsForQuery(namespace, query);

      expect(tags.some((t) => t.startsWith('ns:'))).toBe(true);
    });
  });
});

describe('shouldCacheResponse', () => {
  it('should return true for successful query responses', () => {
    const response: CacheableResponse = {
      status: 200,
      data: { $id: 'https://example.com/api/users/123', name: 'John' },
    };

    expect(shouldCacheResponse(response)).toBe(true);
  });

  it('should return false for error responses', () => {
    const response: CacheableResponse = {
      status: 500,
      error: 'Internal server error',
    };

    expect(shouldCacheResponse(response)).toBe(false);
  });

  it('should return false for 404 responses by default', () => {
    const response: CacheableResponse = {
      status: 404,
      error: 'Not found',
    };

    expect(shouldCacheResponse(response)).toBe(false);
  });

  it('should optionally cache 404 responses for negative caching', () => {
    const response: CacheableResponse = {
      status: 404,
      error: 'Not found',
    };

    expect(shouldCacheResponse(response, { cacheNotFound: true })).toBe(true);
  });

  it('should return false for responses with no-store header', () => {
    const response: CacheableResponse = {
      status: 200,
      data: { $id: 'https://example.com/api/users/123' },
      headers: { 'Cache-Control': 'no-store' },
    };

    expect(shouldCacheResponse(response)).toBe(false);
  });
});

describe('extractCacheableRequest', () => {
  it('should extract cacheable request from HTTP request', () => {
    const httpRequest = new Request('https://example.com/api/query', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: 'https://example.com/api/users/123',
      }),
    });

    const namespace = createNamespace('https://example.com/api/');

    // This test verifies the extraction function exists and returns proper structure
    const result = extractCacheableRequest(httpRequest, namespace, {
      query: 'https://example.com/api/users/123',
    });

    expect(result.type).toBe('query');
    expect(result.namespace).toBe(namespace);
    expect(result.query).toBe('https://example.com/api/users/123');
  });

  it('should detect mutation type from query content', () => {
    const namespace = createNamespace('https://example.com/api/');

    const result = extractCacheableRequest(
      new Request('https://example.com/api/mutate', { method: 'POST' }),
      namespace,
      { query: 'MUTATE https://example.com/api/users/123 SET name = "Jane"' }
    );

    expect(result.type).toBe('mutation');
  });

  it('should generate cache key from query', () => {
    const namespace = createNamespace('https://example.com/api/');

    const result = extractCacheableRequest(
      new Request('https://example.com/api/query'),
      namespace,
      { query: 'https://example.com/api/users/123' }
    );

    expect(result.cacheKey).toBeDefined();
    expect(typeof result.cacheKey).toBe('string');
  });

  it('should extract TTL hint from request headers', () => {
    const httpRequest = new Request('https://example.com/api/query', {
      headers: {
        'X-Cache-TTL': '600',
      },
    });
    const namespace = createNamespace('https://example.com/api/');

    const result = extractCacheableRequest(httpRequest, namespace, {
      query: 'https://example.com/api/users/123',
    });

    expect(result.ttl).toBe(600);
  });

  it('should respect no-cache request header', () => {
    const httpRequest = new Request('https://example.com/api/query', {
      headers: {
        'Cache-Control': 'no-cache',
      },
    });
    const namespace = createNamespace('https://example.com/api/');

    const result = extractCacheableRequest(httpRequest, namespace, {
      query: 'https://example.com/api/users/123',
    });

    expect(result.noCache).toBe(true);
  });
});

describe('BrokerEdgeCache with Configuration', () => {
  it('should accept custom configuration', () => {
    const config: BrokerCacheConfig = {
      defaultTtl: 600,
      maxTtl: 3600,
      cacheKeyPrefix: 'broker-prod',
      enableNegativeCaching: true,
      negativeCacheTtl: 60,
    };

    const brokerCache = new BrokerEdgeCache(config);

    expect(brokerCache.config.defaultTtl).toBe(600);
    expect(brokerCache.config.maxTtl).toBe(3600);
    expect(brokerCache.config.enableNegativeCaching).toBe(true);
  });

  it('should cap TTL at maxTtl', async () => {
    const config: BrokerCacheConfig = {
      maxTtl: 300, // 5 minutes max
    };

    const brokerCache = new BrokerEdgeCache(config);
    const namespace = createNamespace('https://example.com/api/');

    const request: CacheableRequest = {
      type: 'query',
      namespace,
      query: 'https://example.com/api/users/123',
      cacheKey: 'query-abc',
      ttl: 3600, // Request 1 hour
    };

    mockCache.put.mockResolvedValueOnce(undefined);

    await brokerCache.populateCache(request, { data: 'test' });

    const [, response] = mockCache.put.mock.calls[0];
    const cacheControl = response.headers.get('Cache-Control');

    // Should be capped at 300
    expect(cacheControl).toContain('max-age=300');
  });
});

describe('Error Handling', () => {
  let brokerCache: BrokerEdgeCache;
  const testNamespace = createNamespace('https://example.com/api/');

  beforeEach(() => {
    vi.clearAllMocks();
    brokerCache = new BrokerEdgeCache();
  });

  it('should handle cache API errors gracefully', async () => {
    mockCache.match.mockRejectedValueOnce(new Error('Cache unavailable'));

    const request: CacheableRequest = {
      type: 'query',
      namespace: testNamespace,
      query: 'https://example.com/api/users/123',
      cacheKey: 'query-abc',
    };

    // Should not throw
    const result = await brokerCache.checkCache(request);
    expect(result).toBeNull();
  });

  it('should handle cache put errors without throwing', async () => {
    mockCache.put.mockRejectedValueOnce(new Error('Cache full'));

    const request: CacheableRequest = {
      type: 'query',
      namespace: testNamespace,
      query: 'https://example.com/api/users/123',
      cacheKey: 'query-abc',
    };

    // Should not throw
    await expect(
      brokerCache.populateCache(request, { data: 'test' })
    ).resolves.not.toThrow();
  });

  it('should handle malformed cached data', async () => {
    const malformedResponse = new Response('not valid json', {
      headers: { 'Content-Type': 'application/json' },
    });

    mockCache.match.mockResolvedValueOnce(malformedResponse);

    const request: CacheableRequest = {
      type: 'query',
      namespace: testNamespace,
      query: 'https://example.com/api/users/123',
      cacheKey: 'query-abc',
    };

    const result = await brokerCache.checkCache(request);
    expect(result).toBeNull();
  });
});
