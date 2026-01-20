/**
 * Edge Cache Integration Tests for GraphDB
 *
 * TDD RED phase - tests for edge caching GraphCol chunks.
 * Following the design from pocs-s0ks:
 * - Cache-Control headers for chunks and manifests
 * - URL pattern design for cache efficiency
 * - Invalidation support for chunk updates
 *
 * Key patterns:
 * - Chunks: /graphdb/v1/{namespace}/chunks/{chunk-id}.gcol
 *   Cache-Control: public, max-age=31536000, immutable
 * - Manifests: /graphdb/v1/{namespace}/manifest.json
 *   Cache-Control: public, max-age=60, stale-while-revalidate=300
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  type ChunkCacheConfig,
  type ManifestCacheConfig,
  type CacheableChunk,
  type CacheableManifest,
  type CachedChunkResponse,
  ChunkEdgeCache,
  generateChunkCacheUrl,
  generateManifestCacheUrl,
  generateChunkCacheHeaders,
  generateManifestCacheHeaders,
  parseChunkCacheUrl,
  DEFAULT_CHUNK_MAX_AGE,
  DEFAULT_MANIFEST_MAX_AGE,
  DEFAULT_MANIFEST_SWR,
} from '../../src/cache/edge-cache';
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

describe('ChunkEdgeCache', () => {
  let chunkCache: ChunkEdgeCache;
  const testNamespace = createNamespace('https://example.com/graphdb/');
  const testChunkId = 'chunk-2024-01-15-001';

  beforeEach(() => {
    vi.clearAllMocks();
    chunkCache = new ChunkEdgeCache();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('getChunk', () => {
    it('should return null on cache miss', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await chunkCache.getChunk(testNamespace, testChunkId);

      expect(result).toBeNull();
      expect(mockCache.match).toHaveBeenCalledTimes(1);
    });

    it('should return cached chunk data on cache hit', async () => {
      const mockChunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]); // GCOL magic
      const cachedResponse = new Response(mockChunkData, {
        headers: {
          'Content-Type': 'application/octet-stream',
          'X-Chunk-Id': testChunkId,
          'X-Cache-Timestamp': Date.now().toString(),
          'Cache-Control': 'public, max-age=31536000, immutable',
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await chunkCache.getChunk(testNamespace, testChunkId);

      expect(result).not.toBeNull();
      expect(result?.chunkId).toBe(testChunkId);
      expect(result?.metadata.cacheHit).toBe(true);
    });

    it('should use correct cache URL format', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      await chunkCache.getChunk(testNamespace, testChunkId);

      const calledRequest = mockCache.match.mock.calls[0][0];
      expect(calledRequest).toBeInstanceOf(Request);
      expect(calledRequest.url).toContain('/graphdb/v1/');
      expect(calledRequest.url).toContain('/chunks/');
      expect(calledRequest.url).toContain('.gcol');
    });
  });

  describe('putChunk', () => {
    it('should cache chunk with immutable headers', async () => {
      const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);

      mockCache.put.mockResolvedValueOnce(undefined);

      await chunkCache.putChunk(testNamespace, testChunkId, chunkData);

      expect(mockCache.put).toHaveBeenCalledTimes(1);

      const [request, response] = mockCache.put.mock.calls[0];
      expect(request).toBeInstanceOf(Request);
      expect(response).toBeInstanceOf(Response);

      const cacheControl = response.headers.get('Cache-Control');
      expect(cacheControl).toContain('public');
      expect(cacheControl).toContain('max-age=31536000');
      expect(cacheControl).toContain('immutable');
    });

    it('should include chunk ID in response headers', async () => {
      const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);

      mockCache.put.mockResolvedValueOnce(undefined);

      await chunkCache.putChunk(testNamespace, testChunkId, chunkData);

      const [, response] = mockCache.put.mock.calls[0];
      expect(response.headers.get('X-Chunk-Id')).toBe(testChunkId);
    });

    it('should set correct content type for GraphCol chunks', async () => {
      const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);

      mockCache.put.mockResolvedValueOnce(undefined);

      await chunkCache.putChunk(testNamespace, testChunkId, chunkData);

      const [, response] = mockCache.put.mock.calls[0];
      expect(response.headers.get('Content-Type')).toBe('application/octet-stream');
    });
  });

  describe('deleteChunk', () => {
    it('should delete chunk from edge cache', async () => {
      mockCache.delete.mockResolvedValueOnce(true);

      const result = await chunkCache.deleteChunk(testNamespace, testChunkId);

      expect(result).toBe(true);
      expect(mockCache.delete).toHaveBeenCalledTimes(1);
    });

    it('should return false if chunk not in cache', async () => {
      mockCache.delete.mockResolvedValueOnce(false);

      const result = await chunkCache.deleteChunk(testNamespace, testChunkId);

      expect(result).toBe(false);
    });
  });
});

describe('Manifest Caching', () => {
  let chunkCache: ChunkEdgeCache;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    chunkCache = new ChunkEdgeCache();
  });

  describe('getManifest', () => {
    it('should return null on cache miss', async () => {
      mockCache.match.mockResolvedValueOnce(undefined);

      const result = await chunkCache.getManifest(testNamespace);

      expect(result).toBeNull();
      expect(mockCache.match).toHaveBeenCalledTimes(1);
    });

    it('should return cached manifest on cache hit', async () => {
      const manifestData = JSON.stringify({
        version: 1,
        chunks: ['chunk-001', 'chunk-002'],
        lastUpdated: Date.now(),
      });

      const cachedResponse = new Response(manifestData, {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': 'public, max-age=60, stale-while-revalidate=300',
          'X-Cache-Timestamp': Date.now().toString(),
        },
      });

      mockCache.match.mockResolvedValueOnce(cachedResponse);

      const result = await chunkCache.getManifest(testNamespace);

      expect(result).not.toBeNull();
      expect(result?.version).toBe(1);
      expect(result?.chunks).toHaveLength(2);
    });
  });

  describe('putManifest', () => {
    it('should cache manifest with short TTL and stale-while-revalidate', async () => {
      const manifestData = {
        version: 1,
        chunks: ['chunk-001', 'chunk-002'],
        lastUpdated: Date.now(),
      };

      mockCache.put.mockResolvedValueOnce(undefined);

      await chunkCache.putManifest(testNamespace, manifestData);

      expect(mockCache.put).toHaveBeenCalledTimes(1);

      const [, response] = mockCache.put.mock.calls[0];
      const cacheControl = response.headers.get('Cache-Control');
      expect(cacheControl).toContain('public');
      expect(cacheControl).toContain(`max-age=${DEFAULT_MANIFEST_MAX_AGE}`);
      expect(cacheControl).toContain(`stale-while-revalidate=${DEFAULT_MANIFEST_SWR}`);
    });

    it('should use correct URL pattern for manifest', async () => {
      const manifestData = { version: 1, chunks: [], lastUpdated: Date.now() };

      mockCache.put.mockResolvedValueOnce(undefined);

      await chunkCache.putManifest(testNamespace, manifestData);

      const [request] = mockCache.put.mock.calls[0];
      expect(request.url).toContain('/manifest.json');
    });
  });
});

describe('Cache URL Generation', () => {
  const testNamespace = createNamespace('https://example.com/graphdb/');

  describe('generateChunkCacheUrl', () => {
    it('should generate deterministic URLs for chunks', () => {
      const chunkId = 'chunk-001';

      const url1 = generateChunkCacheUrl(testNamespace, chunkId);
      const url2 = generateChunkCacheUrl(testNamespace, chunkId);

      expect(url1).toBe(url2);
    });

    it('should include namespace in the URL path', () => {
      const chunkId = 'chunk-001';

      const url = generateChunkCacheUrl(testNamespace, chunkId);

      expect(url).toContain(encodeURIComponent(testNamespace));
    });

    it('should include chunk ID with .gcol extension', () => {
      const chunkId = 'chunk-001';

      const url = generateChunkCacheUrl(testNamespace, chunkId);

      expect(url).toContain('chunk-001');
      expect(url).toContain('.gcol');
    });

    it('should produce valid URLs', () => {
      const chunkId = 'chunk-001';

      const url = generateChunkCacheUrl(testNamespace, chunkId);

      expect(() => new URL(url)).not.toThrow();
    });

    it('should handle special characters in namespace', () => {
      const specialNamespace = createNamespace('https://example.com/path/with spaces/');
      const chunkId = 'chunk-001';

      const url = generateChunkCacheUrl(specialNamespace, chunkId);

      expect(() => new URL(url)).not.toThrow();
    });
  });

  describe('generateManifestCacheUrl', () => {
    it('should generate URL ending with manifest.json', () => {
      const url = generateManifestCacheUrl(testNamespace);

      expect(url).toContain('manifest.json');
    });

    it('should include namespace in the URL', () => {
      const url = generateManifestCacheUrl(testNamespace);

      expect(url).toContain(encodeURIComponent(testNamespace));
    });
  });

  describe('parseChunkCacheUrl', () => {
    it('should extract namespace and chunk ID from URL', () => {
      const chunkId = 'chunk-001';
      const url = generateChunkCacheUrl(testNamespace, chunkId);

      const parsed = parseChunkCacheUrl(url);

      expect(parsed.namespace).toBe(testNamespace);
      expect(parsed.chunkId).toBe(chunkId);
    });
  });
});

describe('Cache Header Generation', () => {
  describe('generateChunkCacheHeaders', () => {
    it('should generate immutable cache headers for chunks', () => {
      const headers = generateChunkCacheHeaders('chunk-001');

      expect(headers['Cache-Control']).toContain('immutable');
      expect(headers['Cache-Control']).toContain('max-age=31536000');
      expect(headers['Cache-Control']).toContain('public');
    });

    it('should include chunk ID header', () => {
      const headers = generateChunkCacheHeaders('chunk-001');

      expect(headers['X-Chunk-Id']).toBe('chunk-001');
    });

    it('should set correct content type', () => {
      const headers = generateChunkCacheHeaders('chunk-001');

      expect(headers['Content-Type']).toBe('application/octet-stream');
    });

    it('should allow custom max-age override', () => {
      const headers = generateChunkCacheHeaders('chunk-001', { maxAge: 86400 });

      expect(headers['Cache-Control']).toContain('max-age=86400');
    });
  });

  describe('generateManifestCacheHeaders', () => {
    it('should generate short-lived cache headers for manifests', () => {
      const headers = generateManifestCacheHeaders();

      expect(headers['Cache-Control']).toContain(`max-age=${DEFAULT_MANIFEST_MAX_AGE}`);
      expect(headers['Cache-Control']).toContain(`stale-while-revalidate=${DEFAULT_MANIFEST_SWR}`);
    });

    it('should NOT include immutable directive', () => {
      const headers = generateManifestCacheHeaders();

      expect(headers['Cache-Control']).not.toContain('immutable');
    });

    it('should set JSON content type', () => {
      const headers = generateManifestCacheHeaders();

      expect(headers['Content-Type']).toBe('application/json');
    });

    it('should allow custom TTL configuration', () => {
      const headers = generateManifestCacheHeaders({ maxAge: 120, swr: 600 });

      expect(headers['Cache-Control']).toContain('max-age=120');
      expect(headers['Cache-Control']).toContain('stale-while-revalidate=600');
    });
  });
});

describe('Cache Configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should accept custom chunk cache configuration', () => {
    const config: ChunkCacheConfig = {
      maxAge: 86400, // 1 day instead of 1 year
      immutable: true,
      cacheKeyPrefix: 'custom-prefix',
    };

    const cache = new ChunkEdgeCache(config);

    expect(cache.config.maxAge).toBe(86400);
    expect(cache.config.immutable).toBe(true);
  });

  it('should use default values when no config provided', () => {
    const cache = new ChunkEdgeCache();

    expect(cache.config.maxAge).toBe(DEFAULT_CHUNK_MAX_AGE);
    expect(cache.config.immutable).toBe(true);
  });
});

describe('Edge Cache Error Handling', () => {
  let chunkCache: ChunkEdgeCache;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    chunkCache = new ChunkEdgeCache();
  });

  it('should handle cache API errors gracefully for get operations', async () => {
    mockCache.match.mockRejectedValueOnce(new Error('Cache unavailable'));

    const result = await chunkCache.getChunk(testNamespace, 'chunk-001');

    expect(result).toBeNull();
  });

  it('should handle cache API errors gracefully for put operations', async () => {
    mockCache.put.mockRejectedValueOnce(new Error('Cache unavailable'));

    const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);

    await expect(
      chunkCache.putChunk(testNamespace, 'chunk-001', chunkData)
    ).resolves.not.toThrow();
  });

  it('should handle malformed cached data', async () => {
    const malformedResponse = new Response('not binary data', {
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Chunk-Id': 'chunk-001',
      },
    });

    mockCache.match.mockResolvedValueOnce(malformedResponse);

    // Should not throw, just return what's in the cache
    const result = await chunkCache.getChunk(testNamespace, 'chunk-001');
    expect(result).not.toBeNull();
  });
});

describe('Cache Hit Rate Tracking', () => {
  let chunkCache: ChunkEdgeCache;
  const testNamespace = createNamespace('https://example.com/graphdb/');

  beforeEach(() => {
    vi.clearAllMocks();
    chunkCache = new ChunkEdgeCache({ trackMetrics: true });
  });

  it('should track cache hits', async () => {
    const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);
    const cachedResponse = new Response(chunkData, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Chunk-Id': 'chunk-001',
      },
    });

    mockCache.match.mockResolvedValueOnce(cachedResponse);

    await chunkCache.getChunk(testNamespace, 'chunk-001');

    const metrics = chunkCache.getMetrics();
    expect(metrics.hits).toBe(1);
    expect(metrics.misses).toBe(0);
  });

  it('should track cache misses', async () => {
    mockCache.match.mockResolvedValueOnce(undefined);

    await chunkCache.getChunk(testNamespace, 'chunk-001');

    const metrics = chunkCache.getMetrics();
    expect(metrics.hits).toBe(0);
    expect(metrics.misses).toBe(1);
  });

  it('should calculate hit rate correctly', async () => {
    const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);

    // Create fresh Response objects for each mock call (Response body can only be consumed once)
    const createResponse = () => new Response(chunkData.slice(), {
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Chunk-Id': 'chunk-001',
      },
    });

    // 3 hits, 1 miss = 75% hit rate
    mockCache.match.mockResolvedValueOnce(createResponse());
    mockCache.match.mockResolvedValueOnce(createResponse());
    mockCache.match.mockResolvedValueOnce(createResponse());
    mockCache.match.mockResolvedValueOnce(undefined);

    await chunkCache.getChunk(testNamespace, 'chunk-001');
    await chunkCache.getChunk(testNamespace, 'chunk-002');
    await chunkCache.getChunk(testNamespace, 'chunk-003');
    await chunkCache.getChunk(testNamespace, 'chunk-004');

    const metrics = chunkCache.getMetrics();
    expect(metrics.hitRate).toBe(0.75);
  });

  it('should reset metrics when requested', async () => {
    const chunkData = new Uint8Array([0x47, 0x43, 0x4F, 0x4C]);

    // Create fresh Response objects for each mock call
    const createResponse = () => new Response(chunkData.slice(), {
      headers: {
        'Content-Type': 'application/octet-stream',
        'X-Chunk-Id': 'chunk-001',
      },
    });

    mockCache.match.mockResolvedValueOnce(createResponse());
    mockCache.match.mockResolvedValueOnce(createResponse());

    await chunkCache.getChunk(testNamespace, 'chunk-001');
    await chunkCache.getChunk(testNamespace, 'chunk-002');

    chunkCache.resetMetrics();

    const metrics = chunkCache.getMetrics();
    expect(metrics.hits).toBe(0);
    expect(metrics.misses).toBe(0);
  });
});
