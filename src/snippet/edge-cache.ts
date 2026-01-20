/**
 * Edge Cache for Snippet Layer
 *
 * Uses Cloudflare's Cache API (caches.default) to cache bloom filters
 * and index segments at the edge. Edge cache is FREE and critical for
 * cost optimization.
 *
 * Key features:
 * - Cache bloom filters under namespace-specific keys
 * - Cache index segments with configurable TTL
 * - Version-based cache invalidation
 * - Graceful error handling (cache failures don't break the app)
 */

import { type Namespace } from '../core/types.js';
import { type SerializedFilter } from './bloom.js';
import {
  DEFAULT_BLOOM_TTL_SECONDS,
  DEFAULT_SEGMENT_TTL_SECONDS,
  TIMESTAMP_PARSE_RADIX,
} from './constants.js';

// ============================================================================
// Constants (re-exported from constants.ts for backward compatibility)
// ============================================================================

/** Default TTL for bloom filters (5 minutes) */
export const DEFAULT_BLOOM_TTL = DEFAULT_BLOOM_TTL_SECONDS;

/** Default TTL for index segments (1 hour) */
export const DEFAULT_SEGMENT_TTL = DEFAULT_SEGMENT_TTL_SECONDS;

/** Cache key domain for edge cache URLs */
const CACHE_DOMAIN = 'https://edge-cache.graphdb.internal';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration options for edge cache
 */
export interface EdgeCacheConfig {
  /** TTL for bloom filters in seconds */
  bloomTtl?: number;
  /** TTL for index segments in seconds */
  segmentTtl?: number;
  /** Prefix for all cache keys */
  cacheKeyPrefix?: string;
}

/**
 * Cached bloom filter with metadata
 */
export interface CachedBloomFilter {
  /** The serialized bloom filter */
  filter: SerializedFilter;
  /** Version string used for cache busting */
  version: string;
  /** Timestamp when cached */
  cachedAt: number;
  /** Namespace this filter belongs to */
  namespace: Namespace;
}

/**
 * Index segment entry
 */
export interface IndexSegmentEntry {
  key: string;
  positions: number[];
}

/**
 * Cached index segment with metadata
 */
export interface CachedIndexSegment {
  /** Segment identifier */
  id: string;
  /** Index entries */
  entries: IndexSegmentEntry[];
  /** Minimum key in segment (for range queries) */
  minKey?: string;
  /** Maximum key in segment (for range queries) */
  maxKey?: string;
  /** Version string */
  version?: string;
  /** Timestamp when cached */
  cachedAt?: number;
}

/**
 * Options for cache put operations
 */
export interface CachePutOptions {
  /** Override default TTL */
  ttl?: number;
  /** Enable stale-while-revalidate */
  staleWhileRevalidate?: number;
}

/**
 * Options for cache get operations
 */
export interface CacheGetOptions {
  /** R2 fallback function for cache miss */
  r2Fallback?: (namespace: Namespace, segmentId: string) => Promise<CachedIndexSegment | null>;
  /** Whether to populate cache after R2 fallback */
  cacheOnMiss?: boolean;
}

/**
 * Parsed cache key components
 */
export interface ParsedCacheKey {
  type: 'bloom' | 'segment';
  namespace: string;
  segmentId?: string;
}

// ============================================================================
// Cache Key Functions
// ============================================================================

/**
 * Create a deterministic cache key for edge cache
 *
 * Format: https://edge-cache.graphdb.internal/{prefix}/{type}/{encoded-namespace}[/{segmentId}]
 */
export function createEdgeCacheKey(
  type: 'bloom' | 'segment',
  namespace: Namespace,
  segmentId?: string,
  prefix: string = 'graphdb'
): string {
  const encodedNamespace = encodeURIComponent(namespace);

  let path = `/${prefix}/${type}/${encodedNamespace}`;
  if (segmentId) {
    path += `/${encodeURIComponent(segmentId)}`;
  }

  return `${CACHE_DOMAIN}${path}`;
}

/**
 * Parse a cache key back to its components
 */
export function parseEdgeCacheKey(key: string): ParsedCacheKey {
  const url = new URL(key);
  const parts = url.pathname.split('/').filter(Boolean);

  // Expected format: [prefix, type, namespace, segmentId?]
  const type = (parts[1] ?? 'segment') as 'bloom' | 'segment';
  const namespace = decodeURIComponent(parts[2] ?? '');
  const segmentId = parts[3] ? decodeURIComponent(parts[3]) : undefined;

  const result: ParsedCacheKey = {
    type,
    namespace,
  };
  if (segmentId !== undefined) {
    result.segmentId = segmentId;
  }
  return result;
}

// ============================================================================
// EdgeCache Class
// ============================================================================

/**
 * Edge cache manager for bloom filters and index segments
 */
export class EdgeCache {
  readonly config: Required<EdgeCacheConfig>;

  constructor(config: EdgeCacheConfig = {}) {
    this.config = {
      bloomTtl: config.bloomTtl ?? DEFAULT_BLOOM_TTL,
      segmentTtl: config.segmentTtl ?? DEFAULT_SEGMENT_TTL,
      cacheKeyPrefix: config.cacheKeyPrefix ?? 'graphdb',
    };
  }

  // ==========================================================================
  // Bloom Filter Operations
  // ==========================================================================

  /**
   * Get a bloom filter from edge cache
   *
   * @param namespace - The namespace to get bloom filter for
   * @param version - Expected version (returns null if mismatch)
   * @returns Cached bloom filter or null on miss/mismatch
   */
  async getBloomFilter(
    namespace: Namespace,
    version: string
  ): Promise<CachedBloomFilter | null> {
    try {
      const cacheKey = createEdgeCacheKey('bloom', namespace, undefined, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const cache = caches.default;
      const response = await cache.match(request);

      if (!response) {
        return null;
      }

      // Check version header
      const cachedVersion = response.headers.get('X-Cache-Version');
      if (cachedVersion !== version) {
        return null;
      }

      // Parse the cached filter
      const filter = await response.json() as SerializedFilter;
      const cachedAt = parseInt(response.headers.get('X-Cache-Timestamp') || '0', TIMESTAMP_PARSE_RADIX);

      return {
        filter,
        version: cachedVersion,
        cachedAt,
        namespace,
      };
    } catch (error) {
      // Graceful degradation - log but don't throw
      console.error('EdgeCache getBloomFilter error:', error);
      return null;
    }
  }

  /**
   * Store a bloom filter in edge cache
   *
   * @param namespace - The namespace this filter belongs to
   * @param version - Version string for cache busting
   * @param filter - The serialized bloom filter
   * @param options - Optional cache configuration
   */
  async putBloomFilter(
    namespace: Namespace,
    version: string,
    filter: SerializedFilter,
    options: CachePutOptions = {}
  ): Promise<void> {
    try {
      const cacheKey = createEdgeCacheKey('bloom', namespace, undefined, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const ttl = options.ttl ?? this.config.bloomTtl;
      const now = Date.now();

      // Build Cache-Control header
      let cacheControl = `public, max-age=${ttl}, s-maxage=${ttl}`;
      if (options.staleWhileRevalidate) {
        cacheControl += `, stale-while-revalidate=${options.staleWhileRevalidate}`;
      }

      const response = new Response(JSON.stringify(filter), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': cacheControl,
          'X-Cache-Version': version,
          'X-Cache-Timestamp': now.toString(),
          'X-Cache-Type': 'bloom',
        },
      });

      const cache = caches.default;
      await cache.put(request, response);
    } catch (error) {
      // Graceful degradation - log but don't throw
      console.error('EdgeCache putBloomFilter error:', error);
    }
  }

  /**
   * Invalidate a bloom filter from edge cache
   *
   * @param namespace - The namespace to invalidate
   * @returns True if entry was deleted, false if it didn't exist
   */
  async invalidateBloomFilter(namespace: Namespace): Promise<boolean> {
    try {
      const cacheKey = createEdgeCacheKey('bloom', namespace, undefined, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const cache = caches.default;
      return await cache.delete(request);
    } catch (error) {
      console.error('EdgeCache invalidateBloomFilter error:', error);
      return false;
    }
  }

  // ==========================================================================
  // Index Segment Operations
  // ==========================================================================

  /**
   * Get an index segment from edge cache
   *
   * @param namespace - The namespace
   * @param segmentId - The segment identifier
   * @param version - Expected version
   * @param options - Optional fallback configuration
   * @returns Cached segment or null on miss
   */
  async getIndexSegment(
    namespace: Namespace,
    segmentId: string,
    version: string,
    options: CacheGetOptions = {}
  ): Promise<CachedIndexSegment | null> {
    try {
      const cacheKey = createEdgeCacheKey('segment', namespace, segmentId, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const cache = caches.default;
      const response = await cache.match(request);

      if (response) {
        // Check version
        const cachedVersion = response.headers.get('X-Cache-Version');
        if (cachedVersion === version) {
          const segment = await response.json() as CachedIndexSegment;
          return segment;
        }
      }

      // Cache miss - try R2 fallback if provided
      if (options.r2Fallback) {
        const segment = await options.r2Fallback(namespace, segmentId);

        // Populate cache after fallback if requested
        if (segment && options.cacheOnMiss) {
          await this.putIndexSegment(namespace, segmentId, version, segment);
        }

        return segment;
      }

      return null;
    } catch (error) {
      console.error('EdgeCache getIndexSegment error:', error);
      return null;
    }
  }

  /**
   * Store an index segment in edge cache
   *
   * @param namespace - The namespace
   * @param segmentId - The segment identifier
   * @param version - Version string
   * @param segment - The segment data
   * @param options - Optional cache configuration
   */
  async putIndexSegment(
    namespace: Namespace,
    segmentId: string,
    version: string,
    segment: CachedIndexSegment | Omit<CachedIndexSegment, 'cachedAt' | 'version'>,
    options: CachePutOptions = {}
  ): Promise<void> {
    try {
      const cacheKey = createEdgeCacheKey('segment', namespace, segmentId, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const ttl = options.ttl ?? this.config.segmentTtl;
      const now = Date.now();

      // Build Cache-Control header
      let cacheControl = `public, max-age=${ttl}, s-maxage=${ttl}`;
      if (options.staleWhileRevalidate) {
        cacheControl += `, stale-while-revalidate=${options.staleWhileRevalidate}`;
      }

      // Enrich segment with cache metadata
      const enrichedSegment: CachedIndexSegment = {
        ...segment,
        version,
        cachedAt: now,
      };

      const response = new Response(JSON.stringify(enrichedSegment), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': cacheControl,
          'X-Cache-Version': version,
          'X-Segment-Id': segmentId,
          'X-Cache-Timestamp': now.toString(),
          'X-Cache-Type': 'segment',
        },
      });

      const cache = caches.default;
      await cache.put(request, response);
    } catch (error) {
      console.error('EdgeCache putIndexSegment error:', error);
    }
  }

  /**
   * Invalidate an index segment from edge cache
   *
   * @param namespace - The namespace
   * @param segmentId - The segment identifier
   * @returns True if entry was deleted
   */
  async invalidateIndexSegment(
    namespace: Namespace,
    segmentId: string
  ): Promise<boolean> {
    try {
      const cacheKey = createEdgeCacheKey('segment', namespace, segmentId, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const cache = caches.default;
      return await cache.delete(request);
    } catch (error) {
      console.error('EdgeCache invalidateIndexSegment error:', error);
      return false;
    }
  }

  /**
   * Invalidate all segments for a namespace
   *
   * Note: This is a best-effort operation. The Cache API doesn't support
   * wildcard deletion, so we track segments separately or use versioning.
   *
   * @param namespace - The namespace to invalidate
   * @param segmentIds - List of segment IDs to invalidate
   */
  async invalidateAllSegments(
    namespace: Namespace,
    segmentIds: string[]
  ): Promise<{ deleted: number; failed: number }> {
    let deleted = 0;
    let failed = 0;

    for (const segmentId of segmentIds) {
      const success = await this.invalidateIndexSegment(namespace, segmentId);
      if (success) {
        deleted++;
      } else {
        failed++;
      }
    }

    return { deleted, failed };
  }
}
