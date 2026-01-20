/**
 * Immutable Bloom Filter Edge Cache
 *
 * Caches bloom filters at the edge with immutable headers since they rarely change.
 * Uses content-addressed caching where the cache key includes both namespace and version.
 *
 * Key features:
 * - Cache-Control: public, max-age=31536000, immutable (1 year TTL)
 * - Content-addressed keys: namespace + version for automatic invalidation
 * - Graceful error handling (cache failures don't break the app)
 *
 * @packageDocumentation
 */

import { type Namespace } from '../core/types.js';
import { type SerializedFilter } from './bloom.js';
import {
  IMMUTABLE_MAX_AGE_SECONDS,
  TIMESTAMP_PARSE_RADIX,
} from './constants.js';

// ============================================================================
// Constants (re-exported from constants.ts for backward compatibility)
// ============================================================================

/** Default max-age for immutable bloom filters (1 year in seconds) */
export const DEFAULT_IMMUTABLE_MAX_AGE = IMMUTABLE_MAX_AGE_SECONDS;

/** Cache key domain for bloom filter cache URLs */
const CACHE_DOMAIN = 'https://bloom-cache.graphdb.internal';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for immutable bloom filter caching
 */
export interface ImmutableBloomCacheConfig {
  /** Max-age in seconds (default: 1 year) */
  maxAge?: number;
  /** Whether to use immutable directive (default: true) */
  immutable?: boolean;
  /** Cache key prefix (default: 'graphdb/bloom') */
  cacheKeyPrefix?: string;
}

/**
 * Cached bloom filter entry
 */
export interface BloomCacheEntry {
  /** The serialized bloom filter */
  filter: SerializedFilter;
  /** Version string */
  version: string;
  /** Namespace this filter belongs to */
  namespace: Namespace;
  /** Timestamp when cached */
  cachedAt: number;
}

/**
 * Options for generating bloom cache headers
 */
export interface BloomCacheHeaderOptions {
  /** Override default max-age */
  maxAge?: number;
  /** Whether to include immutable directive (default: true) */
  immutable?: boolean;
}

// ============================================================================
// Cache Key Functions
// ============================================================================

/**
 * Create a content-addressed cache key for a bloom filter
 *
 * Format: https://bloom-cache.graphdb.internal/{prefix}/{encoded-namespace}/{version}
 *
 * The key includes both namespace AND version, enabling automatic invalidation
 * when a new version is published (old versions remain cached but unused).
 *
 * @param namespace - The namespace this filter covers
 * @param version - Version string for content addressing
 * @param prefix - Optional cache key prefix (default: 'graphdb/bloom')
 * @returns Cache URL
 */
export function createBloomCacheKey(
  namespace: Namespace,
  version: string,
  prefix: string = 'graphdb/bloom'
): string {
  const encodedNamespace = encodeURIComponent(namespace);
  const encodedVersion = encodeURIComponent(version);
  return `${CACHE_DOMAIN}/${prefix}/${encodedNamespace}/${encodedVersion}`;
}

/**
 * Parse a bloom cache key back to its components
 */
export function parseBloomCacheKey(key: string): { namespace: string; version: string } {
  const url = new URL(key);
  const parts = url.pathname.split('/').filter(Boolean);

  // Expected format: [prefix, 'bloom', namespace, version]
  // or [prefix, namespace, version] if prefix is single segment
  const version = parts[parts.length - 1] ?? '';
  const namespace = parts[parts.length - 2] ?? '';

  return {
    namespace: decodeURIComponent(namespace),
    version: decodeURIComponent(version),
  };
}

// ============================================================================
// Cache Header Generation
// ============================================================================

/**
 * Generate Cache-Control headers for bloom filter responses
 *
 * Uses immutable headers since bloom filters are content-addressed by version.
 * When a filter changes, a new version is created with a new cache key.
 *
 * @param version - The bloom filter version
 * @param options - Optional configuration
 * @returns Headers object
 */
export function generateBloomCacheHeaders(
  version: string,
  options: BloomCacheHeaderOptions = {}
): Record<string, string> {
  const maxAge = options.maxAge ?? DEFAULT_IMMUTABLE_MAX_AGE;
  const useImmutable = options.immutable ?? true;

  let cacheControl = `public, max-age=${maxAge}, s-maxage=${maxAge}`;
  if (useImmutable) {
    cacheControl += ', immutable';
  }

  return {
    'Content-Type': 'application/json',
    'Cache-Control': cacheControl,
    'X-Bloom-Version': version,
    'X-Cache-Timestamp': Date.now().toString(),
  };
}

// ============================================================================
// ImmutableBloomCache Class
// ============================================================================

/**
 * Edge cache manager for bloom filters with immutable caching
 *
 * Uses content-addressed caching where each version gets its own cache key.
 * This enables infinite TTL with automatic invalidation when versions change.
 */
export class ImmutableBloomCache {
  readonly config: Required<ImmutableBloomCacheConfig>;

  constructor(config: ImmutableBloomCacheConfig = {}) {
    this.config = {
      maxAge: config.maxAge ?? DEFAULT_IMMUTABLE_MAX_AGE,
      immutable: config.immutable ?? true,
      cacheKeyPrefix: config.cacheKeyPrefix ?? 'graphdb/bloom',
    };
  }

  /**
   * Get a bloom filter from edge cache
   *
   * @param namespace - The namespace
   * @param version - The expected version (content-addressed)
   * @returns Cached bloom filter entry or null on miss
   */
  async getFilter(
    namespace: Namespace,
    version: string
  ): Promise<BloomCacheEntry | null> {
    try {
      const cacheKey = createBloomCacheKey(namespace, version, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const cache = caches.default;
      const response = await cache.match(request);

      if (!response) {
        return null;
      }

      // Parse the cached filter
      const filter = await response.json() as SerializedFilter;
      const cachedAt = parseInt(response.headers.get('X-Cache-Timestamp') || '0', TIMESTAMP_PARSE_RADIX);
      const cachedVersion = response.headers.get('X-Bloom-Version') || version;

      return {
        filter,
        version: cachedVersion,
        namespace,
        cachedAt,
      };
    } catch (error) {
      // Graceful degradation - log but don't throw
      console.error('ImmutableBloomCache getFilter error:', error);
      return null;
    }
  }

  /**
   * Store a bloom filter in edge cache with immutable headers
   *
   * @param namespace - The namespace this filter covers
   * @param version - Version string for content addressing
   * @param filter - The serialized bloom filter
   */
  async putFilter(
    namespace: Namespace,
    version: string,
    filter: SerializedFilter
  ): Promise<void> {
    try {
      const cacheKey = createBloomCacheKey(namespace, version, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const headers = generateBloomCacheHeaders(version, {
        maxAge: this.config.maxAge,
        immutable: this.config.immutable,
      });

      const response = new Response(JSON.stringify(filter), { headers });

      const cache = caches.default;
      await cache.put(request, response);
    } catch (error) {
      // Graceful degradation - log but don't throw
      console.error('ImmutableBloomCache putFilter error:', error);
    }
  }

  /**
   * Invalidate a specific bloom filter version from edge cache
   *
   * Note: With content-addressed caching, invalidation is usually not needed.
   * New versions automatically get new cache keys. This is provided for
   * explicit cache management if needed.
   *
   * @param namespace - The namespace
   * @param version - The version to invalidate
   * @returns True if entry was deleted
   */
  async invalidateFilter(namespace: Namespace, version: string): Promise<boolean> {
    try {
      const cacheKey = createBloomCacheKey(namespace, version, this.config.cacheKeyPrefix);
      const request = new Request(cacheKey);

      const cache = caches.default;
      return await cache.delete(request);
    } catch (error) {
      console.error('ImmutableBloomCache invalidateFilter error:', error);
      return false;
    }
  }
}
