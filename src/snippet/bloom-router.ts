/**
 * Bloom Router - Wires Edge Cache to Bloom Filter Operations
 *
 * Integrates the ImmutableBloomCache with bloom filter lookup operations.
 * Provides a high-level API for checking entity existence against cached
 * bloom filters at the edge.
 *
 * Key features:
 * - Cache-first bloom filter lookups
 * - Automatic cache population on miss (with fallback loader)
 * - Response headers for downstream caching
 * - Graceful degradation (assume entity exists on cache failures)
 *
 * @packageDocumentation
 */

import { type Namespace } from '../core/types.js';
import {
  type SerializedFilter,
  deserializeFilter,
  mightExist,
} from './bloom.js';
import {
  ImmutableBloomCache,
  type ImmutableBloomCacheConfig,
  generateBloomCacheHeaders,
} from './bloom-cache.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for BloomRouter
 */
export interface BloomRouterConfig {
  /** Cache configuration */
  cacheConfig?: ImmutableBloomCacheConfig;
  /** Fallback function to load filter on cache miss */
  filterLoader?: (namespace: Namespace, version: string) => Promise<SerializedFilter | null>;
  /** Whether to cache filters loaded via fallback (default: true) */
  cacheOnLoad?: boolean;
}

/**
 * Result from checking an entity against the bloom filter
 */
export interface BloomRouteResult {
  /** Whether the entity might exist (false = definitely not, true = maybe) */
  mightExist: boolean;
  /** Whether the result came from cache */
  cacheHit: boolean;
  /** Response headers to propagate (for downstream caching) */
  headers?: Record<string, string>;
  /** Error message if operation failed */
  error?: string;
}

// ============================================================================
// BloomRouter Class
// ============================================================================

/**
 * Router that wires edge cache to bloom filter lookups
 *
 * Provides a cache-first approach to bloom filter checks:
 * 1. Check edge cache for bloom filter
 * 2. If miss, optionally load from fallback (e.g., R2, DO)
 * 3. Check entity against filter
 * 4. Return result with appropriate cache headers
 *
 * @example
 * ```typescript
 * const router = new BloomRouter({
 *   filterLoader: async (ns, version) => {
 *     // Load from R2 or Durable Object
 *     return await r2.get(`bloom/${ns}/${version}`).json();
 *   },
 *   cacheOnLoad: true,
 * });
 *
 * const result = await router.checkEntity(
 *   namespace,
 *   'v1-abc123',
 *   'https://example.com/entity/123'
 * );
 *
 * if (!result.mightExist) {
 *   return new Response('Not Found', { status: 404 });
 * }
 * ```
 */
export class BloomRouter {
  private readonly cache: ImmutableBloomCache;
  private readonly filterLoader: ((namespace: Namespace, version: string) => Promise<SerializedFilter | null>) | undefined;
  private readonly cacheOnLoad: boolean;

  constructor(config: BloomRouterConfig = {}) {
    this.cache = new ImmutableBloomCache(config.cacheConfig);
    this.filterLoader = config.filterLoader;
    this.cacheOnLoad = config.cacheOnLoad ?? true;
  }

  /**
   * Check if an entity might exist using the cached bloom filter
   *
   * @param namespace - The namespace to check against
   * @param version - The bloom filter version to use
   * @param entityId - The entity ID to check
   * @returns Result with existence flag and cache metadata
   */
  async checkEntity(
    namespace: Namespace,
    version: string,
    entityId: string
  ): Promise<BloomRouteResult> {
    try {
      // Try to get filter from cache
      const cached = await this.cache.getFilter(namespace, version);

      if (cached) {
        // Cache hit - check against filter
        const filter = deserializeFilter(cached.filter);
        const exists = mightExist(filter, entityId);

        return {
          mightExist: exists,
          cacheHit: true,
          headers: generateBloomCacheHeaders(version),
        };
      }

      // Cache miss - try fallback loader if available
      if (this.filterLoader) {
        const serialized = await this.filterLoader(namespace, version);

        if (serialized) {
          // Cache the loaded filter if configured
          if (this.cacheOnLoad) {
            await this.cacheFilter(namespace, version, serialized);
          }

          // Check against loaded filter
          const filter = deserializeFilter(serialized);
          const exists = mightExist(filter, entityId);

          return {
            mightExist: exists,
            cacheHit: false,
            headers: generateBloomCacheHeaders(version),
          };
        }
      }

      // No filter available - assume entity might exist (safe default)
      return {
        mightExist: true,
        cacheHit: false,
        headers: generateBloomCacheHeaders(version),
      };
    } catch (error) {
      // Graceful degradation - assume entity exists on error
      console.error('BloomRouter checkEntity error:', error);
      return {
        mightExist: true,
        cacheHit: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  /**
   * Check multiple entities against the bloom filter
   *
   * @param namespace - The namespace to check against
   * @param version - The bloom filter version to use
   * @param entityIds - Array of entity IDs to check
   * @returns Map of entity ID to existence result
   */
  async checkEntities(
    namespace: Namespace,
    version: string,
    entityIds: string[]
  ): Promise<Map<string, boolean>> {
    const results = new Map<string, boolean>();

    try {
      // Get filter from cache
      const cached = await this.cache.getFilter(namespace, version);
      let serialized: SerializedFilter | null = cached?.filter ?? null;

      // Try fallback if not cached
      if (!serialized && this.filterLoader) {
        serialized = await this.filterLoader(namespace, version);
        if (serialized && this.cacheOnLoad) {
          await this.cacheFilter(namespace, version, serialized);
        }
      }

      if (serialized) {
        const filter = deserializeFilter(serialized);
        for (const entityId of entityIds) {
          results.set(entityId, mightExist(filter, entityId));
        }
      } else {
        // No filter - assume all might exist
        for (const entityId of entityIds) {
          results.set(entityId, true);
        }
      }
    } catch (error) {
      console.error('BloomRouter checkEntities error:', error);
      // Assume all might exist on error
      for (const entityId of entityIds) {
        results.set(entityId, true);
      }
    }

    return results;
  }

  /**
   * Cache a bloom filter with immutable headers
   *
   * @param namespace - The namespace this filter covers
   * @param version - Version string for content addressing
   * @param filter - The serialized bloom filter
   */
  async cacheFilter(
    namespace: Namespace,
    version: string,
    filter: SerializedFilter
  ): Promise<void> {
    await this.cache.putFilter(namespace, version, filter);
  }

  /**
   * Generate Cache-Control headers for bloom filter responses
   *
   * Use these headers when serving bloom filter data to clients
   * to enable downstream edge caching.
   *
   * @param version - The bloom filter version
   * @returns Headers object
   */
  generateResponseHeaders(version: string): Record<string, string> {
    return generateBloomCacheHeaders(version);
  }

  /**
   * Invalidate a cached bloom filter
   *
   * Note: With content-addressed caching, this is rarely needed.
   * New versions get new cache keys automatically.
   *
   * @param namespace - The namespace
   * @param version - The version to invalidate
   * @returns True if entry was deleted
   */
  async invalidateFilter(namespace: Namespace, version: string): Promise<boolean> {
    return this.cache.invalidateFilter(namespace, version);
  }
}
