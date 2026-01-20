/**
 * Edge Cache for Broker DO
 *
 * Integrates edge caching into the broker layer to reduce DO invocations.
 * The broker checks edge cache before hitting DOs and populates cache after responses.
 *
 * Key features:
 * - Check edge cache before DO requests
 * - Populate cache after DO responses
 * - Cache tags for selective invalidation
 * - Configurable TTLs and caching policies
 */

import { type Namespace } from '../core/types.js';
import { generateCacheKey as generateQueryCacheKey, canServeFromCache } from '../snippet/router.js';

// ============================================================================
// Constants
// ============================================================================

/** Default TTL for query responses (5 minutes) */
const DEFAULT_QUERY_TTL = 300;

/** Maximum TTL allowed (1 hour) */
const DEFAULT_MAX_TTL = 3600;

/** TTL for negative cache entries (30 seconds) */
const DEFAULT_NEGATIVE_TTL = 30;

/** Cache key domain */
const CACHE_DOMAIN = 'https://broker-cache.graphdb.internal';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for broker edge cache
 */
export interface BrokerCacheConfig {
  /** Default TTL for cached responses */
  defaultTtl?: number;
  /** Maximum allowed TTL */
  maxTtl?: number;
  /** Prefix for cache keys */
  cacheKeyPrefix?: string;
  /** Enable caching of 404 responses */
  enableNegativeCaching?: boolean;
  /** TTL for negative cache entries */
  negativeCacheTtl?: number;
}

/**
 * Type of cacheable request
 */
export type RequestType = 'query' | 'mutation' | 'subscription';

/**
 * A request that may be cacheable
 */
export interface CacheableRequest {
  /** Type of request */
  type: RequestType;
  /** Namespace being queried */
  namespace: Namespace;
  /** The query string */
  query: string;
  /** Pre-computed cache key */
  cacheKey?: string;
  /** Cache tags for invalidation */
  cacheTags?: string[];
  /** TTL hint */
  ttl?: number;
  /** Skip cache */
  noCache?: boolean;
}

/**
 * Response from DO that may be cached
 */
export interface CacheableResponse {
  /** HTTP status code */
  status: number;
  /** Response data */
  data?: unknown;
  /** Error message if any */
  error?: string;
  /** Response headers */
  headers?: Record<string, string>;
}

/**
 * Cached response with metadata
 */
export interface CachedResponse {
  /** The cached data */
  data: unknown;
  /** Cache metadata */
  metadata: {
    /** Whether this was a cache hit */
    cacheHit: boolean;
    /** Age of cached entry in seconds */
    age: number;
    /** Cache tags */
    tags: string[];
    /** When the entry was cached */
    cachedAt: number;
  };
}

/**
 * Result of cache invalidation
 */
export interface InvalidationResult {
  /** Whether the operation succeeded */
  success: boolean;
  /** Number of entries invalidated */
  invalidatedCount: number;
  /** Any errors encountered */
  errors?: string[];
}

/**
 * Options for checking if response should be cached
 */
export interface ShouldCacheOptions {
  /** Cache 404 responses */
  cacheNotFound?: boolean;
}

// ============================================================================
// Cache Tag Functions
// ============================================================================

/**
 * Create cache tags for a namespace
 *
 * @param namespace - The namespace
 * @returns Array of cache tags
 */
export function createCacheTagsForNamespace(namespace: Namespace): string[] {
  const tags: string[] = [];

  try {
    const url = new URL(namespace);

    // Namespace-level tag
    tags.push(`ns:${url.host}${url.pathname}`);

    // Host-level tag for broader invalidation
    tags.push(`host:${url.host}`);
  } catch {
    // Fallback for non-URL namespaces
    tags.push(`ns:${namespace}`);
  }

  return tags;
}

/**
 * Create cache tags for a specific query
 *
 * @param namespace - The namespace
 * @param query - The query string
 * @returns Array of cache tags
 */
export function createCacheTagsForQuery(namespace: Namespace, query: string): string[] {
  const tags = createCacheTagsForNamespace(namespace);

  // Extract entity ID from query
  const entityMatch = query.match(/https?:\/\/[^\s\[\]()'"\.]+/);
  if (entityMatch) {
    try {
      const entityUrl = new URL(entityMatch[0]);
      tags.push(`entity:${entityUrl.pathname}`);
    } catch {
      // Ignore URL parse errors
    }
  }

  // Extract property traversals (e.g., .friends, .name)
  const propertyMatches = query.match(/\.([a-z_][a-z0-9_]*)/gi);
  if (propertyMatches) {
    for (const match of propertyMatches) {
      const prop = match.slice(1).toLowerCase();
      // Skip TLDs
      if (!['com', 'org', 'net', 'io', 'dev'].includes(prop)) {
        tags.push(`prop:${prop}`);
      }
    }
  }

  return [...new Set(tags)]; // Deduplicate
}

// ============================================================================
// Response Caching Helpers
// ============================================================================

/**
 * Determine if a response should be cached
 *
 * @param response - The response to check
 * @param options - Caching options
 * @returns True if response should be cached
 */
export function shouldCacheResponse(
  response: CacheableResponse,
  options: ShouldCacheOptions = {}
): boolean {
  // Check for no-store header
  if (response.headers?.['Cache-Control']?.includes('no-store')) {
    return false;
  }

  // Cache successful responses
  if (response.status >= 200 && response.status < 300) {
    return true;
  }

  // Optionally cache 404 for negative caching
  if (response.status === 404 && options.cacheNotFound) {
    return true;
  }

  // Don't cache errors
  return false;
}

/**
 * Extract cacheable request from HTTP request and body
 *
 * @param request - The HTTP request
 * @param namespace - The namespace
 * @param body - Parsed request body
 * @returns Cacheable request object
 */
export function extractCacheableRequest(
  request: Request,
  namespace: Namespace,
  body: { query: string }
): CacheableRequest {
  const query = body.query;

  // Determine request type
  let type: RequestType = 'query';
  const upperQuery = query.toUpperCase();
  if (
    upperQuery.includes('MUTATE') ||
    upperQuery.includes('INSERT') ||
    upperQuery.includes('DELETE') ||
    upperQuery.includes('UPDATE')
  ) {
    type = 'mutation';
  } else if (upperQuery.includes('SUBSCRIBE')) {
    type = 'subscription';
  }

  // Generate cache key
  const cacheKey = type === 'query' ? generateQueryCacheKey(query) : undefined;

  // Generate cache tags
  const cacheTags = type === 'query' ? createCacheTagsForQuery(namespace, query) : undefined;

  // Extract TTL hint from headers
  const ttlHeader = request.headers.get('X-Cache-TTL');
  const ttl = ttlHeader ? parseInt(ttlHeader, 10) : undefined;

  // Check for no-cache directive
  const cacheControl = request.headers.get('Cache-Control');
  const noCache = cacheControl?.includes('no-cache') || cacheControl?.includes('no-store');

  const result: CacheableRequest = {
    type,
    namespace,
    query,
  };

  if (cacheKey !== undefined) {
    result.cacheKey = cacheKey;
  }
  if (cacheTags !== undefined) {
    result.cacheTags = cacheTags;
  }
  if (ttl !== undefined) {
    result.ttl = ttl;
  }
  if (noCache) {
    result.noCache = noCache;
  }

  return result;
}

// ============================================================================
// BrokerEdgeCache Class
// ============================================================================

/**
 * Edge cache manager for broker DO
 */
export class BrokerEdgeCache {
  readonly config: Required<BrokerCacheConfig>;

  constructor(config: BrokerCacheConfig = {}) {
    this.config = {
      defaultTtl: config.defaultTtl ?? DEFAULT_QUERY_TTL,
      maxTtl: config.maxTtl ?? DEFAULT_MAX_TTL,
      cacheKeyPrefix: config.cacheKeyPrefix ?? 'broker',
      enableNegativeCaching: config.enableNegativeCaching ?? false,
      negativeCacheTtl: config.negativeCacheTtl ?? DEFAULT_NEGATIVE_TTL,
    };
  }

  /**
   * Create a cache key URL for the request
   */
  private createCacheUrl(request: CacheableRequest): string {
    const key = request.cacheKey || generateQueryCacheKey(request.query);
    const encodedNamespace = encodeURIComponent(request.namespace);
    return `${CACHE_DOMAIN}/${this.config.cacheKeyPrefix}/${encodedNamespace}/${key}`;
  }

  /**
   * Determine if a request should use caching
   *
   * @param request - The request to check
   * @returns True if caching should be used
   */
  shouldCache(request: CacheableRequest): boolean {
    // Never cache mutations or subscriptions
    if (request.type !== 'query') {
      return false;
    }

    // Respect no-cache directive
    if (request.noCache) {
      return false;
    }

    // Use router's cache check for query analysis
    return canServeFromCache(request.query);
  }

  /**
   * Check edge cache for a cached response
   *
   * @param request - The cacheable request
   * @returns Cached response or null on miss
   */
  async checkCache(request: CacheableRequest): Promise<CachedResponse | null> {
    // Skip cache for non-cacheable requests
    if (!this.shouldCache(request)) {
      return null;
    }

    try {
      const cacheUrl = this.createCacheUrl(request);
      const cacheRequest = new Request(cacheUrl);

      const cache = caches.default;
      const response = await cache.match(cacheRequest);

      if (!response) {
        return null;
      }

      // Parse cached data
      const data = await response.json();
      const cachedAt = parseInt(response.headers.get('X-Cache-Timestamp') || '0', 10);
      const age = Math.floor((Date.now() - cachedAt) / 1000);
      const tags = (response.headers.get('Cache-Tag') || '').split(',').filter(Boolean);

      return {
        data,
        metadata: {
          cacheHit: true,
          age,
          tags,
          cachedAt,
        },
      };
    } catch (error) {
      console.error('BrokerEdgeCache checkCache error:', error);
      return null;
    }
  }

  /**
   * Populate edge cache after DO response
   *
   * @param request - The original request
   * @param data - The response data to cache
   */
  async populateCache(request: CacheableRequest, data: unknown): Promise<void> {
    // Skip cache for non-cacheable requests
    if (!this.shouldCache(request)) {
      return;
    }

    try {
      const cacheUrl = this.createCacheUrl(request);
      const cacheRequest = new Request(cacheUrl);

      // Calculate TTL (capped at maxTtl)
      const requestedTtl = request.ttl ?? this.config.defaultTtl;
      const ttl = Math.min(requestedTtl, this.config.maxTtl);

      const now = Date.now();

      // Build Cache-Control header
      const cacheControl = `public, max-age=${ttl}, s-maxage=${ttl}`;

      // Build cache tags header
      const cacheTags = request.cacheTags?.join(',') || '';

      const response = new Response(JSON.stringify(data), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': cacheControl,
          'Cache-Tag': cacheTags,
          'X-Cache-Timestamp': now.toString(),
          'X-Cache-Hit': 'false',
        },
      });

      const cache = caches.default;
      await cache.put(cacheRequest, response);
    } catch (error) {
      console.error('BrokerEdgeCache populateCache error:', error);
    }
  }

  /**
   * Invalidate cache entries by tags
   *
   * Note: Cloudflare Cache API doesn't support tag-based invalidation directly.
   * This is a placeholder for when using Cache Tags with purge API.
   *
   * @param tags - Tags to invalidate
   * @returns Invalidation result
   */
  async invalidateByTags(tags: string[]): Promise<InvalidationResult> {
    // In production, this would call the Cloudflare Purge API with cache tags
    // For now, we simulate the operation
    console.log('Invalidating cache tags:', tags);

    return {
      success: true,
      invalidatedCount: tags.length,
    };
  }

  /**
   * Invalidate all cache entries for a namespace
   *
   * @param namespace - The namespace to invalidate
   * @returns Invalidation result
   */
  async invalidateNamespace(namespace: Namespace): Promise<InvalidationResult> {
    const tags = createCacheTagsForNamespace(namespace);
    const result = await this.invalidateByTags(tags);

    // Also try to delete the base namespace cache key
    try {
      const cacheUrl = `${CACHE_DOMAIN}/${this.config.cacheKeyPrefix}/${encodeURIComponent(namespace)}/`;
      const cacheRequest = new Request(cacheUrl);

      const cache = caches.default;
      await cache.delete(cacheRequest);
    } catch (error) {
      console.error('Error invalidating namespace base key:', error);
    }

    return result;
  }

  /**
   * Invalidate a specific cache entry
   *
   * @param request - The request to invalidate
   * @returns True if entry was deleted
   */
  async invalidateEntry(request: CacheableRequest): Promise<boolean> {
    try {
      const cacheUrl = this.createCacheUrl(request);
      const cacheRequest = new Request(cacheUrl);

      const cache = caches.default;
      return await cache.delete(cacheRequest);
    } catch (error) {
      console.error('BrokerEdgeCache invalidateEntry error:', error);
      return false;
    }
  }
}
