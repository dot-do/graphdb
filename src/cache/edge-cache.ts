/**
 * Edge Cache Integration for GraphDB
 *
 * Implements cache headers and key design for GraphCol chunks and manifests.
 * Uses Cloudflare's Cache API (caches.default) to cache data at the edge.
 *
 * URL Pattern Design:
 * - Chunks: /graphdb/v1/{namespace}/chunks/{chunk-id}.gcol
 *   Cache-Control: public, max-age=31536000, immutable
 * - Manifests: /graphdb/v1/{namespace}/manifest.json
 *   Cache-Control: public, max-age=60, stale-while-revalidate=300
 *
 * Key Features:
 * - Immutable cache headers for chunks (1 year TTL)
 * - Short TTL with stale-while-revalidate for manifests
 * - Cache hit/miss metrics tracking
 * - Graceful error handling (cache failures don't break the app)
 *
 * @packageDocumentation
 */

import { type Namespace } from '../core/types.js';

// ============================================================================
// Constants
// ============================================================================

/** Default max-age for chunks (1 year in seconds) */
export const DEFAULT_CHUNK_MAX_AGE = 31536000;

/** Default max-age for manifests (1 minute in seconds) */
export const DEFAULT_MANIFEST_MAX_AGE = 60;

/** Default stale-while-revalidate for manifests (5 minutes in seconds) */
export const DEFAULT_MANIFEST_SWR = 300;

/** Cache key domain for edge cache URLs */
const CACHE_DOMAIN = 'https://graphdb-edge-cache.internal';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for chunk caching
 */
export interface ChunkCacheConfig {
  /** Max-age in seconds (default: 1 year) */
  maxAge?: number;
  /** Whether to use immutable directive (default: true) */
  immutable?: boolean;
  /** Cache key prefix (default: 'graphdb/v1') */
  cacheKeyPrefix?: string;
  /** Track hit/miss metrics (default: false) */
  trackMetrics?: boolean;
}

/**
 * Configuration for manifest caching
 */
export interface ManifestCacheConfig {
  /** Max-age in seconds (default: 60) */
  maxAge?: number;
  /** Stale-while-revalidate in seconds (default: 300) */
  swr?: number;
}

/**
 * A chunk that can be cached
 */
export interface CacheableChunk {
  /** Chunk ID */
  chunkId: string;
  /** Chunk data */
  data: Uint8Array;
  /** Namespace */
  namespace: Namespace;
}

/**
 * A manifest that can be cached
 */
export interface CacheableManifest {
  /** Manifest version */
  version: number;
  /** List of chunk IDs */
  chunks: string[];
  /** Last update timestamp */
  lastUpdated: number;
  /** Optional additional metadata */
  metadata?: Record<string, unknown>;
}

/**
 * Response from cache lookup for a chunk
 */
export interface CachedChunkResponse {
  /** Chunk ID */
  chunkId: string;
  /** Chunk data */
  data: Uint8Array;
  /** Cache metadata */
  metadata: {
    /** Whether this was a cache hit */
    cacheHit: boolean;
    /** Age of cached entry in seconds */
    age: number;
    /** When the entry was cached */
    cachedAt: number;
  };
}

/**
 * Cache metrics
 */
export interface CacheMetricsData {
  /** Number of cache hits */
  hits: number;
  /** Number of cache misses */
  misses: number;
  /** Hit rate (hits / total) */
  hitRate: number;
}

/**
 * Parsed cache URL components
 */
export interface ParsedChunkCacheUrl {
  /** Namespace */
  namespace: Namespace;
  /** Chunk ID */
  chunkId: string;
}

// ============================================================================
// Cache URL Generation
// ============================================================================

/**
 * Generate a cache URL for a chunk
 *
 * Format: https://graphdb-edge-cache.internal/graphdb/v1/{encoded-namespace}/chunks/{chunk-id}.gcol
 *
 * @param namespace - The namespace
 * @param chunkId - The chunk identifier
 * @param prefix - Optional cache key prefix (default: 'graphdb/v1')
 * @returns Cache URL
 */
export function generateChunkCacheUrl(
  namespace: Namespace,
  chunkId: string,
  prefix: string = 'graphdb/v1'
): string {
  const encodedNamespace = encodeURIComponent(namespace);
  return `${CACHE_DOMAIN}/${prefix}/${encodedNamespace}/chunks/${chunkId}.gcol`;
}

/**
 * Generate a cache URL for a manifest
 *
 * Format: https://graphdb-edge-cache.internal/graphdb/v1/{encoded-namespace}/manifest.json
 *
 * @param namespace - The namespace
 * @param prefix - Optional cache key prefix (default: 'graphdb/v1')
 * @returns Cache URL
 */
export function generateManifestCacheUrl(
  namespace: Namespace,
  prefix: string = 'graphdb/v1'
): string {
  const encodedNamespace = encodeURIComponent(namespace);
  return `${CACHE_DOMAIN}/${prefix}/${encodedNamespace}/manifest.json`;
}

/**
 * Parse a chunk cache URL back to its components
 *
 * @param url - The cache URL
 * @returns Parsed components
 */
export function parseChunkCacheUrl(url: string): ParsedChunkCacheUrl {
  const urlObj = new URL(url);
  const parts = urlObj.pathname.split('/').filter(Boolean);

  // Expected format: [prefix, 'v1', encoded-namespace, 'chunks', 'chunk-id.gcol']
  // or [prefix, encoded-namespace, 'chunks', 'chunk-id.gcol'] if prefix is single segment

  // Find chunks index
  const chunksIndex = parts.findIndex(p => p === 'chunks');
  if (chunksIndex === -1 || chunksIndex < 1) {
    throw new Error(`Invalid chunk cache URL: ${url}`);
  }

  const encodedNamespace = parts[chunksIndex - 1]!;
  const chunkIdWithExt = parts[chunksIndex + 1]!;
  const chunkId = chunkIdWithExt.replace(/\.gcol$/, '');

  return {
    namespace: decodeURIComponent(encodedNamespace) as Namespace,
    chunkId,
  };
}

// ============================================================================
// Cache Header Generation
// ============================================================================

/**
 * Generate Cache-Control headers for a chunk
 *
 * Chunks use immutable headers since chunk IDs are content-addressed.
 *
 * @param chunkId - The chunk identifier
 * @param options - Optional configuration
 * @returns Headers object
 */
export function generateChunkCacheHeaders(
  chunkId: string,
  options: { maxAge?: number; immutable?: boolean } = {}
): Record<string, string> {
  const maxAge = options.maxAge ?? DEFAULT_CHUNK_MAX_AGE;
  const immutable = options.immutable ?? true;

  let cacheControl = `public, max-age=${maxAge}, s-maxage=${maxAge}`;
  if (immutable) {
    cacheControl += ', immutable';
  }

  return {
    'Content-Type': 'application/octet-stream',
    'Cache-Control': cacheControl,
    'X-Chunk-Id': chunkId,
    'X-Cache-Timestamp': Date.now().toString(),
  };
}

/**
 * Generate Cache-Control headers for a manifest
 *
 * Manifests use short TTLs with stale-while-revalidate for freshness.
 *
 * @param options - Optional configuration
 * @returns Headers object
 */
export function generateManifestCacheHeaders(
  options: ManifestCacheConfig = {}
): Record<string, string> {
  const maxAge = options.maxAge ?? DEFAULT_MANIFEST_MAX_AGE;
  const swr = options.swr ?? DEFAULT_MANIFEST_SWR;

  return {
    'Content-Type': 'application/json',
    'Cache-Control': `public, max-age=${maxAge}, s-maxage=${maxAge}, stale-while-revalidate=${swr}`,
    'X-Cache-Timestamp': Date.now().toString(),
  };
}

// ============================================================================
// ChunkEdgeCache Class
// ============================================================================

/**
 * Edge cache manager for GraphCol chunks and manifests
 */
export class ChunkEdgeCache {
  readonly config: Required<ChunkCacheConfig>;
  private _hits: number = 0;
  private _misses: number = 0;

  constructor(config: ChunkCacheConfig = {}) {
    this.config = {
      maxAge: config.maxAge ?? DEFAULT_CHUNK_MAX_AGE,
      immutable: config.immutable ?? true,
      cacheKeyPrefix: config.cacheKeyPrefix ?? 'graphdb/v1',
      trackMetrics: config.trackMetrics ?? false,
    };
  }

  // ==========================================================================
  // Chunk Operations
  // ==========================================================================

  /**
   * Get a chunk from edge cache
   *
   * @param namespace - The namespace
   * @param chunkId - The chunk identifier
   * @returns Cached chunk response or null on miss
   */
  async getChunk(namespace: Namespace, chunkId: string): Promise<CachedChunkResponse | null> {
    try {
      const cacheUrl = generateChunkCacheUrl(namespace, chunkId, this.config.cacheKeyPrefix);
      const request = new Request(cacheUrl);

      const cache = caches.default;
      const response = await cache.match(request);

      if (!response) {
        if (this.config.trackMetrics) {
          this._misses++;
        }
        return null;
      }

      if (this.config.trackMetrics) {
        this._hits++;
      }

      const data = new Uint8Array(await response.arrayBuffer());
      const cachedAt = parseInt(response.headers.get('X-Cache-Timestamp') || '0', 10);
      const age = Math.floor((Date.now() - cachedAt) / 1000);

      return {
        chunkId: response.headers.get('X-Chunk-Id') || chunkId,
        data,
        metadata: {
          cacheHit: true,
          age,
          cachedAt,
        },
      };
    } catch (error) {
      console.error('ChunkEdgeCache getChunk error:', error);
      if (this.config.trackMetrics) {
        this._misses++;
      }
      return null;
    }
  }

  /**
   * Store a chunk in edge cache
   *
   * @param namespace - The namespace
   * @param chunkId - The chunk identifier
   * @param data - The chunk data
   */
  async putChunk(namespace: Namespace, chunkId: string, data: Uint8Array): Promise<void> {
    try {
      const cacheUrl = generateChunkCacheUrl(namespace, chunkId, this.config.cacheKeyPrefix);
      const request = new Request(cacheUrl);

      const headers = generateChunkCacheHeaders(chunkId, {
        maxAge: this.config.maxAge,
        immutable: this.config.immutable,
      });

      const response = new Response(data, { headers });

      const cache = caches.default;
      await cache.put(request, response);
    } catch (error) {
      console.error('ChunkEdgeCache putChunk error:', error);
    }
  }

  /**
   * Delete a chunk from edge cache
   *
   * @param namespace - The namespace
   * @param chunkId - The chunk identifier
   * @returns True if chunk was deleted
   */
  async deleteChunk(namespace: Namespace, chunkId: string): Promise<boolean> {
    try {
      const cacheUrl = generateChunkCacheUrl(namespace, chunkId, this.config.cacheKeyPrefix);
      const request = new Request(cacheUrl);

      const cache = caches.default;
      return await cache.delete(request);
    } catch (error) {
      console.error('ChunkEdgeCache deleteChunk error:', error);
      return false;
    }
  }

  // ==========================================================================
  // Manifest Operations
  // ==========================================================================

  /**
   * Get a manifest from edge cache
   *
   * @param namespace - The namespace
   * @returns Cached manifest or null on miss
   */
  async getManifest(namespace: Namespace): Promise<CacheableManifest | null> {
    try {
      const cacheUrl = generateManifestCacheUrl(namespace, this.config.cacheKeyPrefix);
      const request = new Request(cacheUrl);

      const cache = caches.default;
      const response = await cache.match(request);

      if (!response) {
        if (this.config.trackMetrics) {
          this._misses++;
        }
        return null;
      }

      if (this.config.trackMetrics) {
        this._hits++;
      }

      const manifest = await response.json() as CacheableManifest;
      return manifest;
    } catch (error) {
      console.error('ChunkEdgeCache getManifest error:', error);
      if (this.config.trackMetrics) {
        this._misses++;
      }
      return null;
    }
  }

  /**
   * Store a manifest in edge cache
   *
   * @param namespace - The namespace
   * @param manifest - The manifest data
   * @param options - Optional cache configuration
   */
  async putManifest(
    namespace: Namespace,
    manifest: CacheableManifest,
    options: ManifestCacheConfig = {}
  ): Promise<void> {
    try {
      const cacheUrl = generateManifestCacheUrl(namespace, this.config.cacheKeyPrefix);
      const request = new Request(cacheUrl);

      const headers = generateManifestCacheHeaders(options);

      const response = new Response(JSON.stringify(manifest), { headers });

      const cache = caches.default;
      await cache.put(request, response);
    } catch (error) {
      console.error('ChunkEdgeCache putManifest error:', error);
    }
  }

  /**
   * Delete a manifest from edge cache
   *
   * @param namespace - The namespace
   * @returns True if manifest was deleted
   */
  async deleteManifest(namespace: Namespace): Promise<boolean> {
    try {
      const cacheUrl = generateManifestCacheUrl(namespace, this.config.cacheKeyPrefix);
      const request = new Request(cacheUrl);

      const cache = caches.default;
      return await cache.delete(request);
    } catch (error) {
      console.error('ChunkEdgeCache deleteManifest error:', error);
      return false;
    }
  }

  // ==========================================================================
  // Metrics
  // ==========================================================================

  /**
   * Get cache metrics
   *
   * @returns Metrics data
   */
  getMetrics(): CacheMetricsData {
    const total = this._hits + this._misses;
    return {
      hits: this._hits,
      misses: this._misses,
      hitRate: total > 0 ? this._hits / total : 0,
    };
  }

  /**
   * Reset cache metrics
   */
  resetMetrics(): void {
    this._hits = 0;
    this._misses = 0;
  }
}
