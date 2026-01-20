/**
 * Cache Module for GraphDB
 *
 * Provides edge caching integration for GraphCol chunks and manifests.
 * Uses Cloudflare's Cache API with tag-based invalidation.
 *
 * @example
 * ```typescript
 * import {
 *   ChunkEdgeCache,
 *   CacheInvalidator,
 *   CacheMetricsCollector,
 * } from '@dotdo/graphdb/cache';
 *
 * // Create edge cache for chunks
 * const cache = new ChunkEdgeCache({
 *   maxAge: 3600,
 *   namespace: 'graphdb',
 * });
 *
 * // Cache a chunk
 * await cache.put(chunkId, data);
 *
 * // Retrieve from cache
 * const cached = await cache.get(chunkId);
 * ```
 *
 * @packageDocumentation
 */

export {
  // Types
  type ChunkCacheConfig,
  type ManifestCacheConfig,
  type CacheableChunk,
  type CacheableManifest,
  type CachedChunkResponse,
  type CacheMetricsData,
  type ParsedChunkCacheUrl,
  /** Default max-age for chunk cache entries in seconds */
  DEFAULT_CHUNK_MAX_AGE,
  /** Default max-age for manifest cache entries in seconds */
  DEFAULT_MANIFEST_MAX_AGE,
  /** Default stale-while-revalidate window for manifests in seconds */
  DEFAULT_MANIFEST_SWR,
  /**
   * Generate a cache-friendly URL for a GraphCol chunk.
   * @param namespace - The namespace of the chunk
   * @param chunkId - The unique chunk identifier
   * @returns A URL suitable for edge caching
   */
  generateChunkCacheUrl,
  /**
   * Generate a cache-friendly URL for a manifest file.
   * @param namespace - The namespace of the manifest
   * @returns A URL suitable for edge caching with SWR
   */
  generateManifestCacheUrl,
  /**
   * Parse a chunk cache URL to extract components.
   * @param url - The cache URL to parse
   * @returns Parsed namespace and chunk ID
   */
  parseChunkCacheUrl,
  /**
   * Generate Cache-Control headers for chunk responses.
   * @param config - Cache configuration options
   * @returns Headers object with appropriate cache directives
   */
  generateChunkCacheHeaders,
  /**
   * Generate Cache-Control headers for manifest responses.
   * Includes stale-while-revalidate for seamless updates.
   * @param config - Cache configuration options
   * @returns Headers object with SWR directives
   */
  generateManifestCacheHeaders,
  /**
   * Edge cache manager for GraphCol chunks.
   * Handles caching, retrieval, and TTL management.
   */
  ChunkEdgeCache,
} from './edge-cache.js';

export {
  // Types
  type InvalidationConfig,
  type InvalidationResult,
  type CompactionInvalidationEvent,
  type NamespaceInvalidationOptions,
  type InvalidationMetrics,
  /**
   * Create a cache tag for a specific chunk.
   * @param namespace - The namespace
   * @param chunkId - The chunk identifier
   * @returns A cache tag string for purge requests
   */
  createChunkTag,
  /**
   * Create cache tags for an entire namespace.
   * @param namespace - The namespace to tag
   * @returns Array of cache tags for namespace-wide invalidation
   */
  createNamespaceTags,
  /**
   * Create invalidation tags from a compaction event.
   * @param event - The compaction event with affected chunks
   * @returns Array of tags to invalidate
   */
  createInvalidationTags,
  /**
   * Cache invalidation manager for handling purge operations.
   * Supports tag-based invalidation for efficient cache management.
   */
  CacheInvalidator,
} from './cache-invalidation.js';

export {
  // Types
  type MetricsConfig,
  type HitOptions,
  type MissOptions,
  type InvalidationOptions,
  type CacheMetrics,
  type RequestMetric,
  type MetricsSnapshot,
  type SnapshotComparison,
  /** Default metrics collection window in milliseconds */
  METRICS_WINDOW_DEFAULT,
  /**
   * Calculate cache hit rate from metrics.
   * @param metrics - Cache metrics snapshot
   * @returns Hit rate as a decimal between 0 and 1
   */
  calculateHitRate,
  /**
   * Calculate cache miss rate from metrics.
   * @param metrics - Cache metrics snapshot
   * @returns Miss rate as a decimal between 0 and 1
   */
  calculateMissRate,
  /**
   * Format metrics into a human-readable report.
   * @param metrics - Cache metrics snapshot
   * @returns Formatted report string with hit rates and latency
   */
  formatMetricsReport,
  /**
   * Cache metrics collector for monitoring performance.
   * Tracks hit rates, miss rates, latency, and invalidation counts.
   */
  CacheMetricsCollector,
} from './cache-metrics.js';
