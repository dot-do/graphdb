/**
 * Cache Invalidation for GraphDB
 *
 * Handles purging cache entries when data changes, including:
 * - Single chunk invalidation (on update)
 * - Batch invalidation (on compaction)
 * - Namespace-wide invalidation (on schema changes)
 *
 * Key Features:
 * - Tag-based invalidation for efficient purging
 * - Compaction-triggered invalidation
 * - Retry logic for transient failures
 * - Metrics tracking for monitoring
 *
 * @packageDocumentation
 */

import { type Namespace } from '../core/types.js';
import { generateChunkCacheUrl, generateManifestCacheUrl } from './edge-cache.js';

// ============================================================================
// Constants
// ============================================================================

/** Default batch size for invalidation */
const DEFAULT_BATCH_SIZE = 50;

/** Default retry attempts for failed invalidations */
const DEFAULT_RETRY_ATTEMPTS = 0;

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for cache invalidation
 */
export interface InvalidationConfig {
  /** Number of chunks to invalidate in parallel (default: 50) */
  batchSize?: number;
  /** Number of retry attempts for failed invalidations (default: 0) */
  retryAttempts?: number;
  /** Track invalidation metrics (default: false) */
  trackMetrics?: boolean;
  /** Cache key prefix (default: 'graphdb/v1') */
  cacheKeyPrefix?: string;
}

/**
 * Result of an invalidation operation
 */
export interface InvalidationResult {
  /** Whether the operation succeeded */
  success: boolean;
  /** Number of entries invalidated */
  invalidatedCount: number;
  /** Keys that were invalidated */
  invalidatedKeys?: string[] | undefined;
  /** Any errors encountered */
  errors?: string[] | undefined;
}

/**
 * Event triggered by compaction
 */
export interface CompactionInvalidationEvent {
  /** Namespace of the compacted chunks */
  namespace: Namespace;
  /** Source chunk IDs that were compacted */
  sourceChunks: string[];
  /** Target chunk ID after compaction */
  targetChunk: string;
  /** Timestamp of the compaction */
  timestamp: number;
  /** Compaction level (optional) */
  level?: 'L0_TO_L1' | 'L1_TO_L2';
}

/**
 * Options for namespace invalidation
 */
export interface NamespaceInvalidationOptions {
  /** Include these specific chunk IDs */
  includeChunks?: string[];
  /** Skip manifest invalidation */
  skipManifest?: boolean;
}

/**
 * Invalidation metrics
 */
export interface InvalidationMetrics {
  /** Total invalidation operations */
  totalInvalidations: number;
  /** Failed invalidation operations */
  failedInvalidations: number;
  /** Namespace-level invalidations */
  namespaceInvalidations: number;
  /** Compaction-triggered invalidation events */
  compactionEvents: number;
}

// ============================================================================
// Tag Generation Functions
// ============================================================================

/**
 * Create a cache tag for a specific chunk
 *
 * @param namespace - The namespace
 * @param chunkId - The chunk identifier
 * @returns Cache tag string
 */
export function createChunkTag(namespace: Namespace, chunkId: string): string {
  const encodedNamespace = encodeURIComponent(namespace);
  return `chunk:${encodedNamespace}:${chunkId}`;
}

/**
 * Create cache tags for a namespace
 *
 * @param namespace - The namespace
 * @returns Array of cache tags
 */
export function createNamespaceTags(namespace: Namespace): string[] {
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
 * Create all invalidation tags for a chunk
 *
 * @param namespace - The namespace
 * @param chunkId - The chunk identifier
 * @returns Array of cache tags
 */
export function createInvalidationTags(namespace: Namespace, chunkId: string): string[] {
  const tags = createNamespaceTags(namespace);
  tags.push(createChunkTag(namespace, chunkId));
  return tags;
}

// ============================================================================
// CacheInvalidator Class
// ============================================================================

/**
 * Cache invalidation manager for GraphDB
 */
export class CacheInvalidator {
  readonly config: Required<InvalidationConfig>;
  private _totalInvalidations: number = 0;
  private _failedInvalidations: number = 0;
  private _namespaceInvalidations: number = 0;
  private _compactionEvents: number = 0;

  constructor(config: InvalidationConfig = {}) {
    this.config = {
      batchSize: config.batchSize ?? DEFAULT_BATCH_SIZE,
      retryAttempts: config.retryAttempts ?? DEFAULT_RETRY_ATTEMPTS,
      trackMetrics: config.trackMetrics ?? false,
      cacheKeyPrefix: config.cacheKeyPrefix ?? 'graphdb/v1',
    };
  }

  // ==========================================================================
  // Single Chunk Invalidation
  // ==========================================================================

  /**
   * Invalidate a single chunk from cache
   *
   * @param namespace - The namespace
   * @param chunkId - The chunk identifier
   * @returns Invalidation result
   */
  async invalidateChunk(namespace: Namespace, chunkId: string): Promise<InvalidationResult> {
    const errors: string[] = [];
    let invalidatedCount = 0;

    try {
      const cacheUrl = generateChunkCacheUrl(namespace, chunkId, this.config.cacheKeyPrefix);
      const deleted = await this.deleteWithRetry(cacheUrl);

      if (deleted) {
        invalidatedCount = 1;
      }

      if (this.config.trackMetrics) {
        this._totalInvalidations++;
      }

      return {
        success: true,
        invalidatedCount,
        invalidatedKeys: invalidatedCount > 0 ? [chunkId] : [],
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      errors.push(errorMessage);

      if (this.config.trackMetrics) {
        this._totalInvalidations++;
        this._failedInvalidations++;
      }

      return {
        success: false,
        invalidatedCount: 0,
        errors,
      };
    }
  }

  // ==========================================================================
  // Batch Chunk Invalidation
  // ==========================================================================

  /**
   * Invalidate multiple chunks from cache
   *
   * @param namespace - The namespace
   * @param chunkIds - Array of chunk identifiers
   * @returns Invalidation result
   */
  async invalidateChunks(namespace: Namespace, chunkIds: string[]): Promise<InvalidationResult> {
    if (chunkIds.length === 0) {
      return {
        success: true,
        invalidatedCount: 0,
        invalidatedKeys: [],
      };
    }

    const errors: string[] = [];
    const invalidatedKeys: string[] = [];
    let invalidatedCount = 0;

    // Process in batches
    for (let i = 0; i < chunkIds.length; i += this.config.batchSize) {
      const batch = chunkIds.slice(i, i + this.config.batchSize);

      const results = await Promise.allSettled(
        batch.map(async (chunkId) => {
          const cacheUrl = generateChunkCacheUrl(namespace, chunkId, this.config.cacheKeyPrefix);
          const deleted = await this.deleteWithRetry(cacheUrl);
          return { chunkId, deleted };
        })
      );

      for (const result of results) {
        if (result.status === 'fulfilled' && result.value.deleted) {
          invalidatedCount++;
          invalidatedKeys.push(result.value.chunkId);
        } else if (result.status === 'rejected') {
          const errorMessage = result.reason instanceof Error ? result.reason.message : String(result.reason);
          errors.push(errorMessage);
          if (this.config.trackMetrics) {
            this._failedInvalidations++;
          }
        }
      }
    }

    if (this.config.trackMetrics) {
      this._totalInvalidations += chunkIds.length;
    }

    return {
      success: errors.length === 0,
      invalidatedCount,
      invalidatedKeys,
      errors: errors.length > 0 ? errors : undefined,
    };
  }

  // ==========================================================================
  // Namespace Invalidation
  // ==========================================================================

  /**
   * Invalidate all cache entries for a namespace
   *
   * @param namespace - The namespace
   * @param options - Invalidation options
   * @returns Invalidation result
   */
  async invalidateNamespace(
    namespace: Namespace,
    options: NamespaceInvalidationOptions = {}
  ): Promise<InvalidationResult> {
    const errors: string[] = [];
    const invalidatedKeys: string[] = [];
    let invalidatedCount = 0;

    // Invalidate manifest unless skipped
    if (!options.skipManifest) {
      try {
        const manifestUrl = generateManifestCacheUrl(namespace, this.config.cacheKeyPrefix);
        const deleted = await this.deleteWithRetry(manifestUrl);
        if (deleted) {
          invalidatedCount++;
          invalidatedKeys.push('manifest');
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        errors.push(`Manifest: ${errorMessage}`);
      }
    }

    // Invalidate specified chunks
    if (options.includeChunks && options.includeChunks.length > 0) {
      const chunkResult = await this.invalidateChunks(namespace, options.includeChunks);
      invalidatedCount += chunkResult.invalidatedCount;
      if (chunkResult.invalidatedKeys) {
        invalidatedKeys.push(...chunkResult.invalidatedKeys);
      }
      if (chunkResult.errors) {
        errors.push(...chunkResult.errors);
      }
    }

    if (this.config.trackMetrics) {
      this._namespaceInvalidations++;
    }

    return {
      success: errors.length === 0,
      invalidatedCount,
      invalidatedKeys,
      errors: errors.length > 0 ? errors : undefined,
    };
  }

  // ==========================================================================
  // Compaction Invalidation
  // ==========================================================================

  /**
   * Handle cache invalidation after compaction
   *
   * Invalidates all source chunks that were compacted into a new chunk,
   * and also invalidates the manifest to reflect the new chunk list.
   *
   * @param event - Compaction event details
   * @returns Invalidation result
   */
  async onCompaction(event: CompactionInvalidationEvent): Promise<InvalidationResult> {
    const { namespace, sourceChunks } = event;

    // Invalidate all source chunks (they no longer exist)
    const chunkResult = await this.invalidateChunks(namespace, sourceChunks);

    // Also invalidate the manifest (chunk list has changed)
    const manifestUrl = generateManifestCacheUrl(namespace, this.config.cacheKeyPrefix);
    const request = new Request(manifestUrl);

    try {
      const cache = caches.default;
      await cache.delete(request);
    } catch (error) {
      console.error('Failed to invalidate manifest after compaction:', error);
    }

    if (this.config.trackMetrics) {
      this._compactionEvents++;
    }

    return {
      success: chunkResult.success,
      invalidatedCount: chunkResult.invalidatedCount + 1, // +1 for manifest
      invalidatedKeys: [...(chunkResult.invalidatedKeys || [])],
      errors: chunkResult.errors,
    };
  }

  // ==========================================================================
  // Helper Methods
  // ==========================================================================

  /**
   * Delete a cache entry with retry logic
   *
   * @param cacheUrl - The cache URL to delete
   * @returns True if deleted
   */
  private async deleteWithRetry(cacheUrl: string): Promise<boolean> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= this.config.retryAttempts; attempt++) {
      try {
        const request = new Request(cacheUrl);
        const cache = caches.default;
        return await cache.delete(request);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.config.retryAttempts) {
          // Brief delay before retry
          await new Promise((resolve) => setTimeout(resolve, 10 * (attempt + 1)));
        }
      }
    }

    throw lastError || new Error('Delete failed');
  }

  // ==========================================================================
  // Metrics
  // ==========================================================================

  /**
   * Get invalidation metrics
   *
   * @returns Metrics data
   */
  getMetrics(): InvalidationMetrics {
    return {
      totalInvalidations: this._totalInvalidations,
      failedInvalidations: this._failedInvalidations,
      namespaceInvalidations: this._namespaceInvalidations,
      compactionEvents: this._compactionEvents,
    };
  }

  /**
   * Reset invalidation metrics
   */
  resetMetrics(): void {
    this._totalInvalidations = 0;
    this._failedInvalidations = 0;
    this._namespaceInvalidations = 0;
    this._compactionEvents = 0;
  }
}
