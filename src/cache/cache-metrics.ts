/**
 * Cache Metrics for GraphDB
 *
 * Tracks cache performance metrics including:
 * - Hit rate (target: > 90% for repeat reads)
 * - Miss rate
 * - Latency tracking (average, p95)
 * - Invalidation events
 * - Bytes saved from cache
 *
 * Key Features:
 * - Time-window based metrics (rolling window)
 * - Per-namespace tracking (optional)
 * - Detailed request logging (optional)
 * - Snapshot and comparison support
 *
 * @packageDocumentation
 */

// ============================================================================
// Constants
// ============================================================================

/** Default metrics window in milliseconds (5 minutes) */
export const METRICS_WINDOW_DEFAULT = 300000;

/** Default max detail entries to prevent memory issues */
const DEFAULT_MAX_DETAIL_ENTRIES = 1000;

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for metrics collection
 */
export interface MetricsConfig {
  /** Time window for metrics in milliseconds (default: 5 minutes) */
  windowMs?: number;
  /** Track metrics per namespace (default: false) */
  trackByNamespace?: boolean;
  /** Track individual request details (default: false) */
  trackDetails?: boolean;
  /** Maximum detail entries to store (default: 1000) */
  maxDetailEntries?: number;
}

/**
 * Options for recording a hit
 */
export interface HitOptions {
  /** Latency in milliseconds */
  latencyMs?: number;
  /** Namespace for per-namespace tracking */
  namespace?: string;
  /** Bytes served */
  bytesServed?: number;
}

/**
 * Options for recording a miss
 */
export interface MissOptions {
  /** Latency in milliseconds */
  latencyMs?: number;
  /** Namespace for per-namespace tracking */
  namespace?: string;
  /** Bytes served (from origin) */
  bytesServed?: number;
}

/**
 * Options for recording an invalidation
 */
export interface InvalidationOptions {
  /** Reason for invalidation */
  reason?: string;
  /** Namespace for per-namespace tracking */
  namespace?: string;
}

/**
 * Cache metrics data
 */
export interface CacheMetrics {
  /** Number of cache hits */
  hits: number;
  /** Number of cache misses */
  misses: number;
  /** Number of invalidations */
  invalidations: number;
  /** Hit rate (hits / total) */
  hitRate: number;
  /** Miss rate (misses / total) */
  missRate: number;
  /** Total requests */
  totalRequests: number;
  /** Average hit latency in milliseconds */
  averageHitLatencyMs: number;
  /** Average miss latency in milliseconds */
  averageMissLatencyMs: number;
  /** P95 hit latency in milliseconds */
  p95HitLatencyMs?: number | undefined;
  /** P95 miss latency in milliseconds */
  p95MissLatencyMs?: number | undefined;
  /** Invalidations by reason */
  invalidationsByReason: Record<string, number>;
  /** Window start time */
  windowStartTime: number;
  /** Window end time */
  windowEndTime: number;
  /** Total bytes served */
  totalBytesServed?: number | undefined;
  /** Bytes saved from cache hits */
  bytesSavedFromCache?: number | undefined;
}

/**
 * Individual request metric
 */
export interface RequestMetric {
  /** Chunk ID */
  chunkId: string;
  /** Whether it was a cache hit */
  cacheHit: boolean;
  /** Latency in milliseconds */
  latencyMs?: number | undefined;
  /** Timestamp of the request */
  timestamp: number;
  /** Namespace */
  namespace?: string | undefined;
  /** Bytes served */
  bytesServed?: number | undefined;
}

/**
 * Metrics snapshot
 */
export interface MetricsSnapshot {
  /** The metrics at snapshot time */
  metrics: CacheMetrics;
  /** Snapshot timestamp */
  timestamp: number;
}

/**
 * Comparison between two snapshots
 */
export interface SnapshotComparison {
  /** Hits delta */
  hitsDelta: number;
  /** Misses delta */
  missesDelta: number;
  /** Hit rate delta */
  hitRateDelta: number;
  /** Time elapsed between snapshots in ms */
  timeElapsedMs: number;
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Calculate hit rate from hits and total requests
 *
 * @param hits - Number of hits
 * @param total - Total requests
 * @returns Hit rate (0-1)
 */
export function calculateHitRate(hits: number, total: number): number {
  if (total === 0) return 0;
  return hits / total;
}

/**
 * Calculate miss rate from misses and total requests
 *
 * @param misses - Number of misses
 * @param total - Total requests
 * @returns Miss rate (0-1)
 */
export function calculateMissRate(misses: number, total: number): number {
  if (total === 0) return 0;
  return misses / total;
}

/**
 * Calculate percentile from sorted array
 *
 * @param sortedValues - Sorted array of values
 * @param percentile - Percentile (0-100)
 * @returns Percentile value
 */
function calculatePercentile(sortedValues: number[], percentile: number): number {
  if (sortedValues.length === 0) return 0;
  const index = Math.ceil((percentile / 100) * sortedValues.length) - 1;
  return sortedValues[Math.max(0, index)]!;
}

/**
 * Format metrics as a human-readable report
 *
 * @param metrics - Cache metrics
 * @returns Formatted report string
 */
export function formatMetricsReport(metrics: CacheMetrics): string {
  const lines: string[] = [
    '=== Cache Metrics Report ===',
    `Hit Rate: ${(metrics.hitRate * 100).toFixed(2)}%`,
    `Miss Rate: ${(metrics.missRate * 100).toFixed(2)}%`,
    `Hits: ${metrics.hits}`,
    `Misses: ${metrics.misses}`,
    `Total Requests: ${metrics.totalRequests}`,
    `Invalidations: ${metrics.invalidations}`,
    `Average Hit Latency: ${metrics.averageHitLatencyMs.toFixed(2)}ms`,
    `Average Miss Latency: ${metrics.averageMissLatencyMs.toFixed(2)}ms`,
  ];

  if (metrics.p95HitLatencyMs !== undefined) {
    lines.push(`P95 Hit Latency: ${metrics.p95HitLatencyMs.toFixed(2)}ms`);
  }

  if (metrics.totalBytesServed !== undefined) {
    lines.push(`Total Bytes Served: ${metrics.totalBytesServed}`);
  }

  if (metrics.bytesSavedFromCache !== undefined) {
    lines.push(`Bytes Saved from Cache: ${metrics.bytesSavedFromCache}`);
  }

  if (Object.keys(metrics.invalidationsByReason).length > 0) {
    lines.push('Invalidations by Reason:');
    for (const [reason, count] of Object.entries(metrics.invalidationsByReason)) {
      lines.push(`  ${reason}: ${count}`);
    }
  }

  lines.push(`Window: ${new Date(metrics.windowStartTime).toISOString()} - ${new Date(metrics.windowEndTime).toISOString()}`);

  return lines.join('\n');
}

// ============================================================================
// Internal Metric Entry
// ============================================================================

interface MetricEntry {
  type: 'hit' | 'miss' | 'invalidation';
  chunkId: string;
  timestamp: number;
  latencyMs?: number | undefined;
  namespace?: string | undefined;
  reason?: string | undefined;
  bytesServed?: number | undefined;
}

// ============================================================================
// CacheMetricsCollector Class
// ============================================================================

/**
 * Cache metrics collector
 */
export class CacheMetricsCollector {
  readonly config: Required<MetricsConfig>;
  private entries: MetricEntry[] = [];
  private requestDetails: RequestMetric[] = [];
  private namespaceMetrics: Map<string, { hits: number; misses: number }> = new Map();

  constructor(config: MetricsConfig = {}) {
    this.config = {
      windowMs: config.windowMs ?? METRICS_WINDOW_DEFAULT,
      trackByNamespace: config.trackByNamespace ?? false,
      trackDetails: config.trackDetails ?? false,
      maxDetailEntries: config.maxDetailEntries ?? DEFAULT_MAX_DETAIL_ENTRIES,
    };
  }

  // ==========================================================================
  // Recording Methods
  // ==========================================================================

  /**
   * Record a cache hit
   *
   * @param chunkId - The chunk identifier
   * @param options - Optional hit details
   */
  recordHit(chunkId: string, options: HitOptions = {}): void {
    const entry: MetricEntry = {
      type: 'hit',
      chunkId,
      timestamp: Date.now(),
      latencyMs: options.latencyMs,
      namespace: options.namespace,
      bytesServed: options.bytesServed,
    };

    this.addEntry(entry);

    if (this.config.trackByNamespace && options.namespace) {
      const ns = this.namespaceMetrics.get(options.namespace) || { hits: 0, misses: 0 };
      ns.hits++;
      this.namespaceMetrics.set(options.namespace, ns);
    }

    if (this.config.trackDetails) {
      this.addRequestDetail({
        chunkId,
        cacheHit: true,
        latencyMs: options.latencyMs,
        timestamp: Date.now(),
        namespace: options.namespace,
        bytesServed: options.bytesServed,
      });
    }
  }

  /**
   * Record a cache miss
   *
   * @param chunkId - The chunk identifier
   * @param options - Optional miss details
   */
  recordMiss(chunkId: string, options: MissOptions = {}): void {
    const entry: MetricEntry = {
      type: 'miss',
      chunkId,
      timestamp: Date.now(),
      latencyMs: options.latencyMs,
      namespace: options.namespace,
      bytesServed: options.bytesServed,
    };

    this.addEntry(entry);

    if (this.config.trackByNamespace && options.namespace) {
      const ns = this.namespaceMetrics.get(options.namespace) || { hits: 0, misses: 0 };
      ns.misses++;
      this.namespaceMetrics.set(options.namespace, ns);
    }

    if (this.config.trackDetails) {
      this.addRequestDetail({
        chunkId,
        cacheHit: false,
        latencyMs: options.latencyMs,
        timestamp: Date.now(),
        namespace: options.namespace,
        bytesServed: options.bytesServed,
      });
    }
  }

  /**
   * Record an invalidation event
   *
   * @param chunkId - The chunk identifier
   * @param options - Optional invalidation details
   */
  recordInvalidation(chunkId: string, options: InvalidationOptions = {}): void {
    const entry: MetricEntry = {
      type: 'invalidation',
      chunkId,
      timestamp: Date.now(),
      reason: options.reason,
      namespace: options.namespace,
    };

    this.addEntry(entry);
  }

  // ==========================================================================
  // Metric Retrieval
  // ==========================================================================

  /**
   * Get current metrics
   *
   * @returns Cache metrics
   */
  getMetrics(): CacheMetrics {
    const now = Date.now();
    const windowStart = now - this.config.windowMs;

    // Filter entries within window
    const windowEntries = this.entries.filter((e) => e.timestamp >= windowStart);

    // Calculate metrics
    const hits = windowEntries.filter((e) => e.type === 'hit');
    const misses = windowEntries.filter((e) => e.type === 'miss');
    const invalidations = windowEntries.filter((e) => e.type === 'invalidation');

    const hitCount = hits.length;
    const missCount = misses.length;
    const totalRequests = hitCount + missCount;
    const invalidationCount = invalidations.length;

    // Calculate latencies
    const hitLatencies = hits.filter((e) => e.latencyMs !== undefined).map((e) => e.latencyMs!);
    const missLatencies = misses.filter((e) => e.latencyMs !== undefined).map((e) => e.latencyMs!);

    const avgHitLatency = hitLatencies.length > 0
      ? hitLatencies.reduce((a, b) => a + b, 0) / hitLatencies.length
      : 0;

    const avgMissLatency = missLatencies.length > 0
      ? missLatencies.reduce((a, b) => a + b, 0) / missLatencies.length
      : 0;

    // Calculate p95 latencies
    const sortedHitLatencies = [...hitLatencies].sort((a, b) => a - b);
    const p95HitLatency = sortedHitLatencies.length > 0
      ? calculatePercentile(sortedHitLatencies, 95)
      : undefined;

    const sortedMissLatencies = [...missLatencies].sort((a, b) => a - b);
    const p95MissLatency = sortedMissLatencies.length > 0
      ? calculatePercentile(sortedMissLatencies, 95)
      : undefined;

    // Invalidations by reason
    const invalidationsByReason: Record<string, number> = {};
    for (const inv of invalidations) {
      const reason = inv.reason || 'unknown';
      invalidationsByReason[reason] = (invalidationsByReason[reason] || 0) + 1;
    }

    // Bytes tracking
    const totalBytesServed = windowEntries
      .filter((e) => e.bytesServed !== undefined)
      .reduce((sum, e) => sum + (e.bytesServed || 0), 0);

    const bytesSavedFromCache = hits
      .filter((e) => e.bytesServed !== undefined)
      .reduce((sum, e) => sum + (e.bytesServed || 0), 0);

    return {
      hits: hitCount,
      misses: missCount,
      invalidations: invalidationCount,
      hitRate: calculateHitRate(hitCount, totalRequests),
      missRate: calculateMissRate(missCount, totalRequests),
      totalRequests,
      averageHitLatencyMs: avgHitLatency,
      averageMissLatencyMs: avgMissLatency,
      p95HitLatencyMs: p95HitLatency,
      p95MissLatencyMs: p95MissLatency,
      invalidationsByReason,
      windowStartTime: windowStart,
      windowEndTime: now,
      totalBytesServed: totalBytesServed > 0 ? totalBytesServed : undefined,
      bytesSavedFromCache: bytesSavedFromCache > 0 ? bytesSavedFromCache : undefined,
    };
  }

  /**
   * Get metrics for a specific namespace
   *
   * @param namespace - The namespace
   * @returns Cache metrics for the namespace
   */
  getMetricsForNamespace(namespace: string): CacheMetrics {
    const ns = this.namespaceMetrics.get(namespace) || { hits: 0, misses: 0 };
    const totalRequests = ns.hits + ns.misses;

    return {
      hits: ns.hits,
      misses: ns.misses,
      invalidations: 0,
      hitRate: calculateHitRate(ns.hits, totalRequests),
      missRate: calculateMissRate(ns.misses, totalRequests),
      totalRequests,
      averageHitLatencyMs: 0,
      averageMissLatencyMs: 0,
      invalidationsByReason: {},
      windowStartTime: Date.now() - this.config.windowMs,
      windowEndTime: Date.now(),
    };
  }

  /**
   * Get request details (if tracking is enabled)
   *
   * @returns Array of request metrics
   */
  getRequestDetails(): RequestMetric[] {
    return [...this.requestDetails];
  }

  // ==========================================================================
  // Snapshots
  // ==========================================================================

  /**
   * Take a snapshot of current metrics
   *
   * @returns Metrics snapshot
   */
  takeSnapshot(): MetricsSnapshot {
    return {
      metrics: this.getMetrics(),
      timestamp: Date.now(),
    };
  }

  /**
   * Compare two snapshots
   *
   * @param snapshot1 - First snapshot (earlier)
   * @param snapshot2 - Second snapshot (later)
   * @returns Snapshot comparison
   */
  compareSnapshots(snapshot1: MetricsSnapshot, snapshot2: MetricsSnapshot): SnapshotComparison {
    return {
      hitsDelta: snapshot2.metrics.hits - snapshot1.metrics.hits,
      missesDelta: snapshot2.metrics.misses - snapshot1.metrics.misses,
      hitRateDelta: snapshot2.metrics.hitRate - snapshot1.metrics.hitRate,
      timeElapsedMs: snapshot2.timestamp - snapshot1.timestamp,
    };
  }

  // ==========================================================================
  // Reset
  // ==========================================================================

  /**
   * Reset all metrics
   */
  reset(): void {
    this.entries = [];
    this.requestDetails = [];
    this.namespaceMetrics.clear();
  }

  // ==========================================================================
  // Internal Methods
  // ==========================================================================

  /**
   * Add an entry to the metrics store
   */
  private addEntry(entry: MetricEntry): void {
    this.entries.push(entry);

    // Clean up old entries outside the window
    const cutoff = Date.now() - this.config.windowMs;
    while (this.entries.length > 0 && this.entries[0]!.timestamp < cutoff) {
      this.entries.shift();
    }
  }

  /**
   * Add a request detail
   */
  private addRequestDetail(detail: RequestMetric): void {
    this.requestDetails.push(detail);

    // Limit stored details
    while (this.requestDetails.length > this.config.maxDetailEntries) {
      this.requestDetails.shift();
    }
  }
}
