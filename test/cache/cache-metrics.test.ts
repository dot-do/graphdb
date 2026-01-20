/**
 * Cache Metrics Tests for GraphDB
 *
 * TDD RED phase - tests for cache hit rate tracking and metrics.
 * Following the design from pocs-s0ks:
 * - Track cache hit rate (target: > 90% for repeat reads)
 * - Monitor invalidation events
 * - Track latency improvements
 *
 * Key metrics:
 * - Hit rate (hits / total requests)
 * - Miss rate (misses / total requests)
 * - Invalidation count
 * - Average response time
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  type CacheMetrics,
  type MetricsConfig,
  type MetricsSnapshot,
  type RequestMetric,
  CacheMetricsCollector,
  calculateHitRate,
  calculateMissRate,
  formatMetricsReport,
  METRICS_WINDOW_DEFAULT,
} from '../../src/cache/cache-metrics';

describe('CacheMetricsCollector', () => {
  let collector: CacheMetricsCollector;

  beforeEach(() => {
    vi.useFakeTimers();
    collector = new CacheMetricsCollector();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('recordHit', () => {
    it('should increment hit count', () => {
      collector.recordHit('chunk-001');

      const metrics = collector.getMetrics();
      expect(metrics.hits).toBe(1);
    });

    it('should track hit with optional latency', () => {
      collector.recordHit('chunk-001', { latencyMs: 5 });

      const metrics = collector.getMetrics();
      expect(metrics.hits).toBe(1);
      expect(metrics.averageHitLatencyMs).toBe(5);
    });

    it('should accumulate multiple hits', () => {
      collector.recordHit('chunk-001');
      collector.recordHit('chunk-002');
      collector.recordHit('chunk-003');

      const metrics = collector.getMetrics();
      expect(metrics.hits).toBe(3);
    });
  });

  describe('recordMiss', () => {
    it('should increment miss count', () => {
      collector.recordMiss('chunk-001');

      const metrics = collector.getMetrics();
      expect(metrics.misses).toBe(1);
    });

    it('should track miss with optional latency', () => {
      collector.recordMiss('chunk-001', { latencyMs: 50 });

      const metrics = collector.getMetrics();
      expect(metrics.misses).toBe(1);
      expect(metrics.averageMissLatencyMs).toBe(50);
    });
  });

  describe('recordInvalidation', () => {
    it('should track invalidation events', () => {
      collector.recordInvalidation('chunk-001');

      const metrics = collector.getMetrics();
      expect(metrics.invalidations).toBe(1);
    });

    it('should track invalidation reason if provided', () => {
      collector.recordInvalidation('chunk-001', { reason: 'compaction' });

      const metrics = collector.getMetrics();
      expect(metrics.invalidationsByReason.compaction).toBe(1);
    });
  });

  describe('hit rate calculation', () => {
    it('should calculate hit rate correctly', () => {
      // 3 hits, 1 miss = 75% hit rate
      collector.recordHit('chunk-001');
      collector.recordHit('chunk-002');
      collector.recordHit('chunk-003');
      collector.recordMiss('chunk-004');

      const metrics = collector.getMetrics();
      expect(metrics.hitRate).toBe(0.75);
    });

    it('should return 0 hit rate when no requests', () => {
      const metrics = collector.getMetrics();
      expect(metrics.hitRate).toBe(0);
    });

    it('should return 1.0 hit rate when all hits', () => {
      collector.recordHit('chunk-001');
      collector.recordHit('chunk-002');

      const metrics = collector.getMetrics();
      expect(metrics.hitRate).toBe(1.0);
    });

    it('should return 0 hit rate when all misses', () => {
      collector.recordMiss('chunk-001');
      collector.recordMiss('chunk-002');

      const metrics = collector.getMetrics();
      expect(metrics.hitRate).toBe(0);
    });

    it('should meet 90% hit rate target for repeat reads', () => {
      // Simulate 10 repeat reads where 9 hit cache
      collector.recordMiss('chunk-001'); // First read: miss
      for (let i = 0; i < 9; i++) {
        collector.recordHit('chunk-001');
      }

      const metrics = collector.getMetrics();
      expect(metrics.hitRate).toBeGreaterThanOrEqual(0.9);
    });
  });

  describe('reset', () => {
    it('should reset all metrics to zero', () => {
      collector.recordHit('chunk-001');
      collector.recordMiss('chunk-002');
      collector.recordInvalidation('chunk-003');

      collector.reset();

      const metrics = collector.getMetrics();
      expect(metrics.hits).toBe(0);
      expect(metrics.misses).toBe(0);
      expect(metrics.invalidations).toBe(0);
    });
  });

  describe('time window support', () => {
    it('should track metrics within time window', () => {
      const windowCollector = new CacheMetricsCollector({
        windowMs: 60000, // 1 minute window
      });

      windowCollector.recordHit('chunk-001');
      vi.advanceTimersByTime(30000); // Advance 30 seconds
      windowCollector.recordHit('chunk-002');

      const metrics = windowCollector.getMetrics();
      expect(metrics.hits).toBe(2);
    });

    it('should expire old metrics outside window', () => {
      const windowCollector = new CacheMetricsCollector({
        windowMs: 60000, // 1 minute window
      });

      windowCollector.recordHit('chunk-001');
      vi.advanceTimersByTime(90000); // Advance 90 seconds (beyond window)
      windowCollector.recordHit('chunk-002');

      // Only the second hit should count (first one expired)
      const metrics = windowCollector.getMetrics();
      expect(metrics.hits).toBe(1);
    });
  });
});

describe('Utility Functions', () => {
  describe('calculateHitRate', () => {
    it('should calculate hit rate from hits and total', () => {
      expect(calculateHitRate(75, 100)).toBe(0.75);
    });

    it('should return 0 for zero total', () => {
      expect(calculateHitRate(0, 0)).toBe(0);
    });

    it('should handle all hits', () => {
      expect(calculateHitRate(100, 100)).toBe(1.0);
    });
  });

  describe('calculateMissRate', () => {
    it('should calculate miss rate from misses and total', () => {
      expect(calculateMissRate(25, 100)).toBe(0.25);
    });

    it('should return 0 for zero total', () => {
      expect(calculateMissRate(0, 0)).toBe(0);
    });
  });

  describe('formatMetricsReport', () => {
    it('should format metrics as readable report', () => {
      const metrics: CacheMetrics = {
        hits: 90,
        misses: 10,
        invalidations: 5,
        hitRate: 0.9,
        missRate: 0.1,
        totalRequests: 100,
        averageHitLatencyMs: 2,
        averageMissLatencyMs: 50,
        invalidationsByReason: { compaction: 3, update: 2 },
        windowStartTime: Date.now() - 60000,
        windowEndTime: Date.now(),
      };

      const report = formatMetricsReport(metrics);

      expect(report).toContain('Hit Rate: 90.00%');
      expect(report).toContain('Hits: 90');
      expect(report).toContain('Misses: 10');
      expect(typeof report).toBe('string');
    });
  });
});

describe('Metrics Snapshots', () => {
  let collector: CacheMetricsCollector;

  beforeEach(() => {
    vi.useFakeTimers();
    collector = new CacheMetricsCollector();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('takeSnapshot', () => {
    it('should capture current metrics state', () => {
      collector.recordHit('chunk-001');
      collector.recordMiss('chunk-002');

      const snapshot = collector.takeSnapshot();

      expect(snapshot.metrics.hits).toBe(1);
      expect(snapshot.metrics.misses).toBe(1);
      expect(snapshot.timestamp).toBeDefined();
    });

    it('should preserve snapshot after reset', () => {
      collector.recordHit('chunk-001');
      const snapshot = collector.takeSnapshot();
      collector.reset();

      expect(snapshot.metrics.hits).toBe(1);

      const currentMetrics = collector.getMetrics();
      expect(currentMetrics.hits).toBe(0);
    });
  });

  describe('compareSnapshots', () => {
    it('should compare two snapshots', () => {
      collector.recordHit('chunk-001');
      const snapshot1 = collector.takeSnapshot();

      collector.recordHit('chunk-002');
      collector.recordHit('chunk-003');
      const snapshot2 = collector.takeSnapshot();

      const comparison = collector.compareSnapshots(snapshot1, snapshot2);

      expect(comparison.hitsDelta).toBe(2);
      expect(comparison.hitRateDelta).toBeGreaterThanOrEqual(0);
    });
  });
});

describe('Per-Namespace Metrics', () => {
  let collector: CacheMetricsCollector;

  beforeEach(() => {
    collector = new CacheMetricsCollector({ trackByNamespace: true });
  });

  it('should track metrics per namespace', () => {
    collector.recordHit('chunk-001', { namespace: 'https://example.com/ns1/' });
    collector.recordHit('chunk-002', { namespace: 'https://example.com/ns1/' });
    collector.recordMiss('chunk-003', { namespace: 'https://example.com/ns2/' });

    const ns1Metrics = collector.getMetricsForNamespace('https://example.com/ns1/');
    const ns2Metrics = collector.getMetricsForNamespace('https://example.com/ns2/');

    expect(ns1Metrics.hits).toBe(2);
    expect(ns1Metrics.misses).toBe(0);
    expect(ns2Metrics.hits).toBe(0);
    expect(ns2Metrics.misses).toBe(1);
  });

  it('should return empty metrics for unknown namespace', () => {
    const metrics = collector.getMetricsForNamespace('https://unknown.com/');

    expect(metrics.hits).toBe(0);
    expect(metrics.misses).toBe(0);
  });
});

describe('Latency Tracking', () => {
  let collector: CacheMetricsCollector;

  beforeEach(() => {
    collector = new CacheMetricsCollector();
  });

  it('should calculate average hit latency', () => {
    collector.recordHit('chunk-001', { latencyMs: 2 });
    collector.recordHit('chunk-002', { latencyMs: 4 });
    collector.recordHit('chunk-003', { latencyMs: 6 });

    const metrics = collector.getMetrics();
    expect(metrics.averageHitLatencyMs).toBe(4); // (2+4+6)/3
  });

  it('should calculate average miss latency', () => {
    collector.recordMiss('chunk-001', { latencyMs: 50 });
    collector.recordMiss('chunk-002', { latencyMs: 100 });

    const metrics = collector.getMetrics();
    expect(metrics.averageMissLatencyMs).toBe(75); // (50+100)/2
  });

  it('should track p95 latency', () => {
    // Record 100 hits with varying latencies
    for (let i = 1; i <= 100; i++) {
      collector.recordHit(`chunk-${i}`, { latencyMs: i });
    }

    const metrics = collector.getMetrics();
    // p95 should be around 95ms (95th percentile of 1-100)
    expect(metrics.p95HitLatencyMs).toBeGreaterThanOrEqual(90);
    expect(metrics.p95HitLatencyMs).toBeLessThanOrEqual(100);
  });
});

describe('Request Detail Tracking', () => {
  let collector: CacheMetricsCollector;

  beforeEach(() => {
    collector = new CacheMetricsCollector({ trackDetails: true });
  });

  it('should track individual request details when enabled', () => {
    collector.recordHit('chunk-001', { latencyMs: 5 });
    collector.recordMiss('chunk-002', { latencyMs: 50 });

    const details = collector.getRequestDetails();

    expect(details).toHaveLength(2);
    expect(details[0].chunkId).toBe('chunk-001');
    expect(details[0].cacheHit).toBe(true);
    expect(details[1].chunkId).toBe('chunk-002');
    expect(details[1].cacheHit).toBe(false);
  });

  it('should limit stored details to prevent memory issues', () => {
    const limitedCollector = new CacheMetricsCollector({
      trackDetails: true,
      maxDetailEntries: 10,
    });

    for (let i = 0; i < 20; i++) {
      limitedCollector.recordHit(`chunk-${i}`);
    }

    const details = limitedCollector.getRequestDetails();
    expect(details.length).toBeLessThanOrEqual(10);
  });
});

describe('Metrics Configuration', () => {
  it('should accept custom configuration', () => {
    const config: MetricsConfig = {
      windowMs: 300000, // 5 minutes
      trackByNamespace: true,
      trackDetails: false,
      maxDetailEntries: 1000,
    };

    const collector = new CacheMetricsCollector(config);

    expect(collector.config.windowMs).toBe(300000);
    expect(collector.config.trackByNamespace).toBe(true);
  });

  it('should use default values when not specified', () => {
    const collector = new CacheMetricsCollector();

    expect(collector.config.windowMs).toBe(METRICS_WINDOW_DEFAULT);
  });
});

describe('Cache Efficiency Metrics', () => {
  let collector: CacheMetricsCollector;

  beforeEach(() => {
    collector = new CacheMetricsCollector();
  });

  it('should calculate bytes saved from cache hits', () => {
    collector.recordHit('chunk-001', { bytesServed: 1024 });
    collector.recordHit('chunk-002', { bytesServed: 2048 });

    const metrics = collector.getMetrics();
    expect(metrics.bytesSavedFromCache).toBe(3072);
  });

  it('should track total bytes served', () => {
    collector.recordHit('chunk-001', { bytesServed: 1024 });
    collector.recordMiss('chunk-002', { bytesServed: 2048 });

    const metrics = collector.getMetrics();
    expect(metrics.totalBytesServed).toBe(3072);
  });
});
