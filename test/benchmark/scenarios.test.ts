/**
 * Benchmark Scenarios Tests
 *
 * Tests for benchmark scenario utilities and types:
 * - Latency statistics calculation
 * - Scenario registry functions
 * - Type definitions validation
 *
 * Note: Full scenario runners require Cloudflare Workers runtime
 * and are not tested here. These tests focus on pure functions.
 */

import { describe, it, expect } from 'vitest';
import {
  SCENARIOS,
  getScenarioRunner,
  listScenarios,
  type LatencyStats,
  type ThroughputStats,
  type CacheStats,
  type BenchmarkResult,
} from '../../src/benchmark/scenarios.js';

// We need to test the calculateLatencyStats function, but it's not exported
// So we'll test it indirectly through the scenario results structure
// and create a local implementation for testing the algorithm

/**
 * Local implementation of latency stats calculation for testing
 * This mirrors the logic in scenarios.ts
 */
function calculateLatencyStats(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return {
      count: 0,
      min: 0,
      max: 0,
      mean: 0,
      p50: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
    };
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const count = sorted.length;
  const sum = sorted.reduce((a, b) => a + b, 0);
  const mean = sum / count;

  // Calculate standard deviation
  const squaredDiffs = sorted.map((x) => Math.pow(x - mean, 2));
  const avgSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / count;
  const stdDev = Math.sqrt(avgSquaredDiff);

  // Percentiles
  const percentile = (p: number): number => {
    const index = Math.ceil((p / 100) * count) - 1;
    return sorted[Math.max(0, Math.min(index, count - 1))]!;
  };

  return {
    count,
    min: sorted[0]!,
    max: sorted[count - 1]!,
    mean,
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    stdDev,
  };
}

describe('Benchmark Scenarios', () => {
  // ============================================================================
  // Latency Statistics Tests
  // ============================================================================

  describe('calculateLatencyStats', () => {
    it('should return zeros for empty array', () => {
      const stats = calculateLatencyStats([]);

      expect(stats.count).toBe(0);
      expect(stats.min).toBe(0);
      expect(stats.max).toBe(0);
      expect(stats.mean).toBe(0);
      expect(stats.p50).toBe(0);
      expect(stats.p95).toBe(0);
      expect(stats.p99).toBe(0);
      expect(stats.stdDev).toBe(0);
    });

    it('should calculate correct stats for single value', () => {
      const stats = calculateLatencyStats([10]);

      expect(stats.count).toBe(1);
      expect(stats.min).toBe(10);
      expect(stats.max).toBe(10);
      expect(stats.mean).toBe(10);
      expect(stats.p50).toBe(10);
      expect(stats.p95).toBe(10);
      expect(stats.p99).toBe(10);
      expect(stats.stdDev).toBe(0);
    });

    it('should calculate correct min and max', () => {
      const stats = calculateLatencyStats([5, 10, 15, 20, 25]);

      expect(stats.min).toBe(5);
      expect(stats.max).toBe(25);
    });

    it('should calculate correct mean', () => {
      const stats = calculateLatencyStats([10, 20, 30, 40, 50]);

      expect(stats.mean).toBe(30);
    });

    it('should calculate correct p50 (median)', () => {
      // Odd number of elements
      const stats1 = calculateLatencyStats([1, 2, 3, 4, 5]);
      expect(stats1.p50).toBe(3);

      // Even number of elements - returns the element at ceil index
      const stats2 = calculateLatencyStats([1, 2, 3, 4, 5, 6]);
      expect(stats2.p50).toBe(3);
    });

    it('should calculate correct p95', () => {
      // 100 values from 1 to 100
      const latencies = Array.from({ length: 100 }, (_, i) => i + 1);
      const stats = calculateLatencyStats(latencies);

      // p95 should be around 95
      expect(stats.p95).toBe(95);
    });

    it('should calculate correct p99', () => {
      // 100 values from 1 to 100
      const latencies = Array.from({ length: 100 }, (_, i) => i + 1);
      const stats = calculateLatencyStats(latencies);

      // p99 should be around 99
      expect(stats.p99).toBe(99);
    });

    it('should calculate correct standard deviation', () => {
      // Values with known standard deviation
      const stats = calculateLatencyStats([2, 4, 4, 4, 5, 5, 7, 9]);

      // Mean = 5, variance = 4, stdDev = 2
      expect(stats.mean).toBe(5);
      expect(stats.stdDev).toBe(2);
    });

    it('should handle unsorted input', () => {
      const stats = calculateLatencyStats([30, 10, 50, 20, 40]);

      expect(stats.min).toBe(10);
      expect(stats.max).toBe(50);
      expect(stats.mean).toBe(30);
    });

    it('should handle duplicate values', () => {
      const stats = calculateLatencyStats([5, 5, 5, 5, 5]);

      expect(stats.min).toBe(5);
      expect(stats.max).toBe(5);
      expect(stats.mean).toBe(5);
      expect(stats.stdDev).toBe(0);
    });

    it('should handle large datasets', () => {
      const latencies = Array.from({ length: 10000 }, () => Math.random() * 100);
      const stats = calculateLatencyStats(latencies);

      expect(stats.count).toBe(10000);
      expect(stats.min).toBeLessThanOrEqual(stats.p50);
      expect(stats.p50).toBeLessThanOrEqual(stats.p95);
      expect(stats.p95).toBeLessThanOrEqual(stats.p99);
      expect(stats.p99).toBeLessThanOrEqual(stats.max);
    });
  });

  // ============================================================================
  // Scenario Registry Tests
  // ============================================================================

  describe('SCENARIOS', () => {
    it('should have point-lookup scenario', () => {
      expect(SCENARIOS).toHaveProperty('point-lookup');
      expect(typeof SCENARIOS['point-lookup']).toBe('function');
    });

    it('should have traversal-1hop scenario', () => {
      expect(SCENARIOS).toHaveProperty('traversal-1hop');
      expect(typeof SCENARIOS['traversal-1hop']).toBe('function');
    });

    it('should have traversal-3hop scenario', () => {
      expect(SCENARIOS).toHaveProperty('traversal-3hop');
      expect(typeof SCENARIOS['traversal-3hop']).toBe('function');
    });

    it('should have write-throughput scenario', () => {
      expect(SCENARIOS).toHaveProperty('write-throughput');
      expect(typeof SCENARIOS['write-throughput']).toBe('function');
    });

    it('should have bloom-filter-hit-rate scenario', () => {
      expect(SCENARIOS).toHaveProperty('bloom-filter-hit-rate');
      expect(typeof SCENARIOS['bloom-filter-hit-rate']).toBe('function');
    });

    it('should have edge-cache-hit-rate scenario', () => {
      expect(SCENARIOS).toHaveProperty('edge-cache-hit-rate');
      expect(typeof SCENARIOS['edge-cache-hit-rate']).toBe('function');
    });
  });

  describe('getScenarioRunner', () => {
    it('should return runner for valid scenario names', () => {
      expect(getScenarioRunner('point-lookup')).toBeDefined();
      expect(getScenarioRunner('traversal-1hop')).toBeDefined();
      expect(getScenarioRunner('traversal-3hop')).toBeDefined();
      expect(getScenarioRunner('write-throughput')).toBeDefined();
      expect(getScenarioRunner('bloom-filter-hit-rate')).toBeDefined();
      expect(getScenarioRunner('edge-cache-hit-rate')).toBeDefined();
    });

    it('should return undefined for invalid scenario names', () => {
      expect(getScenarioRunner('invalid')).toBeUndefined();
      expect(getScenarioRunner('')).toBeUndefined();
      expect(getScenarioRunner('nonexistent-scenario')).toBeUndefined();
    });

    it('should return the same function as in SCENARIOS', () => {
      expect(getScenarioRunner('point-lookup')).toBe(SCENARIOS['point-lookup']);
      expect(getScenarioRunner('traversal-1hop')).toBe(SCENARIOS['traversal-1hop']);
    });
  });

  describe('listScenarios', () => {
    it('should return array of scenario names', () => {
      const scenarios = listScenarios();

      expect(Array.isArray(scenarios)).toBe(true);
      expect(scenarios.length).toBeGreaterThan(0);
    });

    it('should include all defined scenarios', () => {
      const scenarios = listScenarios();

      expect(scenarios).toContain('point-lookup');
      expect(scenarios).toContain('traversal-1hop');
      expect(scenarios).toContain('traversal-3hop');
      expect(scenarios).toContain('write-throughput');
      expect(scenarios).toContain('bloom-filter-hit-rate');
      expect(scenarios).toContain('edge-cache-hit-rate');
    });

    it('should return the same keys as SCENARIOS object', () => {
      const scenarios = listScenarios();
      const scenarioKeys = Object.keys(SCENARIOS);

      expect(scenarios.sort()).toEqual(scenarioKeys.sort());
    });
  });

  // ============================================================================
  // Type Structure Tests
  // ============================================================================

  describe('Type Structures', () => {
    describe('LatencyStats', () => {
      it('should have all required properties', () => {
        const stats: LatencyStats = {
          count: 100,
          min: 1.5,
          max: 150.3,
          mean: 45.7,
          p50: 42.1,
          p95: 120.5,
          p99: 145.2,
          stdDev: 25.3,
        };

        expect(stats.count).toBe(100);
        expect(stats.min).toBe(1.5);
        expect(stats.max).toBe(150.3);
        expect(stats.mean).toBe(45.7);
        expect(stats.p50).toBe(42.1);
        expect(stats.p95).toBe(120.5);
        expect(stats.p99).toBe(145.2);
        expect(stats.stdDev).toBe(25.3);
      });
    });

    describe('ThroughputStats', () => {
      it('should have all required properties', () => {
        const stats: ThroughputStats = {
          operationsPerSecond: 1000,
          bytesPerSecond: 1024000,
          totalOperations: 10000,
          totalBytes: 10240000,
          durationMs: 10000,
        };

        expect(stats.operationsPerSecond).toBe(1000);
        expect(stats.bytesPerSecond).toBe(1024000);
        expect(stats.totalOperations).toBe(10000);
        expect(stats.totalBytes).toBe(10240000);
        expect(stats.durationMs).toBe(10000);
      });
    });

    describe('CacheStats', () => {
      it('should have all required properties', () => {
        const stats: CacheStats = {
          hits: 800,
          misses: 200,
          hitRate: 0.8,
          totalRequests: 1000,
        };

        expect(stats.hits).toBe(800);
        expect(stats.misses).toBe(200);
        expect(stats.hitRate).toBe(0.8);
        expect(stats.totalRequests).toBe(1000);
      });

      it('should have consistent hit rate calculation', () => {
        const stats: CacheStats = {
          hits: 75,
          misses: 25,
          hitRate: 0.75,
          totalRequests: 100,
        };

        expect(stats.hitRate).toBeCloseTo(stats.hits / stats.totalRequests);
      });
    });

    describe('BenchmarkResult', () => {
      it('should have required base properties', () => {
        const result: BenchmarkResult = {
          scenario: 'point-lookup',
          dataset: 'small',
          timestamp: Date.now(),
          durationMs: 5000,
          iterations: 100,
        };

        expect(result.scenario).toBe('point-lookup');
        expect(result.dataset).toBe('small');
        expect(typeof result.timestamp).toBe('number');
        expect(result.durationMs).toBe(5000);
        expect(result.iterations).toBe(100);
      });

      it('should allow optional latency stats', () => {
        const result: BenchmarkResult = {
          scenario: 'point-lookup',
          dataset: 'small',
          timestamp: Date.now(),
          durationMs: 5000,
          iterations: 100,
          latency: {
            count: 100,
            min: 0.5,
            max: 10.0,
            mean: 2.5,
            p50: 2.0,
            p95: 8.0,
            p99: 9.5,
            stdDev: 1.5,
          },
        };

        expect(result.latency).toBeDefined();
        expect(result.latency!.mean).toBe(2.5);
      });

      it('should allow optional throughput stats', () => {
        const result: BenchmarkResult = {
          scenario: 'write-throughput',
          dataset: 'small',
          timestamp: Date.now(),
          durationMs: 5000,
          iterations: 100,
          throughput: {
            operationsPerSecond: 500,
            bytesPerSecond: 512000,
            totalOperations: 2500,
            totalBytes: 2560000,
            durationMs: 5000,
          },
        };

        expect(result.throughput).toBeDefined();
        expect(result.throughput!.operationsPerSecond).toBe(500);
      });

      it('should allow optional cache stats', () => {
        const result: BenchmarkResult = {
          scenario: 'edge-cache-hit-rate',
          dataset: 'small',
          timestamp: Date.now(),
          durationMs: 5000,
          iterations: 100,
          cache: {
            hits: 80,
            misses: 20,
            hitRate: 0.8,
            totalRequests: 100,
          },
        };

        expect(result.cache).toBeDefined();
        expect(result.cache!.hitRate).toBe(0.8);
      });

      it('should allow optional metadata', () => {
        const result: BenchmarkResult = {
          scenario: 'traversal-1hop',
          dataset: 'medium',
          timestamp: Date.now(),
          durationMs: 10000,
          iterations: 50,
          metadata: {
            description: 'Entity + direct relationships (1-hop)',
            totalHopsPerformed: 250,
            avgHopsPerIteration: 5,
          },
        };

        expect(result.metadata).toBeDefined();
        expect(result.metadata!['description']).toBe('Entity + direct relationships (1-hop)');
        expect(result.metadata!['totalHopsPerformed']).toBe(250);
      });
    });
  });

  // ============================================================================
  // Statistical Edge Cases
  // ============================================================================

  describe('Statistical Edge Cases', () => {
    it('should handle very small latencies', () => {
      const stats = calculateLatencyStats([0.001, 0.002, 0.003]);

      expect(stats.min).toBeCloseTo(0.001);
      expect(stats.max).toBeCloseTo(0.003);
    });

    it('should handle very large latencies', () => {
      const stats = calculateLatencyStats([1e6, 2e6, 3e6]);

      expect(stats.min).toBe(1e6);
      expect(stats.max).toBe(3e6);
    });

    it('should handle mixed scale latencies', () => {
      const stats = calculateLatencyStats([0.1, 1, 10, 100, 1000]);

      expect(stats.min).toBe(0.1);
      expect(stats.max).toBe(1000);
      expect(stats.mean).toBeCloseTo(222.22, 1);
    });

    it('should maintain percentile order', () => {
      const latencies = Array.from({ length: 1000 }, () => Math.random() * 1000);
      const stats = calculateLatencyStats(latencies);

      expect(stats.min).toBeLessThanOrEqual(stats.p50);
      expect(stats.p50).toBeLessThanOrEqual(stats.p95);
      expect(stats.p95).toBeLessThanOrEqual(stats.p99);
      expect(stats.p99).toBeLessThanOrEqual(stats.max);
    });
  });
});
