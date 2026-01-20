/**
 * Shard Scaling Tests
 *
 * TDD RED phase - these tests define the expected behavior
 * for configurable shard ID generation and multi-shard scaling.
 */

import { describe, it, expect } from 'vitest';
import {
  getShardId,
  getShardIndex,
  DEFAULT_SHARD_COUNT,
} from '../../src/snippet/router';
import { createNamespace } from '../../src/core/types';

describe('shard scaling', () => {
  describe('getShardIndex', () => {
    it('should generate unique shard index from namespace', () => {
      const namespace = createNamespace('https://example.com/users/');
      const shardIndex = getShardIndex(namespace);

      expect(typeof shardIndex).toBe('number');
      expect(Number.isInteger(shardIndex)).toBe(true);
      expect(shardIndex).toBeGreaterThanOrEqual(0);
    });

    it('should route same namespace to same shard', () => {
      const namespace = createNamespace('https://example.com/users/');

      const index1 = getShardIndex(namespace);
      const index2 = getShardIndex(namespace);

      expect(index1).toBe(index2);
    });

    it('should distribute namespaces across multiple shards', () => {
      // Generate many namespaces and verify they distribute across shards
      const shardCount = 256;
      const shardHits = new Set<number>();
      const namespaceCount = 1000;

      for (let i = 0; i < namespaceCount; i++) {
        const namespace = createNamespace(`https://domain-${i}.com/`);
        const shardIndex = getShardIndex(namespace, shardCount);
        shardHits.add(shardIndex);
      }

      // With 1000 namespaces distributed across 256 shards,
      // we should see at least 100 unique shards used (good distribution)
      expect(shardHits.size).toBeGreaterThan(100);

      // All shard indices should be within valid range
      for (const idx of shardHits) {
        expect(idx).toBeGreaterThanOrEqual(0);
        expect(idx).toBeLessThan(shardCount);
      }
    });

    it('should support configurable shard count', () => {
      const namespace = createNamespace('https://example.com/');

      // Test with different shard counts
      const index16 = getShardIndex(namespace, 16);
      const index256 = getShardIndex(namespace, 256);
      const index1024 = getShardIndex(namespace, 1024);

      // Each should be within its respective range
      expect(index16).toBeGreaterThanOrEqual(0);
      expect(index16).toBeLessThan(16);

      expect(index256).toBeGreaterThanOrEqual(0);
      expect(index256).toBeLessThan(256);

      expect(index1024).toBeGreaterThanOrEqual(0);
      expect(index1024).toBeLessThan(1024);
    });

    it('should use default shard count of 256', () => {
      expect(DEFAULT_SHARD_COUNT).toBe(256);

      const namespace = createNamespace('https://example.com/');
      const shardIndex = getShardIndex(namespace);

      // Without explicit shard count, should default to 256
      expect(shardIndex).toBeGreaterThanOrEqual(0);
      expect(shardIndex).toBeLessThan(256);
    });

    it('should handle edge case shard counts', () => {
      const namespace = createNamespace('https://example.com/');

      // Shard count of 1 should always return 0
      const index1 = getShardIndex(namespace, 1);
      expect(index1).toBe(0);

      // Shard count of 2 should return 0 or 1
      const index2 = getShardIndex(namespace, 2);
      expect(index2).toBeGreaterThanOrEqual(0);
      expect(index2).toBeLessThan(2);
    });
  });

  describe('getShardId with configurable shard count', () => {
    it('should include shard index in shard ID', () => {
      const namespace = createNamespace('https://example.com/users/');
      const shardId = getShardId(namespace, 256);

      // Shard ID should contain the shard index
      expect(shardId).toMatch(/^shard-\d+-[a-f0-9]+$/);
    });

    it('should produce consistent IDs for same namespace', () => {
      const namespace = createNamespace('https://example.com/users/');

      const id1 = getShardId(namespace, 256);
      const id2 = getShardId(namespace, 256);

      expect(id1).toBe(id2);
    });

    it('should produce different IDs for different shard counts', () => {
      const namespace = createNamespace('https://example.com/users/');

      const id16 = getShardId(namespace, 16);
      const id256 = getShardId(namespace, 256);

      // Different shard counts may produce different IDs
      // (since shard index is part of the ID)
      expect(id16).not.toBe(id256);
    });
  });

  describe('consistent hashing properties', () => {
    it('should maintain determinism across multiple invocations', () => {
      const namespaces = [
        'https://example.com/',
        'https://api.example.com/',
        'https://other.com/users/',
        'https://test.io/api/v1/',
      ];

      const shardCount = 64;
      const firstRun: number[] = [];
      const secondRun: number[] = [];

      for (const ns of namespaces) {
        const namespace = createNamespace(ns);
        firstRun.push(getShardIndex(namespace, shardCount));
      }

      for (const ns of namespaces) {
        const namespace = createNamespace(ns);
        secondRun.push(getShardIndex(namespace, shardCount));
      }

      expect(firstRun).toEqual(secondRun);
    });

    it('should distribute uniformly (chi-square test approximation)', () => {
      const shardCount = 32;
      const namespaceCount = 3200; // 100 per shard expected
      const counts = new Array(shardCount).fill(0);

      for (let i = 0; i < namespaceCount; i++) {
        const namespace = createNamespace(`https://tenant-${i}.example.com/`);
        const idx = getShardIndex(namespace, shardCount);
        counts[idx]++;
      }

      // Expected count per shard
      const expected = namespaceCount / shardCount;

      // Calculate chi-square statistic
      let chiSquare = 0;
      for (const count of counts) {
        chiSquare += Math.pow(count - expected, 2) / expected;
      }

      // For 31 degrees of freedom (shardCount - 1), chi-square critical value
      // at 0.05 significance is about 44.99. We use a more lenient threshold.
      // A well-distributed hash should have chi-square < 60
      expect(chiSquare).toBeLessThan(80);
    });
  });
});
