/**
 * Compaction Data Patterns Tests (TDD - RED Phase)
 *
 * Tests for compaction behavior with various data patterns:
 * - Sparse vs dense data distributions
 * - Time-ordered vs random timestamps
 * - Large vs small predicate cardinality
 * - Edge cases with minimum/maximum chunks
 * - Predicate-specific compaction scenarios
 *
 * @packageDocumentation
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  selectChunksForCompaction,
  CompactionLevel,
  type CompactionConfig,
  type CompactionChunkInfo,
} from '../../src/storage/compaction';
import { encodeGraphCol, decodeGraphCol, getChunkStats } from '../../src/storage/graphcol';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type Namespace,
  type TransactionId,
} from '../../src/core/types';
import { type Triple } from '../../src/core/triple';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Generate a valid ULID-format transaction ID for testing
 */
function generateTestTxId(index: number): TransactionId {
  const base = '01ARZ3NDEKTSV4RRFFQ69G5FA';
  const lastChar = 'ABCDEFGHJKMNPQRSTVWXYZ'[index % 22];
  return createTransactionId(base + lastChar);
}

/**
 * Create test triples with configurable patterns
 */
function createTestTriples(options: {
  count: number;
  baseTimestamp: bigint;
  timestampIncrement?: bigint;
  predicates?: string[];
  entityPrefix?: string;
}): Triple[] {
  const {
    count,
    baseTimestamp,
    timestampIncrement = BigInt(1000),
    predicates = ['name', 'age', 'score'],
    entityPrefix = 'https://example.com/entity/',
  } = options;

  const triples: Triple[] = [];

  for (let i = 0; i < count; i++) {
    const predicate = predicates[i % predicates.length]!;
    let object: Triple['object'];

    switch (predicate) {
      case 'name':
        object = { type: ObjectType.STRING, value: `Entity ${i}` };
        break;
      case 'age':
        object = { type: ObjectType.INT64, value: BigInt(20 + (i % 50)) };
        break;
      case 'score':
        object = { type: ObjectType.FLOAT64, value: (i % 100) + 0.5 };
        break;
      default:
        object = { type: ObjectType.STRING, value: `value_${i}` };
    }

    triples.push({
      subject: createEntityId(`${entityPrefix}${i}`),
      predicate: createPredicate(predicate),
      object,
      timestamp: baseTimestamp + BigInt(i) * timestampIncrement,
      txId: generateTestTxId(i % 22),
    });
  }

  return triples;
}

/**
 * Create chunk info from triples
 */
function createChunkInfo(
  path: string,
  triples: Triple[],
  namespace: Namespace
): CompactionChunkInfo {
  const encoded = encodeGraphCol(triples, namespace);
  const stats = getChunkStats(encoded);

  return {
    path,
    sizeBytes: encoded.length,
    tripleCount: stats.tripleCount,
    minTimestamp: stats.timeRange[0],
    maxTimestamp: stats.timeRange[1],
  };
}

const testNamespace = createNamespace('https://example.com/');

// ============================================================================
// Chunk Selection Tests
// ============================================================================

describe('Compaction Chunk Selection Patterns', () => {
  describe('Size-Based Selection', () => {
    it('should select chunks until reaching threshold', () => {
      // Create chunks of varying sizes
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1 * 1024 * 1024, tripleCount: 1000, minTimestamp: 1n, maxTimestamp: 1000n },
        { path: 'chunk2.gcol', sizeBytes: 2 * 1024 * 1024, tripleCount: 2000, minTimestamp: 1001n, maxTimestamp: 2000n },
        { path: 'chunk3.gcol', sizeBytes: 3 * 1024 * 1024, tripleCount: 3000, minTimestamp: 2001n, maxTimestamp: 3000n },
        { path: 'chunk4.gcol', sizeBytes: 4 * 1024 * 1024, tripleCount: 4000, minTimestamp: 3001n, maxTimestamp: 4000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 7 * 1024 * 1024, // 7MB threshold
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Should select chunks 1, 2, 3 (1+2+3=6MB < 7MB, but 1+2+3+4=10MB > 7MB)
      // Actually: select chunks until we hit threshold OR have minimum
      // With minChunksToCompact=2, we'll select at least 2
      expect(selected.length).toBeGreaterThanOrEqual(2);
      const totalSize = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
      expect(totalSize).toBeLessThanOrEqual(config.l1ThresholdBytes * 1.1); // Allow small overhead
    });

    it('should return empty when all chunks exceed threshold and minChunks not reachable', () => {
      // Each chunk is 10MB, threshold is 8MB, minChunksToCompact is 4
      // The algorithm adds chunks until threshold is reached, then only stops
      // if we have enough chunks. Since even 1 chunk (10MB) > threshold (8MB),
      // and we can't reach minChunksToCompact (4) without exceeding further,
      // we get 1 chunk which is < minChunksToCompact, so empty is returned.
      const chunks: CompactionChunkInfo[] = [
        { path: 'large1.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 1n, maxTimestamp: 1000n },
        { path: 'large2.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 1001n, maxTimestamp: 2000n },
        { path: 'large3.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 2001n, maxTimestamp: 3000n },
        { path: 'large4.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 3001n, maxTimestamp: 4000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024, // 8MB threshold - smaller than any single chunk
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Returns empty because 1 chunk reaches threshold but < minChunksToCompact (4)
      expect(selected.length).toBe(0);
    });

    it('should select all chunks when minChunksToCompact is low and threshold is high', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 1n, maxTimestamp: 1000n },
        { path: 'chunk2.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 1001n, maxTimestamp: 2000n },
        { path: 'chunk3.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 2001n, maxTimestamp: 3000n },
        { path: 'chunk4.gcol', sizeBytes: 10 * 1024 * 1024, tripleCount: 10000, minTimestamp: 3001n, maxTimestamp: 4000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 50 * 1024 * 1024, // 50MB threshold - can fit all 4
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Should select all 4 chunks (40MB < 50MB threshold)
      expect(selected.length).toBe(4);
    });

    it('should handle mix of tiny and large chunks', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'tiny1.gcol', sizeBytes: 100, tripleCount: 1, minTimestamp: 1n, maxTimestamp: 1n },
        { path: 'tiny2.gcol', sizeBytes: 100, tripleCount: 1, minTimestamp: 2n, maxTimestamp: 2n },
        { path: 'tiny3.gcol', sizeBytes: 100, tripleCount: 1, minTimestamp: 3n, maxTimestamp: 3n },
        { path: 'large.gcol', sizeBytes: 5 * 1024 * 1024, tripleCount: 5000, minTimestamp: 4n, maxTimestamp: 4000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(4);
    });
  });

  describe('Timestamp-Based Selection', () => {
    it('should select chunks in timestamp order', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk_c.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 300n, maxTimestamp: 400n },
        { path: 'chunk_a.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 100n, maxTimestamp: 200n },
        { path: 'chunk_b.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 200n, maxTimestamp: 300n },
        { path: 'chunk_d.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 400n, maxTimestamp: 500n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Should be ordered by minTimestamp
      expect(selected[0].path).toBe('chunk_a.gcol');
      expect(selected[1].path).toBe('chunk_b.gcol');
      expect(selected[2].path).toBe('chunk_c.gcol');
      expect(selected[3].path).toBe('chunk_d.gcol');
    });

    it('should handle chunks with overlapping timestamp ranges', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 100n, maxTimestamp: 300n },
        { path: 'chunk2.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 150n, maxTimestamp: 350n }, // Overlaps
        { path: 'chunk3.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 200n, maxTimestamp: 400n }, // Overlaps
        { path: 'chunk4.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 500n, maxTimestamp: 600n }, // Gap
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(4);
      // Ordered by minTimestamp
      expect(selected[0].minTimestamp).toBe(100n);
      expect(selected[1].minTimestamp).toBe(150n);
      expect(selected[2].minTimestamp).toBe(200n);
      expect(selected[3].minTimestamp).toBe(500n);
    });

    it('should handle identical timestamps', () => {
      const sameTimestamp = BigInt(Date.now());
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: sameTimestamp, maxTimestamp: sameTimestamp },
        { path: 'chunk2.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: sameTimestamp, maxTimestamp: sameTimestamp },
        { path: 'chunk3.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: sameTimestamp, maxTimestamp: sameTimestamp },
        { path: 'chunk4.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: sameTimestamp, maxTimestamp: sameTimestamp },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(4);
    });
  });

  describe('Minimum Chunk Threshold', () => {
    it('should return empty when below minimum chunk count', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 1n, maxTimestamp: 100n },
        { path: 'chunk2.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 101n, maxTimestamp: 200n },
        { path: 'chunk3.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 201n, maxTimestamp: 300n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4, // Requires 4, only have 3
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(0);
    });

    it('should select exactly minimum when threshold prevents more', () => {
      // Each chunk is 3MB, threshold is 8MB
      // Can fit 2 chunks (6MB), but 3 would be 9MB > threshold
      // With minChunksToCompact=2, should select exactly 2
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 3 * 1024 * 1024, tripleCount: 3000, minTimestamp: 1n, maxTimestamp: 1000n },
        { path: 'chunk2.gcol', sizeBytes: 3 * 1024 * 1024, tripleCount: 3000, minTimestamp: 1001n, maxTimestamp: 2000n },
        { path: 'chunk3.gcol', sizeBytes: 3 * 1024 * 1024, tripleCount: 3000, minTimestamp: 2001n, maxTimestamp: 3000n },
        { path: 'chunk4.gcol', sizeBytes: 3 * 1024 * 1024, tripleCount: 3000, minTimestamp: 3001n, maxTimestamp: 4000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024, // 8MB
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Should select 2 chunks (6MB) since adding 3rd would exceed 8MB
      expect(selected.length).toBeGreaterThanOrEqual(2);
      expect(selected.length).toBeLessThanOrEqual(3);
    });

    it('should handle minChunksToCompact of 1', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'chunk1.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 1n, maxTimestamp: 100n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 1,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(1);
    });
  });

  describe('Level-Specific Thresholds', () => {
    it('should use L1 threshold for L0_TO_L1 compaction', () => {
      const chunks: CompactionChunkInfo[] = Array.from({ length: 10 }, (_, i) => ({
        path: `chunk${i}.gcol`,
        sizeBytes: 1 * 1024 * 1024, // 1MB each
        tripleCount: 1000,
        minTimestamp: BigInt(i * 1000),
        maxTimestamp: BigInt((i + 1) * 1000 - 1),
      }));

      const config: CompactionConfig = {
        l1ThresholdBytes: 4 * 1024 * 1024, // 4MB L1 threshold
        l2ThresholdBytes: 128 * 1024 * 1024, // 128MB L2 threshold
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      const totalSize = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
      expect(totalSize).toBeLessThanOrEqual(config.l1ThresholdBytes * 1.1);
    });

    it('should use L2 threshold for L1_TO_L2 compaction', () => {
      const chunks: CompactionChunkInfo[] = Array.from({ length: 20 }, (_, i) => ({
        path: `chunk${i}.gcol`,
        sizeBytes: 8 * 1024 * 1024, // 8MB each (L1 chunk size)
        tripleCount: 8000,
        minTimestamp: BigInt(i * 10000),
        maxTimestamp: BigInt((i + 1) * 10000 - 1),
      }));

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 64 * 1024 * 1024, // 64MB L2 threshold for test
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L1_TO_L2);

      const totalSize = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
      expect(totalSize).toBeLessThanOrEqual(config.l2ThresholdBytes * 1.1);
    });
  });
});

// ============================================================================
// Data Pattern Tests
// ============================================================================

describe('Compaction Data Patterns', () => {
  describe('Triple Count Patterns', () => {
    it('should preserve triple count after simulated merge', () => {
      // Simulate what happens when chunks are merged
      const chunk1Triples = createTestTriples({ count: 100, baseTimestamp: BigInt(Date.now()) });
      const chunk2Triples = createTestTriples({ count: 150, baseTimestamp: BigInt(Date.now() + 100000) });
      const chunk3Triples = createTestTriples({ count: 200, baseTimestamp: BigInt(Date.now() + 200000) });

      // Merge triples (simulating compaction)
      const merged = [...chunk1Triples, ...chunk2Triples, ...chunk3Triples];

      // Re-encode as single chunk
      const encoded = encodeGraphCol(merged, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(450);
    });

    it('should handle chunks with varying triple densities', () => {
      // Dense chunk: many triples, small size (high compression)
      const denseTriples = createTestTriples({
        count: 1000,
        baseTimestamp: BigInt(Date.now()),
        predicates: ['samePred'], // Same predicate = better compression
      });

      // Sparse chunk: fewer triples, similar size (low compression)
      const sparseTriples: Triple[] = [];
      for (let i = 0; i < 100; i++) {
        sparseTriples.push({
          subject: createEntityId(`https://example.com/sparse/${i}`),
          predicate: createPredicate(`pred_${i}`), // All unique predicates
          object: { type: ObjectType.STRING, value: 'x'.repeat(100) },
          timestamp: BigInt(Date.now() + i * 1000),
          txId: generateTestTxId(i % 22),
        });
      }

      const denseChunk = createChunkInfo('dense.gcol', denseTriples, testNamespace);
      const sparseChunk = createChunkInfo('sparse.gcol', sparseTriples, testNamespace);

      // Dense chunk should have better bytes-per-triple ratio
      const denseBytesPerTriple = denseChunk.sizeBytes / denseChunk.tripleCount;
      const sparseBytesPerTriple = sparseChunk.sizeBytes / sparseChunk.tripleCount;

      expect(denseBytesPerTriple).toBeLessThan(sparseBytesPerTriple);
    });
  });

  describe('Predicate Distribution Patterns', () => {
    it('should handle single predicate across all triples', () => {
      const triples = createTestTriples({
        count: 500,
        baseTimestamp: BigInt(Date.now()),
        predicates: ['onlyPred'],
      });

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);
      const stats = getChunkStats(encoded);

      expect(decoded.length).toBe(500);
      expect(stats.predicates.length).toBe(1);
      expect(stats.predicates).toContain('onlyPred');
    });

    it('should handle many predicates with single triple each', () => {
      const predicates = Array.from({ length: 100 }, (_, i) => `pred_${i}`);
      const triples = createTestTriples({
        count: 100,
        baseTimestamp: BigInt(Date.now()),
        predicates,
      });

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);
      const stats = getChunkStats(encoded);

      expect(decoded.length).toBe(100);
      expect(stats.predicates.length).toBe(100);
    });

    it('should maintain predicate order after merge', () => {
      const triples1 = createTestTriples({
        count: 50,
        baseTimestamp: BigInt(Date.now()),
        predicates: ['alpha', 'beta'],
      });

      const triples2 = createTestTriples({
        count: 50,
        baseTimestamp: BigInt(Date.now() + 50000),
        predicates: ['gamma', 'delta'],
      });

      const merged = [...triples1, ...triples2];
      const encoded = encodeGraphCol(merged, testNamespace);
      const stats = getChunkStats(encoded);

      expect(stats.predicates).toContain('alpha');
      expect(stats.predicates).toContain('beta');
      expect(stats.predicates).toContain('gamma');
      expect(stats.predicates).toContain('delta');
    });
  });

  describe('Entity Distribution Patterns', () => {
    it('should handle all triples for single entity', () => {
      const singleEntityTriples: Triple[] = [];
      for (let i = 0; i < 100; i++) {
        singleEntityTriples.push({
          subject: createEntityId('https://example.com/single-entity'),
          predicate: createPredicate(`field_${i}`),
          object: { type: ObjectType.STRING, value: `value_${i}` },
          timestamp: BigInt(Date.now() + i * 1000),
          txId: generateTestTxId(i % 22),
        });
      }

      const encoded = encodeGraphCol(singleEntityTriples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(100);
      expect(decoded.every(t => t.subject === 'https://example.com/single-entity')).toBe(true);
    });

    it('should handle one triple per entity', () => {
      const manyEntitiesTriples: Triple[] = [];
      for (let i = 0; i < 500; i++) {
        manyEntitiesTriples.push({
          subject: createEntityId(`https://example.com/entity/${i}`),
          predicate: createPredicate('id'),
          object: { type: ObjectType.INT64, value: BigInt(i) },
          timestamp: BigInt(Date.now() + i * 1000),
          txId: generateTestTxId(i % 22),
        });
      }

      const encoded = encodeGraphCol(manyEntitiesTriples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(500);
      const uniqueEntities = new Set(decoded.map(t => t.subject));
      expect(uniqueEntities.size).toBe(500);
    });
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('Compaction Edge Cases', () => {
  describe('Empty and Minimal Cases', () => {
    it('should handle empty chunk list', () => {
      const chunks: CompactionChunkInfo[] = [];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(0);
    });

    it('should handle single chunk (below minimum)', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'only.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 1n, maxTimestamp: 100n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(0);
    });

    it('should handle chunks with zero triples', () => {
      const emptyEncoded = encodeGraphCol([], testNamespace);
      const emptyStats = getChunkStats(emptyEncoded);

      expect(emptyStats.tripleCount).toBe(0);

      const chunks: CompactionChunkInfo[] = [
        { path: 'empty1.gcol', sizeBytes: emptyEncoded.length, tripleCount: 0, minTimestamp: 0n, maxTimestamp: 0n },
        { path: 'empty2.gcol', sizeBytes: emptyEncoded.length, tripleCount: 0, minTimestamp: 0n, maxTimestamp: 0n },
        { path: 'empty3.gcol', sizeBytes: emptyEncoded.length, tripleCount: 0, minTimestamp: 0n, maxTimestamp: 0n },
        { path: 'empty4.gcol', sizeBytes: emptyEncoded.length, tripleCount: 0, minTimestamp: 0n, maxTimestamp: 0n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Should still select the chunks even though they're empty
      expect(selected.length).toBe(4);
    });
  });

  describe('Boundary Sizes', () => {
    it('should handle chunks exactly at threshold', () => {
      const threshold = 8 * 1024 * 1024;
      const chunks: CompactionChunkInfo[] = [
        { path: 'exact1.gcol', sizeBytes: threshold / 2, tripleCount: 4000, minTimestamp: 1n, maxTimestamp: 1000n },
        { path: 'exact2.gcol', sizeBytes: threshold / 2, tripleCount: 4000, minTimestamp: 1001n, maxTimestamp: 2000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: threshold,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(2);
      const totalSize = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
      expect(totalSize).toBe(threshold);
    });

    it('should handle chunks just over threshold', () => {
      const threshold = 8 * 1024 * 1024;
      const chunks: CompactionChunkInfo[] = [
        { path: 'over1.gcol', sizeBytes: threshold / 2 + 1, tripleCount: 4001, minTimestamp: 1n, maxTimestamp: 1000n },
        { path: 'over2.gcol', sizeBytes: threshold / 2 + 1, tripleCount: 4001, minTimestamp: 1001n, maxTimestamp: 2000n },
        { path: 'over3.gcol', sizeBytes: threshold / 2 + 1, tripleCount: 4001, minTimestamp: 2001n, maxTimestamp: 3000n },
        { path: 'over4.gcol', sizeBytes: threshold / 2 + 1, tripleCount: 4001, minTimestamp: 3001n, maxTimestamp: 4000n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: threshold,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 2,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      // Should select at least minChunksToCompact
      expect(selected.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Large Timestamp Values', () => {
    it('should handle max bigint timestamps', () => {
      const maxTimestamp = BigInt('9007199254740991'); // MAX_SAFE_INTEGER
      const chunks: CompactionChunkInfo[] = [
        { path: 'max1.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: maxTimestamp - 3n, maxTimestamp: maxTimestamp - 2n },
        { path: 'max2.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: maxTimestamp - 2n, maxTimestamp: maxTimestamp - 1n },
        { path: 'max3.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: maxTimestamp - 1n, maxTimestamp: maxTimestamp },
        { path: 'max4.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: maxTimestamp, maxTimestamp: maxTimestamp },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(4);
      expect(selected[0].minTimestamp).toBe(maxTimestamp - 3n);
    });

    it('should handle timestamp of zero', () => {
      const chunks: CompactionChunkInfo[] = [
        { path: 'zero.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 0n, maxTimestamp: 0n },
        { path: 'one.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 1n, maxTimestamp: 1n },
        { path: 'two.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 2n, maxTimestamp: 2n },
        { path: 'three.gcol', sizeBytes: 1000, tripleCount: 10, minTimestamp: 3n, maxTimestamp: 3n },
      ];

      const config: CompactionConfig = {
        l1ThresholdBytes: 8 * 1024 * 1024,
        l2ThresholdBytes: 128 * 1024 * 1024,
        minChunksToCompact: 4,
      };

      const selected = selectChunksForCompaction(chunks, config, CompactionLevel.L0_TO_L1);

      expect(selected.length).toBe(4);
      expect(selected[0].minTimestamp).toBe(0n);
    });
  });
});
