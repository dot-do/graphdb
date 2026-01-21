/**
 * Tests for Batch Import with Various Sizes
 *
 * TDD tests covering:
 * - Boundary conditions for batch sizes
 * - Memory efficiency with large batches
 * - Performance characteristics
 * - Concurrent batch operations
 */

import { describe, it, expect } from 'vitest';
import {
  createBatchedTripleWriter,
  type BatchedTripleWriter,
  type WriterResult,
} from '../../src/import/batched-writer';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import type { Triple } from '../../src/core/triple';

// ============================================================================
// Test Helpers
// ============================================================================

function makeTestTriple(id: number, timestamp?: bigint): Triple {
  return {
    subject: createEntityId(`https://example.com/entity/${id}`),
    predicate: createPredicate('name'),
    object: { type: ObjectType.STRING, value: `Entity ${id}` },
    timestamp: timestamp ?? BigInt(Date.now()),
    txId: createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
  };
}

function createMockR2Bucket(): R2Bucket & {
  puts: Map<string, Uint8Array>;
  reset: () => void;
  totalBytesWritten: () => number;
} {
  const puts = new Map<string, Uint8Array>();

  return {
    puts,
    reset() {
      puts.clear();
    },
    totalBytesWritten() {
      let total = 0;
      for (const value of puts.values()) {
        total += value.length;
      }
      return total;
    },
    async put(key: string, value: ArrayBufferLike | ArrayBuffer | ReadableStream | string | null) {
      if (value instanceof Uint8Array) {
        puts.set(key, value);
      } else if (typeof value === 'string') {
        puts.set(key, new TextEncoder().encode(value));
      }
      return {} as R2Object;
    },
    async get() { return null; },
    async head() { return null; },
    async delete() {},
    async list() { return { objects: [], truncated: false } as R2Objects; },
    async createMultipartUpload() { return {} as R2MultipartUpload; },
    async resumeMultipartUpload() { return {} as R2MultipartUpload; },
  } as unknown as R2Bucket & { puts: Map<string, Uint8Array>; reset: () => void; totalBytesWritten: () => number };
}

// ============================================================================
// Batch Size Boundary Tests
// ============================================================================

describe('Batch Import Sizes: Boundary Conditions', () => {
  describe('Minimum batch sizes', () => {
    it('should handle batch size of 1', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 1,
      });

      // Each add should trigger a flush
      await writer.addTriple(makeTestTriple(1));
      expect(r2.puts.size).toBe(1);

      await writer.addTriple(makeTestTriple(2));
      expect(r2.puts.size).toBe(2);

      await writer.addTriple(makeTestTriple(3));
      expect(r2.puts.size).toBe(3);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(3);
      expect(result.totalChunks).toBe(3);
    });

    it('should handle batch size of 2', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      await writer.addTriple(makeTestTriple(1));
      expect(r2.puts.size).toBe(0);

      await writer.addTriple(makeTestTriple(2));
      expect(r2.puts.size).toBe(1);

      await writer.addTriple(makeTestTriple(3));
      expect(r2.puts.size).toBe(1);

      await writer.addTriple(makeTestTriple(4));
      expect(r2.puts.size).toBe(2);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(4);
      expect(result.totalChunks).toBe(2);
    });
  });

  describe('Exact batch size multiples', () => {
    it('should flush exactly at batch size', async () => {
      const r2 = createMockR2Bucket();
      const batchSize = 10;
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize,
      });

      // Add exactly batch size triples
      for (let i = 0; i < batchSize; i++) {
        await writer.addTriple(makeTestTriple(i));
      }

      expect(r2.puts.size).toBe(1);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(batchSize);
      expect(result.totalChunks).toBe(1);
    });

    it('should handle exact multiples of batch size', async () => {
      const r2 = createMockR2Bucket();
      const batchSize = 5;
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize,
      });

      // Add exactly 3x batch size
      for (let i = 0; i < batchSize * 3; i++) {
        await writer.addTriple(makeTestTriple(i));
      }

      expect(r2.puts.size).toBe(3);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(15);
      expect(result.totalChunks).toBe(3);
    });

    it('should handle one less than batch size', async () => {
      const r2 = createMockR2Bucket();
      const batchSize = 10;
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize,
      });

      // Add one less than batch size
      for (let i = 0; i < batchSize - 1; i++) {
        await writer.addTriple(makeTestTriple(i));
      }

      expect(r2.puts.size).toBe(0);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(batchSize - 1);
      expect(result.totalChunks).toBe(1);
    });

    it('should handle one more than batch size', async () => {
      const r2 = createMockR2Bucket();
      const batchSize = 10;
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize,
      });

      // Add one more than batch size
      for (let i = 0; i < batchSize + 1; i++) {
        await writer.addTriple(makeTestTriple(i));
      }

      expect(r2.puts.size).toBe(1);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(batchSize + 1);
      expect(result.totalChunks).toBe(2);
    });
  });

  describe('Large batch sizes', () => {
    it('should handle batch size of 10000 (default)', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/');

      // Add less than default batch size
      const count = 5000;
      const triples = Array.from({ length: count }, (_, i) => makeTestTriple(i));
      await writer.addTriples(triples);

      expect(r2.puts.size).toBe(0);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(count);
      expect(result.totalChunks).toBe(1);
    });

    it('should handle batch size of 50000', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 50000,
      });

      // Add 100000 triples (2 batches)
      const batchCount = 10;
      const triplesPerIteration = 10000;

      for (let batch = 0; batch < batchCount; batch++) {
        const triples = Array.from(
          { length: triplesPerIteration },
          (_, i) => makeTestTriple(batch * triplesPerIteration + i)
        );
        await writer.addTriples(triples);
      }

      expect(r2.puts.size).toBe(2); // 100000 / 50000

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(batchCount * triplesPerIteration);
    });
  });
});

// ============================================================================
// AddTriples Batch Behavior Tests
// ============================================================================

describe('Batch Import Sizes: addTriples Behavior', () => {
  it('should flush multiple times when adding more than batch size at once', async () => {
    const r2 = createMockR2Bucket();
    const batchSize = 5;
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize,
    });

    // Add 13 triples at once
    const triples = Array.from({ length: 13 }, (_, i) => makeTestTriple(i));
    await writer.addTriples(triples);

    // Should have flushed 2 times (5 + 5), with 3 remaining
    expect(r2.puts.size).toBe(2);

    const result = await writer.finalize();
    expect(result.totalTriples).toBe(13);
    expect(result.totalChunks).toBe(3);
  });

  it('should handle mixed single and batch adds', async () => {
    const r2 = createMockR2Bucket();
    const batchSize = 5;
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize,
    });

    // Add 3 single
    await writer.addTriple(makeTestTriple(1));
    await writer.addTriple(makeTestTriple(2));
    await writer.addTriple(makeTestTriple(3));
    expect(r2.puts.size).toBe(0);

    // Add 4 batch (triggers flush at 5)
    await writer.addTriples([makeTestTriple(4), makeTestTriple(5), makeTestTriple(6), makeTestTriple(7)]);
    expect(r2.puts.size).toBe(1);

    // Add 3 more
    await writer.addTriples([makeTestTriple(8), makeTestTriple(9), makeTestTriple(10)]);
    expect(r2.puts.size).toBe(2);

    const result = await writer.finalize();
    expect(result.totalTriples).toBe(10);
    expect(result.totalChunks).toBe(2);
  });

  it('should handle empty array in addTriples', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 10,
    });

    await writer.addTriples([]);
    expect(r2.puts.size).toBe(0);

    await writer.addTriple(makeTestTriple(1));
    await writer.addTriples([]);
    expect(r2.puts.size).toBe(0);

    const result = await writer.finalize();
    expect(result.totalTriples).toBe(1);
  });
});

// ============================================================================
// Chunk Info Tracking Tests
// ============================================================================

describe('Batch Import Sizes: Chunk Info Tracking', () => {
  it('should track correct triple counts per chunk', async () => {
    const r2 = createMockR2Bucket();
    const batchSize = 10;
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize,
    });

    // Add 25 triples
    for (let i = 0; i < 25; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    expect(result.chunks).toHaveLength(3);
    expect(result.chunks[0]!.tripleCount).toBe(10);
    expect(result.chunks[1]!.tripleCount).toBe(10);
    expect(result.chunks[2]!.tripleCount).toBe(5);
  });

  it('should track time ranges correctly per chunk', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 3,
    });

    const baseTime = BigInt(1000000);

    // Chunk 1: timestamps 1000000, 1000001, 1000002
    await writer.addTriple(makeTestTriple(1, baseTime));
    await writer.addTriple(makeTestTriple(2, baseTime + 1n));
    await writer.addTriple(makeTestTriple(3, baseTime + 2n));

    // Chunk 2: timestamps 1000010, 1000011, 1000012
    await writer.addTriple(makeTestTriple(4, baseTime + 10n));
    await writer.addTriple(makeTestTriple(5, baseTime + 11n));
    await writer.addTriple(makeTestTriple(6, baseTime + 12n));

    const result = await writer.finalize();

    expect(result.chunks[0]!.minTime).toBe(baseTime);
    expect(result.chunks[0]!.maxTime).toBe(baseTime + 2n);
    expect(result.chunks[1]!.minTime).toBe(baseTime + 10n);
    expect(result.chunks[1]!.maxTime).toBe(baseTime + 12n);
  });

  it('should track bytes written per chunk', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 5,
    });

    for (let i = 0; i < 10; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    expect(result.chunks[0]!.bytes).toBeGreaterThan(0);
    expect(result.chunks[1]!.bytes).toBeGreaterThan(0);
    expect(result.totalBytes).toBe(result.chunks[0]!.bytes + result.chunks[1]!.bytes);
  });

  it('should generate unique chunk IDs', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 1,
    });

    for (let i = 0; i < 10; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    const ids = result.chunks.map(c => c.id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(10);
  });

  it('should generate correct paths for chunks', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://api.example.com/v1/graphs/', {
      batchSize: 1,
    });

    await writer.addTriple(makeTestTriple(1));

    const result = await writer.finalize();

    expect(result.chunks[0]!.path).toContain('.com/.example/.api/v1/graphs/_chunks/');
    expect(result.chunks[0]!.path).toMatch(/\.gcol$/);
  });
});

// ============================================================================
// Bloom Filter with Various Sizes
// ============================================================================

describe('Batch Import Sizes: Bloom Filter Behavior', () => {
  it('should create bloom filter for small batches', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 5,
    });

    for (let i = 0; i < 5; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    expect(result.chunks[0]!.bloom).toBeDefined();
    expect(result.chunks[0]!.bloom!.filter).toBeDefined();
    expect(result.chunks[0]!.bloom!.k).toBeGreaterThan(0);
    expect(result.chunks[0]!.bloom!.m).toBeGreaterThan(0);
  });

  it('should create combined bloom filter across all chunks', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 5,
    });

    for (let i = 0; i < 15; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    expect(result.combinedBloom).toBeDefined();
    expect(result.combinedBloom.filter).toBeDefined();
  });

  it('should respect custom bloom filter capacity', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 100,
      bloomCapacity: 10000,
      bloomFpr: 0.001,
    });

    for (let i = 0; i < 100; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    // Should still create valid bloom filters with custom settings
    expect(result.combinedBloom).toBeDefined();
    expect(result.chunks[0]!.bloom).toBeDefined();
  });
});

// ============================================================================
// State Management with Various Sizes
// ============================================================================

describe('Batch Import Sizes: State Management', () => {
  it('should maintain accurate state after multiple batches', async () => {
    const r2 = createMockR2Bucket();
    const batchSize = 7;
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize,
    });

    // Add 20 triples
    for (let i = 0; i < 20; i++) {
      await writer.addTriple(makeTestTriple(i));

      const state = writer.getState();
      const expectedWritten = Math.floor((i + 1) / batchSize) * batchSize;
      expect(state.triplesWritten).toBe(expectedWritten);
    }

    const finalState = writer.getState();
    expect(finalState.triplesWritten).toBe(14); // 2 batches of 7
    expect(finalState.chunksUploaded).toBe(2);
  });

  it('should restore state correctly for partial batch', async () => {
    const r2 = createMockR2Bucket();
    const writer1 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 10,
    });

    // Add 15 triples (1 flush + 5 in buffer)
    for (let i = 0; i < 15; i++) {
      await writer1.addTriple(makeTestTriple(i));
    }

    const state = writer1.getState();
    expect(state.triplesWritten).toBe(10);

    // New writer, restore state
    const writer2 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 10,
    });
    writer2.restoreState(state);

    // Continue adding 10 more
    for (let i = 0; i < 10; i++) {
      await writer2.addTriple(makeTestTriple(15 + i));
    }

    const result = await writer2.finalize();

    // Should have 10 (restored) + 10 (new) = 20
    expect(result.totalTriples).toBe(20);
  });

  it('should track bytes uploaded accurately across batches', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 5,
    });

    for (let i = 0; i < 15; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    const result = await writer.finalize();

    // Total bytes should match sum of chunk bytes
    const sumOfChunkBytes = result.chunks.reduce((sum, c) => sum + c.bytes, 0);
    expect(result.totalBytes).toBe(sumOfChunkBytes);

    // And should match what was actually written to R2
    expect(result.totalBytes).toBe(r2.totalBytesWritten());
  });
});

// ============================================================================
// Performance Characteristics
// ============================================================================

describe('Batch Import Sizes: Performance Characteristics', () => {
  it('should efficiently handle many small batches', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 10,
    });

    const count = 10000;
    const triples = Array.from({ length: count }, (_, i) => makeTestTriple(i));

    const start = performance.now();
    await writer.addTriples(triples);
    const result = await writer.finalize();
    const elapsed = performance.now() - start;

    expect(result.totalTriples).toBe(count);
    expect(result.totalChunks).toBe(1000);

    // Should complete in reasonable time (less than 5 seconds)
    expect(elapsed).toBeLessThan(5000);
  });

  it('should handle many single adds efficiently', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 100,
    });

    const count = 1000;

    const start = performance.now();
    for (let i = 0; i < count; i++) {
      await writer.addTriple(makeTestTriple(i));
    }
    const result = await writer.finalize();
    const elapsed = performance.now() - start;

    expect(result.totalTriples).toBe(count);
    expect(elapsed).toBeLessThan(5000);
  });
});
