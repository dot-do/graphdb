/**
 * Tests for BatchedTripleWriter - Batch triples for memory-efficient storage
 *
 * Tests cover:
 * - Batching behavior and flush triggers
 * - Backpressure management
 * - Finalization and results
 * - State management for checkpointing
 * - R2 path generation
 * - Bloom filter generation
 */

import { describe, it, expect } from 'vitest';
import {
  createBatchedTripleWriter,
  type BatchedTripleWriter,
  type BatchWriterState,
  type WriterResult,
  type ImportChunkInfo,
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

function makeRefTriple(id: number, refId: number): Triple {
  return {
    subject: createEntityId(`https://example.com/entity/${id}`),
    predicate: createPredicate('related'),
    object: { type: ObjectType.REF, value: createEntityId(`https://example.com/entity/${refId}`) },
    timestamp: BigInt(Date.now()),
    txId: createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
  };
}

function makeRefArrayTriple(id: number, refIds: number[]): Triple {
  return {
    subject: createEntityId(`https://example.com/entity/${id}`),
    predicate: createPredicate('relatedMany'),
    object: {
      type: ObjectType.REF_ARRAY,
      value: refIds.map((r) => createEntityId(`https://example.com/entity/${r}`)),
    },
    timestamp: BigInt(Date.now()),
    txId: createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
  };
}

function createMockR2Bucket(): R2Bucket & {
  puts: Map<string, Uint8Array>;
  reset: () => void;
} {
  const puts = new Map<string, Uint8Array>();

  return {
    puts,
    reset() {
      puts.clear();
    },
    async put(key: string, value: ArrayBufferLike | ArrayBuffer | ReadableStream | string | null) {
      if (value instanceof Uint8Array) {
        puts.set(key, value);
      } else if (typeof value === 'string') {
        puts.set(key, new TextEncoder().encode(value));
      }
      return {} as R2Object;
    },
    async get() {
      return null;
    },
    async head() {
      return null;
    },
    async delete() {},
    async list() {
      return { objects: [], truncated: false } as R2Objects;
    },
    async createMultipartUpload() {
      return {} as R2MultipartUpload;
    },
    async resumeMultipartUpload() {
      return {} as R2MultipartUpload;
    },
  } as unknown as R2Bucket & { puts: Map<string, Uint8Array>; reset: () => void };
}

// ============================================================================
// BatchedTripleWriter Tests
// ============================================================================

describe('BatchedTripleWriter', () => {
  describe('Batching Behavior', () => {
    it('should batch triples before flushing', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 3,
      });

      // Add 2 triples - should not flush yet
      await writer.addTriple(makeTestTriple(1));
      await writer.addTriple(makeTestTriple(2));
      expect(r2.puts.size).toBe(0);

      // Add 1 more - should trigger flush
      await writer.addTriple(makeTestTriple(3));
      expect(r2.puts.size).toBe(1);
    });

    it('should batch add multiple triples', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 5,
      });

      const triples = [makeTestTriple(1), makeTestTriple(2), makeTestTriple(3)];
      await writer.addTriples(triples);

      // Not flushed yet (only 3 of 5)
      expect(r2.puts.size).toBe(0);

      // Add 2 more to trigger flush
      await writer.addTriples([makeTestTriple(4), makeTestTriple(5)]);
      expect(r2.puts.size).toBe(1);
    });

    it('should manually flush on demand', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriple(makeTestTriple(1));
      expect(r2.puts.size).toBe(0);

      await writer.flush();
      expect(r2.puts.size).toBe(1);
    });

    it('should not flush when buffer is empty', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 10,
      });

      const result = await writer.flush();
      expect(result).toBeNull();
      expect(r2.puts.size).toBe(0);
    });

    it('should handle multiple flush cycles', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      // First batch
      await writer.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      expect(r2.puts.size).toBe(1);

      // Second batch
      await writer.addTriples([makeTestTriple(3), makeTestTriple(4)]);
      expect(r2.puts.size).toBe(2);

      // Third batch
      await writer.addTriples([makeTestTriple(5), makeTestTriple(6)]);
      expect(r2.puts.size).toBe(3);
    });

    it('should handle adding more triples than batch size at once', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 3,
      });

      // Add 7 triples at once - should trigger 2 flushes
      const triples = Array.from({ length: 7 }, (_, i) => makeTestTriple(i));
      await writer.addTriples(triples);

      // Should have flushed twice (3 + 3), with 1 remaining
      expect(r2.puts.size).toBe(2);

      // Flush remaining
      await writer.flush();
      expect(r2.puts.size).toBe(3);
    });
  });

  describe('Backpressure', () => {
    it('should track backpressure state', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 1,
        maxPendingBatches: 1,
      });

      // Initially not backpressured
      expect(writer.isBackpressured()).toBe(false);
    });

    it('should respect maxPendingBatches configuration', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 10,
        maxPendingBatches: 5,
      });

      // With default R2 mock (sync), backpressure won't trigger
      expect(writer.isBackpressured()).toBe(false);
    });
  });

  describe('Finalization', () => {
    it('should finalize and return results', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriples([makeTestTriple(1), makeTestTriple(2), makeTestTriple(3)]);

      const result = await writer.finalize();

      expect(result.totalTriples).toBe(3);
      expect(result.totalChunks).toBe(1);
      expect(result.totalBytes).toBeGreaterThan(0);
      expect(result.chunks).toHaveLength(1);
      expect(result.combinedBloom).toBeDefined();
    });

    it('should handle finalize with no triples', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      const result = await writer.finalize();

      expect(result.totalTriples).toBe(0);
      expect(result.totalChunks).toBe(0);
      expect(result.totalBytes).toBe(0);
      expect(result.chunks).toHaveLength(0);
      expect(result.combinedBloom).toBeDefined();
    });

    it('should include chunk infos in result', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      await writer.addTriples([makeTestTriple(1), makeTestTriple(2), makeTestTriple(3), makeTestTriple(4)]);
      const result = await writer.finalize();

      expect(result.chunks).toHaveLength(2);
      expect(result.chunks[0]!.tripleCount).toBe(2);
      expect(result.chunks[1]!.tripleCount).toBe(2);
    });

    it('should track time ranges in chunks', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 3,
      });

      const baseTime = BigInt(Date.now());
      await writer.addTriples([
        makeTestTriple(1, baseTime),
        makeTestTriple(2, baseTime + 100n),
        makeTestTriple(3, baseTime + 200n),
      ]);

      const result = await writer.finalize();

      expect(result.chunks[0]!.minTime).toBe(baseTime);
      expect(result.chunks[0]!.maxTime).toBe(baseTime + 200n);
    });
  });

  describe('R2 Path Generation', () => {
    it('should generate correct R2 paths', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/graphs/');

      await writer.addTriple(makeTestTriple(1));
      await writer.flush();

      // Path should be reversed domain + path + _chunks
      const keys = Array.from(r2.puts.keys());
      expect(keys[0]).toContain('.com/.example/data/graphs/_chunks/');
      expect(keys[0]).toContain('.gcol');
    });

    it('should handle namespace with subdomain', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://api.example.com/v1/');

      await writer.addTriple(makeTestTriple(1));
      await writer.flush();

      const keys = Array.from(r2.puts.keys());
      expect(keys[0]).toContain('.com/.example/.api/v1/_chunks/');
    });

    it('should generate unique chunk IDs', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 1,
      });

      await writer.addTriple(makeTestTriple(1));
      await writer.addTriple(makeTestTriple(2));
      await writer.addTriple(makeTestTriple(3));

      const keys = Array.from(r2.puts.keys());
      const uniqueKeys = new Set(keys);
      expect(uniqueKeys.size).toBe(3);
    });
  });

  describe('State Management', () => {
    it('should save and restore state', async () => {
      const r2 = createMockR2Bucket();
      const writer1 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer1.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      await writer1.flush();

      const state = writer1.getState();
      expect(state.triplesWritten).toBe(2);
      expect(state.chunksUploaded).toBe(1);
      expect(state.chunkInfos).toHaveLength(1);

      // Create new writer and restore state
      const writer2 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });
      writer2.restoreState(state);

      const finalState = writer2.getState();
      expect(finalState.triplesWritten).toBe(2);
      expect(finalState.chunksUploaded).toBe(1);
    });

    it('should include bloom state in saved state', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      await writer.flush();

      const state = writer.getState();
      expect(state.bloomState).toBeDefined();
      expect(state.bloomState.filter).toBeDefined();
      expect(state.bloomState.k).toBeGreaterThan(0);
      expect(state.bloomState.m).toBeGreaterThan(0);
    });

    it('should continue writing after restore', async () => {
      const r2 = createMockR2Bucket();
      const writer1 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer1.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      await writer1.flush();

      const state = writer1.getState();

      // Create new writer and restore
      r2.reset();
      const writer2 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });
      writer2.restoreState(state);

      // Add more triples
      await writer2.addTriples([makeTestTriple(3), makeTestTriple(4)]);
      const result = await writer2.finalize();

      // Should have 4 total (2 from restored state + 2 new)
      expect(result.totalTriples).toBe(4);
    });

    it('should preserve chunk infos on restore', async () => {
      const r2 = createMockR2Bucket();
      const writer1 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      await writer1.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      const state = writer1.getState();

      const writer2 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });
      writer2.restoreState(state);

      const restoredState = writer2.getState();
      expect(restoredState.chunkInfos).toHaveLength(1);
      expect(restoredState.chunkInfos[0]!.tripleCount).toBe(2);
    });
  });

  describe('Batch Size Configuration', () => {
    it('should default to 10K batch size', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/');

      // Add 9999 triples - should not flush
      const triples: Triple[] = [];
      for (let i = 0; i < 9999; i++) {
        triples.push(makeTestTriple(i));
      }
      await writer.addTriples(triples);
      expect(r2.puts.size).toBe(0);

      // Add 1 more - should trigger flush
      await writer.addTriple(makeTestTriple(9999));
      expect(r2.puts.size).toBe(1);
    });

    it('should respect custom batch size', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 50,
      });

      const triples: Triple[] = [];
      for (let i = 0; i < 49; i++) {
        triples.push(makeTestTriple(i));
      }
      await writer.addTriples(triples);
      expect(r2.puts.size).toBe(0);

      await writer.addTriple(makeTestTriple(49));
      expect(r2.puts.size).toBe(1);
    });
  });

  describe('Bloom Filter Generation', () => {
    it('should include bloom filter in chunk info', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 3,
      });

      await writer.addTriples([makeTestTriple(1), makeTestTriple(2), makeTestTriple(3)]);
      const result = await writer.finalize();

      expect(result.chunks[0]!.bloom).toBeDefined();
      expect(result.chunks[0]!.bloom!.filter).toBeDefined();
      expect(result.chunks[0]!.bloom!.k).toBeGreaterThan(0);
    });

    it('should include REF entities in bloom filter', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriples([makeRefTriple(1, 10), makeRefTriple(2, 20)]);
      const result = await writer.finalize();

      // Bloom filter should be generated (we can't easily test contents without bloom check)
      expect(result.combinedBloom).toBeDefined();
      expect(result.chunks[0]!.bloom).toBeDefined();
    });

    it('should include REF_ARRAY entities in bloom filter', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriple(makeRefArrayTriple(1, [10, 20, 30]));
      const result = await writer.finalize();

      expect(result.combinedBloom).toBeDefined();
      expect(result.chunks[0]!.bloom).toBeDefined();
    });

    it('should respect bloom filter options', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
        bloomCapacity: 500,
        bloomFpr: 0.001,
      });

      await writer.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      const result = await writer.finalize();

      // Bloom filter should be created with specified parameters
      expect(result.combinedBloom).toBeDefined();
    });
  });

  describe('GraphCol Encoding', () => {
    it('should encode triples to GraphCol format', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriple(makeTestTriple(1));
      await writer.flush();

      // Verify data was written
      expect(r2.puts.size).toBe(1);
      const data = Array.from(r2.puts.values())[0]!;
      expect(data.length).toBeGreaterThan(0);
    });

    it('should write different object types correctly', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriples([
        makeTestTriple(1), // STRING
        makeRefTriple(2, 10), // REF
        {
          subject: createEntityId('https://example.com/entity/3'),
          predicate: createPredicate('count'),
          object: { type: ObjectType.INT64, value: 42n },
          timestamp: BigInt(Date.now()),
          txId: createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
        },
      ]);

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(3);
      expect(result.totalBytes).toBeGreaterThan(0);
    });
  });

  describe('Memory Efficiency', () => {
    it('should process many triples without excessive memory', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 1000,
      });

      // Add 5000 triples
      for (let batch = 0; batch < 5; batch++) {
        const triples = Array.from({ length: 1000 }, (_, i) => makeTestTriple(batch * 1000 + i));
        await writer.addTriples(triples);
      }

      const result = await writer.finalize();
      expect(result.totalTriples).toBe(5000);
      expect(result.totalChunks).toBe(5);
    });
  });
});
