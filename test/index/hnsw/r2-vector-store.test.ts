/**
 * R2 Vector Store Tests
 *
 * TDD tests for the R2-backed HNSW vector storage implementation.
 * Tests include both the R2VectorStore (with mocked R2) and
 * MemoryVectorStore (for testing purposes).
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  R2VectorStore,
  MemoryVectorStore,
} from '../../../src/index/hnsw/r2-vector-store.js';

// ============================================================================
// MOCK HELPERS
// ============================================================================

/**
 * Create a mock R2Bucket for testing
 * Simulates R2 bucket behavior with in-memory storage
 */
function createMockR2Bucket() {
  const storage = new Map<string, ArrayBuffer>();

  return {
    put: vi.fn(async (key: string, value: ArrayBuffer | ArrayBufferLike | string) => {
      if (typeof value === 'string') {
        const encoder = new TextEncoder();
        storage.set(key, encoder.encode(value).buffer);
      } else {
        // Handle ArrayBuffer and ArrayBufferLike (including ArrayBuffer from Float32Array)
        storage.set(key, value as ArrayBuffer);
      }
    }),

    get: vi.fn(async (key: string) => {
      const data = storage.get(key);
      if (!data) return null;
      return {
        arrayBuffer: async () => data,
        text: async () => new TextDecoder().decode(data),
      };
    }),

    delete: vi.fn(async (key: string) => {
      storage.delete(key);
    }),

    list: vi.fn(
      async (options?: { prefix?: string; limit?: number; cursor?: string }) => {
        const prefix = options?.prefix || '';
        const limit = options?.limit || 1000;
        const objects: Array<{ key: string }> = [];

        for (const key of storage.keys()) {
          if (key.startsWith(prefix)) {
            objects.push({ key });
          }
        }

        return {
          objects: objects.slice(0, limit),
          truncated: objects.length > limit,
          cursor: objects.length > limit ? 'next-cursor' : undefined,
        };
      }
    ),

    // For testing: access internal state
    _storage: storage,
  };
}

// ============================================================================
// R2 VECTOR STORE TESTS
// ============================================================================

describe('R2VectorStore', () => {
  let mockR2: ReturnType<typeof createMockR2Bucket>;
  let vectorStore: R2VectorStore;

  beforeEach(() => {
    mockR2 = createMockR2Bucket();
    vectorStore = new R2VectorStore(mockR2 as unknown as R2Bucket, 'embedding');
  });

  // ============================================================================
  // BASIC OPERATIONS
  // ============================================================================

  describe('saveVector', () => {
    it('should save a vector as Float32Array binary data', async () => {
      const vector = [1.0, 2.0, 3.0, 4.0];
      await vectorStore.saveVector('node1', vector);

      expect(mockR2.put).toHaveBeenCalledWith(
        'vectors/embedding/node1',
        expect.any(ArrayBuffer)
      );
    });

    it('should store vector with correct dimensions', async () => {
      const vector = [1.5, -2.5, 3.14159, 0, -0.001];
      await vectorStore.saveVector('node1', vector);

      const stored = mockR2._storage.get('vectors/embedding/node1');
      expect(stored).toBeDefined();
      const float32 = new Float32Array(stored!);
      expect(float32.length).toBe(5);
    });

    it('should handle empty vector', async () => {
      await vectorStore.saveVector('empty', []);
      const stored = mockR2._storage.get('vectors/embedding/empty');
      expect(stored).toBeDefined();
      const float32 = new Float32Array(stored!);
      expect(float32.length).toBe(0);
    });

    it('should overwrite existing vector', async () => {
      await vectorStore.saveVector('node1', [1, 2, 3]);
      await vectorStore.saveVector('node1', [4, 5, 6, 7]);

      const loaded = await vectorStore.loadVector('node1');
      expect(loaded?.length).toBe(4);
      expect(loaded?.[0]).toBeCloseTo(4, 5);
    });
  });

  describe('loadVector', () => {
    it('should load a saved vector', async () => {
      const vector = [1.0, 2.0, 3.0, 4.0];
      await vectorStore.saveVector('node1', vector);

      const loaded = await vectorStore.loadVector('node1');

      expect(loaded).not.toBeNull();
      expect(loaded!.length).toBe(4);
      expect(loaded![0]).toBeCloseTo(1.0, 5);
      expect(loaded![1]).toBeCloseTo(2.0, 5);
      expect(loaded![2]).toBeCloseTo(3.0, 5);
      expect(loaded![3]).toBeCloseTo(4.0, 5);
    });

    it('should return null for non-existent vector', async () => {
      const loaded = await vectorStore.loadVector('nonexistent');
      expect(loaded).toBeNull();
    });

    it('should handle high-dimensional vectors', async () => {
      const dim = 384;
      const vector = Array(dim)
        .fill(0)
        .map((_, i) => Math.sin(i * 0.1));

      await vectorStore.saveVector('high-dim', vector);
      const loaded = await vectorStore.loadVector('high-dim');

      expect(loaded!.length).toBe(dim);
      for (let i = 0; i < dim; i++) {
        expect(loaded![i]).toBeCloseTo(vector[i]!, 5);
      }
    });

    it('should handle 1536-dimensional vectors (OpenAI embedding size)', async () => {
      const dim = 1536;
      const vector = Array(dim)
        .fill(0)
        .map((_, i) => (i % 10) / 10 - 0.5);

      await vectorStore.saveVector('openai', vector);
      const loaded = await vectorStore.loadVector('openai');

      expect(loaded!.length).toBe(dim);
      // Spot check some values
      expect(loaded![0]).toBeCloseTo(vector[0]!, 5);
      expect(loaded![100]).toBeCloseTo(vector[100]!, 5);
      expect(loaded![1000]).toBeCloseTo(vector[1000]!, 5);
    });

    it('should preserve floating point precision within Float32 limits', async () => {
      const vector = [Math.PI, Math.E, Math.SQRT2, -Math.LOG2E];
      await vectorStore.saveVector('precision', vector);
      const loaded = await vectorStore.loadVector('precision');

      // Float32 has ~7 decimal digits of precision
      expect(loaded![0]).toBeCloseTo(Math.PI, 5);
      expect(loaded![1]).toBeCloseTo(Math.E, 5);
      expect(loaded![2]).toBeCloseTo(Math.SQRT2, 5);
      expect(loaded![3]).toBeCloseTo(-Math.LOG2E, 5);
    });
  });

  describe('loadVectors (batch)', () => {
    it('should load multiple vectors', async () => {
      await vectorStore.saveVector('n1', [1, 0, 0]);
      await vectorStore.saveVector('n2', [0, 1, 0]);
      await vectorStore.saveVector('n3', [0, 0, 1]);

      const loaded = await vectorStore.loadVectors(['n1', 'n2', 'n3']);

      expect(loaded.size).toBe(3);
      expect(loaded.get('n1')![0]).toBeCloseTo(1, 5);
      expect(loaded.get('n2')![1]).toBeCloseTo(1, 5);
      expect(loaded.get('n3')![2]).toBeCloseTo(1, 5);
    });

    it('should skip non-existent vectors', async () => {
      await vectorStore.saveVector('n1', [1, 2, 3]);

      const loaded = await vectorStore.loadVectors(['n1', 'nonexistent', 'alsoMissing']);

      expect(loaded.size).toBe(1);
      expect(loaded.has('n1')).toBe(true);
      expect(loaded.has('nonexistent')).toBe(false);
    });

    it('should handle empty array', async () => {
      const loaded = await vectorStore.loadVectors([]);
      expect(loaded.size).toBe(0);
    });

    it('should load vectors in parallel', async () => {
      // Save many vectors
      for (let i = 0; i < 20; i++) {
        await vectorStore.saveVector(`v${i}`, [i, i * 2, i * 3]);
      }

      const ids = Array(20)
        .fill(0)
        .map((_, i) => `v${i}`);
      const loaded = await vectorStore.loadVectors(ids);

      expect(loaded.size).toBe(20);
      expect(loaded.get('v5')![0]).toBeCloseTo(5, 5);
      expect(loaded.get('v15')![1]).toBeCloseTo(30, 5);
    });

    it('should handle partial results', async () => {
      await vectorStore.saveVector('exists1', [1]);
      await vectorStore.saveVector('exists2', [2]);

      const loaded = await vectorStore.loadVectors([
        'exists1',
        'missing1',
        'exists2',
        'missing2',
      ]);

      expect(loaded.size).toBe(2);
      expect(loaded.has('exists1')).toBe(true);
      expect(loaded.has('exists2')).toBe(true);
    });
  });

  describe('deleteVector', () => {
    it('should delete a vector', async () => {
      await vectorStore.saveVector('n1', [1, 2, 3]);
      await vectorStore.deleteVector('n1');

      const loaded = await vectorStore.loadVector('n1');
      expect(loaded).toBeNull();
    });

    it('should handle deleting non-existent vector', async () => {
      // Should not throw
      await expect(vectorStore.deleteVector('nonexistent')).resolves.not.toThrow();
    });

    it('should call R2 delete with correct key', async () => {
      await vectorStore.deleteVector('myNode');
      expect(mockR2.delete).toHaveBeenCalledWith('vectors/embedding/myNode');
    });
  });

  describe('count', () => {
    it('should return 0 for empty store', async () => {
      const count = await vectorStore.count();
      expect(count).toBe(0);
    });

    it('should return correct count', async () => {
      await vectorStore.saveVector('n1', [1]);
      await vectorStore.saveVector('n2', [2]);
      await vectorStore.saveVector('n3', [3]);

      const count = await vectorStore.count();
      expect(count).toBe(3);
    });

    it('should use prefix filtering', async () => {
      // Add vectors to different predicates
      const store1 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'pred1');
      const store2 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'pred2');

      await store1.saveVector('n1', [1]);
      await store1.saveVector('n2', [2]);
      await store2.saveVector('n1', [3]);

      // Each store should only count its own vectors
      expect(await store1.count()).toBe(2);
      expect(await store2.count()).toBe(1);
    });

    it('should update after deletions', async () => {
      await vectorStore.saveVector('n1', [1]);
      await vectorStore.saveVector('n2', [2]);
      expect(await vectorStore.count()).toBe(2);

      await vectorStore.deleteVector('n1');
      expect(await vectorStore.count()).toBe(1);
    });
  });

  // ============================================================================
  // PREDICATE NAMESPACING
  // ============================================================================

  describe('predicate namespacing', () => {
    it('should namespace vectors by predicate', async () => {
      const store1 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'embedding1');
      const store2 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'embedding2');

      await store1.saveVector('node1', [1, 0]);
      await store2.saveVector('node1', [0, 1]);

      const v1 = await store1.loadVector('node1');
      const v2 = await store2.loadVector('node1');

      expect(v1![0]).toBeCloseTo(1, 5);
      expect(v1![1]).toBeCloseTo(0, 5);
      expect(v2![0]).toBeCloseTo(0, 5);
      expect(v2![1]).toBeCloseTo(1, 5);
    });

    it('should isolate deletions by predicate', async () => {
      const store1 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'pred1');
      const store2 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'pred2');

      await store1.saveVector('node1', [1]);
      await store2.saveVector('node1', [2]);

      await store1.deleteVector('node1');

      expect(await store1.loadVector('node1')).toBeNull();
      expect(await store2.loadVector('node1')).not.toBeNull();
    });

    it('should report correct predicate', () => {
      const store = new R2VectorStore(mockR2 as unknown as R2Bucket, 'myPredicate');
      expect(store.getPredicate()).toBe('myPredicate');
    });

    it('should report correct key prefix', () => {
      const store = new R2VectorStore(mockR2 as unknown as R2Bucket, 'myPred');
      expect(store.getKeyPrefix()).toBe('vectors/myPred/');
    });
  });

  // ============================================================================
  // EDGE CASES
  // ============================================================================

  describe('edge cases', () => {
    it('should handle special characters in node IDs', async () => {
      const specialIds = [
        'node/with/slashes',
        'node:with:colons',
        'node with spaces',
        'unicode-\u00e9\u00e8\u00ea',
      ];

      for (const id of specialIds) {
        await vectorStore.saveVector(id, [1, 2, 3]);
        const loaded = await vectorStore.loadVector(id);
        expect(loaded).not.toBeNull();
        expect(loaded!.length).toBe(3);
      }
    });

    it('should handle very small values', async () => {
      const vector = [1e-38, -1e-38, 1e-30];
      await vectorStore.saveVector('tiny', vector);
      const loaded = await vectorStore.loadVector('tiny');

      expect(loaded![0]).toBeCloseTo(1e-38, 40);
      expect(loaded![1]).toBeCloseTo(-1e-38, 40);
    });

    it('should handle very large values', async () => {
      const vector = [1e38, -1e38, 3.4e38];
      await vectorStore.saveVector('huge', vector);
      const loaded = await vectorStore.loadVector('huge');

      expect(loaded![0]).toBeCloseTo(1e38, -35);
      expect(loaded![1]).toBeCloseTo(-1e38, -35);
    });

    it('should handle zero vector', async () => {
      const vector = [0, 0, 0, 0, 0];
      await vectorStore.saveVector('zero', vector);
      const loaded = await vectorStore.loadVector('zero');

      expect(loaded!.every((v) => v === 0)).toBe(true);
    });

    it('should handle negative values', async () => {
      const vector = [-1, -2, -3, -4, -5];
      await vectorStore.saveVector('negative', vector);
      const loaded = await vectorStore.loadVector('negative');

      for (let i = 0; i < 5; i++) {
        expect(loaded![i]).toBeCloseTo(-(i + 1), 5);
      }
    });
  });
});

// ============================================================================
// MEMORY VECTOR STORE TESTS
// ============================================================================

describe('MemoryVectorStore', () => {
  let memoryStore: MemoryVectorStore;

  beforeEach(() => {
    memoryStore = new MemoryVectorStore();
  });

  describe('saveVector / loadVector', () => {
    it('should save and load a vector', async () => {
      const vector = [1.0, 2.0, 3.0];
      await memoryStore.saveVector('node1', vector);

      const loaded = await memoryStore.loadVector('node1');

      expect(loaded).not.toBeNull();
      expect(loaded).toEqual([1.0, 2.0, 3.0]);
    });

    it('should return null for non-existent vector', async () => {
      const loaded = await memoryStore.loadVector('nonexistent');
      expect(loaded).toBeNull();
    });

    it('should store a copy (mutation safe)', async () => {
      const original = [1, 2, 3];
      await memoryStore.saveVector('node1', original);

      // Mutate original
      original[0] = 999;

      const loaded = await memoryStore.loadVector('node1');
      expect(loaded![0]).toBe(1); // Should be unchanged
    });

    it('should return a copy on load (mutation safe)', async () => {
      await memoryStore.saveVector('node1', [1, 2, 3]);

      const loaded1 = await memoryStore.loadVector('node1');
      loaded1![0] = 999;

      const loaded2 = await memoryStore.loadVector('node1');
      expect(loaded2![0]).toBe(1); // Should be unchanged
    });
  });

  describe('loadVectors', () => {
    it('should load multiple vectors', async () => {
      await memoryStore.saveVector('n1', [1]);
      await memoryStore.saveVector('n2', [2]);
      await memoryStore.saveVector('n3', [3]);

      const loaded = await memoryStore.loadVectors(['n1', 'n2', 'n3']);

      expect(loaded.size).toBe(3);
      expect(loaded.get('n1')).toEqual([1]);
      expect(loaded.get('n2')).toEqual([2]);
      expect(loaded.get('n3')).toEqual([3]);
    });

    it('should skip non-existent vectors', async () => {
      await memoryStore.saveVector('n1', [1]);

      const loaded = await memoryStore.loadVectors(['n1', 'missing']);

      expect(loaded.size).toBe(1);
      expect(loaded.has('n1')).toBe(true);
      expect(loaded.has('missing')).toBe(false);
    });

    it('should return copies of vectors', async () => {
      await memoryStore.saveVector('n1', [1, 2, 3]);

      const loaded = await memoryStore.loadVectors(['n1']);
      loaded.get('n1')![0] = 999;

      const loadedAgain = await memoryStore.loadVector('n1');
      expect(loadedAgain![0]).toBe(1);
    });
  });

  describe('deleteVector', () => {
    it('should delete a vector', async () => {
      await memoryStore.saveVector('n1', [1, 2, 3]);
      await memoryStore.deleteVector('n1');

      const loaded = await memoryStore.loadVector('n1');
      expect(loaded).toBeNull();
    });

    it('should handle deleting non-existent vector', async () => {
      await expect(memoryStore.deleteVector('nonexistent')).resolves.not.toThrow();
    });
  });

  describe('count', () => {
    it('should return 0 for empty store', async () => {
      expect(await memoryStore.count()).toBe(0);
    });

    it('should return correct count', async () => {
      await memoryStore.saveVector('n1', [1]);
      await memoryStore.saveVector('n2', [2]);
      expect(await memoryStore.count()).toBe(2);
    });

    it('should update after deletions', async () => {
      await memoryStore.saveVector('n1', [1]);
      await memoryStore.saveVector('n2', [2]);
      await memoryStore.deleteVector('n1');
      expect(await memoryStore.count()).toBe(1);
    });
  });

  describe('clear', () => {
    it('should remove all vectors', async () => {
      await memoryStore.saveVector('n1', [1]);
      await memoryStore.saveVector('n2', [2]);
      await memoryStore.saveVector('n3', [3]);

      memoryStore.clear();

      expect(await memoryStore.count()).toBe(0);
      expect(await memoryStore.loadVector('n1')).toBeNull();
      expect(await memoryStore.loadVector('n2')).toBeNull();
      expect(await memoryStore.loadVector('n3')).toBeNull();
    });

    it('should allow adding vectors after clear', async () => {
      await memoryStore.saveVector('old', [1]);
      memoryStore.clear();
      await memoryStore.saveVector('new', [2]);

      expect(await memoryStore.count()).toBe(1);
      expect(await memoryStore.loadVector('new')).toEqual([2]);
    });
  });

  // ============================================================================
  // TESTING HELPER USE CASES
  // ============================================================================

  describe('testing helper use cases', () => {
    it('should work as a drop-in replacement for R2VectorStore in tests', async () => {
      // This demonstrates how MemoryVectorStore can be used in place of R2VectorStore
      const vectors = new Map<string, number[]>();

      // Simulate building an index
      for (let i = 0; i < 100; i++) {
        const nodeId = `node${i}`;
        const vector = Array(8)
          .fill(0)
          .map(() => Math.random());
        await memoryStore.saveVector(nodeId, vector);
        vectors.set(nodeId, vector);
      }

      // Verify all vectors are stored correctly
      expect(await memoryStore.count()).toBe(100);

      // Batch load some vectors
      const ids = ['node0', 'node50', 'node99'];
      const loaded = await memoryStore.loadVectors(ids);

      expect(loaded.size).toBe(3);
      for (const id of ids) {
        expect(loaded.get(id)).toEqual(vectors.get(id));
      }
    });

    it('should support simulated HNSW search flow', async () => {
      // Save some vectors
      await memoryStore.saveVector('entry', [1, 0, 0]);
      await memoryStore.saveVector('a', [0.9, 0.1, 0]);
      await memoryStore.saveVector('b', [0.8, 0.2, 0]);
      await memoryStore.saveVector('c', [0, 1, 0]);

      // Simulate search: batch load neighbors
      const neighbors = await memoryStore.loadVectors(['entry', 'a', 'b', 'c']);

      // All should be loaded
      expect(neighbors.size).toBe(4);
      expect(neighbors.get('entry')).toEqual([1, 0, 0]);
    });
  });
});

// ============================================================================
// VECTORSTORE INTERFACE COMPLIANCE
// ============================================================================

describe('VectorStore Interface Compliance', () => {
  // Test both implementations conform to the same interface
  const implementations = [
    {
      name: 'R2VectorStore',
      create: () =>
        new R2VectorStore(createMockR2Bucket() as unknown as R2Bucket, 'test'),
    },
    {
      name: 'MemoryVectorStore',
      create: () => new MemoryVectorStore(),
    },
  ];

  for (const impl of implementations) {
    describe(`${impl.name} interface compliance`, () => {
      let store: R2VectorStore | MemoryVectorStore;

      beforeEach(() => {
        store = impl.create();
      });

      it('should implement saveVector', async () => {
        await expect(store.saveVector('id', [1, 2, 3])).resolves.not.toThrow();
      });

      it('should implement loadVector', async () => {
        await store.saveVector('id', [1, 2, 3]);
        const result = await store.loadVector('id');
        expect(result).not.toBeNull();
        expect(Array.isArray(result)).toBe(true);
      });

      it('should implement loadVectors', async () => {
        await store.saveVector('id', [1, 2, 3]);
        const result = await store.loadVectors(['id', 'missing']);
        expect(result instanceof Map).toBe(true);
        expect(result.size).toBe(1);
      });

      it('should implement deleteVector', async () => {
        await store.saveVector('id', [1, 2, 3]);
        await expect(store.deleteVector('id')).resolves.not.toThrow();
        expect(await store.loadVector('id')).toBeNull();
      });

      it('should implement count', async () => {
        expect(await store.count()).toBe(0);
        await store.saveVector('id', [1, 2, 3]);
        expect(await store.count()).toBe(1);
      });
    });
  }
});
