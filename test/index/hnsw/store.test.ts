/**
 * HNSW Storage Tests
 *
 * TDD tests for HNSW storage implementations:
 * - SQLiteGraphStore: DO SQLite-backed graph storage
 * - R2VectorStore: R2-backed vector storage
 *
 * Tests use mocks for DO SQLite and R2 to enable unit testing.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import type {
  HNSWNode,
  VectorStore,
  GraphStore,
  HNSWConfig,
} from '../../../src/index/hnsw/store.js';
import {
  DEFAULT_HNSW_CONFIG,
  cosineDistance,
  euclideanDistance,
  randomLevel,
  float32ToArray,
  arrayToFloat32,
} from '../../../src/index/hnsw/store.js';
import { SQLiteGraphStore } from '../../../src/index/hnsw/sqlite-graph-store.js';
import { R2VectorStore } from '../../../src/index/hnsw/r2-vector-store.js';

// ============================================================================
// MOCK HELPERS
// ============================================================================

/**
 * Create a mock SqlStorage for testing
 */
function createMockSqlStorage() {
  const tables: Record<string, Map<string, Record<string, unknown>>> = {
    hnsw_meta: new Map(),
    hnsw_nodes: new Map(),
    hnsw_edges: new Map(),
  };

  const execResults: Record<string, unknown>[] = [];

  return {
    exec: vi.fn((sql: string, ...params: unknown[]) => {
      // Parse SQL and simulate behavior
      const sqlLower = sql.toLowerCase().trim();

      if (sqlLower.startsWith('create table')) {
        // Table creation - no-op for mock
        return { toArray: () => [] };
      }

      if (sqlLower.startsWith('insert into hnsw_meta')) {
        const [key, value] = params as [string, string];
        tables.hnsw_meta.set(key, { key, value });
        return { toArray: () => [] };
      }

      if (sqlLower.startsWith('insert or replace into hnsw_meta')) {
        const [key, value] = params as [string, string];
        tables.hnsw_meta.set(key, { key, value });
        return { toArray: () => [] };
      }

      if (sqlLower.startsWith('select value from hnsw_meta')) {
        const [key] = params as [string];
        const entry = tables.hnsw_meta.get(key);
        return { toArray: () => (entry ? [{ value: entry.value }] : []) };
      }

      if (sqlLower.startsWith('insert into hnsw_nodes') || sqlLower.startsWith('insert or replace into hnsw_nodes')) {
        const [nodeId, maxLayer] = params as [string, number];
        tables.hnsw_nodes.set(nodeId, { node_id: nodeId, max_layer: maxLayer });
        return { toArray: () => [] };
      }

      if (sqlLower.startsWith('select * from hnsw_nodes where node_id')) {
        const [nodeId] = params as [string];
        const entry = tables.hnsw_nodes.get(nodeId);
        return { toArray: () => (entry ? [entry] : []) };
      }

      if (sqlLower.startsWith('select * from hnsw_nodes')) {
        return { toArray: () => Array.from(tables.hnsw_nodes.values()) };
      }

      if (sqlLower.startsWith('select count(*) as cnt from hnsw_nodes')) {
        return { toArray: () => [{ cnt: tables.hnsw_nodes.size }] };
      }

      if (sqlLower.startsWith('select max(max_layer) as max_layer from hnsw_nodes')) {
        let maxLayer = -1;
        for (const node of tables.hnsw_nodes.values()) {
          if ((node.max_layer as number) > maxLayer) {
            maxLayer = node.max_layer as number;
          }
        }
        return { toArray: () => [{ max_layer: tables.hnsw_nodes.size > 0 ? maxLayer : null }] };
      }

      if (sqlLower.startsWith('insert into hnsw_edges') || sqlLower.startsWith('insert or replace into hnsw_edges')) {
        const [nodeId, layer, connections] = params as [string, number, string];
        const key = `${nodeId}:${layer}`;
        tables.hnsw_edges.set(key, { node_id: nodeId, layer, connections });
        return { toArray: () => [] };
      }

      if (sqlLower.startsWith('select connections from hnsw_edges where node_id')) {
        const [nodeId] = params as [string];
        const results: { connections: string }[] = [];
        for (const [key, entry] of tables.hnsw_edges.entries()) {
          if (key.startsWith(nodeId + ':')) {
            results.push({ connections: entry.connections as string });
          }
        }
        // Sort by layer (extracted from key)
        results.sort((a, b) => {
          const layerA = parseInt(Array.from(tables.hnsw_edges.entries()).find(([, v]) => v.connections === a.connections)?.[0]?.split(':')[1] || '0');
          const layerB = parseInt(Array.from(tables.hnsw_edges.entries()).find(([, v]) => v.connections === b.connections)?.[0]?.split(':')[1] || '0');
          return layerA - layerB;
        });
        return { toArray: () => results };
      }

      if (sqlLower.startsWith('delete from hnsw_nodes where node_id')) {
        const [nodeId] = params as [string];
        tables.hnsw_nodes.delete(nodeId);
        return { toArray: () => [] };
      }

      if (sqlLower.startsWith('delete from hnsw_edges where node_id')) {
        const [nodeId] = params as [string];
        for (const key of tables.hnsw_edges.keys()) {
          if (key.startsWith(nodeId + ':')) {
            tables.hnsw_edges.delete(key);
          }
        }
        return { toArray: () => [] };
      }

      // Default: return empty
      return { toArray: () => [] };
    }),

    // For testing: access internal state
    _tables: tables,
  };
}

/**
 * Create a mock R2Bucket for testing
 */
function createMockR2Bucket() {
  const storage = new Map<string, ArrayBuffer>();

  return {
    put: vi.fn(async (key: string, value: ArrayBuffer | string) => {
      if (typeof value === 'string') {
        const encoder = new TextEncoder();
        storage.set(key, encoder.encode(value).buffer);
      } else {
        storage.set(key, value);
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

    list: vi.fn(async (options?: { prefix?: string; limit?: number; cursor?: string }) => {
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
      };
    }),

    // For testing: access internal state
    _storage: storage,
  };
}

// ============================================================================
// UTILITY FUNCTION TESTS
// ============================================================================

describe('HNSW Utility Functions', () => {
  describe('cosineDistance', () => {
    it('should return 0 for identical vectors', () => {
      const a = [1, 2, 3];
      const distance = cosineDistance(a, a);
      expect(distance).toBeCloseTo(0, 10);
    });

    it('should return 2 for opposite vectors', () => {
      const a = [1, 0, 0];
      const b = [-1, 0, 0];
      const distance = cosineDistance(a, b);
      expect(distance).toBeCloseTo(2, 10);
    });

    it('should return 1 for orthogonal vectors', () => {
      const a = [1, 0];
      const b = [0, 1];
      const distance = cosineDistance(a, b);
      expect(distance).toBeCloseTo(1, 10);
    });

    it('should throw for mismatched dimensions', () => {
      expect(() => cosineDistance([1, 2], [1, 2, 3])).toThrow('Vector dimension mismatch');
    });

    it('should handle zero vectors', () => {
      const distance = cosineDistance([0, 0, 0], [1, 2, 3]);
      expect(distance).toBe(1); // Maximally distant
    });
  });

  describe('euclideanDistance', () => {
    it('should return 0 for identical vectors', () => {
      const a = [1, 2, 3];
      const distance = euclideanDistance(a, a);
      expect(distance).toBe(0);
    });

    it('should calculate correct distance', () => {
      const a = [0, 0];
      const b = [3, 4];
      const distance = euclideanDistance(a, b);
      expect(distance).toBe(5);
    });

    it('should throw for mismatched dimensions', () => {
      expect(() => euclideanDistance([1], [1, 2])).toThrow('Vector dimension mismatch');
    });
  });

  describe('randomLevel', () => {
    it('should return non-negative integers', () => {
      for (let i = 0; i < 100; i++) {
        const level = randomLevel(DEFAULT_HNSW_CONFIG.levelMultiplier);
        expect(level).toBeGreaterThanOrEqual(0);
        expect(Number.isInteger(level)).toBe(true);
      }
    });

    it('should mostly return 0 (base layer)', () => {
      const levels: number[] = [];
      for (let i = 0; i < 1000; i++) {
        levels.push(randomLevel(DEFAULT_HNSW_CONFIG.levelMultiplier));
      }
      const zeros = levels.filter((l) => l === 0).length;
      // Should be roughly 1 - 1/e ≈ 63% at level 0
      expect(zeros / 1000).toBeGreaterThan(0.5);
    });
  });

  describe('float32 conversion', () => {
    it('should round-trip correctly', () => {
      const original = [1.5, -2.5, 3.14159, 0, -0.001];
      const float32 = arrayToFloat32(original);
      const recovered = float32ToArray(float32);

      expect(recovered.length).toBe(original.length);
      for (let i = 0; i < original.length; i++) {
        expect(recovered[i]).toBeCloseTo(original[i]!, 5);
      }
    });
  });

  describe('DEFAULT_HNSW_CONFIG', () => {
    it('should have sensible defaults', () => {
      expect(DEFAULT_HNSW_CONFIG.maxConnections).toBe(16);
      expect(DEFAULT_HNSW_CONFIG.maxConnectionsLayer0).toBe(32);
      expect(DEFAULT_HNSW_CONFIG.efConstruction).toBe(100);
      expect(DEFAULT_HNSW_CONFIG.levelMultiplier).toBeCloseTo(1 / Math.log(16), 10);
    });
  });
});

// ============================================================================
// SQLITE GRAPH STORE TESTS
// ============================================================================

describe('SQLiteGraphStore', () => {
  let mockSql: ReturnType<typeof createMockSqlStorage>;
  let graphStore: SQLiteGraphStore;

  beforeEach(() => {
    mockSql = createMockSqlStorage();
    graphStore = new SQLiteGraphStore(mockSql as unknown as SqlStorage);
  });

  describe('initialization', () => {
    it('should create tables on first operation', async () => {
      await graphStore.saveNode({
        nodeId: 'test',
        maxLayer: 0,
        connections: [[]],
      });

      expect(mockSql.exec).toHaveBeenCalled();
      const calls = mockSql.exec.mock.calls;
      const createTableCalls = calls.filter((c) =>
        (c[0] as string).toLowerCase().includes('create table')
      );
      expect(createTableCalls.length).toBeGreaterThan(0);
    });
  });

  describe('saveNode / loadNode', () => {
    it('should save and load a node', async () => {
      const node: HNSWNode = {
        nodeId: 'node1',
        maxLayer: 2,
        connections: [['a', 'b'], ['c'], []],
      };

      await graphStore.saveNode(node);
      const loaded = await graphStore.loadNode('node1');

      expect(loaded).not.toBeNull();
      expect(loaded!.nodeId).toBe('node1');
      expect(loaded!.maxLayer).toBe(2);
      expect(loaded!.connections).toHaveLength(3);
      expect(loaded!.connections[0]).toEqual(['a', 'b']);
      expect(loaded!.connections[1]).toEqual(['c']);
      expect(loaded!.connections[2]).toEqual([]);
    });

    it('should return null for non-existent node', async () => {
      const loaded = await graphStore.loadNode('nonexistent');
      expect(loaded).toBeNull();
    });

    it('should update existing node', async () => {
      const node1: HNSWNode = {
        nodeId: 'node1',
        maxLayer: 1,
        connections: [['a'], []],
      };

      await graphStore.saveNode(node1);

      const node2: HNSWNode = {
        nodeId: 'node1',
        maxLayer: 2,
        connections: [['a', 'b'], ['c'], []],
      };

      await graphStore.saveNode(node2);

      const loaded = await graphStore.loadNode('node1');
      expect(loaded!.maxLayer).toBe(2);
      expect(loaded!.connections[0]).toEqual(['a', 'b']);
    });
  });

  describe('loadAllNodes', () => {
    it('should return empty array for empty graph', async () => {
      const nodes = await graphStore.loadAllNodes();
      expect(nodes).toEqual([]);
    });

    it('should return all nodes', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 1, connections: [[], []] });

      const nodes = await graphStore.loadAllNodes();
      expect(nodes.length).toBe(2);

      const nodeIds = nodes.map((n) => n.nodeId).sort();
      expect(nodeIds).toEqual(['n1', 'n2']);
    });
  });

  describe('entry point', () => {
    it('should save and load entry point', async () => {
      await graphStore.saveEntryPoint('entry1');
      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBe('entry1');
    });

    it('should return null for unset entry point', async () => {
      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBeNull();
    });

    it('should allow clearing entry point', async () => {
      await graphStore.saveEntryPoint('entry1');
      await graphStore.saveEntryPoint(null);
      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBeNull();
    });
  });

  describe('deleteNode', () => {
    it('should delete a node and its edges', async () => {
      await graphStore.saveNode({
        nodeId: 'n1',
        maxLayer: 1,
        connections: [['n2'], ['n2']],
      });

      await graphStore.deleteNode('n1');

      const loaded = await graphStore.loadNode('n1');
      expect(loaded).toBeNull();
    });

    it('should handle deleting non-existent node', async () => {
      // Should not throw
      await graphStore.deleteNode('nonexistent');
    });
  });

  describe('nodeCount', () => {
    it('should return 0 for empty graph', async () => {
      const count = await graphStore.nodeCount();
      expect(count).toBe(0);
    });

    it('should return correct count', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 0, connections: [[]] });

      const count = await graphStore.nodeCount();
      expect(count).toBe(2);
    });
  });

  describe('maxLayer', () => {
    it('should return -1 for empty graph', async () => {
      const max = await graphStore.maxLayer();
      expect(max).toBe(-1);
    });

    it('should return correct max layer', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 3, connections: [[], [], [], []] });
      await graphStore.saveNode({ nodeId: 'n3', maxLayer: 1, connections: [[], []] });

      const max = await graphStore.maxLayer();
      expect(max).toBe(3);
    });
  });
});

// ============================================================================
// R2 VECTOR STORE TESTS
// ============================================================================

describe('R2VectorStore', () => {
  let mockR2: ReturnType<typeof createMockR2Bucket>;
  let vectorStore: R2VectorStore;

  beforeEach(() => {
    mockR2 = createMockR2Bucket();
    vectorStore = new R2VectorStore(mockR2 as unknown as R2Bucket, 'test-predicate');
  });

  describe('saveVector / loadVector', () => {
    it('should save and load a vector', async () => {
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

    it('should use correct key format', async () => {
      await vectorStore.saveVector('my-node', [1, 2, 3]);

      expect(mockR2.put).toHaveBeenCalledWith(
        'vectors/test-predicate/my-node',
        expect.any(ArrayBuffer)
      );
    });

    it('should handle high-dimensional vectors', async () => {
      const dim = 384;
      const vector = Array(dim)
        .fill(0)
        .map((_, i) => Math.sin(i));

      await vectorStore.saveVector('high-dim', vector);
      const loaded = await vectorStore.loadVector('high-dim');

      expect(loaded!.length).toBe(dim);
      for (let i = 0; i < dim; i++) {
        expect(loaded![i]).toBeCloseTo(vector[i]!, 5);
      }
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

      const loaded = await vectorStore.loadVectors(['n1', 'nonexistent']);

      expect(loaded.size).toBe(1);
      expect(loaded.has('n1')).toBe(true);
      expect(loaded.has('nonexistent')).toBe(false);
    });

    it('should handle empty array', async () => {
      const loaded = await vectorStore.loadVectors([]);
      expect(loaded.size).toBe(0);
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
      await vectorStore.deleteVector('nonexistent');
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
  });

  describe('predicate namespacing', () => {
    it('should namespace vectors by predicate', async () => {
      const store1 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'embedding1');
      const store2 = new R2VectorStore(mockR2 as unknown as R2Bucket, 'embedding2');

      await store1.saveVector('node1', [1, 0]);
      await store2.saveVector('node1', [0, 1]);

      const v1 = await store1.loadVector('node1');
      const v2 = await store2.loadVector('node1');

      expect(v1![0]).toBeCloseTo(1, 5);
      expect(v2![1]).toBeCloseTo(1, 5);
    });
  });
});

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

describe('GraphStore + VectorStore Integration', () => {
  let mockSql: ReturnType<typeof createMockSqlStorage>;
  let mockR2: ReturnType<typeof createMockR2Bucket>;
  let graphStore: SQLiteGraphStore;
  let vectorStore: R2VectorStore;

  beforeEach(() => {
    mockSql = createMockSqlStorage();
    mockR2 = createMockR2Bucket();
    graphStore = new SQLiteGraphStore(mockSql as unknown as SqlStorage);
    vectorStore = new R2VectorStore(mockR2 as unknown as R2Bucket, 'embedding');
  });

  it('should store graph structure and vectors separately', async () => {
    // Save a node with its graph structure
    await graphStore.saveNode({
      nodeId: 'entity1',
      maxLayer: 1,
      connections: [['entity2', 'entity3'], ['entity2']],
    });

    // Save the node's vector separately
    await vectorStore.saveVector('entity1', [1.0, 0.5, -0.3]);

    // Load both
    const node = await graphStore.loadNode('entity1');
    const vector = await vectorStore.loadVector('entity1');

    expect(node).not.toBeNull();
    expect(node!.connections[0]).toEqual(['entity2', 'entity3']);
    expect(vector).not.toBeNull();
    expect(vector![0]).toBeCloseTo(1.0, 5);
  });

  it('should support batch vector loading for search', async () => {
    // Create a small graph
    const nodeIds = ['n1', 'n2', 'n3', 'n4', 'n5'];

    for (const id of nodeIds) {
      await graphStore.saveNode({
        nodeId: id,
        maxLayer: 0,
        connections: [nodeIds.filter((n) => n !== id)],
      });
      await vectorStore.saveVector(
        id,
        Array(3)
          .fill(0)
          .map((_, i) => parseInt(id.slice(1)) * (i + 1))
      );
    }

    // Batch load vectors (as would happen during search)
    const vectors = await vectorStore.loadVectors(['n1', 'n3', 'n5']);

    expect(vectors.size).toBe(3);
    expect(vectors.get('n1')![0]).toBeCloseTo(1, 5);
    expect(vectors.get('n3')![0]).toBeCloseTo(3, 5);
    expect(vectors.get('n5')![0]).toBeCloseTo(5, 5);
  });

  it('should delete both graph and vector data', async () => {
    await graphStore.saveNode({
      nodeId: 'toDelete',
      maxLayer: 0,
      connections: [[]],
    });
    await vectorStore.saveVector('toDelete', [1, 2, 3]);

    // Delete both
    await graphStore.deleteNode('toDelete');
    await vectorStore.deleteVector('toDelete');

    expect(await graphStore.loadNode('toDelete')).toBeNull();
    expect(await vectorStore.loadVector('toDelete')).toBeNull();
  });
});
