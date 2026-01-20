/**
 * SQLite Graph Store Tests
 *
 * TDD tests for the SQLite-backed HNSW graph storage implementation.
 * Tests use a mock SqlStorage to simulate DO SQLite behavior.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  SQLiteGraphStore,
  HNSW_GRAPH_SCHEMA,
} from '../../../src/index/hnsw/sqlite-graph-store.js';
import type { HNSWNode } from '../../../src/index/hnsw/store.js';

// ============================================================================
// MOCK HELPERS
// ============================================================================

/**
 * Create a mock SqlStorage for testing
 * Simulates DO SQLite behavior with in-memory storage
 */
function createMockSqlStorage() {
  const tables: Record<string, Map<string, Record<string, unknown>>> = {
    hnsw_meta: new Map(),
    hnsw_nodes: new Map(),
    hnsw_edges: new Map(),
  };

  return {
    exec: vi.fn((sql: string, ...params: unknown[]) => {
      const sqlLower = sql.toLowerCase().trim();

      // Table creation - no-op for mock
      if (sqlLower.startsWith('create table') || sqlLower.startsWith('create index')) {
        return { toArray: () => [] };
      }

      // INSERT OR REPLACE INTO hnsw_meta
      if (sqlLower.startsWith('insert or replace into hnsw_meta')) {
        const [key, value] = params as [string, string];
        tables.hnsw_meta.set(key, { key, value });
        return { toArray: () => [] };
      }

      // SELECT value FROM hnsw_meta
      if (sqlLower.startsWith('select value from hnsw_meta')) {
        const [key] = params as [string];
        const entry = tables.hnsw_meta.get(key);
        return { toArray: () => (entry ? [{ value: entry.value }] : []) };
      }

      // INSERT OR REPLACE INTO hnsw_nodes
      if (sqlLower.startsWith('insert or replace into hnsw_nodes')) {
        const [nodeId, maxLayer] = params as [string, number];
        tables.hnsw_nodes.set(nodeId, { node_id: nodeId, max_layer: maxLayer });
        return { toArray: () => [] };
      }

      // SELECT * FROM hnsw_nodes WHERE node_id = ?
      if (sqlLower.startsWith('select * from hnsw_nodes where node_id')) {
        const [nodeId] = params as [string];
        const entry = tables.hnsw_nodes.get(nodeId);
        return { toArray: () => (entry ? [entry] : []) };
      }

      // SELECT * FROM hnsw_nodes (all nodes)
      if (sqlLower === 'select * from hnsw_nodes') {
        return { toArray: () => Array.from(tables.hnsw_nodes.values()) };
      }

      // SELECT COUNT(*) as cnt FROM hnsw_nodes
      if (sqlLower.startsWith('select count(*) as cnt from hnsw_nodes')) {
        return { toArray: () => [{ cnt: tables.hnsw_nodes.size }] };
      }

      // SELECT MAX(max_layer) as max_layer FROM hnsw_nodes
      if (sqlLower.startsWith('select max(max_layer) as max_layer from hnsw_nodes')) {
        let maxLayer = -1;
        for (const node of tables.hnsw_nodes.values()) {
          const layer = node.max_layer as number;
          if (layer > maxLayer) {
            maxLayer = layer;
          }
        }
        return {
          toArray: () => [
            { max_layer: tables.hnsw_nodes.size > 0 ? maxLayer : null },
          ],
        };
      }

      // INSERT OR REPLACE INTO hnsw_edges
      if (sqlLower.startsWith('insert or replace into hnsw_edges')) {
        const [nodeId, layer, connections] = params as [string, number, string];
        const key = `${nodeId}:${layer}`;
        tables.hnsw_edges.set(key, { node_id: nodeId, layer, connections });
        return { toArray: () => [] };
      }

      // SELECT connections FROM hnsw_edges WHERE node_id = ? ORDER BY layer ASC
      if (sqlLower.startsWith('select connections from hnsw_edges where node_id')) {
        const [nodeId] = params as [string];
        const results: Array<{ connections: string; layer: number }> = [];

        for (const [key, entry] of tables.hnsw_edges.entries()) {
          if (key.startsWith(nodeId + ':')) {
            results.push({
              connections: entry.connections as string,
              layer: entry.layer as number,
            });
          }
        }

        // Sort by layer ascending
        results.sort((a, b) => a.layer - b.layer);

        return {
          toArray: () => results.map((r) => ({ connections: r.connections })),
        };
      }

      // DELETE FROM hnsw_edges WHERE node_id = ?
      if (sqlLower.startsWith('delete from hnsw_edges where node_id')) {
        const [nodeId] = params as [string];
        for (const key of Array.from(tables.hnsw_edges.keys())) {
          if (key.startsWith(nodeId + ':')) {
            tables.hnsw_edges.delete(key);
          }
        }
        return { toArray: () => [] };
      }

      // DELETE FROM hnsw_nodes WHERE node_id = ?
      if (sqlLower.startsWith('delete from hnsw_nodes where node_id')) {
        const [nodeId] = params as [string];
        tables.hnsw_nodes.delete(nodeId);
        return { toArray: () => [] };
      }

      // Default: return empty
      return { toArray: () => [] };
    }),

    // For testing: access internal state
    _tables: tables,
  };
}

describe('SQLiteGraphStore', () => {
  let mockSql: ReturnType<typeof createMockSqlStorage>;
  let graphStore: SQLiteGraphStore;

  beforeEach(() => {
    mockSql = createMockSqlStorage();
    graphStore = new SQLiteGraphStore(mockSql as unknown as SqlStorage);
  });

  // ============================================================================
  // SCHEMA TESTS
  // ============================================================================

  describe('HNSW_GRAPH_SCHEMA', () => {
    it('should define three tables', () => {
      expect(HNSW_GRAPH_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS hnsw_meta');
      expect(HNSW_GRAPH_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS hnsw_nodes');
      expect(HNSW_GRAPH_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS hnsw_edges');
    });

    it('should define index on hnsw_edges', () => {
      expect(HNSW_GRAPH_SCHEMA).toContain(
        'CREATE INDEX IF NOT EXISTS idx_hnsw_edges_node ON hnsw_edges(node_id)'
      );
    });
  });

  // ============================================================================
  // INITIALIZATION TESTS
  // ============================================================================

  describe('initialization', () => {
    it('should create tables on first operation', async () => {
      await graphStore.saveNode({
        nodeId: 'test',
        maxLayer: 0,
        connections: [[]],
      });

      expect(mockSql.exec).toHaveBeenCalled();
      const calls = mockSql.exec.mock.calls;
      const createTableCalls = calls.filter(
        (c) =>
          (c[0] as string).toLowerCase().includes('create table') ||
          (c[0] as string).toLowerCase().includes('create index')
      );
      expect(createTableCalls.length).toBeGreaterThan(0);
    });

    it('should only initialize once', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      const callCountAfterFirst = mockSql.exec.mock.calls.length;

      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 0, connections: [[]] });
      const callCountAfterSecond = mockSql.exec.mock.calls.length;

      // Second operation should not re-run schema creation
      // Only the INSERT statements should be added
      const additionalCalls = callCountAfterSecond - callCountAfterFirst;
      expect(additionalCalls).toBeLessThan(5); // Just node and edge inserts
    });
  });

  // ============================================================================
  // NODE OPERATIONS
  // ============================================================================

  describe('saveNode', () => {
    it('should save a node with metadata', async () => {
      const node: HNSWNode = {
        nodeId: 'node1',
        maxLayer: 2,
        connections: [['a', 'b'], ['c'], []],
      };

      await graphStore.saveNode(node);

      // Verify node was inserted
      const nodeEntry = mockSql._tables.hnsw_nodes.get('node1');
      expect(nodeEntry).toBeDefined();
      expect(nodeEntry!.max_layer).toBe(2);
    });

    it('should save connections for each layer', async () => {
      const node: HNSWNode = {
        nodeId: 'node1',
        maxLayer: 1,
        connections: [['a', 'b'], ['c']],
      };

      await graphStore.saveNode(node);

      // Check layer 0 connections
      const edge0 = mockSql._tables.hnsw_edges.get('node1:0');
      expect(edge0).toBeDefined();
      expect(JSON.parse(edge0!.connections as string)).toEqual(['a', 'b']);

      // Check layer 1 connections
      const edge1 = mockSql._tables.hnsw_edges.get('node1:1');
      expect(edge1).toBeDefined();
      expect(JSON.parse(edge1!.connections as string)).toEqual(['c']);
    });

    it('should handle node with empty connections', async () => {
      const node: HNSWNode = {
        nodeId: 'isolated',
        maxLayer: 0,
        connections: [[]],
      };

      await graphStore.saveNode(node);

      const edge0 = mockSql._tables.hnsw_edges.get('isolated:0');
      expect(edge0).toBeDefined();
      expect(JSON.parse(edge0!.connections as string)).toEqual([]);
    });

    it('should update existing node (upsert behavior)', async () => {
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

      const nodeEntry = mockSql._tables.hnsw_nodes.get('node1');
      expect(nodeEntry!.max_layer).toBe(2);
    });
  });

  describe('loadNode', () => {
    it('should load a saved node', async () => {
      await graphStore.saveNode({
        nodeId: 'node1',
        maxLayer: 2,
        connections: [['a', 'b'], ['c'], []],
      });

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

    it('should handle node with missing edge data gracefully', async () => {
      // Manually insert node without all edges
      mockSql._tables.hnsw_nodes.set('partial', {
        node_id: 'partial',
        max_layer: 2,
      });
      // Only insert layer 0 edge
      mockSql._tables.hnsw_edges.set('partial:0', {
        node_id: 'partial',
        layer: 0,
        connections: JSON.stringify(['a']),
      });

      const loaded = await graphStore.loadNode('partial');

      expect(loaded).not.toBeNull();
      expect(loaded!.connections[0]).toEqual(['a']);
      expect(loaded!.connections[1]).toEqual([]);
      expect(loaded!.connections[2]).toEqual([]);
    });
  });

  describe('loadAllNodes', () => {
    it('should return empty array for empty graph', async () => {
      const nodes = await graphStore.loadAllNodes();
      expect(nodes).toEqual([]);
    });

    it('should return all saved nodes', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 1, connections: [['n1'], []] });
      await graphStore.saveNode({
        nodeId: 'n3',
        maxLayer: 2,
        connections: [['n1', 'n2'], ['n2'], []],
      });

      const nodes = await graphStore.loadAllNodes();

      expect(nodes.length).toBe(3);
      const nodeIds = nodes.map((n) => n.nodeId).sort();
      expect(nodeIds).toEqual(['n1', 'n2', 'n3']);
    });

    it('should correctly reconstruct all node connections', async () => {
      await graphStore.saveNode({
        nodeId: 'n1',
        maxLayer: 1,
        connections: [['n2', 'n3'], ['n2']],
      });
      await graphStore.saveNode({
        nodeId: 'n2',
        maxLayer: 1,
        connections: [['n1', 'n3'], ['n1']],
      });

      const nodes = await graphStore.loadAllNodes();
      const n1 = nodes.find((n) => n.nodeId === 'n1');
      const n2 = nodes.find((n) => n.nodeId === 'n2');

      expect(n1!.connections[0]).toContain('n2');
      expect(n1!.connections[0]).toContain('n3');
      expect(n2!.connections[0]).toContain('n1');
      expect(n2!.connections[1]).toContain('n1');
    });
  });

  describe('deleteNode', () => {
    it('should delete a node and its edges', async () => {
      await graphStore.saveNode({
        nodeId: 'n1',
        maxLayer: 2,
        connections: [['n2'], ['n2'], []],
      });

      await graphStore.deleteNode('n1');

      const loaded = await graphStore.loadNode('n1');
      expect(loaded).toBeNull();

      // Check edges are also deleted
      expect(mockSql._tables.hnsw_edges.get('n1:0')).toBeUndefined();
      expect(mockSql._tables.hnsw_edges.get('n1:1')).toBeUndefined();
      expect(mockSql._tables.hnsw_edges.get('n1:2')).toBeUndefined();
    });

    it('should handle deleting non-existent node', async () => {
      // Should not throw
      await expect(graphStore.deleteNode('nonexistent')).resolves.not.toThrow();
    });

    it('should not affect other nodes when deleting', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 0, connections: [[]] });

      await graphStore.deleteNode('n1');

      expect(await graphStore.loadNode('n1')).toBeNull();
      expect(await graphStore.loadNode('n2')).not.toBeNull();
    });
  });

  // ============================================================================
  // ENTRY POINT OPERATIONS
  // ============================================================================

  describe('saveEntryPoint', () => {
    it('should save entry point', async () => {
      await graphStore.saveEntryPoint('entry1');

      const metaEntry = mockSql._tables.hnsw_meta.get('entry_point');
      expect(metaEntry).toBeDefined();
      expect(metaEntry!.value).toBe('entry1');
    });

    it('should update entry point', async () => {
      await graphStore.saveEntryPoint('entry1');
      await graphStore.saveEntryPoint('entry2');

      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBe('entry2');
    });

    it('should clear entry point when set to null', async () => {
      await graphStore.saveEntryPoint('entry1');
      await graphStore.saveEntryPoint(null);

      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBeNull();
    });
  });

  describe('loadEntryPoint', () => {
    it('should return null when not set', async () => {
      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBeNull();
    });

    it('should return saved entry point', async () => {
      await graphStore.saveEntryPoint('myEntry');
      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBe('myEntry');
    });

    it('should return null after clearing', async () => {
      await graphStore.saveEntryPoint('entry');
      await graphStore.saveEntryPoint(null);
      const loaded = await graphStore.loadEntryPoint();
      expect(loaded).toBeNull();
    });
  });

  // ============================================================================
  // STATISTICS
  // ============================================================================

  describe('nodeCount', () => {
    it('should return 0 for empty graph', async () => {
      const count = await graphStore.nodeCount();
      expect(count).toBe(0);
    });

    it('should return correct count', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n3', maxLayer: 0, connections: [[]] });

      const count = await graphStore.nodeCount();
      expect(count).toBe(3);
    });

    it('should update after deletions', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 0, connections: [[]] });

      expect(await graphStore.nodeCount()).toBe(2);

      await graphStore.deleteNode('n1');

      expect(await graphStore.nodeCount()).toBe(1);
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

    it('should handle single node', async () => {
      await graphStore.saveNode({ nodeId: 'solo', maxLayer: 5, connections: [[], [], [], [], [], []] });
      expect(await graphStore.maxLayer()).toBe(5);
    });

    it('should handle all nodes at layer 0', async () => {
      await graphStore.saveNode({ nodeId: 'n1', maxLayer: 0, connections: [[]] });
      await graphStore.saveNode({ nodeId: 'n2', maxLayer: 0, connections: [[]] });
      expect(await graphStore.maxLayer()).toBe(0);
    });
  });

  // ============================================================================
  // INTEGRATION SCENARIOS
  // ============================================================================

  describe('integration scenarios', () => {
    it('should handle typical HNSW construction flow', async () => {
      // Insert first node - becomes entry point
      await graphStore.saveNode({ nodeId: 'n0', maxLayer: 2, connections: [[], [], []] });
      await graphStore.saveEntryPoint('n0');

      // Insert more nodes with connections
      await graphStore.saveNode({
        nodeId: 'n1',
        maxLayer: 1,
        connections: [['n0'], ['n0']],
      });
      await graphStore.saveNode({
        nodeId: 'n2',
        maxLayer: 0,
        connections: [['n0', 'n1']],
      });
      await graphStore.saveNode({
        nodeId: 'n3',
        maxLayer: 0,
        connections: [['n1', 'n2']],
      });

      // Verify graph state
      expect(await graphStore.nodeCount()).toBe(4);
      expect(await graphStore.maxLayer()).toBe(2);
      expect(await graphStore.loadEntryPoint()).toBe('n0');

      // Verify connections
      const n1 = await graphStore.loadNode('n1');
      expect(n1!.connections[0]).toContain('n0');
      expect(n1!.connections[1]).toContain('n0');
    });

    it('should support rebuilding graph after clear', async () => {
      // Build initial graph
      await graphStore.saveNode({ nodeId: 'old1', maxLayer: 1, connections: [[], []] });
      await graphStore.saveEntryPoint('old1');

      // Clear by deleting all
      await graphStore.deleteNode('old1');

      // Rebuild
      await graphStore.saveNode({ nodeId: 'new1', maxLayer: 0, connections: [[]] });
      await graphStore.saveEntryPoint('new1');

      expect(await graphStore.nodeCount()).toBe(1);
      expect(await graphStore.loadEntryPoint()).toBe('new1');
    });

    it('should handle concurrent-like access patterns', async () => {
      // Simulate multiple operations in quick succession
      const promises: Promise<void>[] = [];

      for (let i = 0; i < 10; i++) {
        promises.push(
          graphStore.saveNode({
            nodeId: `node${i}`,
            maxLayer: i % 3,
            connections: Array(i % 3 + 1).fill([]).map((_, j) => (j === 0 ? [`node${(i + 1) % 10}`] : [])),
          })
        );
      }

      await Promise.all(promises);

      expect(await graphStore.nodeCount()).toBe(10);
      expect(await graphStore.maxLayer()).toBe(2);
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
        await graphStore.saveNode({ nodeId: id, maxLayer: 0, connections: [[]] });
        const loaded = await graphStore.loadNode(id);
        expect(loaded).not.toBeNull();
        expect(loaded!.nodeId).toBe(id);
      }
    });

    it('should handle many connections in a single layer', async () => {
      const connections = Array(100)
        .fill(0)
        .map((_, i) => `neighbor${i}`);

      await graphStore.saveNode({
        nodeId: 'hub',
        maxLayer: 0,
        connections: [connections],
      });

      const loaded = await graphStore.loadNode('hub');
      expect(loaded!.connections[0].length).toBe(100);
    });

    it('should handle deeply nested layer structure', async () => {
      const maxLayer = 10;
      const connections = Array(maxLayer + 1)
        .fill(0)
        .map((_, i) => (i < 5 ? ['other'] : []));

      await graphStore.saveNode({
        nodeId: 'deep',
        maxLayer,
        connections,
      });

      const loaded = await graphStore.loadNode('deep');
      expect(loaded!.maxLayer).toBe(maxLayer);
      expect(loaded!.connections.length).toBe(maxLayer + 1);
    });
  });
});
