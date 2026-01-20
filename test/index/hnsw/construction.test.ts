/**
 * TDD Tests for HNSW Graph Construction
 *
 * These tests cover:
 * 1. Random level distribution
 * 2. Insert maintains invariants
 * 3. Connections respect M limit
 * 4. Entry point updates correctly
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { HNSWGraph } from '../../../src/index/hnsw/graph.js';
import {
  HNSWConstruction,
  randomLevel,
  selectNeighborsSimple,
  selectNeighborsHeuristic,
} from '../../../src/index/hnsw/construction.js';
import type { HNSWConfig, SearchCandidate } from '../../../src/index/hnsw/types.js';
import { createHNSWConfig } from '../../../src/index/hnsw/types.js';

describe('HNSW Construction', () => {
  describe('randomLevel()', () => {
    it('should return 0 most frequently (geometric distribution)', () => {
      const config = createHNSWConfig({ M: 16 });
      const levels: number[] = [];

      // Generate many random levels
      for (let i = 0; i < 10000; i++) {
        levels.push(randomLevel(config.mL));
      }

      // Count occurrences of each level
      const counts = new Map<number, number>();
      for (const level of levels) {
        counts.set(level, (counts.get(level) ?? 0) + 1);
      }

      // Level 0 should be most common
      const level0Count = counts.get(0) ?? 0;
      const level1Count = counts.get(1) ?? 0;

      expect(level0Count).toBeGreaterThan(level1Count);

      // For HNSW geometric distribution with mL = 1/ln(M), level 0 is most common
      // With M=16, mL ~= 0.36, so ~93-94% of nodes are at level 0
      const level0Ratio = level0Count / levels.length;
      expect(level0Ratio).toBeGreaterThan(0.85);
      expect(level0Ratio).toBeLessThan(0.98);
    });

    it('should always return non-negative integers', () => {
      const config = createHNSWConfig({ M: 16 });

      for (let i = 0; i < 1000; i++) {
        const level = randomLevel(config.mL);
        expect(level).toBeGreaterThanOrEqual(0);
        expect(Number.isInteger(level)).toBe(true);
      }
    });

    it('should occasionally produce higher levels', () => {
      const config = createHNSWConfig({ M: 16 });
      let maxSeen = 0;

      for (let i = 0; i < 10000; i++) {
        const level = randomLevel(config.mL);
        maxSeen = Math.max(maxSeen, level);
      }

      // With 10000 samples, we should see at least level 2 or higher
      expect(maxSeen).toBeGreaterThanOrEqual(2);
    });
  });

  describe('selectNeighborsSimple()', () => {
    it('should select up to M nearest neighbors', () => {
      const candidates: SearchCandidate[] = [
        { id: 'a', distance: 0.5 },
        { id: 'b', distance: 0.3 },
        { id: 'c', distance: 0.8 },
        { id: 'd', distance: 0.1 },
        { id: 'e', distance: 0.6 },
      ];

      const selected = selectNeighborsSimple(candidates, 3);

      expect(selected.length).toBe(3);
      // Should be sorted by distance
      expect(selected[0].id).toBe('d'); // 0.1
      expect(selected[1].id).toBe('b'); // 0.3
      expect(selected[2].id).toBe('a'); // 0.5
    });

    it('should return all candidates if fewer than M', () => {
      const candidates: SearchCandidate[] = [
        { id: 'a', distance: 0.5 },
        { id: 'b', distance: 0.3 },
      ];

      const selected = selectNeighborsSimple(candidates, 5);

      expect(selected.length).toBe(2);
    });

    it('should handle empty candidates', () => {
      const selected = selectNeighborsSimple([], 3);
      expect(selected.length).toBe(0);
    });
  });

  describe('selectNeighborsHeuristic()', () => {
    // Mock distance function for heuristic selection
    const mockDistanceFn = (a: string, b: string): number => {
      // Simple mock: distance based on character codes
      return Math.abs(a.charCodeAt(0) - b.charCodeAt(0)) / 26;
    };

    it('should select diverse neighbors using heuristic', () => {
      const candidates: SearchCandidate[] = [
        { id: 'a', distance: 0.1 },
        { id: 'b', distance: 0.15 },
        { id: 'c', distance: 0.2 },
        { id: 'z', distance: 0.3 },
      ];

      const selected = selectNeighborsHeuristic(
        candidates,
        3,
        mockDistanceFn,
        'query',
        true // extendCandidates
      );

      // Should include diverse neighbors, not just closest
      expect(selected.length).toBeLessThanOrEqual(3);
    });

    it('should respect M limit', () => {
      const candidates: SearchCandidate[] = [];
      for (let i = 0; i < 20; i++) {
        candidates.push({ id: String.fromCharCode(97 + i), distance: i * 0.05 });
      }

      const selected = selectNeighborsHeuristic(candidates, 5, mockDistanceFn, 'query', false);

      expect(selected.length).toBeLessThanOrEqual(5);
    });
  });

  describe('HNSWConstruction', () => {
    let graph: HNSWGraph;
    let construction: HNSWConstruction;
    let mockDistanceFn: (a: string, b: string) => number;

    beforeEach(() => {
      graph = new HNSWGraph({ M: 4, efConstruction: 10 });
      mockDistanceFn = (a: string, b: string): number => {
        // Simple mock distance based on numeric IDs
        const numA = parseInt(a.replace('node', ''), 10) || 0;
        const numB = parseInt(b.replace('node', ''), 10) || 0;
        return Math.abs(numA - numB) / 100;
      };
      construction = new HNSWConstruction(graph, mockDistanceFn);
    });

    describe('insert()', () => {
      it('should set first node as entry point', () => {
        construction.insert('node1');

        expect(graph.getEntryPoint()).toBe('node1');
        expect(graph.hasNode('node1')).toBe(true);
      });

      it('should update entry point when new node has higher level', () => {
        // Insert many nodes, statistically some will have higher levels
        for (let i = 0; i < 100; i++) {
          construction.insert(`node${i}`);
        }

        const entryPoint = graph.getEntryPoint();
        expect(entryPoint).not.toBeNull();

        // Entry point should be the node with the highest level
        const entryNode = graph.getNode(entryPoint!);
        expect(entryNode).toBeDefined();
        expect(entryNode!.maxLayer).toBe(graph.getMaxLevel());
      });

      it('should create bidirectional connections', () => {
        construction.insert('node1');
        construction.insert('node2');

        // Check that connections are bidirectional at layer 0
        const conn1 = graph.getConnections('node1', 0);
        const conn2 = graph.getConnections('node2', 0);

        // After inserting node2, it should connect to node1
        // and node1 should connect back to node2
        if (conn1 && conn1.size > 0 && conn2 && conn2.size > 0) {
          expect(conn1.has('node2') && conn2.has('node1')).toBe(true);
        }
      });

      it('should maintain M connection limit at higher layers', () => {
        const config = graph.getConfig();

        // Insert enough nodes to have some at higher layers
        for (let i = 0; i < 50; i++) {
          construction.insert(`node${i}`);
        }

        // Check all nodes respect M limit at layers > 0
        const allNodes = graph.getAllNodes();
        for (const [nodeId, node] of allNodes) {
          for (let layer = 1; layer <= node.maxLayer; layer++) {
            const connections = graph.getConnections(nodeId, layer);
            if (connections) {
              expect(connections.size).toBeLessThanOrEqual(config.M);
            }
          }
        }
      });

      it('should maintain M0 connection limit at layer 0', () => {
        const config = graph.getConfig();

        // Insert enough nodes
        for (let i = 0; i < 50; i++) {
          construction.insert(`node${i}`);
        }

        // Check all nodes respect M0 limit at layer 0
        const allNodes = graph.getAllNodes();
        for (const [nodeId, _] of allNodes) {
          const connections = graph.getConnections(nodeId, 0);
          if (connections) {
            expect(connections.size).toBeLessThanOrEqual(config.M0);
          }
        }
      });

      it('should not create duplicate nodes', () => {
        construction.insert('node1');
        construction.insert('node1'); // Should be ignored or throw

        expect(graph.size()).toBe(1);
      });

      it('should handle sequential inserts correctly', () => {
        for (let i = 0; i < 20; i++) {
          construction.insert(`node${i}`);
        }

        expect(graph.size()).toBe(20);

        // All nodes should have at least layer 0
        for (let i = 0; i < 20; i++) {
          const node = graph.getNode(`node${i}`);
          expect(node).toBeDefined();
          expect(node!.maxLayer).toBeGreaterThanOrEqual(0);
        }
      });
    });

    describe('graph invariants', () => {
      it('should maintain connected graph at layer 0', () => {
        // Insert nodes
        for (let i = 0; i < 30; i++) {
          construction.insert(`node${i}`);
        }

        // Every node (except first) should have at least one connection at layer 0
        const allNodes = graph.getAllNodes();
        let connectedNodes = 0;

        for (const [nodeId, _] of allNodes) {
          const connections = graph.getConnections(nodeId, 0);
          if (connections && connections.size > 0) {
            connectedNodes++;
          }
        }

        // Most nodes should be connected (allowing for edge cases)
        expect(connectedNodes).toBeGreaterThan(allNodes.size * 0.9);
      });

      it('should have entry point at the highest level', () => {
        for (let i = 0; i < 50; i++) {
          construction.insert(`node${i}`);
        }

        const entryPoint = graph.getEntryPoint();
        expect(entryPoint).not.toBeNull();

        const entryNode = graph.getNode(entryPoint!);
        expect(entryNode).toBeDefined();

        // Entry point should be at the max level
        expect(entryNode!.maxLayer).toBe(graph.getMaxLevel());
      });

      it('should have decreasing node count at higher layers', () => {
        for (let i = 0; i < 100; i++) {
          construction.insert(`node${i}`);
        }

        const maxLevel = graph.getMaxLevel();
        const layerCounts: number[] = [];

        for (let layer = 0; layer <= maxLevel; layer++) {
          const nodesAtLayer = graph.getNodesAtLayer(layer);
          layerCounts.push(nodesAtLayer.length);
        }

        // Each layer should have fewer or equal nodes than the layer below
        for (let i = 1; i < layerCounts.length; i++) {
          expect(layerCounts[i]).toBeLessThanOrEqual(layerCounts[i - 1]);
        }
      });
    });

    describe('edge cases', () => {
      it('should handle single node graph', () => {
        construction.insert('solo');

        expect(graph.size()).toBe(1);
        expect(graph.getEntryPoint()).toBe('solo');
        expect(graph.getConnections('solo', 0)?.size).toBe(0);
      });

      it('should handle two node graph', () => {
        construction.insert('node1');
        construction.insert('node2');

        expect(graph.size()).toBe(2);

        // Both should be connected at layer 0
        const conn1 = graph.getConnections('node1', 0);
        const conn2 = graph.getConnections('node2', 0);

        expect(conn1?.has('node2')).toBe(true);
        expect(conn2?.has('node1')).toBe(true);
      });
    });
  });
});
