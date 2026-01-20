/**
 * HNSW Graph Structure Tests
 *
 * TDD tests for the core graph data structure of HNSW.
 * The graph maintains nodes across multiple layers, with each layer
 * being a navigable small world graph.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { HNSWGraph } from '../../../src/index/hnsw/graph.js';
import { createHNSWConfig, DEFAULT_HNSW_CONFIG } from '../../../src/index/hnsw/types.js';

describe('HNSWGraph', () => {
  let graph: HNSWGraph;

  beforeEach(() => {
    graph = new HNSWGraph();
  });

  // ============================================================================
  // CONSTRUCTOR AND CONFIGURATION
  // ============================================================================

  describe('constructor', () => {
    it('should create an empty graph', () => {
      expect(graph.size()).toBe(0);
      expect(graph.getEntryPoint()).toBeNull();
      expect(graph.getMaxLevel()).toBe(-1);
    });

    it('should use default config when none provided', () => {
      const config = graph.getConfig();
      expect(config.M).toBe(DEFAULT_HNSW_CONFIG.M);
      expect(config.M0).toBe(DEFAULT_HNSW_CONFIG.M0);
      expect(config.efConstruction).toBe(DEFAULT_HNSW_CONFIG.efConstruction);
    });

    it('should accept custom configuration', () => {
      const customGraph = new HNSWGraph({ M: 32, efConstruction: 300 });
      const config = customGraph.getConfig();
      expect(config.M).toBe(32);
      expect(config.M0).toBe(64); // M * 2
      expect(config.efConstruction).toBe(300);
    });

    it('should calculate mL based on M', () => {
      const customGraph = new HNSWGraph({ M: 8 });
      const config = customGraph.getConfig();
      expect(config.mL).toBeCloseTo(1 / Math.log(8), 10);
    });
  });

  // ============================================================================
  // ENTRY POINT MANAGEMENT
  // ============================================================================

  describe('entry point', () => {
    it('should initially have null entry point', () => {
      expect(graph.getEntryPoint()).toBeNull();
    });

    it('should allow setting entry point', () => {
      graph.addNode('node1', 0);
      graph.setEntryPoint('node1');
      expect(graph.getEntryPoint()).toBe('node1');
    });

    it('should allow updating entry point', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 1);
      graph.setEntryPoint('node1');
      graph.setEntryPoint('node2');
      expect(graph.getEntryPoint()).toBe('node2');
    });
  });

  // ============================================================================
  // MAX LEVEL MANAGEMENT
  // ============================================================================

  describe('max level', () => {
    it('should initially be -1', () => {
      expect(graph.getMaxLevel()).toBe(-1);
    });

    it('should update when adding nodes', () => {
      graph.addNode('node1', 2);
      expect(graph.getMaxLevel()).toBe(2);
    });

    it('should track the highest level', () => {
      graph.addNode('node1', 1);
      graph.addNode('node2', 3);
      graph.addNode('node3', 2);
      expect(graph.getMaxLevel()).toBe(3);
    });

    it('should allow manual setting', () => {
      graph.setMaxLevel(5);
      expect(graph.getMaxLevel()).toBe(5);
    });
  });

  // ============================================================================
  // NODE MANAGEMENT
  // ============================================================================

  describe('addNode', () => {
    it('should add a node with the given ID and maxLayer', () => {
      const node = graph.addNode('node1', 2);
      expect(node.id).toBe('node1');
      expect(node.maxLayer).toBe(2);
    });

    it('should initialize empty connections for each layer', () => {
      const node = graph.addNode('node1', 2);
      expect(node.connections.size).toBe(3); // layers 0, 1, 2
      expect(node.connections.get(0)?.size).toBe(0);
      expect(node.connections.get(1)?.size).toBe(0);
      expect(node.connections.get(2)?.size).toBe(0);
    });

    it('should throw when adding duplicate node ID', () => {
      graph.addNode('node1', 0);
      expect(() => graph.addNode('node1', 1)).toThrow(
        'Node with ID "node1" already exists'
      );
    });

    it('should increment graph size', () => {
      expect(graph.size()).toBe(0);
      graph.addNode('node1', 0);
      expect(graph.size()).toBe(1);
      graph.addNode('node2', 0);
      expect(graph.size()).toBe(2);
    });

    it('should update maxLevel if node is at higher layer', () => {
      graph.addNode('node1', 1);
      expect(graph.getMaxLevel()).toBe(1);
      graph.addNode('node2', 5);
      expect(graph.getMaxLevel()).toBe(5);
    });

    it('should not decrease maxLevel when adding lower-layer node', () => {
      graph.addNode('node1', 5);
      graph.addNode('node2', 2);
      expect(graph.getMaxLevel()).toBe(5);
    });
  });

  describe('getNode', () => {
    it('should return node by ID', () => {
      graph.addNode('node1', 2);
      const node = graph.getNode('node1');
      expect(node).toBeDefined();
      expect(node?.id).toBe('node1');
    });

    it('should return undefined for non-existent node', () => {
      expect(graph.getNode('nonexistent')).toBeUndefined();
    });
  });

  describe('hasNode', () => {
    it('should return true for existing node', () => {
      graph.addNode('node1', 0);
      expect(graph.hasNode('node1')).toBe(true);
    });

    it('should return false for non-existent node', () => {
      expect(graph.hasNode('nonexistent')).toBe(false);
    });
  });

  describe('getAllNodes', () => {
    it('should return empty map for empty graph', () => {
      const nodes = graph.getAllNodes();
      expect(nodes.size).toBe(0);
    });

    it('should return all nodes', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 1);
      graph.addNode('node3', 2);

      const nodes = graph.getAllNodes();
      expect(nodes.size).toBe(3);
      expect(nodes.has('node1')).toBe(true);
      expect(nodes.has('node2')).toBe(true);
      expect(nodes.has('node3')).toBe(true);
    });

    it('should return a copy (modifications do not affect graph)', () => {
      graph.addNode('node1', 0);
      const nodes = graph.getAllNodes();
      nodes.delete('node1');
      expect(graph.hasNode('node1')).toBe(true);
    });
  });

  describe('getNodesAtLayer', () => {
    it('should return empty array for empty graph', () => {
      expect(graph.getNodesAtLayer(0)).toEqual([]);
    });

    it('should return nodes that exist at the given layer', () => {
      graph.addNode('node1', 0); // exists at layer 0
      graph.addNode('node2', 2); // exists at layers 0, 1, 2
      graph.addNode('node3', 1); // exists at layers 0, 1

      const layer0Nodes = graph.getNodesAtLayer(0);
      expect(layer0Nodes.length).toBe(3);

      const layer1Nodes = graph.getNodesAtLayer(1);
      expect(layer1Nodes.length).toBe(2);
      expect(layer1Nodes).toContain('node2');
      expect(layer1Nodes).toContain('node3');

      const layer2Nodes = graph.getNodesAtLayer(2);
      expect(layer2Nodes.length).toBe(1);
      expect(layer2Nodes).toContain('node2');
    });

    it('should return empty for non-existent layer', () => {
      graph.addNode('node1', 1);
      expect(graph.getNodesAtLayer(5)).toEqual([]);
    });
  });

  // ============================================================================
  // CONNECTION MANAGEMENT
  // ============================================================================

  describe('getConnections', () => {
    it('should return connections for a node at a layer', () => {
      const node = graph.addNode('node1', 1);
      node.connections.get(0)?.add('node2');
      node.connections.get(0)?.add('node3');

      const connections = graph.getConnections('node1', 0);
      expect(connections?.size).toBe(2);
      expect(connections?.has('node2')).toBe(true);
      expect(connections?.has('node3')).toBe(true);
    });

    it('should return undefined for non-existent node', () => {
      expect(graph.getConnections('nonexistent', 0)).toBeUndefined();
    });

    it('should return undefined for non-existent layer', () => {
      graph.addNode('node1', 1);
      expect(graph.getConnections('node1', 5)).toBeUndefined();
    });
  });

  describe('setConnections', () => {
    it('should set connections for a node at a layer', () => {
      graph.addNode('node1', 1);
      const newConnections = new Set(['node2', 'node3', 'node4']);
      graph.setConnections('node1', 0, newConnections);

      const connections = graph.getConnections('node1', 0);
      expect(connections?.size).toBe(3);
      expect(connections?.has('node2')).toBe(true);
      expect(connections?.has('node3')).toBe(true);
      expect(connections?.has('node4')).toBe(true);
    });

    it('should throw for non-existent node', () => {
      expect(() =>
        graph.setConnections('nonexistent', 0, new Set())
      ).toThrow('Node with ID "nonexistent" does not exist');
    });

    it('should throw when layer exceeds maxLayer', () => {
      graph.addNode('node1', 1);
      expect(() =>
        graph.setConnections('node1', 5, new Set())
      ).toThrow("Layer 5 exceeds node's maxLayer 1");
    });

    it('should allow setting at exact maxLayer', () => {
      graph.addNode('node1', 2);
      graph.setConnections('node1', 2, new Set(['node2']));
      expect(graph.getConnections('node1', 2)?.has('node2')).toBe(true);
    });
  });

  describe('addConnection', () => {
    it('should add bidirectional connection', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 0);
      graph.addConnection('node1', 'node2', 0);

      expect(graph.getConnections('node1', 0)?.has('node2')).toBe(true);
      expect(graph.getConnections('node2', 0)?.has('node1')).toBe(true);
    });

    it('should handle connection when one node lacks the layer', () => {
      graph.addNode('node1', 2);
      graph.addNode('node2', 0); // only has layer 0

      // Adding connection at layer 1 - node2 doesn't have it
      graph.addConnection('node1', 'node2', 1);

      // node1 should have connection (it has layer 1)
      expect(graph.getConnections('node1', 1)?.has('node2')).toBe(true);
      // node2 doesn't have layer 1, so no reverse connection
      expect(graph.getConnections('node2', 1)).toBeUndefined();
    });

    it('should not create duplicate connections', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 0);
      graph.addConnection('node1', 'node2', 0);
      graph.addConnection('node1', 'node2', 0);

      expect(graph.getConnections('node1', 0)?.size).toBe(1);
    });
  });

  describe('removeConnection', () => {
    it('should remove bidirectional connection', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 0);
      graph.addConnection('node1', 'node2', 0);
      graph.removeConnection('node1', 'node2', 0);

      expect(graph.getConnections('node1', 0)?.has('node2')).toBe(false);
      expect(graph.getConnections('node2', 0)?.has('node1')).toBe(false);
    });

    it('should handle removing non-existent connection', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 0);
      // Should not throw
      graph.removeConnection('node1', 'node2', 0);
    });
  });

  describe('getNeighbors', () => {
    it('should return array of neighbor IDs', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 0);
      graph.addNode('node3', 0);
      graph.addConnection('node1', 'node2', 0);
      graph.addConnection('node1', 'node3', 0);

      const neighbors = graph.getNeighbors('node1', 0);
      expect(neighbors.length).toBe(2);
      expect(neighbors).toContain('node2');
      expect(neighbors).toContain('node3');
    });

    it('should return empty array for node with no connections', () => {
      graph.addNode('node1', 0);
      expect(graph.getNeighbors('node1', 0)).toEqual([]);
    });

    it('should return empty array for non-existent node', () => {
      expect(graph.getNeighbors('nonexistent', 0)).toEqual([]);
    });

    it('should return empty array for non-existent layer', () => {
      graph.addNode('node1', 1);
      expect(graph.getNeighbors('node1', 5)).toEqual([]);
    });
  });

  // ============================================================================
  // CLEAR OPERATION
  // ============================================================================

  describe('clear', () => {
    it('should remove all nodes', () => {
      graph.addNode('node1', 0);
      graph.addNode('node2', 1);
      graph.addNode('node3', 2);
      graph.setEntryPoint('node3');

      graph.clear();

      expect(graph.size()).toBe(0);
      expect(graph.getEntryPoint()).toBeNull();
      expect(graph.getMaxLevel()).toBe(-1);
    });

    it('should allow adding nodes after clear', () => {
      graph.addNode('node1', 0);
      graph.clear();
      graph.addNode('node2', 1);

      expect(graph.size()).toBe(1);
      expect(graph.hasNode('node2')).toBe(true);
    });
  });

  // ============================================================================
  // GRAPH INVARIANTS
  // ============================================================================

  describe('graph invariants', () => {
    it('should maintain size accurately after multiple operations', () => {
      for (let i = 0; i < 100; i++) {
        graph.addNode(`node${i}`, i % 5);
      }
      expect(graph.size()).toBe(100);
    });

    it('should maintain correct maxLevel after adding many nodes', () => {
      const maxLayers = [0, 3, 1, 5, 2, 4, 1, 0, 6, 2];
      for (let i = 0; i < maxLayers.length; i++) {
        graph.addNode(`node${i}`, maxLayers[i]!);
      }
      expect(graph.getMaxLevel()).toBe(6);
    });

    it('should handle nodes with many connections', () => {
      const hubGraph = new HNSWGraph({ M: 100 }); // allow many connections
      hubGraph.addNode('hub', 0);

      for (let i = 0; i < 50; i++) {
        hubGraph.addNode(`spoke${i}`, 0);
        hubGraph.addConnection('hub', `spoke${i}`, 0);
      }

      const neighbors = hubGraph.getNeighbors('hub', 0);
      expect(neighbors.length).toBe(50);
    });

    it('should handle deep layer hierarchy', () => {
      // Create a graph with 10 layers
      for (let i = 0; i < 10; i++) {
        graph.addNode(`node${i}`, i);
      }

      expect(graph.getMaxLevel()).toBe(9);
      expect(graph.getNodesAtLayer(0).length).toBe(10);
      expect(graph.getNodesAtLayer(5).length).toBe(5);
      expect(graph.getNodesAtLayer(9).length).toBe(1);
    });
  });

  // ============================================================================
  // EDGE CASES
  // ============================================================================

  describe('edge cases', () => {
    it('should handle empty string node ID', () => {
      const node = graph.addNode('', 0);
      expect(node.id).toBe('');
      expect(graph.hasNode('')).toBe(true);
    });

    it('should handle special characters in node ID', () => {
      const specialIds = [
        'node/with/slashes',
        'node:with:colons',
        'node with spaces',
        'unicode-\u00e9\u00e8\u00ea',
        'emoji-test',
      ];

      for (const id of specialIds) {
        graph.addNode(id, 0);
        expect(graph.hasNode(id)).toBe(true);
      }
    });

    it('should handle layer 0 nodes (base layer only)', () => {
      const node = graph.addNode('base', 0);
      expect(node.maxLayer).toBe(0);
      expect(node.connections.size).toBe(1);
      expect(node.connections.has(0)).toBe(true);
    });

    it('should handle very high layer numbers', () => {
      const node = graph.addNode('high', 100);
      expect(node.maxLayer).toBe(100);
      expect(node.connections.size).toBe(101);
      expect(graph.getMaxLevel()).toBe(100);
    });
  });
});
