/**
 * HNSW Graph Structure
 *
 * This module implements the core graph data structure for HNSW.
 * The graph maintains nodes across multiple layers, with each layer
 * being a navigable small world graph.
 */

import type { HNSWNode, HNSWConfig } from './types.js';
import { createHNSWConfig } from './types.js';

/**
 * HNSWGraph manages the hierarchical graph structure for HNSW.
 *
 * The graph consists of multiple layers (0 to maxLevel), where:
 * - Layer 0 contains all nodes
 * - Higher layers contain exponentially fewer nodes
 * - Each layer forms a navigable small world graph
 */
export class HNSWGraph {
  /** Map of node ID to HNSWNode */
  private nodes: Map<string, HNSWNode>;

  /** Entry point node ID (the node with the highest layer) */
  private entryPoint: string | null;

  /** Maximum level currently in the graph */
  private maxLevel: number;

  /** Configuration parameters */
  private config: HNSWConfig;

  constructor(config: Partial<HNSWConfig> = {}) {
    this.nodes = new Map();
    this.entryPoint = null;
    this.maxLevel = -1;
    this.config = createHNSWConfig(config);
  }

  /**
   * Returns the current entry point node ID.
   */
  getEntryPoint(): string | null {
    return this.entryPoint;
  }

  /**
   * Sets the entry point node ID.
   */
  setEntryPoint(nodeId: string): void {
    this.entryPoint = nodeId;
  }

  /**
   * Returns the current maximum level in the graph.
   */
  getMaxLevel(): number {
    return this.maxLevel;
  }

  /**
   * Sets the maximum level in the graph.
   */
  setMaxLevel(level: number): void {
    this.maxLevel = level;
  }

  /**
   * Returns the configuration parameters.
   */
  getConfig(): HNSWConfig {
    return this.config;
  }

  /**
   * Returns the total number of nodes in the graph.
   */
  size(): number {
    return this.nodes.size;
  }

  /**
   * Checks if a node exists in the graph.
   */
  hasNode(nodeId: string): boolean {
    return this.nodes.has(nodeId);
  }

  /**
   * Adds a new node to the graph.
   *
   * @param id - Unique identifier for the node
   * @param maxLayer - The maximum layer this node will exist in
   * @returns The created HNSWNode
   * @throws Error if node with this ID already exists
   */
  addNode(id: string, maxLayer: number): HNSWNode {
    if (this.nodes.has(id)) {
      throw new Error(`Node with ID "${id}" already exists`);
    }

    const connections = new Map<number, Set<string>>();
    // Initialize empty connection sets for each layer
    for (let layer = 0; layer <= maxLayer; layer++) {
      connections.set(layer, new Set());
    }

    const node: HNSWNode = {
      id,
      maxLayer,
      connections,
    };

    this.nodes.set(id, node);

    // Update max level if necessary
    if (maxLayer > this.maxLevel) {
      this.maxLevel = maxLayer;
    }

    return node;
  }

  /**
   * Retrieves a node by its ID.
   *
   * @param nodeId - The ID of the node to retrieve
   * @returns The HNSWNode or undefined if not found
   */
  getNode(nodeId: string): HNSWNode | undefined {
    return this.nodes.get(nodeId);
  }

  /**
   * Gets the connections for a node at a specific layer.
   *
   * @param nodeId - The ID of the node
   * @param layer - The layer to get connections for
   * @returns Set of connected node IDs, or undefined if node/layer doesn't exist
   */
  getConnections(nodeId: string, layer: number): Set<string> | undefined {
    const node = this.nodes.get(nodeId);
    if (!node) {
      return undefined;
    }
    return node.connections.get(layer);
  }

  /**
   * Sets the connections for a node at a specific layer.
   *
   * @param nodeId - The ID of the node
   * @param layer - The layer to set connections for
   * @param connections - Set of node IDs to connect to
   * @throws Error if node doesn't exist or layer is invalid
   */
  setConnections(nodeId: string, layer: number, connections: Set<string>): void {
    const node = this.nodes.get(nodeId);
    if (!node) {
      throw new Error(`Node with ID "${nodeId}" does not exist`);
    }
    if (layer > node.maxLayer) {
      throw new Error(`Layer ${layer} exceeds node's maxLayer ${node.maxLayer}`);
    }
    node.connections.set(layer, connections);
  }

  /**
   * Adds a bidirectional connection between two nodes at a specific layer.
   *
   * @param nodeId1 - First node ID
   * @param nodeId2 - Second node ID
   * @param layer - The layer to add the connection at
   */
  addConnection(nodeId1: string, nodeId2: string, layer: number): void {
    const connections1 = this.getConnections(nodeId1, layer);
    const connections2 = this.getConnections(nodeId2, layer);

    if (connections1) {
      connections1.add(nodeId2);
    }
    if (connections2) {
      connections2.add(nodeId1);
    }
  }

  /**
   * Removes a bidirectional connection between two nodes at a specific layer.
   *
   * @param nodeId1 - First node ID
   * @param nodeId2 - Second node ID
   * @param layer - The layer to remove the connection at
   */
  removeConnection(nodeId1: string, nodeId2: string, layer: number): void {
    const connections1 = this.getConnections(nodeId1, layer);
    const connections2 = this.getConnections(nodeId2, layer);

    if (connections1) {
      connections1.delete(nodeId2);
    }
    if (connections2) {
      connections2.delete(nodeId1);
    }
  }

  /**
   * Gets all neighbors of a node at a specific layer.
   * This is an alias for getConnections for semantic clarity.
   *
   * @param nodeId - The ID of the node
   * @param layer - The layer to get neighbors for
   * @returns Array of neighbor node IDs
   */
  getNeighbors(nodeId: string, layer: number): string[] {
    const connections = this.getConnections(nodeId, layer);
    return connections ? Array.from(connections) : [];
  }

  /**
   * Returns all nodes in the graph.
   */
  getAllNodes(): Map<string, HNSWNode> {
    return new Map(this.nodes);
  }

  /**
   * Returns all node IDs that exist at a specific layer.
   *
   * @param layer - The layer to query
   * @returns Array of node IDs present at that layer
   */
  getNodesAtLayer(layer: number): string[] {
    const result: string[] = [];
    for (const [id, node] of this.nodes) {
      if (node.maxLayer >= layer) {
        result.push(id);
      }
    }
    return result;
  }

  /**
   * Clears all nodes from the graph.
   */
  clear(): void {
    this.nodes.clear();
    this.entryPoint = null;
    this.maxLevel = -1;
  }
}
