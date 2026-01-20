/**
 * SQLite Graph Store - DO SQLite-backed HNSW Graph Storage
 *
 * Stores HNSW graph structure in Durable Object SQLite:
 * - hnsw_meta: Key-value metadata (entry point, config)
 * - hnsw_nodes: Node ID to max layer mapping
 * - hnsw_edges: Connections per node per layer
 *
 * This separation from vector storage enables:
 * - Fast neighbor traversal (SQLite joins/indexes)
 * - Transactional graph updates
 * - Hibernation-friendly storage
 *
 * @packageDocumentation
 */

import type { GraphStore, HNSWNode } from './store.js';

// ============================================================================
// SQL SCHEMA
// ============================================================================

/**
 * SQL schema for HNSW graph tables
 *
 * Tables:
 * - hnsw_meta: Stores metadata like entry point
 * - hnsw_nodes: Stores node ID and max layer
 * - hnsw_edges: Stores connections per layer (normalized)
 */
export const HNSW_GRAPH_SCHEMA = `
-- Metadata table (entry point, config, etc.)
CREATE TABLE IF NOT EXISTS hnsw_meta (
  key TEXT PRIMARY KEY,
  value TEXT
);

-- Node table: tracks which nodes exist and their max layer
CREATE TABLE IF NOT EXISTS hnsw_nodes (
  node_id TEXT PRIMARY KEY,
  max_layer INTEGER NOT NULL
);

-- Edge table: connections per node per layer (normalized)
CREATE TABLE IF NOT EXISTS hnsw_edges (
  node_id TEXT NOT NULL,
  layer INTEGER NOT NULL,
  connections TEXT NOT NULL,  -- JSON array of connected node IDs
  PRIMARY KEY (node_id, layer)
);

-- Index for loading all edges for a node
CREATE INDEX IF NOT EXISTS idx_hnsw_edges_node ON hnsw_edges(node_id);
`;

// ============================================================================
// SQLITE GRAPH STORE IMPLEMENTATION
// ============================================================================

/**
 * SQLite-backed HNSW graph storage
 *
 * Implements GraphStore interface for DO SQLite persistence.
 */
export class SQLiteGraphStore implements GraphStore {
  private sql: SqlStorage;
  private initialized: boolean = false;

  /**
   * Create a new SQLite graph store
   *
   * @param sql - DO SqlStorage instance
   */
  constructor(sql: SqlStorage) {
    this.sql = sql;
  }

  /**
   * Ensure schema is initialized
   */
  private ensureInitialized(): void {
    if (this.initialized) return;

    // Execute schema creation
    const statements = HNSW_GRAPH_SCHEMA.split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    for (const statement of statements) {
      try {
        this.sql.exec(statement);
      } catch {
        // Ignore errors for tables that already exist
      }
    }

    this.initialized = true;
  }

  /**
   * Save a node to the graph
   *
   * Stores node metadata and all layer connections.
   */
  async saveNode(node: HNSWNode): Promise<void> {
    this.ensureInitialized();

    // Insert or replace node metadata
    this.sql.exec(
      `INSERT OR REPLACE INTO hnsw_nodes (node_id, max_layer) VALUES (?, ?)`,
      node.nodeId,
      node.maxLayer
    );

    // Insert or replace edges for each layer
    for (let layer = 0; layer <= node.maxLayer; layer++) {
      const connections = node.connections[layer] || [];
      this.sql.exec(
        `INSERT OR REPLACE INTO hnsw_edges (node_id, layer, connections) VALUES (?, ?, ?)`,
        node.nodeId,
        layer,
        JSON.stringify(connections)
      );
    }
  }

  /**
   * Load a node by ID
   *
   * Reconstructs HNSWNode from metadata and edge tables.
   */
  async loadNode(nodeId: string): Promise<HNSWNode | null> {
    this.ensureInitialized();

    // Get node metadata
    const nodeRows = this.sql
      .exec(`SELECT * FROM hnsw_nodes WHERE node_id = ?`, nodeId)
      .toArray();

    if (nodeRows.length === 0) {
      return null;
    }

    const maxLayer = nodeRows[0]!['max_layer'] as number;

    // Get all edges for this node, ordered by layer
    const edgeRows = this.sql
      .exec(
        `SELECT connections FROM hnsw_edges WHERE node_id = ? ORDER BY layer ASC`,
        nodeId
      )
      .toArray();

    // Reconstruct connections array
    const connections: string[][] = [];
    for (let layer = 0; layer <= maxLayer; layer++) {
      if (layer < edgeRows.length) {
        connections.push(
          JSON.parse(edgeRows[layer]!['connections'] as string) as string[]
        );
      } else {
        connections.push([]);
      }
    }

    return {
      nodeId,
      maxLayer,
      connections,
    };
  }

  /**
   * Load all nodes from the graph
   *
   * Used for full index reconstruction or debugging.
   */
  async loadAllNodes(): Promise<HNSWNode[]> {
    this.ensureInitialized();

    const nodeRows = this.sql.exec(`SELECT * FROM hnsw_nodes`).toArray();
    const nodes: HNSWNode[] = [];

    for (const row of nodeRows) {
      const nodeId = row['node_id'] as string;
      const node = await this.loadNode(nodeId);
      if (node) {
        nodes.push(node);
      }
    }

    return nodes;
  }

  /**
   * Save the entry point node ID
   *
   * The entry point is the node at the highest layer where search begins.
   */
  async saveEntryPoint(nodeId: string | null): Promise<void> {
    this.ensureInitialized();

    if (nodeId === null) {
      // Clear entry point by storing empty string
      this.sql.exec(
        `INSERT OR REPLACE INTO hnsw_meta (key, value) VALUES (?, ?)`,
        'entry_point',
        ''
      );
    } else {
      this.sql.exec(
        `INSERT OR REPLACE INTO hnsw_meta (key, value) VALUES (?, ?)`,
        'entry_point',
        nodeId
      );
    }
  }

  /**
   * Load the current entry point node ID
   *
   * @returns The entry point node ID or null if index is empty
   */
  async loadEntryPoint(): Promise<string | null> {
    this.ensureInitialized();

    const rows = this.sql
      .exec(`SELECT value FROM hnsw_meta WHERE key = ?`, 'entry_point')
      .toArray();

    if (rows.length === 0) {
      return null;
    }

    const value = rows[0]!['value'] as string;
    return value === '' ? null : value;
  }

  /**
   * Delete a node from the graph
   *
   * Removes both node metadata and all edge data.
   * Note: This does NOT update connections in other nodes that reference this node.
   * The caller is responsible for updating those connections.
   */
  async deleteNode(nodeId: string): Promise<void> {
    this.ensureInitialized();

    // Delete edges first (foreign key style)
    this.sql.exec(`DELETE FROM hnsw_edges WHERE node_id = ?`, nodeId);

    // Delete node metadata
    this.sql.exec(`DELETE FROM hnsw_nodes WHERE node_id = ?`, nodeId);
  }

  /**
   * Get the number of nodes in the graph
   */
  async nodeCount(): Promise<number> {
    this.ensureInitialized();

    const rows = this.sql
      .exec(`SELECT COUNT(*) as cnt FROM hnsw_nodes`)
      .toArray();

    return rows[0]!['cnt'] as number;
  }

  /**
   * Get the current maximum layer in the graph
   *
   * @returns The highest layer number, or -1 if empty
   */
  async maxLayer(): Promise<number> {
    this.ensureInitialized();

    const rows = this.sql
      .exec(`SELECT MAX(max_layer) as max_layer FROM hnsw_nodes`)
      .toArray();

    const maxLayer = rows[0]!['max_layer'];
    return maxLayer === null ? -1 : (maxLayer as number);
  }
}
