/**
 * HNSW Storage Abstractions
 *
 * Defines interfaces for separating HNSW graph structure (DO SQLite)
 * from vector data (R2). This hybrid approach:
 *
 * - Graph in SQLite: Fast neighbor traversal, transactional updates
 * - Vectors in R2: Efficient bulk storage, batch fetching
 *
 * @packageDocumentation
 */

// ============================================================================
// TYPES
// ============================================================================

/**
 * Represents a node in the HNSW graph.
 * Each node has a maximum layer it appears in and connections at each layer.
 */
export interface HNSWNode {
  /**
   * Unique identifier for this node (typically entity ID)
   */
  nodeId: string;

  /**
   * Maximum layer this node appears in (0 = base layer only)
   */
  maxLayer: number;

  /**
   * Connections at each layer. Index = layer number.
   * connections[0] = neighbors at layer 0 (most connections)
   * connections[maxLayer] = neighbors at top layer (fewest connections)
   */
  connections: string[][];
}

/**
 * Configuration for HNSW index construction
 */
export interface HNSWConfig {
  /**
   * Maximum number of connections per node at each layer (M parameter)
   * Default: 16
   */
  maxConnections: number;

  /**
   * Maximum connections at layer 0 (typically 2*M)
   * Default: 32
   */
  maxConnectionsLayer0: number;

  /**
   * Number of candidates to consider during construction (ef_construction)
   * Default: 100
   */
  efConstruction: number;

  /**
   * Level generation multiplier (ml parameter)
   * Default: 1/ln(M)
   */
  levelMultiplier: number;
}

/**
 * Default HNSW configuration
 */
export const DEFAULT_HNSW_CONFIG: HNSWConfig = {
  maxConnections: 16,
  maxConnectionsLayer0: 32,
  efConstruction: 100,
  levelMultiplier: 1 / Math.log(16),
};

// ============================================================================
// VECTOR STORE INTERFACE
// ============================================================================

/**
 * Storage abstraction for vector data.
 * Implementations can use R2, in-memory, or other backends.
 */
export interface VectorStore {
  /**
   * Save a vector for a node
   *
   * @param nodeId - The node identifier
   * @param vector - The vector data (Float32 values)
   */
  saveVector(nodeId: string, vector: number[]): Promise<void>;

  /**
   * Load a single vector by node ID
   *
   * @param nodeId - The node identifier
   * @returns The vector or null if not found
   */
  loadVector(nodeId: string): Promise<number[] | null>;

  /**
   * Batch load vectors for multiple nodes
   *
   * More efficient than multiple loadVector calls for search operations.
   *
   * @param nodeIds - Array of node identifiers
   * @returns Map of nodeId -> vector for found nodes
   */
  loadVectors(nodeIds: string[]): Promise<Map<string, number[]>>;

  /**
   * Delete a vector by node ID
   *
   * @param nodeId - The node identifier
   */
  deleteVector(nodeId: string): Promise<void>;

  /**
   * Get the number of stored vectors
   */
  count(): Promise<number>;
}

// ============================================================================
// GRAPH STORE INTERFACE
// ============================================================================

/**
 * Storage abstraction for HNSW graph structure.
 * Stores nodes, edges, and entry point metadata.
 */
export interface GraphStore {
  /**
   * Save a node to the graph
   *
   * @param node - The HNSW node to save
   */
  saveNode(node: HNSWNode): Promise<void>;

  /**
   * Load a node by ID
   *
   * @param nodeId - The node identifier
   * @returns The node or null if not found
   */
  loadNode(nodeId: string): Promise<HNSWNode | null>;

  /**
   * Load all nodes from the graph
   *
   * Used for full index reconstruction or debugging.
   *
   * @returns Array of all nodes
   */
  loadAllNodes(): Promise<HNSWNode[]>;

  /**
   * Save the entry point node ID
   *
   * The entry point is the node at the highest layer where search begins.
   *
   * @param nodeId - The entry point node ID, or null to clear
   */
  saveEntryPoint(nodeId: string | null): Promise<void>;

  /**
   * Load the current entry point node ID
   *
   * @returns The entry point node ID or null if index is empty
   */
  loadEntryPoint(): Promise<string | null>;

  /**
   * Delete a node from the graph
   *
   * Also removes any edges pointing to this node.
   *
   * @param nodeId - The node identifier
   */
  deleteNode(nodeId: string): Promise<void>;

  /**
   * Get the number of nodes in the graph
   */
  nodeCount(): Promise<number>;

  /**
   * Get the current maximum layer in the graph
   *
   * @returns The highest layer number, or -1 if empty
   */
  maxLayer(): Promise<number>;
}

// ============================================================================
// COMBINED HNSW STORE
// ============================================================================

/**
 * Combined HNSW store that coordinates graph and vector storage
 */
export interface HNSWStore {
  /**
   * The underlying graph store
   */
  readonly graphStore: GraphStore;

  /**
   * The underlying vector store
   */
  readonly vectorStore: VectorStore;

  /**
   * HNSW configuration
   */
  readonly config: HNSWConfig;

  /**
   * Insert a new vector into the index
   *
   * @param nodeId - Unique identifier for this vector
   * @param vector - The vector data
   */
  insert(nodeId: string, vector: number[]): Promise<void>;

  /**
   * Search for k nearest neighbors
   *
   * @param query - The query vector
   * @param k - Number of neighbors to return
   * @param ef - Search beam width (higher = more accurate but slower)
   * @returns Array of {nodeId, distance} sorted by distance ascending
   */
  search(query: number[], k: number, ef?: number): Promise<Array<{ nodeId: string; distance: number }>>;

  /**
   * Delete a vector from the index
   *
   * @param nodeId - The node to delete
   */
  delete(nodeId: string): Promise<void>;

  /**
   * Get index statistics
   */
  stats(): Promise<{
    nodeCount: number;
    vectorCount: number;
    maxLayer: number;
    entryPoint: string | null;
  }>;
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Calculate cosine distance between two vectors.
 * Distance = 1 - cosine_similarity (range: [0, 2])
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Cosine distance (0 = identical, 2 = opposite)
 */
export function cosineDistance(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i]! * b[i]!;
    normA += a[i]! * a[i]!;
    normB += b[i]! * b[i]!;
  }

  if (normA === 0 || normB === 0) {
    return 1; // Treat zero vectors as maximally distant
  }

  const similarity = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  return 1 - similarity;
}

/**
 * Calculate Euclidean (L2) distance between two vectors.
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Euclidean distance (>= 0)
 */
export function euclideanDistance(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i]! - b[i]!;
    sum += diff * diff;
  }

  return Math.sqrt(sum);
}

/**
 * Generate a random layer for a new node based on the level multiplier.
 *
 * @param ml - Level multiplier (typically 1/ln(M))
 * @returns Random layer number (0 = base layer)
 */
export function randomLevel(ml: number): number {
  // P(level = L) = (1/e)^L * (1 - 1/e)
  // Equivalent to: floor(-ln(uniform(0,1)) * ml)
  return Math.floor(-Math.log(Math.random()) * ml);
}

/**
 * Convert Float32Array to number[]
 */
export function float32ToArray(float32: Float32Array): number[] {
  return Array.from(float32);
}

/**
 * Convert number[] to Float32Array
 */
export function arrayToFloat32(arr: number[]): Float32Array {
  return new Float32Array(arr);
}
