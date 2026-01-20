/**
 * HNSW (Hierarchical Navigable Small World) Module
 *
 * This module provides a pure TypeScript implementation of the HNSW
 * graph construction algorithm for approximate nearest neighbor search.
 *
 * @example
 * ```typescript
 * import { HNSWGraph, HNSWConstruction, createHNSWConfig } from './hnsw';
 *
 * // Create a graph with custom config
 * const graph = new HNSWGraph({ M: 16, efConstruction: 200 });
 *
 * // Define a distance function for your vectors
 * const distanceFn = (a: string, b: string) => {
 *   // Return distance between vectors a and b
 *   return computeDistance(vectors.get(a), vectors.get(b));
 * };
 *
 * // Create construction instance
 * const construction = new HNSWConstruction(graph, distanceFn);
 *
 * // Insert nodes
 * construction.insert('node1');
 * construction.insert('node2');
 * construction.insert('node3');
 * ```
 */

// Types
export type {
  HNSWNode,
  HNSWConfig,
  SearchCandidate,
  DistanceFunction,
} from './types.js';

export {
  DEFAULT_HNSW_CONFIG,
  createHNSWConfig,
} from './types.js';

// Graph structure
export { HNSWGraph } from './graph.js';

// Construction algorithm
export {
  HNSWConstruction,
  randomLevel,
  selectNeighborsSimple,
  selectNeighborsHeuristic,
} from './construction.js';

// Storage abstractions
export type {
  VectorStore,
  GraphStore,
  HNSWStore,
} from './store.js';

export {
  DEFAULT_HNSW_CONFIG as DEFAULT_STORE_CONFIG,
  cosineDistance,
  euclideanDistance,
  float32ToArray,
  arrayToFloat32,
  randomLevel as randomLevelFromStore,
} from './store.js';

// Storage implementations
export { SQLiteGraphStore, HNSW_GRAPH_SCHEMA } from './sqlite-graph-store.js';
export { R2VectorStore, MemoryVectorStore } from './r2-vector-store.js';
