/**
 * HNSW Search Algorithm
 *
 * Pure TypeScript implementation of the Hierarchical Navigable Small World
 * (HNSW) search algorithm for approximate nearest neighbor search.
 *
 * The HNSW algorithm:
 * 1. Start at entry point on highest layer
 * 2. Greedy search down to layer 1 (single best at each layer)
 * 3. At layer 0, beam search with ef candidates
 * 4. Return top k results
 *
 * @see https://arxiv.org/abs/1603.09320 - Original HNSW paper
 * @packageDocumentation
 */

import { cosineDistance, type DistanceFunction } from './distance.js';

// ============================================================================
// TYPES
// ============================================================================

/**
 * Result of a vector similarity search.
 */
export interface SearchResult {
  /** Unique identifier of the found node */
  nodeId: string;
  /** Distance from query vector (lower = more similar) */
  distance: number;
}

/**
 * HNSW graph structure for search operations.
 *
 * The graph is organized in layers where:
 * - Layer 0 (base) contains all nodes with dense connections
 * - Higher layers contain progressively fewer nodes with sparser connections
 * - Search starts at the highest layer and descends to layer 0
 */
export interface HNSWGraph {
  /** Entry point node ID at the highest layer (null if empty) */
  entryPoint: string | null;
  /** Maximum layer in the graph (0 = only base layer) */
  maxLayer: number;
  /** Layers array: layers[i] = Map<nodeId, neighborIds[]> */
  layers: Map<string, string[]>[];
  /** Total number of nodes in the graph */
  nodeCount: number;
}

// ============================================================================
// INTERNAL TYPES
// ============================================================================

/**
 * Candidate node during search with its distance
 */
interface Candidate {
  nodeId: string;
  distance: number;
}

// ============================================================================
// SEARCH LAYER
// ============================================================================

/**
 * Search a single layer of the HNSW graph using beam search.
 *
 * This implements the search-layer algorithm from the HNSW paper:
 * - Maintains a dynamic list of candidates
 * - Expands candidates in order of increasing distance
 * - Tracks visited nodes to avoid cycles
 * - Returns ef best candidates found
 *
 * @param graph - The HNSW graph structure
 * @param getVector - Function to retrieve vector for a node ID
 * @param queryVector - The query vector to search for
 * @param entryPoints - Initial entry points for search
 * @param ef - Number of candidates to track (beam width)
 * @param layer - Which layer to search
 * @param distanceFn - Distance function to use
 * @returns Array of SearchResults sorted by distance (ascending)
 */
export function searchLayer(
  graph: HNSWGraph,
  getVector: (id: string) => number[],
  queryVector: number[],
  entryPoints: string[],
  ef: number,
  layer: number,
  distanceFn: DistanceFunction
): SearchResult[] {
  if (entryPoints.length === 0) {
    return [];
  }

  const layerGraph = graph.layers[layer];
  if (!layerGraph) {
    return [];
  }

  // Initialize visited set and candidate structures
  const visited = new Set<string>();

  // Candidates to explore - priority queue (min-heap by distance)
  const candidates: Candidate[] = [];

  // Best results found so far - priority queue (max-heap by distance)
  const results: Candidate[] = [];

  // Initialize with entry points
  for (const ep of entryPoints) {
    if (visited.has(ep)) continue;
    visited.add(ep);

    const vec = getVector(ep);
    const dist = distanceFn(queryVector, vec);

    candidates.push({ nodeId: ep, distance: dist });
    results.push({ nodeId: ep, distance: dist });
  }

  // Sort candidates by distance (ascending - closest first)
  candidates.sort((a, b) => a.distance - b.distance);

  // Process candidates
  while (candidates.length > 0) {
    // Get closest candidate
    const current = candidates.shift()!;

    // Get furthest result distance for pruning
    results.sort((a, b) => b.distance - a.distance); // Sort descending
    const furthestResultDist = results.length > 0 ? results[0]!.distance : Infinity;

    // If current candidate is further than worst result and we have ef results, stop
    if (current.distance > furthestResultDist && results.length >= ef) {
      break;
    }

    // Get neighbors in this layer
    const neighbors = layerGraph.get(current.nodeId) || [];

    for (const neighborId of neighbors) {
      if (visited.has(neighborId)) continue;
      visited.add(neighborId);

      const neighborVec = getVector(neighborId);
      const neighborDist = distanceFn(queryVector, neighborVec);

      // Update furthest result distance
      results.sort((a, b) => b.distance - a.distance);
      const currentFurthest = results.length > 0 ? results[0]!.distance : Infinity;

      // Add to results if better than worst or we don't have ef yet
      if (neighborDist < currentFurthest || results.length < ef) {
        candidates.push({ nodeId: neighborId, distance: neighborDist });
        results.push({ nodeId: neighborId, distance: neighborDist });

        // Trim results to ef
        if (results.length > ef) {
          results.sort((a, b) => a.distance - b.distance);
          results.pop();
        }
      }
    }

    // Re-sort candidates
    candidates.sort((a, b) => a.distance - b.distance);
  }

  // Return results sorted by distance ascending
  results.sort((a, b) => a.distance - b.distance);

  return results.map(c => ({
    nodeId: c.nodeId,
    distance: c.distance,
  }));
}

// ============================================================================
// MAIN SEARCH
// ============================================================================

/**
 * Search the HNSW graph for k nearest neighbors.
 *
 * Algorithm:
 * 1. Start at entry point on highest layer
 * 2. Greedy search down to layer 1 (keep single best at each layer)
 * 3. At layer 0, beam search with ef candidates
 * 4. Return top k results sorted by distance
 *
 * @param graph - The HNSW graph structure
 * @param getVector - Function to retrieve vector for a node ID
 * @param queryVector - The query vector to search for
 * @param k - Number of nearest neighbors to return
 * @param ef - Search beam width (higher = more accurate but slower). Default: max(k, 10)
 * @param distanceFn - Distance function to use. Default: cosineDistance
 * @returns Array of k nearest SearchResults sorted by distance (ascending)
 */
export function search(
  graph: HNSWGraph,
  getVector: (id: string) => number[],
  queryVector: number[],
  k: number,
  ef?: number,
  distanceFn?: DistanceFunction
): SearchResult[] {
  // Handle empty graph
  if (graph.entryPoint === null || graph.nodeCount === 0) {
    return [];
  }

  // Default parameters
  const searchEf = ef ?? Math.max(k, 10);
  const distance = distanceFn ?? cosineDistance;

  // Start at entry point
  let currentBest: string[] = [graph.entryPoint];

  // Greedy search from top layer down to layer 1
  // At each layer, find the single best entry point for the next layer
  for (let layer = graph.maxLayer; layer >= 1; layer--) {
    const layerResults = searchLayer(
      graph,
      getVector,
      queryVector,
      currentBest,
      1, // ef=1 for greedy search at upper layers
      layer,
      distance
    );

    if (layerResults.length > 0) {
      currentBest = [layerResults[0]!.nodeId];
    }
  }

  // Beam search at layer 0 with full ef
  const layer0Results = searchLayer(
    graph,
    getVector,
    queryVector,
    currentBest,
    searchEf,
    0,
    distance
  );

  // Return top k results
  return layer0Results.slice(0, Math.min(k, layer0Results.length));
}
