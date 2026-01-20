/**
 * HNSW Construction Algorithm
 *
 * This module implements the HNSW insert algorithm as described in
 * "Efficient and robust approximate nearest neighbor search using
 * Hierarchical Navigable Small World graphs" by Malkov & Yashunin.
 */

import { HNSWGraph } from './graph.js';
import type { SearchCandidate, DistanceFunction, HNSWConfig } from './types.js';

/**
 * Generates a random level for a new node using geometric distribution.
 * The probability of a node being at level l is proportional to 1/M^l.
 *
 * @param mL - Level multiplier (typically 1/ln(M))
 * @returns The randomly generated level (0 or higher)
 */
export function randomLevel(mL: number): number {
  // Generate uniform random in (0, 1)
  const r = Math.random();

  // Convert to geometric distribution: floor(-ln(r) * mL)
  // This gives P(level = l) = (1 - exp(-1/mL)) * exp(-l/mL)
  return Math.floor(-Math.log(r) * mL);
}

/**
 * Simple neighbor selection: select the M nearest candidates.
 *
 * @param candidates - List of candidate nodes with distances
 * @param M - Maximum number of neighbors to select
 * @returns Selected neighbors sorted by distance
 */
export function selectNeighborsSimple(
  candidates: SearchCandidate[],
  M: number
): SearchCandidate[] {
  // Sort by distance (ascending)
  const sorted = [...candidates].sort((a, b) => a.distance - b.distance);

  // Return up to M nearest
  return sorted.slice(0, M);
}

/**
 * Heuristic neighbor selection for better graph connectivity.
 * This algorithm favors diverse neighbors over just the closest ones.
 *
 * @param candidates - List of candidate nodes with distances
 * @param M - Maximum number of neighbors to select
 * @param distanceFn - Distance function between nodes
 * @param _queryId - ID of the query node (for distance calculations)
 * @param extendCandidates - Whether to extend candidates with their neighbors
 * @returns Selected neighbors
 */
export function selectNeighborsHeuristic(
  candidates: SearchCandidate[],
  M: number,
  distanceFn: DistanceFunction,
  _queryId: string,
  extendCandidates: boolean = false
): SearchCandidate[] {
  if (candidates.length === 0) {
    return [];
  }

  // Sort candidates by distance
  const workingQueue = [...candidates].sort((a, b) => a.distance - b.distance);

  const result: SearchCandidate[] = [];

  // Greedy selection: pick candidates that improve diversity
  while (workingQueue.length > 0 && result.length < M) {
    const closest = workingQueue.shift()!;

    // Check if this candidate is closer to query than to any already selected neighbor
    let shouldAdd = true;

    if (extendCandidates) {
      for (const selected of result) {
        const distToSelected = distanceFn(closest.id, selected.id);
        if (distToSelected < closest.distance) {
          shouldAdd = false;
          break;
        }
      }
    }

    if (shouldAdd) {
      result.push(closest);
    }
  }

  // If we don't have enough, fill with remaining closest
  if (result.length < M) {
    const resultIds = new Set(result.map((r) => r.id));
    const remaining = candidates
      .filter((c) => !resultIds.has(c.id))
      .sort((a, b) => a.distance - b.distance);

    for (const candidate of remaining) {
      if (result.length >= M) break;
      result.push(candidate);
    }
  }

  return result.slice(0, M);
}

/**
 * Priority queue implementation for search candidates.
 * Implemented as a simple sorted array for clarity.
 */
class CandidateQueue {
  private items: SearchCandidate[] = [];
  private maxSize: number;

  constructor(maxSize: number = Infinity) {
    this.maxSize = maxSize;
  }

  push(candidate: SearchCandidate): void {
    // Binary search insert position
    let left = 0;
    let right = this.items.length;

    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      const midItem = this.items[mid];
      if (midItem && midItem.distance < candidate.distance) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }

    this.items.splice(left, 0, candidate);

    // Trim if exceeds max size
    if (this.items.length > this.maxSize) {
      this.items.pop();
    }
  }

  pop(): SearchCandidate | undefined {
    return this.items.shift();
  }

  peek(): SearchCandidate | undefined {
    return this.items[0];
  }

  peekLast(): SearchCandidate | undefined {
    return this.items[this.items.length - 1];
  }

  size(): number {
    return this.items.length;
  }

  isEmpty(): boolean {
    return this.items.length === 0;
  }

  toArray(): SearchCandidate[] {
    return [...this.items];
  }

  has(id: string): boolean {
    return this.items.some((item) => item.id === id);
  }
}

/**
 * HNSW Construction class that manages the insert operation.
 */
export class HNSWConstruction {
  private graph: HNSWGraph;
  private distanceFn: DistanceFunction;
  private config: HNSWConfig;

  constructor(graph: HNSWGraph, distanceFn: DistanceFunction) {
    this.graph = graph;
    this.distanceFn = distanceFn;
    this.config = graph.getConfig();
  }

  /**
   * Inserts a new node into the HNSW graph.
   *
   * @param nodeId - Unique identifier for the new node
   * @returns true if inserted, false if node already exists
   */
  insert(nodeId: string): boolean {
    // Check if node already exists
    if (this.graph.hasNode(nodeId)) {
      return false;
    }

    // Generate random level for the new node
    const nodeLevel = randomLevel(this.config.mL);

    // Capture current max level BEFORE adding the node
    // (addNode updates maxLevel internally, which would break our comparison)
    const currentMaxLevel = this.graph.getMaxLevel();

    // Handle first node case
    const entryPoint = this.graph.getEntryPoint();
    if (entryPoint === null) {
      // Add node to graph
      this.graph.addNode(nodeId, nodeLevel);
      this.graph.setEntryPoint(nodeId);
      this.graph.setMaxLevel(nodeLevel);
      return true;
    }

    // Add node to graph
    this.graph.addNode(nodeId, nodeLevel);

    // Current entry point and max level
    let currentNode = entryPoint;
    const maxLevel = currentMaxLevel;

    // Phase 1: Greedy search from top layer to nodeLevel + 1
    // Find closest node at each layer
    for (let layer = maxLevel; layer > nodeLevel; layer--) {
      currentNode = this.searchLayerGreedy(nodeId, currentNode, layer);
    }

    // Phase 2: Search and connect at layers nodeLevel down to 0
    for (let layer = Math.min(nodeLevel, maxLevel); layer >= 0; layer--) {
      // Search for efConstruction nearest neighbors at this layer
      const neighbors = this.searchLayer(nodeId, currentNode, this.config.efConstruction, layer);

      // Select M or M0 neighbors
      const M = layer === 0 ? this.config.M0 : this.config.M;
      const selectedNeighbors = selectNeighborsSimple(neighbors, M);

      // Connect new node to selected neighbors
      for (const neighbor of selectedNeighbors) {
        this.graph.addConnection(nodeId, neighbor.id, layer);
      }

      // Shrink connections for neighbors if they exceed M limit
      for (const neighbor of selectedNeighbors) {
        this.shrinkConnections(neighbor.id, layer);
      }

      // Update current node for next layer
      const firstNeighbor = selectedNeighbors[0];
      if (firstNeighbor) {
        currentNode = firstNeighbor.id;
      }
    }

    // Update entry point if new node has higher level
    if (nodeLevel > maxLevel) {
      this.graph.setEntryPoint(nodeId);
      this.graph.setMaxLevel(nodeLevel);
    }

    return true;
  }

  /**
   * Greedy search to find the closest node at a given layer.
   */
  private searchLayerGreedy(queryId: string, entryId: string, layer: number): string {
    let currentId = entryId;
    let currentDist = this.distanceFn(queryId, currentId);
    let changed = true;

    while (changed) {
      changed = false;
      const neighbors = this.graph.getNeighbors(currentId, layer);

      for (const neighborId of neighbors) {
        const dist = this.distanceFn(queryId, neighborId);
        if (dist < currentDist) {
          currentDist = dist;
          currentId = neighborId;
          changed = true;
        }
      }
    }

    return currentId;
  }

  /**
   * Search for ef nearest neighbors at a given layer starting from entry point.
   */
  private searchLayer(
    queryId: string,
    entryId: string,
    ef: number,
    layer: number
  ): SearchCandidate[] {
    const visited = new Set<string>([entryId]);
    const candidates = new CandidateQueue();
    const results = new CandidateQueue(ef);

    const entryDist = this.distanceFn(queryId, entryId);
    candidates.push({ id: entryId, distance: entryDist });
    results.push({ id: entryId, distance: entryDist });

    while (!candidates.isEmpty()) {
      const closest = candidates.pop()!;

      // Stop if closest candidate is farther than furthest result
      const furthestResult = results.peekLast();
      if (furthestResult && closest.distance > furthestResult.distance) {
        break;
      }

      // Explore neighbors
      const neighbors = this.graph.getNeighbors(closest.id, layer);

      for (const neighborId of neighbors) {
        if (!visited.has(neighborId)) {
          visited.add(neighborId);

          const dist = this.distanceFn(queryId, neighborId);
          const furthest = results.peekLast();

          // Add to candidates and results if closer than furthest result
          if (!furthest || dist < furthest.distance || results.size() < ef) {
            candidates.push({ id: neighborId, distance: dist });
            results.push({ id: neighborId, distance: dist });
          }
        }
      }
    }

    return results.toArray();
  }

  /**
   * Shrink connections of a node if they exceed the M limit.
   */
  private shrinkConnections(nodeId: string, layer: number): void {
    const connections = this.graph.getConnections(nodeId, layer);
    if (!connections) return;

    const M = layer === 0 ? this.config.M0 : this.config.M;

    if (connections.size <= M) return;

    // Convert connections to candidates with distances
    const candidates: SearchCandidate[] = [];
    for (const neighborId of connections) {
      candidates.push({
        id: neighborId,
        distance: this.distanceFn(nodeId, neighborId),
      });
    }

    // Select best M neighbors
    const selected = selectNeighborsSimple(candidates, M);
    const selectedIds = new Set(selected.map((s) => s.id));

    // Update connections
    this.graph.setConnections(nodeId, layer, selectedIds);
  }
}
