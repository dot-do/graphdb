/**
 * HNSW Vector Search Accuracy Tests (TDD RED Phase)
 *
 * Tests for vector similarity search accuracy including:
 * - Recall@k metrics (what fraction of true nearest neighbors are found)
 * - Precision metrics
 * - Distance accuracy vs brute-force
 * - Effect of ef parameter on accuracy
 * - Edge cases in vector search
 *
 * @see src/index/hnsw/search.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import {
  cosineDistance,
  euclideanDistance,
} from '../../../src/index/hnsw/distance.js';
import {
  search,
  searchLayer,
  type HNSWGraph,
} from '../../../src/index/hnsw/search.js';

// ============================================================================
// TEST UTILITIES
// ============================================================================

/**
 * Generate a random normalized vector
 */
function randomNormalizedVector(dimensions: number): number[] {
  const vec = Array(dimensions).fill(0).map(() => Math.random() - 0.5);
  const norm = Math.sqrt(vec.reduce((sum, v) => sum + v * v, 0));
  return vec.map(v => v / norm);
}

/**
 * Generate vectors clustered around specific points
 */
function generateClusteredVectors(
  numClusters: number,
  vectorsPerCluster: number,
  dimensions: number,
  noiseScale: number = 0.1
): Map<string, number[]> {
  const vectors = new Map<string, number[]>();

  for (let c = 0; c < numClusters; c++) {
    // Generate cluster center
    const center = randomNormalizedVector(dimensions);

    for (let v = 0; v < vectorsPerCluster; v++) {
      // Add noise to cluster center
      const vec = center.map(x => x + (Math.random() - 0.5) * noiseScale);
      const norm = Math.sqrt(vec.reduce((sum, x) => sum + x * x, 0));
      const normalized = vec.map(x => x / norm);

      vectors.set(`cluster_${c}_vec_${v}`, normalized);
    }
  }

  return vectors;
}

/**
 * Brute-force k-nearest neighbors search for ground truth
 */
function bruteForceKNN(
  vectors: Map<string, number[]>,
  query: number[],
  k: number,
  distanceFn: (a: number[], b: number[]) => number
): Array<{ nodeId: string; distance: number }> {
  const results: Array<{ nodeId: string; distance: number }> = [];

  for (const [id, vec] of vectors) {
    const distance = distanceFn(query, vec);
    results.push({ nodeId: id, distance });
  }

  results.sort((a, b) => a.distance - b.distance);
  return results.slice(0, k);
}

/**
 * Calculate recall@k: what fraction of true k-nearest neighbors did we find?
 */
function calculateRecall(
  found: string[],
  groundTruth: string[]
): number {
  const truthSet = new Set(groundTruth);
  const correct = found.filter(id => truthSet.has(id)).length;
  return correct / groundTruth.length;
}

/**
 * Create a test HNSW graph with vectors
 */
function createTestGraph(vectors: Map<string, number[]>, maxLayers: number = 3): HNSWGraph {
  const layers: Map<string, string[]>[] = [];
  for (let i = 0; i < maxLayers; i++) {
    layers.push(new Map());
  }

  const ids = Array.from(vectors.keys());
  if (ids.length === 0) {
    return {
      entryPoint: null,
      maxLayer: 0,
      layers,
      nodeCount: 0,
    };
  }

  // Create connections based on actual distances
  const layer0Nodes = ids;
  const layer1Nodes = ids.filter((_, i) => i % 3 === 0);
  const layer2Nodes = ids.filter((_, i) => i % 9 === 0);

  // Layer 0: connect each node to its M nearest neighbors
  const M = 16;
  for (const id of layer0Nodes) {
    const vec = vectors.get(id)!;
    const others = layer0Nodes.filter(other => other !== id);
    const nearest = others
      .map(other => ({ id: other, dist: cosineDistance(vec, vectors.get(other)!) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, M)
      .map(n => n.id);
    layers[0]!.set(id, nearest);
  }

  // Layer 1
  if (maxLayers >= 2 && layer1Nodes.length > 1) {
    for (const id of layer1Nodes) {
      const vec = vectors.get(id)!;
      const others = layer1Nodes.filter(other => other !== id);
      const nearest = others
        .map(other => ({ id: other, dist: cosineDistance(vec, vectors.get(other)!) }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, M / 2)
        .map(n => n.id);
      layers[1]!.set(id, nearest);
    }
  }

  // Layer 2
  if (maxLayers >= 3 && layer2Nodes.length > 1) {
    for (const id of layer2Nodes) {
      const vec = vectors.get(id)!;
      const others = layer2Nodes.filter(other => other !== id);
      const nearest = others
        .map(other => ({ id: other, dist: cosineDistance(vec, vectors.get(other)!) }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, M / 4)
        .map(n => n.id);
      layers[2]!.set(id, nearest);
    }
  }

  const hasLayer2 = maxLayers >= 3 && layer2Nodes.length > 0;
  const hasLayer1 = maxLayers >= 2 && layer1Nodes.length > 0;

  const entryPoint = hasLayer2 ? layer2Nodes[0]! :
                     hasLayer1 ? layer1Nodes[0]! :
                     layer0Nodes[0]!;
  const actualMaxLayer = hasLayer2 ? 2 : hasLayer1 ? 1 : 0;

  return {
    entryPoint,
    maxLayer: actualMaxLayer,
    layers,
    nodeCount: ids.length,
  };
}

// ============================================================================
// ACCURACY TESTS
// ============================================================================

describe('HNSW Search Accuracy', () => {
  describe('Recall@k Metrics', () => {
    it('should achieve at least 90% recall@10 on random dataset with high ef', () => {
      const dimensions = 64;
      const numVectors = 100;

      // Generate random vectors
      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 3);
      const getVector = (id: string) => vectors.get(id)!;

      // Run multiple queries and measure recall
      const numQueries = 20;
      const k = 10;
      const ef = 100; // High ef for accuracy

      let totalRecall = 0;

      for (let q = 0; q < numQueries; q++) {
        const queryVec = randomNormalizedVector(dimensions);

        // Get ground truth
        const groundTruth = bruteForceKNN(vectors, queryVec, k, cosineDistance)
          .map(r => r.nodeId);

        // Get HNSW results
        const results = search(graph, getVector, queryVec, k, ef, cosineDistance);
        const found = results.map(r => r.nodeId);

        const recall = calculateRecall(found, groundTruth);
        totalRecall += recall;
      }

      const avgRecall = totalRecall / numQueries;
      expect(avgRecall).toBeGreaterThanOrEqual(0.9);
    });

    it('should achieve at least 80% recall@10 on clustered dataset', () => {
      const dimensions = 32;
      const numClusters = 5;
      const vectorsPerCluster = 20;

      const vectors = generateClusteredVectors(numClusters, vectorsPerCluster, dimensions, 0.1);
      const graph = createTestGraph(vectors, 3);
      const getVector = (id: string) => vectors.get(id)!;

      const numQueries = 20;
      const k = 10;
      const ef = 50;

      let totalRecall = 0;

      for (let q = 0; q < numQueries; q++) {
        // Query from a random cluster center
        const clusterIdx = Math.floor(Math.random() * numClusters);
        const queryVec = vectors.get(`cluster_${clusterIdx}_vec_0`)!;

        const groundTruth = bruteForceKNN(vectors, queryVec, k, cosineDistance)
          .map(r => r.nodeId);

        const results = search(graph, getVector, queryVec, k, ef, cosineDistance);
        const found = results.map(r => r.nodeId);

        totalRecall += calculateRecall(found, groundTruth);
      }

      const avgRecall = totalRecall / numQueries;
      expect(avgRecall).toBeGreaterThanOrEqual(0.8);
    });

    it('should improve recall with higher ef parameter', () => {
      const dimensions = 64;
      const numVectors = 200;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 3);
      const getVector = (id: string) => vectors.get(id)!;

      const k = 10;
      const queryVec = randomNormalizedVector(dimensions);
      const groundTruth = bruteForceKNN(vectors, queryVec, k, cosineDistance)
        .map(r => r.nodeId);

      // Test with different ef values
      const efValues = [10, 20, 50, 100];
      const recalls: number[] = [];

      for (const ef of efValues) {
        const results = search(graph, getVector, queryVec, k, ef, cosineDistance);
        const found = results.map(r => r.nodeId);
        recalls.push(calculateRecall(found, groundTruth));
      }

      // Higher ef should generally give better or equal recall
      for (let i = 1; i < recalls.length; i++) {
        // Allow small decreases due to randomness, but overall should trend up
        expect(recalls[i]).toBeGreaterThanOrEqual(recalls[i - 1]! - 0.1);
      }
    });

    it('should find exact match with recall 1.0 when query is in dataset', () => {
      const dimensions = 32;
      const numVectors = 50;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      // Use an existing vector as query
      const queryId = 'vec_25';
      const queryVec = vectors.get(queryId)!;

      const results = search(graph, getVector, queryVec, 1, 50, cosineDistance);

      expect(results.length).toBe(1);
      expect(results[0]!.nodeId).toBe(queryId);
      expect(results[0]!.distance).toBeCloseTo(0, 5);
    });
  });

  describe('Distance Accuracy', () => {
    it('should return results with distances matching brute-force', () => {
      const dimensions = 16;
      const numVectors = 30;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      const queryVec = randomNormalizedVector(dimensions);
      const k = 5;
      const ef = 100;

      const hnswResults = search(graph, getVector, queryVec, k, ef, cosineDistance);

      // Verify distances are correct
      for (const result of hnswResults) {
        const vec = vectors.get(result.nodeId)!;
        const actualDistance = cosineDistance(queryVec, vec);
        expect(result.distance).toBeCloseTo(actualDistance, 5);
      }
    });

    it('should return results in sorted order by distance', () => {
      const dimensions = 32;
      const numVectors = 100;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 3);
      const getVector = (id: string) => vectors.get(id)!;

      for (let q = 0; q < 10; q++) {
        const queryVec = randomNormalizedVector(dimensions);
        const results = search(graph, getVector, queryVec, 20, 50, cosineDistance);

        // Verify sorted order
        for (let i = 1; i < results.length; i++) {
          expect(results[i]!.distance).toBeGreaterThanOrEqual(results[i - 1]!.distance - 1e-10);
        }
      }
    });

    it('should work with Euclidean distance', () => {
      const dimensions = 16;
      const numVectors = 50;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        // Use non-normalized vectors for Euclidean
        const vec = Array(dimensions).fill(0).map(() => Math.random() * 10);
        vectors.set(`vec_${i}`, vec);
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      const queryVec = Array(dimensions).fill(0).map(() => Math.random() * 10);
      const k = 5;

      const hnswResults = search(graph, getVector, queryVec, k, 50, euclideanDistance);

      // Verify distances are correct for Euclidean
      for (const result of hnswResults) {
        const vec = vectors.get(result.nodeId)!;
        const actualDistance = euclideanDistance(queryVec, vec);
        expect(result.distance).toBeCloseTo(actualDistance, 5);
      }
    });
  });

  describe('Edge Cases', () => {
    it('should handle high-dimensional vectors (384 dims)', () => {
      const dimensions = 384; // Common embedding size
      const numVectors = 50;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 3);
      const getVector = (id: string) => vectors.get(id)!;

      const queryVec = randomNormalizedVector(dimensions);
      const results = search(graph, getVector, queryVec, 10, 30);

      expect(results.length).toBe(10);
      // Results should be valid distances in [0, 2] for cosine
      for (const result of results) {
        expect(result.distance).toBeGreaterThanOrEqual(0);
        expect(result.distance).toBeLessThanOrEqual(2);
      }
    });

    it('should handle very similar vectors (near duplicates)', () => {
      const dimensions = 32;
      const baseVec = randomNormalizedVector(dimensions);

      // Create near-duplicate vectors
      const vectors = new Map<string, number[]>();
      for (let i = 0; i < 20; i++) {
        // Add tiny noise
        const vec = baseVec.map(x => x + (Math.random() - 0.5) * 0.001);
        const norm = Math.sqrt(vec.reduce((sum, x) => sum + x * x, 0));
        vectors.set(`vec_${i}`, vec.map(x => x / norm));
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      const results = search(graph, getVector, baseVec, 10, 50);

      expect(results.length).toBe(10);
      // All results should be very close
      for (const result of results) {
        expect(result.distance).toBeLessThan(0.01);
      }
    });

    it('should handle sparse clusters (large distances between clusters)', () => {
      const dimensions = 16;

      // Create two well-separated clusters
      const vectors = new Map<string, number[]>();

      // Cluster 1: around [1, 0, 0, ...]
      for (let i = 0; i < 10; i++) {
        const vec = Array(dimensions).fill(0);
        vec[0] = 1 + (Math.random() - 0.5) * 0.1;
        const norm = Math.sqrt(vec.reduce((sum, x) => sum + x * x, 0));
        vectors.set(`cluster1_${i}`, vec.map(x => x / norm));
      }

      // Cluster 2: around [-1, 0, 0, ...] (opposite direction)
      for (let i = 0; i < 10; i++) {
        const vec = Array(dimensions).fill(0);
        vec[0] = -1 + (Math.random() - 0.5) * 0.1;
        const norm = Math.sqrt(vec.reduce((sum, x) => sum + x * x, 0));
        vectors.set(`cluster2_${i}`, vec.map(x => x / norm));
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      // Query near cluster 1
      const queryVec = Array(dimensions).fill(0);
      queryVec[0] = 1;

      const results = search(graph, getVector, queryVec, 10, 50);

      // All results should be from cluster 1
      for (const result of results) {
        expect(result.nodeId).toMatch(/^cluster1_/);
      }
    });

    it('should handle queries outside the convex hull of data', () => {
      const dimensions = 8;
      const numVectors = 30;

      // Generate vectors in a small region
      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        const vec = Array(dimensions).fill(0).map(() => Math.random() * 0.1); // Small values
        vectors.set(`vec_${i}`, vec);
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      // Query far outside the data region
      const queryVec = Array(dimensions).fill(10); // Large values

      const results = search(graph, getVector, queryVec, 5, 50, euclideanDistance);

      expect(results.length).toBe(5);
      // Should still find the closest points
      for (const result of results) {
        expect(vectors.has(result.nodeId)).toBe(true);
      }
    });

    it('should handle k=1 (single nearest neighbor)', () => {
      const dimensions = 16;
      const numVectors = 50;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 2);
      const getVector = (id: string) => vectors.get(id)!;

      const queryVec = randomNormalizedVector(dimensions);
      const results = search(graph, getVector, queryVec, 1, 50, cosineDistance);

      expect(results.length).toBe(1);

      // Verify it's actually the closest
      const groundTruth = bruteForceKNN(vectors, queryVec, 1, cosineDistance);
      expect(results[0]!.nodeId).toBe(groundTruth[0]!.nodeId);
    });

    it('should handle k larger than dataset size', () => {
      const dimensions = 8;
      const numVectors = 5;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 1);
      const getVector = (id: string) => vectors.get(id)!;

      const queryVec = randomNormalizedVector(dimensions);
      const results = search(graph, getVector, queryVec, 100, 50);

      // Should return all available vectors
      expect(results.length).toBe(numVectors);
    });
  });

  describe('Performance Characteristics', () => {
    it('should complete search in reasonable time for medium dataset', () => {
      const dimensions = 128;
      const numVectors = 1000;

      const vectors = new Map<string, number[]>();
      for (let i = 0; i < numVectors; i++) {
        vectors.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const graph = createTestGraph(vectors, 3);
      const getVector = (id: string) => vectors.get(id)!;

      const queryVec = randomNormalizedVector(dimensions);

      const startTime = performance.now();
      const results = search(graph, getVector, queryVec, 10, 50);
      const endTime = performance.now();

      expect(results.length).toBe(10);
      // Should complete in under 100ms (even with simple graph construction)
      expect(endTime - startTime).toBeLessThan(100);
    });

    it('should scale sub-linearly with dataset size', () => {
      const dimensions = 32;

      const smallDataset = new Map<string, number[]>();
      for (let i = 0; i < 100; i++) {
        smallDataset.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const largeDataset = new Map<string, number[]>();
      for (let i = 0; i < 500; i++) {
        largeDataset.set(`vec_${i}`, randomNormalizedVector(dimensions));
      }

      const smallGraph = createTestGraph(smallDataset, 3);
      const largeGraph = createTestGraph(largeDataset, 3);

      const queryVec = randomNormalizedVector(dimensions);

      // Time small dataset
      const smallStart = performance.now();
      for (let i = 0; i < 10; i++) {
        search(smallGraph, (id) => smallDataset.get(id)!, queryVec, 10, 30);
      }
      const smallTime = performance.now() - smallStart;

      // Time large dataset
      const largeStart = performance.now();
      for (let i = 0; i < 10; i++) {
        search(largeGraph, (id) => largeDataset.get(id)!, queryVec, 10, 30);
      }
      const largeTime = performance.now() - largeStart;

      // Large dataset (5x bigger) should take less than 3x the time (sub-linear)
      expect(largeTime).toBeLessThan(smallTime * 3);
    });
  });
});
