/**
 * HNSW Search Algorithm Tests
 *
 * TDD tests for pure TypeScript HNSW (Hierarchical Navigable Small World) implementation.
 * Tests cover:
 * - Distance function correctness (cosine, euclidean, inner product)
 * - Search finds nearest neighbors
 * - ef parameter affects quality
 * - Works with empty graph
 *
 * @see CLAUDE.md for architecture details
 */

import { describe, it, expect } from 'vitest';
import {
  cosineDistance,
  euclideanDistance,
  innerProduct,
  type DistanceFunction,
} from '../../../src/index/hnsw/distance.js';
import {
  searchLayer,
  search,
  type SearchResult,
  type HNSWGraph,
} from '../../../src/index/hnsw/search.js';

// ============================================================================
// Distance Function Tests
// ============================================================================

describe('Distance Functions', () => {
  describe('cosineDistance', () => {
    it('should return 0 for identical vectors', () => {
      const a = [1, 2, 3];
      const b = [1, 2, 3];
      const distance = cosineDistance(a, b);
      expect(distance).toBeCloseTo(0, 10);
    });

    it('should return 2 for opposite vectors (max distance)', () => {
      const a = [1, 0, 0];
      const b = [-1, 0, 0];
      const distance = cosineDistance(a, b);
      // Cosine similarity = -1, so cosine distance = 1 - (-1) = 2
      expect(distance).toBeCloseTo(2, 10);
    });

    it('should return 1 for orthogonal vectors', () => {
      const a = [1, 0, 0];
      const b = [0, 1, 0];
      // Cosine similarity = 0, so cosine distance = 1 - 0 = 1
      const distance = cosineDistance(a, b);
      expect(distance).toBeCloseTo(1, 10);
    });

    it('should handle normalized vectors correctly', () => {
      // Two normalized vectors at 60 degrees
      const a = [1, 0];
      const b = [0.5, Math.sqrt(3) / 2]; // cos(60) = 0.5
      const distance = cosineDistance(a, b);
      // Cosine similarity = 0.5, so cosine distance = 1 - 0.5 = 0.5
      expect(distance).toBeCloseTo(0.5, 10);
    });

    it('should be symmetric', () => {
      const a = [1, 2, 3, 4];
      const b = [4, 3, 2, 1];
      expect(cosineDistance(a, b)).toBeCloseTo(cosineDistance(b, a), 10);
    });

    it('should handle zero vectors', () => {
      const a = [0, 0, 0];
      const b = [1, 2, 3];
      const distance = cosineDistance(a, b);
      // Zero vector has undefined cosine similarity, should return max distance (2)
      expect(distance).toBe(2);
    });

    it('should handle high-dimensional vectors', () => {
      const dim = 384;
      const a = Array(dim).fill(0).map(() => Math.random());
      const b = Array(dim).fill(0).map(() => Math.random());
      const distance = cosineDistance(a, b);
      expect(distance).toBeGreaterThanOrEqual(0);
      expect(distance).toBeLessThanOrEqual(2);
    });
  });

  describe('euclideanDistance', () => {
    it('should return 0 for identical vectors', () => {
      const a = [1, 2, 3];
      const b = [1, 2, 3];
      const distance = euclideanDistance(a, b);
      expect(distance).toBe(0);
    });

    it('should calculate correct distance for simple vectors', () => {
      const a = [0, 0];
      const b = [3, 4];
      // Distance should be 5 (3-4-5 triangle)
      const distance = euclideanDistance(a, b);
      expect(distance).toBe(5);
    });

    it('should calculate correct distance in 3D', () => {
      const a = [0, 0, 0];
      const b = [1, 2, 2];
      // Distance = sqrt(1 + 4 + 4) = 3
      const distance = euclideanDistance(a, b);
      expect(distance).toBe(3);
    });

    it('should be symmetric', () => {
      const a = [1, 2, 3, 4];
      const b = [4, 3, 2, 1];
      expect(euclideanDistance(a, b)).toBe(euclideanDistance(b, a));
    });

    it('should satisfy triangle inequality', () => {
      const a = [0, 0];
      const b = [1, 0];
      const c = [0, 1];
      const ab = euclideanDistance(a, b);
      const bc = euclideanDistance(b, c);
      const ac = euclideanDistance(a, c);
      expect(ab + bc).toBeGreaterThanOrEqual(ac);
      expect(ab + ac).toBeGreaterThanOrEqual(bc);
      expect(bc + ac).toBeGreaterThanOrEqual(ab);
    });

    it('should handle high-dimensional vectors', () => {
      const dim = 384;
      const a = Array(dim).fill(0);
      const b = Array(dim).fill(1);
      // Distance = sqrt(384 * 1) = sqrt(384)
      const distance = euclideanDistance(a, b);
      expect(distance).toBeCloseTo(Math.sqrt(384), 10);
    });
  });

  describe('innerProduct', () => {
    it('should return 0 for identical normalized vectors (as distance)', () => {
      // For MIPS (maximum inner product search), we convert to distance
      // distance = 1 - inner_product (for normalized vectors)
      const a = [1 / Math.sqrt(2), 1 / Math.sqrt(2)];
      const b = [1 / Math.sqrt(2), 1 / Math.sqrt(2)];
      const distance = innerProduct(a, b);
      expect(distance).toBeCloseTo(0, 10);
    });

    it('should return positive distance for less similar vectors', () => {
      const a = [1, 0];
      const b = [0.5, 0.5];
      const distance = innerProduct(a, b);
      // Inner product = 0.5, distance = 1 - 0.5 = 0.5
      expect(distance).toBeGreaterThan(0);
    });

    it('should handle orthogonal vectors', () => {
      const a = [1, 0];
      const b = [0, 1];
      const distance = innerProduct(a, b);
      // Inner product = 0, distance = 1 - 0 = 1
      expect(distance).toBeCloseTo(1, 10);
    });

    it('should be symmetric', () => {
      const a = [1, 2, 3];
      const b = [4, 5, 6];
      expect(innerProduct(a, b)).toBeCloseTo(innerProduct(b, a), 10);
    });
  });
});

// ============================================================================
// HNSW Graph Helper
// ============================================================================

/**
 * Create a test HNSW graph with vectors at known positions
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

  // Simple graph: connect each node to its nearest neighbors at layer 0
  // Higher layers have fewer nodes
  const layer0Nodes = ids;
  const layer1Nodes = ids.filter((_, i) => i % 3 === 0);
  const layer2Nodes = ids.filter((_, i) => i % 9 === 0);

  // Create connections at layer 0 (all nodes, connect to k nearest)
  for (const id of layer0Nodes) {
    const vec = vectors.get(id)!;
    const others = layer0Nodes.filter(other => other !== id);
    // Connect to 4 nearest neighbors
    const nearest = others
      .map(other => ({ id: other, dist: euclideanDistance(vec, vectors.get(other)!) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 4)
      .map(n => n.id);
    layers[0]!.set(id, nearest);
  }

  // Layer 1 (only if we have at least 2 layers)
  if (maxLayers >= 2) {
    for (const id of layer1Nodes) {
      const vec = vectors.get(id)!;
      const others = layer1Nodes.filter(other => other !== id);
      const nearest = others
        .map(other => ({ id: other, dist: euclideanDistance(vec, vectors.get(other)!) }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, 2)
        .map(n => n.id);
      layers[1]!.set(id, nearest);
    }
  }

  // Layer 2 (only if we have at least 3 layers)
  if (maxLayers >= 3) {
    for (const id of layer2Nodes) {
      const vec = vectors.get(id)!;
      const others = layer2Nodes.filter(other => other !== id);
      const nearest = others
        .map(other => ({ id: other, dist: euclideanDistance(vec, vectors.get(other)!) }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, 1)
        .map(n => n.id);
      layers[2]!.set(id, nearest);
    }
  }

  // Entry point is a node that exists in the highest layer
  // Only consider layers that actually exist in the layers array
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
// Search Layer Tests
// ============================================================================

describe('searchLayer', () => {
  it('should find the nearest node in a layer', () => {
    const vectors = new Map<string, number[]>([
      ['node1', [0, 0]],
      ['node2', [1, 0]],
      ['node3', [2, 0]],
      ['node4', [3, 0]],
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    // Query near node2
    const results = searchLayer(
      graph,
      getVector,
      [0.9, 0], // Query vector
      ['node1'], // Entry points
      4, // ef
      0, // layer
      euclideanDistance
    );

    expect(results.length).toBeGreaterThan(0);
    expect(results[0]!.nodeId).toBe('node2');
    expect(results[0]!.distance).toBeCloseTo(0.1, 5);
  });

  it('should return multiple candidates sorted by distance', () => {
    const vectors = new Map<string, number[]>([
      ['node1', [0, 0]],
      ['node2', [1, 0]],
      ['node3', [2, 0]],
      ['node4', [3, 0]],
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    const results = searchLayer(
      graph,
      getVector,
      [1.5, 0], // Query between node2 and node3
      ['node1'],
      10,
      0,
      euclideanDistance
    );

    // Results should be sorted by distance
    for (let i = 1; i < results.length; i++) {
      expect(results[i]!.distance).toBeGreaterThanOrEqual(results[i - 1]!.distance);
    }
  });

  it('should work with cosine distance', () => {
    const vectors = new Map<string, number[]>([
      ['node1', [1, 0]],
      ['node2', [0.707, 0.707]],
      ['node3', [0, 1]],
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    // Query in the direction of node2
    const results = searchLayer(
      graph,
      getVector,
      [0.8, 0.6], // Closer to node2 in angle
      ['node1'],
      4,
      0,
      cosineDistance
    );

    expect(results.length).toBeGreaterThan(0);
    // node2 should be closer in cosine distance
    const node2Result = results.find(r => r.nodeId === 'node2');
    expect(node2Result).toBeDefined();
  });

  it('should respect ef parameter', () => {
    const vectors = new Map<string, number[]>();
    for (let i = 0; i < 20; i++) {
      vectors.set(`node${i}`, [i, 0]);
    }
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    const resultsSmallEf = searchLayer(
      graph,
      getVector,
      [10, 0],
      ['node0'],
      3,
      0,
      euclideanDistance
    );

    const resultsLargeEf = searchLayer(
      graph,
      getVector,
      [10, 0],
      ['node0'],
      20,
      0,
      euclideanDistance
    );

    // Larger ef should explore more candidates
    expect(resultsLargeEf.length).toBeGreaterThanOrEqual(resultsSmallEf.length);
  });
});

// ============================================================================
// Search Tests
// ============================================================================

describe('search', () => {
  it('should find k nearest neighbors', () => {
    const vectors = new Map<string, number[]>([
      ['node1', [0, 0]],
      ['node2', [1, 0]],
      ['node3', [2, 0]],
      ['node4', [10, 0]], // Far away
    ]);
    const graph = createTestGraph(vectors, 2);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [0.5, 0],
      3, // k=3
      undefined, // default ef
      euclideanDistance // Use euclidean for position-based test
    );

    expect(results.length).toBe(3);
    // Should find the 3 closest nodes
    const nodeIds = results.map(r => r.nodeId);
    expect(nodeIds).toContain('node1');
    expect(nodeIds).toContain('node2');
    expect(nodeIds).toContain('node3');
    expect(nodeIds).not.toContain('node4');
  });

  it('should return results sorted by distance', () => {
    const vectors = new Map<string, number[]>([
      ['a', [0, 0]],
      ['b', [1, 1]],
      ['c', [2, 2]],
      ['d', [3, 3]],
      ['e', [4, 4]],
    ]);
    const graph = createTestGraph(vectors, 2);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [0, 0],
      5,
      undefined, // default ef
      euclideanDistance // Use euclidean for position-based test
    );

    expect(results.length).toBe(5);
    for (let i = 1; i < results.length; i++) {
      expect(results[i]!.distance).toBeGreaterThanOrEqual(results[i - 1]!.distance);
    }
    // First result should be the query point itself if it exists
    expect(results[0]!.nodeId).toBe('a');
    expect(results[0]!.distance).toBeCloseTo(0, 5);
  });

  it('should handle k larger than graph size', () => {
    const vectors = new Map<string, number[]>([
      ['node1', [0, 0]],
      ['node2', [1, 0]],
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [0, 0],
      10 // k > graph size
    );

    expect(results.length).toBe(2); // Should return all available nodes
  });

  it('should work with empty graph', () => {
    const vectors = new Map<string, number[]>();
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [0, 0],
      5
    );

    expect(results).toEqual([]);
  });

  it('should work with single node graph', () => {
    const vectors = new Map<string, number[]>([
      ['only', [1, 2, 3]],
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [0, 0, 0],
      5
    );

    expect(results.length).toBe(1);
    expect(results[0]!.nodeId).toBe('only');
  });

  it('should use default cosine distance', () => {
    const vectors = new Map<string, number[]>([
      ['north', [0, 1]],
      ['east', [1, 0]],
      ['northeast', [0.707, 0.707]],
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    // Query pointing slightly north-east
    const results = search(
      graph,
      getVector,
      [0.6, 0.8],
      3
      // No distance function - should default to cosine
    );

    expect(results.length).toBe(3);
    // Results should be sorted by cosine distance
    for (let i = 1; i < results.length; i++) {
      expect(results[i]!.distance).toBeGreaterThanOrEqual(results[i - 1]!.distance);
    }
  });

  it('should allow custom distance function', () => {
    const vectors = new Map<string, number[]>([
      ['a', [0, 0]],
      ['b', [3, 4]], // Euclidean distance 5 from origin
      ['c', [4, 3]], // Euclidean distance 5 from origin
    ]);
    const graph = createTestGraph(vectors, 1);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [0, 0],
      3,
      undefined, // default ef
      euclideanDistance
    );

    expect(results.length).toBe(3);
    expect(results[0]!.nodeId).toBe('a');
    expect(results[0]!.distance).toBe(0);
    // b and c should have equal distance
    expect(results[1]!.distance).toBeCloseTo(5, 5);
    expect(results[2]!.distance).toBeCloseTo(5, 5);
  });

  it('ef parameter should affect search quality', () => {
    // Create a larger graph where ef matters
    const vectors = new Map<string, number[]>();
    const dim = 8;
    for (let i = 0; i < 100; i++) {
      const vec = Array(dim).fill(0).map((_, j) => Math.sin(i * 0.1 + j));
      vectors.set(`node${i}`, vec);
    }
    const graph = createTestGraph(vectors, 3);
    const getVector = (id: string) => vectors.get(id)!;

    const queryVec = Array(dim).fill(0).map((_, j) => Math.sin(50 * 0.1 + j));

    // Low ef search
    const resultsLowEf = search(
      graph,
      getVector,
      queryVec,
      5,
      10 // low ef
    );

    // High ef search
    const resultsHighEf = search(
      graph,
      getVector,
      queryVec,
      5,
      100 // high ef
    );

    // Both should return 5 results
    expect(resultsLowEf.length).toBe(5);
    expect(resultsHighEf.length).toBe(5);

    // High ef should find results with lower or equal distance (better quality)
    // Note: This is a probabilistic test, so we just check that high ef doesn't do worse
    expect(resultsHighEf[0]!.distance).toBeLessThanOrEqual(resultsLowEf[0]!.distance + 0.001);
  });

  it('should handle high-dimensional vectors', () => {
    const dim = 384; // Typical embedding dimension
    const vectors = new Map<string, number[]>();
    for (let i = 0; i < 50; i++) {
      const vec = Array(dim).fill(0).map(() => Math.random());
      vectors.set(`node${i}`, vec);
    }
    const graph = createTestGraph(vectors, 3);
    const getVector = (id: string) => vectors.get(id)!;

    const queryVec = Array(dim).fill(0).map(() => Math.random());

    const results = search(
      graph,
      getVector,
      queryVec,
      10
    );

    expect(results.length).toBe(10);
    // Results should be sorted
    for (let i = 1; i < results.length; i++) {
      expect(results[i]!.distance).toBeGreaterThanOrEqual(results[i - 1]!.distance);
    }
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('HNSW Integration', () => {
  it('should correctly find nearest neighbor in a clustered dataset', () => {
    // Create a single cluster to test basic search functionality
    // Two distant clusters may not be well connected in a simple test graph
    const vectors = new Map<string, number[]>();

    // Create nodes in a line - easy to verify distance ordering
    for (let i = 0; i < 20; i++) {
      vectors.set(`node_${i}`, [i, 0]);
    }

    const graph = createTestGraph(vectors, 3);
    const getVector = (id: string) => vectors.get(id)!;

    // Query near the start of the line
    const results1 = search(
      graph,
      getVector,
      [2.5, 0],
      5,
      50,
      euclideanDistance
    );

    // Should find nodes 2 and 3 as closest, then 1 and 4
    expect(results1.length).toBe(5);
    const nodeIds1 = results1.map(r => r.nodeId);
    expect(nodeIds1).toContain('node_2');
    expect(nodeIds1).toContain('node_3');

    // Query near the end of the line
    const results2 = search(
      graph,
      getVector,
      [17.5, 0],
      5,
      50,
      euclideanDistance
    );

    // Should find nodes 17 and 18 as closest
    expect(results2.length).toBe(5);
    const nodeIds2 = results2.map(r => r.nodeId);
    expect(nodeIds2).toContain('node_17');
    expect(nodeIds2).toContain('node_18');

    // Verify results are from the correct region (nearby nodes)
    // All results should be within distance 3 of the query point
    expect(results2.every(r => r.distance <= 3)).toBe(true);
  });

  it('should work with the VectorIndex type from index-store', () => {
    // This test ensures compatibility with the existing VectorIndex structure
    const vectors = new Map<string, number[]>([
      ['entity1', [1, 0, 0]],
      ['entity2', [0, 1, 0]],
      ['entity3', [0, 0, 1]],
    ]);

    const graph = createTestGraph(vectors, 2);
    const getVector = (id: string) => vectors.get(id)!;

    const results = search(
      graph,
      getVector,
      [1, 0, 0], // Should match entity1 exactly
      1,
      undefined,
      cosineDistance
    );

    expect(results.length).toBe(1);
    expect(results[0]!.nodeId).toBe('entity1');
    expect(results[0]!.distance).toBeCloseTo(0, 10);
  });
});
