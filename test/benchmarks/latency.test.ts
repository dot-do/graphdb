/**
 * Latency Budget Performance Tests
 *
 * Architecture latency budgets:
 * - Snippet operations < 5ms total
 * - 1-hop traversal < 10ms
 * - 3-hop traversal < 50ms
 *
 * Specific component budgets tested:
 * - Snippet bloom check < 2ms
 * - Query tokenizer parse < 1ms
 * - Shard router lookup < 1ms
 * - Point lookup < 5ms
 * - 1-hop traversal < 10ms
 * - 3-hop traversal < 50ms
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import {
  createBloomFilter,
  addToFilter,
  mightExist,
  serializeFilter,
  deserializeFilter,
  type BloomFilter,
} from '../../src/snippet/bloom.js';
import { tokenize, createLexer } from '../../src/snippet/lexer.js';
import {
  routeEntity,
  routeQuery,
  getShardId,
  extractNamespace,
  estimateQueryCost,
  generateCacheKey,
} from '../../src/snippet/router.js';
import { createNamespace, type EntityId, type Namespace } from '../../src/core/types.js';

// ============================================================================
// Test Configuration
// ============================================================================

/** Number of iterations for latency measurements */
const ITERATIONS = 1000;

/** Number of warmup iterations before measuring */
const WARMUP_ITERATIONS = 100;

/** Target latencies in milliseconds */
const LATENCY_BUDGETS = {
  bloomCheck: 2, // < 2ms
  tokenizerParse: 1, // < 1ms
  shardRouterLookup: 1, // < 1ms
  pointLookup: 5, // < 5ms (simulated, real DO would have network)
  oneHopTraversal: 10, // < 10ms (simulated)
  threeHopTraversal: 50, // < 50ms (simulated)
};

// ============================================================================
// Utility Functions
// ============================================================================

interface LatencyStats {
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
}

/**
 * Measure latency over multiple iterations
 */
function measureLatency(fn: () => void, iterations: number = ITERATIONS): LatencyStats {
  const latencies: number[] = [];

  // Warmup
  for (let i = 0; i < WARMUP_ITERATIONS; i++) {
    fn();
  }

  // Measure
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    fn();
    const end = performance.now();
    latencies.push(end - start);
  }

  // Sort for percentile calculations
  latencies.sort((a, b) => a - b);

  const sum = latencies.reduce((a, b) => a + b, 0);

  return {
    min: latencies[0],
    max: latencies[latencies.length - 1],
    avg: sum / latencies.length,
    p50: latencies[Math.floor(latencies.length * 0.5)],
    p95: latencies[Math.floor(latencies.length * 0.95)],
    p99: latencies[Math.floor(latencies.length * 0.99)],
  };
}

/**
 * Format latency stats for logging
 */
function formatStats(stats: LatencyStats): string {
  return `avg=${stats.avg.toFixed(3)}ms, p50=${stats.p50.toFixed(3)}ms, p95=${stats.p95.toFixed(3)}ms, p99=${stats.p99.toFixed(3)}ms, max=${stats.max.toFixed(3)}ms`;
}

// ============================================================================
// Bloom Filter Latency Tests
// ============================================================================

describe('Bloom Filter Latency Budget', () => {
  let filter: BloomFilter;
  let testEntityIds: string[];
  let nonExistentIds: string[];

  beforeAll(() => {
    // Create filter with 10K entries (realistic production size)
    filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
    });

    testEntityIds = [];
    for (let i = 0; i < 10000; i++) {
      const id = `https://example.com/entities/entity_${i.toString(16).padStart(8, '0')}`;
      testEntityIds.push(id);
      addToFilter(filter, id);
    }

    // Generate non-existent IDs for testing false positive path
    nonExistentIds = [];
    for (let i = 0; i < 1000; i++) {
      nonExistentIds.push(`https://example.com/entities/__nonexistent__${i}`);
    }
  });

  it('single bloom check completes in < 2ms', () => {
    const testId = testEntityIds[5000];

    const stats = measureLatency(() => {
      mightExist(filter, testId);
    });

    console.log(`Bloom check (existing entity): ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
  });

  it('bloom check for non-existent entity < 2ms', () => {
    const nonExistent = nonExistentIds[0];

    const stats = measureLatency(() => {
      mightExist(filter, nonExistent);
    });

    console.log(`Bloom check (non-existent): ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
  });

  it('batch of 10 bloom checks < 2ms total', () => {
    const batchIds = testEntityIds.slice(0, 10);

    const stats = measureLatency(() => {
      for (const id of batchIds) {
        mightExist(filter, id);
      }
    });

    console.log(`Bloom check batch (10 entities): ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
  });

  it('bloom check on deserialized filter < 2ms', () => {
    // Simulate filter loaded from edge cache
    const serialized = serializeFilter(filter);
    const deserializedFilter = deserializeFilter(serialized);
    const testId = testEntityIds[5000];

    const stats = measureLatency(() => {
      mightExist(deserializedFilter, testId);
    });

    console.log(`Bloom check (deserialized filter): ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.bloomCheck);
  });
});

// ============================================================================
// Query Tokenizer Latency Tests
// ============================================================================

describe('Query Tokenizer Latency Budget', () => {
  // The lexer uses simplified query syntax: type:id.predicate
  // Not full URLs - those are handled at the routing layer
  const simpleQuery = 'user:123.friends';
  const complexQuery = 'user:123.friends[?age > 30 and status = "active"].posts';
  const multiHopQuery = 'user:123.friends.friends.friends[?depth <= 3]';
  const reverseQuery = 'post:456 <- likes';

  it('simple query tokenization < 1ms', () => {
    const stats = measureLatency(() => {
      tokenize(simpleQuery);
    });

    console.log(`Tokenize simple query: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
  });

  it('complex query with filters tokenization < 1ms', () => {
    const stats = measureLatency(() => {
      tokenize(complexQuery);
    });

    console.log(`Tokenize complex query: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
  });

  it('multi-hop query tokenization < 1ms', () => {
    const stats = measureLatency(() => {
      tokenize(multiHopQuery);
    });

    console.log(`Tokenize multi-hop query: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
  });

  it('reverse traversal query tokenization < 1ms', () => {
    const stats = measureLatency(() => {
      tokenize(reverseQuery);
    });

    console.log(`Tokenize reverse traversal: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
  });

  it('lexer creation and iteration < 1ms', () => {
    const stats = measureLatency(() => {
      const lexer = createLexer(complexQuery);
      while (!lexer.isAtEnd()) {
        lexer.next();
      }
    });

    console.log(`Lexer full iteration: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.tokenizerParse);
  });
});

// ============================================================================
// Shard Router Latency Tests
// ============================================================================

describe('Shard Router Latency Budget', () => {
  const testEntityId = 'https://example.com/crm/acme/customer/123' as EntityId;
  const simpleQuery = 'https://example.com/users/123.friends';
  const crossNamespaceQuery =
    'https://example.com/users/123.friends.https://other.com/entities/456';

  it('single entity routing < 1ms', () => {
    const stats = measureLatency(() => {
      routeEntity(testEntityId);
    });

    console.log(`Route single entity: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
  });

  it('namespace extraction < 1ms', () => {
    const stats = measureLatency(() => {
      extractNamespace(testEntityId);
    });

    console.log(`Extract namespace: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
  });

  it('shard ID generation < 1ms', () => {
    const namespace = createNamespace('https://example.com/crm/');

    const stats = measureLatency(() => {
      getShardId(namespace);
    });

    console.log(`Generate shard ID: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
  });

  it('query routing (single namespace) < 1ms', () => {
    const stats = measureLatency(() => {
      routeQuery(simpleQuery);
    });

    console.log(`Route simple query: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
  });

  it('query cost estimation < 1ms', () => {
    const stats = measureLatency(() => {
      estimateQueryCost(crossNamespaceQuery);
    });

    console.log(`Estimate query cost: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
  });

  it('cache key generation < 1ms', () => {
    const stats = measureLatency(() => {
      generateCacheKey(simpleQuery);
    });

    console.log(`Generate cache key: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.shardRouterLookup);
  });
});

// ============================================================================
// Point Lookup Latency Tests (Simulated)
// ============================================================================

describe('Point Lookup Latency Budget', () => {
  // Simulates the in-memory portion of a point lookup
  // Real DO would add network latency, but we test the computation

  let entityMap: Map<string, object>;
  let testIds: string[];

  beforeAll(() => {
    entityMap = new Map();
    testIds = [];

    // Simulate 10K entities in memory
    for (let i = 0; i < 10000; i++) {
      const id = `https://example.com/entities/entity_${i}`;
      testIds.push(id);
      entityMap.set(id, {
        $id: id,
        $type: 'https://schema.org/Person',
        name: `Person ${i}`,
        age: 20 + (i % 60),
        email: `person${i}@example.com`,
      });
    }
  });

  it('map lookup < 5ms (computational portion)', () => {
    const testId = testIds[5000];

    const stats = measureLatency(() => {
      entityMap.get(testId);
    });

    console.log(`Map lookup: ${formatStats(stats)}`);

    // Map lookup should be sub-microsecond
    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.pointLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.pointLookup);
  });

  it('full point lookup simulation < 5ms', () => {
    // Simulate: bloom check + route + lookup
    const filter = createBloomFilter({ capacity: 10000, targetFpr: 0.01 });
    for (const id of testIds) {
      addToFilter(filter, id);
    }

    const testId = testIds[5000] as EntityId;

    const stats = measureLatency(() => {
      // Step 1: Bloom check
      const maybeExists = mightExist(filter, testId);
      if (!maybeExists) return null;

      // Step 2: Route to shard
      const shardInfo = routeEntity(testId);

      // Step 3: Lookup (simulated - real would be DO fetch)
      const entity = entityMap.get(testId);

      return { shardInfo, entity };
    });

    console.log(`Full point lookup simulation: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.pointLookup);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.pointLookup);
  });
});

// ============================================================================
// 1-Hop Traversal Latency Tests (Simulated)
// ============================================================================

describe('1-Hop Traversal Latency Budget', () => {
  // Simulates the computational portion of a 1-hop traversal

  interface GraphNode {
    id: string;
    neighbors: string[];
    data: object;
  }

  let graph: Map<string, GraphNode>;
  let startNodeId: string;

  beforeAll(() => {
    graph = new Map();

    // Build a graph: 1000 nodes, each with 10 neighbors
    for (let i = 0; i < 1000; i++) {
      const id = `https://example.com/entities/node_${i}`;
      const neighbors: string[] = [];

      // Connect to 10 random neighbors
      for (let j = 0; j < 10; j++) {
        const neighborIdx = (i + j + 1) % 1000;
        neighbors.push(`https://example.com/entities/node_${neighborIdx}`);
      }

      graph.set(id, {
        id,
        neighbors,
        data: { name: `Node ${i}`, value: i * 100 },
      });
    }

    startNodeId = 'https://example.com/entities/node_0';
  });

  it('1-hop traversal simulation < 10ms', () => {
    const stats = measureLatency(() => {
      // Step 1: Get start node
      const startNode = graph.get(startNodeId);
      if (!startNode) return [];

      // Step 2: Collect all 1-hop neighbors
      const results: GraphNode[] = [];
      for (const neighborId of startNode.neighbors) {
        const neighbor = graph.get(neighborId);
        if (neighbor) {
          results.push(neighbor);
        }
      }

      return results;
    });

    console.log(`1-hop traversal simulation: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.oneHopTraversal);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.oneHopTraversal);
  });

  it('1-hop with bloom check and routing < 10ms', () => {
    // More realistic: include bloom check and routing
    const filter = createBloomFilter({ capacity: 1000, targetFpr: 0.01 });
    for (const id of graph.keys()) {
      addToFilter(filter, id);
    }

    const stats = measureLatency(() => {
      // Bloom check start
      if (!mightExist(filter, startNodeId)) return [];

      // Route start node
      routeEntity(startNodeId as EntityId);

      // Get start node
      const startNode = graph.get(startNodeId);
      if (!startNode) return [];

      // Collect 1-hop neighbors
      const results: GraphNode[] = [];
      for (const neighborId of startNode.neighbors) {
        // Bloom check each neighbor
        if (mightExist(filter, neighborId)) {
          const neighbor = graph.get(neighborId);
          if (neighbor) {
            results.push(neighbor);
          }
        }
      }

      return results;
    });

    console.log(`1-hop with bloom/routing: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.oneHopTraversal);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.oneHopTraversal);
  });
});

// ============================================================================
// 3-Hop Traversal Latency Tests (Simulated)
// ============================================================================

describe('3-Hop Traversal Latency Budget', () => {
  interface GraphNode {
    id: string;
    neighbors: string[];
    data: object;
  }

  let graph: Map<string, GraphNode>;
  let startNodeId: string;

  beforeAll(() => {
    graph = new Map();

    // Build a graph: 5000 nodes, each with 5 neighbors
    for (let i = 0; i < 5000; i++) {
      const id = `https://example.com/entities/node_${i}`;
      const neighbors: string[] = [];

      for (let j = 0; j < 5; j++) {
        const neighborIdx = (i + j + 1) % 5000;
        neighbors.push(`https://example.com/entities/node_${neighborIdx}`);
      }

      graph.set(id, {
        id,
        neighbors,
        data: { name: `Node ${i}`, depth: 0 },
      });
    }

    startNodeId = 'https://example.com/entities/node_0';
  });

  it('3-hop BFS traversal simulation < 50ms', () => {
    const stats = measureLatency(() => {
      const visited = new Set<string>();
      const results: GraphNode[] = [];
      let frontier = [startNodeId];

      // 3 hops
      for (let depth = 0; depth < 3; depth++) {
        const nextFrontier: string[] = [];

        for (const nodeId of frontier) {
          if (visited.has(nodeId)) continue;
          visited.add(nodeId);

          const node = graph.get(nodeId);
          if (!node) continue;

          results.push(node);

          for (const neighborId of node.neighbors) {
            if (!visited.has(neighborId)) {
              nextFrontier.push(neighborId);
            }
          }
        }

        frontier = nextFrontier;
      }

      return results;
    });

    console.log(`3-hop BFS traversal: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.threeHopTraversal);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.threeHopTraversal);
  });

  it('3-hop with max results limit < 50ms', () => {
    const MAX_RESULTS = 100;

    const stats = measureLatency(() => {
      const visited = new Set<string>();
      const results: GraphNode[] = [];
      let frontier = [startNodeId];

      for (let depth = 0; depth < 3 && results.length < MAX_RESULTS; depth++) {
        const nextFrontier: string[] = [];

        for (const nodeId of frontier) {
          if (results.length >= MAX_RESULTS) break;
          if (visited.has(nodeId)) continue;
          visited.add(nodeId);

          const node = graph.get(nodeId);
          if (!node) continue;

          results.push(node);

          for (const neighborId of node.neighbors) {
            if (!visited.has(neighborId)) {
              nextFrontier.push(neighborId);
            }
          }
        }

        frontier = nextFrontier;
      }

      return results;
    });

    console.log(`3-hop with limit (max ${MAX_RESULTS}): ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.threeHopTraversal);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.threeHopTraversal);
  });

  it('3-hop with filter predicate < 50ms', () => {
    const stats = measureLatency(() => {
      const visited = new Set<string>();
      const results: GraphNode[] = [];
      let frontier = [startNodeId];

      for (let depth = 0; depth < 3; depth++) {
        const nextFrontier: string[] = [];

        for (const nodeId of frontier) {
          if (visited.has(nodeId)) continue;
          visited.add(nodeId);

          const node = graph.get(nodeId);
          if (!node) continue;

          // Apply filter: only include nodes with even index
          const nodeIndex = parseInt(node.id.split('_').pop() || '0');
          if (nodeIndex % 2 === 0) {
            results.push(node);
          }

          for (const neighborId of node.neighbors) {
            if (!visited.has(neighborId)) {
              nextFrontier.push(neighborId);
            }
          }
        }

        frontier = nextFrontier;
      }

      return results;
    });

    console.log(`3-hop with filter: ${formatStats(stats)}`);

    expect(stats.avg).toBeLessThan(LATENCY_BUDGETS.threeHopTraversal);
    expect(stats.p99).toBeLessThan(LATENCY_BUDGETS.threeHopTraversal);
  });
});

// ============================================================================
// Combined Snippet Layer Latency Tests
// ============================================================================

describe('Combined Snippet Layer Latency Budget', () => {
  let filter: BloomFilter;
  const testEntityIds: string[] = [];

  beforeAll(() => {
    filter = createBloomFilter({ capacity: 10000, targetFpr: 0.01 });

    for (let i = 0; i < 10000; i++) {
      const id = `https://example.com/entities/entity_${i}`;
      testEntityIds.push(id);
      addToFilter(filter, id);
    }
  });

  it('full snippet pipeline (tokenize + bloom + route) < 5ms', () => {
    // Lexer uses simplified syntax; router handles full URLs
    const query = 'entity:5000.friends[?age > 30]';
    const routerQuery = 'https://example.com/entities/entity_5000.friends';
    const entityId = testEntityIds[5000] as EntityId;

    const stats = measureLatency(() => {
      // Step 1: Tokenize query (uses simplified syntax)
      tokenize(query);

      // Step 2: Bloom check
      mightExist(filter, entityId);

      // Step 3: Route to shard (uses full URL format)
      routeQuery(routerQuery);

      // Step 4: Estimate cost
      estimateQueryCost(routerQuery);

      // Step 5: Generate cache key
      generateCacheKey(routerQuery);
    });

    console.log(`Full snippet pipeline: ${formatStats(stats)}`);

    // Combined snippet operations should stay well under 5ms budget
    expect(stats.avg).toBeLessThan(5);
    expect(stats.p99).toBeLessThan(5);
  });
});
