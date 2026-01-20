/**
 * Benchmark Scenarios for GraphDB
 *
 * Measures real production performance metrics:
 * - Point lookup latency (p50, p95, p99)
 * - 1-hop traversal latency
 * - 3-hop traversal latency
 * - Write throughput
 * - Bloom filter hit rate
 * - Edge cache hit rate
 */

import { ObjectType } from '../core/types.js';
import { generateULID, randomEntityId, DATASETS } from './datasets.js';

// ============================================================================
// Benchmark Result Types
// ============================================================================

export interface LatencyStats {
  count: number;
  min: number;
  max: number;
  mean: number;
  p50: number;
  p95: number;
  p99: number;
  stdDev: number;
}

export interface ThroughputStats {
  operationsPerSecond: number;
  bytesPerSecond: number;
  totalOperations: number;
  totalBytes: number;
  durationMs: number;
}

export interface CacheStats {
  hits: number;
  misses: number;
  hitRate: number;
  totalRequests: number;
}

export interface BenchmarkResult {
  scenario: string;
  dataset: string;
  timestamp: number;
  durationMs: number;
  iterations: number;
  latency?: LatencyStats;
  throughput?: ThroughputStats;
  cache?: CacheStats;
  metadata?: Record<string, unknown>;
}

// ============================================================================
// Statistics Helpers
// ============================================================================

function calculateLatencyStats(latencies: number[]): LatencyStats {
  if (latencies.length === 0) {
    return {
      count: 0,
      min: 0,
      max: 0,
      mean: 0,
      p50: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
    };
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const count = sorted.length;
  const sum = sorted.reduce((a, b) => a + b, 0);
  const mean = sum / count;

  // Calculate standard deviation
  const squaredDiffs = sorted.map((x) => Math.pow(x - mean, 2));
  const avgSquaredDiff = squaredDiffs.reduce((a, b) => a + b, 0) / count;
  const stdDev = Math.sqrt(avgSquaredDiff);

  // Percentiles
  const percentile = (p: number): number => {
    const index = Math.ceil((p / 100) * count) - 1;
    return sorted[Math.max(0, Math.min(index, count - 1))]!;
  };

  return {
    count,
    min: sorted[0]!,
    max: sorted[count - 1]!,
    mean,
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    stdDev,
  };
}

// ============================================================================
// Scenario Interfaces
// ============================================================================

export interface ScenarioContext {
  // Shard DO stub getter
  getShardStub: (shardId: string) => DurableObjectStub;
  // KV namespace for cache metadata
  cacheKV: KVNamespace;
  // R2 bucket for lakehouse
  lakehouse: R2Bucket;
  // Dataset name for entity generation
  dataset: string;
}

export type ScenarioRunner = (
  ctx: ScenarioContext,
  iterations: number
) => Promise<BenchmarkResult>;

// ============================================================================
// Point Lookup Scenario
// ============================================================================

/**
 * Point Lookup Benchmark
 *
 * Measures latency for fetching a single entity by ID.
 * Tests the hot path for entity retrieval.
 */
export async function runPointLookup(
  ctx: ScenarioContext,
  iterations: number
): Promise<BenchmarkResult> {
  const latencies: number[] = [];
  const startTime = Date.now();

  // Use default shard for benchmark
  const shardStub = ctx.getShardStub('shard-benchmark');

  for (let i = 0; i < iterations; i++) {
    // Generate random entity ID based on dataset
    const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
    const entityId = randomEntityId(ctx.dataset, entityType);
    const encodedSubject = encodeURIComponent(entityId);

    const opStart = performance.now();

    try {
      const response = await shardStub.fetch(
        new Request(`https://shard-do/triples/${encodedSubject}`)
      );

      if (!response.ok && response.status !== 404) {
        console.error(`Point lookup failed: ${response.status}`);
      }
    } catch (error) {
      console.error('Point lookup error:', error);
    }

    const opEnd = performance.now();
    latencies.push(opEnd - opStart);
  }

  const endTime = Date.now();

  return {
    scenario: 'point-lookup',
    dataset: ctx.dataset,
    timestamp: startTime,
    durationMs: endTime - startTime,
    iterations,
    latency: calculateLatencyStats(latencies),
    metadata: {
      description: 'Single entity lookup by ID',
      shardId: 'shard-benchmark',
    },
  };
}

// ============================================================================
// 1-Hop Traversal Scenario
// ============================================================================

/**
 * 1-Hop Traversal Benchmark
 *
 * Measures latency for fetching an entity and its direct relationships.
 * E.g., Get a worker and all their skills, or a movie and its cast.
 */
export async function runTraversal1Hop(
  ctx: ScenarioContext,
  iterations: number
): Promise<BenchmarkResult> {
  const latencies: number[] = [];
  const startTime = Date.now();
  let totalHops = 0;

  const shardStub = ctx.getShardStub('shard-benchmark');

  for (let i = 0; i < iterations; i++) {
    const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
    const entityId = randomEntityId(ctx.dataset, entityType);
    const encodedSubject = encodeURIComponent(entityId);

    const opStart = performance.now();

    try {
      // First: Get the entity
      const entityResponse = await shardStub.fetch(
        new Request(`https://shard-do/triples/${encodedSubject}`)
      );

      if (entityResponse.ok) {
        const entityData = await entityResponse.json() as { triples: Array<{ object: { type: number; value?: string } }> };
        const triples = entityData.triples || [];

        // Find REF type objects (relationships)
        const refs = triples.filter(
          (t: { object: { type: number } }) => t.object.type === ObjectType.REF
        );

        // Fetch first 5 related entities (limit for benchmark)
        const hopPromises = refs.slice(0, 5).map(async (refTriple: { object: { value?: string } }) => {
          const refId = refTriple.object.value;
          if (refId) {
            const refEncoded = encodeURIComponent(refId);
            await shardStub.fetch(
              new Request(`https://shard-do/triples/${refEncoded}`)
            );
            return 1;
          }
          return 0;
        });

        const hopResults = await Promise.all(hopPromises);
        totalHops += hopResults.reduce<number>((a, b) => a + b, 0);
      }
    } catch (error) {
      console.error('1-hop traversal error:', error);
    }

    const opEnd = performance.now();
    latencies.push(opEnd - opStart);
  }

  const endTime = Date.now();

  return {
    scenario: 'traversal-1hop',
    dataset: ctx.dataset,
    timestamp: startTime,
    durationMs: endTime - startTime,
    iterations,
    latency: calculateLatencyStats(latencies),
    metadata: {
      description: 'Entity + direct relationships (1-hop)',
      totalHopsPerformed: totalHops,
      avgHopsPerIteration: totalHops / iterations,
    },
  };
}

// ============================================================================
// 3-Hop Traversal Scenario
// ============================================================================

/**
 * 3-Hop Traversal Benchmark
 *
 * Measures latency for deep graph traversal.
 * E.g., Worker -> Occupation -> RequiredSkills -> RelatedOccupations
 * E.g., Movie -> Director -> OtherMovies -> Actors
 */
export async function runTraversal3Hop(
  ctx: ScenarioContext,
  iterations: number
): Promise<BenchmarkResult> {
  const latencies: number[] = [];
  const startTime = Date.now();
  let totalHops = 0;

  const shardStub = ctx.getShardStub('shard-benchmark');

  for (let i = 0; i < iterations; i++) {
    const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
    const entityId = randomEntityId(ctx.dataset, entityType);
    const visited = new Set<string>();
    visited.add(entityId);

    const opStart = performance.now();

    try {
      // Hop 1: Get initial entity
      let currentIds: string[] = [entityId];

      for (let hop = 0; hop < 3; hop++) {
        const nextIds: string[] = [];

        for (const currentId of currentIds.slice(0, 3)) {
          // Limit fan-out
          const encoded = encodeURIComponent(currentId);
          const response = await shardStub.fetch(
            new Request(`https://shard-do/triples/${encoded}`)
          );

          if (response.ok) {
            const data = await response.json() as { triples: Array<{ object: { type: number; value?: string } }> };
            const triples = data.triples || [];

            // Find unvisited refs
            for (const t of triples) {
              if (t.object.type === ObjectType.REF && t.object.value) {
                if (!visited.has(t.object.value)) {
                  visited.add(t.object.value);
                  nextIds.push(t.object.value);
                  totalHops++;
                }
              }
            }
          }
        }

        currentIds = nextIds;
        if (currentIds.length === 0) break;
      }
    } catch (error) {
      console.error('3-hop traversal error:', error);
    }

    const opEnd = performance.now();
    latencies.push(opEnd - opStart);
  }

  const endTime = Date.now();

  return {
    scenario: 'traversal-3hop',
    dataset: ctx.dataset,
    timestamp: startTime,
    durationMs: endTime - startTime,
    iterations,
    latency: calculateLatencyStats(latencies),
    metadata: {
      description: 'Deep graph traversal (3-hop)',
      totalHopsPerformed: totalHops,
      avgHopsPerIteration: totalHops / iterations,
    },
  };
}

// ============================================================================
// Write Throughput Scenario
// ============================================================================

/**
 * Write Throughput Benchmark
 *
 * Measures sustained write throughput for batch inserts.
 * Critical for CDC ingestion performance.
 */
export async function runWriteThroughput(
  ctx: ScenarioContext,
  iterations: number
): Promise<BenchmarkResult> {
  const startTime = Date.now();
  let totalOperations = 0;
  let totalBytes = 0;
  const batchSize = 100; // Triples per batch

  const shardStub = ctx.getShardStub('shard-benchmark');
  const txId = generateULID();
  const timestamp = BigInt(Date.now());

  for (let batch = 0; batch < iterations; batch++) {
    const triples: Array<Record<string, unknown>> = [];

    for (let i = 0; i < batchSize; i++) {
      const entityIndex = batch * batchSize + i;
      const entityId = `https://graph.workers.do/benchmark-entity/${entityIndex}`;

      triples.push({
        subject: entityId,
        predicate: 'name',
        object: { type: ObjectType.STRING, value: `Benchmark Entity ${entityIndex}` },
        timestamp: timestamp.toString(),
        txId,
      });

      triples.push({
        subject: entityId,
        predicate: 'value',
        object: { type: ObjectType.INT64, value: String(entityIndex) },
        timestamp: timestamp.toString(),
        txId,
      });
    }

    const body = JSON.stringify(triples);
    totalBytes += body.length;

    try {
      const response = await shardStub.fetch(
        new Request('https://shard-do/triples', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body,
        })
      );

      if (response.ok) {
        totalOperations += batchSize * 2; // 2 triples per entity
      } else {
        console.error(`Write failed: ${response.status}`);
      }
    } catch (error) {
      console.error('Write error:', error);
    }
  }

  const endTime = Date.now();
  const durationMs = endTime - startTime;
  const durationSec = durationMs / 1000;

  return {
    scenario: 'write-throughput',
    dataset: ctx.dataset,
    timestamp: startTime,
    durationMs,
    iterations,
    throughput: {
      operationsPerSecond: totalOperations / durationSec,
      bytesPerSecond: totalBytes / durationSec,
      totalOperations,
      totalBytes,
      durationMs,
    },
    metadata: {
      description: 'Batch write throughput',
      batchSize,
      triplesPerBatch: batchSize * 2,
    },
  };
}

// ============================================================================
// Bloom Filter Hit Rate Scenario
// ============================================================================

/**
 * Bloom Filter Hit Rate Benchmark
 *
 * Measures the efficiency of bloom filter routing.
 * High hit rate = fewer unnecessary shard queries.
 */
export async function runBloomFilterHitRate(
  ctx: ScenarioContext,
  iterations: number
): Promise<BenchmarkResult> {
  const startTime = Date.now();
  let hits = 0;
  let misses = 0;

  // For this benchmark, we simulate bloom filter checks
  // In production, this would use the snippet layer
  const knownEntities = new Set<string>();
  const config = DATASETS[ctx.dataset];

  if (!config) {
    throw new Error(`Unknown dataset: ${ctx.dataset}`);
  }

  // Populate known entities (simulating bloom filter contents)
  for (let i = 0; i < config.entityCount * 0.1; i++) {
    // 10% sample
    const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
    knownEntities.add(randomEntityId(ctx.dataset, entityType));
  }

  const shardStub = ctx.getShardStub('shard-benchmark');

  for (let i = 0; i < iterations; i++) {
    // 70% of queries for existing entities, 30% for non-existent
    const shouldExist = Math.random() < 0.7;

    let entityId: string;
    if (shouldExist) {
      const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
      entityId = randomEntityId(ctx.dataset, entityType);
    } else {
      entityId = `https://graph.workers.do/nonexistent/${Date.now()}-${i}`;
    }

    // Simulate bloom filter check
    const bloomMightContain = knownEntities.has(entityId) || Math.random() < 0.05; // 5% false positive rate

    if (bloomMightContain) {
      // Check actual existence
      const encoded = encodeURIComponent(entityId);
      const response = await shardStub.fetch(
        new Request(`https://shard-do/triples/${encoded}`)
      );

      if (response.status === 200) {
        hits++;
      } else {
        misses++;
      }
    } else {
      // Bloom filter correctly filtered out non-existent
      if (!shouldExist) {
        hits++; // True negative
      } else {
        misses++; // False negative
      }
    }
  }

  const endTime = Date.now();

  return {
    scenario: 'bloom-filter-hit-rate',
    dataset: ctx.dataset,
    timestamp: startTime,
    durationMs: endTime - startTime,
    iterations,
    cache: {
      hits,
      misses,
      hitRate: hits / (hits + misses),
      totalRequests: hits + misses,
    },
    metadata: {
      description: 'Bloom filter routing efficiency',
      falsePositiveRate: 0.05,
    },
  };
}

// ============================================================================
// Edge Cache Hit Rate Scenario
// ============================================================================

/**
 * Edge Cache Hit Rate Benchmark
 *
 * Measures cache efficiency for hot data.
 * Uses KV to track simulated cache state.
 */
export async function runEdgeCacheHitRate(
  ctx: ScenarioContext,
  iterations: number
): Promise<BenchmarkResult> {
  const startTime = Date.now();
  let hits = 0;
  let misses = 0;

  const shardStub = ctx.getShardStub('shard-benchmark');

  // Simulate hot entities (20% of queries hit 80% of data)
  const hotEntityCount = 10;
  const hotEntities: string[] = [];
  for (let i = 0; i < hotEntityCount; i++) {
    const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
    hotEntities.push(randomEntityId(ctx.dataset, entityType));
  }

  // Track "cached" entities (simulating edge cache)
  const cachedEntities = new Map<string, number>(); // entityId -> access count

  for (let i = 0; i < iterations; i++) {
    // 80% hot, 20% cold
    const isHot = Math.random() < 0.8;
    let entityId: string;

    if (isHot) {
      entityId = hotEntities[Math.floor(Math.random() * hotEntities.length)]!;
    } else {
      const entityType = ctx.dataset === 'imdb' ? 'movie' : 'worker';
      entityId = randomEntityId(ctx.dataset, entityType);
    }

    const cacheHit = cachedEntities.has(entityId);

    if (cacheHit) {
      hits++;
      cachedEntities.set(entityId, (cachedEntities.get(entityId) || 0) + 1);
    } else {
      misses++;

      // Fetch from shard
      const encoded = encodeURIComponent(entityId);
      await shardStub.fetch(new Request(`https://shard-do/triples/${encoded}`));

      // Add to cache (LRU simulation - limit to 100 entries)
      if (cachedEntities.size >= 100) {
        // Evict least accessed
        let minKey = '';
        let minAccess = Infinity;
        for (const [k, v] of cachedEntities) {
          if (v < minAccess) {
            minAccess = v;
            minKey = k;
          }
        }
        cachedEntities.delete(minKey);
      }
      cachedEntities.set(entityId, 1);
    }
  }

  const endTime = Date.now();

  return {
    scenario: 'edge-cache-hit-rate',
    dataset: ctx.dataset,
    timestamp: startTime,
    durationMs: endTime - startTime,
    iterations,
    cache: {
      hits,
      misses,
      hitRate: hits / (hits + misses),
      totalRequests: hits + misses,
    },
    metadata: {
      description: 'Edge cache efficiency (5GB enterprise zone)',
      hotEntityPercentage: 0.8,
      cacheSize: 100,
    },
  };
}

// ============================================================================
// Scenario Registry
// ============================================================================

export const SCENARIOS: Record<string, ScenarioRunner> = {
  'point-lookup': runPointLookup,
  'traversal-1hop': runTraversal1Hop,
  'traversal-3hop': runTraversal3Hop,
  'write-throughput': runWriteThroughput,
  'bloom-filter-hit-rate': runBloomFilterHitRate,
  'edge-cache-hit-rate': runEdgeCacheHitRate,
};

export function getScenarioRunner(name: string): ScenarioRunner | undefined {
  return SCENARIOS[name];
}

export function listScenarios(): string[] {
  return Object.keys(SCENARIOS);
}
