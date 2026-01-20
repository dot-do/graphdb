/**
 * Benchmark Worker Endpoints for GraphDB
 *
 * Provides HTTP endpoints for running production benchmarks:
 * - POST /benchmark/seed - Seed test data
 * - POST /benchmark/run/:scenario - Run specific benchmark
 * - GET /benchmark/results - Get latest results
 * - GET /benchmark/scenarios - List available scenarios
 */

import type { Env } from '../core/index.js';
import {
  DATASETS,
  getDatasetGenerator,
  estimateTripleCount,
} from './datasets.js';
import {
  SCENARIOS,
  listScenarios,
  getScenarioRunner,
  type BenchmarkResult,
  type ScenarioContext,
} from './scenarios.js';
import type { TypedObject } from '../core/triple.js';
import { typedObjectToJson } from '../core/type-converters.js';

// ============================================================================
// Benchmark State (stored in KV)
// ============================================================================

interface BenchmarkState {
  lastSeededDataset: string | null;
  seedTimestamp: number | null;
  tripleCount: number;
  results: BenchmarkResult[];
}

const BENCHMARK_STATE_KEY = 'benchmark:state';
const MAX_STORED_RESULTS = 100;

async function getBenchmarkState(kv: KVNamespace): Promise<BenchmarkState> {
  const stored = await kv.get(BENCHMARK_STATE_KEY, 'json');
  if (stored) {
    return stored as BenchmarkState;
  }
  return {
    lastSeededDataset: null,
    seedTimestamp: null,
    tripleCount: 0,
    results: [],
  };
}

async function saveBenchmarkState(
  kv: KVNamespace,
  state: BenchmarkState
): Promise<void> {
  // Trim results to max
  if (state.results.length > MAX_STORED_RESULTS) {
    state.results = state.results.slice(-MAX_STORED_RESULTS);
  }
  await kv.put(BENCHMARK_STATE_KEY, JSON.stringify(state));
}

// ============================================================================
// Benchmark Router
// ============================================================================

export async function handleBenchmarkRequest(
  request: Request,
  env: Env,
  pathname: string
): Promise<Response> {
  const method = request.method;

  // GET /benchmark/scenarios - List available scenarios
  if (pathname === '/benchmark/scenarios' && method === 'GET') {
    return new Response(
      JSON.stringify({
        scenarios: listScenarios(),
        datasets: Object.keys(DATASETS),
        datasetDetails: Object.entries(DATASETS).map(([name, config]) => ({
          name,
          entityCount: config.entityCount,
          estimatedTriples: estimateTripleCount(name),
          estimatedSizeMB: config.estimatedSizeMB,
        })),
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  // GET /benchmark/results - Get stored results
  if (pathname === '/benchmark/results' && method === 'GET') {
    const state = await getBenchmarkState(env.CACHE_META);
    return new Response(
      JSON.stringify({
        state: {
          lastSeededDataset: state.lastSeededDataset,
          seedTimestamp: state.seedTimestamp,
          tripleCount: state.tripleCount,
        },
        results: state.results,
      }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }

  // POST /benchmark/seed - Seed test data
  if (pathname === '/benchmark/seed' && method === 'POST') {
    return handleSeed(request, env);
  }

  // POST /benchmark/run/:scenario - Run benchmark
  const runMatch = pathname.match(/^\/benchmark\/run\/([a-z0-9-]+)$/);
  if (runMatch && method === 'POST') {
    const scenario = runMatch[1]!;
    return handleRun(request, env, scenario);
  }

  // POST /benchmark/run-all - Run all benchmarks
  if (pathname === '/benchmark/run-all' && method === 'POST') {
    return handleRunAll(request, env);
  }

  // DELETE /benchmark/reset - Reset benchmark data
  if (pathname === '/benchmark/reset' && method === 'DELETE') {
    return handleReset(env);
  }

  return new Response('Not Found', { status: 404 });
}

// ============================================================================
// Seed Handler
// ============================================================================

async function handleSeed(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const datasetName = url.searchParams.get('dataset') || 'small';

  if (!DATASETS[datasetName]) {
    return new Response(
      JSON.stringify({
        error: `Unknown dataset: ${datasetName}`,
        available: Object.keys(DATASETS),
      }),
      { status: 400, headers: { 'Content-Type': 'application/json' } }
    );
  }

  const startTime = Date.now();
  const shardStub = env.SHARD.get(env.SHARD.idFromName('shard-benchmark'));

  let totalTriples = 0;
  let batchCount = 0;

  try {
    const generator = getDatasetGenerator(datasetName);

    for (const batch of generator) {
      // Convert Triple objects to JSON-safe format
      const jsonBatch = batch.map((triple) => ({
        subject: triple.subject,
        predicate: triple.predicate,
        object: tripleObjectToJson(triple.object),
        timestamp: triple.timestamp.toString(),
        txId: triple.txId,
      }));

      const response = await shardStub.fetch(
        new Request('https://shard-do/triples', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(jsonBatch),
        })
      );

      if (!response.ok) {
        const error = await response.text();
        console.error(`Seed batch ${batchCount} failed: ${error}`);
      } else {
        totalTriples += batch.length;
        batchCount++;
      }

      // Log progress every 10 batches
      if (batchCount % 10 === 0) {
        console.log(`Seeded ${totalTriples} triples (${batchCount} batches)`);
      }
    }
  } catch (error) {
    return new Response(
      JSON.stringify({
        error: `Seed failed: ${error instanceof Error ? error.message : String(error)}`,
        triplesSeeded: totalTriples,
      }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }

  const durationMs = Date.now() - startTime;

  // Update state
  const state = await getBenchmarkState(env.CACHE_META);
  state.lastSeededDataset = datasetName;
  state.seedTimestamp = startTime;
  state.tripleCount = totalTriples;
  await saveBenchmarkState(env.CACHE_META, state);

  return new Response(
    JSON.stringify({
      success: true,
      dataset: datasetName,
      tripleCount: totalTriples,
      batchCount,
      durationMs,
      triplesPerSecond: Math.round((totalTriples / durationMs) * 1000),
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
}

// Helper to convert TypedObject to JSON-safe format
// Uses consolidated type-converters module
function tripleObjectToJson(obj: TypedObject): Record<string, unknown> {
  return typedObjectToJson(obj) as unknown as Record<string, unknown>;
}

// ============================================================================
// Run Handler
// ============================================================================

async function handleRun(
  request: Request,
  env: Env,
  scenarioName: string
): Promise<Response> {
  const runner = getScenarioRunner(scenarioName);

  if (!runner) {
    return new Response(
      JSON.stringify({
        error: `Unknown scenario: ${scenarioName}`,
        available: listScenarios(),
      }),
      { status: 400, headers: { 'Content-Type': 'application/json' } }
    );
  }

  const url = new URL(request.url);
  const iterations = parseInt(url.searchParams.get('iterations') || '100', 10);
  const dataset = url.searchParams.get('dataset') || 'small';

  if (iterations < 1 || iterations > 10000) {
    return new Response(
      JSON.stringify({ error: 'Iterations must be between 1 and 10000' }),
      { status: 400, headers: { 'Content-Type': 'application/json' } }
    );
  }

  const ctx: ScenarioContext = {
    getShardStub: (shardId: string) => {
      return env.SHARD.get(env.SHARD.idFromName(shardId));
    },
    cacheKV: env.CACHE_META,
    lakehouse: env.LAKEHOUSE,
    dataset,
  };

  try {
    const result = await runner(ctx, iterations);

    // Store result
    const state = await getBenchmarkState(env.CACHE_META);
    state.results.push(result);
    await saveBenchmarkState(env.CACHE_META, state);

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    return new Response(
      JSON.stringify({
        error: `Benchmark failed: ${error instanceof Error ? error.message : String(error)}`,
        scenario: scenarioName,
      }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
}

// ============================================================================
// Run All Handler
// ============================================================================

async function handleRunAll(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const iterations = parseInt(url.searchParams.get('iterations') || '100', 10);
  const dataset = url.searchParams.get('dataset') || 'small';

  const ctx: ScenarioContext = {
    getShardStub: (shardId: string) => {
      return env.SHARD.get(env.SHARD.idFromName(shardId));
    },
    cacheKV: env.CACHE_META,
    lakehouse: env.LAKEHOUSE,
    dataset,
  };

  const results: BenchmarkResult[] = [];
  const errors: Array<{ scenario: string; error: string }> = [];

  for (const [name, runner] of Object.entries(SCENARIOS)) {
    try {
      console.log(`Running scenario: ${name}`);
      const result = await runner(ctx, iterations);
      results.push(result);
    } catch (error) {
      errors.push({
        scenario: name,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  // Store results
  const state = await getBenchmarkState(env.CACHE_META);
  state.results.push(...results);
  await saveBenchmarkState(env.CACHE_META, state);

  return new Response(
    JSON.stringify({
      completed: results.length,
      failed: errors.length,
      results,
      errors: errors.length > 0 ? errors : undefined,
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
}

// ============================================================================
// Reset Handler
// ============================================================================

async function handleReset(env: Env): Promise<Response> {
  // Reset shard data
  const shardStub = env.SHARD.get(env.SHARD.idFromName('shard-benchmark'));
  await shardStub.fetch(new Request('https://shard-do/reset'));

  // Reset benchmark state
  await env.CACHE_META.delete(BENCHMARK_STATE_KEY);

  return new Response(
    JSON.stringify({
      success: true,
      message: 'Benchmark data reset',
    }),
    { headers: { 'Content-Type': 'application/json' } }
  );
}
