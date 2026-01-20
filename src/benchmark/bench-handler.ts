/**
 * Bench Handler - JSONBench-compatible endpoint
 *
 * Handles the /bench endpoint for in-memory triple store benchmarking.
 * This provides a simple, jsonbench-compatible interface for quick benchmarks.
 *
 * GET /bench?dataset=test&rows=1000
 */

import { generateULID } from './datasets.js';
import {
  InMemoryTripleStore,
  rowToTriples,
  executeBenchQuery,
  generateTestData,
  BENCH_QUERIES,
} from './in-memory-store.js';

/**
 * CORS headers for /bench endpoint
 */
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

/**
 * Handle CORS preflight requests
 */
export function handleBenchCors(): Response {
  return new Response(null, { headers: corsHeaders });
}

/**
 * Handle /bench endpoint - jsonbench-compatible
 *
 * @param url - The request URL with query parameters
 * @returns Response with benchmark results
 */
export async function handleBenchEndpoint(url: URL): Promise<Response> {
  const requestStart = Date.now();

  const datasetId = url.searchParams.get('dataset') ?? 'test';
  const rows = parseInt(url.searchParams.get('rows') ?? '1000', 10);

  // Limit rows for safety
  const safeRows = Math.min(rows, 50000);

  // Get queries for this dataset
  const queries = BENCH_QUERIES[datasetId] ?? BENCH_QUERIES['test'];

  try {
    // 1. Initialize triple store
    const initStart = Date.now();
    const store = new InMemoryTripleStore();
    const initMs = Date.now() - initStart;

    // 2. Generate and load data
    const loadStart = Date.now();
    const data = generateTestData(safeRows);

    const txId = generateULID();
    const timestamp = BigInt(Date.now());
    let triplesInserted = 0;

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      if (!row) continue;
      const triples = rowToTriples(row, datasetId, i, txId, timestamp);
      for (const triple of triples) {
        store.insert(triple);
        triplesInserted++;
      }
    }
    const loadDataMs = Date.now() - loadStart;

    // 3. Execute all queries
    const queryResults: Array<{
      id: string;
      name: string;
      queryType: string;
      queryMs: number;
      rowCount: number;
    }> = [];

    const queryPhaseStart = Date.now();
    if (queries) {
      for (const query of queries) {
        const queryStart = Date.now();
        const result = executeBenchQuery(store, query, datasetId);
        const queryMs = Date.now() - queryStart;

        queryResults.push({
          id: query.id,
          name: query.name,
          queryType: query.type,
          queryMs,
          rowCount: result.rowCount,
        });
      }
    }
    const queryPhaseMs = Date.now() - queryPhaseStart;

    const requestTimeMs = Date.now() - requestStart;

    // If individual query times are all 0, distribute query phase time
    const totalMeasuredQueryMs = queryResults.reduce((sum, q) => sum + q.queryMs, 0);
    if (totalMeasuredQueryMs === 0 && queryResults.length > 0 && queryPhaseMs > 0) {
      const perQueryMs = queryPhaseMs / queryResults.length;
      for (const q of queryResults) {
        q.queryMs = perQueryMs;
      }
    }

    const avgQueryMs = queryResults.length > 0
      ? queryResults.reduce((sum, q) => sum + q.queryMs, 0) / queryResults.length
      : 0;

    // Build jsonbench-compatible response
    // Ensure we have at least 1ms for calculations to avoid division by zero
    const effectiveRequestTimeMs = Math.max(requestTimeMs, 1);
    const effectiveLoadDataMs = Math.max(loadDataMs, 1);

    const response = {
      engine: 'graphdb',
      dataset: datasetId,
      rowCount: safeRows,
      timing: {
        initMs,
        loadDataMs,
        totalMs: requestTimeMs,
      },
      queries: queryResults,
      summary: {
        totalQueries: queryResults.length,
        avgQueryMs,
        fastestQuery: queryResults.length > 0 ? queryResults.reduce((min, q) => q.queryMs < min.queryMs ? q : min) : null,
        slowestQuery: queryResults.length > 0 ? queryResults.reduce((max, q) => q.queryMs > max.queryMs ? q : max) : null,
      },
      requestTimeMs,
      throughput: {
        rowsPerSecond: Math.round((safeRows / effectiveRequestTimeMs) * 1000),
        queriesPerSecond: Math.round((queryResults.length / effectiveRequestTimeMs) * 1000),
        triplesPerSecond: Math.round((triplesInserted / effectiveLoadDataMs) * 1000),
      },
      dataStats: {
        rowsGenerated: data.length,
        triplesInserted,
        entityCount: store.entityCount(),
      },
    };

    return new Response(JSON.stringify(response), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  } catch (err) {
    return new Response(
      JSON.stringify({
        error: `GraphDB benchmark error: ${err instanceof Error ? err.message : 'Unknown error'}`,
      }),
      { status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
    );
  }
}
