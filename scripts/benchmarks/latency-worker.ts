/**
 * Internal Latency Benchmark Worker
 *
 * Measures latency from WITHIN Cloudflare's network:
 * - Worker → R2 latency (list, head, get)
 * - GraphCol decode time
 *
 * Deploy and call to get real internal latencies.
 */

interface Env {
  LAKEHOUSE: R2Bucket;
}

// R2 region mapping - colos that are "local" to each R2 region
const R2_REGIONS: Record<string, string[]> = {
  // Eastern North America
  ENAM: ['ORD', 'IAD', 'EWR', 'ATL', 'MIA', 'BOS', 'CLT', 'DTW', 'JFK', 'PHL', 'DFW', 'IAH', 'MSP'],
  // Western North America
  WNAM: ['LAX', 'SJC', 'SEA', 'DEN', 'PHX', 'SLC', 'PDX', 'LAS'],
  // Europe
  WEUR: ['AMS', 'LHR', 'FRA', 'CDG', 'DUB', 'MAD', 'MXP', 'MRS', 'BRU', 'VIE', 'ZRH', 'CPH', 'ARN', 'HEL', 'WAW', 'PRG'],
  // Asia Pacific
  APAC: ['SIN', 'HKG', 'NRT', 'KIX', 'ICN', 'SYD', 'MEL', 'BOM', 'DEL', 'BKK', 'TPE'],
};

function isColoInRegion(colo: string | undefined, region: string): boolean {
  if (!colo) return false;
  return R2_REGIONS[region]?.includes(colo) ?? false;
}

function getColoRegion(colo: string | undefined): string {
  if (!colo) return 'unknown';
  for (const [region, colos] of Object.entries(R2_REGIONS)) {
    if (colos.includes(colo)) return region;
  }
  return 'other';
}

// Estimated cross-region latency (one-way, ms)
const CROSS_REGION_LATENCY: Record<string, Record<string, number>> = {
  ENAM: { ENAM: 10, WNAM: 40, WEUR: 80, APAC: 150 },
  WNAM: { ENAM: 40, WNAM: 10, WEUR: 120, APAC: 100 },
  WEUR: { ENAM: 80, WNAM: 120, WEUR: 10, APAC: 150 },
  APAC: { ENAM: 150, WNAM: 100, WEUR: 150, APAC: 20 },
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const colo = (request as unknown as { cf?: { colo?: string } }).cf?.colo;

    if (url.pathname === '/benchmark' || url.pathname === '/benchmark/r2') {
      return benchmarkR2(env, colo);
    }

    if (url.pathname === '/benchmark/cache') {
      return benchmarkCache(env, request, colo);
    }

    if (url.pathname === '/benchmark/lookup') {
      return benchmarkLookup(env, request, colo);
    }

    if (url.pathname === '/benchmark/traversal') {
      const depth = parseInt(url.searchParams.get('depth') || '3');
      return benchmarkTraversal(env, request, colo, depth);
    }

    if (url.pathname === '/benchmark/chunk-size') {
      return benchmarkChunkSize(env, request, colo);
    }

    if (url.pathname === '/benchmark/region') {
      return benchmarkRegion(env, colo);
    }

    return new Response(JSON.stringify({
      endpoints: {
        '/benchmark': 'Run R2 benchmark suite',
        '/benchmark/cache': 'Test edge cache latency (warm vs cold)',
        '/benchmark/lookup': 'Test single ID lookup latency (bloom + chunk + decode)',
        '/benchmark/traversal?depth=N': 'Test N-hop graph traversal (default 3)',
        '/benchmark/chunk-size': 'Test cache latency vs chunk size (1MB to 100MB)',
        '/benchmark/region': 'Show colo + R2 region + latency analysis',
      },
      colo,
      r2Region: 'ENAM',
      isLocalToR2: isColoInRegion(colo, 'ENAM'),
      timestamp: new Date().toISOString(),
    }, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  },
};

async function benchmarkR2(env: Env, colo?: string): Promise<Response> {
  const results: Record<string, unknown> = {
    timestamp: new Date().toISOString(),
    colo,
    iterations: 20,
  };

  // List operation
  const listTimes: number[] = [];
  for (let i = 0; i < 20; i++) {
    const start = performance.now();
    await env.LAKEHOUSE.list({ prefix: 'datasets/', limit: 100 });
    listTimes.push(performance.now() - start);
  }
  results.list = formatStats(listTimes);

  // Head operation
  const headTimes: number[] = [];
  for (let i = 0; i < 20; i++) {
    const start = performance.now();
    await env.LAKEHOUSE.head('datasets/imdb/index.json');
    headTimes.push(performance.now() - start);
  }
  results.head = formatStats(headTimes);

  // Get operation (small file)
  const getTimes: number[] = [];
  for (let i = 0; i < 20; i++) {
    const start = performance.now();
    const obj = await env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (obj) await obj.text(); // Consume body
    getTimes.push(performance.now() - start);
  }
  results.getSmall = formatStats(getTimes);

  // Get operation (chunk ~500KB)
  const chunkList = await env.LAKEHOUSE.list({ prefix: '.com/.imdb/', limit: 1 });
  if (chunkList.objects.length > 0) {
    const chunkKey = chunkList.objects[0].key;
    const chunkSize = chunkList.objects[0].size;
    const chunkTimes: number[] = [];
    for (let i = 0; i < 10; i++) {
      const start = performance.now();
      const obj = await env.LAKEHOUSE.get(chunkKey);
      if (obj) await obj.arrayBuffer(); // Consume body
      chunkTimes.push(performance.now() - start);
    }
    results.getChunk = { ...formatStats(chunkTimes), sizeKB: Math.round(chunkSize / 1024) };
  }

  return new Response(JSON.stringify(results, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}

function formatStats(times: number[]): {
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
} {
  const sorted = [...times].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);

  return {
    min: Math.round(sorted[0] * 100) / 100,
    max: Math.round(sorted[sorted.length - 1] * 100) / 100,
    avg: Math.round((sum / sorted.length) * 100) / 100,
    p50: Math.round(sorted[Math.floor(sorted.length * 0.5)] * 100) / 100,
    p95: Math.round(sorted[Math.floor(sorted.length * 0.95)] * 100) / 100,
    p99: Math.round(sorted[Math.floor(sorted.length * 0.99)] * 100) / 100,
  };
}

/**
 * Benchmark edge cache latency vs R2
 * Uses Cache API to store and retrieve data
 */
async function benchmarkCache(env: Env, request: Request, colo?: string): Promise<Response> {
  const cache = caches.default;
  const results: Record<string, unknown> = {
    timestamp: new Date().toISOString(),
    colo,
    iterations: 20,
  };

  // First, get a chunk from R2 to use as test data
  const obj = await env.LAKEHOUSE.get('datasets/imdb/index.json');
  if (!obj) {
    return new Response(JSON.stringify({ error: 'Test file not found' }), { status: 404 });
  }
  const testData = await obj.text();
  const testSize = new TextEncoder().encode(testData).length;

  // Create a cache key unique to this colo
  const cacheUrl = new URL(request.url);
  cacheUrl.pathname = `/cache-test-${colo}-${Date.now()}`;
  const cacheKey = new Request(cacheUrl.toString());

  // Warm the cache
  const warmResponse = new Response(testData, {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'max-age=60',
    },
  });
  await cache.put(cacheKey, warmResponse);

  // Measure cache read latency (warm)
  const cacheReadTimes: number[] = [];
  for (let i = 0; i < 20; i++) {
    const start = performance.now();
    const cached = await cache.match(cacheKey);
    if (cached) await cached.text(); // Consume body
    cacheReadTimes.push(performance.now() - start);
  }
  results.cacheRead = { ...formatStats(cacheReadTimes), sizeBytes: testSize };

  // Compare with R2 direct read
  const r2ReadTimes: number[] = [];
  for (let i = 0; i < 20; i++) {
    const start = performance.now();
    const r2Obj = await env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (r2Obj) await r2Obj.text();
    r2ReadTimes.push(performance.now() - start);
  }
  results.r2Read = { ...formatStats(r2ReadTimes), sizeBytes: testSize };

  // Cleanup
  await cache.delete(cacheKey);

  // Summary
  const cacheP50 = (results.cacheRead as { p50: number }).p50;
  const r2P50 = (results.r2Read as { p50: number }).p50;
  results.summary = {
    cacheP50_ms: cacheP50,
    r2P50_ms: r2P50,
    speedup: Math.round((r2P50 / cacheP50) * 10) / 10,
  };

  return new Response(JSON.stringify(results, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}

/**
 * Benchmark full ID lookup: bloom check → chunk fetch → decode
 * Simulates what a real entity lookup does
 */
async function benchmarkLookup(env: Env, request: Request, colo?: string): Promise<Response> {
  const cache = caches.default;
  const results: Record<string, unknown> = {
    timestamp: new Date().toISOString(),
    colo,
  };

  // Load manifest (simulates bloom filter routing index)
  const manifestStart = performance.now();
  const manifestObj = await env.LAKEHOUSE.get('datasets/imdb/index.json');
  if (!manifestObj) {
    return new Response(JSON.stringify({ error: 'Manifest not found' }), { status: 404 });
  }
  const manifest = await manifestObj.json() as { chunks?: string[] };
  results.manifestLoad_ms = Math.round((performance.now() - manifestStart) * 100) / 100;

  // Find a real chunk to test with
  const chunkList = await env.LAKEHOUSE.list({ prefix: 'datasets/imdb/chunks/', limit: 1 });
  if (chunkList.objects.length === 0) {
    // Try alternate prefix
    const altList = await env.LAKEHOUSE.list({ prefix: 'imdb/', limit: 5 });
    results.availablePrefixes = altList.objects.map(o => o.key);
    return new Response(JSON.stringify({ error: 'No chunks found', ...results }), { status: 404 });
  }

  const chunkKey = chunkList.objects[0].key;
  const chunkSize = chunkList.objects[0].size;

  // Benchmark: Cold lookup (R2 fetch)
  const coldTimes: number[] = [];
  for (let i = 0; i < 10; i++) {
    const start = performance.now();
    // Simulate full lookup: fetch chunk, decode, find entity
    const chunkObj = await env.LAKEHOUSE.get(chunkKey);
    if (chunkObj) {
      const data = await chunkObj.arrayBuffer();
      // Simulate decode time (~2-5ms for 500KB chunk)
      const decodeStart = performance.now();
      const view = new Uint8Array(data);
      let checksum = 0;
      for (let j = 0; j < Math.min(view.length, 10000); j++) {
        checksum += view[j];
      }
      const decodeTime = performance.now() - decodeStart;
      if (i === 0) results.decodeTime_ms = Math.round(decodeTime * 100) / 100;
    }
    coldTimes.push(performance.now() - start);
  }
  results.coldLookup = { ...formatStats(coldTimes), chunkSizeKB: Math.round(chunkSize / 1024) };

  // Warm the cache
  const cacheUrl = new URL(request.url);
  cacheUrl.pathname = `/chunk-cache-${colo}`;
  const cacheKey = new Request(cacheUrl.toString());

  const chunkForCache = await env.LAKEHOUSE.get(chunkKey);
  if (chunkForCache) {
    const cacheResponse = new Response(chunkForCache.body, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Cache-Control': 'max-age=3600',
      },
    });
    await cache.put(cacheKey, cacheResponse);
  }

  // Benchmark: Warm lookup (edge cache)
  const warmTimes: number[] = [];
  for (let i = 0; i < 20; i++) {
    const start = performance.now();
    const cached = await cache.match(cacheKey);
    if (cached) {
      const data = await cached.arrayBuffer();
      // Simulate decode
      const view = new Uint8Array(data);
      let checksum = 0;
      for (let j = 0; j < Math.min(view.length, 10000); j++) {
        checksum += view[j];
      }
    }
    warmTimes.push(performance.now() - start);
  }
  results.warmLookup = { ...formatStats(warmTimes), chunkSizeKB: Math.round(chunkSize / 1024) };

  // Cleanup
  await cache.delete(cacheKey);

  // Summary for single ID lookup
  const coldP50 = (results.coldLookup as { p50: number }).p50;
  const warmP50 = (results.warmLookup as { p50: number }).p50;
  results.summary = {
    coldLookup_p50_ms: coldP50,
    warmLookup_p50_ms: warmP50,
    speedup: Math.round((coldP50 / warmP50) * 10) / 10,
    note: 'Warm = edge cache hit, Cold = R2 fetch',
  };

  return new Response(JSON.stringify(results, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}

/**
 * Benchmark cache latency vs chunk size
 * Tests how edge cache scales from 1MB to 100MB+
 */
async function benchmarkChunkSize(
  env: Env,
  request: Request,
  colo: string | undefined
): Promise<Response> {
  const cache = caches.default;
  const results: Record<string, unknown> = {
    timestamp: new Date().toISOString(),
    colo,
  };

  // Test sizes: 1MB, 10MB, 50MB, 100MB
  // Note: We'll create synthetic data since we may not have R2 objects of each size
  const sizes = [
    { name: '1MB', bytes: 1 * 1024 * 1024 },
    { name: '10MB', bytes: 10 * 1024 * 1024 },
    { name: '50MB', bytes: 50 * 1024 * 1024 },
    { name: '100MB', bytes: 100 * 1024 * 1024 },
  ];

  const sizeResults: Record<string, unknown>[] = [];

  for (const size of sizes) {
    // Create synthetic data
    const data = new Uint8Array(size.bytes);
    // Fill with pattern (simulates real data)
    for (let i = 0; i < data.length; i += 1024) {
      data[i] = i % 256;
    }

    // Create cache key
    const cacheUrl = new URL(request.url);
    cacheUrl.pathname = `/chunk-size-test-${colo}-${size.name}`;
    const cacheKey = new Request(cacheUrl.toString());

    // Store in cache
    const storeStart = performance.now();
    const cacheResponse = new Response(data, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Cache-Control': 'max-age=60',
      },
    });
    await cache.put(cacheKey, cacheResponse);
    const storeTime = performance.now() - storeStart;

    // Measure cache read latency (multiple iterations)
    const readTimes: number[] = [];
    for (let i = 0; i < 5; i++) {
      const start = performance.now();
      const cached = await cache.match(cacheKey);
      if (cached) {
        await cached.arrayBuffer(); // Consume full body
      }
      readTimes.push(performance.now() - start);
    }

    // Measure decode simulation (scan through data)
    const decodeStart = performance.now();
    let checksum = 0;
    for (let i = 0; i < Math.min(data.length, 100000); i++) {
      checksum += data[i];
    }
    const decodeTime = performance.now() - decodeStart;

    // Cleanup
    await cache.delete(cacheKey);

    sizeResults.push({
      size: size.name,
      bytes: size.bytes,
      storeTime_ms: Math.round(storeTime),
      read: formatStats(readTimes),
      decodeTime_ms: Math.round(decodeTime * 100) / 100,
      // Estimate 4-hop traversal if all in this chunk
      estimated4HopSameChunk_ms: Math.round(formatStats(readTimes).p50 + decodeTime * 4),
    });
  }

  results.sizes = sizeResults;

  // Summary comparison
  const s1MB = sizeResults.find(s => s.size === '1MB') as { read: { p50: number } };
  const s100MB = sizeResults.find(s => s.size === '100MB') as { read: { p50: number } };

  results.summary = {
    cache_1MB_p50_ms: s1MB?.read.p50,
    cache_100MB_p50_ms: s100MB?.read.p50,
    ratio_100MB_vs_1MB: Math.round((s100MB?.read.p50 / s1MB?.read.p50) * 10) / 10,
    insight: 'Larger chunks = higher single-fetch cost, but amortized over many entities',
  };

  // Traversal comparison
  results.traversalComparison = {
    scenario: '4-hop traversal, all entities in same chunk',
    with1MBChunks: {
      fetches: 4,
      estimated_ms: '80-120ms (4 × cache fetch)',
    },
    with100MBChunk: {
      fetches: 1,
      estimated_ms: `${Math.round(s100MB?.read.p50 + 20)}ms (1 fetch + 4 decodes)`,
    },
  };

  return new Response(JSON.stringify(results, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}

/**
 * Benchmark N-hop graph traversal
 *
 * Simulates real traversal patterns:
 * - Best case: All hops in same cached chunk
 * - Worst case: Each hop hits different chunk via R2
 * - Mixed: Some cache hits, some R2 fetches
 */
async function benchmarkTraversal(
  env: Env,
  request: Request,
  colo: string | undefined,
  depth: number
): Promise<Response> {
  const cache = caches.default;
  const results: Record<string, unknown> = {
    timestamp: new Date().toISOString(),
    colo,
    depth,
  };

  // Get list of available chunks
  const chunkList = await env.LAKEHOUSE.list({ prefix: 'datasets/imdb/chunks/', limit: 10 });
  if (chunkList.objects.length < depth) {
    return new Response(JSON.stringify({
      error: `Need at least ${depth} chunks for ${depth}-hop traversal, found ${chunkList.objects.length}`,
      availableChunks: chunkList.objects.map(o => o.key),
    }), { status: 400 });
  }

  const chunks = chunkList.objects.slice(0, depth);
  const chunkSizes = chunks.map(c => c.size);
  const avgChunkSize = Math.round(chunkSizes.reduce((a, b) => a + b, 0) / chunks.length / 1024);

  results.chunksUsed = chunks.length;
  results.avgChunkSizeKB = avgChunkSize;

  // Warm the cache with all chunks
  const cacheKeys: Request[] = [];
  for (let i = 0; i < chunks.length; i++) {
    const cacheUrl = new URL(request.url);
    cacheUrl.pathname = `/traversal-cache-${colo}-${i}`;
    cacheKeys.push(new Request(cacheUrl.toString()));

    const chunkObj = await env.LAKEHOUSE.get(chunks[i].key);
    if (chunkObj) {
      const cacheResponse = new Response(chunkObj.body, {
        headers: {
          'Content-Type': 'application/octet-stream',
          'Cache-Control': 'max-age=3600',
        },
      });
      await cache.put(cacheKeys[i], cacheResponse);
    }
  }

  // Benchmark: Cold traversal (all R2 fetches - worst case)
  const coldTimes: number[] = [];
  const coldHopTimes: number[][] = Array.from({ length: depth }, () => []);

  for (let iter = 0; iter < 5; iter++) {
    const traversalStart = performance.now();
    for (let hop = 0; hop < depth; hop++) {
      const hopStart = performance.now();
      const chunkObj = await env.LAKEHOUSE.get(chunks[hop].key);
      if (chunkObj) {
        const data = await chunkObj.arrayBuffer();
        // Simulate decode + entity lookup
        const view = new Uint8Array(data);
        let checksum = 0;
        for (let j = 0; j < Math.min(view.length, 10000); j++) {
          checksum += view[j];
        }
      }
      coldHopTimes[hop].push(performance.now() - hopStart);
    }
    coldTimes.push(performance.now() - traversalStart);
  }

  results.coldTraversal = {
    total: formatStats(coldTimes),
    perHop: coldHopTimes.map((times, i) => ({
      hop: i + 1,
      ...formatStats(times),
    })),
    scenario: 'All R2 fetches (worst case - each hop hits different shard)',
  };

  // Benchmark: Warm traversal (all cache hits - best realistic case)
  const warmTimes: number[] = [];
  const warmHopTimes: number[][] = Array.from({ length: depth }, () => []);

  for (let iter = 0; iter < 10; iter++) {
    const traversalStart = performance.now();
    for (let hop = 0; hop < depth; hop++) {
      const hopStart = performance.now();
      const cached = await cache.match(cacheKeys[hop]);
      if (cached) {
        const data = await cached.arrayBuffer();
        // Simulate decode + entity lookup
        const view = new Uint8Array(data);
        let checksum = 0;
        for (let j = 0; j < Math.min(view.length, 10000); j++) {
          checksum += view[j];
        }
      }
      warmHopTimes[hop].push(performance.now() - hopStart);
    }
    warmTimes.push(performance.now() - traversalStart);
  }

  results.warmTraversal = {
    total: formatStats(warmTimes),
    perHop: warmHopTimes.map((times, i) => ({
      hop: i + 1,
      ...formatStats(times),
    })),
    scenario: 'All cache hits (best case - popular entities cached at edge)',
  };

  // Benchmark: Same chunk traversal (all hops in one chunk - ideal case)
  const sameChunkTimes: number[] = [];
  const singleCacheKey = cacheKeys[0];

  for (let iter = 0; iter < 10; iter++) {
    const traversalStart = performance.now();
    // Fetch chunk once
    const cached = await cache.match(singleCacheKey);
    if (cached) {
      const data = await cached.arrayBuffer();
      const view = new Uint8Array(data);
      // Simulate N hops within same chunk (just decode operations)
      for (let hop = 0; hop < depth; hop++) {
        let checksum = 0;
        const offset = hop * 1000;
        for (let j = offset; j < Math.min(offset + 10000, view.length); j++) {
          checksum += view[j];
        }
      }
    }
    sameChunkTimes.push(performance.now() - traversalStart);
  }

  results.sameChunkTraversal = {
    total: formatStats(sameChunkTimes),
    scenario: 'All hops in same cached chunk (ideal - dense local subgraph)',
  };

  // Cleanup
  for (const key of cacheKeys) {
    await cache.delete(key);
  }

  // Summary
  const coldP50 = (results.coldTraversal as { total: { p50: number } }).total.p50;
  const warmP50 = (results.warmTraversal as { total: { p50: number } }).total.p50;
  const sameP50 = (results.sameChunkTraversal as { total: { p50: number } }).total.p50;

  results.summary = {
    depth,
    coldTraversal_p50_ms: coldP50,
    warmTraversal_p50_ms: warmP50,
    sameChunkTraversal_p50_ms: sameP50,
    perHop_cold_ms: Math.round(coldP50 / depth),
    perHop_warm_ms: Math.round(warmP50 / depth),
    speedup_warm_vs_cold: Math.round((coldP50 / warmP50) * 10) / 10,
    note: 'Cold=R2, Warm=edge cache, SameChunk=single fetch + N decodes',
  };

  return new Response(JSON.stringify(results, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}

/**
 * Benchmark showing region impact on traversal latency
 * Demonstrates why executing traversals from R2 colo matters
 */
async function benchmarkRegion(env: Env, colo: string | undefined): Promise<Response> {
  const r2Region = 'ENAM'; // Our bucket is in Eastern North America
  const workerRegion = getColoRegion(colo);
  const isLocal = isColoInRegion(colo, r2Region);

  // Measure actual R2 latency from this colo
  const r2Times: number[] = [];
  for (let i = 0; i < 10; i++) {
    const start = performance.now();
    const obj = await env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (obj) await obj.text();
    r2Times.push(performance.now() - start);
  }
  const r2Stats = formatStats(r2Times);

  // Estimate cross-region penalty
  const crossRegionPenalty = CROSS_REGION_LATENCY[r2Region]?.[workerRegion] ?? 100;

  const results = {
    timestamp: new Date().toISOString(),
    worker: {
      colo,
      region: workerRegion,
    },
    r2Bucket: {
      name: 'graphdb-lakehouse-prod',
      region: r2Region,
    },
    isLocalToR2: isLocal,
    measuredR2Latency: r2Stats,

    // Traversal estimates by region
    traversalEstimates: {
      description: '4-hop traversal, each hop = different chunk (worst case)',
      fromLocalColo: {
        region: r2Region,
        perHopR2_ms: r2Stats.p50,
        total4Hop_ms: Math.round(r2Stats.p50 * 4),
        note: 'Execute traversal from R2 region for best latency',
      },
      fromWNAM: {
        region: 'WNAM',
        perHopR2_ms: r2Stats.p50 + CROSS_REGION_LATENCY[r2Region].WNAM,
        total4Hop_ms: Math.round((r2Stats.p50 + CROSS_REGION_LATENCY[r2Region].WNAM) * 4),
        penalty_ms: CROSS_REGION_LATENCY[r2Region].WNAM,
      },
      fromWEUR: {
        region: 'WEUR',
        perHopR2_ms: r2Stats.p50 + CROSS_REGION_LATENCY[r2Region].WEUR,
        total4Hop_ms: Math.round((r2Stats.p50 + CROSS_REGION_LATENCY[r2Region].WEUR) * 4),
        penalty_ms: CROSS_REGION_LATENCY[r2Region].WEUR,
      },
      fromAPAC: {
        region: 'APAC',
        perHopR2_ms: r2Stats.p50 + CROSS_REGION_LATENCY[r2Region].APAC,
        total4Hop_ms: Math.round((r2Stats.p50 + CROSS_REGION_LATENCY[r2Region].APAC) * 4),
        penalty_ms: CROSS_REGION_LATENCY[r2Region].APAC,
      },
    },

    architecture: {
      recommendation: 'Route traversal queries to worker in R2 region',
      pattern: {
        step1: 'User request hits edge worker (any colo)',
        step2: 'Edge worker checks cache for result',
        step3: 'On cache miss, route to R2-region worker via Service Binding',
        step4: 'R2-region worker executes traversal locally (~' + Math.round(r2Stats.p50 * 4) + 'ms)',
        step5: 'Result cached at edge, returned to user',
      },
      benefit: 'Consistent ' + Math.round(r2Stats.p50 * 4) + 'ms traversal regardless of user location',
      alternative: 'Use Smart Placement to hint worker should run near R2',
    },
  };

  return new Response(JSON.stringify(results, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}
