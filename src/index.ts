/**
 * GraphDB - Cost-optimized graph database for Cloudflare Workers
 *
 * Architecture:
 * - Snippets (FREE): Bloom routing, query parsing, shard routing
 * - Edge Cache (FREE): Index segments, geohash cells
 * - Broker DO (95% discount): Hibernating WebSocket, capnweb RPC
 * - Shard DO: SQLite triples with typed object columns
 * - R2: CDC streaming, GraphCol format, tiered compaction
 *
 * Worker Entry Point:
 * - /connect -> BrokerDO for WebSocket upgrade (hibernation-enabled)
 * - /shard/* -> ShardDO endpoints
 * - /broker/* -> BrokerDO HTTP endpoints
 * - /benchmark/* -> Benchmark endpoints (production metrics)
 */

// ============================================================================
// Durable Object class exports (required for Wrangler)
// ============================================================================

/**
 * Broker Durable Object - handles WebSocket connections with hibernation support.
 * Provides 95% cost savings via hibernation while maintaining fresh subrequest quotas.
 * @see {@link https://developers.cloudflare.com/durable-objects/api/hibernatable-websockets/ | Hibernatable WebSockets}
 */
export { BrokerDO } from './broker/index.js';

/**
 * Shard Durable Object - SQLite triple storage with typed object columns.
 * Stores graph triples using columnar storage for efficient querying.
 */
export { ShardDO } from './shard/index.js';

/**
 * Coordinator Durable Object - manages distributed coordination.
 * CDCCoordinatorDO handles Change Data Capture streaming to R2.
 */
export { CoordinatorDO, CDCCoordinatorDO } from './coordinator/index.js';

/**
 * Traversal Durable Object - region-optimized graph traversal.
 * Deployed at R2-local colos for low-latency data access.
 */
export { TraversalDO } from './traversal/index.js';

// ============================================================================
// Type-only re-exports (safe for worker entry point)
// ============================================================================
export type { WebSocketAttachment, SubrequestBatchResult, BrokerMetrics } from './broker/index.js';
export type { ShardStats } from './shard/index.js';
export type { CoordinatorStats } from './coordinator/index.js';
export type { BenchmarkResult, LatencyStats, ThroughputStats, CacheStats } from './benchmark/index.js';
export type { TraversalEnv, TraversalApi, TraversalEntity, DOEntity, Edge, TraversalResult } from './traversal/index.js';

// ============================================================================
// Import module exports (streaming data ingestion)
// ============================================================================

/**
 * Create a streaming line reader for memory-efficient text processing.
 * Processes data chunks without loading entire files into memory.
 * @see {@link StreamingLineReaderOptions} for configuration options
 */
export { createStreamingLineReader } from './import/index.js';

/**
 * Create a batched triple writer that buffers triples before flushing.
 * Batches up to 10K triples before writing to improve throughput.
 * @see {@link BatchedTripleWriterOptions} for configuration options
 */
export { createBatchedTripleWriter } from './import/index.js';

/**
 * Create a resumable import state manager for checkpoint-based imports.
 * Enables recovery from failures by persisting progress to DO storage.
 * @see {@link ImportCheckpoint} for checkpoint structure
 */
export { createResumableImportState } from './import/index.js';

/**
 * Create a range fetcher for HTTP Range requests on large files.
 * Enables partial downloads for efficient large file processing.
 * @see {@link RangeFetcherOptions} for configuration options
 */
export { createRangeFetcher } from './import/index.js';
export type {
  StreamingLineReader,
  StreamingLineReaderOptions,
  LineReaderState,
  BatchedTripleWriter,
  BatchedTripleWriterOptions,
  BatchWriterState,
  WriterResult,
  ImportChunkInfo,
  ResumableImportState,
  ImportCheckpoint,
  RangeFetcher,
  RangeFetcherOptions,
  RangeFetchResult,
} from './import/index.js';

// ============================================================================
// Cache module exports (edge cache integration)
// ============================================================================

/**
 * Edge cache for GraphCol chunks - manages caching of columnar data segments.
 * Provides automatic TTL management and cache invalidation.
 */
export { ChunkEdgeCache } from './cache/index.js';

/**
 * Cache invalidator for managing cache purges on data updates.
 * Supports tag-based invalidation for efficient cache management.
 */
export { CacheInvalidator } from './cache/index.js';

/**
 * Metrics collector for cache performance monitoring.
 * Tracks hit rates, miss rates, and latency distributions.
 */
export { CacheMetricsCollector } from './cache/index.js';

// URL generation functions
export {
  /**
   * Generate a cache-friendly URL for a chunk.
   * @param namespace - The namespace of the chunk
   * @param chunkId - The unique chunk identifier
   * @returns A URL suitable for edge caching
   */
  generateChunkCacheUrl,
  /**
   * Generate a cache-friendly URL for a manifest.
   * @param namespace - The namespace of the manifest
   * @returns A URL suitable for edge caching with SWR
   */
  generateManifestCacheUrl,
  /**
   * Parse a chunk cache URL to extract namespace and chunk ID.
   * @param url - The cache URL to parse
   * @returns Parsed URL components
   */
  parseChunkCacheUrl,
} from './cache/index.js';

// Header generation functions
export {
  /**
   * Generate cache headers for chunk responses.
   * @param config - Cache configuration options
   * @returns Headers object with Cache-Control directives
   */
  generateChunkCacheHeaders,
  /**
   * Generate cache headers for manifest responses with SWR support.
   * @param config - Cache configuration options
   * @returns Headers object with Cache-Control and SWR directives
   */
  generateManifestCacheHeaders,
} from './cache/index.js';

// Tag generation functions
export {
  /**
   * Create a cache tag for a specific chunk.
   * @param namespace - The namespace
   * @param chunkId - The chunk identifier
   * @returns A cache tag string
   */
  createChunkTag,
  /**
   * Create cache tags for an entire namespace.
   * @param namespace - The namespace to tag
   * @returns Array of cache tags
   */
  createNamespaceTags,
  /**
   * Create invalidation tags for cache purging.
   * @param event - The event triggering invalidation
   * @returns Array of tags to invalidate
   */
  createInvalidationTags,
} from './cache/index.js';

// Metric utility functions
export {
  /**
   * Calculate cache hit rate from metrics.
   * @param metrics - Cache metrics snapshot
   * @returns Hit rate as a decimal (0-1)
   */
  calculateHitRate,
  /**
   * Calculate cache miss rate from metrics.
   * @param metrics - Cache metrics snapshot
   * @returns Miss rate as a decimal (0-1)
   */
  calculateMissRate,
  /**
   * Format metrics into a human-readable report.
   * @param metrics - Cache metrics snapshot
   * @returns Formatted report string
   */
  formatMetricsReport,
} from './cache/index.js';

// Constants
export {
  /** Default max-age for chunk cache entries (seconds) */
  DEFAULT_CHUNK_MAX_AGE,
  /** Default max-age for manifest cache entries (seconds) */
  DEFAULT_MANIFEST_MAX_AGE,
  /** Default stale-while-revalidate window for manifests (seconds) */
  DEFAULT_MANIFEST_SWR,
  /** Default metrics collection window (milliseconds) */
  METRICS_WINDOW_DEFAULT,
} from './cache/index.js';
export type {
  // Edge cache types
  ChunkCacheConfig,
  ManifestCacheConfig,
  CacheableChunk,
  CacheableManifest,
  CachedChunkResponse,
  CacheMetricsData,
  ParsedChunkCacheUrl,
  // Invalidation types
  InvalidationConfig,
  InvalidationResult,
  CompactionInvalidationEvent,
  NamespaceInvalidationOptions,
  InvalidationMetrics,
  // Metrics types
  MetricsConfig,
  HitOptions,
  MissOptions,
  InvalidationOptions,
  CacheMetrics,
  RequestMetric,
  MetricsSnapshot,
  SnapshotComparison,
} from './cache/index.js';

// Environment bindings interface (centralized in core to avoid circular deps)
export type { Env } from './core/index.js';
import type { Env } from './core/index.js';

// Import benchmark handlers
import { handleBenchmarkRequest, handleBenchEndpoint, handleBenchCors } from './benchmark/index.js';

/**
 * Worker fetch handler
 */
export default {
  async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // Handle CORS preflight for /bench endpoint
    if (request.method === 'OPTIONS' && url.pathname === '/bench') {
      return handleBenchCors();
    }

    // /bench endpoint - jsonbench-compatible
    if (url.pathname === '/bench') {
      return handleBenchEndpoint(url);
    }

    // Benchmark endpoints
    if (url.pathname.startsWith('/benchmark')) {
      return handleBenchmarkRequest(request, env, url.pathname);
    }

    // Health check
    if (url.pathname === '/health') {
      return new Response(
        JSON.stringify({
          status: 'healthy',
          timestamp: Date.now(),
          service: 'graphdb',
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // WebSocket connection endpoint
    if (url.pathname === '/connect' || url.pathname.startsWith('/connect/')) {
      // Extract broker ID from path or use default
      const brokerId = url.pathname.split('/')[2] || 'default';
      const id = env.BROKER.idFromName(brokerId);
      const stub = env.BROKER.get(id);
      return stub.fetch(request);
    }

    // Broker HTTP endpoints
    if (url.pathname.startsWith('/broker')) {
      const brokerId = url.pathname.split('/')[2] || 'default';
      const subPath = '/' + url.pathname.split('/').slice(3).join('/') || '/';

      const id = env.BROKER.idFromName(brokerId);
      const stub = env.BROKER.get(id);

      // Rewrite URL to just the sub-path
      const newUrl = new URL(request.url);
      newUrl.pathname = subPath || '/metrics';
      return stub.fetch(new Request(newUrl.toString(), request as RequestInit));
    }

    // Shard DO endpoints
    if (url.pathname.startsWith('/shard')) {
      const shardId = url.pathname.split('/')[2] || 'shard-node-1';
      const subPath = '/' + url.pathname.split('/').slice(3).join('/') || '/count';

      const id = env.SHARD.idFromName(shardId);
      const stub = env.SHARD.get(id);

      // Rewrite URL to just the sub-path
      const newUrl = new URL(request.url);
      newUrl.pathname = subPath;
      return stub.fetch(new Request(newUrl.toString(), request as RequestInit));
    }

    // =========================================================================
    // Bootstrap endpoints for colo.do DO placement
    // =========================================================================

    // R2-local colos in the ENAM region for bootstrap
    const R2_LOCAL_COLOS = ['ORD', 'IAD', 'EWR', 'ATL', 'MIA', 'BOS', 'DFW', 'IAH'] as const;
    type R2LocalColo = (typeof R2_LOCAL_COLOS)[number];

    // Get the current colo from the request
    const getRequestColo = (req: Request): string => {
      const cf = (req as Request & { cf?: { colo?: string } }).cf;
      return cf?.colo ?? 'UNKNOWN';
    };

    // /bootstrap - Create DO at current colo (use via colo.do)
    if (url.pathname === '/bootstrap') {
      const workerColo = getRequestColo(request);
      const doId = env.TRAVERSAL_DO.idFromName(workerColo);
      const doStub = env.TRAVERSAL_DO.get(doId);

      // Call DO's /init endpoint with bootstrap context
      const initBody = JSON.stringify({
        colo: workerColo,
        bootstrapTimestamp: Date.now(),
        bootstrapSource: 'edge-worker',
      });

      const initResponse = await doStub.fetch(
        new Request('https://internal/init', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: initBody,
        })
      );
      const result = await initResponse.json();

      return new Response(
        JSON.stringify({
          success: true,
          action: 'bootstrap',
          colo: workerColo,
          doId: doId.toString(),
          doResponse: result,
          message: `Successfully bootstrapped TraversalDO at ${workerColo}`,
          usage: `Call via ${workerColo.toLowerCase()}.colo.do/https://graphdb.workers.do/bootstrap to create DO at ${workerColo}`,
        }, null, 2),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // /bootstrap/all - List all R2-local colos and their bootstrap status
    if (url.pathname === '/bootstrap/all') {
      const coloStatuses: Array<{
        colo: string;
        doId: string;
        initialized: boolean;
        initTimestamp?: number | undefined;
        actualColo?: string | undefined;
        error?: string | undefined;
      }> = [];

      let initializedCount = 0;
      let errorCount = 0;

      // Check status of each R2-local colo
      for (const colo of R2_LOCAL_COLOS) {
        const doId = env.TRAVERSAL_DO.idFromName(colo);
        const doStub = env.TRAVERSAL_DO.get(doId);

        try {
          const statusResponse = await doStub.fetch(new Request('https://internal/status'));
          if (statusResponse.ok) {
            const status = (await statusResponse.json()) as {
              initialized: boolean;
              initTimestamp?: number;
              colo?: string;
            };
            coloStatuses.push({
              colo,
              doId: doId.toString(),
              initialized: status.initialized,
              initTimestamp: status.initTimestamp,
              actualColo: status.colo,
            });
            if (status.initialized) initializedCount++;
          } else {
            coloStatuses.push({
              colo,
              doId: doId.toString(),
              initialized: false,
              error: `Status check failed: ${statusResponse.status}`,
            });
            errorCount++;
          }
        } catch (err) {
          coloStatuses.push({
            colo,
            doId: doId.toString(),
            initialized: false,
            error: err instanceof Error ? err.message : String(err),
          });
          errorCount++;
        }
      }

      // Generate bootstrap URLs for uninitialized colos
      const workerUrl = 'https://graphdb.workers.do';
      const pendingColos = coloStatuses.filter((s) => !s.initialized);
      const bootstrapUrls = pendingColos.map(
        (s) => `https://${s.colo.toLowerCase()}.colo.do/${workerUrl}/bootstrap`
      );

      return new Response(
        JSON.stringify({
          colos: coloStatuses,
          summary: {
            total: R2_LOCAL_COLOS.length,
            initialized: initializedCount,
            pending: pendingColos.length,
            errors: errorCount,
          },
          instructions: {
            message:
              pendingColos.length > 0
                ? `Bootstrap ${pendingColos.length} colos by calling the URLs below. Each URL routes through colo.do to ensure the DO is created at the correct colo.`
                : 'All R2-local colos have been bootstrapped.',
            bootstrapUrls,
            curlCommands: bootstrapUrls.map((bootstrapUrl) => `curl -X POST "${bootstrapUrl}"`),
          },
        }, null, 2),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // /bootstrap/colo/:colo - Get bootstrap info for specific colo
    const coloMatch = url.pathname.match(/^\/bootstrap\/colo\/([A-Za-z]+)$/);
    if (coloMatch?.[1]) {
      const colo = coloMatch[1].toUpperCase();

      if (!R2_LOCAL_COLOS.includes(colo as R2LocalColo)) {
        return new Response(
          JSON.stringify({
            error: `Invalid colo: ${colo}`,
            validColos: R2_LOCAL_COLOS,
          }),
          {
            status: 400,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      const doId = env.TRAVERSAL_DO.idFromName(colo);
      const doStub = env.TRAVERSAL_DO.get(doId);

      let status: { initialized: boolean; initTimestamp?: number; colo?: string } = {
        initialized: false,
      };

      try {
        const statusResponse = await doStub.fetch(new Request('https://internal/status'));
        if (statusResponse.ok) {
          status = await statusResponse.json();
        }
      } catch {
        // DO doesn't exist yet, which is fine
      }

      const workerUrl = 'https://graphdb.workers.do';
      const bootstrapUrl = `https://${colo.toLowerCase()}.colo.do/${workerUrl}/bootstrap`;

      return new Response(
        JSON.stringify({
          colo,
          doId: doId.toString(),
          initialized: status.initialized,
          initTimestamp: status.initTimestamp,
          actualColo: status.colo,
          bootstrapUrl,
          curlCommand: `curl -X POST "${bootstrapUrl}"`,
        }, null, 2),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // API documentation
    if (url.pathname === '/' || url.pathname === '/api') {
      const origin = url.origin;
      return new Response(
        JSON.stringify({
          name: 'GraphDB',
          version: '0.1.0-rc.1',
          description: 'Cost-optimized graph database for Cloudflare Workers',
          endpoints: {
            [`${origin}/`]: 'API information (this page)',
            [`${origin}/health`]: 'Health check',
            [`${origin}/bench?dataset=test&rows=1000`]: 'JSONBench-compatible benchmark',
            [`${origin}/connect`]: 'WebSocket upgrade to BrokerDO (hibernation-enabled)',
            [`${origin}/connect/:brokerId`]: 'Connect to specific broker instance',
            [`${origin}/broker/:id/metrics`]: 'Get broker metrics',
            [`${origin}/broker/:id/state`]: 'Get broker state value',
            [`${origin}/broker/:id/reset`]: 'Reset broker state and metrics',
            [`${origin}/shard/:id/count`]: 'ShardDO counter endpoint',
            [`${origin}/shard/:id/stats`]: 'ShardDO statistics',
            [`${origin}/shard/:id/reset`]: 'Reset ShardDO counters',
            [`${origin}/bootstrap`]: 'Bootstrap TraversalDO at current colo (use via colo.do)',
            [`${origin}/bootstrap/all`]: 'List all R2-local colos and their bootstrap status',
            [`${origin}/bootstrap/colo/:colo`]: 'Get bootstrap info for specific colo (e.g., /bootstrap/colo/ORD)',
            [`${origin}/benchmark/scenarios`]: 'List available benchmark scenarios',
            [`${origin}/benchmark/results`]: 'Get benchmark results',
            [`${origin}/benchmark/seed`]: 'POST - Seed test data',
            [`${origin}/benchmark/run/:scenario`]: 'POST - Run specific benchmark',
            [`${origin}/benchmark/run-all`]: 'POST - Run all benchmarks',
            [`${origin}/benchmark/reset`]: 'DELETE - Reset benchmark data',
          },
          bootstrap: {
            description: 'colo.do bootstrap mechanism for DO placement at R2-local colos',
            r2LocalColos: ['ORD', 'IAD', 'EWR', 'ATL', 'MIA', 'BOS', 'DFW', 'IAH'],
            usage: {
              bootstrapORD: 'curl https://ord.colo.do/https://graphdb.workers.do/bootstrap',
              bootstrapIAD: 'curl https://iad.colo.do/https://graphdb.workers.do/bootstrap',
              checkStatus: `${origin}/bootstrap/all`,
            },
          },
          benchParams: {
            dataset: 'test | onet | imdb (default: test)',
            rows: '100-50000 (default: 1000)',
          },
          websocketProtocol: {
            messages: {
              ping: '{ "type": "ping", "timestamp": number }',
              setState: '{ "type": "setState", "value": number }',
              getState: '{ "type": "getState" }',
              subrequests: '{ "subrequests": number, "messageId": number }',
            },
            responses: {
              connected: '{ "type": "connected", "clientId": string }',
              pong: '{ "type": "pong", "timestamp": number, "serverTime": number }',
              stateSet: '{ "type": "stateSet", "value": number }',
              state: '{ "type": "state", "value": number }',
              subrequestResult:
                '{ "type": "subrequestResult", "result": { successCount, failureCount, ... }, "metrics": {...} }',
            },
          },
          architecture: {
            costOptimization: 'Hibernating WebSocket = 95% discount vs active connections',
            subrequestQuota: 'Fresh 1000 subrequest quota per webSocketMessage wake',
          },
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    return new Response('Not Found', { status: 404 });
  },
};
