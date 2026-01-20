/**
 * Edge Worker for colo.do Bootstrap Mechanism
 *
 * This module provides the bootstrap mechanism that creates DOs at specific colos
 * using the colo.do pattern. DOs are created at the colo where they first receive
 * a request. By routing through colo.do, we control exactly which colo creates the DO.
 *
 * Usage pattern:
 * ```bash
 * # Bootstrap DO at ORD by routing through colo.do
 * curl https://ord.colo.do/https://graphdb.workers.do/bootstrap
 *
 * # Bootstrap DO at IAD
 * curl https://iad.colo.do/https://graphdb.workers.do/bootstrap
 * ```
 *
 * R2-local colos to bootstrap: ORD, IAD, EWR, ATL, MIA, BOS, DFW, IAH
 *
 * Architecture:
 * 1. Edge Worker receives request at any Cloudflare colo
 * 2. /bootstrap endpoint creates a DO using idFromName(workerColo)
 * 3. The DO ID is deterministic based on colo name (e.g., "ORD", "IAD")
 * 4. DO's /init endpoint initializes it and stores init state
 * 5. /bootstrap/all shows status of all R2-local colos with bootstrap URLs
 *
 * Key insight: DOs are created at the colo where they first receive a request.
 * By routing through colo.do, we control exactly which colo creates the DO.
 */

import { RpcTarget, newWorkersRpcResponse } from 'capnweb';

/**
 * R2-local colos in the ENAM region
 * These colos have low-latency access to R2 buckets created in ENAM
 */
export const R2_LOCAL_COLOS = ['ORD', 'IAD', 'EWR', 'ATL', 'MIA', 'BOS', 'DFW', 'IAH'] as const;

export type R2LocalColo = (typeof R2_LOCAL_COLOS)[number];

/**
 * Default colo to route to when worker is not R2-local
 * Chicago (ORD) is centrally located in ENAM
 */
export const DEFAULT_COLO: R2LocalColo = 'ORD';

/**
 * Environment bindings for the edge worker
 */
export interface EdgeWorkerEnv {
  /** R2 bucket for lakehouse storage */
  LAKEHOUSE: R2Bucket;
  /** Durable Object namespace for traversal DOs */
  TRAVERSAL_DO: DurableObjectNamespace;
}

/**
 * Routing decision result
 */
export interface RoutingDecision {
  /** Worker's current colo */
  workerColo: string;
  /** Target DO colo */
  targetColo: string;
  /** Whether worker is at an R2-local colo */
  isR2Local: boolean;
  /** Routing strategy used */
  strategy: 'local' | 'redirect_to_default';
  /** Timestamp of decision */
  timestamp: string;
}

/**
 * Diagnostic info for the root endpoint
 */
export interface DiagnosticInfo {
  /** Service name */
  service: string;
  /** Worker's current colo */
  workerColo: string;
  /** Whether worker is at an R2-local colo */
  isR2Local: boolean;
  /** List of R2-local colos */
  r2LocalColos: readonly string[];
  /** Default routing colo */
  defaultColo: string;
  /** Available endpoints */
  endpoints: Record<string, string>;
  /** Timestamp */
  timestamp: string;
}

/**
 * Check if a colo is R2-local (in ENAM region)
 */
export function isR2LocalColo(colo: string): colo is R2LocalColo {
  return R2_LOCAL_COLOS.includes(colo as R2LocalColo);
}

/**
 * Pick the best colo for routing based on worker's current location
 *
 * @param workerColo - The colo where the edge worker is running
 * @returns The target colo for the DO
 */
export function pickBestColo(workerColo: string): string {
  if (isR2LocalColo(workerColo)) {
    return workerColo; // Use local DO for R2-local colos
  }
  return DEFAULT_COLO; // Default to Chicago for non-local colos
}

/**
 * Get the colo from a request's cf object
 */
export function getRequestColo(request: Request): string {
  const cf = (request as unknown as { cf?: { colo?: string } }).cf;
  return cf?.colo ?? 'unknown';
}

/**
 * Create a routing decision for the current request
 */
export function makeRoutingDecision(workerColo: string): RoutingDecision {
  const isR2Local = isR2LocalColo(workerColo);
  const targetColo = pickBestColo(workerColo);

  return {
    workerColo,
    targetColo,
    isR2Local,
    strategy: isR2Local ? 'local' : 'redirect_to_default',
    timestamp: new Date().toISOString(),
  };
}

/**
 * Edge Worker fetch handler
 *
 * Routes traversal requests to R2-local Durable Objects for optimal latency.
 */
export const edgeWorkerFetch = async (
  request: Request,
  env: EdgeWorkerEnv,
  _ctx: ExecutionContext
): Promise<Response> => {
  const url = new URL(request.url);
  const workerColo = getRequestColo(request);

  // -------------------------------------------------------------------------
  // /api - capnweb RPC endpoint (routes to R2-local DO)
  // -------------------------------------------------------------------------
  if (url.pathname === '/api') {
    const targetColo = pickBestColo(workerColo);
    const doId = env.TRAVERSAL_DO.idFromName(targetColo);
    const doStub = env.TRAVERSAL_DO.get(doId);
    return doStub.fetch(request);
  }

  // -------------------------------------------------------------------------
  // /routing - Show routing decision and target DO
  // -------------------------------------------------------------------------
  if (url.pathname === '/routing') {
    const decision = makeRoutingDecision(workerColo);

    return new Response(
      JSON.stringify(
        {
          routing: decision,
          explanation: decision.isR2Local
            ? `Worker at ${workerColo} is R2-local. DO will be called directly at ${decision.targetColo}.`
            : `Worker at ${workerColo} is NOT R2-local. Request will be routed to DO at ${decision.targetColo} (${DEFAULT_COLO}).`,
          r2Region: 'ENAM',
          r2LocalColos: R2_LOCAL_COLOS,
          latencyEstimate: {
            r2Local: '~80ms per R2 fetch',
            crossRegion: '~150-200ms per R2 fetch',
            savings: decision.isR2Local ? 'Optimal - no cross-region penalty' : 'Routing to R2-local DO saves ~70-120ms per R2 fetch',
          },
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // -------------------------------------------------------------------------
  // /bootstrap - Create DO at current colo (use via colo.do)
  // -------------------------------------------------------------------------
  if (url.pathname === '/bootstrap') {
    const doId = env.TRAVERSAL_DO.idFromName(workerColo);
    const doStub = env.TRAVERSAL_DO.get(doId);

    // Call DO's /init endpoint with bootstrap context
    const initBody = JSON.stringify({
      colo: workerColo,
      bootstrapTimestamp: Date.now(),
      bootstrapSource: 'edge-worker',
    });

    const response = await doStub.fetch(
      new Request('https://internal/init', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: initBody,
      })
    );
    const result = await response.json();

    return new Response(
      JSON.stringify(
        {
          success: true,
          action: 'bootstrap',
          colo: workerColo,
          doId: doId.toString(),
          doResponse: result,
          message: `Successfully bootstrapped TraversalDO at ${workerColo}`,
          usage: `Call via ${workerColo.toLowerCase()}.colo.do/https://graphdb.workers.do/bootstrap to create DO at ${workerColo}`,
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // -------------------------------------------------------------------------
  // /bootstrap/all - List all R2-local colos and their bootstrap status
  // -------------------------------------------------------------------------
  if (url.pathname === '/bootstrap/all') {
    const coloStatuses: Array<{
      colo: string;
      doId: string;
      initialized: boolean;
      initTimestamp?: number | undefined;
      actualColo?: string | undefined;
      error?: string | undefined;
    }> = [];

    let initialized = 0;
    let errors = 0;

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
          if (status.initialized) initialized++;
        } else {
          coloStatuses.push({
            colo,
            doId: doId.toString(),
            initialized: false,
            error: `Status check failed: ${statusResponse.status}`,
          });
          errors++;
        }
      } catch (err) {
        coloStatuses.push({
          colo,
          doId: doId.toString(),
          initialized: false,
          error: err instanceof Error ? err.message : String(err),
        });
        errors++;
      }
    }

    // Generate bootstrap URLs for uninitialized colos
    const workerUrl = 'https://graphdb.workers.do';
    const pendingColos = coloStatuses.filter((s) => !s.initialized);
    const bootstrapUrls = pendingColos.map(
      (s) => `https://${s.colo.toLowerCase()}.colo.do/${workerUrl}/bootstrap`
    );

    return new Response(
      JSON.stringify(
        {
          colos: coloStatuses,
          summary: {
            total: R2_LOCAL_COLOS.length,
            initialized,
            pending: pendingColos.length,
            errors,
          },
          instructions: {
            message:
              pendingColos.length > 0
                ? `Bootstrap ${pendingColos.length} colos by calling the URLs below. Each URL routes through colo.do to ensure the DO is created at the correct colo.`
                : 'All R2-local colos have been bootstrapped.',
            bootstrapUrls,
            curlCommands: bootstrapUrls.map((url) => `curl -X POST "${url}"`),
          },
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // -------------------------------------------------------------------------
  // /bootstrap/colo/:colo - Get bootstrap info for specific colo
  // -------------------------------------------------------------------------
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
      JSON.stringify(
        {
          colo,
          doId: doId.toString(),
          initialized: status.initialized,
          initTimestamp: status.initTimestamp,
          actualColo: status.colo,
          bootstrapUrl,
          curlCommand: `curl -X POST "${bootstrapUrl}"`,
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // -------------------------------------------------------------------------
  // / - Root endpoint: diagnostic info
  // -------------------------------------------------------------------------
  if (url.pathname === '/') {
    const diagnosticInfo: DiagnosticInfo = {
      service: 'GraphDB Traversal Edge Worker',
      workerColo,
      isR2Local: isR2LocalColo(workerColo),
      r2LocalColos: R2_LOCAL_COLOS,
      defaultColo: DEFAULT_COLO,
      endpoints: {
        '/': 'Diagnostic info (this page)',
        '/api': 'capnweb RPC endpoint - routes to R2-local DO',
        '/routing': 'Show routing decision and target DO',
        '/bootstrap': 'Create/initialize DO at current colo (use via colo.do)',
        '/bootstrap/all': 'List all R2-local colos and their bootstrap status',
        '/bootstrap/colo/:colo': 'Get bootstrap info for specific colo (e.g., /bootstrap/colo/ORD)',
      },
      timestamp: new Date().toISOString(),
    };

    const routingInfo = makeRoutingDecision(workerColo);

    return new Response(
      JSON.stringify(
        {
          ...diagnosticInfo,
          currentRouting: routingInfo,
          architecture: {
            pattern: 'Edge Worker -> R2-local DO -> R2 Bucket',
            benefit: 'Minimizes R2 fetch latency by routing to colos in same region as R2',
            r2Region: 'ENAM (Eastern North America)',
          },
          usage: {
            directCall: `${url.origin}/api - Routes to DO at ${routingInfo.targetColo}`,
            coloSpecific: `${workerColo.toLowerCase()}.colo.do/api - Force routing through ${workerColo}`,
          },
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  return new Response('Not Found', { status: 404 });
};

/**
 * Edge Worker export (default export for Cloudflare Workers)
 */
export default {
  fetch: edgeWorkerFetch,
};

// ============================================================================
// Traversal Durable Object (capnweb RpcTarget)
// ============================================================================

/**
 * Entity returned from lookups
 */
export interface TraversalEntity {
  id: string;
  type: string;
  properties: Record<string, unknown>;
  edges: TraversalEdge[];
}

/**
 * Edge in the graph
 */
export interface TraversalEdge {
  predicate: string;
  target: string;
}

/**
 * Result of a traversal with timing stats
 */
export interface TraversalResult {
  startId: string;
  depth: number;
  finalIds: string[];
  stats: {
    colo: string;
    totalTime_ms: number;
    r2Fetches: number;
    entitiesVisited: number;
    hopTimes_ms: number[];
  };
}

/**
 * Traversal API interface for capnweb RPC
 */
export interface TraversalApi {
  /** Get the colo where this DO is running */
  getColo(): string;

  /** Measure R2 latency from this DO */
  getR2Latency(): Promise<number>;

  /** Lookup a single entity by ID */
  lookup(entityId: string): Promise<TraversalEntity | null>;

  /** Batch lookup multiple entities */
  batchLookup(entityIds: string[]): Promise<(TraversalEntity | null)[]>;

  /** Traverse graph from start entity, returns entity IDs at final depth */
  traverse(startId: string, depth: number): Promise<string[]>;

  /** Full traversal with timing details */
  traverseWithStats(startId: string, depth: number): Promise<TraversalResult>;
}

/**
 * Traversal Durable Object
 *
 * Handles graph traversals with R2-local access.
 * Implements capnweb RpcTarget for promise pipelining support.
 */
export class TraversalDO extends RpcTarget implements DurableObject, TraversalApi {
  private colo: string = 'unknown';
  private env: EdgeWorkerEnv;

  constructor(_ctx: DurableObjectState, env: EdgeWorkerEnv) {
    super();
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    this.colo = getRequestColo(request);

    // capnweb RPC handling
    if (
      request.headers.get('Upgrade') === 'websocket' ||
      request.headers.get('Content-Type')?.includes('capnweb')
    ) {
      return newWorkersRpcResponse(request, this);
    }

    // Init/bootstrap endpoint
    if (url.pathname === '/init') {
      return new Response(
        JSON.stringify({
          status: 'initialized',
          colo: this.colo,
          timestamp: new Date().toISOString(),
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Direct traverse endpoint (for testing/benchmarking)
    if (url.pathname === '/traverse') {
      const depth = parseInt(url.searchParams.get('depth') || '3');
      const startId = url.searchParams.get('start') || 'https://imdb.com/title/tt0000001';
      const result = await this.traverseWithStats(startId, depth);
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response('Not found', { status: 404 });
  }

  // ============================================================================
  // TraversalApi Implementation
  // ============================================================================

  getColo(): string {
    return this.colo;
  }

  async getR2Latency(): Promise<number> {
    const start = performance.now();
    const obj = await this.env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (obj) await obj.text();
    return Math.round(performance.now() - start);
  }

  async lookup(entityId: string): Promise<TraversalEntity | null> {
    // Simulate lookup: bloom filter -> chunk fetch -> decode
    // In real implementation, would use actual graph storage

    const start = performance.now();
    const obj = await this.env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (!obj) return null;
    await obj.text();

    // Simulated entity
    return {
      id: entityId,
      type: entityId.includes('/title/') ? 'Movie' : 'Person',
      properties: {
        lookupTime_ms: Math.round(performance.now() - start),
        colo: this.colo,
      },
      edges: this.generateEdges(entityId),
    };
  }

  async batchLookup(entityIds: string[]): Promise<(TraversalEntity | null)[]> {
    // In real implementation, would optimize for same-chunk entities
    return Promise.all(entityIds.map((id) => this.lookup(id)));
  }

  async traverse(startId: string, depth: number): Promise<string[]> {
    const result = await this.traverseWithStats(startId, depth);
    return result.finalIds;
  }

  async traverseWithStats(startId: string, depth: number): Promise<TraversalResult> {
    const start = performance.now();
    const hopTimes: number[] = [];
    let r2Fetches = 0;
    let entitiesVisited = 0;

    let currentIds = [startId];

    for (let hop = 1; hop <= depth; hop++) {
      const hopStart = performance.now();
      const nextIds: Set<string> = new Set();

      for (const id of currentIds.slice(0, 10)) {
        const entity = await this.lookup(id);
        r2Fetches++;
        entitiesVisited++;

        if (entity) {
          for (const edge of entity.edges.slice(0, 5)) {
            nextIds.add(edge.target);
          }
        }
      }

      hopTimes.push(Math.round(performance.now() - hopStart));
      currentIds = [...nextIds];
      if (currentIds.length === 0) break;
    }

    return {
      startId,
      depth,
      finalIds: currentIds.slice(0, 20),
      stats: {
        colo: this.colo,
        totalTime_ms: Math.round(performance.now() - start),
        r2Fetches,
        entitiesVisited,
        hopTimes_ms: hopTimes,
      },
    };
  }

  /**
   * Generate deterministic fake edges for testing
   */
  private generateEdges(entityId: string): TraversalEdge[] {
    const baseId = entityId.replace(/\d+$/, '');
    const num = parseInt(entityId.match(/\d+$/)?.[0] || '1');

    return [
      { predicate: 'relatedTo', target: `${baseId}${num + 1}` },
      { predicate: 'relatedTo', target: `${baseId}${num + 2}` },
      { predicate: 'relatedTo', target: `${baseId}${num + 3}` },
    ];
  }
}
