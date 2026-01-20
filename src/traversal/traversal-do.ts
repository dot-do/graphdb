/**
 * TraversalDO - Region-optimized graph traversal using colo.do + capnweb
 *
 * This Durable Object handles graph traversals at R2-local colos and supports
 * the colo.do bootstrap mechanism for DO placement.
 *
 * Bootstrap Mechanism:
 * - /init (POST): Initialize the DO at the current colo, stores initialization state
 * - /status (GET): Check if the DO has been initialized and at which colo
 *
 * The key insight: DOs are created at the colo where they first receive a request.
 * By routing through colo.do, we control exactly which colo creates the DO.
 *
 * Usage pattern:
 * ```bash
 * # Bootstrap DO at ORD by routing through colo.do
 * curl https://ord.colo.do/https://graphdb.workers.do/bootstrap
 * ```
 *
 * Architecture:
 * 1. Edge Worker routes through colo.do to reach R2-local colo
 *    e.g., ord.colo.do/api -> request executes at ORD (Chicago)
 *
 * 2. TraversalDO at ORD is an RpcTarget accessible via capnweb
 *
 * 3. capnweb's .map() enables remote array operations in single round-trip:
 *    let ids = api.traverse(startId, 3);
 *    let entities = await ids.map(id => api.lookup(id)); // pipelined!
 *
 * Result: Consistent traversal latency regardless of user location
 * - Edge -> ORD via colo.do: ~40-80ms (one-time)
 * - Traversal at ORD with local R2: ~330ms (4-hop)
 * - Total: ~400ms from anywhere
 */

import { RpcTarget, newWorkersRpcResponse } from 'capnweb';
import { GraphLookup } from './graph-lookup.js';
import { EdgeCache } from '../snippet/edge-cache.js';

// ============================================================================
// Constants
// ============================================================================

/**
 * Maximum depth for path traversal operations to prevent infinite recursion.
 * This is the absolute upper limit; callers can specify lower limits.
 */
export const MAX_PATH_DEPTH = 100;

/**
 * Default depth for path traversal when not specified.
 */
export const DEFAULT_PATH_DEPTH = 3;

// ============================================================================
// Types
// ============================================================================

export interface TraversalEnv {
  LAKEHOUSE: R2Bucket;
  TRAVERSAL_DO: DurableObjectNamespace;
}

/**
 * TraversalApi - capnweb RPC interface for graph traversals
 */
export interface TraversalApi {
  /** Get the colo where this DO is running */
  getColo(): string;

  /** Measure R2 fetch latency from this colo */
  getR2Latency(): Promise<number>;

  /**
   * Lookup entity - returns properties and edges
   * Note: For now, simulates with R2 fetch. Real implementation in pocs-ud70.
   */
  lookup(entityId: string): Promise<DOEntity | null>;

  /** Batch lookup - optimized for same-chunk entities */
  batchLookup(entityIds: string[]): Promise<(DOEntity | null)[]>;

  /**
   * Traverse graph from start entity
   * Returns array of entity IDs at final depth
   *
   * With capnweb .map(), you can pipeline:
   *   let finalIds = api.traverse(startId, 3);
   *   let entities = await finalIds.map(id => api.lookup(id));
   */
  traverse(startId: string, depth: number): Promise<string[]>;

  /** Full traversal with timing details */
  traverseWithStats(startId: string, depth: number): Promise<TraversalResult>;
}

export interface DOEntity {
  id: string;
  type: string;
  properties: Record<string, unknown>;
  edges: Edge[];
}

export interface Edge {
  predicate: string;
  target: string;
}

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

// ============================================================================
// TraversalDO - capnweb RpcTarget
// ============================================================================

/**
 * Bootstrap state persisted in DO storage
 */
export interface BootstrapState {
  initialized: boolean;
  initTimestamp: number;
  colo: string;
  bootstrapSource: string;
  lastRequestTimestamp?: number;
  requestCount: number;
}

/**
 * TraversalDO - Durable Object that handles graph traversals at R2-local colos
 *
 * Extends RpcTarget to enable capnweb RPC over WebSocket or HTTP.
 *
 * Supports colo.do bootstrap mechanism:
 * - /init (POST): Initialize the DO at the current colo
 * - /status (GET): Check initialization status
 */
export class TraversalDO extends RpcTarget implements DurableObject, TraversalApi {
  private colo: string = 'unknown';
  private env: TraversalEnv;
  private ctx: DurableObjectState;

  // Bootstrap state - restored from storage on first request
  private bootstrapState: BootstrapState | null = null;
  private stateLoaded: boolean = false;

  // Graph lookup with bloom filter routing (lazy initialized)
  private graphLookup: GraphLookup | null = null;
  private edgeCache: EdgeCache | null = null;

  constructor(ctx: DurableObjectState, env: TraversalEnv) {
    super();
    this.ctx = ctx;
    this.env = env;
  }

  /**
   * Get or create the GraphLookup instance
   */
  private getGraphLookup(): GraphLookup {
    if (!this.graphLookup) {
      // Initialize edge cache
      this.edgeCache = new EdgeCache({
        bloomTtl: 300,
        segmentTtl: 3600,
      });

      // Initialize graph lookup
      this.graphLookup = new GraphLookup({
        r2: this.env.LAKEHOUSE,
        cache: this.edgeCache,
        colo: this.colo,
      });
    }
    return this.graphLookup;
  }

  /**
   * Load bootstrap state from storage (once per DO instance)
   */
  private async loadState(): Promise<void> {
    if (this.stateLoaded) return;

    const stored = await this.ctx.storage.get<BootstrapState>('bootstrapState');
    if (stored) {
      this.bootstrapState = stored;
    }
    this.stateLoaded = true;
  }

  /**
   * Save bootstrap state to storage
   */
  private async saveState(): Promise<void> {
    if (this.bootstrapState) {
      await this.ctx.storage.put('bootstrapState', this.bootstrapState);
    }
  }

  /**
   * Handle incoming requests
   *
   * Supports:
   * - capnweb RPC via WebSocket upgrade or capnweb content-type
   * - Direct HTTP endpoints: /init, /traverse
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Extract colo from Cloudflare request properties
    this.colo = (request as unknown as { cf?: { colo?: string } }).cf?.colo ?? 'unknown';

    // capnweb RPC - WebSocket upgrade or capnweb content-type
    if (
      request.headers.get('Upgrade') === 'websocket' ||
      request.headers.get('Content-Type')?.includes('capnweb')
    ) {
      return newWorkersRpcResponse(request, this);
    }

    // /init - Bootstrap/initialization endpoint (POST to initialize, GET to check)
    if (url.pathname === '/init') {
      await this.loadState();

      if (request.method === 'POST') {
        // Initialize bootstrap state
        const now = Date.now();
        this.bootstrapState = {
          initialized: true,
          initTimestamp: now,
          colo: this.colo,
          bootstrapSource: 'edge-worker',
          requestCount: 1,
        };
        await this.saveState();

        return new Response(
          JSON.stringify({
            status: 'initialized',
            colo: this.colo,
            initTimestamp: now,
            timestamp: new Date().toISOString(),
          }),
          {
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      // GET - return current state
      return new Response(
        JSON.stringify({
          status: this.bootstrapState?.initialized ? 'initialized' : 'not_initialized',
          colo: this.colo,
          bootstrapState: this.bootstrapState,
          timestamp: new Date().toISOString(),
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // /status - Check initialization status (alias for GET /init)
    if (url.pathname === '/status') {
      await this.loadState();
      return new Response(
        JSON.stringify({
          initialized: this.bootstrapState?.initialized ?? false,
          initTimestamp: this.bootstrapState?.initTimestamp,
          colo: this.bootstrapState?.colo,
          requestCount: this.bootstrapState?.requestCount,
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // /traverse - Direct traversal endpoint
    if (url.pathname === '/traverse') {
      const requestedDepth = parseInt(url.searchParams.get('depth') || String(DEFAULT_PATH_DEPTH));
      const depth = Math.min(Math.max(0, requestedDepth), MAX_PATH_DEPTH);
      const startId = url.searchParams.get('start') || 'https://imdb.com/title/tt0000001';
      const result = await this.traverseWithStats(startId, depth);
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // /r2-latency - Measure R2 latency from this colo
    if (url.pathname === '/r2-latency') {
      const latency = await this.getR2Latency();
      return new Response(
        JSON.stringify({
          colo: this.colo,
          r2Latency_ms: latency,
          timestamp: new Date().toISOString(),
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // /lookup - Single entity lookup
    if (url.pathname === '/lookup') {
      const entityId = url.searchParams.get('id');
      if (!entityId) {
        return new Response(JSON.stringify({ error: 'Missing id parameter' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        });
      }
      const entity = await this.lookup(entityId);
      return new Response(JSON.stringify(entity, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response('Not found', { status: 404 });
  }

  // ============================================================================
  // TraversalApi Implementation
  // ============================================================================

  /**
   * Get the colo where this DO is running
   */
  getColo(): string {
    return this.colo;
  }

  /**
   * Measure R2 fetch latency from this colo
   */
  async getR2Latency(): Promise<number> {
    const start = performance.now();
    const obj = await this.env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (obj) await obj.text();
    return Math.round(performance.now() - start);
  }

  /**
   * Lookup entity by ID
   *
   * Uses bloom filter routing to efficiently find the entity:
   * 1. Check combined bloom filter (quick reject)
   * 2. Check per-chunk bloom filters to find candidates
   * 3. Fetch and decode candidate chunks
   * 4. Extract entity from triples
   */
  async lookup(entityId: string): Promise<DOEntity | null> {
    const graphLookup = this.getGraphLookup();
    const { entity, stats } = await graphLookup.lookup(entityId);

    if (entity) {
      // Convert LookupEntity to Entity format
      return {
        id: entity.id,
        type: entity.type,
        properties: {
          ...entity.properties,
          lookupTime_ms: stats.timeMs,
          chunksChecked: stats.chunksChecked,
          cacheHit: stats.cacheHit,
          colo: this.colo,
        },
        edges: entity.edges,
      };
    }

    // Entity not found - return null (could also return a stub with fake edges for testing)
    return null;
  }

  /**
   * Batch lookup - optimized for same-chunk entities
   *
   * Groups entities by namespace and chunk to minimize R2 fetches.
   * Much more efficient than individual lookups for related entities.
   */
  async batchLookup(entityIds: string[]): Promise<(DOEntity | null)[]> {
    const graphLookup = this.getGraphLookup();
    const { entities } = await graphLookup.batchLookup(entityIds);

    // Convert LookupEntity[] to Entity[]
    return entities.map((entity) => {
      if (!entity) return null;
      return {
        id: entity.id,
        type: entity.type,
        properties: {
          ...entity.properties,
          colo: this.colo,
        },
        edges: entity.edges,
      };
    });
  }

  /**
   * Traverse graph from start entity
   *
   * Returns array of entity IDs at final depth.
   * Calls traverseWithStats internally and returns just the finalIds.
   */
  async traverse(startId: string, depth: number): Promise<string[]> {
    const result = await this.traverseWithStats(startId, depth);
    return result.finalIds;
  }

  /**
   * Full traversal with timing statistics
   *
   * Performs breadth-first traversal up to specified depth.
   * Tracks timing for each hop and total R2 fetches.
   *
   * Enforces MAX_PATH_DEPTH as an absolute upper bound to prevent
   * infinite recursion or DoS attacks via deeply nested traversals.
   */
  async traverseWithStats(startId: string, depth: number): Promise<TraversalResult> {
    const start = performance.now();
    const hopTimes: number[] = [];
    let r2Fetches = 0;
    let entitiesVisited = 0;

    // Enforce absolute maximum while respecting user-specified limit
    const effectiveDepth = Math.min(Math.max(0, depth), MAX_PATH_DEPTH);

    let currentIds = [startId];

    for (let hop = 1; hop <= effectiveDepth; hop++) {
      const hopStart = performance.now();
      const nextIds: Set<string> = new Set();

      // Process up to 10 entities per hop to limit fan-out
      for (const id of currentIds.slice(0, 10)) {
        const entity = await this.lookup(id);
        r2Fetches++;
        entitiesVisited++;

        if (entity) {
          // Take up to 5 edges per entity
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
      finalIds: currentIds.slice(0, 20), // Limit to 20 results
      stats: {
        colo: this.colo,
        totalTime_ms: Math.round(performance.now() - start),
        r2Fetches,
        entitiesVisited,
        hopTimes_ms: hopTimes,
      },
    };
  }

}
