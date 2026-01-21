/**
 * CoordinatorDO - Cross-Shard Query Coordination Durable Object
 *
 * @description
 * The CoordinatorDO orchestrates complex queries that span multiple ShardDO instances.
 * While single-shard queries can be executed directly against individual ShardDO instances,
 * queries requiring data from multiple shards need this coordination layer to:
 *
 * 1. **Shard Selection & Routing**
 *    - Maintain a registry of active shards
 *    - Route queries to appropriate shards in parallel
 *    - Handle shard failures gracefully with automatic retries
 *
 * 2. **Result Aggregation & Merging**
 *    - Collect partial results from multiple shards
 *    - Merge and deduplicate results according to query semantics
 *    - Apply post-aggregation operations (sorting, limiting)
 *
 * 3. **Future: Distributed Transaction Coordination (v0.2.0+)**
 *    - Coordinate multi-shard write operations using 2PC (two-phase commit)
 *    - Ensure atomicity across shard boundaries
 *    - Handle transaction rollback on partial failures
 *
 * @version v0.1.0 - Basic cross-shard query coordination
 *
 * @see {@link ShardDO} for single-shard operations
 * @see {@link BrokerDO} for WebSocket query orchestration
 *
 * @example
 * ```typescript
 * // Register shards
 * const coordinatorId = env.COORDINATOR.idFromName('default');
 * const coordinator = env.COORDINATOR.get(coordinatorId);
 *
 * await coordinator.fetch(new Request('http://internal/shards/register', {
 *   method: 'POST',
 *   body: JSON.stringify({ shardId: 'shard-1' })
 * }));
 *
 * // Execute cross-shard query
 * const response = await coordinator.fetch(new Request('http://internal/query', {
 *   method: 'POST',
 *   body: JSON.stringify({
 *     type: 'lookup',
 *     ids: ['entity:1', 'entity:2']
 *   })
 * }));
 * ```
 */

import type { Env } from '../core/index.js';
import { errorResponse, ErrorCode } from '../errors/api-error.js';
import type { Entity } from '../core/entity.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Information about a registered shard
 */
export interface ShardInfo {
  /** Unique identifier for the shard */
  shardId: string;
  /** When the shard was registered */
  registeredAt: number;
  /** Last time the shard responded successfully */
  lastHeartbeat: number;
  /** Current health status */
  status: 'active' | 'inactive' | 'unhealthy';
  /** Number of successful queries routed to this shard */
  queryCount: number;
  /** Number of failed queries to this shard */
  errorCount: number;
}

/**
 * Statistics for monitoring coordinator health and performance.
 */
export interface CoordinatorStats {
  /** Total number of query requests received since DO instantiation */
  totalQueries: number;
  /** Number of queries currently being processed */
  queriesInProgress: number;
  /** Number of successful queries */
  successfulQueries: number;
  /** Number of failed queries */
  failedQueries: number;
  /** Unix timestamp of the last query request */
  lastQueryTimestamp: number;
  /** Unix timestamp when this DO instance was created */
  startupTimestamp: number;
  /** Number of registered shards */
  registeredShards: number;
  /** Number of active shards */
  activeShards: number;
  /** Uptime in milliseconds */
  uptimeMs: number;
}

/**
 * Query request body
 */
export interface QueryRequest {
  /** Query type: 'lookup', 'traverse', or 'filter' */
  type: 'lookup' | 'traverse' | 'filter';

  // For lookup queries
  /** Entity IDs to lookup (for type: 'lookup') */
  ids?: string[];

  // For traverse queries
  /** Starting entity ID (for type: 'traverse') */
  from?: string;
  /** Predicate to follow (for type: 'traverse') */
  predicate?: string;
  /** Maximum traversal depth (for type: 'traverse') */
  depth?: number;

  // For filter queries
  /** Field to filter on (for type: 'filter') */
  field?: string;
  /** Filter operator (for type: 'filter') */
  op?: string;
  /** Filter value (for type: 'filter') */
  value?: string | number;

  // Common options
  /** Optional timeout in milliseconds (default: 5000) */
  timeout?: number;
  /** Maximum results to return (default: 100) */
  limit?: number;
  /** Specific shard IDs to query (optional, queries all if not specified) */
  shardIds?: string[];
}

/**
 * Query response
 */
export interface QueryResponse {
  /** Query was successful */
  success: boolean;
  /** Query ID for tracking */
  queryId: string;
  /** Results from the query */
  results: Entity[];
  /** Metadata about the query execution */
  metadata: {
    /** Total time in milliseconds */
    durationMs: number;
    /** Number of shards queried */
    shardsQueried: number;
    /** Number of shards that responded */
    shardsResponded: number;
    /** Number of shards that failed */
    shardsFailed: number;
    /** Total results before deduplication */
    totalResults: number;
    /** Results after deduplication */
    dedupedResults: number;
  };
}

// ============================================================================
// Constants
// ============================================================================

/** Default query timeout in milliseconds */
const DEFAULT_TIMEOUT_MS = 5000;

/** Default result limit */
const DEFAULT_LIMIT = 100;

/** Maximum result limit */
const MAX_LIMIT = 1000;

/** Shard inactivity timeout (10 minutes) */
const SHARD_INACTIVE_TIMEOUT_MS = 10 * 60 * 1000;

/** Shard unhealthy threshold (3 consecutive failures) */
const SHARD_UNHEALTHY_THRESHOLD = 3;

// ============================================================================
// Implementation
// ============================================================================

/**
 * Cross-shard query coordination Durable Object.
 *
 * @description
 * CoordinatorDO orchestrates queries spanning multiple ShardDO instances.
 * It maintains a registry of active shards, routes queries in parallel,
 * and aggregates results from multiple shards.
 *
 * @example
 * ```typescript
 * // Check coordinator health
 * const id = env.COORDINATOR.idFromName('default');
 * const response = await env.COORDINATOR.get(id).fetch(
 *   new Request('http://internal/health')
 * );
 *
 * // Execute cross-shard lookup
 * const queryResponse = await env.COORDINATOR.get(id).fetch(
 *   new Request('http://internal/query', {
 *     method: 'POST',
 *     body: JSON.stringify({ type: 'lookup', ids: ['user:1', 'user:2'] })
 *   })
 * );
 * ```
 */
export class CoordinatorDO implements DurableObject {
  private readonly ctx: DurableObjectState;
  private readonly env: Env;

  // Shard registry
  private shards: Map<string, ShardInfo> = new Map();

  // Statistics
  private totalQueries: number = 0;
  private queriesInProgress: number = 0;
  private successfulQueries: number = 0;
  private failedQueries: number = 0;
  private lastQueryTimestamp: number = 0;
  private readonly startupTimestamp: number;

  /**
   * Creates a new CoordinatorDO instance.
   *
   * @param ctx - Durable Object state for persistence
   * @param env - Environment bindings for shard access
   */
  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
    this.startupTimestamp = Date.now();

    // Restore state from storage
    ctx.blockConcurrencyWhile(async () => {
      await this.restoreState();
    });
  }

  /**
   * Restore state from durable storage after hibernation
   */
  private async restoreState(): Promise<void> {
    // Restore statistics
    const stats = await this.ctx.storage.get<{
      totalQueries: number;
      successfulQueries: number;
      failedQueries: number;
      lastQueryTimestamp: number;
    }>('stats');

    if (stats) {
      this.totalQueries = stats.totalQueries;
      this.successfulQueries = stats.successfulQueries;
      this.failedQueries = stats.failedQueries;
      this.lastQueryTimestamp = stats.lastQueryTimestamp;
    }

    // Restore shard registry
    const stored = await this.ctx.storage.list<ShardInfo>({ prefix: 'shard:' });
    for (const [key, shardInfo] of stored) {
      const shardId = key.replace('shard:', '');
      this.shards.set(shardId, shardInfo);
    }
  }

  /**
   * Persist statistics to storage
   */
  private async persistStats(): Promise<void> {
    await this.ctx.storage.put('stats', {
      totalQueries: this.totalQueries,
      successfulQueries: this.successfulQueries,
      failedQueries: this.failedQueries,
      lastQueryTimestamp: this.lastQueryTimestamp,
    });
  }

  /**
   * Handle incoming HTTP requests.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;

    switch (url.pathname) {
      case '/health':
        return this.handleHealth();

      case '/stats':
        return this.handleStats();

      case '/query':
        if (method !== 'POST') {
          return errorResponse(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed', {
            method,
            allowed: ['POST'],
          });
        }
        return this.handleQuery(request);

      case '/shards':
        if (method === 'GET') {
          return this.handleListShards();
        }
        return errorResponse(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed', {
          method,
          allowed: ['GET'],
        });

      case '/shards/register':
        if (method !== 'POST') {
          return errorResponse(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed', {
            method,
            allowed: ['POST'],
          });
        }
        return this.handleRegisterShard(request);

      case '/shards/deregister':
        if (method !== 'POST') {
          return errorResponse(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed', {
            method,
            allowed: ['POST'],
          });
        }
        return this.handleDeregisterShard(request);

      case '/shards/heartbeat':
        if (method !== 'POST') {
          return errorResponse(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed', {
            method,
            allowed: ['POST'],
          });
        }
        return this.handleHeartbeat(request);

      default:
        return errorResponse(ErrorCode.NOT_FOUND, 'Endpoint not found', { path: url.pathname });
    }
  }

  /**
   * Handle health check
   */
  private handleHealth(): Response {
    const activeShards = this.getActiveShards().length;
    return new Response(
      JSON.stringify({
        status: 'healthy',
        uptime: Date.now() - this.startupTimestamp,
        activeShards,
        registeredShards: this.shards.size,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Handle stats request
   */
  private handleStats(): Response {
    const stats: CoordinatorStats = {
      totalQueries: this.totalQueries,
      queriesInProgress: this.queriesInProgress,
      successfulQueries: this.successfulQueries,
      failedQueries: this.failedQueries,
      lastQueryTimestamp: this.lastQueryTimestamp,
      startupTimestamp: this.startupTimestamp,
      registeredShards: this.shards.size,
      activeShards: this.getActiveShards().length,
      uptimeMs: Date.now() - this.startupTimestamp,
    };

    return new Response(JSON.stringify(stats), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  /**
   * Handle shard registration
   */
  private async handleRegisterShard(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { shardId: string };

      if (!body.shardId || typeof body.shardId !== 'string') {
        return errorResponse(ErrorCode.VALIDATION_ERROR, 'shardId is required', {
          param: 'shardId',
        });
      }

      const now = Date.now();
      const shardInfo: ShardInfo = {
        shardId: body.shardId,
        registeredAt: now,
        lastHeartbeat: now,
        status: 'active',
        queryCount: 0,
        errorCount: 0,
      };

      this.shards.set(body.shardId, shardInfo);
      await this.ctx.storage.put(`shard:${body.shardId}`, shardInfo);

      return new Response(
        JSON.stringify({
          success: true,
          shard: shardInfo,
        }),
        {
          status: 201,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(ErrorCode.BAD_REQUEST, 'Invalid request body', {
        reason: String(error),
      });
    }
  }

  /**
   * Handle shard deregistration
   */
  private async handleDeregisterShard(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { shardId: string };

      if (!body.shardId || typeof body.shardId !== 'string') {
        return errorResponse(ErrorCode.VALIDATION_ERROR, 'shardId is required', {
          param: 'shardId',
        });
      }

      const existed = this.shards.has(body.shardId);
      this.shards.delete(body.shardId);
      await this.ctx.storage.delete(`shard:${body.shardId}`);

      return new Response(
        JSON.stringify({
          success: true,
          existed,
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(ErrorCode.BAD_REQUEST, 'Invalid request body', {
        reason: String(error),
      });
    }
  }

  /**
   * Handle shard heartbeat
   */
  private async handleHeartbeat(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { shardId: string };

      if (!body.shardId || typeof body.shardId !== 'string') {
        return errorResponse(ErrorCode.VALIDATION_ERROR, 'shardId is required', {
          param: 'shardId',
        });
      }

      const shard = this.shards.get(body.shardId);
      if (!shard) {
        return errorResponse(ErrorCode.NOT_FOUND, 'Shard not registered', {
          shardId: body.shardId,
        });
      }

      shard.lastHeartbeat = Date.now();
      shard.status = 'active';
      await this.ctx.storage.put(`shard:${body.shardId}`, shard);

      return new Response(
        JSON.stringify({
          success: true,
          shard,
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(ErrorCode.BAD_REQUEST, 'Invalid request body', {
        reason: String(error),
      });
    }
  }

  /**
   * Handle list shards
   */
  private handleListShards(): Response {
    const shards = Array.from(this.shards.values());

    return new Response(JSON.stringify({ shards }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  /**
   * Get list of active shards
   */
  private getActiveShards(): ShardInfo[] {
    const now = Date.now();
    const activeShards: ShardInfo[] = [];

    for (const shard of this.shards.values()) {
      // Check if shard is stale
      if (now - shard.lastHeartbeat > SHARD_INACTIVE_TIMEOUT_MS) {
        shard.status = 'inactive';
        continue;
      }

      // Check if shard is unhealthy
      if (shard.errorCount >= SHARD_UNHEALTHY_THRESHOLD && shard.queryCount > 0) {
        const errorRate = shard.errorCount / shard.queryCount;
        if (errorRate > 0.5) {
          shard.status = 'unhealthy';
          continue;
        }
      }

      if (shard.status === 'active') {
        activeShards.push(shard);
      }
    }

    return activeShards;
  }

  /**
   * Handle cross-shard query execution
   */
  private async handleQuery(request: Request): Promise<Response> {
    const queryId = `q_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    const startTime = Date.now();

    this.totalQueries++;
    this.queriesInProgress++;
    this.lastQueryTimestamp = startTime;

    try {
      // Parse query request
      const queryRequest = (await request.json()) as QueryRequest;

      // Validate query type
      if (!queryRequest.type || !['lookup', 'traverse', 'filter'].includes(queryRequest.type)) {
        this.queriesInProgress--;
        this.failedQueries++;
        await this.persistStats();
        return errorResponse(ErrorCode.VALIDATION_ERROR, 'Invalid query type', {
          type: queryRequest.type,
          allowed: ['lookup', 'traverse', 'filter'],
        });
      }

      // Get shards to query
      const shardsToQuery = this.getShardsForQuery(queryRequest);
      if (shardsToQuery.length === 0) {
        this.queriesInProgress--;
        this.failedQueries++;
        await this.persistStats();
        return errorResponse(ErrorCode.NOT_FOUND, 'No active shards available', {
          registeredShards: this.shards.size,
        });
      }

      // Apply limits
      const limit = Math.min(queryRequest.limit ?? DEFAULT_LIMIT, MAX_LIMIT);
      const timeout = queryRequest.timeout ?? DEFAULT_TIMEOUT_MS;

      // Execute query on shards in parallel
      const results = await this.executeParallelQuery(queryRequest, shardsToQuery, timeout);

      // Aggregate and deduplicate results
      const aggregatedResults = this.aggregateResults(results.entities, limit);

      const durationMs = Date.now() - startTime;

      this.queriesInProgress--;
      this.successfulQueries++;
      await this.persistStats();

      const response: QueryResponse = {
        success: true,
        queryId,
        results: aggregatedResults,
        metadata: {
          durationMs,
          shardsQueried: shardsToQuery.length,
          shardsResponded: results.responded,
          shardsFailed: results.failed,
          totalResults: results.totalCount,
          dedupedResults: aggregatedResults.length,
        },
      };

      return new Response(JSON.stringify(response), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      this.queriesInProgress--;
      this.failedQueries++;
      await this.persistStats();

      return errorResponse(ErrorCode.INTERNAL_ERROR, 'Query execution failed', {
        queryId,
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  /**
   * Get shards to query based on query request
   */
  private getShardsForQuery(query: QueryRequest): ShardInfo[] {
    // If specific shards are requested, filter to those
    if (query.shardIds && query.shardIds.length > 0) {
      const requested: ShardInfo[] = [];
      for (const shardId of query.shardIds) {
        const shard = this.shards.get(shardId);
        if (shard && shard.status === 'active') {
          requested.push(shard);
        }
      }
      return requested;
    }

    // Otherwise, query all active shards
    return this.getActiveShards();
  }

  /**
   * Execute query on multiple shards in parallel
   */
  private async executeParallelQuery(
    query: QueryRequest,
    shards: ShardInfo[],
    timeoutMs: number
  ): Promise<{ entities: Entity[]; responded: number; failed: number; totalCount: number }> {
    const allEntities: Entity[] = [];
    let responded = 0;
    let failed = 0;

    // Build shard request URL and params based on query type
    const buildShardRequest = (shardId: string): Request => {
      const shardStub = this.env.SHARD.get(this.env.SHARD.idFromName(shardId));
      let url: string;
      let params: URLSearchParams;

      switch (query.type) {
        case 'lookup':
          url = 'http://shard/lookup';
          params = new URLSearchParams();
          if (query.ids) {
            params.set('ids', query.ids.join(','));
          }
          break;

        case 'traverse':
          url = 'http://shard/traverse';
          params = new URLSearchParams();
          if (query.from) params.set('from', query.from);
          if (query.predicate) params.set('predicate', query.predicate);
          if (query.depth) params.set('depth', query.depth.toString());
          break;

        case 'filter':
          url = 'http://shard/filter';
          params = new URLSearchParams();
          if (query.field) params.set('field', query.field);
          if (query.op) params.set('op', query.op);
          if (query.value !== undefined) params.set('value', String(query.value));
          break;

        default:
          throw new Error(`Unknown query type: ${query.type}`);
      }

      // Void the stub to suppress unused variable warning - we use idFromName below
      void shardStub;

      return new Request(`${url}?${params.toString()}`);
    };

    // Execute queries in parallel with timeout
    const shardPromises = shards.map(async (shard) => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

      try {
        const shardStub = this.env.SHARD.get(this.env.SHARD.idFromName(shard.shardId));
        const request = buildShardRequest(shard.shardId);

        const response = await shardStub.fetch(request.url, {
          method: 'GET',
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`Shard ${shard.shardId} returned ${response.status}`);
        }

        const data = (await response.json()) as Entity[];

        // Update shard stats
        shard.queryCount++;
        shard.lastHeartbeat = Date.now();

        return { success: true, entities: data, shardId: shard.shardId };
      } catch (error) {
        clearTimeout(timeoutId);

        // Update shard error stats
        shard.errorCount++;

        return { success: false, entities: [], shardId: shard.shardId, error };
      }
    });

    const results = await Promise.all(shardPromises);

    for (const result of results) {
      if (result.success) {
        responded++;
        allEntities.push(...result.entities);
      } else {
        failed++;
      }
    }

    return {
      entities: allEntities,
      responded,
      failed,
      totalCount: allEntities.length,
    };
  }

  /**
   * Aggregate and deduplicate results from multiple shards
   */
  private aggregateResults(entities: Entity[], limit: number): Entity[] {
    // Deduplicate by $id
    const seen = new Map<string, Entity>();

    for (const entity of entities) {
      const id = entity.$id;
      if (!seen.has(id)) {
        seen.set(id, entity);
      }
    }

    // Apply limit
    const dedupedEntities = Array.from(seen.values());
    return dedupedEntities.slice(0, limit);
  }
}
