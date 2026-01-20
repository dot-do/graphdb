/**
 * CoordinatorDO - Cross-Shard Query Coordination Durable Object
 *
 * @description
 * The CoordinatorDO is designed to orchestrate complex queries that span multiple
 * ShardDO instances. While single-shard queries can be executed directly against
 * individual ShardDO instances, queries requiring data from multiple shards need
 * a coordination layer to:
 *
 * 1. **Query Planning & Optimization**
 *    - Parse and analyze incoming queries to determine which shards contain relevant data
 *    - Generate optimal execution plans that minimize cross-shard communication
 *    - Leverage bloom filters from the snippet layer to prune unnecessary shard accesses
 *    - Apply predicate pushdown to reduce data transfer between shards
 *
 * 2. **Shard Selection & Routing**
 *    - Maintain a registry of active shards and their data distributions
 *    - Use bloom filter metadata to determine which shards might contain target entities
 *    - Route query fragments to appropriate shards in parallel when possible
 *    - Handle shard failures gracefully with automatic retries
 *
 * 3. **Result Aggregation & Merging**
 *    - Collect partial results from multiple shards
 *    - Merge and deduplicate results according to query semantics
 *    - Apply post-aggregation operations (sorting, limiting, grouping)
 *    - Stream large result sets to avoid memory exhaustion
 *
 * 4. **Distributed Transaction Coordination**
 *    - Coordinate multi-shard write operations using 2PC (two-phase commit)
 *    - Ensure atomicity across shard boundaries
 *    - Handle transaction rollback on partial failures
 *    - Maintain transaction logs for recovery
 *
 * @status STUB - Not Yet Implemented
 *
 * This Durable Object currently exists as a stub to establish the API contract
 * and provide a placeholder for future cross-shard coordination capabilities.
 * The current architecture supports efficient single-shard operations, which
 * covers the majority of use cases. Cross-shard coordination is planned for
 * v0.2.0 or later based on user demand and performance requirements.
 *
 * @rationale
 * The stub exists because:
 * - The Durable Object binding and type definitions need to be in place early
 * - Client code can be written against the planned API
 * - The `/health` and `/stats` endpoints provide operational visibility
 * - Gradual rollout allows validating the coordination patterns before full implementation
 *
 * @targetVersion v0.2.0+
 *
 * @see {@link https://github.com/dotdo/graphdb/blob/main/docs/ROADMAP.md} for implementation timeline
 * @see {@link BrokerDO} for WebSocket query orchestration (implemented)
 * @see {@link ShardDO} for single-shard operations (implemented)
 *
 * @example
 * // Future usage (not yet implemented):
 * const coordinatorId = env.COORDINATOR.idFromName('default');
 * const coordinator = env.COORDINATOR.get(coordinatorId);
 * const response = await coordinator.fetch(new Request('http://internal/query', {
 *   method: 'POST',
 *   body: JSON.stringify({
 *     query: 'user:123.friends[*].posts',
 *     options: { maxHops: 3, timeout: 5000 }
 *   })
 * }));
 */

import type { Env } from '../core/index.js';
import { errorResponse, ErrorCode } from '../errors/api-error.js';

/**
 * Statistics for monitoring coordinator health and performance.
 *
 * @remarks
 * These statistics are tracked even in stub mode to provide operational
 * visibility into how many coordination requests are being attempted.
 * This data helps inform the prioritization of full implementation.
 */
export interface CoordinatorStats {
  /** Total number of query requests received since DO instantiation */
  totalQueries: number;
  /** Number of queries currently being processed (always 0 in stub mode) */
  queriesInProgress: number;
  /** Unix timestamp of the last query request */
  lastQueryTimestamp: number;
  /** Unix timestamp when this DO instance was created */
  startupTimestamp: number;
}

/**
 * Cross-shard query coordination Durable Object.
 *
 * @status STUB - Query coordination not yet implemented (target: v0.2.0+)
 *
 * @description
 * CoordinatorDO will orchestrate queries spanning multiple ShardDO instances.
 * Currently, only the `/health` and `/stats` endpoints are functional.
 * The `/query` endpoint returns a helpful error with alternatives.
 *
 * @remarks
 * **Why this stub exists:**
 * - Establishes the API contract early for client code development
 * - Provides operational visibility via health/stats endpoints
 * - Tracks demand for cross-shard coordination via query attempt metrics
 *
 * **Current alternatives:**
 * - Single-shard queries: Use `ShardDO` directly
 * - Multi-hop traversals: Use `BrokerDO` with WebSocket connections
 *
 * @example
 * ```typescript
 * // Check coordinator health (works now)
 * const id = env.COORDINATOR.idFromName('default');
 * const response = await env.COORDINATOR.get(id).fetch(
 *   new Request('http://internal/health')
 * );
 *
 * // Query endpoint returns NOT_IMPLEMENTED with guidance
 * const queryResponse = await env.COORDINATOR.get(id).fetch(
 *   new Request('http://internal/query', { method: 'POST', body: '...' })
 * );
 * // Returns: { error: 'NOT_IMPLEMENTED', alternatives: {...} }
 * ```
 */
export class CoordinatorDO implements DurableObject {
  private readonly ctx: DurableObjectState;
  // @ts-expect-error Reserved for future use when implementing cross-shard coordination
  private readonly _env: Env;
  private totalQueries: number = 0;
  private readonly startupTimestamp: number;

  /**
   * Creates a new CoordinatorDO instance.
   *
   * @param ctx - Durable Object state for persistence
   * @param env - Environment bindings (reserved for future shard access)
   */
  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this._env = env;
    this.startupTimestamp = Date.now();

    // Restore state from storage to track query attempts across hibernations
    ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.get<number>('totalQueries');
      if (stored !== undefined) {
        this.totalQueries = stored;
      }
    });
  }

  /**
   * Handle incoming HTTP requests.
   *
   * @description
   * Routes requests to appropriate handlers based on pathname:
   *
   * | Endpoint | Method | Status | Description |
   * |----------|--------|--------|-------------|
   * | `/health` | GET | Implemented | Health check with uptime |
   * | `/stats` | GET | Implemented | Query attempt statistics |
   * | `/query` | POST | **Stub** | Returns NOT_IMPLEMENTED with alternatives |
   *
   * @param request - Incoming HTTP request
   * @returns Response with JSON body
   *
   * @example
   * ```typescript
   * // Health check
   * GET /health
   * // Response: { "status": "healthy", "uptime": 12345 }
   *
   * // Statistics
   * GET /stats
   * // Response: { "totalQueries": 42, "startupTimestamp": ..., "uptimeMs": ... }
   *
   * // Query (returns helpful error)
   * POST /query
   * // Response: { "error": "NOT_IMPLEMENTED", "message": "...", "details": {...} }
   * ```
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    switch (url.pathname) {
      case '/health':
        return new Response(
          JSON.stringify({
            status: 'healthy',
            uptime: Date.now() - this.startupTimestamp,
          }),
          {
            headers: { 'Content-Type': 'application/json' },
          }
        );

      case '/stats':
        return new Response(
          JSON.stringify({
            totalQueries: this.totalQueries,
            startupTimestamp: this.startupTimestamp,
            uptimeMs: Date.now() - this.startupTimestamp,
          }),
          {
            headers: { 'Content-Type': 'application/json' },
          }
        );

      case '/query':
        if (request.method !== 'POST') {
          return errorResponse(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed', { method: request.method, allowed: ['POST'] });
        }
        return this.handleQuery(request);

      default:
        return errorResponse(ErrorCode.NOT_FOUND, 'Endpoint not found', { path: url.pathname });
    }
  }

  /**
   * Handle query execution (stub).
   *
   * @remarks
   * This method currently returns a NOT_IMPLEMENTED error with guidance on
   * alternatives. Cross-shard query coordination is planned for v0.2.0+.
   *
   * For single-shard queries, use the ShardDO directly via the shard binding.
   * For multi-hop traversals within a single shard, use the BrokerDO with
   * WebSocket connections which provides query orchestration capabilities.
   *
   * @param _request - The incoming query request (unused in stub)
   * @returns Error response with documentation links and alternatives
   */
  private async handleQuery(_request: Request): Promise<Response> {
    this.totalQueries++;
    await this.ctx.storage.put('totalQueries', this.totalQueries);

    return errorResponse(
      ErrorCode.NOT_IMPLEMENTED,
      'Cross-shard query coordination is not yet implemented. ' +
        'This feature is planned for v0.2.0+. ' +
        'For single-shard queries, use ShardDO directly. ' +
        'For multi-hop traversals, use BrokerDO with WebSocket connections.',
      {
        queryId: this.totalQueries,
        status: 'stub',
        targetVersion: 'v0.2.0+',
        alternatives: {
          singleShard: 'Use env.SHARD.get(id).fetch() for single-shard operations',
          multiHop: 'Use WebSocket connection to BrokerDO for query orchestration',
        },
        documentation: 'https://github.com/dotdo/graphdb#roadmap',
      }
    );
  }
}
