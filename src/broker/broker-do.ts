/**
 * BrokerDO - Hibernating WebSocket Durable Object
 *
 * Core broker component for GraphDB that:
 * - Accepts WebSocket connections and hibernates between messages
 * - Gets fresh 1000 subrequest quota per webSocketMessage wake
 * - Orchestrates queries to Shard DOs using the quota
 * - Uses serializeAttachment/deserializeAttachment for state preservation
 * - Exposes GraphAPI via capnweb RPC
 *
 * Cost benefit: 95% discount vs active connections ($0.0075 vs $0.15/million)
 */

import { newWorkersRpcResponse } from 'capnweb';
import type { Env } from '../core/index.js';
import { GraphAPITarget } from '../protocol/graph-api.js';
import { safeJsonParse, JsonParseError } from '../security/json-validator.js';
import { errorResponse, ErrorCode, wsErrorJson, WsErrorCode } from '../errors/api-error.js';
import { routeEntity, getShardId } from '../snippet/router.js';
import { createEntityId, createNamespace } from '../core/types.js';
import { validateRpcCall, type RpcCallMessage } from '../rpc/types.js';

/**
 * Maximum number of entries to keep in the subrequestsPerWake rolling window.
 * This prevents unbounded memory growth while still providing useful metrics.
 */
const MAX_SUBREQUESTS_PER_WAKE_ENTRIES = 100;

/**
 * Minimum allowed value for requestedSubrequests parameter.
 * Must be at least 1 to perform any meaningful work.
 */
const MIN_SUBREQUESTS = 1;

/**
 * Maximum allowed value for requestedSubrequests parameter.
 * Capped at 1000 to match Cloudflare Workers subrequest quota per wake cycle
 * and prevent abuse or accidental resource exhaustion.
 */
const MAX_SUBREQUESTS = 1000;

/**
 * Attachment data stored with hibernated WebSocket connections
 */
export interface WebSocketAttachment {
  clientId: string;
  connectedAt: number;
  totalMessagesReceived: number;
  totalSubrequestsMade: number;
  /**
   * Stored cursors for pagination across hibernation cycles.
   * Maps queryId to cursor string.
   * This enables cursor survival during DO hibernation.
   */
  cursors?: Record<string, string>;
}

/**
 * Result from a subrequest batch
 */
export interface SubrequestBatchResult {
  messageId: number;
  requestedCount: number;
  successCount: number;
  failureCount: number;
  errors: string[];
  durationMs: number;
  /** The shard ID that was targeted based on hash-based routing */
  shardId: string;
}

/**
 * Metrics tracked across hibernation cycles
 */
export interface BrokerMetrics {
  totalWakes: number;
  totalSubrequests: number;
  totalFailures: number;
  subrequestsPerWake: number[];
  lastWakeTimestamp: number;
}

export class BrokerDO implements DurableObject {
  private readonly ctx: DurableObjectState;
  private readonly env: Env;

  // Metrics persisted in memory (survives within session, reset on eviction)
  private metrics: BrokerMetrics = {
    totalWakes: 0,
    totalSubrequests: 0,
    totalFailures: 0,
    subrequestsPerWake: [],
    lastWakeTimestamp: 0,
  };

  // Track state preservation across hibernation
  private stateValue: number = 0;

  // GraphAPI target for capnweb RPC
  private graphApi: GraphAPITarget;

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;

    // Create GraphAPI target with shard stub getter for orchestrator integration
    this.graphApi = new GraphAPITarget((shardId: string) => {
      const id = env.SHARD.idFromName(shardId);
      return env.SHARD.get(id);
    });

    // Restore state from storage on wake
    ctx.blockConcurrencyWhile(async () => {
      const stored = await ctx.storage.get<number>('stateValue');
      if (stored !== undefined) {
        this.stateValue = stored;
      }

      const storedMetrics = await ctx.storage.get<BrokerMetrics>('metrics');
      if (storedMetrics) {
        this.metrics = storedMetrics;
      }
    });
  }

  /**
   * Handle incoming HTTP requests
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket upgrade
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebSocketUpgrade(request);
    }

    // capnweb RPC over HTTP (batch mode for non-WebSocket clients)
    if (url.pathname === '/api' || url.pathname === '/rpc') {
      return newWorkersRpcResponse(request, this.graphApi);
    }

    // HTTP API endpoints
    switch (url.pathname) {
      case '/metrics':
        return this.handleMetrics();

      case '/state':
        return this.handleGetState();

      case '/reset':
        return this.handleReset();

      case '/health':
        return new Response(
          JSON.stringify({
            status: 'ok',
            connections: this.ctx.getWebSockets('broker-client').length,
            timestamp: Date.now(),
          }),
          { headers: { 'Content-Type': 'application/json' } }
        );

      default:
        return errorResponse(ErrorCode.NOT_FOUND, 'Endpoint not found', { path: url.pathname });
    }
  }

  /**
   * Handle WebSocket upgrade with hibernation
   *
   * Uses ctx.acceptWebSocket() for hibernation support - this is the key
   * pattern that enables the 95% cost discount and fresh subrequest quotas.
   */
  private handleWebSocketUpgrade(_request: Request): Response {
    // Create WebSocket pair
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // Create attachment for hibernation state preservation
    const attachment: WebSocketAttachment = {
      clientId: `client_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
      connectedAt: Date.now(),
      totalMessagesReceived: 0,
      totalSubrequestsMade: 0,
    };

    // Accept with hibernation - this is key!
    // Tags allow filtering WebSockets by purpose
    this.ctx.acceptWebSocket(server, ['broker-client']);

    // Store attachment data that persists across hibernation
    server.serializeAttachment(attachment);

    // Send welcome message before hibernating
    server.send(
      JSON.stringify({
        type: 'connected',
        clientId: attachment.clientId,
        message: 'BrokerDO connected. Ready for GraphDB queries.',
      })
    );

    return new Response(null, {
      status: 101,
      webSocket: client,
    } as ResponseInit);
  }

  /**
   * Handle WebSocket message - called on wake from hibernation
   *
   * CRITICAL: Each wake gets fresh 1000 subrequest quota!
   * This is what makes the hibernating pattern cost-effective.
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const startTime = Date.now();
    this.metrics.totalWakes++;
    this.metrics.lastWakeTimestamp = startTime;

    // Convert ArrayBuffer to string if needed
    const messageStr =
      typeof message === 'string' ? message : new TextDecoder().decode(message as ArrayBuffer);

    // Parse message with DoS protection (size checked BEFORE parsing)
    const parseResult = safeJsonParse<Record<string, unknown>>(messageStr);

    if (parseResult instanceof JsonParseError) {
      ws.send(JSON.stringify(parseResult.toResponse()));
      return;
    }

    const data = parseResult;

    // Restore attachment from hibernation
    const attachment = ws.deserializeAttachment() as WebSocketAttachment;
    if (!attachment) {
      ws.send(wsErrorJson(WsErrorCode.MISSING_ATTACHMENT, 'No attachment found'));
      return;
    }

    attachment.totalMessagesReceived++;

    // Handle different message types
    if (data['type'] === 'ping') {
      ws.send(
        JSON.stringify({
          type: 'pong',
          timestamp: data['timestamp'],
          serverTime: Date.now(),
          stateValue: this.stateValue,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (data['type'] === 'setState') {
      this.stateValue = data['value'] as number;
      await this.ctx.storage.put('stateValue', this.stateValue);
      ws.send(
        JSON.stringify({
          type: 'stateSet',
          value: this.stateValue,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (data['type'] === 'getState') {
      ws.send(
        JSON.stringify({
          type: 'state',
          value: this.stateValue,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    // Cursor storage for pagination across hibernation
    if (data['type'] === 'storeCursor') {
      const queryId = data['queryId'] as string;
      const cursor = data['cursor'] as string | undefined;

      if (!queryId) {
        ws.send(wsErrorJson(WsErrorCode.MISSING_PARAMETER, 'queryId is required for storeCursor', undefined, { parameter: 'queryId' }));
        ws.serializeAttachment(attachment);
        return;
      }

      // Initialize cursors map if not present
      if (!attachment.cursors) {
        attachment.cursors = {};
      }

      // Store the cursor (or remove if undefined)
      if (cursor) {
        attachment.cursors[queryId] = cursor;
      } else {
        delete attachment.cursors[queryId];
      }

      ws.send(
        JSON.stringify({
          type: 'cursorStored',
          queryId,
          success: true,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (data['type'] === 'getCursor') {
      const queryId = data['queryId'] as string;

      if (!queryId) {
        ws.send(wsErrorJson(WsErrorCode.MISSING_PARAMETER, 'queryId is required for getCursor', undefined, { parameter: 'queryId' }));
        ws.serializeAttachment(attachment);
        return;
      }

      const cursor = attachment.cursors?.[queryId];

      ws.send(
        JSON.stringify({
          type: 'cursor',
          queryId,
          cursor,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (data['type'] === 'clearCursor') {
      const queryId = data['queryId'] as string;

      if (!queryId) {
        ws.send(wsErrorJson(WsErrorCode.MISSING_PARAMETER, 'queryId is required for clearCursor', undefined, { parameter: 'queryId' }));
        ws.serializeAttachment(attachment);
        return;
      }

      if (attachment.cursors) {
        delete attachment.cursors[queryId];
      }

      ws.send(
        JSON.stringify({
          type: 'cursorCleared',
          queryId,
          success: true,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    // Handle query execution with pagination support
    if (data['type'] === 'query') {
      const query = data['query'] as string;
      const limit = (data['limit'] as number) ?? 100;
      const queryId = data['queryId'] as string | undefined;

      // Get cursor from request, or fall back to stored cursor for this query
      let cursor = data['cursor'] as string | undefined;
      if (!cursor && queryId && attachment.cursors?.[queryId]) {
        cursor = attachment.cursors[queryId];
      }

      try {
        // Execute query using GraphAPI with cursor support
        const queryOptions = cursor !== undefined ? { cursor, limit } : { limit };
        const result = await this.graphApi.query(query, queryOptions);

        // Store or clear cursor in attachment for hibernation survival
        if (queryId) {
          if (!attachment.cursors) {
            attachment.cursors = {};
          }

          if (result.cursor && result.hasMore) {
            // Store cursor for next page
            attachment.cursors[queryId] = result.cursor;
          } else {
            // Clear cursor when pagination is complete
            delete attachment.cursors[queryId];
          }
        }

        // Return result with pagination info
        ws.send(
          JSON.stringify({
            type: 'queryResult',
            queryId,
            result: {
              entities: result.entities,
              cursor: result.cursor,
              hasMore: result.hasMore,
              stats: result.stats,
            },
          })
        );
      } catch (error) {
        ws.send(wsErrorJson(WsErrorCode.QUERY_FAILED, error instanceof Error ? error.message : 'Query execution failed'));
      }
      ws.serializeAttachment(attachment);
      return;
    }

    // capnweb RPC message handling
    // Messages with 'method' field are RPC calls
    if (data['method']) {
      try {
        const result = await this.handleRpcCall(data);
        ws.send(JSON.stringify(result));
      } catch (error) {
        ws.send(wsErrorJson(
          WsErrorCode.RPC_ERROR,
          error instanceof Error ? error.message : 'Unknown error',
          data['id'] as string | undefined
        ));
      }
      ws.serializeAttachment(attachment);
      return;
    }

    // Handle batched RPC calls (for promise pipelining)
    // Each call in the batch can return a different type, so we use an array
    // of RPC result objects { type: 'result', id: string, result: unknown }
    if (Array.isArray(data['calls'])) {
      try {
        const results: Array<{ type: 'result'; id: string | undefined; result: unknown }> = [];
        for (const call of data['calls'] as Record<string, unknown>[]) {
          results.push(await this.handleRpcCall(call) as { type: 'result'; id: string | undefined; result: unknown });
        }
        ws.send(JSON.stringify({ id: data['id'], type: 'batch', results }));
      } catch (error) {
        ws.send(wsErrorJson(
          WsErrorCode.RPC_ERROR,
          error instanceof Error ? error.message : 'Unknown error',
          data['id'] as string | undefined
        ));
      }
      ws.serializeAttachment(attachment);
      return;
    }

    // Main operation: trigger N subrequests to shard DOs
    const requestedSubrequests = (data['subrequests'] as number) ?? 10;
    const messageId = (data['messageId'] as number) ?? attachment.totalMessagesReceived;

    // Validate requestedSubrequests is within allowed bounds (1-1000)
    // This prevents abuse and aligns with Cloudflare's subrequest quota per wake
    if (
      typeof requestedSubrequests !== 'number' ||
      !Number.isFinite(requestedSubrequests) ||
      requestedSubrequests < MIN_SUBREQUESTS ||
      requestedSubrequests > MAX_SUBREQUESTS
    ) {
      ws.send(wsErrorJson(
        WsErrorCode.VALIDATION_ERROR,
        `subrequests must be a number between ${MIN_SUBREQUESTS} and ${MAX_SUBREQUESTS}`,
        data['messageId'] as string | undefined,
        {
          parameter: 'subrequests',
          received: requestedSubrequests,
          min: MIN_SUBREQUESTS,
          max: MAX_SUBREQUESTS,
        }
      ));
      ws.serializeAttachment(attachment);
      return;
    }

    // Track results
    let successCount = 0;
    let failureCount = 0;
    const errors: string[] = [];

    // Determine shard using hash-based routing (FNV-1a)
    // If a subject is provided, route based on its namespace
    // Otherwise, use a default namespace
    const subject = data['subject'] as string | undefined;
    let shardIdName: string;

    if (subject) {
      // Use routeEntity for proper namespace extraction and hash-based routing
      try {
        const entityId = createEntityId(subject);
        const routeInfo = routeEntity(entityId);
        shardIdName = routeInfo.shardId;
      } catch (error) {
        ws.send(wsErrorJson(
          WsErrorCode.VALIDATION_ERROR,
          error instanceof Error ? error.message : 'Invalid subject entity ID',
          data['messageId'] as string | undefined,
          { subject }
        ));
        ws.serializeAttachment(attachment);
        return;
      }
    } else {
      // Default namespace when no subject provided
      const defaultNamespace = createNamespace('https://graphdb.default/');
      shardIdName = getShardId(defaultNamespace);
    }

    // Get Shard DO stub using the hash-based shard ID
    const shardId = this.env.SHARD.idFromName(shardIdName);
    const shardStub = this.env.SHARD.get(shardId);

    // Make subrequests in parallel batches to maximize throughput
    const batchSize = 50; // Process 50 at a time to avoid overwhelming
    const numSubrequests = Number(requestedSubrequests);
    const batches = Math.ceil(numSubrequests / batchSize);

    for (let batch = 0; batch < batches; batch++) {
      const batchStart = batch * batchSize;
      const batchEnd = Math.min(batchStart + batchSize, numSubrequests);
      const batchPromises: Promise<void>[] = [];

      for (let i = batchStart; i < batchEnd; i++) {
        const promise = (async () => {
          try {
            const response = await shardStub.fetch(
              new Request(`https://shard-do/count?messageId=${messageId}&index=${i}`)
            );

            if (response.ok) {
              successCount++;
            } else {
              failureCount++;
              const text = await response.text();
              if (errors.length < 5) {
                // Limit error collection
                errors.push(`Request ${i}: ${response.status} - ${text.slice(0, 100)}`);
              }
            }
          } catch (error) {
            failureCount++;
            if (errors.length < 5) {
              errors.push(`Request ${i}: ${error instanceof Error ? error.message : String(error)}`);
            }
          }
        })();

        batchPromises.push(promise);
      }

      await Promise.all(batchPromises);
    }

    const durationMs = Date.now() - startTime;

    // Update metrics
    this.metrics.totalSubrequests += successCount;
    this.metrics.totalFailures += failureCount;
    this.metrics.subrequestsPerWake.push(successCount);

    // Enforce rolling window to prevent unbounded growth
    if (this.metrics.subrequestsPerWake.length > MAX_SUBREQUESTS_PER_WAKE_ENTRIES) {
      // Remove oldest entries to maintain window size
      this.metrics.subrequestsPerWake = this.metrics.subrequestsPerWake.slice(
        this.metrics.subrequestsPerWake.length - MAX_SUBREQUESTS_PER_WAKE_ENTRIES
      );
    }

    // Persist metrics
    await this.ctx.storage.put('metrics', this.metrics);

    // Update attachment
    attachment.totalSubrequestsMade += successCount;
    ws.serializeAttachment(attachment);

    // Send result
    const result: SubrequestBatchResult = {
      messageId: Number(messageId),
      requestedCount: numSubrequests,
      successCount,
      failureCount,
      errors,
      durationMs,
      shardId: shardIdName,
    };

    ws.send(
      JSON.stringify({
        type: 'subrequestResult',
        result,
        metrics: {
          wakeNumber: this.metrics.totalWakes,
          totalSubrequestsThisSession: this.metrics.totalSubrequests,
          stateValue: this.stateValue,
        },
      })
    );
  }

  /**
   * Handle WebSocket close
   */
  async webSocketClose(ws: WebSocket, code: number, reason: string, _wasClean: boolean): Promise<void> {
    const attachment = ws.deserializeAttachment() as WebSocketAttachment;
    console.log(`WebSocket closed: ${attachment?.clientId}, code=${code}, reason=${reason}`);
  }

  /**
   * Handle WebSocket error
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('WebSocket error:', error);
    const attachment = ws.deserializeAttachment() as WebSocketAttachment;
    if (attachment) {
      console.error(`Client ${attachment.clientId} error:`, error);
    }
  }

  /**
   * Get metrics endpoint
   */
  private handleMetrics(): Response {
    // Get all connected WebSockets
    const sockets = this.ctx.getWebSockets('broker-client');

    return new Response(
      JSON.stringify({
        metrics: this.metrics,
        activeConnections: sockets.length,
        stateValue: this.stateValue,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Get state endpoint
   */
  private handleGetState(): Response {
    return new Response(
      JSON.stringify({
        stateValue: this.stateValue,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Reset metrics
   */
  private async handleReset(): Promise<Response> {
    this.metrics = {
      totalWakes: 0,
      totalSubrequests: 0,
      totalFailures: 0,
      subrequestsPerWake: [],
      lastWakeTimestamp: 0,
    };
    this.stateValue = 0;

    await this.ctx.storage.deleteAll();

    return new Response(
      JSON.stringify({
        message: 'Reset complete',
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Handle capnweb RPC call with type-safe parameter validation.
   *
   * This method processes incoming RPC requests and routes them to the GraphAPI.
   * Validates parameters before execution to ensure type safety and prevent
   * malformed requests from reaching the API layer.
   *
   * Supports promise pipelining by processing batched calls.
   */
  private async handleRpcCall(call: Record<string, unknown>): Promise<unknown> {
    // Convert to RpcCallMessage for validation
    const callId = call['id'] as string | undefined;
    const rpcCall: RpcCallMessage = {
      method: call['method'] as string,
      args: (call['args'] as unknown[]) ?? [],
    };
    // Only set id if defined (exactOptionalPropertyTypes compliance)
    if (callId !== undefined) {
      rpcCall.id = callId;
    }

    // Validate the RPC call parameters
    const validation = validateRpcCall(rpcCall);

    if (!validation.valid || !validation.params) {
      throw new Error(`Invalid RPC call: ${validation.error}`);
    }

    // Now we have type-safe params
    const params = validation.params;

    // Route to GraphAPI method with type-safe arguments
    switch (params.method) {
      case 'getEntity':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.getEntity(params.args[0]),
        };

      case 'createEntity':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.createEntity(params.args[0]),
        };

      case 'updateEntity':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.updateEntity(params.args[0], params.args[1]),
        };

      case 'deleteEntity':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.deleteEntity(params.args[0]),
        };

      case 'traverse':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.traverse(params.args[0], params.args[1], params.args[2]),
        };

      case 'reverseTraverse':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.reverseTraverse(params.args[0], params.args[1], params.args[2]),
        };

      case 'pathTraverse':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.pathTraverse(params.args[0], params.args[1], params.args[2]),
        };

      case 'query':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.query(params.args[0], params.args[1]),
        };

      case 'batchGet':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.batchGet(params.args[0]),
        };

      case 'batchCreate':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.batchCreate(params.args[0]),
        };

      case 'batchExecute':
        return {
          type: 'result',
          id: call['id'],
          result: await this.graphApi.batchExecute(params.args[0]),
        };

      default: {
        // TypeScript exhaustiveness check
        const _exhaustive: never = params;
        throw new Error(`Unhandled method: ${(_exhaustive as { method: string }).method}`);
      }
    }
  }
}
