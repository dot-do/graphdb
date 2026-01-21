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
import { errorResponse, ErrorCode, wsErrorJson, WsErrorCode, WsRpcError } from '../errors/api-error.js';
import { routeEntity, getShardId } from '../snippet/router.js';
import { createEntityId, createNamespace } from '../core/types.js';
import { validateRpcCall, type RpcCallMessage, type PingResponse, type ExecuteSubrequestsResponse } from '../rpc/types.js';

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
 * Number of metric updates before flushing to storage.
 * This batches writes to reduce storage operations while ensuring
 * metrics survive DO eviction with minimal data loss.
 */
const METRICS_FLUSH_THRESHOLD = 10;

/**
 * Interval in milliseconds for alarm-based metrics persistence.
 * Ensures metrics are saved even during periods of low activity.
 * Default: 60 seconds (1 minute)
 */
const METRICS_ALARM_INTERVAL_MS = 60_000;

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

  // Metrics persisted in memory with periodic flush to storage
  // Survives DO eviction (not just hibernation)
  private metrics: BrokerMetrics = {
    totalWakes: 0,
    totalSubrequests: 0,
    totalFailures: 0,
    subrequestsPerWake: [],
    lastWakeTimestamp: 0,
  };

  // Counter for batching metrics storage writes
  private metricsDirtyCount: number = 0;

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

      // Schedule initial alarm for periodic metrics persistence
      // This ensures metrics are saved even during low activity periods
      await this.scheduleMetricsAlarm();
    });
  }

  /**
   * Flush metrics to durable storage.
   * Called periodically based on METRICS_FLUSH_THRESHOLD or by alarm.
   */
  private async flushMetrics(): Promise<void> {
    await this.ctx.storage.put('metrics', this.metrics);
    this.metricsDirtyCount = 0;
  }

  /**
   * Mark metrics as dirty and flush if threshold reached.
   * This batches writes to reduce storage operations.
   */
  private async markMetricsDirty(): Promise<void> {
    this.metricsDirtyCount++;
    if (this.metricsDirtyCount >= METRICS_FLUSH_THRESHOLD) {
      await this.flushMetrics();
    }
  }

  /**
   * Schedule alarm for periodic metrics persistence.
   * Only schedules if no alarm is currently set.
   */
  private async scheduleMetricsAlarm(): Promise<void> {
    const currentAlarm = await this.ctx.storage.getAlarm();
    if (currentAlarm === null) {
      await this.ctx.storage.setAlarm(Date.now() + METRICS_ALARM_INTERVAL_MS);
    }
  }

  /**
   * Handle alarm - flush dirty metrics and reschedule.
   * This ensures metrics survive DO eviction even during low activity.
   */
  async alarm(): Promise<void> {
    // Flush any dirty metrics to storage
    if (this.metricsDirtyCount > 0) {
      await this.flushMetrics();
    }

    // Reschedule alarm for continuous protection
    await this.ctx.storage.setAlarm(Date.now() + METRICS_ALARM_INTERVAL_MS);
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

    // Mark metrics dirty - will be flushed after threshold or by alarm
    await this.markMetricsDirty();

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

    // ========================================================================
    // ALL messages are now routed through the capnweb RPC system for consistency.
    // Legacy 'type'-based messages are converted to RPC format for backward compatibility.
    // ========================================================================

    // Convert legacy 'type'-based messages to RPC format
    // This maintains backward compatibility while standardizing on capnweb RPC
    const rpcData = this.convertLegacyToRpc(data, attachment);

    // Handle batched RPC calls (for promise pipelining)
    if (Array.isArray(rpcData['calls'])) {
      try {
        const results: Array<{ type: 'result'; id: string | undefined; result: unknown }> = [];
        for (const call of rpcData['calls'] as Record<string, unknown>[]) {
          results.push(await this.handleRpcCall(call, ws, attachment) as { type: 'result'; id: string | undefined; result: unknown });
        }
        ws.send(JSON.stringify({ id: rpcData['id'], type: 'batch', results }));
      } catch (error) {
        // Preserve WsRpcError codes for proper error categorization
        if (error instanceof WsRpcError) {
          ws.send(error.toJson());
        } else {
          ws.send(wsErrorJson(
            WsErrorCode.RPC_ERROR,
            error instanceof Error ? error.message : 'Unknown error',
            rpcData['id'] as string | undefined
          ));
        }
      }
      ws.serializeAttachment(attachment);
      return;
    }

    // Handle single RPC call (including converted legacy messages)
    if (rpcData['method']) {
      try {
        const result = await this.handleRpcCall(rpcData, ws, attachment);
        ws.send(JSON.stringify(result));
      } catch (error) {
        // Preserve WsRpcError codes for proper error categorization
        if (error instanceof WsRpcError) {
          ws.send(error.toJson());
        } else {
          ws.send(wsErrorJson(
            WsErrorCode.RPC_ERROR,
            error instanceof Error ? error.message : 'Unknown error',
            rpcData['id'] as string | undefined
          ));
        }
      }
      ws.serializeAttachment(attachment);
      return;
    }

    // Unknown message format
    ws.send(wsErrorJson(
      WsErrorCode.INVALID_REQUEST,
      'Unknown message format. Use capnweb RPC format: { method: string, args: unknown[], id?: string }',
      undefined,
      { receivedKeys: Object.keys(data) }
    ));
    ws.serializeAttachment(attachment);
  }

  /**
   * Convert legacy 'type'-based messages to capnweb RPC format.
   * This maintains backward compatibility while standardizing all messages on RPC.
   *
   * Legacy format: { type: 'ping', timestamp: 123 }
   * RPC format: { method: 'ping', args: [123], id?: string }
   */
  private convertLegacyToRpc(
    data: Record<string, unknown>,
    attachment: WebSocketAttachment
  ): Record<string, unknown> {
    // If already in RPC format (has 'method' or 'calls'), return as-is
    if (data['method'] || data['calls']) {
      return data;
    }

    const type = data['type'] as string | undefined;

    // Convert legacy message types to RPC format
    switch (type) {
      case 'ping':
        return {
          method: 'ping',
          args: data['timestamp'] !== undefined ? [data['timestamp']] : [],
        };

      case 'setState':
        return {
          method: 'setState',
          args: [data['value']],
        };

      case 'getState':
        return {
          method: 'getState',
          args: [],
        };

      case 'storeCursor':
        return {
          method: 'storeCursor',
          args: data['cursor'] !== undefined
            ? [data['queryId'], data['cursor']]
            : [data['queryId']],
        };

      case 'getCursor':
        return {
          method: 'getCursor',
          args: [data['queryId']],
        };

      case 'clearCursor':
        return {
          method: 'clearCursor',
          args: [data['queryId']],
        };

      case 'query':
        // Legacy query format with optional cursor from attachment
        return {
          method: 'query',
          args: [
            data['query'],
            {
              limit: data['limit'] ?? 100,
              cursor: data['cursor'],
            },
          ],
          // Pass queryId for cursor storage in attachment
          _queryId: data['queryId'],
        };

      default:
        // Legacy subrequest format (no type, just { subrequests: N })
        if (data['subrequests'] !== undefined || !type) {
          return {
            method: 'executeSubrequests',
            args: [
              data['subrequests'] ?? 10,
              data['messageId'] ?? attachment.totalMessagesReceived,
              data['subject'],
            ],
          };
        }

        // Return original data (will be handled as unknown format)
        return data;
    }
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
   * This method processes incoming RPC requests and routes them to the GraphAPI
   * or handles utility methods (ping, setState, cursor operations, etc.).
   * Validates parameters before execution to ensure type safety and prevent
   * malformed requests from reaching the API layer.
   *
   * Supports promise pipelining by processing batched calls.
   *
   * @param call - The RPC call message
   * @param ws - WebSocket for attachment access (optional, needed for cursor operations)
   * @param attachment - WebSocket attachment for cursor storage (optional)
   */
  private async handleRpcCall(
    call: Record<string, unknown>,
    _ws?: WebSocket,
    attachment?: WebSocketAttachment
  ): Promise<unknown> {
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

    // Route to appropriate handler based on method
    switch (params.method) {
      // ======================================================================
      // GraphAPI Methods
      // ======================================================================

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

      case 'query': {
        const queryResult = await this.graphApi.query(params.args[0], params.args[1]);

        // Handle cursor storage in attachment for legacy query format
        const queryId = call['_queryId'] as string | undefined;
        if (queryId && attachment) {
          if (!attachment.cursors) {
            attachment.cursors = {};
          }
          if (queryResult.cursor && queryResult.hasMore) {
            attachment.cursors[queryId] = queryResult.cursor;
          } else {
            delete attachment.cursors[queryId];
          }
        }

        return {
          type: 'result',
          id: call['id'],
          result: queryResult,
        };
      }

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

      // ======================================================================
      // Utility Methods (migrated from ad-hoc messages)
      // ======================================================================

      case 'ping': {
        const pingResult: PingResponse = {
          serverTime: Date.now(),
          stateValue: this.stateValue,
        };
        if (params.args[0] !== undefined) {
          pingResult.timestamp = params.args[0];
        }
        // Also send legacy 'pong' type for backward compatibility
        return {
          type: 'pong', // Legacy type for backward compatibility
          id: call['id'],
          ...pingResult,
        };
      }

      case 'setState': {
        this.stateValue = params.args[0];
        await this.ctx.storage.put('stateValue', this.stateValue);
        return {
          type: 'stateSet', // Legacy type for backward compatibility
          id: call['id'],
          value: this.stateValue,
        };
      }

      case 'getState':
        return {
          type: 'state', // Legacy type for backward compatibility
          id: call['id'],
          value: this.stateValue,
        };

      case 'storeCursor': {
        const queryIdStore = params.args[0];
        const cursorValue = params.args[1];

        if (attachment) {
          if (!attachment.cursors) {
            attachment.cursors = {};
          }
          if (cursorValue) {
            attachment.cursors[queryIdStore] = cursorValue;
          } else {
            delete attachment.cursors[queryIdStore];
          }
        }

        return {
          type: 'cursorStored', // Legacy type for backward compatibility
          id: call['id'],
          queryId: queryIdStore,
          success: true,
        };
      }

      case 'getCursor': {
        const queryIdGet = params.args[0];
        const storedCursor = attachment?.cursors?.[queryIdGet];

        return {
          type: 'cursor', // Legacy type for backward compatibility
          id: call['id'],
          queryId: queryIdGet,
          cursor: storedCursor,
        };
      }

      case 'clearCursor': {
        const queryIdClear = params.args[0];

        if (attachment?.cursors) {
          delete attachment.cursors[queryIdClear];
        }

        return {
          type: 'cursorCleared', // Legacy type for backward compatibility
          id: call['id'],
          queryId: queryIdClear,
          success: true,
        };
      }

      case 'executeSubrequests': {
        const startTime = Date.now();
        const requestedSubrequests = params.args[0];
        const messageId = params.args[1] ?? (attachment?.totalMessagesReceived ?? 0);
        const subject = params.args[2];

        // Validate requestedSubrequests is within allowed bounds (1-1000)
        if (
          !Number.isFinite(requestedSubrequests) ||
          requestedSubrequests < MIN_SUBREQUESTS ||
          requestedSubrequests > MAX_SUBREQUESTS
        ) {
          throw new WsRpcError(
            WsErrorCode.VALIDATION_ERROR,
            `subrequests must be a number between ${MIN_SUBREQUESTS} and ${MAX_SUBREQUESTS}`,
            call['id'] as string | undefined,
            {
              parameter: 'subrequests',
              received: requestedSubrequests,
              min: MIN_SUBREQUESTS,
              max: MAX_SUBREQUESTS,
            }
          );
        }

        // Track results
        let successCount = 0;
        let failureCount = 0;
        const errors: string[] = [];

        // Determine shard using hash-based routing
        let shardIdName: string;

        if (subject) {
          try {
            const entityId = createEntityId(subject);
            const routeInfo = routeEntity(entityId);
            shardIdName = routeInfo.shardId;
          } catch (error) {
            throw new WsRpcError(
              WsErrorCode.VALIDATION_ERROR,
              `Invalid subject entity ID: ${error instanceof Error ? error.message : String(error)}`,
              call['id'] as string | undefined,
              { subject }
            );
          }
        } else {
          const defaultNamespace = createNamespace('https://graphdb.default/');
          shardIdName = getShardId(defaultNamespace);
        }

        // Get Shard DO stub
        const shardId = this.env.SHARD.idFromName(shardIdName);
        const shardStub = this.env.SHARD.get(shardId);

        // Make subrequests in parallel batches
        const batchSize = 50;
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

        // Enforce rolling window
        if (this.metrics.subrequestsPerWake.length > MAX_SUBREQUESTS_PER_WAKE_ENTRIES) {
          this.metrics.subrequestsPerWake = this.metrics.subrequestsPerWake.slice(
            this.metrics.subrequestsPerWake.length - MAX_SUBREQUESTS_PER_WAKE_ENTRIES
          );
        }

        await this.markMetricsDirty();

        // Update attachment
        if (attachment) {
          attachment.totalSubrequestsMade += successCount;
        }

        const subrequestResult: ExecuteSubrequestsResponse = {
          messageId: Number(messageId),
          requestedCount: numSubrequests,
          successCount,
          failureCount,
          errors,
          durationMs,
          shardId: shardIdName,
          metrics: {
            wakeNumber: this.metrics.totalWakes,
            totalSubrequestsThisSession: this.metrics.totalSubrequests,
            stateValue: this.stateValue,
          },
        };

        return {
          type: 'subrequestResult', // Legacy type for backward compatibility
          id: call['id'],
          result: subrequestResult,
          metrics: subrequestResult.metrics,
        };
      }

      default: {
        // TypeScript exhaustiveness check
        const _exhaustive: never = params;
        throw new Error(`Unhandled method: ${(_exhaustive as { method: string }).method}`);
      }
    }
  }
}
