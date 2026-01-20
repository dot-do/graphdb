/**
 * Protocol Client - capnweb client SDK wrapper for GraphDB
 *
 * @deprecated This module is deprecated. Use `@dotdo/graphdb/client` instead for the
 * recommended client SDK with auto-reconnection, exponential backoff, and better
 * connection state management.
 *
 * Migration guide:
 * ```typescript
 * // Old (deprecated):
 * import { createGraphClient } from '@dotdo/graphdb/protocol';
 *
 * // New (recommended):
 * import { createGraphClient } from '@dotdo/graphdb/client';
 * ```
 *
 * The new client SDK provides:
 * - Auto-reconnection with exponential backoff
 * - Connection state lifecycle callbacks
 * - Request timeouts
 * - Cleaner API (insert, query, update, delete)
 *
 * This module is preserved for backward compatibility and for low-level
 * use cases that require direct capnweb integration or manual WebSocket handling.
 *
 * @module protocol/client
 * @internal
 */

import { newWebSocketRpcSession } from 'capnweb';
import type {
  GraphAPI,
  TraversalOptions,
  BatchResult,
} from './graph-api.js';
import type { Entity } from '../core/entity.js';
import type { TypedRpcCall, RpcMethodName } from '../rpc/types.js';

// ============================================================================
// Re-export from new client module for backward compatibility
// ============================================================================

// Re-export the new client SDK types with deprecation notices
// Users importing from protocol/client will get these, but should migrate
// to importing directly from client/

export {
  createGraphClient as createGraphClientV2,
  createGraphClientFromWebSocket as createGraphClientFromWebSocketV2,
} from '../client/index.js';

export type {
  GraphClient as GraphClientV2,
  ClientOptions,
  ConnectionState,
  ConnectionStats as ConnectionStatsV2,
  TraversalOptions as TraversalOptionsV2,
  QueryResult as QueryResultV2,
  BatchResult as BatchResultV2,
  EntityInput,
  BatchOperation,
} from '../client/index.js';

// ============================================================================
// Legacy Client Types (deprecated)
// ============================================================================

/**
 * @deprecated Use `ClientOptions` from `@dotdo/graphdb/client` instead.
 */
export interface GraphClientOptions {
  /** WebSocket URL to connect to */
  url: string;
  /** Connection timeout in ms */
  timeout?: number;
  /** Auto-reconnect on disconnect */
  autoReconnect?: boolean;
  /** Max reconnect attempts */
  maxReconnectAttempts?: number;
}

/**
 * @deprecated Use `GraphClient` from `@dotdo/graphdb/client` instead.
 *
 * This interface is kept for backward compatibility with code that
 * depends on the GraphAPI-style methods (getEntity, createEntity, etc.)
 */
export interface GraphClient extends GraphAPI {
  /** Close the connection */
  close(): void;
  /** Check if connected */
  isConnected(): boolean;
  /** Get connection stats */
  getStats(): ConnectionStats;
}

/**
 * @deprecated Use `ConnectionStats` from `@dotdo/graphdb/client` instead.
 * The new ConnectionStats has additional fields like `state`, `reconnectAttempts`,
 * `lastConnectedAt`, and `lastDisconnectedAt`.
 */
export interface ConnectionStats {
  connected: boolean;
  latencyMs: number | null;
  messagesReceived: number;
  messagesSent: number;
  reconnectCount: number;
}

// ============================================================================
// createGraphClient - Legacy factory function (deprecated)
// ============================================================================

/**
 * Create a capnweb GraphDB client from a WebSocket URL
 *
 * @deprecated Use `createGraphClient` from `@dotdo/graphdb/client` instead.
 * The new client provides auto-reconnection with exponential backoff,
 * connection state lifecycle callbacks, and request timeouts.
 *
 * @param urlOrOptions - WebSocket URL string or GraphClientOptions
 * @returns GraphClient with all GraphAPI methods
 *
 * @example
 * ```typescript
 * // Deprecated usage:
 * import { createGraphClient } from '@dotdo/graphdb/protocol';
 * const client = createGraphClient('wss://example.com/graph');
 *
 * // Recommended usage:
 * import { createGraphClient } from '@dotdo/graphdb/client';
 * const client = createGraphClient({
 *   url: 'wss://example.com/graph',
 *   autoReconnect: true,
 *   onConnect: () => console.log('Connected!'),
 * });
 * ```
 */
export function createGraphClient(urlOrOptions: string | GraphClientOptions): GraphClient {
  const options: GraphClientOptions =
    typeof urlOrOptions === 'string' ? { url: urlOrOptions } : urlOrOptions;

  const {
    url,
    timeout: _timeout = 30000,
    autoReconnect: _autoReconnect = true,
    maxReconnectAttempts: _maxReconnectAttempts = 5,
  } = options;
  void _timeout; void _autoReconnect; void _maxReconnectAttempts; // Reserved for future use

  // Stats tracking
  const stats: ConnectionStats = {
    connected: false,
    latencyMs: null,
    messagesReceived: 0,
    messagesSent: 0,
    reconnectCount: 0,
  };

  // Create capnweb WebSocket session
  // This returns a proxy that enables promise pipelining
  const session = newWebSocketRpcSession<GraphAPI>(url);

  // Create the client wrapper
  const client: GraphClient = {
    // GraphAPI methods - delegate to session
    getEntity: (id: string) => session.getEntity(id),
    createEntity: (entity: Entity) => session.createEntity(entity),
    updateEntity: (id: string, props: Record<string, unknown>) =>
      session.updateEntity(id, props),
    deleteEntity: (id: string) => session.deleteEntity(id),
    traverse: (
      startId: string,
      predicate: string,
      options?: TraversalOptions
    ) => session.traverse(startId, predicate, options),
    reverseTraverse: (
      targetId: string,
      predicate: string,
      options?: TraversalOptions
    ) => session.reverseTraverse(targetId, predicate, options),
    pathTraverse: (
      startId: string,
      path: string[],
      options?: TraversalOptions
    ) => session.pathTraverse(startId, path, options),
    query: (queryString: string) => session.query(queryString),
    batchGet: (ids: string[]) => session.batchGet(ids),
    batchCreate: (entities: Entity[]) => session.batchCreate(entities),
    batchExecute: (
      operations: Array<{
        type: 'get' | 'create' | 'update' | 'delete';
        id?: string;
        entity?: Entity;
        props?: Record<string, unknown>;
      }>
    ) => session.batchExecute(operations),

    // Client-specific methods
    close: () => {
      // Dispose the capnweb session
      const disposable = session as unknown as { dispose?: () => void };
      disposable.dispose?.();
      stats.connected = false;
    },
    isConnected: () => stats.connected,
    getStats: () => ({ ...stats }),
  };

  // Mark as connected (capnweb handles connection internally)
  stats.connected = true;

  return client;
}

/**
 * Create a capnweb GraphDB client from an existing WebSocket
 *
 * @deprecated Use `createGraphClientFromWebSocket` from `@dotdo/graphdb/client` instead.
 * The new client provides better connection state management and request timeouts.
 *
 * @param ws - WebSocket instance (already connected or connecting)
 * @returns GraphClient with all GraphAPI methods
 *
 * @example
 * ```typescript
 * // Deprecated usage:
 * import { createGraphClientFromWebSocket } from '@dotdo/graphdb/protocol';
 * const ws = new WebSocket('wss://example.com/graph');
 * const client = createGraphClientFromWebSocket(ws);
 *
 * // Recommended usage:
 * import { createGraphClientFromWebSocket } from '@dotdo/graphdb/client';
 * const ws = new WebSocket('wss://example.com/graph');
 * const client = createGraphClientFromWebSocket(ws, {
 *   timeout: 30000,
 *   onDisconnect: (reason) => console.log('Disconnected:', reason),
 * });
 * ```
 */
export function createGraphClientFromWebSocket(ws: WebSocket): GraphClient {
  // Stats tracking
  const stats: ConnectionStats = {
    connected: ws.readyState === WebSocket.OPEN,
    latencyMs: null,
    messagesReceived: 0,
    messagesSent: 0,
    reconnectCount: 0,
  };

  // Create a capnweb session from existing WebSocket
  // Note: capnweb's newWebSocketRpcSession expects a URL, so we need to wrap
  // the WebSocket in a simple RPC handler
  const pendingRequests = new Map<
    string,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();
  let requestId = 0;

  // Handle incoming messages
  ws.addEventListener('message', (event: MessageEvent) => {
    stats.messagesReceived++;
    try {
      const data = JSON.parse(event.data as string);
      const pending = pendingRequests.get(data.id);
      if (pending) {
        pendingRequests.delete(data.id);
        if (data.error) {
          pending.reject(new Error(data.error));
        } else {
          pending.resolve(data.result);
        }
      }
    } catch (e) {
      console.error('Failed to parse response:', e);
    }
  });

  ws.addEventListener('open', () => {
    stats.connected = true;
  });

  ws.addEventListener('close', () => {
    stats.connected = false;
    for (const pending of pendingRequests.values()) {
      pending.reject(new Error('Connection closed'));
    }
    pendingRequests.clear();
  });

  // RPC call helper with type-safe method signatures
  // Using TypedRpcCall for the public interface while keeping internal flexibility
  const call: TypedRpcCall = ((method: RpcMethodName, ...args: unknown[]): Promise<unknown> => {
    return new Promise((resolve, reject) => {
      if (ws.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const id = `req-${++requestId}`;
      stats.messagesSent++;

      pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
      });

      ws.send(JSON.stringify({ id, method, args }));
    });
  }) as TypedRpcCall;

  // Create the client wrapper
  const client: GraphClient = {
    getEntity: (id: string) => call('getEntity', id),
    createEntity: (entity: Entity) => call('createEntity', entity),
    updateEntity: (id: string, props: Record<string, unknown>) =>
      call('updateEntity', id, props),
    deleteEntity: (id: string) => call('deleteEntity', id),
    traverse: (startId: string, predicate: string, options?: TraversalOptions) =>
      call('traverse', startId, predicate, options),
    reverseTraverse: (
      targetId: string,
      predicate: string,
      options?: TraversalOptions
    ) => call('reverseTraverse', targetId, predicate, options),
    pathTraverse: (
      startId: string,
      path: string[],
      options?: TraversalOptions
    ) => call('pathTraverse', startId, path, options),
    query: (queryString: string) => call('query', queryString),
    batchGet: (ids: string[]) => call('batchGet', ids),
    batchCreate: (entities: Entity[]) =>
      call('batchCreate', entities),
    batchExecute: (
      operations: Array<{
        type: 'get' | 'create' | 'update' | 'delete';
        id?: string;
        entity?: Entity;
        props?: Record<string, unknown>;
      }>
    ) => call('batchExecute', operations),

    close: () => {
      ws.close();
      stats.connected = false;
    },
    isConnected: () => stats.connected,
    getStats: () => ({ ...stats }),
  };

  return client;
}

// ============================================================================
// Promise Pipelining Examples (kept for documentation/reference)
// ============================================================================

/**
 * Demonstrate promise pipelining for chained traversals
 *
 * @deprecated This is a documentation example. For production use,
 * consider using the new client SDK from `@dotdo/graphdb/client`.
 *
 * With promise pipelining, multiple dependent calls are executed
 * in a single round-trip instead of waiting for each one.
 *
 * @example
 * ```typescript
 * // Without pipelining: 3 round trips
 * const user = await client.getEntity('https://example.com/user/1');
 * const friends = await client.traverse(user.$id, 'friends');
 * const posts = await Promise.all(friends.map(f => client.traverse(f.$id, 'posts')));
 *
 * // With pipelining: 1 round trip
 * const posts = await pipelineExample(client, 'https://example.com/user/1');
 * ```
 */
export async function pipelineExample(
  client: GraphAPI,
  userId: string
): Promise<Entity[]> {
  // capnweb enables calling methods on promises without awaiting
  // This creates a pipeline that executes in a single round-trip

  // Get user -> traverse friends -> get their posts
  // All three operations are pipelined
  const posts = await client.pathTraverse(userId, ['friends', 'posts']);

  return posts;
}

/**
 * Demonstrate batch operations for bulk data transfer
 *
 * @deprecated This is a documentation example. For production use,
 * consider using the new client SDK from `@dotdo/graphdb/client`.
 *
 * Batching 100 requests in a single WebSocket frame for efficiency.
 */
export async function batchExample(
  client: GraphAPI
): Promise<BatchResult<Entity | null>> {
  // Generate 100 entity IDs
  const ids = Array.from(
    { length: 100 },
    (_, i) => `https://example.com/entity/${i}`
  );

  // Batch get all entities in a single request
  const result = await client.batchGet(ids);

  return result;
}

/**
 * Demonstrate the .map() transform feature
 *
 * @deprecated This is a documentation example. For production use,
 * consider using the new client SDK from `@dotdo/graphdb/client`.
 *
 * The .map() method allows transforming arrays remotely without
 * pulling all data to the client first.
 *
 * @example
 * ```typescript
 * // Without .map(): pulls all entities, then looks up names
 * const entities = await client.batchGet(['https://example.com/user/1', 'https://example.com/user/2']);
 * const names = entities.results.map(e => e?.name);
 *
 * // With .map(): transforms on server, returns only names
 * // (This is a conceptual example - actual implementation depends on capnweb version)
 * ```
 */
export async function mapExample(
  client: GraphAPI,
  userIds: string[]
): Promise<string[]> {
  // Get all users
  const result = await client.batchGet(userIds);

  // Extract names (done locally for this spike, but demonstrates the pattern)
  const names = result.results
    .filter((e): e is Entity => e !== null)
    .map((e) => e['name'] as string);

  return names;
}

// ============================================================================
// ManualWebSocketClient (kept for testing/benchmarking)
// ============================================================================

/**
 * Low-level WebSocket client for testing capnweb protocol
 *
 * This class is useful for benchmarking and testing the raw protocol
 * without the capnweb client library overhead.
 *
 * Note: This class is designed for browser/Node.js environments.
 * For Cloudflare Workers, use the capnweb client directly.
 *
 * @internal For testing and benchmarking purposes only.
 */
export class ManualWebSocketClient {
  private ws: globalThis.WebSocket | null = null;
  private pendingRequests: Map<
    string,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  > = new Map();
  private requestId = 0;

  constructor(private url: string) {}

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Use globalThis.WebSocket for browser/Node.js compatibility
      const WebSocketImpl =
        globalThis.WebSocket ||
        (globalThis as unknown as { WebSocket: typeof WebSocket }).WebSocket;
      this.ws = new WebSocketImpl(this.url) as globalThis.WebSocket;

      this.ws.addEventListener('open', () => resolve());
      this.ws.addEventListener('error', () =>
        reject(new Error('WebSocket error'))
      );
      this.ws.addEventListener('close', () => {
        for (const pending of this.pendingRequests.values()) {
          pending.reject(new Error('Connection closed'));
        }
        this.pendingRequests.clear();
      });

      this.ws.addEventListener('message', (event: MessageEvent) => {
        try {
          const data = JSON.parse(event.data as string);
          const pending = this.pendingRequests.get(data.id);
          if (pending) {
            this.pendingRequests.delete(data.id);
            if (data.error) {
              pending.reject(new Error(data.error));
            } else {
              pending.resolve(data.result);
            }
          }
        } catch (e) {
          console.error('Failed to parse response:', e);
        }
      });
    });
  }

  /**
   * Make a type-safe RPC call.
   *
   * @param method - RPC method name (must be a valid RpcMethodName)
   * @param args - Method arguments (type-checked at runtime)
   * @returns Promise resolving to the method result
   */
  async call<T>(method: RpcMethodName, ...args: Parameters<TypedRpcCall>extends [infer _M, ...infer Rest] ? Rest : never[]): Promise<T> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      const wsState = this.ws ? ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][this.ws.readyState] : 'null';
      throw new Error(
        `RPC call failed: not connected to server. ` +
        `Method: "${method}", WebSocket state: ${wsState}. ` +
        `Ensure the client is connected before making calls.`
      );
    }

    const id = `req-${++this.requestId}`;

    return new Promise((resolve, reject) => {
      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
      });

      this.ws!.send(
        JSON.stringify({
          id,
          method,
          args,
        })
      );
    });
  }

  /**
   * Send multiple calls in a single frame (batching).
   *
   * Each call must specify a valid RPC method name and typed arguments.
   */
  async batchCall<T>(
    calls: Array<{ method: RpcMethodName; args: readonly unknown[] }>
  ): Promise<T[]> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      const wsState = this.ws ? ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][this.ws.readyState] : 'null';
      throw new Error(
        `Batch RPC call failed: not connected to server. ` +
        `Batch size: ${calls.length}, WebSocket state: ${wsState}. ` +
        `Ensure the client is connected before making calls.`
      );
    }

    const batchId = `batch-${++this.requestId}`;

    return new Promise((resolve, reject) => {
      this.pendingRequests.set(batchId, {
        resolve: (value: unknown) => {
          const results = value as { results: T[] };
          resolve(results.results);
        },
        reject,
      });

      this.ws!.send(
        JSON.stringify({
          id: batchId,
          calls: calls.map((c, i) => ({
            id: `${batchId}-${i}`,
            method: c.method,
            args: c.args,
          })),
        })
      );
    });
  }

  close(): void {
    this.ws?.close();
    this.ws = null;
  }
}
