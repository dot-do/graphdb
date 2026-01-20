/**
 * GraphDB Client SDK
 *
 * A minimal, user-friendly client SDK for GraphDB with:
 * - Auto-reconnection with exponential backoff
 * - Connection state management
 * - Simple method wrappers for common operations
 * - Full TypeScript support
 *
 * @example
 * ```typescript
 * import { createGraphClient } from '@dotdo/graphdb/client';
 *
 * const client = createGraphClient('wss://graph.example.com');
 *
 * // Insert entities
 * await client.insert({
 *   $id: 'https://example.com/user/1',
 *   $type: 'User',
 *   name: 'Alice',
 * });
 *
 * // Query entities
 * const user = await client.query('https://example.com/user/1');
 *
 * // Traverse relationships
 * const friends = await client.traverse('https://example.com/user/1', 'friends');
 *
 * // Close when done
 * client.close();
 * ```
 */

import type { Entity } from '../core/entity.js';
import { resolveNamespace } from '../core/entity.js';
import type {
  GraphClient,
  ClientOptions,
  ConnectionState,
  ConnectionStats,
  TraversalOptions,
  QueryResult,
  BatchResult,
  EntityInput,
  BatchOperation,
} from './types.js';
import type { RpcMethodName } from '../rpc/types.js';
import {
  withRpcRetry,
  createRetryConfig,
  type RetryConfig,
} from '../rpc/retry.js';

// Re-export types
export type {
  GraphClient,
  ClientOptions,
  ConnectionState,
  ConnectionStats,
  TraversalOptions,
  QueryResult,
  BatchResult,
  EntityInput,
  BatchOperation,
} from './types.js';

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_TIMEOUT = 30000;
const DEFAULT_RECONNECT_BASE_DELAY = 1000;
const DEFAULT_RECONNECT_MAX_DELAY = 30000;
const DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;

// ============================================================================
// Private Helpers
// ============================================================================

/**
 * Convert an EntityInput to a full Entity with resolved namespace info.
 * @internal
 */
function entityInputToEntity(input: EntityInput): Entity {
  const { $id, $type, $context, ...props } = input;

  // Resolve namespace info
  let namespace: string;
  let context: string;
  let localId: string;

  try {
    const resolved = resolveNamespace($id);
    namespace = resolved.namespace;
    context = $context || resolved.context;
    localId = resolved.localId;
  } catch {
    // Fallback if URL resolution fails
    namespace = '';
    context = $context || '';
    localId = '';
  }

  return {
    $id: $id as Entity['$id'],
    $type,
    $context: context,
    _namespace: namespace as Entity['_namespace'],
    _localId: localId,
    ...props,
  };
}

// ============================================================================
// Internal Types
// ============================================================================

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
}

/**
 * Expected structure of RPC response messages.
 * @internal
 */
interface RpcResponse {
  id: string;
  result?: unknown;
  error?: string;
}

/**
 * Type guard to validate RPC response structure.
 * @internal
 */
function isRpcResponse(value: unknown): value is RpcResponse {
  return (
    typeof value === 'object' &&
    value !== null &&
    'id' in value &&
    typeof (value as RpcResponse).id === 'string'
  );
}

// ============================================================================
// createGraphClient - Main Factory Function
// ============================================================================

/**
 * Create a new GraphDB client.
 *
 * @param urlOrOptions - WebSocket URL string or ClientOptions object
 * @returns GraphClient instance
 *
 * @example
 * ```typescript
 * // Simple usage with URL
 * const client = createGraphClient('wss://graph.example.com');
 *
 * // With options
 * const client = createGraphClient({
 *   url: 'wss://graph.example.com',
 *   autoReconnect: true,
 *   maxReconnectAttempts: 5,
 *   onConnect: () => console.log('Connected!'),
 *   onDisconnect: (reason) => console.log('Disconnected:', reason),
 * });
 * ```
 */
export function createGraphClient(urlOrOptions: string | ClientOptions): GraphClient {
  const options: ClientOptions =
    typeof urlOrOptions === 'string' ? { url: urlOrOptions } : urlOrOptions;

  const {
    url,
    autoReconnect = true,
    maxReconnectAttempts = DEFAULT_MAX_RECONNECT_ATTEMPTS,
    reconnectBaseDelay = DEFAULT_RECONNECT_BASE_DELAY,
    reconnectMaxDelay = DEFAULT_RECONNECT_MAX_DELAY,
    timeout = DEFAULT_TIMEOUT,
    onStateChange,
    onConnect,
    onDisconnect,
    onReconnect,
    onError,
    enableRetry = false,
    retryConfig: userRetryConfig,
    onRetry,
  } = options;

  // Build retry configuration if enabled
  // Note: We don't set onRetry here because we add method-specific callbacks in call()
  const retryConfig: RetryConfig | null = enableRetry
    ? createRetryConfig(userRetryConfig)
    : null;

  // Internal state
  let ws: WebSocket | null = null;
  let state: ConnectionState = 'disconnected';
  let requestId = 0;
  let reconnectAttempts = 0;
  let reconnectTimeoutId: ReturnType<typeof setTimeout> | null = null;
  let isClosing = false;

  const pendingRequests = new Map<string, PendingRequest>();

  // Stats
  const stats: ConnectionStats = {
    state: 'disconnected',
    connected: false,
    latencyMs: null,
    messagesSent: 0,
    messagesReceived: 0,
    reconnectAttempts: 0,
    lastConnectedAt: null,
    lastDisconnectedAt: null,
  };

  // -------------------------------------------------------------------------
  // State Management
  // -------------------------------------------------------------------------

  function setState(newState: ConnectionState): void {
    if (state !== newState) {
      state = newState;
      stats.state = newState;
      stats.connected = newState === 'connected';
      onStateChange?.(newState);
    }
  }

  // -------------------------------------------------------------------------
  // Connection Management
  // -------------------------------------------------------------------------

  function connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        resolve();
        return;
      }

      if (ws && ws.readyState === WebSocket.CONNECTING) {
        // Wait for existing connection
        const checkInterval = setInterval(() => {
          if (ws && ws.readyState === WebSocket.OPEN) {
            clearInterval(checkInterval);
            resolve();
          } else if (!ws || ws.readyState === WebSocket.CLOSED) {
            clearInterval(checkInterval);
            reject(new Error('Connection failed'));
          }
        }, 50);
        return;
      }

      setState('connecting');

      ws = new WebSocket(url);

      ws.addEventListener('open', () => {
        setState('connected');
        reconnectAttempts = 0;
        stats.lastConnectedAt = Date.now();
        onConnect?.();
        resolve();
      });

      ws.addEventListener('close', (event) => {
        const reason = event.reason || `Code: ${event.code}`;
        stats.lastDisconnectedAt = Date.now();

        if (!isClosing) {
          setState('disconnected');
          onDisconnect?.(reason);

          // Reject all pending requests
          for (const [id, pending] of pendingRequests) {
            clearTimeout(pending.timeoutId);
            pending.reject(new Error('Connection closed'));
            pendingRequests.delete(id);
          }

          // Auto-reconnect if enabled
          if (autoReconnect && reconnectAttempts < maxReconnectAttempts) {
            scheduleReconnect();
          }
        }
      });

      ws.addEventListener('error', () => {
        const error = new Error('WebSocket error');
        onError?.(error);

        if (state === 'connecting') {
          reject(error);
        }
      });

      ws.addEventListener('message', handleMessage);
    });
  }

  function handleMessage(event: MessageEvent): void {
    stats.messagesReceived++;

    try {
      const data: unknown = JSON.parse(event.data as string);

      // Validate response structure before accessing properties
      if (!isRpcResponse(data)) {
        onError?.(new Error('Invalid RPC response: missing or invalid id field'));
        return;
      }

      const pending = pendingRequests.get(data.id);

      if (pending) {
        clearTimeout(pending.timeoutId);
        pendingRequests.delete(data.id);

        if (data.error !== undefined) {
          pending.reject(new Error(data.error));
        } else {
          pending.resolve(data.result);
        }
      }
    } catch (e) {
      onError?.(new Error(`Failed to parse response: ${e}`));
    }
  }

  function scheduleReconnect(): void {
    if (reconnectTimeoutId) {
      clearTimeout(reconnectTimeoutId);
    }

    reconnectAttempts++;
    stats.reconnectAttempts = reconnectAttempts;

    // Exponential backoff with jitter
    const baseDelay = reconnectBaseDelay * Math.pow(2, reconnectAttempts - 1);
    const jitter = Math.random() * 0.3 * baseDelay;
    const delay = Math.min(baseDelay + jitter, reconnectMaxDelay);

    setState('reconnecting');
    onReconnect?.(reconnectAttempts);

    reconnectTimeoutId = setTimeout(() => {
      reconnectTimeoutId = null;
      connect().catch(() => {
        // Connection failed, scheduleReconnect will be called from close handler
      });
    }, delay);
  }

  // -------------------------------------------------------------------------
  // RPC Helper
  // -------------------------------------------------------------------------

  /**
   * Raw RPC call without retry logic.
   * @internal
   */
  function rawCall<T>(method: RpcMethodName, ...args: readonly unknown[]): Promise<T> {
    return new Promise((resolve, reject) => {
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        // Try to connect first
        connect()
          .then(() => rawCall<T>(method, ...args))
          .then(resolve)
          .catch(reject);
        return;
      }

      const id = `req-${++requestId}`;

      const timeoutId = setTimeout(() => {
        pendingRequests.delete(id);
        reject(new Error(`Request timeout after ${timeout}ms`));
      }, timeout);

      pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeoutId,
      });

      stats.messagesSent++;

      ws.send(JSON.stringify({ id, method, args }));
    });
  }

  /**
   * Type-safe RPC call helper with optional retry for transient failures.
   *
   * The method parameter is constrained to valid RPC method names,
   * providing compile-time safety for method calls.
   *
   * When retry is enabled, idempotent operations (getEntity, traverse, query, batchGet)
   * will be automatically retried on transient errors (network, timeout, 503).
   */
  async function call<T>(method: RpcMethodName, ...args: readonly unknown[]): Promise<T> {
    // If retry is disabled, just do a raw call
    if (!retryConfig) {
      return rawCall<T>(method, ...args);
    }

    // Use retry wrapper with method-specific config
    const retryConfigWithCallback: RetryConfig = {
      ...retryConfig,
      onRetry: (attempt, error, delayMs) => {
        onRetry?.(method, attempt, error, delayMs);
        retryConfig.onRetry?.(attempt, error, delayMs);
      },
    };

    const result = await withRpcRetry(
      method,
      () => rawCall<T>(method, ...args),
      retryConfigWithCallback
    );

    if (result.success) {
      return result.value as T;
    }

    throw result.error;
  }

  // -------------------------------------------------------------------------
  // Query Result Helper
  // -------------------------------------------------------------------------

  function isQueryResult(result: unknown): result is QueryResult {
    return (
      typeof result === 'object' &&
      result !== null &&
      'entities' in result &&
      'hasMore' in result &&
      'stats' in result
    );
  }

  // -------------------------------------------------------------------------
  // Client Implementation
  // -------------------------------------------------------------------------

  const client: GraphClient = {
    // CRUD Operations
    async insert(entity: EntityInput): Promise<void> {
      const fullEntity = entityInputToEntity(entity);
      return call<void>('createEntity', fullEntity);
    },

    async query(idOrQuery: string): Promise<Entity | null | QueryResult> {
      // If it looks like a URL, do a simple lookup
      if (idOrQuery.startsWith('http://') || idOrQuery.startsWith('https://')) {
        return call<Entity | null>('getEntity', idOrQuery);
      }

      // Otherwise, execute as a query string
      const result = await call<QueryResult>('query', idOrQuery);

      // If it's a simple lookup query that returned one entity, return just the entity
      if (
        isQueryResult(result) &&
        result.entities.length === 1 &&
        !result.hasMore &&
        !idOrQuery.includes('.')
      ) {
        return result.entities[0] ?? null;
      }

      return result;
    },

    async update(id: string, props: Record<string, unknown>): Promise<void> {
      return call<void>('updateEntity', id, props);
    },

    async delete(id: string): Promise<void> {
      return call<void>('deleteEntity', id);
    },

    // Traversal Operations
    async traverse(
      startId: string,
      predicate: string,
      options?: TraversalOptions
    ): Promise<Entity[]> {
      return call<Entity[]>('traverse', startId, predicate, options);
    },

    async reverseTraverse(
      targetId: string,
      predicate: string,
      options?: TraversalOptions
    ): Promise<Entity[]> {
      return call<Entity[]>('reverseTraverse', targetId, predicate, options);
    },

    async pathTraverse(
      startId: string,
      path: string[],
      options?: TraversalOptions
    ): Promise<Entity[]> {
      return call<Entity[]>('pathTraverse', startId, path, options);
    },

    // Batch Operations
    async batchGet(ids: string[]): Promise<BatchResult<Entity | null>> {
      return call<BatchResult<Entity | null>>('batchGet', ids);
    },

    async batchInsert(entities: EntityInput[]): Promise<BatchResult<void>> {
      const fullEntities = entities.map(entityInputToEntity);
      return call<BatchResult<void>>('batchCreate', fullEntities);
    },

    async batchExecute(operations: BatchOperation[]): Promise<BatchResult<unknown>> {
      // Convert operations to internal format
      const internalOps = operations.map((op) => {
        switch (op.type) {
          case 'get':
            return { type: 'get' as const, id: op.id };
          case 'create':
            return { type: 'create' as const, entity: entityInputToEntity(op.entity) };
          case 'update':
            return { type: 'update' as const, id: op.id, props: op.props };
          case 'delete':
            return { type: 'delete' as const, id: op.id };
        }
      });

      return call<BatchResult<unknown>>('batchExecute', internalOps);
    },

    // Connection Management
    isConnected(): boolean {
      return state === 'connected' && ws !== null && ws.readyState === WebSocket.OPEN;
    },

    getState(): ConnectionState {
      return state;
    },

    getStats(): ConnectionStats {
      return { ...stats };
    },

    async reconnect(): Promise<void> {
      if (reconnectTimeoutId) {
        clearTimeout(reconnectTimeoutId);
        reconnectTimeoutId = null;
      }

      if (ws) {
        ws.close();
        ws = null;
      }

      reconnectAttempts = 0;
      return connect();
    },

    close(): void {
      isClosing = true;

      if (reconnectTimeoutId) {
        clearTimeout(reconnectTimeoutId);
        reconnectTimeoutId = null;
      }

      // Reject all pending requests
      for (const [id, pending] of pendingRequests) {
        clearTimeout(pending.timeoutId);
        pending.reject(new Error('Client closed'));
        pendingRequests.delete(id);
      }

      if (ws) {
        ws.close();
        ws = null;
      }

      setState('disconnected');
    },
  };

  // Start connecting
  connect().catch(() => {
    // Initial connection failure will trigger auto-reconnect if enabled
  });

  return client;
}

// ============================================================================
// createGraphClientFromWebSocket - Create client from existing WebSocket
// ============================================================================

/**
 * Create a GraphDB client from an existing WebSocket connection.
 *
 * Useful when you have a WebSocket that was created elsewhere or when
 * integrating with existing WebSocket management.
 *
 * @param ws - Existing WebSocket instance
 * @param options - Optional client options (url is not used)
 * @returns GraphClient instance
 *
 * @example
 * ```typescript
 * const ws = new WebSocket('wss://graph.example.com');
 * const client = createGraphClientFromWebSocket(ws);
 * ```
 */
export function createGraphClientFromWebSocket(
  ws: WebSocket,
  options?: Omit<ClientOptions, 'url'>
): GraphClient {
  const {
    timeout = DEFAULT_TIMEOUT,
    onStateChange,
    onDisconnect,
    onError,
    enableRetry = false,
    retryConfig: userRetryConfig,
    onRetry,
  } = options || {};

  // Build retry configuration if enabled
  // Note: We don't set onRetry here because we add method-specific callbacks in call()
  const retryConfig: RetryConfig | null = enableRetry
    ? createRetryConfig(userRetryConfig)
    : null;

  // Internal state
  let state: ConnectionState = ws.readyState === WebSocket.OPEN ? 'connected' : 'connecting';
  let requestId = 0;

  const pendingRequests = new Map<string, PendingRequest>();

  // Stats
  const stats: ConnectionStats = {
    state,
    connected: state === 'connected',
    latencyMs: null,
    messagesSent: 0,
    messagesReceived: 0,
    reconnectAttempts: 0,
    lastConnectedAt: state === 'connected' ? Date.now() : null,
    lastDisconnectedAt: null,
  };

  function setState(newState: ConnectionState): void {
    if (state !== newState) {
      state = newState;
      stats.state = newState;
      stats.connected = newState === 'connected';
      onStateChange?.(newState);
    }
  }

  // Set up event listeners
  ws.addEventListener('open', () => {
    setState('connected');
    stats.lastConnectedAt = Date.now();
  });

  ws.addEventListener('close', (event) => {
    const reason = event.reason || `Code: ${event.code}`;
    stats.lastDisconnectedAt = Date.now();
    setState('disconnected');
    onDisconnect?.(reason);

    // Reject all pending requests
    for (const [id, pending] of pendingRequests) {
      clearTimeout(pending.timeoutId);
      pending.reject(new Error('Connection closed'));
      pendingRequests.delete(id);
    }
  });

  ws.addEventListener('error', () => {
    onError?.(new Error('WebSocket error'));
  });

  ws.addEventListener('message', (event: MessageEvent) => {
    stats.messagesReceived++;

    try {
      const data: unknown = JSON.parse(event.data as string);

      // Validate response structure before accessing properties
      if (!isRpcResponse(data)) {
        onError?.(new Error('Invalid RPC response: missing or invalid id field'));
        return;
      }

      const pending = pendingRequests.get(data.id);

      if (pending) {
        clearTimeout(pending.timeoutId);
        pendingRequests.delete(data.id);

        if (data.error !== undefined) {
          pending.reject(new Error(data.error));
        } else {
          pending.resolve(data.result);
        }
      }
    } catch (e) {
      onError?.(new Error(`Failed to parse response: ${e}`));
    }
  });

  /**
   * Raw RPC call without retry logic.
   * @internal
   */
  function rawCall<T>(method: RpcMethodName, ...args: readonly unknown[]): Promise<T> {
    return new Promise((resolve, reject) => {
      if (ws.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      const id = `req-${++requestId}`;

      const timeoutId = setTimeout(() => {
        pendingRequests.delete(id);
        reject(new Error(`Request timeout after ${timeout}ms`));
      }, timeout);

      pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeoutId,
      });

      stats.messagesSent++;

      ws.send(JSON.stringify({ id, method, args }));
    });
  }

  /**
   * Type-safe RPC call helper with optional retry for transient failures.
   *
   * The method parameter is constrained to valid RPC method names.
   */
  async function call<T>(method: RpcMethodName, ...args: readonly unknown[]): Promise<T> {
    // If retry is disabled, just do a raw call
    if (!retryConfig) {
      return rawCall<T>(method, ...args);
    }

    // Use retry wrapper with method-specific config
    const retryConfigWithCallback: RetryConfig = {
      ...retryConfig,
      onRetry: (attempt, error, delayMs) => {
        onRetry?.(method, attempt, error, delayMs);
        retryConfig.onRetry?.(attempt, error, delayMs);
      },
    };

    const result = await withRpcRetry(
      method,
      () => rawCall<T>(method, ...args),
      retryConfigWithCallback
    );

    if (result.success) {
      return result.value as T;
    }

    throw result.error;
  }

  function isQueryResult(result: unknown): result is QueryResult {
    return (
      typeof result === 'object' &&
      result !== null &&
      'entities' in result &&
      'hasMore' in result &&
      'stats' in result
    );
  }

  const client: GraphClient = {
    async insert(entity: EntityInput): Promise<void> {
      const fullEntity = entityInputToEntity(entity);
      return call<void>('createEntity', fullEntity);
    },

    async query(idOrQuery: string): Promise<Entity | null | QueryResult> {
      if (idOrQuery.startsWith('http://') || idOrQuery.startsWith('https://')) {
        return call<Entity | null>('getEntity', idOrQuery);
      }

      const result = await call<QueryResult>('query', idOrQuery);

      if (
        isQueryResult(result) &&
        result.entities.length === 1 &&
        !result.hasMore &&
        !idOrQuery.includes('.')
      ) {
        return result.entities[0] ?? null;
      }

      return result;
    },

    async update(id: string, props: Record<string, unknown>): Promise<void> {
      return call<void>('updateEntity', id, props);
    },

    async delete(id: string): Promise<void> {
      return call<void>('deleteEntity', id);
    },

    async traverse(
      startId: string,
      predicate: string,
      options?: TraversalOptions
    ): Promise<Entity[]> {
      return call<Entity[]>('traverse', startId, predicate, options);
    },

    async reverseTraverse(
      targetId: string,
      predicate: string,
      options?: TraversalOptions
    ): Promise<Entity[]> {
      return call<Entity[]>('reverseTraverse', targetId, predicate, options);
    },

    async pathTraverse(
      startId: string,
      path: string[],
      options?: TraversalOptions
    ): Promise<Entity[]> {
      return call<Entity[]>('pathTraverse', startId, path, options);
    },

    async batchGet(ids: string[]): Promise<BatchResult<Entity | null>> {
      return call<BatchResult<Entity | null>>('batchGet', ids);
    },

    async batchInsert(entities: EntityInput[]): Promise<BatchResult<void>> {
      const fullEntities = entities.map(entityInputToEntity);
      return call<BatchResult<void>>('batchCreate', fullEntities);
    },

    async batchExecute(operations: BatchOperation[]): Promise<BatchResult<unknown>> {
      const internalOps = operations.map((op) => {
        switch (op.type) {
          case 'get':
            return { type: 'get' as const, id: op.id };
          case 'create':
            return { type: 'create' as const, entity: entityInputToEntity(op.entity) };
          case 'update':
            return { type: 'update' as const, id: op.id, props: op.props };
          case 'delete':
            return { type: 'delete' as const, id: op.id };
        }
      });

      return call<BatchResult<unknown>>('batchExecute', internalOps);
    },

    isConnected(): boolean {
      return state === 'connected' && ws.readyState === WebSocket.OPEN;
    },

    getState(): ConnectionState {
      return state;
    },

    getStats(): ConnectionStats {
      return { ...stats };
    },

    async reconnect(): Promise<void> {
      throw new Error(
        'Cannot reconnect: WebSocket was provided externally via createGraphClientFromWebSocket(). ' +
        'Use createGraphClient() instead if you need auto-reconnection support, ' +
        'or manage the WebSocket connection manually.'
      );
    },

    close(): void {
      // Reject all pending requests
      for (const [id, pending] of pendingRequests) {
        clearTimeout(pending.timeoutId);
        pending.reject(new Error('Client closed'));
        pendingRequests.delete(id);
      }

      ws.close();
      setState('disconnected');
    },
  };

  return client;
}
