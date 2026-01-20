/**
 * Client SDK Types for GraphDB
 *
 * This module defines the public types for the GraphDB client SDK.
 * Designed for ease of use with full TypeScript support.
 */

import type { Entity } from '../core/entity.js';
import type { RetryConfig } from '../rpc/retry.js';

// ============================================================================
// Connection & Client Options
// ============================================================================

/**
 * Connection state enum for tracking WebSocket connection status.
 */
export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

/**
 * Options for configuring the GraphDB client.
 *
 * @example
 * ```typescript
 * const options: ClientOptions = {
 *   url: 'wss://graph.example.com/v1',
 *   autoReconnect: true,
 *   maxReconnectAttempts: 5,
 *   reconnectBaseDelay: 1000,
 *   timeout: 30000,
 * };
 * ```
 */
export interface ClientOptions {
  /** WebSocket URL to connect to (required) */
  url: string;

  /** Auto-reconnect on connection loss (default: true) */
  autoReconnect?: boolean;

  /** Maximum reconnection attempts before giving up (default: 10) */
  maxReconnectAttempts?: number;

  /** Base delay in ms for exponential backoff (default: 1000) */
  reconnectBaseDelay?: number;

  /** Maximum delay in ms between reconnect attempts (default: 30000) */
  reconnectMaxDelay?: number;

  /** Request timeout in ms (default: 30000) */
  timeout?: number;

  /** Called when connection state changes */
  onStateChange?: (state: ConnectionState) => void;

  /** Called when connection is established */
  onConnect?: () => void;

  /** Called when connection is lost */
  onDisconnect?: (reason?: string) => void;

  /** Called on reconnection attempt */
  onReconnect?: (attempt: number) => void;

  /** Called on error */
  onError?: (error: Error) => void;

  /**
   * Enable automatic retry for transient failures on idempotent operations.
   * When enabled, read operations (getEntity, traverse, query, batchGet)
   * will be automatically retried on network errors, timeouts, or 503 responses.
   * @default false
   */
  enableRetry?: boolean;

  /**
   * Retry configuration for transient failures.
   * Only used when enableRetry is true.
   */
  retryConfig?: Partial<RetryConfig>;

  /**
   * Callback for retry events (useful for logging/monitoring).
   * Called before each retry attempt.
   */
  onRetry?: (method: string, attempt: number, error: Error, delayMs: number) => void;
}

/**
 * Connection statistics for monitoring client health.
 */
export interface ConnectionStats {
  /** Current connection state */
  state: ConnectionState;

  /** Whether client is currently connected */
  connected: boolean;

  /** Round-trip latency in ms (from last ping or null if unknown) */
  latencyMs: number | null;

  /** Total messages sent */
  messagesSent: number;

  /** Total messages received */
  messagesReceived: number;

  /** Number of reconnection attempts */
  reconnectAttempts: number;

  /** Timestamp of last successful connection */
  lastConnectedAt: number | null;

  /** Timestamp of last disconnection */
  lastDisconnectedAt: number | null;
}

// ============================================================================
// Query Types
// ============================================================================

/**
 * Options for traversal operations.
 */
export interface TraversalOptions {
  /** Maximum traversal depth (default: 1 for single hop) */
  maxDepth?: number;

  /** Maximum number of results to return (default: 100) */
  limit?: number;

  /** Pagination cursor from previous response */
  cursor?: string;

  /** Filter results by property values */
  filter?: Record<string, unknown>;
}

/**
 * Result of a query operation.
 */
export interface QueryResult {
  /** Resulting entities */
  entities: Entity[];

  /** Pagination cursor for next page (if hasMore is true) */
  cursor?: string;

  /** Whether more results are available */
  hasMore: boolean;

  /** Query execution statistics */
  stats: QueryStats;
}

/**
 * Statistics about query execution.
 */
export interface QueryStats {
  /** Number of shard queries executed */
  shardQueries: number;

  /** Number of entities scanned */
  entitiesScanned: number;

  /** Total duration in milliseconds */
  durationMs: number;
}

/**
 * Result of a batch operation.
 */
export interface BatchResult<T> {
  /** Results array (same order as input) */
  results: T[];

  /** Errors for failed operations */
  errors: Array<{ index: number; error: string }>;

  /** Count of successful operations */
  successCount: number;

  /** Count of failed operations */
  errorCount: number;
}

// ============================================================================
// Insert/Update Types
// ============================================================================

/**
 * Input for creating a new entity.
 * Simplified version that doesn't require internal fields.
 */
export interface EntityInput {
  /** Entity ID (URL-based) */
  $id: string;

  /** Entity type(s) */
  $type: string | string[];

  /** Optional context URL (derived from $id if not provided) */
  $context?: string;

  /** Additional properties */
  [key: string]: unknown;
}

/**
 * Batch operation for mixed operations.
 */
export type BatchOperation =
  | { type: 'get'; id: string }
  | { type: 'create'; entity: EntityInput }
  | { type: 'update'; id: string; props: Record<string, unknown> }
  | { type: 'delete'; id: string };

// ============================================================================
// GraphClient Interface
// ============================================================================

/**
 * GraphClient - The main client interface for GraphDB operations.
 *
 * Provides a simple, user-friendly API for:
 * - Entity CRUD operations (insert, query, update, delete)
 * - Graph traversals
 * - Batch operations
 * - Connection management
 *
 * @example
 * ```typescript
 * const client = createGraphClient('wss://graph.example.com');
 *
 * // Insert an entity
 * await client.insert({
 *   $id: 'https://example.com/user/1',
 *   $type: 'User',
 *   name: 'Alice',
 * });
 *
 * // Query by ID
 * const user = await client.query('https://example.com/user/1');
 *
 * // Traverse relationships
 * const friends = await client.traverse('https://example.com/user/1', 'friends');
 *
 * // Close when done
 * client.close();
 * ```
 */
export interface GraphClient {
  // --------------------------------------------------------------------------
  // CRUD Operations
  // --------------------------------------------------------------------------

  /**
   * Insert a new entity into the graph.
   *
   * @param entity - Entity to insert
   * @throws Error if entity already exists or validation fails
   *
   * @example
   * ```typescript
   * await client.insert({
   *   $id: 'https://example.com/user/1',
   *   $type: 'User',
   *   name: 'Alice',
   *   email: 'alice@example.com',
   * });
   * ```
   */
  insert(entity: EntityInput): Promise<void>;

  /**
   * Query an entity by ID or execute a query string.
   *
   * @param idOrQuery - Entity ID (URL) or query string
   * @returns Entity if found, null if not found, or QueryResult for complex queries
   *
   * @example
   * ```typescript
   * // Simple lookup
   * const user = await client.query('https://example.com/user/1');
   *
   * // Path query
   * const result = await client.query('user:1.friends.posts');
   * ```
   */
  query(idOrQuery: string): Promise<Entity | null | QueryResult>;

  /**
   * Update an existing entity's properties.
   *
   * @param id - Entity ID to update
   * @param props - Properties to merge with existing entity
   * @throws Error if entity does not exist
   *
   * @example
   * ```typescript
   * await client.update('https://example.com/user/1', {
   *   name: 'Alice Smith',
   *   updatedAt: new Date().toISOString(),
   * });
   * ```
   */
  update(id: string, props: Record<string, unknown>): Promise<void>;

  /**
   * Delete an entity from the graph.
   *
   * @param id - Entity ID to delete
   * @throws Error if entity does not exist
   *
   * @example
   * ```typescript
   * await client.delete('https://example.com/user/1');
   * ```
   */
  delete(id: string): Promise<void>;

  // --------------------------------------------------------------------------
  // Traversal Operations
  // --------------------------------------------------------------------------

  /**
   * Traverse from an entity following a predicate.
   *
   * @param startId - Starting entity ID
   * @param predicate - Relationship predicate to follow
   * @param options - Optional traversal options
   * @returns Array of connected entities
   *
   * @example
   * ```typescript
   * // Get all friends
   * const friends = await client.traverse('https://example.com/user/1', 'friends');
   *
   * // With options
   * const friends = await client.traverse('https://example.com/user/1', 'friends', {
   *   limit: 10,
   *   filter: { status: 'active' },
   * });
   * ```
   */
  traverse(
    startId: string,
    predicate: string,
    options?: TraversalOptions
  ): Promise<Entity[]>;

  /**
   * Reverse traverse: find entities pointing to a target.
   *
   * @param targetId - Target entity ID
   * @param predicate - Relationship predicate to follow in reverse
   * @param options - Optional traversal options
   * @returns Array of entities pointing to target
   *
   * @example
   * ```typescript
   * // Find users who follow this user
   * const followers = await client.reverseTraverse(
   *   'https://example.com/user/1',
   *   'follows'
   * );
   * ```
   */
  reverseTraverse(
    targetId: string,
    predicate: string,
    options?: TraversalOptions
  ): Promise<Entity[]>;

  /**
   * Multi-hop traversal following a path of predicates.
   *
   * @param startId - Starting entity ID
   * @param path - Array of predicates to follow
   * @param options - Optional traversal options
   * @returns Array of entities at the end of the path
   *
   * @example
   * ```typescript
   * // Get friends of friends' posts
   * const posts = await client.pathTraverse(
   *   'https://example.com/user/1',
   *   ['friends', 'friends', 'posts']
   * );
   * ```
   */
  pathTraverse(
    startId: string,
    path: string[],
    options?: TraversalOptions
  ): Promise<Entity[]>;

  // --------------------------------------------------------------------------
  // Batch Operations
  // --------------------------------------------------------------------------

  /**
   * Get multiple entities by ID in a single request.
   *
   * @param ids - Array of entity IDs
   * @returns Batch result with entities (null for not found)
   *
   * @example
   * ```typescript
   * const result = await client.batchGet([
   *   'https://example.com/user/1',
   *   'https://example.com/user/2',
   * ]);
   * console.log(result.results[0]?.name);
   * ```
   */
  batchGet(ids: string[]): Promise<BatchResult<Entity | null>>;

  /**
   * Insert multiple entities in a single request.
   *
   * @param entities - Array of entities to insert
   * @returns Batch result
   *
   * @example
   * ```typescript
   * const result = await client.batchInsert([
   *   { $id: 'https://example.com/user/1', $type: 'User', name: 'Alice' },
   *   { $id: 'https://example.com/user/2', $type: 'User', name: 'Bob' },
   * ]);
   * console.log(`Created ${result.successCount} entities`);
   * ```
   */
  batchInsert(entities: EntityInput[]): Promise<BatchResult<void>>;

  /**
   * Execute multiple mixed operations in a single request.
   *
   * @param operations - Array of batch operations
   * @returns Batch result with results for each operation
   *
   * @example
   * ```typescript
   * const result = await client.batchExecute([
   *   { type: 'get', id: 'https://example.com/user/1' },
   *   { type: 'create', entity: { $id: 'https://example.com/user/2', $type: 'User' } },
   *   { type: 'delete', id: 'https://example.com/user/3' },
   * ]);
   * ```
   */
  batchExecute(operations: BatchOperation[]): Promise<BatchResult<unknown>>;

  // --------------------------------------------------------------------------
  // Connection Management
  // --------------------------------------------------------------------------

  /**
   * Check if the client is currently connected.
   */
  isConnected(): boolean;

  /**
   * Get the current connection state.
   */
  getState(): ConnectionState;

  /**
   * Get connection statistics.
   */
  getStats(): ConnectionStats;

  /**
   * Manually trigger reconnection (if disconnected).
   */
  reconnect(): Promise<void>;

  /**
   * Close the connection and clean up resources.
   */
  close(): void;
}
