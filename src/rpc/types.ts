/**
 * RPC Types for GraphDB capnweb integration
 *
 * Defines the TraversalApi interface and related types for
 * cross-colo RPC communication using capnweb.
 *
 * This module provides:
 * - Type-safe RPC method parameter definitions
 * - Discriminated unions for all GraphAPI methods
 * - Validation functions for incoming RPC parameters
 */

import type { EntityId, Namespace } from '../core/types.js';
import type { Entity } from '../core/entity.js';
import type { TraversalOptions, QueryOptions } from '../protocol/graph-api.js';

// ============================================================================
// Core RPC Types
// ============================================================================

/**
 * RpcEntity represents a graph node with properties.
 *
 * This is the RPC-layer entity representation optimized for
 * transport over capnweb. Uses JSON-LD style identifiers.
 *
 * @example
 * ```typescript
 * const entity: RpcEntity = {
 *   $id: "https://example.com/users/123",
 *   $type: "User",
 *   $context: "https://example.com/users",
 *   name: "Alice",
 *   email: "alice@example.com"
 * };
 * ```
 */
export interface RpcEntity {
  /** URL-based entity identifier */
  $id: EntityId;
  /** Entity type(s) - string or array of strings */
  $type: string | string[];
  /** Context URL (usually parent path of $id) */
  $context: string;
  /** Namespace extracted from $id hostname */
  _namespace?: Namespace;
  /** Local ID extracted from $id path */
  _localId?: string;
  /** Properties (JS/TS native field names - NO colons) */
  [predicate: string]: unknown;
}

/**
 * Edge represents a directed relationship between two entities.
 *
 * Edges are stored as triples (subject-predicate-object) but
 * this interface provides a more intuitive representation
 * for traversal operations.
 *
 * @example
 * ```typescript
 * const edge: Edge = {
 *   source: "https://example.com/users/123",
 *   predicate: "follows",
 *   target: "https://example.com/users/456",
 *   weight: 1.0,
 *   metadata: { since: "2024-01-01" }
 * };
 * ```
 */
export interface Edge {
  /** Source entity ID (subject of the triple) */
  source: string;
  /** Relationship predicate (edge label) */
  predicate: string;
  /** Target entity ID (object of the triple) */
  target: string;
  /** Optional edge weight for weighted graph algorithms */
  weight?: number;
  /** Optional metadata attached to the edge */
  metadata?: Record<string, unknown>;
  /** Timestamp when the edge was created/updated */
  timestamp?: number;
  /** Transaction ID that created this edge */
  txId?: string;
}

/**
 * TraversalResult contains the results of a graph traversal operation.
 *
 * Includes both the traversed entity IDs and performance statistics
 * for monitoring and optimization.
 *
 * @example
 * ```typescript
 * const result: TraversalResult = {
 *   entities: ["https://example.com/users/456", "https://example.com/users/789"],
 *   edges: [...],
 *   stats: {
 *     nodesVisited: 100,
 *     edgesTraversed: 250,
 *     durationMs: 15.4,
 *     shardQueries: 3,
 *     cacheHits: 2,
 *     cacheMisses: 1
 *   }
 * };
 * ```
 */
export interface TraversalResult {
  /** Array of entity IDs reached by the traversal */
  entities: string[];
  /** Optional array of edges traversed (for path queries) */
  edges?: Edge[];
  /** Traversal performance statistics */
  stats: TraversalStats;
  /** Pagination cursor for continuing the traversal */
  cursor?: string;
  /** Whether more results are available beyond the limit */
  hasMore?: boolean;
}

/**
 * TraversalStats provides performance metrics for traversal operations.
 */
export interface TraversalStats {
  /** Number of nodes visited during traversal */
  nodesVisited: number;
  /** Number of edges traversed */
  edgesTraversed: number;
  /** Total traversal duration in milliseconds */
  durationMs: number;
  /** Number of shard queries executed */
  shardQueries: number;
  /** Number of cache hits */
  cacheHits?: number;
  /** Number of cache misses */
  cacheMisses?: number;
  /** R2 latency for cold storage access */
  r2LatencyMs?: number;
}

// ============================================================================
// TraversalApi Interface
// ============================================================================

/**
 * TraversalApi defines the capnweb RPC interface for graph traversal operations.
 *
 * This interface is designed for cross-colo communication with:
 * - Promise pipelining support for efficient batching
 * - Hibernation-compatible methods (no long-running state)
 * - Optimized for Cloudflare Workers global network
 *
 * @example
 * ```typescript
 * // Create a capnweb client
 * import { RpcClient } from 'capnweb';
 *
 * const client = new RpcClient(websocket);
 * const api = client.bootstrap<TraversalApi>();
 *
 * // Get colo information
 * const colo = api.getColo();
 *
 * // Lookup entities
 * const entity = await api.lookup("https://example.com/users/123");
 *
 * // Batch lookup
 * const entities = await api.batchLookup([
 *   "https://example.com/users/123",
 *   "https://example.com/users/456"
 * ]);
 *
 * // Traverse relationships
 * const friendIds = await api.traverse("https://example.com/users/123", 2);
 *
 * // Traverse with stats
 * const result = await api.traverseWithStats("https://example.com/users/123", 3);
 * console.log(`Visited ${result.stats.nodesVisited} nodes in ${result.stats.durationMs}ms`);
 * ```
 */
export interface TraversalApi {
  /**
   * Get the current colo (data center) identifier.
   *
   * Returns the Cloudflare colo code (e.g., "SJC", "IAD", "AMS")
   * where this RPC target is running. Useful for debugging
   * and latency-aware routing.
   *
   * @returns The colo identifier string
   */
  getColo(): string;

  /**
   * Get the current R2 latency for cold storage access.
   *
   * Measures the round-trip time to R2 storage from this colo.
   * Useful for determining if data should be cached locally
   * or fetched from a nearby colo with cached data.
   *
   * @returns Promise resolving to latency in milliseconds
   */
  getR2Latency(): Promise<number>;

  /**
   * Lookup a single entity by ID.
   *
   * Performs a point lookup for an entity, checking local cache
   * first, then SQLite storage, then R2 cold storage.
   *
   * @param entityId - The URL-based entity identifier
   * @returns Promise resolving to the entity or null if not found
   */
  lookup(entityId: string): Promise<RpcEntity | null>;

  /**
   * Batch lookup multiple entities by ID.
   *
   * Optimized for bulk retrieval - batches requests to the same
   * shard and parallelizes across shards. Results maintain the
   * same order as the input IDs array.
   *
   * @param entityIds - Array of entity IDs to lookup
   * @returns Promise resolving to array of entities (null for not found)
   */
  batchLookup(entityIds: string[]): Promise<(RpcEntity | null)[]>;

  /**
   * Traverse the graph from a starting entity to a given depth.
   *
   * Performs breadth-first traversal following all outgoing edges.
   * Returns only the entity IDs reached, not full entities.
   *
   * @param startId - The starting entity ID
   * @param depth - Maximum traversal depth (1 = direct neighbors)
   * @returns Promise resolving to array of reached entity IDs
   */
  traverse(startId: string, depth: number): Promise<string[]>;

  /**
   * Traverse the graph with detailed statistics.
   *
   * Like traverse(), but returns a TraversalResult with full
   * statistics about the traversal operation including timing,
   * cache hits, and shard queries.
   *
   * @param startId - The starting entity ID
   * @param depth - Maximum traversal depth
   * @returns Promise resolving to TraversalResult with stats
   */
  traverseWithStats(startId: string, depth: number): Promise<TraversalResult>;
}

// ============================================================================
// RPC Target Implementation Helpers
// ============================================================================

/**
 * Options for creating a TraversalApi RPC target.
 */
export interface TraversalApiOptions {
  /** Cloudflare colo code for this instance */
  colo: string;
  /** Function to measure R2 latency */
  measureR2Latency?: () => Promise<number>;
  /** Function to get a shard stub by ID */
  getShardStub?: (shardId: string) => DurableObjectStub;
  /** Optional R2 bucket for cold storage */
  r2Bucket?: R2Bucket;
}

/**
 * Context passed to TraversalApi methods for request handling.
 */
export interface TraversalContext {
  /** Request timestamp */
  timestamp: number;
  /** Request ID for tracing */
  requestId: string;
  /** Originating colo (if cross-colo request) */
  originColo?: string;
}

// ============================================================================
// Type-Safe RPC Method Parameters (Discriminated Unions)
// ============================================================================

/**
 * Parameter types for each GraphAPI method.
 *
 * These types provide compile-time safety for RPC calls and enable
 * proper validation at runtime.
 */

/** Parameters for getEntity RPC call */
export interface GetEntityParams {
  method: 'getEntity';
  args: [id: string];
}

/** Parameters for createEntity RPC call */
export interface CreateEntityParams {
  method: 'createEntity';
  args: [entity: Entity];
}

/** Parameters for updateEntity RPC call */
export interface UpdateEntityParams {
  method: 'updateEntity';
  args: [id: string, props: Record<string, unknown>];
}

/** Parameters for deleteEntity RPC call */
export interface DeleteEntityParams {
  method: 'deleteEntity';
  args: [id: string];
}

/** Parameters for traverse RPC call */
export interface TraverseParams {
  method: 'traverse';
  args: [startId: string, predicate: string, options?: TraversalOptions];
}

/** Parameters for reverseTraverse RPC call */
export interface ReverseTraverseParams {
  method: 'reverseTraverse';
  args: [targetId: string, predicate: string, options?: TraversalOptions];
}

/** Parameters for pathTraverse RPC call */
export interface PathTraverseParams {
  method: 'pathTraverse';
  args: [startId: string, path: string[], options?: TraversalOptions];
}

/** Parameters for query RPC call */
export interface QueryParams {
  method: 'query';
  args: [queryString: string, options?: QueryOptions];
}

/** Parameters for batchGet RPC call */
export interface BatchGetParams {
  method: 'batchGet';
  args: [ids: string[]];
}

/** Parameters for batchCreate RPC call */
export interface BatchCreateParams {
  method: 'batchCreate';
  args: [entities: Entity[]];
}

/** Batch operation type */
export interface BatchOperation {
  type: 'get' | 'create' | 'update' | 'delete';
  id?: string;
  entity?: Entity;
  props?: Record<string, unknown>;
}

/** Parameters for batchExecute RPC call */
export interface BatchExecuteParams {
  method: 'batchExecute';
  args: [operations: BatchOperation[]];
}

/**
 * Discriminated union of all valid RPC call parameters.
 *
 * Use this type to ensure type-safe RPC handling:
 *
 * @example
 * ```typescript
 * function handleRpcCall(call: RpcCallParams): Promise<unknown> {
 *   switch (call.method) {
 *     case 'getEntity':
 *       // TypeScript knows args is [string]
 *       return api.getEntity(call.args[0]);
 *     case 'createEntity':
 *       // TypeScript knows args is [Entity]
 *       return api.createEntity(call.args[0]);
 *     // ... etc
 *   }
 * }
 * ```
 */
export type RpcCallParams =
  | GetEntityParams
  | CreateEntityParams
  | UpdateEntityParams
  | DeleteEntityParams
  | TraverseParams
  | ReverseTraverseParams
  | PathTraverseParams
  | QueryParams
  | BatchGetParams
  | BatchCreateParams
  | BatchExecuteParams;

/**
 * Valid RPC method names (derived from RpcCallParams).
 */
export type RpcMethodName = RpcCallParams['method'];

/**
 * Incoming RPC call message structure.
 */
export interface RpcCallMessage {
  /** Request ID for correlation */
  id?: string;
  /** RPC method name */
  method: string;
  /** Method arguments */
  args?: unknown[];
}

/**
 * Incoming batched RPC call message structure.
 */
export interface RpcBatchMessage {
  /** Batch ID for correlation */
  id?: string;
  /** Array of RPC calls */
  calls: RpcCallMessage[];
}

// ============================================================================
// RPC Parameter Validation
// ============================================================================

/**
 * Validation result for RPC parameters.
 */
export interface RpcValidationResult {
  /** Whether validation passed */
  valid: boolean;
  /** Error message if validation failed */
  error?: string;
  /** The validated and typed call params (only if valid) */
  params?: RpcCallParams;
}

/**
 * Set of valid RPC method names for quick lookup.
 */
const VALID_RPC_METHODS = new Set<RpcMethodName>([
  'getEntity',
  'createEntity',
  'updateEntity',
  'deleteEntity',
  'traverse',
  'reverseTraverse',
  'pathTraverse',
  'query',
  'batchGet',
  'batchCreate',
  'batchExecute',
]);

/**
 * Check if a method name is a valid RPC method.
 */
export function isValidRpcMethod(method: string): method is RpcMethodName {
  return VALID_RPC_METHODS.has(method as RpcMethodName);
}

/**
 * Validate that a value is a non-empty string.
 */
function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Validate that a value is an object (not null, not array).
 */
function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Validate that a value is an array of strings.
 */
function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

/**
 * Validate Entity structure.
 */
function isValidEntity(value: unknown): value is Entity {
  if (!isObject(value)) return false;
  if (!isNonEmptyString(value['$id'])) return false;
  if (value['$type'] === undefined) return false;
  return true;
}

/**
 * Validate Entity array.
 */
function isEntityArray(value: unknown): value is Entity[] {
  return Array.isArray(value) && value.every(isValidEntity);
}

/**
 * Validate BatchOperation.
 */
function isValidBatchOperation(value: unknown): value is BatchOperation {
  if (!isObject(value)) return false;
  const type = value['type'];
  if (!['get', 'create', 'update', 'delete'].includes(type as string)) return false;

  switch (type) {
    case 'get':
    case 'delete':
      return isNonEmptyString(value['id']);
    case 'create':
      return isValidEntity(value['entity']);
    case 'update':
      return isNonEmptyString(value['id']) && isObject(value['props']);
    default:
      return false;
  }
}

/**
 * Validate BatchOperation array.
 */
function isBatchOperationArray(value: unknown): value is BatchOperation[] {
  return Array.isArray(value) && value.every(isValidBatchOperation);
}

/**
 * Validate TraversalOptions.
 */
function isValidTraversalOptions(value: unknown): value is TraversalOptions | undefined {
  if (value === undefined) return true;
  if (!isObject(value)) return false;

  // All fields are optional, just check types if present
  if (value['maxDepth'] !== undefined && typeof value['maxDepth'] !== 'number') return false;
  if (value['limit'] !== undefined && typeof value['limit'] !== 'number') return false;
  if (value['cursor'] !== undefined && typeof value['cursor'] !== 'string') return false;
  if (value['filter'] !== undefined && !isObject(value['filter'])) return false;

  return true;
}

/**
 * Validate QueryOptions.
 */
function isValidQueryOptions(value: unknown): value is QueryOptions | undefined {
  if (value === undefined) return true;
  if (!isObject(value)) return false;

  if (value['limit'] !== undefined && typeof value['limit'] !== 'number') return false;
  if (value['cursor'] !== undefined && typeof value['cursor'] !== 'string') return false;

  return true;
}

/**
 * Validate incoming RPC call parameters.
 *
 * This function validates that an incoming RPC message has valid parameters
 * for the specified method. It provides type narrowing so the result can be
 * used in a type-safe manner.
 *
 * @param call - The incoming RPC call message
 * @returns Validation result with typed params if valid
 *
 * @example
 * ```typescript
 * const result = validateRpcCall({ method: 'getEntity', args: ['https://example.com/1'] });
 * if (result.valid) {
 *   // result.params is typed as RpcCallParams
 *   const entityId = result.params.args[0]; // TypeScript knows this is string
 * }
 * ```
 */
export function validateRpcCall(call: RpcCallMessage): RpcValidationResult {
  const { method, args = [] } = call;

  // Validate method name
  if (!isNonEmptyString(method)) {
    return { valid: false, error: 'Method name must be a non-empty string' };
  }

  if (!isValidRpcMethod(method)) {
    return { valid: false, error: `Unknown RPC method: ${method}` };
  }

  // Validate args is an array
  if (!Array.isArray(args)) {
    return { valid: false, error: 'Args must be an array' };
  }

  // Method-specific validation
  switch (method) {
    case 'getEntity': {
      if (args.length < 1 || !isNonEmptyString(args[0])) {
        return { valid: false, error: 'getEntity requires a non-empty string id' };
      }
      return {
        valid: true,
        params: { method: 'getEntity', args: [args[0]] },
      };
    }

    case 'createEntity': {
      if (args.length < 1 || !isValidEntity(args[0])) {
        return { valid: false, error: 'createEntity requires a valid entity with $id and $type' };
      }
      return {
        valid: true,
        params: { method: 'createEntity', args: [args[0] as Entity] },
      };
    }

    case 'updateEntity': {
      if (args.length < 2 || !isNonEmptyString(args[0]) || !isObject(args[1])) {
        return { valid: false, error: 'updateEntity requires (id: string, props: object)' };
      }
      return {
        valid: true,
        params: { method: 'updateEntity', args: [args[0], args[1] as Record<string, unknown>] },
      };
    }

    case 'deleteEntity': {
      if (args.length < 1 || !isNonEmptyString(args[0])) {
        return { valid: false, error: 'deleteEntity requires a non-empty string id' };
      }
      return {
        valid: true,
        params: { method: 'deleteEntity', args: [args[0]] },
      };
    }

    case 'traverse': {
      if (args.length < 2 || !isNonEmptyString(args[0]) || !isNonEmptyString(args[1])) {
        return { valid: false, error: 'traverse requires (startId: string, predicate: string)' };
      }
      if (!isValidTraversalOptions(args[2])) {
        return { valid: false, error: 'traverse options must be a valid TraversalOptions object' };
      }
      const traverseArgs: [string, string, TraversalOptions?] = args[2] !== undefined
        ? [args[0], args[1], args[2] as TraversalOptions]
        : [args[0], args[1]];
      return {
        valid: true,
        params: { method: 'traverse', args: traverseArgs },
      };
    }

    case 'reverseTraverse': {
      if (args.length < 2 || !isNonEmptyString(args[0]) || !isNonEmptyString(args[1])) {
        return { valid: false, error: 'reverseTraverse requires (targetId: string, predicate: string)' };
      }
      if (!isValidTraversalOptions(args[2])) {
        return { valid: false, error: 'reverseTraverse options must be a valid TraversalOptions object' };
      }
      const reverseTraverseArgs: [string, string, TraversalOptions?] = args[2] !== undefined
        ? [args[0], args[1], args[2] as TraversalOptions]
        : [args[0], args[1]];
      return {
        valid: true,
        params: { method: 'reverseTraverse', args: reverseTraverseArgs },
      };
    }

    case 'pathTraverse': {
      if (args.length < 2 || !isNonEmptyString(args[0]) || !isStringArray(args[1])) {
        return { valid: false, error: 'pathTraverse requires (startId: string, path: string[])' };
      }
      if (!isValidTraversalOptions(args[2])) {
        return { valid: false, error: 'pathTraverse options must be a valid TraversalOptions object' };
      }
      const pathTraverseArgs: [string, string[], TraversalOptions?] = args[2] !== undefined
        ? [args[0], args[1], args[2] as TraversalOptions]
        : [args[0], args[1]];
      return {
        valid: true,
        params: { method: 'pathTraverse', args: pathTraverseArgs },
      };
    }

    case 'query': {
      if (args.length < 1 || !isNonEmptyString(args[0])) {
        return { valid: false, error: 'query requires a non-empty query string' };
      }
      if (!isValidQueryOptions(args[1])) {
        return { valid: false, error: 'query options must be a valid QueryOptions object' };
      }
      const queryArgs: [string, QueryOptions?] = args[1] !== undefined
        ? [args[0], args[1] as QueryOptions]
        : [args[0]];
      return {
        valid: true,
        params: { method: 'query', args: queryArgs },
      };
    }

    case 'batchGet': {
      if (args.length < 1 || !isStringArray(args[0])) {
        return { valid: false, error: 'batchGet requires an array of string ids' };
      }
      return {
        valid: true,
        params: { method: 'batchGet', args: [args[0]] },
      };
    }

    case 'batchCreate': {
      if (args.length < 1 || !isEntityArray(args[0])) {
        return { valid: false, error: 'batchCreate requires an array of valid entities' };
      }
      return {
        valid: true,
        params: { method: 'batchCreate', args: [args[0]] },
      };
    }

    case 'batchExecute': {
      if (args.length < 1 || !isBatchOperationArray(args[0])) {
        return { valid: false, error: 'batchExecute requires an array of valid batch operations' };
      }
      return {
        valid: true,
        params: { method: 'batchExecute', args: [args[0]] },
      };
    }

    default: {
      // TypeScript exhaustiveness check - this should never be reached
      const _exhaustive: never = method;
      return { valid: false, error: `Unhandled method: ${_exhaustive}` };
    }
  }
}

/**
 * Type-safe RPC call function signature.
 *
 * This type allows creating type-safe call functions that map method names
 * to their expected argument types.
 */
export type TypedRpcCall = {
  // Entity operations
  (method: 'getEntity', id: string): Promise<Entity | null>;
  (method: 'createEntity', entity: Entity): Promise<void>;
  (method: 'updateEntity', id: string, props: Record<string, unknown>): Promise<void>;
  (method: 'deleteEntity', id: string): Promise<void>;

  // Traversal operations
  (method: 'traverse', startId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>;
  (method: 'reverseTraverse', targetId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>;
  (method: 'pathTraverse', startId: string, path: string[], options?: TraversalOptions): Promise<Entity[]>;

  // Query operations
  (method: 'query', queryString: string, options?: QueryOptions): Promise<import('../broker/orchestrator.js').QueryResult>;

  // Batch operations
  (method: 'batchGet', ids: string[]): Promise<import('../protocol/graph-api.js').BatchResult<Entity | null>>;
  (method: 'batchCreate', entities: Entity[]): Promise<import('../protocol/graph-api.js').BatchResult<void>>;
  (method: 'batchExecute', operations: BatchOperation[]): Promise<import('../protocol/graph-api.js').BatchResult<unknown>>;
};
