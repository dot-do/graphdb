/**
 * Graph API - Types and interfaces for graph operations exposed via capnweb
 *
 * This module defines the core graph API that will be exposed via capnweb RPC.
 * All operations are designed to work with promise pipelining for optimal performance.
 */

import { RpcTarget } from 'capnweb';
import type { Entity } from '../core/entity.js';
import { resolveNamespace } from '../core/entity.js';
import { ObjectType } from '../core/types.js';
import type { QueryResult } from '../broker/orchestrator.js';
import { planQuery, orchestrateQuery } from '../broker/orchestrator.js';
import { validateEntityId } from '../core/validation.js';
import { parseQueryString } from './graph-api-executor.js';

// ============================================================================
// Batch Size Limits (DoS Prevention)
// ============================================================================

/**
 * Maximum number of items allowed in a single batch operation.
 *
 * This limit prevents denial-of-service attacks where an attacker could
 * submit extremely large batches that exhaust server resources (memory,
 * CPU, or subrequest quotas in Cloudflare Workers).
 *
 * The limit of 1000 is chosen to:
 * - Allow efficient bulk operations for legitimate use cases
 * - Stay well within Cloudflare Workers' 1000 subrequest quota per wake
 * - Prevent memory exhaustion from processing huge arrays
 *
 * Clients needing to process more items should split into multiple batches.
 */
export const MAX_BATCH_SIZE = 1000;

/**
 * Error thrown when a batch operation exceeds the maximum allowed size.
 */
export class BatchSizeLimitError extends Error {
  constructor(actualSize: number, maxSize: number = MAX_BATCH_SIZE) {
    super(
      `Batch size ${actualSize} exceeds maximum allowed size of ${maxSize}. ` +
        `Split your request into smaller batches.`
    );
    this.name = 'BatchSizeLimitError';
  }
}

// ============================================================================
// Additional Types for Protocol Layer
// ============================================================================

/**
 * ProtocolTriple represents a subject-predicate-object relationship.
 *
 * This is the protocol-layer representation of a triple,
 * optimized for RPC transport with simpler types than the core Triple.
 *
 * @example
 * ```typescript
 * const triple: ProtocolTriple = {
 *   subject: "https://example.com/users/123",
 *   predicate: "name",
 *   objectType: ObjectType.STRING,
 *   objectValue: "Alice",
 *   timestamp: Date.now(),
 *   txId: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
 * };
 * ```
 */
export interface ProtocolTriple {
  subject: string;
  predicate: string;
  objectType: ObjectType;
  objectValue: unknown;
  timestamp: number;
  txId: string;
}

/**
 * Traversal options for graph traversal operations.
 *
 * Controls the scope and filtering of graph traversals.
 *
 * @example
 * ```typescript
 * const options: TraversalOptions = {
 *   maxDepth: 3,
 *   limit: 100,
 *   filter: { status: "active" }
 * };
 * const friends = await api.traverse(userId, "friends", options);
 * ```
 */
export interface TraversalOptions {
  /** Maximum depth for traversal (default: unlimited for single hop) */
  maxDepth?: number;
  /** Maximum number of results (default: 100) */
  limit?: number;
  /** Cursor for pagination (opaque string from previous response) */
  cursor?: string;
  /** Filter predicate for entities (key-value equality matching) */
  filter?: Record<string, unknown>;
}

/**
 * Query options for pagination and result control.
 *
 * @example
 * ```typescript
 * const result = await api.query("MATCH (n) RETURN n", {
 *   limit: 50,
 *   cursor: previousResult.cursor
 * });
 * ```
 */
export interface QueryOptions {
  /** Maximum number of results to return (default: 100) */
  limit?: number;
  /** Cursor from previous response for pagination (opaque string) */
  cursor?: string;
}

/**
 * Batch operation result.
 *
 * Contains results for all operations in a batch, along with
 * error information for any that failed. Operations are processed
 * in order and results correspond by index.
 *
 * @example
 * ```typescript
 * const result = await api.batchGet(["id1", "id2", "id3"]);
 * if (result.errorCount > 0) {
 *   for (const err of result.errors) {
 *     console.error(`Operation ${err.index} failed: ${err.error}`);
 *   }
 * }
 * ```
 */
export interface BatchResult<T> {
  /** Results array (same length as input, null for failed operations) */
  results: T[];
  /** Array of errors with their original index */
  errors: Array<{ index: number; error: string }>;
  /** Number of successful operations */
  successCount: number;
  /** Number of failed operations */
  errorCount: number;
}

// ============================================================================
// Graph API Interface
// ============================================================================

/**
 * GraphAPI - The main interface exposed via capnweb RPC.
 *
 * All methods return promises to enable promise pipelining.
 * Designed for optimal performance with Durable Object hibernation.
 *
 * This interface defines the complete graph database API for:
 * - Entity CRUD operations
 * - Graph traversals (forward, reverse, multi-hop)
 * - Query execution
 * - Batch operations
 *
 * @example
 * ```typescript
 * // Via capnweb RPC client
 * const api = client.bootstrap<GraphAPI>();
 *
 * // Get an entity
 * const user = await api.getEntity("https://example.com/users/123");
 *
 * // Traverse relationships
 * const friends = await api.traverse(user.$id, "friends");
 *
 * // Execute a query
 * const result = await api.query("user:123.friends.posts");
 * ```
 */
export interface GraphAPI {
  // --------------------------------------------------------------------------
  // Entity Operations
  // --------------------------------------------------------------------------

  /**
   * Get a single entity by ID
   * @param id - URL-based entity identifier
   */
  getEntity(id: string): Promise<Entity | null>;

  /**
   * Create a new entity
   * @param entity - Entity to create (must include $id and $type)
   */
  createEntity(entity: Entity): Promise<void>;

  /**
   * Update entity properties
   * @param id - Entity ID
   * @param props - Properties to update (merged with existing)
   */
  updateEntity(id: string, props: Record<string, unknown>): Promise<void>;

  /**
   * Delete an entity and all its triples
   * @param id - Entity ID to delete
   */
  deleteEntity(id: string): Promise<void>;

  // --------------------------------------------------------------------------
  // Traversal Operations
  // --------------------------------------------------------------------------

  /**
   * Forward traversal: follow an edge from a starting entity
   * @param startId - Starting entity ID
   * @param predicate - Edge predicate to follow
   * @param options - Optional traversal options
   */
  traverse(
    startId: string,
    predicate: string,
    options?: TraversalOptions
  ): Promise<Entity[]>;

  /**
   * Reverse traversal: find entities pointing to a target
   * @param targetId - Target entity ID
   * @param predicate - Edge predicate to follow in reverse
   * @param options - Optional traversal options
   */
  reverseTraverse(
    targetId: string,
    predicate: string,
    options?: TraversalOptions
  ): Promise<Entity[]>;

  /**
   * Multi-hop traversal with path expression
   * Supports promise pipelining for chained traversals
   * @param startId - Starting entity ID
   * @param path - Array of predicates to follow
   * @param options - Optional traversal options
   */
  pathTraverse(
    startId: string,
    path: string[],
    options?: TraversalOptions
  ): Promise<Entity[]>;

  // --------------------------------------------------------------------------
  // Query Operations
  // --------------------------------------------------------------------------

  /**
   * Execute a query string (path expression syntax)
   * @param queryString - Query in path expression syntax
   * @param options - Optional query options including cursor and limit for pagination
   */
  query(queryString: string, options?: QueryOptions): Promise<QueryResult>;

  // --------------------------------------------------------------------------
  // Batch Operations
  // --------------------------------------------------------------------------

  /**
   * Get multiple entities by ID in a single request
   * Optimized for batch retrieval
   * @param ids - Array of entity IDs
   */
  batchGet(ids: string[]): Promise<BatchResult<Entity | null>>;

  /**
   * Create multiple entities in a single request
   * @param entities - Array of entities to create
   */
  batchCreate(entities: Entity[]): Promise<BatchResult<void>>;

  /**
   * Execute multiple operations in a single batch
   * @param operations - Array of operations
   */
  batchExecute(
    operations: Array<{
      type: 'get' | 'create' | 'update' | 'delete';
      id?: string;
      entity?: Entity;
      props?: Record<string, unknown>;
    }>
  ): Promise<BatchResult<unknown>>;
}

// ============================================================================
// capnweb RpcTarget Implementation
// ============================================================================

/**
 * GraphAPITarget - capnweb RpcTarget implementation of GraphAPI.
 *
 * This class extends RpcTarget to expose methods via capnweb RPC.
 * Methods and getters become remotely callable while private fields
 * (prefixed with #) remain private.
 *
 * Designed to run inside a Durable Object with hibernation support.
 * Each wake from hibernation gets a fresh 1000 subrequest quota.
 *
 * @example
 * ```typescript
 * // In a Durable Object
 * export class BrokerDO {
 *   private api: GraphAPITarget;
 *
 *   constructor(ctx: DurableObjectState) {
 *     this.api = new GraphAPITarget((shardId) => {
 *       return ctx.env.SHARD.get(ctx.env.SHARD.idFromName(shardId));
 *     });
 *   }
 * }
 * ```
 */
export class GraphAPITarget extends RpcTarget implements GraphAPI {
  /** In-memory entity store (for spike validation) */
  #entities: Map<string, Entity> = new Map();

  /** In-memory triple store */
  #triples: ProtocolTriple[] = [];

  /** Transaction counter */
  #txCounter = 0;

  /** Optional callback to get shard stub for orchestrator integration */
  #getShardStub?: (shardId: string) => DurableObjectStub;

  constructor(getShardStub?: (shardId: string) => DurableObjectStub) {
    super();
    this.#getShardStub = getShardStub!;
  }

  // --------------------------------------------------------------------------
  // Entity Operations
  // --------------------------------------------------------------------------

  async getEntity(id: string): Promise<Entity | null> {
    // Validate entity ID before processing
    validateEntityId(id);
    return this.#entities.get(id) ?? null;
  }

  async createEntity(entity: Entity): Promise<void> {
    if (!entity.$id || !entity.$type) {
      const missing = [];
      if (!entity.$id) missing.push('$id');
      if (!entity.$type) missing.push('$type');
      throw new Error(
        `Entity creation failed: missing required field(s): ${missing.join(', ')}. ` +
        `Received entity: ${JSON.stringify(entity).slice(0, 200)}${JSON.stringify(entity).length > 200 ? '...' : ''}`
      );
    }

    const idStr = String(entity.$id);

    // Validate entity ID before processing
    validateEntityId(idStr);

    if (this.#entities.has(idStr)) {
      throw new Error(
        `Entity creation failed: entity with ID "${idStr}" already exists. ` +
        `Use updateEntity() to modify existing entities, or deleteEntity() first to replace.`
      );
    }

    // Resolve namespace info if not already present
    let storedEntity = entity;
    if (!entity._namespace || !entity.$context) {
      try {
        const { namespace, context, localId } = resolveNamespace(idStr);
        storedEntity = {
          ...entity,
          $context: entity.$context || context,
          _namespace: entity._namespace || namespace,
          _localId: entity._localId || localId,
        } as Entity;
      } catch {
        // If URL resolution fails, store entity as-is
        storedEntity = entity;
      }
    }

    this.#entities.set(idStr, { ...storedEntity });

    // Create triples for all properties
    const txId = `tx-${++this.#txCounter}`;
    const timestamp = Date.now();

    for (const [key, value] of Object.entries(entity)) {
      if (key.startsWith('$') || key.startsWith('_')) continue;

      const objectType = this.#inferObjectType(value);
      this.#triples.push({
        subject: idStr,
        predicate: key,
        objectType,
        objectValue: value,
        timestamp,
        txId,
      });
    }
  }

  async updateEntity(id: string, props: Record<string, unknown>): Promise<void> {
    // Validate entity ID before processing
    validateEntityId(id);

    const entity = this.#entities.get(id);
    if (!entity) {
      throw new Error(
        `Entity update failed: entity with ID "${id}" not found. ` +
        `Verify the entity exists before updating, or use createEntity() for new entities.`
      );
    }

    // Merge properties
    Object.assign(entity, props);

    // Update triples
    const txId = `tx-${++this.#txCounter}`;
    const timestamp = Date.now();

    for (const [key, value] of Object.entries(props)) {
      if (key.startsWith('$') || key.startsWith('_')) continue;

      const objectType = this.#inferObjectType(value);
      this.#triples.push({
        subject: id,
        predicate: key,
        objectType,
        objectValue: value,
        timestamp,
        txId,
      });
    }
  }

  async deleteEntity(id: string): Promise<void> {
    // Validate entity ID before processing
    validateEntityId(id);

    if (!this.#entities.has(id)) {
      throw new Error(
        `Entity deletion failed: entity with ID "${id}" not found. ` +
        `The entity may have already been deleted or never existed.`
      );
    }

    this.#entities.delete(id);

    // Remove all triples for this entity
    this.#triples = this.#triples.filter(
      (t) => t.subject !== id && t.objectValue !== id
    );
  }

  // --------------------------------------------------------------------------
  // Traversal Operations
  // --------------------------------------------------------------------------

  async traverse(
    startId: string,
    predicate: string,
    options?: TraversalOptions
  ): Promise<Entity[]> {
    // Validate entity ID before processing
    validateEntityId(startId);

    const limit = options?.limit ?? 100;
    const results: Entity[] = [];

    // Find all triples matching subject and predicate
    for (const triple of this.#triples) {
      if (
        triple.subject === startId &&
        triple.predicate === predicate &&
        triple.objectType === ObjectType.REF
      ) {
        const entity = this.#entities.get(triple.objectValue as string);
        if (entity) {
          // Apply filter if provided
          if (options?.filter && !this.#matchesFilter(entity, options.filter)) {
            continue;
          }
          results.push(entity);
          if (results.length >= limit) break;
        }
      }
    }

    return results;
  }

  async reverseTraverse(
    targetId: string,
    predicate: string,
    options?: TraversalOptions
  ): Promise<Entity[]> {
    // Validate entity ID before processing
    validateEntityId(targetId);

    const limit = options?.limit ?? 100;
    const results: Entity[] = [];

    // Find all triples pointing to target
    for (const triple of this.#triples) {
      if (
        triple.objectValue === targetId &&
        triple.predicate === predicate &&
        triple.objectType === ObjectType.REF
      ) {
        const entity = this.#entities.get(triple.subject);
        if (entity) {
          if (options?.filter && !this.#matchesFilter(entity, options.filter)) {
            continue;
          }
          results.push(entity);
          if (results.length >= limit) break;
        }
      }
    }

    return results;
  }

  async pathTraverse(
    startId: string,
    path: string[],
    options?: TraversalOptions
  ): Promise<Entity[]> {
    // Validate entity ID before processing
    validateEntityId(startId);

    if (path.length === 0) {
      const entity = this.#entities.get(startId);
      return entity ? [entity] : [];
    }

    let currentIds = [startId];
    const maxDepth = options?.maxDepth ?? path.length;

    for (let i = 0; i < Math.min(path.length, maxDepth); i++) {
      const predicate = path[i];
      const nextIds: string[] = [];

      for (const id of currentIds) {
        for (const triple of this.#triples) {
          if (
            triple.subject === id &&
            triple.predicate === predicate &&
            triple.objectType === ObjectType.REF
          ) {
            nextIds.push(triple.objectValue as string);
          }
        }
      }

      currentIds = [...new Set(nextIds)]; // Deduplicate
    }

    // Get entities for final IDs
    const results: Entity[] = [];
    const limit = options?.limit ?? 100;

    for (const id of currentIds) {
      const entity = this.#entities.get(id);
      if (entity) {
        if (options?.filter && !this.#matchesFilter(entity, options.filter)) {
          continue;
        }
        results.push(entity);
        if (results.length >= limit) break;
      }
    }

    return results;
  }

  // --------------------------------------------------------------------------
  // Query Operations
  // --------------------------------------------------------------------------

  async query(queryString: string, options?: QueryOptions): Promise<QueryResult> {
    const startTime = performance.now();
    const limit = options?.limit ?? 100;
    const cursor = options?.cursor;

    // If we have a shard stub getter, use the orchestrator for Cypher-like queries
    if (this.#getShardStub) {
      const plan = planQuery(queryString);
      const paginationOptions = cursor !== undefined ? { cursor, limit } : { limit };
      return orchestrateQuery(plan, this.#getShardStub, paginationOptions);
    }

    // Use the graph-api-executor for local query execution
    // This handles both full URL and short-form entity lookups
    const { entityId, path } = parseQueryString(queryString);

    // Handle empty entity ID (invalid query)
    if (!entityId) {
      return {
        entities: [],
        hasMore: false,
        stats: {
          shardQueries: 0,
          entitiesScanned: 0,
          durationMs: performance.now() - startTime,
        },
      };
    }

    // Simple entity lookup (no traversal)
    if (path.length === 0) {
      const entity = this.#entities.get(entityId);
      return {
        entities: entity ? [entity] : [],
        hasMore: false,
        stats: {
          shardQueries: 1,
          entitiesScanned: entity ? 1 : 0,
          durationMs: performance.now() - startTime,
        },
      };
    }

    // Path traversal with pagination support
    const allEntities = await this.pathTraverse(entityId, path);

    // Parse cursor for offset
    let offset = 0;
    if (cursor) {
      try {
        const decoded = JSON.parse(atob(cursor));
        offset = decoded.offset ?? 0;
      } catch {
        // Invalid cursor, start from beginning
        offset = 0;
      }
    }

    // Apply pagination
    const paginatedEntities = allEntities.slice(offset, offset + limit);
    const hasMore = offset + limit < allEntities.length;
    const nextCursor = hasMore
      ? btoa(JSON.stringify({ offset: offset + limit }))
      : undefined;

    const result: QueryResult = {
      entities: paginatedEntities,
      hasMore,
      stats: {
        shardQueries: path.length,
        entitiesScanned: allEntities.length,
        durationMs: performance.now() - startTime,
      },
    };

    if (nextCursor !== undefined) {
      result.cursor = nextCursor;
    }

    return result;
  }

  // --------------------------------------------------------------------------
  // Batch Operations
  // --------------------------------------------------------------------------

  async batchGet(ids: string[]): Promise<BatchResult<Entity | null>> {
    // Validate batch size to prevent DoS attacks
    if (ids.length > MAX_BATCH_SIZE) {
      throw new BatchSizeLimitError(ids.length);
    }

    const results: (Entity | null)[] = [];
    const errors: Array<{ index: number; error: string }> = [];

    for (let i = 0; i < ids.length; i++) {
      try {
        results.push(await this.getEntity(ids[i]!));
      } catch (e) {
        results.push(null);
        errors.push({ index: i, error: String(e) });
      }
    }

    return {
      results,
      errors,
      successCount: ids.length - errors.length,
      errorCount: errors.length,
    };
  }

  async batchCreate(entities: Entity[]): Promise<BatchResult<void>> {
    // Validate batch size to prevent DoS attacks
    if (entities.length > MAX_BATCH_SIZE) {
      throw new BatchSizeLimitError(entities.length);
    }

    const results: void[] = [];
    const errors: Array<{ index: number; error: string }> = [];

    for (let i = 0; i < entities.length; i++) {
      try {
        await this.createEntity(entities[i]!);
        results.push(undefined);
      } catch (e) {
        errors.push({ index: i, error: String(e) });
      }
    }

    return {
      results,
      errors,
      successCount: entities.length - errors.length,
      errorCount: errors.length,
    };
  }

  async batchExecute(
    operations: Array<{
      type: 'get' | 'create' | 'update' | 'delete';
      id?: string;
      entity?: Entity;
      props?: Record<string, unknown>;
    }>
  ): Promise<BatchResult<unknown>> {
    // Validate batch size to prevent DoS attacks
    if (operations.length > MAX_BATCH_SIZE) {
      throw new BatchSizeLimitError(operations.length);
    }

    const results: unknown[] = [];
    const errors: Array<{ index: number; error: string }> = [];

    for (let i = 0; i < operations.length; i++) {
      const op = operations[i]!;
      try {
        switch (op.type) {
          case 'get':
            results.push(await this.getEntity(op.id!));
            break;
          case 'create':
            await this.createEntity(op.entity!);
            results.push(null);
            break;
          case 'update':
            await this.updateEntity(op.id!, op.props!);
            results.push(null);
            break;
          case 'delete':
            await this.deleteEntity(op.id!);
            results.push(null);
            break;
        }
      } catch (e) {
        errors.push({ index: i, error: String(e) });
      }
    }

    return {
      results,
      errors,
      successCount: operations.length - errors.length,
      errorCount: errors.length,
    };
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  #inferObjectType(value: unknown): ObjectType {
    if (value === null || value === undefined) {
      return ObjectType.NULL;
    }
    if (typeof value === 'string') {
      // Check if it looks like a reference (URL-based ID)
      if (
        value.startsWith('http://') ||
        value.startsWith('https://') ||
        (value.includes(':') && !value.startsWith('http'))
      ) {
        return ObjectType.REF;
      }
      return ObjectType.STRING;
    }
    if (typeof value === 'number') {
      return Number.isInteger(value) ? ObjectType.INT64 : ObjectType.FLOAT64;
    }
    if (typeof value === 'boolean') {
      return ObjectType.BOOL;
    }
    if (typeof value === 'bigint') {
      return ObjectType.INT64;
    }
    if (value instanceof Date) {
      return ObjectType.TIMESTAMP;
    }
    if (
      typeof value === 'object' &&
      value !== null &&
      'lat' in value &&
      'lng' in value
    ) {
      return ObjectType.GEO_POINT;
    }
    return ObjectType.JSON;
  }

  #matchesFilter(
    entity: Entity,
    filter: Record<string, unknown>
  ): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (entity[key] !== value) {
        return false;
      }
    }
    return true;
  }
}
