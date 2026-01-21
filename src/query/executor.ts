/**
 * GraphDB Traversal Executor (E7.2: RED)
 *
 * Executes query plans by coordinating shard queries and traversals.
 * Integrates with the traversal executor spike for efficient BFS/DFS traversal.
 *
 * Following TDD approach: tests written first, then implementation to GREEN.
 */

import type { QueryPlan, PlanStep, FilterExpr } from './planner';
import type { Triple } from '../core/triple';
import type { Entity } from '../core/entity';
import { fnv1aHash, hashToHex } from '../core/hash';

// ============================================================================
// Constants
// ============================================================================

/**
 * Maximum depth for path traversal operations to prevent infinite recursion.
 * This is the absolute upper limit; callers can specify lower limits.
 * Default value: 100 (configurable via options)
 */
export const MAX_PATH_DEPTH = 100;

/**
 * Default depth for path traversal when not specified.
 */
export const DEFAULT_PATH_DEPTH = 10;

/**
 * Maximum traversal time in milliseconds to prevent long-running queries
 * on high fan-out graphs. Default: 30 seconds.
 */
export const MAX_TRAVERSAL_TIME_MS = 30000;

// ============================================================================
// Types
// ============================================================================

/**
 * Execution context for running query plans
 */
export interface ExecutionContext {
  /** Function to get a Durable Object stub for a shard */
  getShardStub: (shardId: string) => DurableObjectStub;
  /** Maximum results to return */
  maxResults?: number;
  /** Timeout in milliseconds */
  timeout?: number;
}

/**
 * Pagination options for query execution
 */
export interface PaginationOptions {
  /** Cursor from previous query result to continue pagination */
  cursor?: string;
}

/**
 * Internal cursor state - encoded in the opaque cursor string
 */
interface CursorState {
  /** Last seen entity ID for cursor-based pagination */
  lastId: string;
  /** Hash of the query to ensure cursor is used with the same query */
  queryHash: string;
  /** Timestamp when cursor was created (for expiration) */
  ts: number;
  /** Offset for result pagination */
  offset: number;
}

/**
 * Result of executing a query plan
 */
export interface ExecutionResult {
  /** Entities found */
  entities: Entity[];
  /** Triples found */
  triples: Triple[];
  /** Cursor for pagination */
  cursor?: string;
  /** Whether there are more results */
  hasMore: boolean;
  /** Execution statistics */
  stats: ExecutionStats;
}

/**
 * Statistics about execution
 */
export interface ExecutionStats {
  /** Number of shard queries made */
  shardQueries: number;
  /** Number of entities scanned */
  entitiesScanned: number;
  /** Total duration in milliseconds */
  durationMs: number;
}

/**
 * Direction for traversal
 */
export type Direction = 'outgoing' | 'incoming' | 'both';

/**
 * Internal traversal state
 */
interface TraversalState {
  /** Visited entity IDs */
  visited: Set<string>;
  /** Current frontier of entity IDs */
  frontier: string[];
  /** Current depth */
  depth: number;
  /** Collected entities */
  entities: Entity[];
  /** Collected triples */
  triples: Triple[];
  /** Statistics */
  stats: ExecutionStats;
}

// ============================================================================
// Cursor Encoding/Decoding Utilities
// ============================================================================

/** Cursor expiration time (1 hour) */
const CURSOR_EXPIRATION_MS = 60 * 60 * 1000;

/**
 * Serialize a FilterExpr to a deterministic string representation.
 * Ensures consistent output regardless of object property order.
 */
function serializeFilterDeterministic(filter: FilterExpr | undefined): string | undefined {
  if (!filter) return undefined;

  // Build a deterministic representation with sorted keys
  const parts: string[] = [];

  // Always include these fields in a fixed order
  parts.push(`field:${filter.field}`);
  parts.push(`op:${filter.op}`);

  // Serialize value deterministically
  if (filter.value === null) {
    parts.push('value:null');
  } else if (typeof filter.value === 'object') {
    // For object values, sort keys for deterministic order
    const sortedKeys = Object.keys(filter.value as object).sort();
    const sortedObj: Record<string, unknown> = {};
    for (const key of sortedKeys) {
      sortedObj[key] = (filter.value as Record<string, unknown>)[key];
    }
    parts.push(`value:${JSON.stringify(sortedObj)}`);
  } else {
    parts.push(`value:${JSON.stringify(filter.value)}`);
  }

  // Recursively serialize AND condition
  if (filter.and) {
    parts.push(`and:(${serializeFilterDeterministic(filter.and)})`);
  }

  // Recursively serialize OR condition
  if (filter.or) {
    parts.push(`or:(${serializeFilterDeterministic(filter.or)})`);
  }

  return parts.join(';');
}

/**
 * Generate a hash for the query plan to validate cursor reuse.
 * Ensures deterministic output for caching and debugging.
 */
function hashQueryPlan(plan: QueryPlan): string {
  // Create a stable string representation of the plan
  // Sort all array fields and serialize filters deterministically
  const planStr = JSON.stringify(plan.steps.map((s) => ({
    type: s.type,
    shardId: s.shardId,
    entityIds: s.entityIds?.slice().sort(),
    predicate: s.predicate,
    maxDepth: s.maxDepth,
    fields: s.fields?.slice().sort(),
    filter: serializeFilterDeterministic(s.filter),
  })));

  // Use centralized FNV-1a hash function
  return hashToHex(fnv1aHash(planStr));
}

/**
 * Encode cursor state into an opaque base64 string
 */
function encodeCursor(state: CursorState): string {
  const json = JSON.stringify(state);
  // Use btoa for base64 encoding (available in Workers runtime)
  return btoa(json);
}

/**
 * Decode and validate a cursor string
 * @throws Error if cursor is invalid or tampered
 */
function decodeCursor(cursor: string, expectedQueryHash: string): CursorState {
  let cursorState: CursorState;

  try {
    const json = atob(cursor);
    cursorState = JSON.parse(json) as CursorState;
  } catch (e) {
    const preview = cursor.length > 50 ? cursor.slice(0, 50) + '...' : cursor;
    throw new Error(
      `Invalid cursor: failed to decode cursor "${preview}". ` +
      `Expected a valid base64-encoded JSON string. ` +
      `Cause: ${e instanceof Error ? e.message : String(e)}`
    );
  }

  // Validate required fields
  if (
    typeof cursorState.lastId !== 'string' ||
    typeof cursorState.queryHash !== 'string' ||
    typeof cursorState.ts !== 'number' ||
    typeof cursorState.offset !== 'number'
  ) {
    const missing: string[] = [];
    if (typeof cursorState.lastId !== 'string') missing.push('lastId (string)');
    if (typeof cursorState.queryHash !== 'string') missing.push('queryHash (string)');
    if (typeof cursorState.ts !== 'number') missing.push('ts (number)');
    if (typeof cursorState.offset !== 'number') missing.push('offset (number)');
    throw new Error(
      `Invalid cursor: missing or invalid fields. ` +
      `Expected fields: ${missing.join(', ')}. ` +
      `Received: ${JSON.stringify(cursorState)}`
    );
  }

  // Validate offset is non-negative
  if (cursorState.offset < 0) {
    throw new Error(
      `Invalid cursor: offset must be non-negative, got ${cursorState.offset}. ` +
      `This may indicate a corrupted or tampered cursor.`
    );
  }

  // Validate query hash matches
  if (cursorState.queryHash !== expectedQueryHash) {
    throw new Error(
      `Invalid cursor: cursor query mismatch. ` +
      `Cursor was created for query hash "${cursorState.queryHash}" ` +
      `but current query has hash "${expectedQueryHash}". ` +
      `Cursors cannot be reused across different queries.`
    );
  }

  // Check expiration
  const now = Date.now();
  if (now - cursorState.ts > CURSOR_EXPIRATION_MS) {
    const ageMs = now - cursorState.ts;
    const ageMinutes = Math.round(ageMs / 60000);
    const expirationMinutes = CURSOR_EXPIRATION_MS / 60000;
    throw new Error(
      `Invalid cursor: cursor has expired. ` +
      `Cursor age: ${ageMinutes} minutes, max allowed: ${expirationMinutes} minutes. ` +
      `Please re-execute the query to get a fresh cursor.`
    );
  }

  return cursorState;
}

// ============================================================================
// Main Execution Functions
// ============================================================================

/**
 * Execute a query plan
 *
 * Coordinates execution of plan steps across shards, handling
 * lookups, traversals, filters, and aggregation.
 *
 * @param plan - Query plan to execute
 * @param ctx - Execution context
 * @param options - Optional pagination options including cursor
 * @returns Execution result with entities, triples, and stats
 */
export async function executePlan(
  plan: QueryPlan,
  ctx: ExecutionContext,
  options?: PaginationOptions
): Promise<ExecutionResult> {
  const startTime = performance.now();
  const maxResults = ctx.maxResults ?? 100;
  const queryHash = hashQueryPlan(plan);

  // Decode and validate cursor if provided
  let startOffset = 0;
  if (options?.cursor) {
    const cursorState = decodeCursor(options.cursor, queryHash);
    startOffset = cursorState.offset;
  }

  const state: TraversalState = {
    visited: new Set<string>(),
    frontier: [],
    depth: 0,
    entities: [],
    triples: [],
    stats: {
      shardQueries: 0,
      entitiesScanned: 0,
      durationMs: 0,
    },
  };

  // Execute each step in the plan
  // We need to collect more than maxResults to know if there are more
  const collectLimit = startOffset + maxResults + 1;

  for (const step of plan.steps) {
    // Check if we've hit collect limit
    if (state.entities.length >= collectLimit) {
      break;
    }

    // Check timeout
    if (ctx.timeout && performance.now() - startTime > ctx.timeout) {
      break;
    }

    await executeStepInternal(step, state, ctx);
  }

  state.stats.durationMs = performance.now() - startTime;

  // Apply pagination: skip offset, take maxResults
  const paginatedEntities = state.entities.slice(startOffset, startOffset + maxResults);
  const hasMore = state.entities.length > startOffset + maxResults;

  // Generate cursor for next page if there are more results
  let cursor: string | undefined;
  if (hasMore && paginatedEntities.length > 0) {
    const lastEntity = paginatedEntities[paginatedEntities.length - 1];
    const cursorState: CursorState = {
      lastId: lastEntity!.$id,
      queryHash,
      ts: Date.now(),
      offset: startOffset + maxResults,
    };
    cursor = encodeCursor(cursorState);
  }

  const result: ExecutionResult = {
    entities: paginatedEntities,
    triples: state.triples,
    hasMore,
    stats: state.stats,
  };
  if (cursor !== undefined) {
    result.cursor = cursor;
  }
  return result;
}

/**
 * Execute a single plan step
 *
 * @param step - Step to execute
 * @param ctx - Execution context
 * @returns Triples found in this step
 */
export async function executeStep(
  step: PlanStep,
  ctx: ExecutionContext
): Promise<Triple[]> {
  const state: TraversalState = {
    visited: new Set<string>(),
    frontier: [],
    depth: 0,
    entities: [],
    triples: [],
    stats: {
      shardQueries: 0,
      entitiesScanned: 0,
      durationMs: 0,
    },
  };

  await executeStepInternal(step, state, ctx);
  return state.triples;
}

/**
 * Internal step execution
 */
async function executeStepInternal(
  step: PlanStep,
  state: TraversalState,
  ctx: ExecutionContext
): Promise<void> {
  switch (step.type) {
    case 'lookup':
      await executeLookup(step, state, ctx);
      break;
    case 'traverse':
      await executeTraverse(step, state, ctx);
      break;
    case 'reverse':
      await executeReverse(step, state, ctx);
      break;
    case 'filter':
      await executeFilter(step, state);
      break;
    case 'expand':
      await executeExpand(step, state, ctx);
      break;
    case 'recurse':
      await executeRecurse(step, state, ctx);
      break;
  }
}

// ============================================================================
// Step Executors
// ============================================================================

/**
 * Execute lookup step - fetch entities by ID
 */
async function executeLookup(
  step: PlanStep,
  state: TraversalState,
  ctx: ExecutionContext
): Promise<void> {
  if (!step.entityIds || step.entityIds.length === 0) {
    return;
  }

  const stub = ctx.getShardStub(step.shardId);
  state.stats.shardQueries++;

  // Query the shard for entities
  const response = await stub.fetch(
    new Request('http://shard/lookup', {
      method: 'POST',
      body: JSON.stringify({ entityIds: step.entityIds }),
    })
  );

  if (!response.ok) {
    const statusText = response.statusText || 'Unknown error';
    throw new Error(
      `Shard lookup failed for shard "${step.shardId}": HTTP ${response.status} ${statusText}. ` +
      `Requested entity IDs: ${step.entityIds?.slice(0, 5).join(', ')}${(step.entityIds?.length ?? 0) > 5 ? ` (and ${(step.entityIds?.length ?? 0) - 5} more)` : ''}`
    );
  }

  const result = (await response.json()) as {
    entities: Entity[];
    triples: Triple[];
  };

  // Add to state
  for (const entity of result.entities) {
    if (!state.visited.has(entity.$id)) {
      state.visited.add(entity.$id);
      state.entities.push(entity);
      state.frontier.push(entity.$id);
      state.stats.entitiesScanned++;
    }
  }

  state.triples.push(...result.triples);
}

/**
 * Execute traverse step - follow outgoing edges
 */
async function executeTraverse(
  step: PlanStep,
  state: TraversalState,
  ctx: ExecutionContext
): Promise<void> {
  if (state.frontier.length === 0 || !step.predicate) {
    return;
  }

  const stub = ctx.getShardStub(step.shardId);
  state.stats.shardQueries++;

  // Query for outgoing edges with the predicate
  const response = await stub.fetch(
    new Request('http://shard/traverse', {
      method: 'POST',
      body: JSON.stringify({
        entityIds: state.frontier,
        predicate: step.predicate,
        direction: 'outgoing',
      }),
    })
  );

  if (!response.ok) {
    const statusText = response.statusText || 'Unknown error';
    throw new Error(
      `Shard traverse failed for shard "${step.shardId}": HTTP ${response.status} ${statusText}. ` +
      `Traversal predicate: "${step.predicate}", direction: outgoing, ` +
      `from entities: ${state.frontier.slice(0, 3).join(', ')}${state.frontier.length > 3 ? ` (and ${state.frontier.length - 3} more)` : ''}`
    );
  }

  const result = (await response.json()) as {
    entities: Entity[];
    triples: Triple[];
  };

  // Update frontier with new entities
  const newFrontier: string[] = [];

  for (const entity of result.entities) {
    if (!state.visited.has(entity.$id)) {
      state.visited.add(entity.$id);
      state.entities.push(entity);
      newFrontier.push(entity.$id);
      state.stats.entitiesScanned++;
    }
  }

  state.frontier = newFrontier;
  state.triples.push(...result.triples);
  state.depth++;
}

/**
 * Execute reverse step - follow incoming edges
 */
async function executeReverse(
  step: PlanStep,
  state: TraversalState,
  ctx: ExecutionContext
): Promise<void> {
  if (state.frontier.length === 0 || !step.predicate) {
    return;
  }

  const stub = ctx.getShardStub(step.shardId);
  state.stats.shardQueries++;

  // Query for incoming edges with the predicate
  const response = await stub.fetch(
    new Request('http://shard/traverse', {
      method: 'POST',
      body: JSON.stringify({
        entityIds: state.frontier,
        predicate: step.predicate,
        direction: 'incoming',
      }),
    })
  );

  if (!response.ok) {
    const statusText = response.statusText || 'Unknown error';
    throw new Error(
      `Shard reverse traverse failed for shard "${step.shardId}": HTTP ${response.status} ${statusText}. ` +
      `Traversal predicate: "${step.predicate}", direction: incoming, ` +
      `to entities: ${state.frontier.slice(0, 3).join(', ')}${state.frontier.length > 3 ? ` (and ${state.frontier.length - 3} more)` : ''}`
    );
  }

  const result = (await response.json()) as {
    entities: Entity[];
    triples: Triple[];
  };

  // Update frontier with new entities
  const newFrontier: string[] = [];

  for (const entity of result.entities) {
    if (!state.visited.has(entity.$id)) {
      state.visited.add(entity.$id);
      state.entities.push(entity);
      newFrontier.push(entity.$id);
      state.stats.entitiesScanned++;
    }
  }

  state.frontier = newFrontier;
  state.triples.push(...result.triples);
}

/**
 * Execute filter step - apply filter conditions
 */
async function executeFilter(step: PlanStep, state: TraversalState): Promise<void> {
  if (!step.filter) {
    return;
  }

  // Filter entities based on the filter expression
  const filteredEntities = state.entities.filter((entity) =>
    evaluateFilter(entity, step.filter!)
  );

  // Update frontier to only include filtered entities
  const filteredIds = new Set<string>(filteredEntities.map((e) => e.$id));
  state.frontier = state.frontier.filter((id) => filteredIds.has(id));
  state.entities = filteredEntities;
}

/**
 * Execute expand step - fetch additional fields
 */
async function executeExpand(
  step: PlanStep,
  state: TraversalState,
  ctx: ExecutionContext
): Promise<void> {
  if (!step.fields || step.fields.length === 0 || state.frontier.length === 0) {
    return;
  }

  const stub = ctx.getShardStub(step.shardId);
  state.stats.shardQueries++;

  // Request specific fields for entities
  const response = await stub.fetch(
    new Request('http://shard/expand', {
      method: 'POST',
      body: JSON.stringify({
        entityIds: state.frontier,
        fields: step.fields,
      }),
    })
  );

  if (!response.ok) {
    const statusText = response.statusText || 'Unknown error';
    throw new Error(
      `Shard expand failed for shard "${step.shardId}": HTTP ${response.status} ${statusText}. ` +
      `Requested fields: ${step.fields?.join(', ')}, ` +
      `for entities: ${state.frontier.slice(0, 3).join(', ')}${state.frontier.length > 3 ? ` (and ${state.frontier.length - 3} more)` : ''}`
    );
  }

  const result = (await response.json()) as {
    entities: Entity[];
    triples: Triple[];
  };

  // Merge expanded fields into existing entities
  const entityMap = new Map(state.entities.map((e) => [e.$id, e]));
  for (const expanded of result.entities) {
    const existing = entityMap.get(expanded.$id);
    if (existing !== undefined) {
      // Merge fields
      for (const field of step.fields!) {
        if (field in expanded) {
          (existing as Record<string, unknown>)[field] = (
            expanded as Record<string, unknown>
          )[field];
        }
      }
    }
  }

  state.triples.push(...result.triples);
}

/**
 * Execute recurse step - recursive traversal with depth limit
 *
 * Enforces MAX_PATH_DEPTH as an absolute upper bound to prevent
 * infinite recursion or DoS attacks via deeply nested traversals.
 * Also enforces timeout to prevent long execution on high fan-out graphs.
 */
async function executeRecurse(
  step: PlanStep,
  state: TraversalState,
  ctx: ExecutionContext
): Promise<void> {
  // Enforce absolute maximum while respecting user-specified limit
  const requestedDepth = step.maxDepth ?? DEFAULT_PATH_DEPTH;
  const maxDepth = Math.min(requestedDepth, MAX_PATH_DEPTH);
  let currentDepth = 0;

  // Timeout configuration: use ctx.timeout if provided, otherwise default
  const timeoutMs = ctx.timeout ?? MAX_TRAVERSAL_TIME_MS;
  const startTime = Date.now();

  while (currentDepth < maxDepth && state.frontier.length > 0) {
    // Check timeout at the start of each iteration
    if (Date.now() - startTime > timeoutMs) {
      // Return partial results collected so far
      break;
    }

    const stub = ctx.getShardStub(step.shardId);
    state.stats.shardQueries++;

    // Traverse all outgoing edges (no predicate filter for recursion)
    const response = await stub.fetch(
      new Request('http://shard/traverse', {
        method: 'POST',
        body: JSON.stringify({
          entityIds: state.frontier,
          direction: 'outgoing',
        }),
      })
    );

    if (!response.ok) {
      break;
    }

    const result = (await response.json()) as {
      entities: Entity[];
      triples: Triple[];
    };

    // Update frontier with new (unvisited) entities
    const newFrontier: string[] = [];

    for (const entity of result.entities) {
      if (!state.visited.has(entity.$id)) {
        state.visited.add(entity.$id);
        state.entities.push(entity);
        newFrontier.push(entity.$id);
        state.stats.entitiesScanned++;
      }
    }

    state.frontier = newFrontier;
    state.triples.push(...result.triples);
    currentDepth++;
    state.depth = currentDepth;
  }
}

// ============================================================================
// Traversal Functions (High-Level API)
// ============================================================================

/**
 * Traverse from an entity following a predicate
 *
 * Enforces MAX_PATH_DEPTH as an absolute upper bound to prevent
 * infinite recursion or DoS attacks via deeply nested traversals.
 *
 * @param startId - Starting entity ID
 * @param predicate - Predicate to follow
 * @param options - Traversal options (maxDepth capped at MAX_PATH_DEPTH)
 * @param ctx - Execution context
 * @returns Entities reachable via the predicate
 */
export async function traverseFrom(
  startId: string,
  predicate: string,
  options: { maxDepth?: number; maxResults?: number },
  ctx: ExecutionContext
): Promise<Entity[]> {
  // Enforce absolute maximum while respecting user-specified limit
  const requestedDepth = options.maxDepth ?? 1;
  const maxDepth = Math.min(requestedDepth, MAX_PATH_DEPTH);
  const maxResults = options.maxResults ?? 100;

  const state: TraversalState = {
    visited: new Set<string>([startId]),
    frontier: [startId],
    depth: 0,
    entities: [],
    triples: [],
    stats: {
      shardQueries: 0,
      entitiesScanned: 1,
      durationMs: 0,
    },
  };

  const startTime = performance.now();

  // BFS traversal
  while (state.depth < maxDepth && state.frontier.length > 0) {
    // Determine shard for current frontier (simplified: use first entity's shard)
    // Non-null assertion safe: loop condition ensures frontier is non-empty
    const shardId = getShardIdForEntity(state.frontier[0]!);
    const stub = ctx.getShardStub(shardId);
    state.stats.shardQueries++;

    const response = await stub.fetch(
      new Request('http://shard/traverse', {
        method: 'POST',
        body: JSON.stringify({
          entityIds: state.frontier,
          predicate,
          direction: 'outgoing',
        }),
      })
    );

    if (!response.ok) {
      break;
    }

    const result = (await response.json()) as {
      entities: Entity[];
      triples: Triple[];
    };

    const newFrontier: string[] = [];

    for (const entity of result.entities) {
      if (!state.visited.has(entity.$id)) {
        state.visited.add(entity.$id);
        state.entities.push(entity);
        newFrontier.push(entity.$id);
        state.stats.entitiesScanned++;

        if (state.entities.length >= maxResults) {
          break;
        }
      }
    }

    if (state.entities.length >= maxResults) {
      break;
    }

    state.frontier = newFrontier;
    state.depth++;
  }

  state.stats.durationMs = performance.now() - startTime;
  return state.entities.slice(0, maxResults);
}

/**
 * Reverse traverse to find entities pointing to a target
 *
 * @param targetId - Target entity ID
 * @param predicate - Predicate to follow in reverse
 * @param ctx - Execution context
 * @returns Entities that point to the target via the predicate
 */
export async function traverseTo(
  targetId: string,
  predicate: string,
  ctx: ExecutionContext
): Promise<Entity[]> {
  const shardId = getShardIdForEntity(targetId);
  const stub = ctx.getShardStub(shardId);

  const response = await stub.fetch(
    new Request('http://shard/traverse', {
      method: 'POST',
      body: JSON.stringify({
        entityIds: [targetId],
        predicate,
        direction: 'incoming',
      }),
    })
  );

  if (!response.ok) {
    const statusText = response.statusText || 'Unknown error';
    throw new Error(
      `Reverse traversal failed: HTTP ${response.status} ${statusText}. ` +
      `Target entity: "${targetId}", predicate: "${predicate}"`
    );
  }

  const result = (await response.json()) as {
    entities: Entity[];
    triples: Triple[];
  };

  return result.entities;
}

// ============================================================================
// Filter Evaluation
// ============================================================================

/**
 * Evaluate a filter expression against an entity
 */
function evaluateFilter(entity: Entity, filter: FilterExpr): boolean {
  const value = (entity as Record<string, unknown>)[filter.field];
  const result = compareValues(value, filter.op, filter.value);

  // Handle AND condition
  if (result && filter.and) {
    return evaluateFilter(entity, filter.and);
  }

  // Handle OR condition
  if (!result && filter.or) {
    return evaluateFilter(entity, filter.or);
  }

  return result;
}

/**
 * Compare two values with an operator
 */
function compareValues(
  actual: unknown,
  op: '=' | '!=' | '>' | '<' | '>=' | '<=',
  expected: unknown
): boolean {
  switch (op) {
    case '=':
      return actual === expected;
    case '!=':
      return actual !== expected;
    case '>':
      return (actual as number) > (expected as number);
    case '<':
      return (actual as number) < (expected as number);
    case '>=':
      return (actual as number) >= (expected as number);
    case '<=':
      return (actual as number) <= (expected as number);
    default:
      return false;
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Get shard ID for an entity using centralized FNV-1a hash
 */
function getShardIdForEntity(entityId: string): string {
  // Extract namespace from entity ID if it's a URL or namespace:id format
  let namespace = entityId;

  // Handle URL format
  if (entityId.startsWith('http://') || entityId.startsWith('https://')) {
    try {
      const url = new URL(entityId);
      namespace = url.hostname;
    } catch {
      // Keep as-is
    }
  } else if (entityId.includes(':')) {
    // Handle namespace:id format
    namespace = entityId.split(':')[0] ?? entityId;
  }

  // Use centralized FNV-1a hash function
  return `shard-${hashToHex(fnv1aHash(namespace))}`;
}
