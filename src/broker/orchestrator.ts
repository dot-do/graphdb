/**
 * Query Orchestrator for GraphDB
 *
 * Handles query planning and execution across shards:
 * - planQuery: Parse query and create execution plan
 * - executeStep: Execute a single step against a shard
 * - orchestrateQuery: Coordinate full query across shards
 * - batchLookups: Optimize multiple lookups to same shard
 */

import type { EntityId } from '../core/types';
import { createEntityId } from '../core/types';
import type { Entity } from '../core/entity';
import { createEntity } from '../core/entity';
import { encodeString, toBase64 } from '../core/index';
import { validateShardResponse, isShardError } from './response-validator';

// ============================================================================
// Types
// ============================================================================

export interface QueryPlan {
  steps: QueryStep[];
  estimatedCost: number;
  canBatch: boolean;
}

export interface QueryStep {
  type: 'lookup' | 'traverse' | 'filter' | 'expand';
  shardId: string;
  entityIds?: EntityId[];
  predicate?: string;
  filter?: FilterExpr;
  depth?: number;
}

export interface FilterExpr {
  field: string;
  op: '>' | '<' | '>=' | '<=' | '=' | '!=';
  value: unknown;
}

/**
 * Raw entity data received from shard RPC responses.
 * This is the shape of data before it's converted to a full Entity.
 * May be a complete Entity or partial data that needs wrapping.
 */
export interface ShardEntityResponse {
  $id?: string;
  $type?: string | string[];
  $context?: string;
  _namespace?: string;
  _localId?: string;
  [key: string]: unknown;
}

export interface QueryResult {
  entities: Entity[];
  cursor?: string;
  hasMore: boolean;
  stats: {
    shardQueries: number;
    entitiesScanned: number;
    durationMs: number;
  };
}

/**
 * Pagination options for query orchestration
 */
export interface PaginationOptions {
  /** Cursor from previous response (base64 encoded JSON with offset) */
  cursor?: string;
  /** Maximum number of results to return (default: 100) */
  limit?: number;
}

// ============================================================================
// Query Planning
// ============================================================================

/**
 * Simple hash function to determine shard routing from entity ID
 */
function hashToShard(id: string): string {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    const char = id.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return `shard-${Math.abs(hash) % 16}`;
}

/**
 * Parse a query string and create an execution plan
 *
 * Supports a subset of Cypher-like syntax:
 * - MATCH (n {$id: "..."}) RETURN n
 * - MATCH (p)-[:rel]->(q) WHERE p.$id = "..." RETURN q
 * - MATCH (p) WHERE p.field op value RETURN p
 */
export function planQuery(query: string): QueryPlan {
  if (!query || query.trim() === '') {
    throw new Error(
      'Query cannot be empty. ' +
      'Provide a valid Cypher-like query string (e.g., MATCH (n {$id: "..."}) RETURN n).'
    );
  }

  const normalizedQuery = query.trim().toUpperCase();

  // Basic validation - must have MATCH and RETURN
  if (!normalizedQuery.includes('MATCH') || !normalizedQuery.includes('RETURN')) {
    const hasMATCH = normalizedQuery.includes('MATCH');
    const hasRETURN = normalizedQuery.includes('RETURN');
    const missing = [];
    if (!hasMATCH) missing.push('MATCH');
    if (!hasRETURN) missing.push('RETURN');
    throw new Error(
      `Invalid query: missing required clause(s): ${missing.join(', ')}. ` +
      `Query received: "${query.slice(0, 100)}${query.length > 100 ? '...' : ''}". ` +
      `Expected format: MATCH (n {$id: "..."}) WHERE ... RETURN n`
    );
  }

  const steps: QueryStep[] = [];
  let estimatedCost = 0;
  let canBatch = false;

  // Check for IN clause (batch query)
  if (query.toUpperCase().includes(' IN [')) {
    canBatch = true;
  }

  // Parse entity ID lookups
  const idMatch = query.match(/\$id:\s*"([^"]+)"/i) ||
                  query.match(/\$id\s*=\s*"([^"]+)"/i);

  if (idMatch && idMatch[1]) {
    const entityId = idMatch[1];
    steps.push({
      type: 'lookup',
      shardId: hashToShard(entityId),
      entityIds: [createEntityId(entityId)],
    });
    estimatedCost += 1;
  }

  // Parse IN clause for batch lookups
  const inMatch = query.match(/\$id\s+IN\s+\[([^\]]+)\]/i);
  if (inMatch && inMatch[1]) {
    const ids = inMatch[1].match(/"([^"]+)"/g)?.map(s => s.replace(/"/g, '')) ?? [];
    if (ids.length > 0) {
      // Group by shard
      const byShardMap = new Map<string, EntityId[]>();
      for (const id of ids) {
        const shard = hashToShard(id);
        const existing = byShardMap.get(shard) ?? [];
        existing.push(createEntityId(id));
        byShardMap.set(shard, existing);
      }

      for (const [shardId, entityIds] of byShardMap) {
        steps.push({
          type: 'lookup',
          shardId,
          entityIds,
        });
      }
      estimatedCost += ids.length * 0.5; // Batch is cheaper per entity
    }
  }

  // Parse relationship traversals: -[:rel]->
  const relMatch = query.match(/-\[:(\w+)\]->/);
  if (relMatch && relMatch[1]) {
    const predicate = relMatch[1];
    // Get the shard from the source entity if available
    const shardId = steps.length > 0 ? steps[0]!.shardId : 'shard-0';

    const traverseStep: QueryStep = {
      type: 'traverse',
      shardId,
      predicate,
    };
    steps.push(traverseStep);
    estimatedCost += 2;
  }

  // Parse multi-hop with depth: -[:rel*min..max]->
  const multiHopMatch = query.match(/-\[:(\w+)\*(\d+)\.\.(\d+)\]->/);
  if (multiHopMatch && multiHopMatch[1] && multiHopMatch[3]) {
    const predicate = multiHopMatch[1];
    const maxDepth = parseInt(multiHopMatch[3], 10);
    const shardId = steps.length > 0 ? steps[0]!.shardId : 'shard-0';

    // Replace existing traverse with expand
    const existingTraverseIdx = steps.findIndex(s => s.type === 'traverse' && s.predicate === predicate);
    if (existingTraverseIdx >= 0) {
      steps.splice(existingTraverseIdx, 1);
    }

    const expandStep: QueryStep = {
      type: 'expand',
      shardId,
      predicate,
      depth: maxDepth,
    };
    steps.push(expandStep);
    estimatedCost += maxDepth * 3;
  }

  // Check for second hop in multi-hop queries
  const secondHopMatches = query.match(/-\[:(\w+)\]->/g);
  if (secondHopMatches && secondHopMatches.length > 1 && secondHopMatches[1]) {
    const shardId = steps.length > 0 ? steps[0]!.shardId : 'shard-0';

    // Add expand step for the second hop
    const secondRel = secondHopMatches[1].match(/-\[:(\w+)\]->/)?.[1];
    if (secondRel) {
      const secondExpandStep: QueryStep = {
        type: 'expand',
        shardId,
        predicate: secondRel,
        depth: 1,
      };
      steps.push(secondExpandStep);
      estimatedCost += 3;
    }
  }

  // Parse WHERE filters
  const filterMatch = query.match(/WHERE\s+\w+\.(\w+)\s*(>=|<=|!=|>|<|=)\s*("[^"]+"|[\d.]+)/i);
  if (filterMatch && filterMatch[1] && filterMatch[2] && filterMatch[3]) {
    const field = filterMatch[1];
    const op = filterMatch[2] as FilterExpr['op'];
    const rawValue = filterMatch[3];

    // Parse value type - convert from string to appropriate type
    let value: unknown;
    if (rawValue.startsWith('"')) {
      value = rawValue.slice(1, -1); // Remove quotes for string values
    } else if (!isNaN(Number(rawValue))) {
      value = Number(rawValue);
    } else {
      value = rawValue;
    }

    const shardId = steps.length > 0 ? steps[0]!.shardId : 'shard-0';

    steps.push({
      type: 'filter',
      shardId,
      filter: { field, op, value },
    });
    estimatedCost += 1;
  }

  // If no steps were created but query was valid syntax, create empty lookup
  if (steps.length === 0) {
    steps.push({
      type: 'lookup',
      shardId: 'shard-0',
    });
    estimatedCost = 1;
  }

  return {
    steps,
    estimatedCost,
    canBatch,
  };
}

// ============================================================================
// Step Execution
// ============================================================================

/**
 * Execute a single query step against a shard
 */
export async function executeStep(
  step: QueryStep,
  shardStub: DurableObjectStub
): Promise<Entity[]> {
  let url: string;

  switch (step.type) {
    case 'lookup': {
      const ids = step.entityIds?.join(',') ?? '';
      url = `https://shard-do/lookup?ids=${encodeURIComponent(ids)}`;
      break;
    }

    case 'traverse': {
      const fromId = step.entityIds?.[0] ?? '';
      url = `https://shard-do/traverse?from=${encodeURIComponent(fromId)}&predicate=${step.predicate ?? ''}`;
      break;
    }

    case 'expand': {
      const fromId = step.entityIds?.[0] ?? '';
      url = `https://shard-do/traverse?from=${encodeURIComponent(fromId)}&predicate=${step.predicate ?? ''}&depth=${step.depth ?? 1}`;
      break;
    }

    case 'filter': {
      const filter = step.filter!;
      url = `https://shard-do/filter?field=${filter.field}&op=${encodeURIComponent(filter.op)}&value=${encodeURIComponent(String(filter.value))}`;
      break;
    }

    default:
      throw new Error(
        `Unknown step type: "${step.type}". ` +
        `Valid step types are: lookup, traverse, filter, expand. ` +
        `This may indicate a bug in query planning.`
      );
  }

  const response = await shardStub.fetch(new Request(url));

  if (!response.ok) {
    const statusText = response.statusText || 'Unknown error';
    throw new Error(
      `Shard request failed for shard "${step.shardId}": HTTP ${response.status} ${statusText}. ` +
      `Step type: ${step.type}, predicate: ${step.predicate ?? 'none'}`
    );
  }

  const rawData = await response.json();

  // Validate response structure
  const validatedResponse = validateShardResponse<ShardEntityResponse[]>(rawData);

  // Check for shard errors
  if (isShardError(validatedResponse)) {
    throw new Error(
      `Shard error [${validatedResponse.error.code}]: ${validatedResponse.error.message}. ` +
      `Shard: "${step.shardId}", step type: ${step.type}${step.predicate ? `, predicate: "${step.predicate}"` : ''}`
    );
  }

  const data = validatedResponse.data;

  // Parse response into Entity objects
  if (Array.isArray(data)) {
    return data.map((item: ShardEntityResponse) => {
      if (item.$id && item.$type && item.$context) {
        return item as Entity;
      }
      // If raw entity data, wrap it with required fields
      const id = createEntityId(item.$id ?? 'https://unknown');
      return createEntity(id, item.$type ?? 'Unknown', {});
    });
  }

  return [];
}

// ============================================================================
// Batch Optimization
// ============================================================================

/**
 * Combine multiple lookup steps to the same shard into a single batch
 *
 * Note: The entityIds in input steps are already validated EntityId types.
 * We use Set<EntityId> to preserve the branded type through deduplication.
 */
export function batchLookups(steps: QueryStep[]): QueryStep[] {
  if (steps.length === 0) {
    return [];
  }

  // Use Set<EntityId> to preserve the branded type
  const lookupsByShardMap = new Map<string, Set<EntityId>>();
  const nonLookupSteps: QueryStep[] = [];

  for (const step of steps) {
    if (step.type === 'lookup' && step.entityIds) {
      const shardId = step.shardId;
      const existing = lookupsByShardMap.get(shardId) ?? new Set<EntityId>();

      for (const id of step.entityIds) {
        // id is already EntityId from step.entityIds type
        existing.add(id);
      }

      lookupsByShardMap.set(shardId, existing);
    } else {
      nonLookupSteps.push(step);
    }
  }

  const result: QueryStep[] = [];

  // Add batched lookup steps - Array.from preserves EntityId type
  for (const [shardId, ids] of lookupsByShardMap) {
    result.push({
      type: 'lookup',
      shardId,
      entityIds: Array.from(ids),
    });
  }

  // Add non-lookup steps
  result.push(...nonLookupSteps);

  return result;
}

// ============================================================================
// Query Orchestration
// ============================================================================

const DEFAULT_LIMIT = 100;

/**
 * Parse a cursor string to extract offset
 * @param cursor - Base64 encoded JSON cursor string
 * @returns offset value or 0 if invalid/missing
 */
function parseCursor(cursor?: string): number {
  if (!cursor) return 0;

  try {
    // Handle both btoa and toBase64 encoded cursors
    let decoded: string;
    try {
      // Try standard atob first
      decoded = atob(cursor);
    } catch {
      // Fall back to manual base64 decode for Uint8Array-based encoding
      const binaryString = atob(cursor);
      const bytes = new Uint8Array(binaryString.length);
      for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }
      decoded = new TextDecoder().decode(bytes);
    }

    const parsed = JSON.parse(decoded);
    return typeof parsed.offset === 'number' ? parsed.offset : 0;
  } catch {
    return 0;
  }
}

/**
 * Create a cursor string from an offset
 * @param offset - The offset value to encode
 * @returns Base64 encoded cursor string
 */
function createCursor(offset: number): string {
  return toBase64(encodeString(JSON.stringify({ offset })));
}

/**
 * Orchestrate a full query execution across shards
 *
 * @param plan - The query execution plan
 * @param getShardStub - Function to get a shard stub by ID
 * @param options - Optional pagination options (cursor and limit)
 */
export async function orchestrateQuery(
  plan: QueryPlan,
  getShardStub: (shardId: string) => DurableObjectStub,
  options?: PaginationOptions
): Promise<QueryResult> {
  const startTime = Date.now();
  const limit = options?.limit ?? DEFAULT_LIMIT;
  const offset = parseCursor(options?.cursor);

  let shardQueries = 0;
  let entitiesScanned = 0;
  let currentEntities: Entity[] = [];

  // Execute steps sequentially, passing results between steps
  for (const step of plan.steps) {
    const stub = getShardStub(step.shardId);

    // For traverse/expand steps, use previous results as input
    if ((step.type === 'traverse' || step.type === 'expand') && currentEntities.length > 0) {
      // Execute for each entity from previous step
      const results: Entity[] = [];

      if (step.type === 'expand' && step.depth) {
        // Handle depth-limited expansion
        let depthCount = 0;
        let frontier = currentEntities;

        while (depthCount < step.depth && frontier.length > 0) {
          const nextFrontier: Entity[] = [];

          for (const entity of frontier) {
            const modifiedStep: QueryStep = {
              ...step,
              type: 'traverse',
              entityIds: [entity.$id],
            };

            const stepResults = await executeStep(modifiedStep, stub);
            shardQueries++;
            entitiesScanned += stepResults.length;
            nextFrontier.push(...stepResults);
          }

          results.push(...nextFrontier);
          frontier = nextFrontier;
          depthCount++;
        }
      } else {
        // Regular traverse
        for (const entity of currentEntities) {
          const modifiedStep: QueryStep = {
            ...step,
            entityIds: [entity.$id],
          };

          const stepResults = await executeStep(modifiedStep, stub);
          shardQueries++;
          entitiesScanned += stepResults.length;
          results.push(...stepResults);
        }
      }

      currentEntities = results;
    } else {
      // Execute step directly
      const results = await executeStep(step, stub);
      shardQueries++;
      entitiesScanned += results.length;
      currentEntities = results;
    }
  }

  // Apply pagination with offset and limit
  const totalEntities = currentEntities.length;
  const paginatedEntities = currentEntities.slice(offset, offset + limit);
  const hasMore = offset + limit < totalEntities;
  const nextCursor = hasMore ? createCursor(offset + limit) : undefined;

  const durationMs = Date.now() - startTime;

  const result: QueryResult = {
    entities: paginatedEntities,
    hasMore,
    stats: {
      shardQueries,
      entitiesScanned,
      durationMs,
    },
  };

  if (nextCursor !== undefined) {
    result.cursor = nextCursor;
  }

  return result;
}
