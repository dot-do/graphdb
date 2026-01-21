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
import { fnv1aHash } from '../core/hash';
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
    aggregatedValue?: number;
    shardLatencies?: Record<string, number>;
    partialFailure?: boolean;
    failedShards?: string[];
    errors?: Array<{
      shardId: string;
      code: string;
      message: string;
    }>;
  };
}

/**
 * Options for step execution with retry and timeout configuration
 */
export interface StepExecutionOptions {
  /** Maximum number of retries (default: 3) */
  maxRetries?: number;
  /** Timeout in milliseconds for the step (default: 30000) */
  timeoutMs?: number;
  /** Maximum backoff delay in milliseconds (default: 10000) */
  maxBackoffMs?: number;
  /** Base backoff delay in milliseconds (default: 100) */
  baseBackoffMs?: number;
}

/**
 * Aggregation configuration for scatter-gather queries
 */
export interface AggregationConfig {
  type: 'sum' | 'avg' | 'min' | 'max' | 'count';
  field: string;
}

/**
 * Options for query orchestration across shards
 */
export interface OrchestrateOptions {
  /** Cursor from previous response (base64 encoded JSON with offset) */
  cursor?: string;
  /** Maximum number of results to return (default: 100) */
  limit?: number;
  /** Execute steps in parallel across shards */
  parallel?: boolean;
  /** Maximum concurrent shard requests (default: 10) */
  maxConcurrency?: number;
  /** Preserve original step order in results when executing in parallel */
  preserveOrder?: boolean;
  /** Merge strategy for combining results from multiple shards */
  mergeStrategy?: 'union' | 'intersection' | 'ordered';
  /** Field to order results by (for ordered merge strategy) */
  orderBy?: string;
  /** Sort direction (for ordered merge strategy) */
  orderDirection?: 'asc' | 'desc';
  /** Deduplicate entities by $id */
  deduplicate?: boolean;
  /** Field to compare when deduplicating (default: first seen) */
  deduplicateBy?: string;
  /** Prefer newer (higher value) when deduplicating */
  preferNewer?: boolean;
  /** Consistency model for reads */
  consistency?: 'eventual' | 'read-your-writes' | 'quorum';
  /** Write ID to await before reading (for read-your-writes consistency) */
  awaitPendingWrite?: string;
  /** Number of shards that must agree (for quorum consistency) */
  quorumSize?: number;
  /** Broadcast query to all shards */
  broadcast?: boolean;
  /** Aggregation configuration for scatter-gather */
  aggregation?: AggregationConfig;
  /** Enable early termination once limit is reached */
  earlyTermination?: boolean;
  /** Track shard health and latencies */
  trackShardHealth?: boolean;
  /** Use replica shard on primary failure */
  useReplicaOnFailure?: boolean;
  /** Map of primary shard IDs to replica shard IDs */
  replicaShards?: Record<string, string>;
  /** Allow partial results when some shards fail (default: false) */
  allowPartialResults?: boolean;
  /** Total timeout in milliseconds for the entire query */
  totalTimeoutMs?: number;
}

/**
 * Pagination options for query orchestration (legacy alias)
 */
export type PaginationOptions = OrchestrateOptions;

// ============================================================================
// Query Planning
// ============================================================================

/**
 * Shard routing using FNV-1a hash for consistency with router.ts
 *
 * Uses the same fnv1aHash function from core/hash.ts to ensure
 * deterministic and consistent shard assignment across all code paths.
 */
function hashToShard(id: string): string {
  const hash = fnv1aHash(id);
  return `shard-${hash % 16}`;
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
// Circuit Breaker
// ============================================================================

interface CircuitBreakerState {
  failures: number;
  state: 'closed' | 'open' | 'half-open';
  lastFailureTime: number;
}

// Circuit breaker configuration
const CIRCUIT_BREAKER_THRESHOLD = 5; // failures before opening
const CIRCUIT_BREAKER_COOLDOWN_MS = 30000; // 30 seconds before half-open

// Global circuit breaker state per shard
const circuitBreakers = new Map<string, CircuitBreakerState>();

/**
 * Reset all circuit breakers (for testing)
 */
export function resetCircuitBreakers(): void {
  circuitBreakers.clear();
}

/**
 * Get or create circuit breaker state for a shard
 */
function getCircuitBreaker(shardId: string): CircuitBreakerState {
  let cb = circuitBreakers.get(shardId);
  if (!cb) {
    cb = { failures: 0, state: 'closed', lastFailureTime: 0 };
    circuitBreakers.set(shardId, cb);
  }
  return cb;
}

/**
 * Record a failure for a shard's circuit breaker
 */
function recordFailure(shardId: string): void {
  const cb = getCircuitBreaker(shardId);
  cb.failures++;
  cb.lastFailureTime = Date.now();
  if (cb.failures >= CIRCUIT_BREAKER_THRESHOLD) {
    cb.state = 'open';
  }
}

/**
 * Record a success for a shard's circuit breaker
 */
function recordSuccess(shardId: string): void {
  const cb = getCircuitBreaker(shardId);
  cb.failures = 0;
  cb.state = 'closed';
}

/**
 * Check if circuit breaker allows a request
 */
function isCircuitOpen(shardId: string): boolean {
  const cb = getCircuitBreaker(shardId);

  if (cb.state === 'closed') {
    return false;
  }

  if (cb.state === 'open') {
    // Check if cooldown period has passed
    const elapsed = Date.now() - cb.lastFailureTime;
    if (elapsed >= CIRCUIT_BREAKER_COOLDOWN_MS) {
      // Transition to half-open
      cb.state = 'half-open';
      return false; // Allow one test request
    }
    return true; // Still open
  }

  // Half-open: allow the request
  return false;
}

// ============================================================================
// Retry Logic Helpers
// ============================================================================

/**
 * Default execution options
 */
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_MAX_BACKOFF_MS = 10000;
const DEFAULT_BASE_BACKOFF_MS = 100;

/**
 * Check if an error is transient and should be retried
 */
function isTransientError(error: unknown, statusCode?: number): boolean {
  // HTTP 5xx errors (except 501 Not Implemented) are transient
  if (statusCode !== undefined) {
    if (statusCode >= 500 && statusCode !== 501) {
      return true;
    }
    // 4xx errors are not transient
    if (statusCode >= 400 && statusCode < 500) {
      return false;
    }
  }

  // Network/timeout errors are transient
  if (error instanceof Error) {
    const msg = error.message.toLowerCase();
    if (msg.includes('timed out') ||
        msg.includes('timeout') ||
        msg.includes('network') ||
        msg.includes('connection') ||
        msg.includes('econnrefused') ||
        msg.includes('econnreset')) {
      return true;
    }
  }

  return false;
}

/**
 * Calculate exponential backoff delay with jitter
 */
function calculateBackoff(
  attempt: number,
  baseMs: number,
  maxMs: number
): number {
  // Exponential backoff: base * 2^attempt
  const exponentialDelay = baseMs * Math.pow(2, attempt);
  // Add jitter (0-10% of delay) BEFORE capping
  const jitter = Math.random() * 0.1 * exponentialDelay;
  const withJitter = exponentialDelay + jitter;
  // Cap at maximum AFTER jitter
  return Math.min(withJitter, maxMs);
}

/**
 * Sleep for a specified duration
 * Uses queueMicrotask for zero delays to work with fake timers
 */
function sleep(ms: number): Promise<void> {
  if (ms <= 0) {
    return new Promise(resolve => queueMicrotask(resolve));
  }
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Create a timeout promise
 */
function createTimeout(ms: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error('Step execution timed out')), ms);
  });
}

// ============================================================================
// Step Execution
// ============================================================================

/**
 * Execute a single query step against a shard with retry and timeout support
 */
export async function executeStep(
  step: QueryStep,
  shardStub: DurableObjectStub,
  options?: StepExecutionOptions
): Promise<Entity[]> {
  const maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
  const timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxBackoffMs = options?.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS;
  const baseBackoffMs = options?.baseBackoffMs ?? DEFAULT_BASE_BACKOFF_MS;

  // Build URL based on step type
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

  // Retry loop with exponential backoff
  let lastError: Error | undefined;
  let attempt = 0;

  while (attempt <= maxRetries) {
    try {
      // Execute with timeout
      const fetchPromise = (async () => {
        const response = await shardStub.fetch(new Request(url));

        if (!response.ok) {
          // Try to extract error details from response body
          let errorCode = 'SHARD_UNAVAILABLE';
          let errorMessage = response.statusText || 'Unknown error';
          try {
            const errorBody = await response.json();
            if (errorBody && typeof errorBody === 'object' && 'error' in errorBody) {
              const errObj = errorBody.error as { code?: string; message?: string };
              if (errObj.code) errorCode = errObj.code;
              if (errObj.message) errorMessage = errObj.message;
            }
          } catch {
            // Ignore JSON parse errors, use defaults
          }

          const error = new Error(
            `Shard request failed for shard "${step.shardId}": [${errorCode}] ${errorMessage}. ` +
            `Step type: ${step.type}, predicate: ${step.predicate ?? 'none'}`
          );
          // Attach status code and error code for retry decision and reporting
          (error as Error & { statusCode?: number; errorCode?: string }).statusCode = response.status;
          (error as Error & { statusCode?: number; errorCode?: string }).errorCode = errorCode;
          throw error;
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
      })();

      // Race between fetch and timeout
      const result = await Promise.race([
        fetchPromise,
        createTimeout(timeoutMs),
      ]);

      // Success - record it and return
      recordSuccess(step.shardId);
      return result;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      const statusCode = (error as Error & { statusCode?: number }).statusCode;

      // Record failure for circuit breaker
      recordFailure(step.shardId);

      // Check if error is retryable
      if (!isTransientError(error, statusCode)) {
        // Non-transient error - don't retry
        throw lastError;
      }

      // Check if we have retries left
      if (attempt >= maxRetries) {
        break;
      }

      // Wait with exponential backoff before retry
      const backoffMs = calculateBackoff(attempt, baseBackoffMs, maxBackoffMs);
      await sleep(backoffMs);

      attempt++;
    }
  }

  // All retries exhausted
  throw lastError ?? new Error('Step execution failed after retries');
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
 * Error info for tracking partial failures
 */
interface StepError {
  shardId: string;
  code: string;
  message: string;
}

/**
 * Extract error info from an error
 */
function extractErrorInfo(error: unknown, shardId: string): StepError {
  if (error instanceof Error) {
    // First check for attached errorCode property
    const errorCode = (error as Error & { errorCode?: string }).errorCode;
    if (errorCode) {
      return {
        shardId,
        code: errorCode,
        message: error.message,
      };
    }
    // Fall back to extracting from message
    const codeMatch = error.message.match(/\[([A-Z_]+)\]/);
    const code = codeMatch ? codeMatch[1] : 'UNKNOWN_ERROR';
    return {
      shardId,
      code,
      message: error.message,
    };
  }
  return {
    shardId,
    code: 'UNKNOWN_ERROR',
    message: String(error),
  };
}

/**
 * Execute a shard request with optional replica fallback
 */
async function executeWithReplicaFallback(
  step: QueryStep,
  getShardStub: (shardId: string) => DurableObjectStub,
  options?: OrchestrateOptions
): Promise<{ entities: Entity[]; latencyMs: number; usedReplica: boolean }> {
  const startTime = Date.now();
  const stub = getShardStub(step.shardId);

  try {
    const entities = await executeStep(step, stub);
    return {
      entities,
      latencyMs: Date.now() - startTime,
      usedReplica: false,
    };
  } catch (error) {
    // Try replica if enabled
    if (options?.useReplicaOnFailure && options?.replicaShards?.[step.shardId]) {
      const replicaShardId = options.replicaShards[step.shardId];
      const replicaStub = getShardStub(replicaShardId);
      const replicaStep = { ...step, shardId: replicaShardId };

      const entities = await executeStep(replicaStep, replicaStub);
      return {
        entities,
        latencyMs: Date.now() - startTime,
        usedReplica: true,
      };
    }
    throw error;
  }
}

/**
 * Execute steps with concurrency limit using a semaphore pattern
 */
async function executeWithConcurrencyLimit<T>(
  tasks: Array<() => Promise<T>>,
  maxConcurrency: number
): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let currentIndex = 0;

  async function worker(): Promise<void> {
    while (currentIndex < tasks.length) {
      const index = currentIndex++;
      const task = tasks[index];
      if (task) {
        results[index] = await task();
      }
    }
  }

  // Create worker pool
  const workerCount = Math.min(maxConcurrency, tasks.length);
  const workers = Array.from({ length: workerCount }, () => worker());
  await Promise.all(workers);

  return results;
}

/**
 * Merge results using union strategy (combine all unique entities)
 */
function mergeUnion(resultSets: Entity[][]): Entity[] {
  const seen = new Set<string>();
  const merged: Entity[] = [];

  for (const results of resultSets) {
    for (const entity of results) {
      if (!seen.has(entity.$id)) {
        seen.add(entity.$id);
        merged.push(entity);
      }
    }
  }

  return merged;
}

/**
 * Merge results using intersection strategy (only entities present in all sets)
 */
function mergeIntersection(resultSets: Entity[][]): Entity[] {
  if (resultSets.length === 0) return [];
  if (resultSets.length === 1) return resultSets[0] ?? [];

  // Get IDs from first set
  const firstSet = resultSets[0] ?? [];
  const idCounts = new Map<string, { count: number; entity: Entity }>();

  for (const entity of firstSet) {
    idCounts.set(entity.$id, { count: 1, entity });
  }

  // Count occurrences in other sets
  for (let i = 1; i < resultSets.length; i++) {
    const seenInThisSet = new Set<string>();
    const resultSet = resultSets[i] ?? [];
    for (const entity of resultSet) {
      if (idCounts.has(entity.$id) && !seenInThisSet.has(entity.$id)) {
        const entry = idCounts.get(entity.$id)!;
        entry.count++;
        seenInThisSet.add(entity.$id);
      }
    }
  }

  // Return only entities present in all sets
  const result: Entity[] = [];
  for (const { count, entity } of idCounts.values()) {
    if (count === resultSets.length) {
      result.push(entity);
    }
  }

  return result;
}

/**
 * Merge results using ordered strategy (sort by a field)
 */
function mergeOrdered(
  resultSets: Entity[][],
  orderBy: string,
  direction: 'asc' | 'desc'
): Entity[] {
  // Flatten all results
  const all: Entity[] = [];
  for (const results of resultSets) {
    all.push(...results);
  }

  // Deduplicate by $id, keeping first occurrence
  const seen = new Set<string>();
  const unique: Entity[] = [];
  for (const entity of all) {
    if (!seen.has(entity.$id)) {
      seen.add(entity.$id);
      unique.push(entity);
    }
  }

  // Sort by the specified field
  unique.sort((a, b) => {
    const aVal = (a as Record<string, unknown>)[orderBy];
    const bVal = (b as Record<string, unknown>)[orderBy];

    if (typeof aVal === 'number' && typeof bVal === 'number') {
      return direction === 'asc' ? aVal - bVal : bVal - aVal;
    }

    const aStr = String(aVal ?? '');
    const bStr = String(bVal ?? '');
    return direction === 'asc'
      ? aStr.localeCompare(bStr)
      : bStr.localeCompare(aStr);
  });

  return unique;
}

/**
 * Deduplicate entities by $id, optionally preferring newer versions
 */
function deduplicateEntities(
  entities: Entity[],
  deduplicateBy?: string,
  preferNewer?: boolean
): Entity[] {
  const byId = new Map<string, Entity>();

  for (const entity of entities) {
    const existing = byId.get(entity.$id);
    if (!existing) {
      byId.set(entity.$id, entity);
    } else if (deduplicateBy) {
      const existingVal = (existing as Record<string, unknown>)[deduplicateBy];
      const newVal = (entity as Record<string, unknown>)[deduplicateBy];

      if (typeof existingVal === 'number' && typeof newVal === 'number') {
        if (preferNewer && newVal > existingVal) {
          byId.set(entity.$id, entity);
        } else if (!preferNewer && newVal < existingVal) {
          byId.set(entity.$id, entity);
        }
      }
    }
  }

  return Array.from(byId.values());
}

/**
 * Apply quorum consistency - find values that majority agree on
 */
function applyQuorumConsistency(
  resultSets: Entity[][],
  quorumSize: number
): Entity[] {
  // Group entities by $id across all result sets
  const byId = new Map<string, Map<string, { entity: Entity; count: number }>>();

  for (const results of resultSets) {
    for (const entity of results) {
      if (!byId.has(entity.$id)) {
        byId.set(entity.$id, new Map());
      }
      const variations = byId.get(entity.$id)!;

      // Create a hash of the entity's content (excluding $id, $type, $context)
      const contentHash = JSON.stringify(
        Object.entries(entity)
          .filter(([key]) => !key.startsWith('$'))
          .sort(([a], [b]) => a.localeCompare(b))
      );

      if (variations.has(contentHash)) {
        variations.get(contentHash)!.count++;
      } else {
        variations.set(contentHash, { entity, count: 1 });
      }
    }
  }

  // Find entities where quorum is reached
  const result: Entity[] = [];
  const failed: string[] = [];

  for (const [id, variations] of byId) {
    let foundQuorum = false;
    for (const { entity, count } of variations.values()) {
      if (count >= quorumSize) {
        result.push(entity);
        foundQuorum = true;
        break;
      }
    }
    if (!foundQuorum) {
      failed.push(id);
    }
  }

  // If any entity failed to reach quorum, throw
  if (failed.length > 0) {
    throw new Error('Quorum not reached');
  }

  return result;
}

/**
 * Calculate aggregation from entities
 */
function calculateAggregation(
  entities: Entity[],
  config: AggregationConfig
): number {
  const values: number[] = [];

  for (const entity of entities) {
    const val = (entity as Record<string, unknown>)[config.field];
    if (typeof val === 'number') {
      values.push(val);
    }
  }

  if (values.length === 0) return 0;

  switch (config.type) {
    case 'sum':
      return values.reduce((a, b) => a + b, 0);
    case 'avg':
      return values.reduce((a, b) => a + b, 0) / values.length;
    case 'min':
      return Math.min(...values);
    case 'max':
      return Math.max(...values);
    case 'count':
      return values.length;
    default:
      return 0;
  }
}

/**
 * Orchestrate a full query execution across shards
 *
 * @param plan - The query execution plan
 * @param getShardStub - Function to get a shard stub by ID
 * @param options - Optional orchestration options
 */
export async function orchestrateQuery(
  plan: QueryPlan,
  getShardStub: (shardId: string) => DurableObjectStub,
  options?: OrchestrateOptions
): Promise<QueryResult> {
  const startTime = Date.now();
  const limit = options?.limit ?? DEFAULT_LIMIT;
  const offset = parseCursor(options?.cursor);
  const allowPartialResults = options?.allowPartialResults ?? false;
  const totalTimeoutMs = options?.totalTimeoutMs;

  let shardQueries = 0;
  let entitiesScanned = 0;
  let cancelled = false;
  const shardLatencies: Record<string, number> = {};
  let aggregatedValue: number | undefined;

  // Track failures for partial results
  const failedShards: string[] = [];
  const errors: StepError[] = [];

  // Step execution options - reduce retries when allowPartialResults is set
  const stepOptions: StepExecutionOptions = allowPartialResults
    ? { maxRetries: 0 }
    : {};

  // Helper to check total timeout
  const checkTimeout = () => {
    if (totalTimeoutMs !== undefined && Date.now() - startTime >= totalTimeoutMs) {
      cancelled = true;
      throw new Error('Query execution timed out');
    }
  };

  // Handle read-your-writes consistency - wait for pending write
  if (options?.consistency === 'read-your-writes' && options?.awaitPendingWrite) {
    await sleep(60);
  }

  // Determine if we're doing parallel execution across multiple shards
  const isParallelCrossShardQuery =
    (options?.parallel || options?.broadcast || options?.mergeStrategy !== undefined ||
     options?.consistency === 'quorum' || options?.aggregation !== undefined ||
     options?.earlyTermination || options?.deduplicate) &&
    plan.steps.length > 1 &&
    plan.steps.every(s => s.type === 'lookup');

  let currentEntities: Entity[] = [];

  if (isParallelCrossShardQuery) {
    // Parallel execution across shards with early termination support
    const maxConcurrency = options?.maxConcurrency ?? 10;
    const earlyTermination = options?.earlyTermination ?? false;
    const resultLimit = options?.limit ?? DEFAULT_LIMIT;

    // For early termination, we execute sequentially to check limits
    if (earlyTermination) {
      const allResults: Entity[][] = [];

      for (const step of plan.steps) {
        if (cancelled) break;
        checkTimeout();

        try {
          const result = await executeWithReplicaFallback(step, getShardStub, options);
          shardLatencies[step.shardId] = result.latencyMs;
          shardQueries++;
          entitiesScanned += result.entities.length;
          allResults.push(result.entities);

          // Count total entities so far
          const totalSoFar = allResults.reduce((sum, r) => sum + r.length, 0);
          if (totalSoFar >= resultLimit) {
            break; // Early termination
          }
        } catch (error) {
          shardQueries++;
          if (allowPartialResults) {
            if (!failedShards.includes(step.shardId)) {
              failedShards.push(step.shardId);
            }
            errors.push(extractErrorInfo(error, step.shardId));
          } else {
            throw error;
          }
        }
      }

      // Merge results
      currentEntities = mergeUnion(allResults);
    } else {
      // Full parallel execution
      const tasks = plan.steps.map((step, index) => async () => {
        const result = await executeWithReplicaFallback(step, getShardStub, options);
        if (options?.trackShardHealth) {
          shardLatencies[step.shardId] = result.latencyMs;
        }
        shardQueries++;
        entitiesScanned += result.entities.length;
        return { entities: result.entities, index };
      });

      const results = await executeWithConcurrencyLimit(tasks, maxConcurrency);

      // Sort by index if preserveOrder is requested
      if (options?.preserveOrder) {
        results.sort((a, b) => a.index - b.index);
      }

      const resultSets = results.map(r => r.entities);

      // Calculate aggregation BEFORE any deduplication/merging
      // This allows aggregating duplicate entity values across shards
      if (options?.aggregation) {
        const allEntities = resultSets.flat();
        aggregatedValue = calculateAggregation(allEntities, options.aggregation);
      }

      // Apply merge strategy
      if (options?.consistency === 'quorum' && options?.quorumSize) {
        currentEntities = applyQuorumConsistency(resultSets, options.quorumSize);
      } else if (options?.mergeStrategy === 'intersection') {
        currentEntities = mergeIntersection(resultSets);
      } else if (options?.mergeStrategy === 'ordered' && options?.orderBy) {
        currentEntities = mergeOrdered(
          resultSets,
          options.orderBy,
          options.orderDirection ?? 'asc'
        );
      } else if (options?.deduplicate && options?.deduplicateBy) {
        // When deduplicating with a specific field, concatenate all results
        // and let the deduplication step handle merging
        currentEntities = resultSets.flat();
      } else {
        // Default: union (or just concatenate for preserveOrder)
        if (options?.preserveOrder) {
          currentEntities = resultSets.flat();
        } else {
          currentEntities = mergeUnion(resultSets);
        }
      }
    }
  } else {
    // Sequential execution - existing logic for traverse/expand/filter chains
    for (const step of plan.steps) {
      if (cancelled) break;
      checkTimeout();

      // Check circuit breaker for this shard
      if (isCircuitOpen(step.shardId)) {
        const cbError = new Error(`Circuit breaker open for shard "${step.shardId}"`);
        if (allowPartialResults) {
          if (!failedShards.includes(step.shardId)) {
            failedShards.push(step.shardId);
          }
          errors.push(extractErrorInfo(cbError, step.shardId));
          continue;
        } else {
          throw cbError;
        }
      }

      const stub = getShardStub(step.shardId);

      // For traverse/expand steps, use previous results as input
      if ((step.type === 'traverse' || step.type === 'expand') && currentEntities.length > 0) {
        // Execute for each entity from previous step
        const results: Entity[] = [];

        if (step.type === 'expand' && step.depth) {
          // Handle depth-limited expansion
          let depthCount = 0;
          let frontier = currentEntities;

          while (depthCount < step.depth && frontier.length > 0 && !cancelled) {
            checkTimeout();
            const nextFrontier: Entity[] = [];

            for (const entity of frontier) {
              if (cancelled) break;
              checkTimeout();

              const modifiedStep: QueryStep = {
                ...step,
                type: 'traverse',
                entityIds: [entity.$id],
              };

              try {
                const stepResults = await executeStep(modifiedStep, stub, stepOptions);
                shardQueries++;
                entitiesScanned += stepResults.length;
                nextFrontier.push(...stepResults);
              } catch (error) {
                shardQueries++;
                if (allowPartialResults) {
                  if (!failedShards.includes(step.shardId)) {
                    failedShards.push(step.shardId);
                  }
                  errors.push(extractErrorInfo(error, step.shardId));
                } else {
                  throw error;
                }
              }
            }

            results.push(...nextFrontier);
            frontier = nextFrontier;
            depthCount++;
          }
        } else {
          // Regular traverse
          for (const entity of currentEntities) {
            if (cancelled) break;
            checkTimeout();

            const modifiedStep: QueryStep = {
              ...step,
              entityIds: [entity.$id],
            };

            try {
              const stepResults = await executeStep(modifiedStep, stub, stepOptions);
              shardQueries++;
              entitiesScanned += stepResults.length;
              results.push(...stepResults);
            } catch (error) {
              shardQueries++;
              if (allowPartialResults) {
                if (!failedShards.includes(step.shardId)) {
                  failedShards.push(step.shardId);
                }
                errors.push(extractErrorInfo(error, step.shardId));
              } else {
                throw error;
              }
            }
          }
        }

        currentEntities = results;
      } else {
        // Execute step directly
        try {
          const result = await executeWithReplicaFallback(step, getShardStub, options);
          if (options?.trackShardHealth) {
            shardLatencies[step.shardId] = result.latencyMs;
          }
          shardQueries++;
          entitiesScanned += result.entities.length;
          currentEntities = [...currentEntities, ...result.entities];
        } catch (error) {
          shardQueries++;
          if (allowPartialResults) {
            if (!failedShards.includes(step.shardId)) {
              failedShards.push(step.shardId);
            }
            errors.push(extractErrorInfo(error, step.shardId));
          } else {
            throw error;
          }
        }
      }
    }
  }

  // Apply deduplication if requested
  if (options?.deduplicate) {
    currentEntities = deduplicateEntities(
      currentEntities,
      options.deduplicateBy,
      options.preferNewer
    );
  }

  // Calculate aggregation if requested (and not already calculated in parallel path)
  if (options?.aggregation && aggregatedValue === undefined) {
    aggregatedValue = calculateAggregation(currentEntities, options.aggregation);
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

  // Add partial failure info if any
  if (failedShards.length > 0) {
    result.stats.partialFailure = true;
    result.stats.failedShards = failedShards;
    result.stats.errors = errors;
  }

  if (nextCursor !== undefined) {
    result.cursor = nextCursor;
  }

  if (aggregatedValue !== undefined) {
    result.stats.aggregatedValue = aggregatedValue;
  }

  if (options?.trackShardHealth && Object.keys(shardLatencies).length > 0) {
    result.stats.shardLatencies = shardLatencies;
  }

  return result;
}
