/**
 * GraphDB Query Planner
 *
 * Converts parsed query AST into execution plans with shard routing,
 * cost estimation, and caching information.
 */

import type {
  QueryNode,
  FilterCondition,
} from './parser';
import type { ShardInfo } from '../snippet/router';
import { generateCacheKey, canServeFromCache } from '../snippet/router';
import type { Namespace } from '../core/types';
import { isNamespace, createNamespace } from '../core/types';
import { fnv1aHash, hashToHex } from '../core/hash';

// ============================================================================
// Types
// ============================================================================

/**
 * A complete execution plan for a query
 */
export interface QueryPlan {
  /** Ordered list of execution steps */
  steps: PlanStep[];
  /** Shards involved in this query */
  shards: ShardInfo[];
  /** Estimated cost for rate limiting */
  estimatedCost: number;
  /** Whether this query can be cached */
  canCache: boolean;
  /** Cache key if cacheable */
  cacheKey?: string;
}

/**
 * A single step in the execution plan
 */
export interface PlanStep {
  /** Type of operation */
  type: 'lookup' | 'traverse' | 'reverse' | 'filter' | 'expand' | 'recurse';
  /** Shard to execute on */
  shardId: string;
  /** Entity IDs to look up (for lookup step) */
  entityIds?: string[];
  /** Predicate name (for traverse/reverse) */
  predicate?: string;
  /** Filter expression (for filter step) */
  filter?: FilterExpr;
  /** Maximum depth (for recurse step) */
  maxDepth?: number;
  /** Fields to expand (for expand step) */
  fields?: string[];
}

/**
 * Filter expression for query execution
 */
export interface FilterExpr {
  /** Field to filter on */
  field: string;
  /** Comparison operator */
  op: '=' | '!=' | '>' | '<' | '>=' | '<=';
  /** Value to compare against */
  value: unknown;
  /** AND condition (optional) */
  and?: FilterExpr;
  /** OR condition (optional) */
  or?: FilterExpr;
}

// ============================================================================
// Cost Constants
// ============================================================================

/** Base cost for entity lookup */
const LOOKUP_COST = 1;

/** Cost per traversal hop */
const TRAVERSE_COST = 2;

/** Cost for reverse traversal (more expensive) */
const REVERSE_COST = 3;

/** Cost per filter operation */
const FILTER_COST = 1;

/** Cost per expanded field */
const EXPAND_COST_PER_FIELD = 0.5;

/** Cost multiplier for recursion per depth level */
const RECURSE_COST_PER_DEPTH = 5;

/** Default max depth if unbounded */
const DEFAULT_MAX_DEPTH = 10;

// ============================================================================
// Shard ID Generation (uses shared hash from core)
// ============================================================================

/**
 * Get shard ID for a namespace string
 * Works with both URL namespaces and short-form (e.g., "user")
 */
function getShardIdForNamespace(namespace: string): string {
  const hash = fnv1aHash(namespace);
  return `shard-${hashToHex(hash)}`;
}

/**
 * Create namespace-like string for shard info
 * Handles both URL-based and short-form namespaces
 */
function createNamespaceForPlan(namespace: string): Namespace {
  // If it's already a valid URL, use it
  if (isNamespace(namespace)) {
    return createNamespace(namespace);
  }

  // For short-form namespaces, create a placeholder URL-like namespace
  // This allows the planner to work with both query syntaxes
  const placeholderUrl = `https://graphdb.local/${namespace}/`;
  return createNamespace(placeholderUrl);
}

// ============================================================================
// Main Functions
// ============================================================================

/**
 * Create an execution plan from a parsed query AST
 */
export function planQuery(ast: QueryNode): QueryPlan {
  const steps: PlanStep[] = [];
  const shardSet = new Map<string, ShardInfo>();

  // Collect all steps by walking the AST
  // AST is built bottom-up, but collectSteps processes source first,
  // so steps are already in execution order (lookup -> traverse -> filter -> etc.)
  collectSteps(ast, steps, shardSet);

  // Calculate shard info
  const shards = Array.from(shardSet.values());

  // Calculate cost
  const estimatedCost = calculateCost(steps);

  // Determine cacheability
  const queryString = stringifyForCache(ast);
  const cacheable = canServeFromCache(queryString);

  const result: QueryPlan = {
    steps,
    shards,
    estimatedCost,
    canCache: cacheable,
  };
  if (cacheable) {
    result.cacheKey = generateCacheKey(queryString);
  }
  return result;
}

/**
 * Optimize a query plan for better performance
 */
export function optimizePlan(plan: QueryPlan): QueryPlan {
  const optimizedSteps = [...plan.steps];

  // Optimization 1: Combine adjacent lookups to the same shard
  let i = 0;
  while (i < optimizedSteps.length - 1) {
    // Non-null assertions safe: loop condition ensures indices are in bounds
    const current = optimizedSteps[i]!;
    const next = optimizedSteps[i + 1]!;

    if (
      current.type === 'lookup' &&
      next.type === 'lookup' &&
      current.shardId === next.shardId
    ) {
      // Merge entity IDs
      current.entityIds = [
        ...(current.entityIds || []),
        ...(next.entityIds || []),
      ];
      optimizedSteps.splice(i + 1, 1);
    } else {
      i++;
    }
  }

  // Optimization 2: Push filters down closer to data source
  // (When possible, filter before traversal to reduce data volume)
  // This is a simple heuristic - a more sophisticated optimizer would analyze data flow

  return {
    ...plan,
    steps: optimizedSteps,
    estimatedCost: calculateCost(optimizedSteps),
  };
}

/**
 * Estimate the cost of executing a query plan
 */
export function estimateCost(plan: QueryPlan): number {
  return plan.estimatedCost;
}

// ============================================================================
// Internal Functions
// ============================================================================

/**
 * Recursively collect execution steps from AST
 */
function collectSteps(
  node: QueryNode,
  steps: PlanStep[],
  shardSet: Map<string, ShardInfo>
): void {
  switch (node.type) {
    case 'entity': {
      const entityId = `${node.namespace}:${node.id}`;
      const namespace = createNamespaceForPlan(node.namespace);
      const shardId = getShardIdForNamespace(node.namespace);

      // Add shard info
      if (!shardSet.has(shardId)) {
        shardSet.set(shardId, { namespace, shardId });
      }

      steps.push({
        type: 'lookup',
        shardId,
        entityIds: [entityId],
      });
      break;
    }

    case 'property': {
      // First, collect steps from source
      collectSteps(node.source, steps, shardSet);

      // Get shard from most recent step
      const lastStep = steps[steps.length - 1];
      const shardId = lastStep?.shardId || 'default';

      steps.push({
        type: 'traverse',
        shardId,
        predicate: node.name,
      });
      break;
    }

    case 'reverse': {
      // First, collect steps from source
      collectSteps(node.source, steps, shardSet);

      // Get shard from most recent step
      const lastStep = steps[steps.length - 1];
      const shardId = lastStep?.shardId || 'default';

      steps.push({
        type: 'reverse',
        shardId,
        predicate: node.predicate,
      });
      break;
    }

    case 'filter': {
      // First, collect steps from source
      collectSteps(node.source, steps, shardSet);

      // Get shard from most recent step
      const lastStep = steps[steps.length - 1];
      const shardId = lastStep?.shardId || 'default';

      // Convert filter condition to FilterExpr
      const filterExpr = conditionToFilterExpr(node.condition);

      steps.push({
        type: 'filter',
        shardId,
        filter: filterExpr,
      });
      break;
    }

    case 'expand': {
      // First, collect steps from source
      collectSteps(node.source, steps, shardSet);

      // Get shard from most recent step
      const lastStep = steps[steps.length - 1];
      const shardId = lastStep?.shardId || 'default';

      // Extract field names
      const fields = node.fields.map((f) => f.name);

      steps.push({
        type: 'expand',
        shardId,
        fields,
      });
      break;
    }

    case 'recurse': {
      // First, collect steps from source
      collectSteps(node.source, steps, shardSet);

      // Get shard from most recent step
      const lastStep = steps[steps.length - 1];
      const shardId = lastStep?.shardId || 'default';

      const recurseStep: PlanStep = {
        type: 'recurse',
        shardId,
      };
      if (node.maxDepth !== undefined) {
        recurseStep.maxDepth = node.maxDepth;
      }
      steps.push(recurseStep);
      break;
    }
  }
}

/**
 * Convert a FilterCondition to a FilterExpr
 */
function conditionToFilterExpr(condition: FilterCondition): FilterExpr {
  if (condition.type === 'comparison') {
    return {
      field: condition.field,
      op: condition.operator,
      value: condition.value,
    };
  }

  if (condition.type === 'logical') {
    const leftExpr = conditionToFilterExpr(condition.left);
    const rightExpr = conditionToFilterExpr(condition.right);

    if (condition.operator === 'and') {
      return {
        ...leftExpr,
        and: rightExpr,
      };
    } else {
      return {
        ...leftExpr,
        or: rightExpr,
      };
    }
  }

  if (condition.type === 'depth') {
    // Depth conditions are handled separately in recursion
    return {
      field: 'depth',
      op: condition.operator,
      value: condition.value,
    };
  }

  // Fallback for unknown types
  return {
    field: 'unknown',
    op: '=',
    value: null,
  };
}

/**
 * Calculate cost from execution steps
 */
function calculateCost(steps: PlanStep[]): number {
  let cost = 0;

  for (const step of steps) {
    switch (step.type) {
      case 'lookup':
        cost += LOOKUP_COST * (step.entityIds?.length || 1);
        break;
      case 'traverse':
        cost += TRAVERSE_COST;
        break;
      case 'reverse':
        cost += REVERSE_COST;
        break;
      case 'filter':
        cost += FILTER_COST;
        break;
      case 'expand':
        cost += (step.fields?.length || 1) * EXPAND_COST_PER_FIELD;
        break;
      case 'recurse':
        const depth = step.maxDepth ?? DEFAULT_MAX_DEPTH;
        cost += depth * RECURSE_COST_PER_DEPTH;
        break;
    }
  }

  return cost;
}

/**
 * Simple stringification for cache key generation
 */
function stringifyForCache(node: QueryNode): string {
  switch (node.type) {
    case 'entity':
      return `${node.namespace}:${node.id}`;
    case 'property':
      return `${stringifyForCache(node.source)}.${node.name}`;
    case 'reverse':
      return `${stringifyForCache(node.source)}<-${node.predicate}`;
    case 'filter':
      return `${stringifyForCache(node.source)}[?]`;
    case 'expand':
      return `${stringifyForCache(node.source)}{${node.fields.map((f) => f.name).join(',')}}`;
    case 'recurse':
      return `${stringifyForCache(node.source)}*${node.maxDepth ?? ''}`;
    default:
      return '';
  }
}

// ============================================================================
// Cached Planner
// ============================================================================

import { createPlanCache, type PlanCache } from './plan-cache';
import { stringify, parse } from './parser';

/**
 * Options for creating a cached planner
 */
export interface CachedPlannerOptions {
  /** Maximum number of plans to cache (default: 1000) */
  maxSize?: number;
}

/**
 * A planner that caches query plans for reuse
 */
export interface CachedPlanner {
  /** Plan a query, using cache if available */
  plan(query: string): QueryPlan;
  /** Plan a query from AST, using cache if available */
  planFromAst(ast: QueryNode): QueryPlan;
  /** Invalidate all cached plans (call after schema changes) */
  invalidateCache(): void;
  /** Get the underlying cache for inspection/testing */
  getCache(): PlanCache;
}

/**
 * Create a planner that caches query plans for reuse
 *
 * @param options - Configuration options
 * @returns A CachedPlanner instance
 *
 * @example
 * ```typescript
 * const planner = createCachedPlanner({ maxSize: 500 });
 *
 * // First call plans and caches
 * const plan1 = planner.plan('user:123.friends');
 *
 * // Second call returns cached plan
 * const plan2 = planner.plan('user:123.friends');
 *
 * // After schema change, invalidate cache
 * planner.invalidateCache();
 * ```
 */
export function createCachedPlanner(options: CachedPlannerOptions = {}): CachedPlanner {
  const maxSize = options.maxSize ?? 1000;
  const cache = createPlanCache(maxSize);

  return {
    plan(query: string): QueryPlan {
      // Check cache first
      const cached = cache.get(query);
      if (cached !== undefined) {
        return cached;
      }

      // Parse and plan
      const ast = parse(query);
      const plan = planQuery(ast);

      // Cache and return
      cache.set(query, plan);
      return plan;
    },

    planFromAst(ast: QueryNode): QueryPlan {
      // Generate cache key from AST
      const query = stringify(ast);

      // Check cache first
      const cached = cache.get(query);
      if (cached !== undefined) {
        return cached;
      }

      // Plan
      const plan = planQuery(ast);

      // Cache and return
      cache.set(query, plan);
      return plan;
    },

    invalidateCache(): void {
      cache.invalidate();
    },

    getCache(): PlanCache {
      return cache;
    },
  };
}
