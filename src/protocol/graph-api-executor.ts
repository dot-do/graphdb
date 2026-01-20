/**
 * Graph API Query Executor
 *
 * Wires GraphAPITarget.query() to actual query execution.
 * Handles:
 * - Full URL entity lookups (https://example.com/user/123)
 * - Short-form entity lookups (user:123)
 * - Path traversals (user:123.friends.posts)
 * - Query DSL parsing and execution
 */

import type { QueryResult } from '../broker/orchestrator';
import type { Entity } from '../core/entity';
import { isEntityId } from '../core/types';
import { parse, type QueryNode } from '../query/parser';

// ============================================================================
// Types
// ============================================================================

/**
 * Entity store interface for query execution.
 * Allows the executor to be decoupled from specific storage implementations.
 */
export interface EntityStore {
  /** Get an entity by its full ID */
  get(id: string): Entity | undefined;
  /** Check if an entity exists */
  has(id: string): boolean;
}

/**
 * Traversal function type for path traversals.
 * Returns entities reachable via a predicate from a starting entity.
 */
export type TraverseFunction = (
  startId: string,
  predicate: string,
  options?: { limit?: number }
) => Promise<Entity[]>;

/**
 * Context for query execution.
 */
export interface ExecutorContext {
  /** Entity store for lookups */
  entities: EntityStore;
  /** Optional traverse function for path queries */
  traverse?: TraverseFunction;
}

// ============================================================================
// Query Parsing Helpers
// ============================================================================

/**
 * Check if a query string is a full URL entity lookup.
 *
 * @param queryString - The query to check
 * @returns True if the query is a full URL
 */
export function isFullUrlQuery(queryString: string): boolean {
  return isEntityId(queryString);
}

/**
 * Check if a query string is a simple entity lookup (no traversal).
 *
 * Handles both:
 * - Full URLs: https://example.com/user/123
 * - Short-form: user:123
 *
 * Returns false if query includes path traversal (contains '.' not in URL path).
 */
export function isSimpleLookup(queryString: string): boolean {
  // Full URL without query params or fragment is a simple lookup
  if (isEntityId(queryString)) {
    const trimmed = queryString.trim();
    // Check for path traversal after URL
    // A simple URL lookup won't have trailing dot-separated predicates
    // e.g., "https://example.com/user/123.friends" is a traversal
    // but "https://example.com/user/123" is a simple lookup
    const url = new URL(trimmed);
    const afterUrl = trimmed.slice(url.href.length);
    return !afterUrl || !afterUrl.startsWith('.');
  }

  // Short-form: check for dots after the namespace:id part
  const parts = queryString.split('.');
  return parts.length === 1;
}

/**
 * Parse a query string into its components.
 *
 * Supports:
 * - Full URL: https://example.com/user/123
 * - Full URL with traversal: https://example.com/user/123.friends
 * - Short-form: user:123
 * - Short-form with traversal: user:123.friends.posts
 *
 * @param queryString - The query to parse
 * @returns Object with entityId and optional traversal path
 */
export function parseQueryString(queryString: string): {
  entityId: string;
  path: string[];
} {
  const trimmed = queryString.trim();

  // Handle full URL queries
  if (isEntityId(trimmed)) {
    try {
      const url = new URL(trimmed);
      const baseUrl = url.href;

      // Check for path traversal after URL
      // This is a bit tricky - we need to handle cases like:
      // https://example.com/user/123.friends
      // where ".friends" is a traversal, not part of the URL path
      const afterBase = trimmed.slice(baseUrl.length);

      if (afterBase.startsWith('.')) {
        // There's a traversal path after the URL
        const path = afterBase.slice(1).split('.').filter(Boolean);
        return { entityId: baseUrl, path };
      }

      // Simple URL lookup
      return { entityId: baseUrl, path: [] };
    } catch {
      // Not a valid URL, fall through to short-form parsing
    }
  }

  // Handle short-form queries (user:123.friends.posts)
  const parts = trimmed.split('.');
  const entityId = parts[0] || '';
  const path = parts.slice(1).filter(Boolean);

  return { entityId, path };
}

// ============================================================================
// Query Execution
// ============================================================================

/**
 * Execute a query against the entity store.
 *
 * Handles:
 * - Simple entity lookups (by full URL or short-form ID)
 * - Path traversals using the provided traverse function
 *
 * @param queryString - The query to execute
 * @param ctx - Execution context with entity store and traverse function
 * @returns QueryResult with matched entities and stats
 */
export async function executeQuery(
  queryString: string,
  ctx: ExecutorContext
): Promise<QueryResult> {
  const startTime = performance.now();
  const { entityId, path } = parseQueryString(queryString);

  // Handle empty entity ID
  if (!entityId) {
    return createEmptyResult(startTime);
  }

  // Simple entity lookup (no traversal)
  if (path.length === 0) {
    const entity = ctx.entities.get(entityId);
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

  // Path traversal requires traverse function
  if (!ctx.traverse) {
    // Fall back to simple lookup of the base entity if no traverse function
    const entity = ctx.entities.get(entityId);
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

  // Execute path traversal step by step
  let currentIds = [entityId];
  let entitiesScanned = 0;
  let shardQueries = 0;

  for (const predicate of path) {
    const nextEntities: Entity[] = [];

    for (const currentId of currentIds) {
      const results = await ctx.traverse(currentId, predicate, { limit: 100 });
      shardQueries++;
      entitiesScanned += results.length;
      nextEntities.push(...results);
    }

    currentIds = nextEntities.map((e) => String(e.$id));
  }

  // Get final entities
  const finalEntities: Entity[] = [];
  for (const id of currentIds) {
    const entity = ctx.entities.get(id);
    if (entity) {
      finalEntities.push(entity);
    }
  }

  return {
    entities: finalEntities,
    hasMore: false,
    stats: {
      shardQueries,
      entitiesScanned,
      durationMs: performance.now() - startTime,
    },
  };
}

/**
 * Execute a query using the parsed AST.
 *
 * This function uses the query parser to handle complex query syntax
 * like filtering, expansion, and reverse traversals.
 *
 * @param queryString - The query string to parse and execute
 * @param ctx - Execution context
 * @returns QueryResult with matched entities and stats
 */
export async function executeQueryWithParser(
  queryString: string,
  ctx: ExecutorContext
): Promise<QueryResult> {
  const startTime = performance.now();

  try {
    // Try to parse as DSL query
    const ast = parse(queryString);
    return await executeAst(ast, ctx, startTime);
  } catch {
    // If DSL parsing fails, try as simple entity lookup
    return executeQuery(queryString, ctx);
  }
}

/**
 * Execute a parsed query AST.
 */
async function executeAst(
  ast: QueryNode,
  ctx: ExecutorContext,
  startTime: number
): Promise<QueryResult> {
  switch (ast.type) {
    case 'entity': {
      // Simple entity lookup by namespace:id
      const entityId = `${ast.namespace}:${ast.id}`;
      const entity = ctx.entities.get(entityId);
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

    case 'property': {
      // Property access (traversal)
      if (!ctx.traverse) {
        return createEmptyResult(startTime);
      }

      // Get the source entities first
      const sourceResult = await executeAst(ast.source, ctx, startTime);
      if (sourceResult.entities.length === 0) {
        return sourceResult;
      }

      // Traverse from each source entity
      const allResults: Entity[] = [];
      let shardQueries = sourceResult.stats.shardQueries;
      let entitiesScanned = sourceResult.stats.entitiesScanned;

      for (const sourceEntity of sourceResult.entities) {
        const traverseResults = await ctx.traverse(
          String(sourceEntity.$id),
          ast.name,
          { limit: 100 }
        );
        shardQueries++;
        entitiesScanned += traverseResults.length;
        allResults.push(...traverseResults);
      }

      return {
        entities: allResults,
        hasMore: false,
        stats: {
          shardQueries,
          entitiesScanned,
          durationMs: performance.now() - startTime,
        },
      };
    }

    case 'filter':
    case 'expand':
    case 'reverse':
    case 'recurse': {
      // For now, fall back to simple execution for complex query types
      // These can be implemented as needed
      const sourceResult = await executeAst(ast.source, ctx, startTime);
      return sourceResult;
    }

    default:
      return createEmptyResult(startTime);
  }
}

/**
 * Create an empty query result.
 */
function createEmptyResult(startTime: number): QueryResult {
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
