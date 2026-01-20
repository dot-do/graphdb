/**
 * GraphDB Query Module
 *
 * Exports query parsing and planning functionality for the GraphDB path query language.
 *
 * @example
 * ```typescript
 * import { parse, planQuery, executePlan } from '@dotdo/graphdb/query';
 *
 * // Parse a query string into an AST
 * const ast = parse('user:123.friends[?age > 30]');
 *
 * // Create an execution plan
 * const plan = planQuery(ast);
 *
 * // Execute the plan
 * const result = await executePlan(plan, context);
 * ```
 *
 * @packageDocumentation
 */

// Parser exports
export {
  /**
   * Parse a graph query string into an AST.
   * @param query - The query string to parse
   * @returns Parsed AST node
   * @throws {ParseError} If the query syntax is invalid
   * @example
   * ```typescript
   * const ast = parse('user:123.friends');
   * // Returns: { type: 'property', name: 'friends', source: { type: 'entity', namespace: 'user', id: '123' } }
   * ```
   */
  parse,
  /**
   * Convert an AST node back to query string format.
   * @param node - The AST node to stringify
   * @returns Query string representation
   */
  stringify,
  /**
   * Parse a filter condition expression (internal use).
   * @param lexer - Lexer instance positioned at filter start
   * @returns Parsed filter condition
   */
  parseFilterCondition,
  /**
   * Parse a JSON-style expansion block (internal use).
   * @param lexer - Lexer instance
   * @param source - Source node to expand
   * @returns Expansion AST node
   */
  parseExpansion,
  /**
   * Parse expansion field list (internal use).
   * @param lexer - Lexer instance
   * @returns Array of expansion fields
   */
  parseExpansionFields,

  // AST builders - programmatic query construction
  /**
   * Create an entity lookup node.
   * @param namespace - Entity namespace (e.g., 'user')
   * @param id - Entity ID
   * @returns EntityLookup AST node
   */
  entity,
  /**
   * Create a property access node.
   * @param name - Property/edge name to traverse
   * @param source - Source node to traverse from
   * @returns PropertyAccess AST node
   */
  property,
  /**
   * Create a reverse traversal node.
   * @param predicate - Predicate to traverse in reverse
   * @param source - Source node
   * @returns ReverseTraversal AST node
   */
  reverse,
  /**
   * Create a filter node.
   * @param condition - Filter condition
   * @param source - Source node to filter
   * @returns Filter AST node
   */
  filter,
  /**
   * Create an expansion node.
   * @param fields - Fields to expand
   * @param source - Source node to expand
   * @returns Expansion AST node
   */
  expand,
  /**
   * Create a recursion node.
   * @param source - Source node to recurse from
   * @param maxDepth - Optional maximum depth
   * @returns Recursion AST node
   */
  recurse,
  /**
   * Create a comparison condition.
   * @param field - Field name to compare
   * @param operator - Comparison operator
   * @param value - Value to compare against
   * @returns ComparisonCondition
   */
  comparison,
  /**
   * Create a logical condition (AND/OR).
   * @param operator - Logical operator ('and' or 'or')
   * @param left - Left operand
   * @param right - Right operand
   * @returns LogicalCondition
   */
  logical,
  /**
   * Create a depth condition for recursion bounds.
   * @param operator - Comparison operator
   * @param value - Maximum depth value
   * @returns DepthCondition
   */
  depth,

  /**
   * Count the number of traversal hops in a query.
   * @param node - Query AST node
   * @returns Number of hops (Infinity for unbounded recursion)
   */
  countHops,

  /**
   * Error thrown when query parsing fails.
   * Contains position information for error reporting.
   */
  ParseError,

  // Types
  type QueryNode,
  type EntityLookup,
  type PropertyAccess,
  type ReverseTraversal,
  type Filter,
  type Expansion,
  type Recursion,
  type FilterCondition,
  type ComparisonCondition,
  type LogicalCondition,
  type DepthCondition,
  type ComparisonOperator,
  type LogicalOperator,
  type FilterValue,
  type ExpansionField,
} from './parser';

// Planner exports
export {
  /**
   * Create an execution plan from a parsed query AST.
   * @param ast - Parsed query AST node
   * @returns Query execution plan with steps, shards, and cost estimate
   * @example
   * ```typescript
   * const ast = parse('user:123.friends');
   * const plan = planQuery(ast);
   * console.log(plan.estimatedCost); // Cost estimate for rate limiting
   * ```
   */
  planQuery,
  /**
   * Optimize a query plan for better performance.
   * Combines adjacent lookups and pushes filters down.
   * @param plan - Query plan to optimize
   * @returns Optimized query plan
   */
  optimizePlan,
  /**
   * Get the estimated cost of a query plan.
   * @param plan - Query plan
   * @returns Cost estimate (used for rate limiting)
   */
  estimateCost,
  /**
   * Create a planner that caches query plans for reuse.
   * Improves performance by avoiding re-planning identical queries.
   * @param options - Cache configuration options
   * @returns CachedPlanner instance
   * @example
   * ```typescript
   * const planner = createCachedPlanner({ maxSize: 500 });
   * const plan1 = planner.plan('user:123.friends'); // Plans and caches
   * const plan2 = planner.plan('user:123.friends'); // Returns cached plan
   * ```
   */
  createCachedPlanner,

  // Types
  type QueryPlan,
  type PlanStep,
  type FilterExpr,
  type CachedPlanner,
  type CachedPlannerOptions,
} from './planner';

// Executor exports
export {
  /**
   * Execute a query plan against the graph database.
   * Coordinates shard queries and handles pagination.
   * @param plan - Query plan to execute
   * @param ctx - Execution context with shard access
   * @param options - Optional pagination options
   * @returns Execution result with entities and statistics
   * @throws {Error} If shard queries fail
   * @example
   * ```typescript
   * const result = await executePlan(plan, {
   *   getShardStub: (id) => env.SHARD.get(env.SHARD.idFromName(id)),
   *   maxResults: 100,
   *   timeout: 5000,
   * });
   * console.log(result.entities);
   * ```
   */
  executePlan,
  /**
   * Execute a single plan step.
   * @param step - Step to execute
   * @param ctx - Execution context
   * @returns Triples found in this step
   */
  executeStep,
  /**
   * Traverse from an entity following a predicate (BFS).
   * @param startId - Starting entity ID
   * @param predicate - Predicate to follow
   * @param options - Traversal options (maxDepth, maxResults)
   * @param ctx - Execution context
   * @returns Entities reachable via the predicate
   */
  traverseFrom,
  /**
   * Find entities pointing to a target via reverse traversal.
   * @param targetId - Target entity ID
   * @param predicate - Predicate to traverse in reverse
   * @param ctx - Execution context
   * @returns Entities that point to the target
   */
  traverseTo,

  // Types
  type ExecutionContext,
  type ExecutionResult,
  type ExecutionStats,
  type Direction,
  type PaginationOptions,
} from './executor';

// Materializer exports
export {
  /**
   * Materialize triples into entity objects.
   * Groups triples by subject and constructs entity objects.
   * @param triples - Array of triples to materialize
   * @param options - Materialization options
   * @returns Array of materialized entities
   */
  materializeTriples,
  /**
   * Group triples by subject into a map.
   * @param triples - Triples to group
   * @returns Map of subject ID to triples array
   */
  groupBySubject,
  /**
   * Expand REF-type triples by resolving referenced entities.
   * @param entities - Entities with REF fields
   * @param resolver - Function to resolve entity IDs
   * @returns Entities with expanded references
   */
  expandRefs,
  /**
   * Project specific fields from entities.
   * @param entities - Entities to project
   * @param fields - Field names to include
   * @returns Entities with only specified fields
   */
  projectFields,
  /**
   * Format execution result for API response.
   * @param result - Raw execution result
   * @returns Formatted result suitable for JSON serialization
   */
  formatResult,

  // Types
  type MaterializeOptions,
  type FormattedResult,
  type EntityResolver,
} from './materializer';

// Plan cache exports
export {
  /**
   * Create an LRU cache for query plans.
   * @param maxSize - Maximum number of plans to cache
   * @returns PlanCache instance
   */
  createPlanCache,

  // Types
  type PlanCache,
} from './plan-cache';
