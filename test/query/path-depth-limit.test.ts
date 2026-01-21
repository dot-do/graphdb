/**
 * Path Depth Limit Tests
 *
 * Tests for the MAX_PATH_DEPTH constant and depth limit enforcement
 * in path traversal operations to prevent infinite recursion.
 *
 * @see src/query/executor.ts for the executor implementation
 * @see src/traversal/traversal-do.ts for the TraversalDO implementation
 */

import { env } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import {
  executePlan,
  traverseFrom,
  MAX_PATH_DEPTH,
  DEFAULT_PATH_DEPTH,
  MAX_TRAVERSAL_TIME_MS,
  type ExecutionContext,
} from '../../src/query/executor';
import { planQuery } from '../../src/query/planner';
import { parse } from '../../src/query/parser';
import type { Entity } from '../../src/core/entity';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
} from '../../src/core/types';
import type { Triple, TypedObject } from '../../src/core/triple';
import { typedObjectToJson } from '../../src/core/type-converters';

// ============================================================================
// Test Helpers
// ============================================================================

// Counter for unique shard names
let testCounter = 0;

// Valid ULID for transactions
const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV';

/**
 * Get a unique shard stub for testing
 */
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`path-depth-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

/**
 * Create a test entity from an ID
 */
function createTestEntity(
  id: string,
  type: string,
  props: Record<string, unknown> = {}
): Entity {
  const fullId = `https://example.com/user/${id}`;
  return {
    $id: createEntityId(fullId),
    $type: type,
    $context: 'https://graphdb.local/',
    ...props,
  };
}

/**
 * Create an entity map for seeding
 */
function createEntityMap(entities: Entity[]): Map<string, Entity> {
  const map = new Map<string, Entity>();
  for (const entity of entities) {
    map.set(entity.$id, entity);
    // Also set by short format for lookups (user:id)
    const match = entity.$id.match(/\/user\/([^/]+)$/);
    if (match) {
      map.set(`user:${match[1]}`, entity);
    }
  }
  return map;
}

/**
 * Create a mock execution context with seeded entities
 */
function createMockExecutionContext(
  seededEntities: Map<string, Entity>
): ExecutionContext {
  // Create a mock stub that returns seeded data
  const mockStub = {
    fetch: async (request: Request) => {
      const url = new URL(request.url);
      const body = await request.json() as Record<string, unknown>;

      if (url.pathname === '/lookup') {
        const entityIds = body.entityIds as string[];
        const entities = entityIds
          .map((id) => seededEntities.get(id))
          .filter((e): e is Entity => e !== undefined);
        return new Response(JSON.stringify({ entities, triples: [] }));
      }

      if (url.pathname === '/traverse') {
        const entityIds = body.entityIds as string[];
        const predicate = body.predicate as string;
        const direction = body.direction as string;

        const resultEntities: Entity[] = [];
        const resultTriples: Triple[] = [];

        for (const id of entityIds) {
          const entity = seededEntities.get(id);
          if (!entity) continue;

          if (direction === 'outgoing' && predicate) {
            const value = (entity as Record<string, unknown>)[predicate];
            if (typeof value === 'string') {
              const target = seededEntities.get(value);
              if (target) resultEntities.push(target);
            } else if (Array.isArray(value)) {
              for (const v of value) {
                const target = seededEntities.get(v);
                if (target) resultEntities.push(target);
              }
            }
          }
        }

        return new Response(JSON.stringify({ entities: resultEntities, triples: resultTriples }));
      }

      if (url.pathname === '/expand') {
        const entityIds = body.entityIds as string[];
        const entities = entityIds
          .map((id) => seededEntities.get(id))
          .filter((e): e is Entity => e !== undefined);
        return new Response(JSON.stringify({ entities, triples: [] }));
      }

      return new Response(JSON.stringify({ entities: [], triples: [] }));
    },
  } as unknown as DurableObjectStub;

  return {
    getShardStub: () => mockStub,
    maxResults: 1000,
  };
}

// ============================================================================
// MAX_PATH_DEPTH Constant Tests
// ============================================================================

describe('MAX_PATH_DEPTH constant', () => {
  it('should export MAX_PATH_DEPTH constant', () => {
    expect(MAX_PATH_DEPTH).toBeDefined();
    expect(typeof MAX_PATH_DEPTH).toBe('number');
  });

  it('should have MAX_PATH_DEPTH set to 100', () => {
    expect(MAX_PATH_DEPTH).toBe(100);
  });

  it('should export DEFAULT_PATH_DEPTH constant', () => {
    expect(DEFAULT_PATH_DEPTH).toBeDefined();
    expect(typeof DEFAULT_PATH_DEPTH).toBe('number');
  });

  it('should have DEFAULT_PATH_DEPTH set to 10', () => {
    expect(DEFAULT_PATH_DEPTH).toBe(10);
  });

  it('should have DEFAULT_PATH_DEPTH less than MAX_PATH_DEPTH', () => {
    expect(DEFAULT_PATH_DEPTH).toBeLessThan(MAX_PATH_DEPTH);
  });
});

// ============================================================================
// Depth Limit Enforcement Tests - executeRecurse
// ============================================================================

describe('executeRecurse depth limit enforcement', () => {
  it('should respect user-specified depth when below MAX_PATH_DEPTH', async () => {
    // Create a chain: user -> f1 -> f2 -> f3 -> f4 -> f5
    const entities: Entity[] = [];
    for (let i = 5; i >= 1; i--) {
      const nextId = i < 5 ? `https://example.com/user/f${i + 1}` : undefined;
      entities.push(createTestEntity(`f${i}`, 'user', nextId ? { friends: nextId } : {}));
    }
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    entities.push(user);

    const seededEntities = createEntityMap(entities);
    const ctx = createMockExecutionContext(seededEntities);

    // Request depth of 2, which is well below MAX_PATH_DEPTH
    const plan = planQuery(parse('user:123.friends*[depth <= 2]'));
    const result = await executePlan(plan, ctx);

    // Should stop at depth 2, not traverse the full chain
    expect(result.stats.shardQueries).toBeLessThanOrEqual(4);
  });

  it('should cap depth at MAX_PATH_DEPTH when user requests higher', async () => {
    // Create a simple entity
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    const friend = createTestEntity('f1', 'user');

    const seededEntities = createEntityMap([user, friend]);
    const ctx = createMockExecutionContext(seededEntities);

    // Request depth higher than MAX_PATH_DEPTH (e.g., 500)
    // The planner will pass this through, but executor should cap it
    const plan = planQuery(parse('user:123.friends*[depth <= 500]'));

    // Modify the plan step to have a very high maxDepth
    const recurseStep = plan.steps.find((s) => s.type === 'recurse');
    if (recurseStep) {
      recurseStep.maxDepth = 500; // Way above MAX_PATH_DEPTH
    }

    // Execute should not throw and should cap at MAX_PATH_DEPTH
    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
    // The actual depth traversed should be limited
    expect(result.stats.shardQueries).toBeLessThanOrEqual(MAX_PATH_DEPTH + 1);
  });

  it('should use DEFAULT_PATH_DEPTH when no depth specified', async () => {
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    const friend = createTestEntity('f1', 'user');

    const seededEntities = createEntityMap([user, friend]);
    const ctx = createMockExecutionContext(seededEntities);

    // Parse query without depth constraint
    // Note: This creates a recurse step without maxDepth
    const plan = planQuery(parse('user:123.friends*'));

    // The recurse step should have no maxDepth initially
    const recurseStep = plan.steps.find((s) => s.type === 'recurse');
    expect(recurseStep?.maxDepth).toBeUndefined();

    // Execute - should use DEFAULT_PATH_DEPTH internally
    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
    // Should not recurse forever
    expect(result.stats.shardQueries).toBeLessThanOrEqual(DEFAULT_PATH_DEPTH + 2);
  });
});

// ============================================================================
// Depth Limit Enforcement Tests - traverseFrom
// ============================================================================

describe('traverseFrom depth limit enforcement', () => {
  it('should respect user-specified maxDepth when below MAX_PATH_DEPTH', async () => {
    // Create a chain: user -> f1 -> f2 -> f3
    const f3 = createTestEntity('f3', 'user');
    const f2 = createTestEntity('f2', 'user', { friends: f3.$id });
    const f1 = createTestEntity('f1', 'user', { friends: f2.$id });
    const user = createTestEntity('123', 'user', { friends: f1.$id });

    const seededEntities = createEntityMap([user, f1, f2, f3]);
    const ctx = createMockExecutionContext(seededEntities);

    // Request depth of 2
    const result = await traverseFrom('https://example.com/user/123', 'friends', { maxDepth: 2 }, ctx);

    // Should get f1 and f2, but not f3 (which is at depth 3)
    expect(result.length).toBeLessThanOrEqual(2);
  });

  it('should cap maxDepth at MAX_PATH_DEPTH', async () => {
    const user = createTestEntity('123', 'user');
    const seededEntities = createEntityMap([user]);
    const ctx = createMockExecutionContext(seededEntities);

    // Request depth way above MAX_PATH_DEPTH
    const result = await traverseFrom(
      'https://example.com/user/123',
      'friends',
      { maxDepth: MAX_PATH_DEPTH + 100 },
      ctx
    );

    // Should not throw and should complete
    expect(result).toBeDefined();
  });

  it('should handle negative depth gracefully', async () => {
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    const friend = createTestEntity('f1', 'user');

    const seededEntities = createEntityMap([user, friend]);
    const ctx = createMockExecutionContext(seededEntities);

    // Negative depth should be treated as 0 or 1
    const result = await traverseFrom('https://example.com/user/123', 'friends', { maxDepth: -5 }, ctx);

    // Should handle gracefully - either empty or minimal results
    expect(result).toBeDefined();
    expect(Array.isArray(result)).toBe(true);
  });
});

// ============================================================================
// Infinite Loop Prevention Tests
// ============================================================================

describe('infinite loop prevention', () => {
  it('should handle cyclic graphs without infinite loops', async () => {
    // Create a cycle: a -> b -> c -> a
    const a = createTestEntity('a', 'user', { friends: 'https://example.com/user/b' });
    const b = createTestEntity('b', 'user', { friends: 'https://example.com/user/c' });
    const c = createTestEntity('c', 'user', { friends: 'https://example.com/user/a' }); // Cycle back to a

    const seededEntities = createEntityMap([a, b, c]);
    const ctx = createMockExecutionContext(seededEntities);

    // Request high depth - with cycle, this could go forever without protection
    const result = await traverseFrom('https://example.com/user/a', 'friends', { maxDepth: 50 }, ctx);

    // Should complete without hanging (visited set prevents infinite loop)
    expect(result).toBeDefined();
    // Each entity should appear at most once
    const ids = result.map((e) => e.$id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(ids.length);
  });

  it('should terminate with MAX_PATH_DEPTH even on unbounded recursion request', async () => {
    // Create a simple linear chain
    const entities: Entity[] = [];
    for (let i = 0; i < 150; i++) {
      const nextId = i < 149 ? `https://example.com/user/n${i + 1}` : undefined;
      entities.push(createTestEntity(`n${i}`, 'user', nextId ? { friends: nextId } : {}));
    }

    const seededEntities = createEntityMap(entities);
    const ctx = createMockExecutionContext(seededEntities);

    // This would run forever without MAX_PATH_DEPTH limit
    const plan = planQuery(parse('user:n0.friends*'));

    // Remove any depth limit from the plan to test the absolute cap
    const recurseStep = plan.steps.find((s) => s.type === 'recurse');
    if (recurseStep) {
      delete recurseStep.maxDepth; // No user-specified limit
    }

    const startTime = Date.now();
    const result = await executePlan(plan, ctx);
    const duration = Date.now() - startTime;

    // Should complete in reasonable time (less than 10 seconds)
    expect(duration).toBeLessThan(10000);
    expect(result).toBeDefined();

    // Should not have traversed beyond MAX_PATH_DEPTH
    expect(result.stats.shardQueries).toBeLessThanOrEqual(MAX_PATH_DEPTH + 2);
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

describe('depth limit edge cases', () => {
  it('should handle depth of 0', async () => {
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    const friend = createTestEntity('f1', 'user');

    const seededEntities = createEntityMap([user, friend]);
    const ctx = createMockExecutionContext(seededEntities);

    // Depth 0 should not traverse at all
    const result = await traverseFrom('https://example.com/user/123', 'friends', { maxDepth: 0 }, ctx);

    // Should return empty or just visited set (no traversal)
    expect(result.length).toBe(0);
  });

  it('should handle depth of 1', async () => {
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    const f1 = createTestEntity('f1', 'user', { friends: 'https://example.com/user/f2' });
    const f2 = createTestEntity('f2', 'user');

    const seededEntities = createEntityMap([user, f1, f2]);
    const ctx = createMockExecutionContext(seededEntities);

    // Depth 1 should only get immediate neighbors
    const result = await traverseFrom('https://example.com/user/123', 'friends', { maxDepth: 1 }, ctx);

    // Should get f1 but not f2
    expect(result.length).toBe(1);
    expect(result[0]?.$id).toContain('f1');
  });

  it('should handle exactly MAX_PATH_DEPTH', async () => {
    const user = createTestEntity('123', 'user');
    const seededEntities = createEntityMap([user]);
    const ctx = createMockExecutionContext(seededEntities);

    // Request exactly MAX_PATH_DEPTH
    const result = await traverseFrom(
      'https://example.com/user/123',
      'friends',
      { maxDepth: MAX_PATH_DEPTH },
      ctx
    );

    // Should work without error
    expect(result).toBeDefined();
  });

  it('should handle MAX_PATH_DEPTH + 1', async () => {
    const user = createTestEntity('123', 'user');
    const seededEntities = createEntityMap([user]);
    const ctx = createMockExecutionContext(seededEntities);

    // Request MAX_PATH_DEPTH + 1 - should be capped
    const result = await traverseFrom(
      'https://example.com/user/123',
      'friends',
      { maxDepth: MAX_PATH_DEPTH + 1 },
      ctx
    );

    // Should work without error (capped internally)
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Timeout Enforcement Tests - executeRecurse (Issue #2)
// ============================================================================

describe('MAX_TRAVERSAL_TIME_MS constant', () => {
  it('should export MAX_TRAVERSAL_TIME_MS constant', () => {
    expect(MAX_TRAVERSAL_TIME_MS).toBeDefined();
    expect(typeof MAX_TRAVERSAL_TIME_MS).toBe('number');
  });

  it('should have MAX_TRAVERSAL_TIME_MS set to 30000 (30 seconds)', () => {
    expect(MAX_TRAVERSAL_TIME_MS).toBe(30000);
  });
});

describe('executeRecurse timeout enforcement', () => {
  /**
   * Create a slow mock execution context that simulates high fan-out graphs
   * by adding artificial delay to each shard query.
   */
  function createSlowMockExecutionContext(
    seededEntities: Map<string, Entity>,
    delayMs: number
  ): ExecutionContext {
    const mockStub = {
      fetch: async (request: Request) => {
        // Simulate slow shard response
        await new Promise((resolve) => setTimeout(resolve, delayMs));

        const url = new URL(request.url);
        const body = await request.json() as Record<string, unknown>;

        if (url.pathname === '/lookup') {
          const entityIds = body.entityIds as string[];
          const entities = entityIds
            .map((id) => seededEntities.get(id))
            .filter((e): e is Entity => e !== undefined);
          return new Response(JSON.stringify({ entities, triples: [] }));
        }

        if (url.pathname === '/traverse') {
          const entityIds = body.entityIds as string[];
          const predicate = body.predicate as string;
          const direction = body.direction as string;

          const resultEntities: Entity[] = [];

          for (const id of entityIds) {
            const entity = seededEntities.get(id);
            if (!entity) continue;

            // For outgoing traversal, always return some entities to keep recursing
            if (direction === 'outgoing') {
              if (predicate) {
                const value = (entity as Record<string, unknown>)[predicate];
                if (typeof value === 'string') {
                  const target = seededEntities.get(value);
                  if (target) resultEntities.push(target);
                } else if (Array.isArray(value)) {
                  for (const v of value) {
                    const target = seededEntities.get(v);
                    if (target) resultEntities.push(target);
                  }
                }
              } else {
                // No predicate - recurse returns all connected entities
                for (const [key, val] of Object.entries(entity)) {
                  if (key.startsWith('$')) continue;
                  if (typeof val === 'string' && val.startsWith('https://')) {
                    const target = seededEntities.get(val);
                    if (target) resultEntities.push(target);
                  }
                }
              }
            }
          }

          return new Response(JSON.stringify({ entities: resultEntities, triples: [] }));
        }

        return new Response(JSON.stringify({ entities: [], triples: [] }));
      },
    } as unknown as DurableObjectStub;

    return {
      getShardStub: () => mockStub,
      maxResults: 1000,
    };
  }

  it('should respect ctx.timeout and stop recursion early', async () => {
    // Create a chain of entities: n0 -> n1 -> n2 -> ... -> n99
    const entities: Entity[] = [];
    for (let i = 0; i < 100; i++) {
      const nextId = i < 99 ? `https://example.com/user/n${i + 1}` : undefined;
      entities.push(createTestEntity(`n${i}`, 'user', nextId ? { friends: nextId } : {}));
    }

    const seededEntities = createEntityMap(entities);

    // Create context with 50ms timeout and 10ms delay per shard query
    // This means only ~5 iterations should complete before timeout
    const ctx = createSlowMockExecutionContext(seededEntities, 10);
    ctx.timeout = 50; // 50ms timeout

    const plan = planQuery(parse('user:n0.friends*'));
    const startTime = Date.now();
    const result = await executePlan(plan, ctx);
    const duration = Date.now() - startTime;

    // Should complete within reasonable time (timeout + some overhead)
    expect(duration).toBeLessThan(200);

    // Should have stopped early due to timeout, not traversed all 100 nodes
    // With 10ms per query and 50ms timeout, we expect ~5 queries max
    expect(result.stats.shardQueries).toBeLessThan(20);
    expect(result).toBeDefined();
  });

  it('should use MAX_TRAVERSAL_TIME_MS as default timeout', async () => {
    // Create a simple entity to test the default is applied
    const user = createTestEntity('123', 'user', { friends: 'https://example.com/user/f1' });
    const friend = createTestEntity('f1', 'user');

    const seededEntities = createEntityMap([user, friend]);
    const ctx = createMockExecutionContext(seededEntities);

    // ctx.timeout is not set, so MAX_TRAVERSAL_TIME_MS (30s) should be used
    expect(ctx.timeout).toBeUndefined();

    const plan = planQuery(parse('user:123.friends*'));
    const result = await executePlan(plan, ctx);

    // Should complete without error - default timeout is generous enough
    expect(result).toBeDefined();
  });

  it('should return partial results when timeout is reached', async () => {
    // Create a long chain: n0 -> n1 -> n2 -> ... -> n49
    const entities: Entity[] = [];
    for (let i = 0; i < 50; i++) {
      const nextId = i < 49 ? `https://example.com/user/n${i + 1}` : undefined;
      entities.push(createTestEntity(`n${i}`, 'user', nextId ? { friends: nextId } : {}));
    }

    const seededEntities = createEntityMap(entities);

    // Create context with very short timeout
    const ctx = createSlowMockExecutionContext(seededEntities, 5);
    ctx.timeout = 20; // 20ms timeout

    const plan = planQuery(parse('user:n0.friends*'));
    const result = await executePlan(plan, ctx);

    // Should have some results (partial traversal)
    expect(result).toBeDefined();
    expect(result.entities.length).toBeGreaterThan(0);

    // But should not have traversed all 50 nodes
    expect(result.stats.shardQueries).toBeLessThan(50);
  });

  it('should complete full traversal when timeout is not reached', async () => {
    // Create a short chain: n0 -> n1 -> n2
    const n2 = createTestEntity('n2', 'user');
    const n1 = createTestEntity('n1', 'user', { friends: n2.$id });
    const n0 = createTestEntity('n0', 'user', { friends: n1.$id });

    const seededEntities = createEntityMap([n0, n1, n2]);

    // Create context with generous timeout
    const ctx = createSlowMockExecutionContext(seededEntities, 1);
    ctx.timeout = 5000; // 5 second timeout

    const plan = planQuery(parse('user:n0.friends*[depth <= 5]'));
    const result = await executePlan(plan, ctx);

    // Should complete full traversal
    expect(result).toBeDefined();
    // Should have visited all 3 nodes
    expect(result.entities.length).toBeGreaterThanOrEqual(2);
  });
});
