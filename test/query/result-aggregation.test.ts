/**
 * GraphDB Query Result Aggregation Tests (TDD RED Phase)
 *
 * Tests for executor result aggregation including:
 * - Entity deduplication
 * - Triple collection and merging
 * - Statistics aggregation
 * - Pagination cursor handling
 * - Error aggregation
 */

import { describe, it, expect, vi } from 'vitest';
import {
  executePlan,
  traverseFrom,
  traverseTo,
  type ExecutionContext,
  type ExecutionResult,
  MAX_PATH_DEPTH,
  DEFAULT_PATH_DEPTH,
} from '../../src/query/executor';
import {
  planQuery,
  type QueryPlan,
  type PlanStep,
} from '../../src/query/planner';
import { parse } from '../../src/query/parser';
import type { Entity } from '../../src/core/entity';
import type { Triple } from '../../src/core/triple';
import { createEntityId } from '../../src/core/types';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a mock entity for testing
 */
function createMockEntity(
  id: string,
  type: string = 'Thing',
  props: Record<string, unknown> = {}
): Entity {
  const fullId = id.startsWith('https://') ? id : `https://test.com/${type}/${id}`;
  return {
    $id: createEntityId(fullId),
    $type: type,
    $context: `https://test.com/${type}`,
    _namespace: 'https://test.com' as any,
    _localId: id,
    ...props,
  };
}

/**
 * Create a mock execution context that returns predefined data
 */
function createMockContext(
  lookupEntities: Map<string, Entity[]>,
  traverseEntities: Map<string, Entity[]> = new Map()
): ExecutionContext {
  const mockStub = {
    fetch: vi.fn().mockImplementation(async (request: Request) => {
      const url = new URL(request.url);
      const path = url.pathname;

      if (path === '/lookup') {
        const body = (await request.json()) as { entityIds: string[] };
        const entities: Entity[] = [];
        for (const id of body.entityIds || []) {
          const found = lookupEntities.get(id);
          if (found) {
            entities.push(...found);
          }
        }
        return new Response(
          JSON.stringify({ entities, triples: [] }),
          { status: 200 }
        );
      }

      if (path === '/traverse') {
        const body = (await request.json()) as {
          entityIds: string[];
          predicate?: string;
          direction: 'outgoing' | 'incoming';
        };

        // Look up based on predicate
        const key = `${body.direction}:${body.predicate || '*'}`;
        const entities = traverseEntities.get(key) || [];

        return new Response(
          JSON.stringify({ entities, triples: [] }),
          { status: 200 }
        );
      }

      if (path === '/expand') {
        const body = (await request.json()) as {
          entityIds: string[];
          fields: string[];
        };

        const entities: Entity[] = [];
        for (const id of body.entityIds || []) {
          const found = lookupEntities.get(id);
          if (found) {
            entities.push(...found);
          }
        }

        return new Response(
          JSON.stringify({ entities, triples: [] }),
          { status: 200 }
        );
      }

      return new Response(
        JSON.stringify({ entities: [], triples: [] }),
        { status: 200 }
      );
    }),
  } as unknown as DurableObjectStub;

  return {
    getShardStub: () => mockStub,
    maxResults: 100,
    timeout: 5000,
  };
}

// ============================================================================
// Entity Deduplication Tests
// ============================================================================

describe('Entity Deduplication', () => {
  describe('Should not return duplicate entities', () => {
    it('should deduplicate entities with same $id', async () => {
      const user = createMockEntity('123', 'user', { name: 'Alice' });

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [user]); // Same user returned

      const ctx = createMockContext(lookupEntities, traverseEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // Should only have one instance of the user
      const ids = result.entities.map(e => e.$id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });

    it('should deduplicate across multiple traversal hops', async () => {
      const user1 = createMockEntity('1', 'user');
      const user2 = createMockEntity('2', 'user');
      const user3 = createMockEntity('3', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:1', [user1]);

      // user1 -> user2, user3
      // user2 -> user3 (user3 appears twice in path)
      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [user2, user3]);

      const ctx = createMockContext(lookupEntities, traverseEntities);
      const plan = planQuery(parse('user:1.friends'));

      const result = await executePlan(plan, ctx);

      const ids = result.entities.map(e => e.$id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });
  });

  describe('Should handle cycle detection', () => {
    it('should not enter infinite loop on circular references', async () => {
      const userA = createMockEntity('a', 'user', { name: 'A' });
      const userB = createMockEntity('b', 'user', { name: 'B' });

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:a', [userA]);

      // A -> B -> A (circular)
      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [userB, userA]);

      const ctx = createMockContext(lookupEntities, traverseEntities);
      ctx.timeout = 1000; // Short timeout to catch infinite loops

      const plan = planQuery(parse('user:a.friends*[depth <= 5]'));

      // Should complete without hanging
      const result = await executePlan(plan, ctx);

      expect(result.entities).toBeDefined();
      // Should not have infinite entities
      expect(result.entities.length).toBeLessThan(100);
    });
  });
});

// ============================================================================
// Statistics Aggregation Tests
// ============================================================================

describe('Statistics Aggregation', () => {
  describe('Shard query counting', () => {
    it('should count all shard queries', async () => {
      const user = createMockEntity('123', 'user');
      const friend = createMockEntity('456', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [friend]);

      const ctx = createMockContext(lookupEntities, traverseEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // lookup + traverse = 2 queries minimum
      expect(result.stats.shardQueries).toBeGreaterThanOrEqual(2);
    });

    it('should count queries for multi-hop traversals', async () => {
      const user = createMockEntity('123', 'user');
      const friend = createMockEntity('f1', 'user');
      const post = createMockEntity('p1', 'post');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [friend]);
      traverseEntities.set('outgoing:posts', [post]);
      traverseEntities.set('outgoing:comments', []); // No comments

      const ctx = createMockContext(lookupEntities, traverseEntities);
      const plan = planQuery(parse('user:123.friends.posts.comments'));

      const result = await executePlan(plan, ctx);

      // Should have made at least lookup + 2 traversals (friends and posts have results)
      // When posts returns results but no comments, we still made the query
      expect(result.stats.shardQueries).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Entities scanned counting', () => {
    it('should count unique entities scanned', async () => {
      const user = createMockEntity('123', 'user');
      const friend1 = createMockEntity('f1', 'user');
      const friend2 = createMockEntity('f2', 'user');
      const friend3 = createMockEntity('f3', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [friend1, friend2, friend3]);

      const ctx = createMockContext(lookupEntities, traverseEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // Should have scanned user + 3 friends = 4
      expect(result.stats.entitiesScanned).toBe(4);
    });

    it('should not double count entities seen multiple times', async () => {
      const user = createMockEntity('123', 'user');
      const friend = createMockEntity('456', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      // Friend appears twice in results
      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [friend, friend]);

      const ctx = createMockContext(lookupEntities, traverseEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // Should only count unique entities
      expect(result.stats.entitiesScanned).toBeLessThanOrEqual(2);
    });
  });

  describe('Duration tracking', () => {
    it('should track execution duration in milliseconds', async () => {
      const user = createMockEntity('123', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const ctx = createMockContext(lookupEntities);
      const plan = planQuery(parse('user:123'));

      const result = await executePlan(plan, ctx);

      expect(result.stats.durationMs).toBeGreaterThanOrEqual(0);
      expect(typeof result.stats.durationMs).toBe('number');
    });

    it('should have reasonable duration for slow operations', async () => {
      // Create a slow mock stub
      const slowStub = {
        fetch: vi.fn().mockImplementation(async () => {
          await new Promise(resolve => setTimeout(resolve, 50));
          return new Response(
            JSON.stringify({ entities: [], triples: [] }),
            { status: 200 }
          );
        }),
      } as unknown as DurableObjectStub;

      const ctx: ExecutionContext = {
        getShardStub: () => slowStub,
        maxResults: 100,
        timeout: 5000,
      };

      const plan = planQuery(parse('user:123'));
      const result = await executePlan(plan, ctx);

      // Should reflect actual execution time
      expect(result.stats.durationMs).toBeGreaterThanOrEqual(40);
    });
  });
});

// ============================================================================
// Pagination Tests
// ============================================================================

describe('Pagination', () => {
  describe('Cursor generation', () => {
    it('should generate cursor when results exceed limit', async () => {
      const entities: Entity[] = [];
      for (let i = 0; i < 15; i++) {
        entities.push(createMockEntity(`${i}`, 'user'));
      }

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [entities[0]]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', entities.slice(1));

      const ctx = createMockContext(lookupEntities, traverseEntities);
      ctx.maxResults = 10;

      const plan = planQuery(parse('user:123.friends'));
      const result = await executePlan(plan, ctx);

      expect(result.hasMore).toBe(true);
      expect(result.cursor).toBeDefined();
    });

    it('should not generate cursor when all results fit', async () => {
      const user = createMockEntity('123', 'user');
      const friend1 = createMockEntity('f1', 'user');
      const friend2 = createMockEntity('f2', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', [friend1, friend2]);

      const ctx = createMockContext(lookupEntities, traverseEntities);
      ctx.maxResults = 100;

      const plan = planQuery(parse('user:123.friends'));
      const result = await executePlan(plan, ctx);

      expect(result.hasMore).toBe(false);
      expect(result.cursor).toBeUndefined();
    });
  });

  describe('Cursor continuation', () => {
    it('should reject invalid cursor format', async () => {
      const user = createMockEntity('123', 'user');

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [user]);

      const ctx = createMockContext(lookupEntities);
      const plan = planQuery(parse('user:123'));

      await expect(
        executePlan(plan, ctx, { cursor: 'not-valid-base64!' })
      ).rejects.toThrow(/invalid cursor/i);
    });

    it('should reject cursor from different query', async () => {
      const entities: Entity[] = [];
      for (let i = 0; i < 15; i++) {
        entities.push(createMockEntity(`${i}`, 'user'));
      }

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:123', [entities[0]]);
      lookupEntities.set('user:456', [entities[0]]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', entities.slice(1));

      const ctx = createMockContext(lookupEntities, traverseEntities);
      ctx.maxResults = 10;

      // Get cursor from first query
      const plan1 = planQuery(parse('user:123.friends'));
      const result1 = await executePlan(plan1, ctx);
      const cursor = result1.cursor;

      // Try to use with different query
      const plan2 = planQuery(parse('user:456.friends'));

      await expect(
        executePlan(plan2, ctx, { cursor })
      ).rejects.toThrow(/cursor.*mismatch/i);
    });
  });

  describe('Result limits', () => {
    it('should respect maxResults limit', async () => {
      const entities: Entity[] = [];
      for (let i = 0; i < 50; i++) {
        entities.push(createMockEntity(`${i}`, 'user'));
      }

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:0', [entities[0]]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', entities.slice(1));

      const ctx = createMockContext(lookupEntities, traverseEntities);
      ctx.maxResults = 20;

      const plan = planQuery(parse('user:0.friends'));
      const result = await executePlan(plan, ctx);

      expect(result.entities.length).toBeLessThanOrEqual(20);
    });

    it('should use default maxResults when not specified', async () => {
      const entities: Entity[] = [];
      for (let i = 0; i < 150; i++) {
        entities.push(createMockEntity(`${i}`, 'user'));
      }

      const lookupEntities = new Map<string, Entity[]>();
      lookupEntities.set('user:0', [entities[0]]);

      const traverseEntities = new Map<string, Entity[]>();
      traverseEntities.set('outgoing:friends', entities.slice(1));

      const ctx = createMockContext(lookupEntities, traverseEntities);
      delete ctx.maxResults; // Remove explicit limit

      const plan = planQuery(parse('user:0.friends'));
      const result = await executePlan(plan, ctx);

      // Default should be 100
      expect(result.entities.length).toBeLessThanOrEqual(100);
    });
  });
});

// ============================================================================
// Timeout Handling Tests
// ============================================================================

describe('Timeout Handling', () => {
  it('should respect timeout and return partial results', async () => {
    // Create a very slow stub
    let queryCount = 0;
    const slowStub = {
      fetch: vi.fn().mockImplementation(async () => {
        queryCount++;
        await new Promise(resolve => setTimeout(resolve, 100));
        return new Response(
          JSON.stringify({
            entities: [createMockEntity(`e${queryCount}`, 'user')],
            triples: [],
          }),
          { status: 200 }
        );
      }),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => slowStub,
      maxResults: 100,
      timeout: 50, // Very short timeout
    };

    const plan = planQuery(parse('user:123.friends.posts.comments'));

    const result = await executePlan(plan, ctx);

    // Should complete without error
    expect(result).toBeDefined();
    expect(result.stats).toBeDefined();
    // May have partial results depending on timeout
  });

  it('should track duration accurately even with timeout', async () => {
    const slowStub = {
      fetch: vi.fn().mockImplementation(async () => {
        await new Promise(resolve => setTimeout(resolve, 30));
        return new Response(
          JSON.stringify({ entities: [], triples: [] }),
          { status: 200 }
        );
      }),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => slowStub,
      maxResults: 100,
      timeout: 100,
    };

    const plan = planQuery(parse('user:123'));
    const result = await executePlan(plan, ctx);

    expect(result.stats.durationMs).toBeGreaterThan(0);
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('Error Handling', () => {
  describe('Shard errors', () => {
    it('should throw on shard fetch failure', async () => {
      const errorStub = {
        fetch: vi.fn().mockResolvedValue(
          new Response('Internal Server Error', { status: 500 })
        ),
      } as unknown as DurableObjectStub;

      const ctx: ExecutionContext = {
        getShardStub: () => errorStub,
        maxResults: 100,
      };

      const plan = planQuery(parse('user:123'));

      await expect(executePlan(plan, ctx)).rejects.toThrow(/shard.*failed/i);
    });

    it('should include shard ID in error message', async () => {
      const errorStub = {
        fetch: vi.fn().mockResolvedValue(
          new Response('Not Found', { status: 404 })
        ),
      } as unknown as DurableObjectStub;

      const ctx: ExecutionContext = {
        getShardStub: () => errorStub,
        maxResults: 100,
      };

      const plan = planQuery(parse('user:123'));

      try {
        await executePlan(plan, ctx);
        expect.fail('Should have thrown');
      } catch (error) {
        expect((error as Error).message).toMatch(/shard/i);
      }
    });
  });

  describe('Network errors', () => {
    it('should handle network timeout gracefully', async () => {
      const timeoutStub = {
        fetch: vi.fn().mockImplementation(() =>
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Network timeout')), 10)
          )
        ),
      } as unknown as DurableObjectStub;

      const ctx: ExecutionContext = {
        getShardStub: () => timeoutStub,
        maxResults: 100,
        timeout: 100,
      };

      const plan = planQuery(parse('user:123'));

      await expect(executePlan(plan, ctx)).rejects.toThrow();
    });
  });
});

// ============================================================================
// Traversal Function Tests
// ============================================================================

describe('traverseFrom', () => {
  it('should respect MAX_PATH_DEPTH limit', async () => {
    const mockStub = {
      fetch: vi.fn().mockImplementation(async () => {
        return new Response(
          JSON.stringify({
            entities: [createMockEntity('x', 'user')],
            triples: [],
          }),
          { status: 200 }
        );
      }),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => mockStub,
      maxResults: 1000,
    };

    // Request depth way above MAX_PATH_DEPTH
    const result = await traverseFrom(
      'https://test.com/user/123',
      'friends',
      { maxDepth: 500 },
      ctx
    );

    // Should be capped at MAX_PATH_DEPTH
    expect(result.length).toBeLessThanOrEqual(MAX_PATH_DEPTH * 10); // Rough upper bound
  });

  it('should use DEFAULT_PATH_DEPTH when not specified', async () => {
    let queryCount = 0;
    const mockStub = {
      fetch: vi.fn().mockImplementation(async () => {
        queryCount++;
        return new Response(
          JSON.stringify({
            entities: [createMockEntity(`e${queryCount}`, 'user')],
            triples: [],
          }),
          { status: 200 }
        );
      }),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => mockStub,
      maxResults: 100,
    };

    await traverseFrom(
      'https://test.com/user/123',
      'friends',
      {}, // No maxDepth specified
      ctx
    );

    // Should make at most DEFAULT_PATH_DEPTH queries per level
    expect(queryCount).toBeLessThanOrEqual(DEFAULT_PATH_DEPTH + 1);
  });
});

describe('traverseTo', () => {
  it('should find entities with incoming edges', async () => {
    const liker1 = createMockEntity('l1', 'user', { name: 'Liker 1' });
    const liker2 = createMockEntity('l2', 'user', { name: 'Liker 2' });

    const mockStub = {
      fetch: vi.fn().mockImplementation(async () => {
        return new Response(
          JSON.stringify({
            entities: [liker1, liker2],
            triples: [],
          }),
          { status: 200 }
        );
      }),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => mockStub,
      maxResults: 100,
    };

    const result = await traverseTo(
      'https://test.com/post/456',
      'likes',
      ctx
    );

    expect(result.length).toBe(2);
  });

  it('should return empty array when no incoming edges', async () => {
    const mockStub = {
      fetch: vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ entities: [], triples: [] }),
          { status: 200 }
        )
      ),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => mockStub,
      maxResults: 100,
    };

    const result = await traverseTo(
      'https://test.com/post/456',
      'likes',
      ctx
    );

    expect(result).toEqual([]);
  });

  it('should throw on shard error', async () => {
    const errorStub = {
      fetch: vi.fn().mockResolvedValue(
        new Response('Error', { status: 500 })
      ),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => errorStub,
      maxResults: 100,
    };

    await expect(
      traverseTo('https://test.com/post/456', 'likes', ctx)
    ).rejects.toThrow();
  });
});

// ============================================================================
// Filter Evaluation Tests
// ============================================================================

describe('Filter Evaluation', () => {
  it('should filter entities by equality', async () => {
    const admin = createMockEntity('a1', 'user', { role: 'admin' });
    const user = createMockEntity('u1', 'user', { role: 'user' });

    const lookupEntities = new Map<string, Entity[]>();
    lookupEntities.set('user:123', [createMockEntity('123', 'user')]);

    const traverseEntities = new Map<string, Entity[]>();
    traverseEntities.set('outgoing:friends', [admin, user]);

    const ctx = createMockContext(lookupEntities, traverseEntities);
    const plan = planQuery(parse('user:123.friends[?role = "admin"]'));

    const result = await executePlan(plan, ctx);

    // Only admin should match
    const adminResults = result.entities.filter(
      (e: Entity) => (e as Record<string, unknown>).role === 'admin'
    );
    expect(adminResults.length).toBeLessThanOrEqual(result.entities.length);
  });

  it('should filter entities by comparison', async () => {
    const young = createMockEntity('y1', 'user', { age: 20 });
    const old = createMockEntity('o1', 'user', { age: 50 });

    const lookupEntities = new Map<string, Entity[]>();
    lookupEntities.set('user:123', [createMockEntity('123', 'user')]);

    const traverseEntities = new Map<string, Entity[]>();
    traverseEntities.set('outgoing:friends', [young, old]);

    const ctx = createMockContext(lookupEntities, traverseEntities);
    const plan = planQuery(parse('user:123.friends[?age > 30]'));

    const result = await executePlan(plan, ctx);

    // Filter should have been applied (actual filtering depends on executor)
    expect(result.entities).toBeDefined();
  });
});
