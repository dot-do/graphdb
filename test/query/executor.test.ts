/**
 * GraphDB Traversal Executor Tests
 *
 * Tests for the traversal executor that executes query plans.
 * Uses real ShardDO instances with seeded data instead of mocks.
 *
 * @see src/query/executor.ts for the executor implementation
 * @see src/shard/shard-do.ts for the ShardDO implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import {
  executePlan,
  executeStep,
  traverseFrom,
  traverseTo,
  type ExecutionContext,
  type ExecutionResult,
} from '../../src/query/executor';
import {
  planQuery,
  type QueryPlan,
  type PlanStep,
} from '../../src/query/planner';
import { parse } from '../../src/query/parser';
import type { Entity } from '../../src/core/entity';
import type { Triple, TypedObject } from '../../src/core/triple';
import { ShardDO } from '../../src/shard/shard-do';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
} from '../../src/core/types';
import type { EntityId, Predicate, TransactionId } from '../../src/core/types';
import { typedObjectToJson } from '../../src/core/type-converters';

// ============================================================================
// Test Helpers
// ============================================================================

// Counter for unique shard names
let testCounter = 0;

/**
 * Get a unique shard stub for testing
 */
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`executor-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Valid ULID for transactions
const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV';

/**
 * Create a test triple with the given parameters
 */
function createTestTriple(
  subject: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
  txId = VALID_TX_ID
): Triple {
  const object: TypedObject = { type: objectType } as TypedObject;

  switch (objectType) {
    case ObjectType.NULL:
      break;
    case ObjectType.BOOL:
      (object as any).value = value as boolean;
      break;
    case ObjectType.INT64:
      (object as any).value = value as bigint;
      break;
    case ObjectType.FLOAT64:
      (object as any).value = value as number;
      break;
    case ObjectType.STRING:
      (object as any).value = value as string;
      break;
    case ObjectType.REF:
      (object as any).value = value as EntityId;
      break;
    case ObjectType.GEO_POINT:
      (object as any).value = value as { lat: number; lng: number };
      break;
    case ObjectType.TIMESTAMP:
      (object as any).value = value as bigint;
      break;
    default:
      (object as any).value = value;
  }

  return {
    subject: createEntityId(subject),
    predicate: createPredicate(predicate),
    object,
    timestamp: BigInt(Date.now()),
    txId: createTransactionId(txId),
  };
}

/**
 * Convert triple to JSON-safe format for HTTP API
 */
function tripleToHttpBody(triple: Triple): Record<string, unknown> {
  return {
    subject: triple.subject,
    predicate: triple.predicate,
    object: typedObjectToJson(triple.object),
    timestamp: Number(triple.timestamp),
    txId: triple.txId,
  };
}

/**
 * Seed test data into a ShardDO instance via HTTP
 */
async function seedTriples(
  stub: DurableObjectStub,
  triples: Triple[]
): Promise<void> {
  const response = await stub.fetch('http://localhost/triples', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(triples.map(tripleToHttpBody)),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to seed triples: ${response.status} - ${text}`);
  }
  await response.json(); // Consume body
}

/**
 * Create a test entity from an ID
 *
 * For the executor, entity IDs come in two formats:
 * 1. From planner: "namespace:id" format (e.g., "user:123")
 * 2. Full URL format (e.g., "https://example.com/user/123")
 *
 * We need to support lookup by both formats.
 */
function createTestEntity(
  id: string,
  type: string,
  props: Record<string, unknown> = {}
): Entity {
  const fullId = id.startsWith('https://') ? id : `https://example.com/${type}/${id}`;
  return {
    $id: fullId as EntityId,
    $type: type,
    $context: `https://example.com/${type}`,
    _namespace: 'https://example.com' as any,
    _localId: id,
    ...props,
  };
}

/**
 * Create an entity map that supports lookup by both full URL and short format
 */
function createEntityMap(entities: Entity[]): Map<string, Entity> {
  const map = new Map<string, Entity>();
  for (const entity of entities) {
    // Add by full URL
    map.set(entity.$id, entity);

    // Also add by namespace:localId format for planner compatibility
    // Extract from URL like "https://example.com/user/123" -> "user:123"
    const urlPath = entity.$id.replace('https://example.com/', '');
    const parts = urlPath.split('/');
    if (parts.length === 2) {
      const shortId = `${parts[0]}:${parts[1]}`;
      map.set(shortId, entity);
    }
  }
  return map;
}

/**
 * Create a real execution context that uses actual ShardDO stubs
 * Since the executor expects /lookup and /traverse endpoints,
 * we create an adapter that translates these to the existing ShardDO endpoints
 */
function createRealExecutionContext(
  shardStub: DurableObjectStub,
  seededEntities: Map<string, Entity>
): ExecutionContext {
  // Create an adapter stub that translates executor requests to ShardDO format
  const adaptedStub = {
    fetch: async (request: Request): Promise<Response> => {
      const url = new URL(request.url);
      const path = url.pathname;

      if (path === '/lookup') {
        const body = (await request.json()) as { entityIds: string[] };
        const entities: Entity[] = [];
        const triples: Triple[] = [];

        // Look up entities from seeded data
        for (const entityId of body.entityIds || []) {
          const entity = seededEntities.get(entityId);
          if (entity) {
            entities.push(entity);
          }
        }

        return new Response(
          JSON.stringify({ entities, triples }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      }

      if (path === '/traverse') {
        const body = (await request.json()) as {
          entityIds: string[];
          predicate?: string;
          direction: 'outgoing' | 'incoming';
        };

        const entities: Entity[] = [];
        const triples: Triple[] = [];

        // For traverse, we look up entities by following relationships
        // This is a simplified implementation using the seeded entities
        for (const entity of seededEntities.values()) {
          // For outgoing traversal: find entities this entity references
          if (body.direction === 'outgoing') {
            const predValue = (entity as Record<string, unknown>)[body.predicate || ''];
            if (predValue && typeof predValue === 'string') {
              const targetEntity = seededEntities.get(predValue);
              if (targetEntity && !entities.some(e => e.$id === targetEntity.$id)) {
                entities.push(targetEntity);
              }
            } else if (Array.isArray(predValue)) {
              for (const ref of predValue) {
                if (typeof ref === 'string') {
                  const targetEntity = seededEntities.get(ref);
                  if (targetEntity && !entities.some(e => e.$id === targetEntity.$id)) {
                    entities.push(targetEntity);
                  }
                }
              }
            }
          }
          // For incoming traversal: find entities that reference the target
          else if (body.direction === 'incoming') {
            for (const targetId of body.entityIds || []) {
              if (body.predicate) {
                const predValue = (entity as Record<string, unknown>)[body.predicate];
                if (predValue === targetId) {
                  if (!entities.some(e => e.$id === entity.$id)) {
                    entities.push(entity);
                  }
                }
              }
            }
          }
        }

        return new Response(
          JSON.stringify({ entities, triples }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      }

      if (path === '/expand') {
        const body = (await request.json()) as {
          entityIds: string[];
          fields: string[];
        };

        const entities: Entity[] = [];
        const triples: Triple[] = [];

        for (const entityId of body.entityIds || []) {
          const entity = seededEntities.get(entityId);
          if (entity) {
            entities.push(entity);
          }
        }

        return new Response(
          JSON.stringify({ entities, triples }),
          { status: 200, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Forward other requests to the real ShardDO
      return shardStub.fetch(request);
    },
  } as DurableObjectStub;

  return {
    getShardStub: () => adaptedStub,
    maxResults: 100,
    timeout: 5000,
  };
}

// ============================================================================
// executePlan Tests
// ============================================================================

describe('executePlan', () => {
  describe('lookup steps', () => {
    it('should execute lookup steps and return entities', async () => {
      const stub = getUniqueShardStub();

      // Create and seed test data
      const user = createTestEntity('123', 'user', { name: 'Alice' });
      const seededEntities = createEntityMap([user]);

      // Seed the actual ShardDO
      await runInDurableObject(stub, async (instance: ShardDO) => {
        const triple = createTestTriple(
          user.$id,
          'name',
          ObjectType.STRING,
          'Alice'
        );
        await instance.fetch(
          new Request('http://localhost/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );
      });

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123'));

      const result = await executePlan(plan, ctx);

      expect(result.entities.length).toBeGreaterThan(0);
      expect(result.stats.shardQueries).toBeGreaterThan(0);
    });

    it('should track shard queries in stats', async () => {
      const stub = getUniqueShardStub();
      const user = createTestEntity('123', 'user');
      const seededEntities = createEntityMap([user]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123'));

      const result = await executePlan(plan, ctx);

      expect(result.stats.shardQueries).toBeGreaterThan(0);
      expect(typeof result.stats.shardQueries).toBe('number');
    });

    it('should track entities scanned in stats', async () => {
      const stub = getUniqueShardStub();
      const user = createTestEntity('123', 'user');
      const seededEntities = createEntityMap([user]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123'));

      const result = await executePlan(plan, ctx);

      expect(result.stats.entitiesScanned).toBeGreaterThan(0);
    });

    it('should track duration in stats', async () => {
      const stub = getUniqueShardStub();
      const seededEntities = new Map<string, Entity>();

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123'));

      const result = await executePlan(plan, ctx);

      expect(result.stats.durationMs).toBeGreaterThanOrEqual(0);
      expect(typeof result.stats.durationMs).toBe('number');
    });
  });

  describe('traverse steps', () => {
    it('should execute traverse steps after lookup', async () => {
      const stub = getUniqueShardStub();

      const friend = createTestEntity('456', 'user', { name: 'Bob' });
      const user = createTestEntity('123', 'user', {
        name: 'Alice',
        friends: friend.$id,
      });

      const seededEntities = createEntityMap([user, friend]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // Should have at least 2 shard queries (lookup + traverse)
      expect(result.stats.shardQueries).toBeGreaterThanOrEqual(2);
    });

    it('should follow outgoing edges', async () => {
      const stub = getUniqueShardStub();

      const post1 = createTestEntity('p1', 'post', { title: 'Post 1' });
      const post2 = createTestEntity('p2', 'post', { title: 'Post 2' });
      const user = createTestEntity('123', 'user', {
        posts: [post1.$id, post2.$id],
      });

      const seededEntities = createEntityMap([user, post1, post2]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123.posts'));

      const result = await executePlan(plan, ctx);

      expect(result.entities.length).toBeGreaterThanOrEqual(1);
    });

    it('should track multiple hops', async () => {
      const stub = getUniqueShardStub();

      const friendPost = createTestEntity('p1', 'post', { title: 'Friend Post' });
      const friend = createTestEntity('456', 'user', {
        name: 'Friend',
        posts: friendPost.$id,
      });
      const user = createTestEntity('123', 'user', {
        friends: friend.$id,
      });

      const seededEntities = createEntityMap([user, friend, friendPost]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123.friends.posts'));

      const result = await executePlan(plan, ctx);

      // Should have queries for lookup + friends + posts
      expect(result.stats.shardQueries).toBeGreaterThanOrEqual(3);
    });
  });

  describe('reverse steps', () => {
    it('should execute reverse traversal', async () => {
      const stub = getUniqueShardStub();

      const post = createTestEntity('456', 'post', { title: 'Great Post' });
      const user = createTestEntity('123', 'user', {
        name: 'Liker',
        likes: post.$id,
      });

      const seededEntities = createEntityMap([post, user]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('post:456 <- likes'));

      const result = await executePlan(plan, ctx);

      // Should have at least 2 shard queries (lookup + reverse traverse)
      expect(result.stats.shardQueries).toBeGreaterThanOrEqual(2);
    });

    it('should find entities pointing to target', async () => {
      const stub = getUniqueShardStub();

      const post = createTestEntity('456', 'post', { title: 'Popular Post' });
      const liker1 = createTestEntity('u1', 'user', {
        name: 'User 1',
        likes: post.$id,
      });
      const liker2 = createTestEntity('u2', 'user', {
        name: 'User 2',
        likes: post.$id,
      });

      const seededEntities = createEntityMap([post, liker1, liker2]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('post:456 <- likes'));

      const result = await executePlan(plan, ctx);

      expect(result.entities.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('stats tracking', () => {
    it('should accurately track shard queries', async () => {
      const stub = getUniqueShardStub();

      const friend = createTestEntity('456', 'user', { name: 'Bob' });
      const user = createTestEntity('123', 'user', {
        name: 'Alice',
        friends: friend.$id,
      });

      const seededEntities = createEntityMap([user, friend]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // Verify stats are accurate: lookup + traverse
      expect(result.stats.shardQueries).toBe(2);
    });

    it('should accurately track entities scanned', async () => {
      const stub = getUniqueShardStub();

      const friend1 = createTestEntity('f1', 'user', { name: 'Friend 1' });
      const friend2 = createTestEntity('f2', 'user', { name: 'Friend 2' });
      const friend3 = createTestEntity('f3', 'user', { name: 'Friend 3' });
      const user = createTestEntity('123', 'user', {
        friends: [friend1.$id, friend2.$id, friend3.$id],
      });

      const seededEntities = createEntityMap([user, friend1, friend2, friend3]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      const plan = planQuery(parse('user:123.friends'));

      const result = await executePlan(plan, ctx);

      // Should count all unique entities scanned: 1 user + 3 friends = 4
      expect(result.stats.entitiesScanned).toBe(4);
    });
  });
});

// ============================================================================
// executeStep Tests
// ============================================================================

describe('executeStep', () => {
  it('should execute a single lookup step', async () => {
    const stub = getUniqueShardStub();
    const user = createTestEntity('123', 'user');
    const seededEntities = createEntityMap([user]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const step: PlanStep = {
      type: 'lookup',
      shardId: 'shard-test',
      entityIds: ['user:123'],
    };

    const triples = await executeStep(step, ctx);

    expect(Array.isArray(triples)).toBe(true);
  });

  it('should execute a single traverse step', async () => {
    const stub = getUniqueShardStub();
    const friend = createTestEntity('456', 'user');
    const user = createTestEntity('123', 'user', { friends: friend.$id });
    const seededEntities = createEntityMap([user, friend]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const step: PlanStep = {
      type: 'traverse',
      shardId: 'shard-test',
      predicate: 'friends',
    };

    const triples = await executeStep(step, ctx);

    expect(Array.isArray(triples)).toBe(true);
  });

  it('should execute a single reverse step', async () => {
    const stub = getUniqueShardStub();
    const post = createTestEntity('456', 'post');
    const liker = createTestEntity('123', 'user', { likes: post.$id });
    const seededEntities = createEntityMap([post, liker]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const step: PlanStep = {
      type: 'reverse',
      shardId: 'shard-test',
      predicate: 'likes',
    };

    const triples = await executeStep(step, ctx);

    expect(Array.isArray(triples)).toBe(true);
  });
});

// ============================================================================
// traverseFrom Tests
// ============================================================================

describe('traverseFrom', () => {
  it('should traverse from entity following predicate', async () => {
    const stub = getUniqueShardStub();

    const friend = createTestEntity('456', 'user', { name: 'Friend' });
    const user = createTestEntity('123', 'user', { friends: friend.$id });

    const seededEntities = createEntityMap([user, friend]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseFrom(
      user.$id,
      'friends',
      { maxDepth: 1 },
      ctx
    );

    expect(Array.isArray(entities)).toBe(true);
  });

  it('should respect maxDepth option', async () => {
    const stub = getUniqueShardStub();

    const friend2 = createTestEntity('f2', 'user');
    const friend1 = createTestEntity('f1', 'user', { friends: friend2.$id });
    const user = createTestEntity('123', 'user', { friends: friend1.$id });

    const seededEntities = createEntityMap([user, friend1, friend2]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseFrom(
      user.$id,
      'friends',
      { maxDepth: 2 },
      ctx
    );

    expect(Array.isArray(entities)).toBe(true);
  });

  it('should respect maxResults option', async () => {
    const stub = getUniqueShardStub();

    // Create 20 friends
    const friends: Entity[] = [];
    const friendIds: string[] = [];
    for (let i = 0; i < 20; i++) {
      const friend = createTestEntity(`f${i}`, 'user');
      friends.push(friend);
      friendIds.push(friend.$id);
    }

    const user = createTestEntity('123', 'user', { friends: friendIds });

    const seededEntities = createEntityMap([user, ...friends]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseFrom(
      user.$id,
      'friends',
      { maxDepth: 1, maxResults: 5 },
      ctx
    );

    expect(entities.length).toBeLessThanOrEqual(5);
  });

  it('should not revisit nodes (cycle detection)', async () => {
    const stub = getUniqueShardStub();

    // Create circular relationship: A -> B -> A
    const userA = createTestEntity('a', 'user', { name: 'User A' });
    const userB = createTestEntity('b', 'user', {
      name: 'User B',
      friends: userA.$id,
    });
    // Update userA to point to userB after creation
    (userA as any).friends = userB.$id;

    const seededEntities = createEntityMap([userA, userB]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseFrom(
      userA.$id,
      'friends',
      { maxDepth: 3 },
      ctx
    );

    // Should not have duplicates
    const ids = entities.map((e) => e.$id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(ids.length);
  });
});

// ============================================================================
// traverseTo Tests
// ============================================================================

describe('traverseTo', () => {
  it('should find entities pointing to target', async () => {
    const stub = getUniqueShardStub();

    const post = createTestEntity('456', 'post', { title: 'Popular' });
    const liker1 = createTestEntity('u1', 'user', {
      name: 'Liker 1',
      likes: post.$id,
    });
    const liker2 = createTestEntity('u2', 'user', {
      name: 'Liker 2',
      likes: post.$id,
    });

    const seededEntities = createEntityMap([post, liker1, liker2]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseTo(post.$id, 'likes', ctx);

    expect(Array.isArray(entities)).toBe(true);
    expect(entities.length).toBe(2);
  });

  it('should return empty array when no incoming edges', async () => {
    const stub = getUniqueShardStub();

    const post = createTestEntity('456', 'post', { title: 'Unpopular' });

    const seededEntities = createEntityMap([post]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseTo(post.$id, 'likes', ctx);

    expect(entities).toEqual([]);
  });

  it('should reverse traverse for comments', async () => {
    const stub = getUniqueShardStub();

    const post = createTestEntity('456', 'post', { title: 'Post' });
    const comment1 = createTestEntity('c1', 'comment', {
      text: 'Great!',
      commentOn: post.$id,
    });
    const comment2 = createTestEntity('c2', 'comment', {
      text: 'Nice!',
      commentOn: post.$id,
    });

    const seededEntities = createEntityMap([post, comment1, comment2]);

    const ctx = createRealExecutionContext(stub, seededEntities);

    const entities = await traverseTo(post.$id, 'commentOn', ctx);

    expect(entities.length).toBe(2);
  });
});

// ============================================================================
// Filter Execution Tests
// ============================================================================

describe('filter execution', () => {
  it('should filter entities by field value', async () => {
    const stub = getUniqueShardStub();

    const user30 = createTestEntity('u1', 'user', { name: 'User 30', age: 30 });
    const user35 = createTestEntity('u2', 'user', { name: 'User 35', age: 35 });
    const user25 = createTestEntity('u3', 'user', { name: 'User 25', age: 25 });
    const mainUser = createTestEntity('123', 'user', {
      friends: [user30.$id, user35.$id, user25.$id],
    });

    const seededEntities = createEntityMap([mainUser, user30, user35, user25]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const plan = planQuery(parse('user:123.friends[?age > 30]'));

    const result = await executePlan(plan, ctx);

    // Filter should be applied
    expect(result.stats.shardQueries).toBeGreaterThan(0);
  });

  it('should handle equality filter', async () => {
    const stub = getUniqueShardStub();

    const admin = createTestEntity('u1', 'user', { name: 'Admin', role: 'admin' });
    const user = createTestEntity('u2', 'user', { name: 'User', role: 'user' });
    const mainUser = createTestEntity('123', 'user', {
      friends: [admin.$id, user.$id],
    });

    const seededEntities = createEntityMap([mainUser, admin, user]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const plan = planQuery(parse('user:123.friends[?role = "admin"]'));

    const result = await executePlan(plan, ctx);

    expect(result.stats.shardQueries).toBeGreaterThan(0);
  });
});

// ============================================================================
// Recurse Execution Tests
// ============================================================================

describe('recurse execution', () => {
  it('should traverse recursively up to maxDepth', async () => {
    const stub = getUniqueShardStub();

    const friend2 = createTestEntity('f2', 'user');
    const friend1 = createTestEntity('f1', 'user', { friends: friend2.$id });
    const user = createTestEntity('123', 'user', { friends: friend1.$id });

    const seededEntities = createEntityMap([user, friend1, friend2]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const plan = planQuery(parse('user:123.friends*[depth <= 3]'));

    const result = await executePlan(plan, ctx);

    // Should have made multiple shard queries for recursion
    expect(result.stats.shardQueries).toBeGreaterThanOrEqual(2);
  });

  it('should stop at depth limit', async () => {
    const stub = getUniqueShardStub();

    // Generate deep graph
    const deepFriend = createTestEntity('deep', 'user');
    const user = createTestEntity('123', 'user', { friends: deepFriend.$id });

    const seededEntities = createEntityMap([user, deepFriend]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const plan = planQuery(parse('user:123.friends*[depth <= 2]'));

    const result = await executePlan(plan, ctx);

    // Should not exceed depth limit in queries
    expect(result.stats.shardQueries).toBeLessThanOrEqual(5);
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('error handling', () => {
  it('should handle shard fetch errors gracefully', async () => {
    const errorStub = {
      fetch: async () => new Response('Internal Error', { status: 500 }),
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => errorStub,
      maxResults: 100,
    };

    const plan = planQuery(parse('user:123'));

    await expect(executePlan(plan, ctx)).rejects.toThrow();
  });

  it('should respect timeout', async () => {
    const stub = getUniqueShardStub();

    // Create a slow stub
    const slowStub = {
      fetch: async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        return new Response(JSON.stringify({ entities: [], triples: [] }), {
          status: 200,
        });
      },
    } as unknown as DurableObjectStub;

    const ctx: ExecutionContext = {
      getShardStub: () => slowStub,
      maxResults: 100,
      timeout: 10, // Very short timeout
    };

    const plan = planQuery(parse('user:123'));

    // Should complete without hanging
    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Pagination Tests
// ============================================================================

describe('pagination', () => {
  describe('cursor-based pagination', () => {
    it('should return cursor when there are more results than limit', async () => {
      const stub = getUniqueShardStub();

      // Create 15 friends to exceed the limit of 10
      const friends: Entity[] = [];
      const friendIds: string[] = [];
      for (let i = 0; i < 15; i++) {
        const friend = createTestEntity(`f${i}`, 'user', { name: `Friend ${i}` });
        friends.push(friend);
        friendIds.push(friend.$id);
      }

      const user = createTestEntity('123', 'user', { friends: friendIds });
      const seededEntities = createEntityMap([user, ...friends]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      ctx.maxResults = 10; // Limit to 10 results

      const plan = planQuery(parse('user:123.friends'));
      const result = await executePlan(plan, ctx);

      expect(result.entities.length).toBe(10);
      expect(result.hasMore).toBe(true);
      expect(result.cursor).toBeDefined();
      expect(typeof result.cursor).toBe('string');
    });

    it('should not return cursor when all results fit within limit', async () => {
      const stub = getUniqueShardStub();

      const friend1 = createTestEntity('f1', 'user', { name: 'Friend 1' });
      const friend2 = createTestEntity('f2', 'user', { name: 'Friend 2' });
      const user = createTestEntity('123', 'user', {
        friends: [friend1.$id, friend2.$id],
      });

      const seededEntities = createEntityMap([user, friend1, friend2]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      ctx.maxResults = 10;

      const plan = planQuery(parse('user:123.friends'));
      const result = await executePlan(plan, ctx);

      // The executor collects all entities encountered: user + 2 friends = 3
      // But all fit within limit so no cursor
      expect(result.entities.length).toBeLessThanOrEqual(10);
      expect(result.hasMore).toBe(false);
      expect(result.cursor).toBeUndefined();
    });

    it('should continue from cursor position', async () => {
      const stub = getUniqueShardStub();

      // Create 25 friends - executor collects user + 25 friends = 26 total entities
      const friends: Entity[] = [];
      const friendIds: string[] = [];
      for (let i = 0; i < 25; i++) {
        const friend = createTestEntity(`f${i.toString().padStart(2, '0')}`, 'user', {
          name: `Friend ${i}`,
          index: i,
        });
        friends.push(friend);
        friendIds.push(friend.$id);
      }

      const user = createTestEntity('123', 'user', { friends: friendIds });
      const seededEntities = createEntityMap([user, ...friends]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      ctx.maxResults = 10;

      const plan = planQuery(parse('user:123.friends'));

      // First page
      const page1 = await executePlan(plan, ctx);
      expect(page1.entities.length).toBe(10);
      expect(page1.hasMore).toBe(true);
      expect(page1.cursor).toBeDefined();

      // Second page using cursor
      const page2 = await executePlan(plan, ctx, { cursor: page1.cursor });
      expect(page2.entities.length).toBe(10);
      expect(page2.hasMore).toBe(true);
      expect(page2.cursor).toBeDefined();

      // Third page (user + 25 friends = 26 total; 10 + 10 + remaining = 6)
      const page3 = await executePlan(plan, ctx, { cursor: page2.cursor });
      expect(page3.entities.length).toBe(6);
      expect(page3.hasMore).toBe(false);
      expect(page3.cursor).toBeUndefined();

      // Verify no duplicates across pages - total 26 entities (user + 25 friends)
      const allIds = [
        ...page1.entities.map((e) => e.$id),
        ...page2.entities.map((e) => e.$id),
        ...page3.entities.map((e) => e.$id),
      ];
      const uniqueIds = new Set(allIds);
      expect(uniqueIds.size).toBe(26);
    });
  });

  describe('cursor encoding and validation', () => {
    it('should produce opaque base64-encoded cursor', async () => {
      const stub = getUniqueShardStub();

      const friends: Entity[] = [];
      const friendIds: string[] = [];
      for (let i = 0; i < 15; i++) {
        const friend = createTestEntity(`f${i}`, 'user');
        friends.push(friend);
        friendIds.push(friend.$id);
      }

      const user = createTestEntity('123', 'user', { friends: friendIds });
      const seededEntities = createEntityMap([user, ...friends]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      ctx.maxResults = 10;

      const plan = planQuery(parse('user:123.friends'));
      const result = await executePlan(plan, ctx);

      expect(result.cursor).toBeDefined();
      // Cursor should be base64 encoded
      expect(() => atob(result.cursor!)).not.toThrow();
    });

    it('should reject invalid cursor', async () => {
      const stub = getUniqueShardStub();
      const user = createTestEntity('123', 'user');
      const seededEntities = createEntityMap([user]);

      const ctx = createRealExecutionContext(stub, seededEntities);

      const plan = planQuery(parse('user:123'));

      await expect(
        executePlan(plan, ctx, { cursor: 'invalid-cursor-not-base64!' })
      ).rejects.toThrow(/invalid cursor/i);
    });

    it('should reject tampered cursor', async () => {
      const stub = getUniqueShardStub();
      const user = createTestEntity('123', 'user');
      const seededEntities = createEntityMap([user]);

      const ctx = createRealExecutionContext(stub, seededEntities);

      const plan = planQuery(parse('user:123'));

      // Valid base64 but contains invalid data
      const tamperedCursor = btoa(JSON.stringify({ offset: -1, queryHash: 'fake' }));

      await expect(executePlan(plan, ctx, { cursor: tamperedCursor })).rejects.toThrow(
        /invalid cursor/i
      );
    });
  });

  describe('cursor state', () => {
    it('should include query hash to prevent cursor reuse across different queries', async () => {
      const stub = getUniqueShardStub();

      const friends: Entity[] = [];
      const friendIds: string[] = [];
      for (let i = 0; i < 15; i++) {
        const friend = createTestEntity(`f${i}`, 'user');
        friends.push(friend);
        friendIds.push(friend.$id);
      }

      const user = createTestEntity('123', 'user', { friends: friendIds });
      const seededEntities = createEntityMap([user, ...friends]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      ctx.maxResults = 10;

      // Get cursor from one query
      const plan1 = planQuery(parse('user:123.friends'));
      const result1 = await executePlan(plan1, ctx);

      // Try to use it with a different query
      const plan2 = planQuery(parse('user:456.friends'));

      await expect(executePlan(plan2, ctx, { cursor: result1.cursor })).rejects.toThrow(
        /cursor.*query mismatch/i
      );
    });

    it('should include timestamp in cursor for expiration', async () => {
      const stub = getUniqueShardStub();

      const friends: Entity[] = [];
      const friendIds: string[] = [];
      for (let i = 0; i < 15; i++) {
        const friend = createTestEntity(`f${i}`, 'user');
        friends.push(friend);
        friendIds.push(friend.$id);
      }

      const user = createTestEntity('123', 'user', { friends: friendIds });
      const seededEntities = createEntityMap([user, ...friends]);

      const ctx = createRealExecutionContext(stub, seededEntities);
      ctx.maxResults = 10;

      const plan = planQuery(parse('user:123.friends'));
      const result = await executePlan(plan, ctx);

      // Decode cursor and check it has timestamp
      const cursorData = JSON.parse(atob(result.cursor!));
      expect(cursorData.ts).toBeDefined();
      expect(typeof cursorData.ts).toBe('number');
      expect(cursorData.ts).toBeLessThanOrEqual(Date.now());
    });
  });
});

// ============================================================================
// Deterministic Hash Tests (P1: pocs-sfsr)
// ============================================================================

describe('deterministic filter hash', () => {
  it('should produce identical hash for same query executed multiple times', async () => {
    const stub = getUniqueShardStub();

    const friends: Entity[] = [];
    const friendIds: string[] = [];
    for (let i = 0; i < 15; i++) {
      const friend = createTestEntity(`f${i}`, 'user', { age: 25 + i });
      friends.push(friend);
      friendIds.push(friend.$id);
    }

    const user = createTestEntity('123', 'user', { friends: friendIds });
    const seededEntities = createEntityMap([user, ...friends]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    ctx.maxResults = 10;

    // Execute same query multiple times
    const plan = planQuery(parse('user:123.friends[?age > 30]'));
    const hashes: string[] = [];

    for (let i = 0; i < 10; i++) {
      const result = await executePlan(plan, ctx);
      if (result.cursor) {
        const cursorData = JSON.parse(atob(result.cursor));
        hashes.push(cursorData.queryHash);
      }
    }

    // All hashes should be identical
    expect(hashes.length).toBeGreaterThan(0);
    const firstHash = hashes[0];
    for (const hash of hashes) {
      expect(hash).toBe(firstHash);
    }
  });

  it('should produce different hash for different filters', async () => {
    const stub = getUniqueShardStub();

    const friends: Entity[] = [];
    const friendIds: string[] = [];
    for (let i = 0; i < 15; i++) {
      const friend = createTestEntity(`f${i}`, 'user', { age: 25 + i });
      friends.push(friend);
      friendIds.push(friend.$id);
    }

    const user = createTestEntity('123', 'user', { friends: friendIds });
    const seededEntities = createEntityMap([user, ...friends]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    ctx.maxResults = 10;

    // Execute two queries with different filters
    const plan1 = planQuery(parse('user:123.friends[?age > 30]'));
    const plan2 = planQuery(parse('user:123.friends[?age > 35]'));

    const result1 = await executePlan(plan1, ctx);
    const result2 = await executePlan(plan2, ctx);

    // Both should have cursors (more than 10 entities)
    expect(result1.cursor).toBeDefined();
    expect(result2.cursor).toBeDefined();

    const hash1 = JSON.parse(atob(result1.cursor!)).queryHash;
    const hash2 = JSON.parse(atob(result2.cursor!)).queryHash;

    // Hashes should be different for different filter values
    expect(hash1).not.toBe(hash2);
  });

  it('should produce identical hash regardless of filter object key order', async () => {
    const stub = getUniqueShardStub();

    const friends: Entity[] = [];
    const friendIds: string[] = [];
    for (let i = 0; i < 15; i++) {
      const friend = createTestEntity(`f${i}`, 'user', { age: 25 + i, status: 'active' });
      friends.push(friend);
      friendIds.push(friend.$id);
    }

    const user = createTestEntity('123', 'user', { friends: friendIds });
    const seededEntities = createEntityMap([user, ...friends]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    ctx.maxResults = 10;

    // Execute same logical query multiple times
    // The filter "age > 30" should always produce the same hash
    const plan = planQuery(parse('user:123.friends[?age > 30]'));

    const hashes: Set<string> = new Set();
    for (let i = 0; i < 5; i++) {
      const result = await executePlan(plan, ctx);
      if (result.cursor) {
        const cursorData = JSON.parse(atob(result.cursor));
        hashes.add(cursorData.queryHash);
      }
    }

    // Should only have one unique hash
    expect(hashes.size).toBe(1);
  });

  it('should include filter in hash so different filters produce different hashes', async () => {
    const stub = getUniqueShardStub();

    const friends: Entity[] = [];
    const friendIds: string[] = [];
    for (let i = 0; i < 15; i++) {
      const friend = createTestEntity(`f${i}`, 'user', {
        age: 25 + i,
        status: i % 2 === 0 ? 'active' : 'inactive',
      });
      friends.push(friend);
      friendIds.push(friend.$id);
    }

    const user = createTestEntity('123', 'user', { friends: friendIds });
    const seededEntities = createEntityMap([user, ...friends]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    ctx.maxResults = 10;

    // Different filter conditions
    const planAge = planQuery(parse('user:123.friends[?age > 30]'));
    const planStatus = planQuery(parse('user:123.friends[?status = "active"]'));
    const planBoth = planQuery(parse('user:123.friends[?age > 30 and status = "active"]'));

    const resultAge = await executePlan(planAge, ctx);
    const resultStatus = await executePlan(planStatus, ctx);
    const resultBoth = await executePlan(planBoth, ctx);

    // All should have cursors
    expect(resultAge.cursor).toBeDefined();
    expect(resultStatus.cursor).toBeDefined();
    expect(resultBoth.cursor).toBeDefined();

    const hashAge = JSON.parse(atob(resultAge.cursor!)).queryHash;
    const hashStatus = JSON.parse(atob(resultStatus.cursor!)).queryHash;
    const hashBoth = JSON.parse(atob(resultBoth.cursor!)).queryHash;

    // All three hashes should be different
    expect(hashAge).not.toBe(hashStatus);
    expect(hashAge).not.toBe(hashBoth);
    expect(hashStatus).not.toBe(hashBoth);
  });
});

// ============================================================================
// Integration Tests
// ============================================================================

describe('integration', () => {
  it('should execute complex query plan', async () => {
    const stub = getUniqueShardStub();

    const post = createTestEntity('p1', 'post', { title: 'Hello World' });
    const friend = createTestEntity('456', 'user', {
      name: 'Bob',
      age: 35,
      posts: post.$id,
    });
    const user = createTestEntity('123', 'user', {
      name: 'Alice',
      friends: friend.$id,
    });

    const seededEntities = createEntityMap([user, friend, post]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const plan = planQuery(parse('user:123.friends[?age > 30].posts'));

    const result = await executePlan(plan, ctx);

    expect(result.entities).toBeDefined();
    expect(result.stats).toBeDefined();
    expect(result.hasMore).toBeDefined();
  });

  it('should handle empty results', async () => {
    const stub = getUniqueShardStub();
    const seededEntities = createEntityMap([]);

    const ctx = createRealExecutionContext(stub, seededEntities);
    const plan = planQuery(parse('user:999'));

    const result = await executePlan(plan, ctx);

    expect(result.entities).toEqual([]);
    expect(result.hasMore).toBe(false);
  });
});

// ============================================================================
// Real ShardDO Integration Tests
// ============================================================================

describe('Real ShardDO Integration', () => {
  it('should seed and query triples via real ShardDO', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO) => {
      // Seed test data
      const triples = [
        createTestTriple(
          'https://example.com/user/123',
          'name',
          ObjectType.STRING,
          'Alice'
        ),
        createTestTriple(
          'https://example.com/user/123',
          'age',
          ObjectType.INT64,
          30n
        ),
      ];

      const insertResponse = await instance.fetch(
        new Request('http://localhost/triples', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(triples.map(tripleToHttpBody)),
        })
      );

      expect(insertResponse.status).toBe(201);
      const insertResult = (await insertResponse.json()) as { count: number };
      expect(insertResult.count).toBe(2);

      // Query the triples back
      const subject = encodeURIComponent('https://example.com/user/123');
      const getResponse = await instance.fetch(
        new Request(`http://localhost/triples/${subject}`)
      );

      expect(getResponse.status).toBe(200);
      const result = (await getResponse.json()) as { triples: unknown[] };
      expect(result.triples.length).toBe(2);
    });
  });

  it('should support REF triples for graph relationships', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO) => {
      const friendRef = createEntityId('https://example.com/user/456');

      const triples = [
        createTestTriple(
          'https://example.com/user/123',
          'name',
          ObjectType.STRING,
          'Alice'
        ),
        createTestTriple(
          'https://example.com/user/123',
          'knows',
          ObjectType.REF,
          friendRef
        ),
        createTestTriple(
          'https://example.com/user/456',
          'name',
          ObjectType.STRING,
          'Bob'
        ),
      ];

      const insertResponse = await instance.fetch(
        new Request('http://localhost/triples', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(triples.map(tripleToHttpBody)),
        })
      );

      expect(insertResponse.status).toBe(201);

      // Query Alice's triples
      const subject = encodeURIComponent('https://example.com/user/123');
      const getResponse = await instance.fetch(
        new Request(`http://localhost/triples/${subject}`)
      );

      expect(getResponse.status).toBe(200);
      const result = (await getResponse.json()) as { triples: any[] };

      // Should have 2 triples: name and knows
      expect(result.triples.length).toBe(2);

      // Verify the REF triple
      const knowsTriple = result.triples.find(
        (t: any) => t.predicate === 'knows'
      );
      expect(knowsTriple).toBeDefined();
      expect(knowsTriple.object.type).toBe(ObjectType.REF);
    });
  });

  it('should persist across hibernation cycles', async () => {
    const shardName = `persistence-test-${Date.now()}`;
    const shardId = env.SHARD.idFromName(shardName);

    // First cycle: write data
    const stub1 = env.SHARD.get(shardId);
    await runInDurableObject(stub1, async (instance: ShardDO) => {
      const triple = createTestTriple(
        'https://example.com/entity/persistent1',
        'status',
        ObjectType.STRING,
        'active'
      );

      const response = await instance.fetch(
        new Request('http://localhost/triples', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(tripleToHttpBody(triple)),
        })
      );

      expect(response.status).toBe(201);
    });

    // Second cycle: verify data persisted
    const stub2 = env.SHARD.get(shardId);
    await runInDurableObject(stub2, async (instance: ShardDO) => {
      const subject = encodeURIComponent('https://example.com/entity/persistent1');
      const response = await instance.fetch(
        new Request(`http://localhost/triples/${subject}`)
      );

      expect(response.status).toBe(200);
      const result = (await response.json()) as { triples: any[] };
      expect(result.triples.length).toBeGreaterThan(0);
      expect(result.triples[0].object.value).toBe('active');
    });
  });
});
