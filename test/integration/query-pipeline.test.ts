/**
 * Query Pipeline Integration Tests
 *
 * Exercises the full query pipeline: parse -> plan -> optimize -> execute -> serialize
 *
 * This test ensures regressions in any layer of the query pipeline are caught.
 * Tests various query types:
 * - Simple entity lookups
 * - Filtered traversals
 * - Joined/multi-hop queries
 * - Aggregation-style queries
 * - Expansion queries
 * - Recursive traversals
 *
 * @see P2 issue pocs-qb5m
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';

// Query module imports
import {
  parse,
  stringify,
  planQuery,
  optimizePlan,
  executePlan,
  materializeTriples,
  formatResult,
  type QueryNode,
  type QueryPlan,
  type ExecutionContext,
  type ExecutionResult,
  type FormattedResult,
} from '../../src/query/index';

// Core types
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type EntityId,
} from '../../src/core/types';
import type { Triple, TypedObject } from '../../src/core/triple';
import type { Entity } from '../../src/core/entity';
import { typedObjectToJson } from '../../src/core/type-converters';

// Shard module for data seeding
import { ShardDO } from '../../src/shard/shard-do';
import { createChunkStore } from '../../src/shard/chunk-store';
import { initializeSchema } from '../../src/shard/schema';

// ============================================================================
// Test Helpers
// ============================================================================

let testCounter = 0;

function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`query-pipeline-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

const testNamespace = createNamespace('https://example.com/');
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
  let object: TypedObject;

  switch (objectType) {
    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: value as string };
      break;
    case ObjectType.INT64:
      object = { type: ObjectType.INT64, value: BigInt(value as number) };
      break;
    case ObjectType.FLOAT64:
      object = { type: ObjectType.FLOAT64, value: value as number };
      break;
    case ObjectType.REF:
      object = { type: ObjectType.REF, value: createEntityId(value as string) };
      break;
    case ObjectType.GEO_POINT:
      object = { type: ObjectType.GEO_POINT, value: value as { lat: number; lng: number } };
      break;
    case ObjectType.BOOL:
      object = { type: ObjectType.BOOL, value: value as boolean };
      break;
    case ObjectType.NULL:
    default:
      object = { type: ObjectType.NULL };
      break;
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
 * Write triples to shard via ChunkStore
 */
async function writeTriplesToShard(
  shardStub: DurableObjectStub,
  triples: Triple[]
): Promise<void> {
  await runInDurableObject(shardStub, async (_instance: ShardDO, state: DurableObjectState) => {
    const sql = state.storage.sql;
    initializeSchema(sql);
    const chunkStore = createChunkStore(sql, testNamespace);
    chunkStore.write(triples);
    await chunkStore.forceFlush();
  });
}

/**
 * Query triples from shard via ChunkStore
 */
async function queryTriplesFromShard(
  shardStub: DurableObjectStub,
  subject: EntityId
): Promise<Triple[]> {
  let result: Triple[] = [];
  await runInDurableObject(shardStub, async (_instance: ShardDO, state: DurableObjectState) => {
    const sql = state.storage.sql;
    initializeSchema(sql);
    const chunkStore = createChunkStore(sql, testNamespace);
    result = await chunkStore.query(subject);
  });
  return result;
}

/**
 * Create a test entity from an ID
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
 * Convert triple to JSON-safe format (handles BigInt serialization)
 */
function tripleToJsonSafe(triple: Triple): Record<string, unknown> {
  return {
    subject: triple.subject,
    predicate: triple.predicate,
    object: typedObjectToJson(triple.object),
    timestamp: Number(triple.timestamp),
    txId: triple.txId,
  };
}

/**
 * Custom JSON stringify that handles BigInt
 */
function jsonStringifyWithBigInt(obj: unknown): string {
  return JSON.stringify(obj, (_key, value) =>
    typeof value === 'bigint' ? Number(value) : value
  );
}

/**
 * Create mock execution context with pre-seeded entities
 */
function createMockExecutionContext(
  entities: Map<string, Entity>,
  triples: Triple[] = []
): ExecutionContext {
  return {
    getShardStub: (_shardId: string) => {
      // Create a mock stub that responds to executor requests
      return {
        fetch: async (request: Request): Promise<Response> => {
          const url = new URL(request.url);
          const path = url.pathname;
          const body = await request.json() as Record<string, unknown>;

          if (path === '/lookup') {
            const entityIds = body.entityIds as string[];
            const foundEntities: Entity[] = [];
            const foundTriples: Triple[] = [];

            for (const id of entityIds) {
              const entity = entities.get(id);
              if (entity) {
                foundEntities.push(entity);
              }
              // Add any triples for this entity
              const entityTriples = triples.filter(t => t.subject === id);
              foundTriples.push(...entityTriples);
            }

            // Convert triples to JSON-safe format
            const jsonSafeTriples = foundTriples.map(tripleToJsonSafe);

            return new Response(jsonStringifyWithBigInt({
              entities: foundEntities,
              triples: jsonSafeTriples,
            }));
          }

          if (path === '/traverse') {
            const entityIds = body.entityIds as string[];
            const predicate = body.predicate as string;
            const direction = body.direction as string;
            const foundEntities: Entity[] = [];
            const foundTriples: Triple[] = [];

            for (const id of entityIds) {
              const entity = entities.get(id);
              if (!entity) continue;

              if (direction === 'outgoing') {
                // Find triples where this entity is the subject
                const outgoingTriples = triples.filter(
                  t => t.subject === id && (!predicate || t.predicate === predicate)
                );
                for (const triple of outgoingTriples) {
                  if (triple.object.type === ObjectType.REF) {
                    const refId = triple.object.value as string;
                    const refEntity = entities.get(refId);
                    if (refEntity && !foundEntities.some(e => e.$id === refId)) {
                      foundEntities.push(refEntity);
                    }
                  }
                  foundTriples.push(triple);
                }
              } else if (direction === 'incoming') {
                // Find triples where this entity is the object (REF)
                const incomingTriples = triples.filter(
                  t => t.object.type === ObjectType.REF &&
                       t.object.value === id &&
                       (!predicate || t.predicate === predicate)
                );
                for (const triple of incomingTriples) {
                  const sourceEntity = entities.get(triple.subject);
                  if (sourceEntity && !foundEntities.some(e => e.$id === triple.subject)) {
                    foundEntities.push(sourceEntity);
                  }
                  foundTriples.push(triple);
                }
              }
            }

            // Convert triples to JSON-safe format
            const jsonSafeTriples = foundTriples.map(tripleToJsonSafe);

            return new Response(jsonStringifyWithBigInt({
              entities: foundEntities,
              triples: jsonSafeTriples,
            }));
          }

          if (path === '/expand') {
            const entityIds = body.entityIds as string[];
            const fields = body.fields as string[];
            const foundEntities: Entity[] = [];
            const foundTriples: Triple[] = [];

            for (const id of entityIds) {
              const entity = entities.get(id);
              if (entity) {
                // Project only requested fields
                const projected: Record<string, unknown> = {
                  $id: entity.$id,
                  $type: entity.$type,
                  $context: entity.$context,
                  _namespace: entity._namespace,
                  _localId: entity._localId,
                };
                for (const field of fields) {
                  if (field in entity) {
                    projected[field] = entity[field];
                  }
                }
                foundEntities.push(projected as Entity);
              }
            }

            return new Response(jsonStringifyWithBigInt({
              entities: foundEntities,
              triples: foundTriples,
            }));
          }

          return new Response(JSON.stringify({ entities: [], triples: [] }));
        },
      } as unknown as DurableObjectStub;
    },
    maxResults: 100,
    timeout: 30000,
  };
}

// ============================================================================
// Test Data Setup
// ============================================================================

/**
 * Create a test social network graph
 */
function createSocialNetworkData(): { entities: Map<string, Entity>; triples: Triple[] } {
  const entities = new Map<string, Entity>();
  const triples: Triple[] = [];

  // Create users
  const alice = createTestEntity('https://example.com/user/alice', 'User', {
    name: 'Alice Smith',
    age: 30,
    active: true,
  });
  const bob = createTestEntity('https://example.com/user/bob', 'User', {
    name: 'Bob Jones',
    age: 25,
    active: true,
  });
  const charlie = createTestEntity('https://example.com/user/charlie', 'User', {
    name: 'Charlie Brown',
    age: 35,
    active: false,
  });
  const david = createTestEntity('https://example.com/user/david', 'User', {
    name: 'David Wilson',
    age: 28,
    active: true,
  });

  // Create posts
  const post1 = createTestEntity('https://example.com/post/1', 'Post', {
    title: 'Hello World',
    content: 'My first post about graph databases',
    likes: 42,
  });
  const post2 = createTestEntity('https://example.com/post/2', 'Post', {
    title: 'Graph Traversals',
    content: 'How to efficiently traverse graphs',
    likes: 100,
  });

  // Add to map (both full URL and short format)
  [alice, bob, charlie, david, post1, post2].forEach(entity => {
    entities.set(entity.$id, entity);
    // Also add by namespace:id format
    const shortId = entity.$id.replace('https://example.com/', '').replace('/', ':');
    entities.set(shortId, entity);
  });

  // Create relationship triples
  // Alice follows Bob and Charlie
  triples.push(
    createTestTriple('https://example.com/user/alice', 'follows', ObjectType.REF, 'https://example.com/user/bob'),
    createTestTriple('https://example.com/user/alice', 'follows', ObjectType.REF, 'https://example.com/user/charlie'),
    createTestTriple('https://example.com/user/alice', 'name', ObjectType.STRING, 'Alice Smith'),
    createTestTriple('https://example.com/user/alice', 'age', ObjectType.INT64, 30),
  );

  // Bob follows Charlie and David
  triples.push(
    createTestTriple('https://example.com/user/bob', 'follows', ObjectType.REF, 'https://example.com/user/charlie'),
    createTestTriple('https://example.com/user/bob', 'follows', ObjectType.REF, 'https://example.com/user/david'),
    createTestTriple('https://example.com/user/bob', 'name', ObjectType.STRING, 'Bob Jones'),
    createTestTriple('https://example.com/user/bob', 'age', ObjectType.INT64, 25),
  );

  // Charlie follows David
  triples.push(
    createTestTriple('https://example.com/user/charlie', 'follows', ObjectType.REF, 'https://example.com/user/david'),
    createTestTriple('https://example.com/user/charlie', 'name', ObjectType.STRING, 'Charlie Brown'),
    createTestTriple('https://example.com/user/charlie', 'age', ObjectType.INT64, 35),
    createTestTriple('https://example.com/user/charlie', 'active', ObjectType.BOOL, false),
  );

  // David data
  triples.push(
    createTestTriple('https://example.com/user/david', 'name', ObjectType.STRING, 'David Wilson'),
    createTestTriple('https://example.com/user/david', 'age', ObjectType.INT64, 28),
  );

  // Posts authored by users
  triples.push(
    createTestTriple('https://example.com/user/alice', 'posts', ObjectType.REF, 'https://example.com/post/1'),
    createTestTriple('https://example.com/user/bob', 'posts', ObjectType.REF, 'https://example.com/post/2'),
  );

  // Post data
  triples.push(
    createTestTriple('https://example.com/post/1', 'title', ObjectType.STRING, 'Hello World'),
    createTestTriple('https://example.com/post/1', 'likes', ObjectType.INT64, 42),
    createTestTriple('https://example.com/post/2', 'title', ObjectType.STRING, 'Graph Traversals'),
    createTestTriple('https://example.com/post/2', 'likes', ObjectType.INT64, 100),
  );

  // Likes (reverse relationship)
  triples.push(
    createTestTriple('https://example.com/user/bob', 'liked', ObjectType.REF, 'https://example.com/post/1'),
    createTestTriple('https://example.com/user/charlie', 'liked', ObjectType.REF, 'https://example.com/post/1'),
    createTestTriple('https://example.com/user/david', 'liked', ObjectType.REF, 'https://example.com/post/2'),
  );

  return { entities, triples };
}

// ============================================================================
// Test Suite: Simple Entity Lookup Pipeline
// ============================================================================

describe('Query Pipeline: Simple Entity Lookup', () => {
  it('should execute full pipeline for simple entity lookup', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Step 1: Parse
    const query = 'user:alice';
    const ast = parse(query);

    expect(ast).toBeDefined();
    expect(ast.type).toBe('entity');
    if (ast.type === 'entity') {
      expect(ast.namespace).toBe('user');
      expect(ast.id).toBe('alice');
    }

    // Step 2: Plan
    const plan = planQuery(ast);

    expect(plan).toBeDefined();
    expect(plan.steps.length).toBeGreaterThan(0);
    expect(plan.steps[0]?.type).toBe('lookup');

    // Step 3: Optimize
    const optimizedPlan = optimizePlan(plan);

    expect(optimizedPlan).toBeDefined();
    expect(optimizedPlan.steps.length).toBeGreaterThan(0);

    // Step 4: Execute
    const result = await executePlan(optimizedPlan, ctx);

    expect(result).toBeDefined();
    expect(result.entities).toBeDefined();
    expect(result.stats).toBeDefined();
    expect(result.stats.shardQueries).toBeGreaterThan(0);

    // Step 5: Format/Serialize
    const formatted = formatResult(result);

    expect(formatted).toBeDefined();
    expect(formatted.data).toBeDefined();
    expect(formatted.meta).toBeDefined();
    expect(formatted.meta?.duration).toBeGreaterThanOrEqual(0);
  });

  it('should roundtrip query through parse and stringify', () => {
    const originalQuery = 'user:123';
    const ast = parse(originalQuery);
    const stringified = stringify(ast);

    expect(stringified).toBe(originalQuery);
  });

  it('should estimate cost correctly for simple lookup', () => {
    const ast = parse('user:alice');
    const plan = planQuery(ast);

    expect(plan.estimatedCost).toBeGreaterThan(0);
    // Simple lookup should have low cost
    expect(plan.estimatedCost).toBeLessThan(10);
  });
});

// ============================================================================
// Test Suite: Filtered Query Pipeline
// ============================================================================

describe('Query Pipeline: Filtered Queries', () => {
  it('should execute full pipeline for filtered traversal', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.follows[?age > 25]
    const query = 'user:alice.follows[?age > 25]';
    const ast = parse(query);

    expect(ast).toBeDefined();
    expect(ast.type).toBe('filter');

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'filter')).toBe(true);

    const optimizedPlan = optimizePlan(plan);
    const result = await executePlan(optimizedPlan, ctx);

    expect(result).toBeDefined();
    expect(result.stats.shardQueries).toBeGreaterThan(0);

    const formatted = formatResult(result);
    expect(formatted.data).toBeDefined();
  });

  it('should handle AND filter conditions', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query with AND condition
    const query = 'user:alice.follows[?age > 20 and active = true]';
    const ast = parse(query);

    expect(ast.type).toBe('filter');
    if (ast.type === 'filter') {
      expect(ast.condition.type).toBe('logical');
    }

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });

  it('should handle OR filter conditions', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query with OR condition
    const query = 'user:alice.follows[?age < 26 or age > 34]';
    const ast = parse(query);

    expect(ast.type).toBe('filter');

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });

  it('should handle equality filter', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    const query = 'user:alice.follows[?name = "Bob Jones"]';
    const ast = parse(query);

    expect(ast.type).toBe('filter');

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Multi-Hop/Joined Query Pipeline
// ============================================================================

describe('Query Pipeline: Multi-Hop Traversals', () => {
  it('should execute full pipeline for 2-hop traversal', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.follows.follows (friends of friends)
    const query = 'user:alice.follows.follows';
    const ast = parse(query);

    expect(ast).toBeDefined();
    expect(ast.type).toBe('property');

    const plan = planQuery(ast);
    // Should have lookup + 2 traverse steps
    const traverseSteps = plan.steps.filter(s => s.type === 'traverse');
    expect(traverseSteps.length).toBe(2);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();

    const formatted = formatResult(result);
    expect(formatted.data).toBeDefined();
  });

  it('should execute full pipeline for 3-hop traversal', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.follows.follows.follows
    const query = 'user:alice.follows.follows.follows';
    const ast = parse(query);

    const plan = planQuery(ast);
    const traverseSteps = plan.steps.filter(s => s.type === 'traverse');
    expect(traverseSteps.length).toBe(3);

    // Higher cost for multi-hop
    expect(plan.estimatedCost).toBeGreaterThan(5);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });

  it('should handle property chain with different predicates', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.posts (get Alice's posts)
    const query = 'user:alice.posts';
    const ast = parse(query);

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'traverse' && s.predicate === 'posts')).toBe(true);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Reverse Traversal Pipeline
// ============================================================================

describe('Query Pipeline: Reverse Traversals', () => {
  it('should execute full pipeline for reverse traversal', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: post:1 <- liked (find who liked post 1)
    const query = 'post:1 <- liked';
    const ast = parse(query);

    expect(ast).toBeDefined();
    expect(ast.type).toBe('reverse');
    if (ast.type === 'reverse') {
      expect(ast.predicate).toBe('liked');
    }

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'reverse')).toBe(true);

    // Reverse traversal has higher cost
    const reverseStep = plan.steps.find(s => s.type === 'reverse');
    expect(reverseStep).toBeDefined();

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });

  it('should handle reverse traversal with filter', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: post:1 <- liked[?active = true]
    const query = 'post:1 <- liked[?active = true]';
    const ast = parse(query);

    expect(ast.type).toBe('filter');

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'reverse')).toBe(true);
    expect(plan.steps.some(s => s.type === 'filter')).toBe(true);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Expansion Query Pipeline
// ============================================================================

describe('Query Pipeline: Expansion Queries', () => {
  it('should execute full pipeline for expansion query', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice { name, age }
    const query = 'user:alice { name, age }';
    const ast = parse(query);

    expect(ast).toBeDefined();
    expect(ast.type).toBe('expand');
    if (ast.type === 'expand') {
      expect(ast.fields.length).toBe(2);
      expect(ast.fields.map(f => f.name)).toContain('name');
      expect(ast.fields.map(f => f.name)).toContain('age');
    }

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'expand')).toBe(true);

    const expandStep = plan.steps.find(s => s.type === 'expand');
    expect(expandStep?.fields).toEqual(['name', 'age']);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();

    const formatted = formatResult(result, { fields: ['name', 'age'] });
    expect(formatted.data).toBeDefined();
  });

  it('should handle nested expansion', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice { name, follows { name } }
    const query = 'user:alice { name, follows { name } }';
    const ast = parse(query);

    expect(ast.type).toBe('expand');
    if (ast.type === 'expand') {
      const followsField = ast.fields.find(f => f.name === 'follows');
      expect(followsField?.nested).toBeDefined();
      expect(followsField?.nested?.length).toBe(1);
    }

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });

  it('should handle expansion with traversal', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.follows { name, age, active }
    const query = 'user:alice.follows { name, age, active }';
    const ast = parse(query);

    expect(ast.type).toBe('expand');

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'traverse')).toBe(true);
    expect(plan.steps.some(s => s.type === 'expand')).toBe(true);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Recursive Query Pipeline
// ============================================================================

describe('Query Pipeline: Recursive Traversals', () => {
  it('should execute full pipeline for bounded recursion', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.follows*[depth <= 2]
    const query = 'user:alice.follows*[depth <= 2]';
    const ast = parse(query);

    expect(ast).toBeDefined();
    expect(ast.type).toBe('recurse');
    if (ast.type === 'recurse') {
      expect(ast.maxDepth).toBe(2);
    }

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'recurse')).toBe(true);

    const recurseStep = plan.steps.find(s => s.type === 'recurse');
    expect(recurseStep?.maxDepth).toBe(2);

    // Recursion has higher cost
    expect(plan.estimatedCost).toBeGreaterThan(5);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });

  it('should handle unbounded recursion with star operator', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Query: user:alice.follows* (unbounded)
    const query = 'user:alice.follows*';
    const ast = parse(query);

    expect(ast.type).toBe('recurse');
    if (ast.type === 'recurse') {
      expect(ast.maxDepth).toBeUndefined();
    }

    const plan = planQuery(ast);

    // Unbounded recursion should have very high cost
    expect(plan.estimatedCost).toBeGreaterThan(10);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });

  it('should handle recursion with depth limit of 3', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    const query = 'user:alice.follows*[depth <= 3]';
    const ast = parse(query);

    expect(ast.type).toBe('recurse');
    if (ast.type === 'recurse') {
      expect(ast.maxDepth).toBe(3);
    }

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Materialization and Serialization
// ============================================================================

describe('Query Pipeline: Materialization', () => {
  it('should materialize triples to entities correctly', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/test', 'name', ObjectType.STRING, 'Test User'),
      createTestTriple('https://example.com/user/test', 'age', ObjectType.INT64, 25),
      createTestTriple('https://example.com/user/test', 'active', ObjectType.BOOL, true),
    ];

    const entities = materializeTriples(triples);

    expect(entities.length).toBe(1);
    expect(entities[0]?.$id).toBe('https://example.com/user/test');
    expect(entities[0]?.name).toBe('Test User');
    // INT64 values are stored as bigint, so compare accordingly
    expect(entities[0]?.age).toBe(25n);
    expect(entities[0]?.active).toBe(true);
  });

  it('should handle multiple entities in materialization', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', ObjectType.STRING, 'User 1'),
      createTestTriple('https://example.com/user/2', 'name', ObjectType.STRING, 'User 2'),
      createTestTriple('https://example.com/user/1', 'age', ObjectType.INT64, 20),
      createTestTriple('https://example.com/user/2', 'age', ObjectType.INT64, 30),
    ];

    const entities = materializeTriples(triples);

    expect(entities.length).toBe(2);
    expect(entities.find(e => e.$id === 'https://example.com/user/1')?.name).toBe('User 1');
    expect(entities.find(e => e.$id === 'https://example.com/user/2')?.name).toBe('User 2');
  });

  it('should format result with pagination info', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    const ast = parse('user:alice');
    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    const formatted = formatResult(result);

    expect(formatted.data).toBeDefined();
    expect(formatted.meta).toBeDefined();
    expect(formatted.meta?.duration).toBeGreaterThanOrEqual(0);
    expect(formatted.meta?.shardQueries).toBeGreaterThan(0);
  });

  it('should handle empty results', () => {
    const result: ExecutionResult = {
      entities: [],
      triples: [],
      hasMore: false,
      stats: {
        shardQueries: 1,
        entitiesScanned: 0,
        durationMs: 10,
      },
    };

    const formatted = formatResult(result);

    expect(formatted.data).toEqual([]);
    expect(formatted.pagination).toBeUndefined();
  });
});

// ============================================================================
// Test Suite: Plan Caching
// ============================================================================

describe('Query Pipeline: Plan Caching', () => {
  it('should generate consistent cache keys for identical queries', () => {
    const ast1 = parse('user:alice');
    const ast2 = parse('user:alice');

    const plan1 = planQuery(ast1);
    const plan2 = planQuery(ast2);

    // Cache keys should be identical for same query
    if (plan1.canCache && plan2.canCache) {
      expect(plan1.cacheKey).toBe(plan2.cacheKey);
    }
  });

  it('should generate different cache keys for different queries', () => {
    const ast1 = parse('user:alice');
    const ast2 = parse('user:bob');

    const plan1 = planQuery(ast1);
    const plan2 = planQuery(ast2);

    if (plan1.canCache && plan2.canCache) {
      expect(plan1.cacheKey).not.toBe(plan2.cacheKey);
    }
  });

  it('should mark simple queries as cacheable', () => {
    const ast = parse('user:alice');
    const plan = planQuery(ast);

    expect(plan.canCache).toBe(true);
  });
});

// ============================================================================
// Test Suite: Plan Optimization
// ============================================================================

describe('Query Pipeline: Plan Optimization', () => {
  it('should merge adjacent lookups to same shard', () => {
    // Create a plan with multiple lookups to same shard
    const plan: QueryPlan = {
      steps: [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['id1'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['id2'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['id3'] },
      ],
      shards: [],
      estimatedCost: 3,
      canCache: false,
    };

    const optimized = optimizePlan(plan);

    // Should merge into single lookup
    const lookupSteps = optimized.steps.filter(s => s.type === 'lookup');
    expect(lookupSteps.length).toBe(1);
    expect(lookupSteps[0]?.entityIds?.length).toBe(3);
  });

  it('should not merge lookups to different shards', () => {
    const plan: QueryPlan = {
      steps: [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['id1'] },
        { type: 'lookup', shardId: 'shard-2', entityIds: ['id2'] },
      ],
      shards: [],
      estimatedCost: 2,
      canCache: false,
    };

    const optimized = optimizePlan(plan);

    const lookupSteps = optimized.steps.filter(s => s.type === 'lookup');
    expect(lookupSteps.length).toBe(2);
  });

  it('should recalculate cost after optimization', () => {
    const plan: QueryPlan = {
      steps: [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['id1'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['id2'] },
      ],
      shards: [],
      estimatedCost: 2,
      canCache: false,
    };

    const optimized = optimizePlan(plan);

    // Cost should be recalculated based on merged lookups
    expect(optimized.estimatedCost).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Error Handling
// ============================================================================

describe('Query Pipeline: Error Handling', () => {
  it('should throw ParseError for invalid query syntax', () => {
    expect(() => parse('')).toThrow();
    expect(() => parse('   ')).toThrow();
  });

  it('should throw ParseError for malformed entity reference', () => {
    expect(() => parse('user')).toThrow(); // Missing :id
  });

  it('should throw ParseError for unclosed brackets', () => {
    expect(() => parse('user:alice[?age > 30')).toThrow();
  });

  it('should throw ParseError for unclosed braces', () => {
    expect(() => parse('user:alice { name')).toThrow();
  });

  it('should handle execution timeout gracefully', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);
    ctx.timeout = 1; // 1ms timeout (will trigger immediately)

    const ast = parse('user:alice.follows.follows.follows');
    const plan = planQuery(ast);

    // Should complete without throwing, even with timeout
    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();
  });
});

// ============================================================================
// Test Suite: Complex Query Scenarios
// ============================================================================

describe('Query Pipeline: Complex Scenarios', () => {
  it('should handle combined traversal with filter and expansion', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    // Complex query: get friends older than 25 with specific fields
    const query = 'user:alice.follows[?age > 25] { name, age }';
    const ast = parse(query);

    expect(ast.type).toBe('expand');

    const plan = planQuery(ast);
    expect(plan.steps.some(s => s.type === 'traverse')).toBe(true);
    expect(plan.steps.some(s => s.type === 'filter')).toBe(true);
    expect(plan.steps.some(s => s.type === 'expand')).toBe(true);

    const result = await executePlan(plan, ctx);
    expect(result).toBeDefined();

    const formatted = formatResult(result);
    expect(formatted.data).toBeDefined();
  });

  it('should handle string comparison in filter', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    const query = 'user:alice.follows[?name = "Bob Jones"]';
    const ast = parse(query);

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });

  it('should handle boolean comparison in filter', async () => {
    const { entities, triples } = createSocialNetworkData();
    const ctx = createMockExecutionContext(entities, triples);

    const query = 'user:alice.follows[?active = true]';
    const ast = parse(query);

    const plan = planQuery(ast);
    const result = await executePlan(plan, ctx);

    expect(result).toBeDefined();
  });

  it('should execute end-to-end pipeline with real shard data', async () => {
    const shardStub = getUniqueShardStub();

    // Seed data
    const triples = [
      createTestTriple('https://example.com/user/e2e', 'name', ObjectType.STRING, 'E2E Test User'),
      createTestTriple('https://example.com/user/e2e', 'age', ObjectType.INT64, 42),
    ];

    await writeTriplesToShard(shardStub, triples);

    // Verify data was written
    const readTriples = await queryTriplesFromShard(
      shardStub,
      createEntityId('https://example.com/user/e2e')
    );
    expect(readTriples.length).toBeGreaterThan(0);

    // Now test the pipeline with this data
    const ast = parse('user:e2e');
    const plan = planQuery(ast);
    const optimizedPlan = optimizePlan(plan);

    // Verify plan structure
    expect(optimizedPlan.steps.length).toBeGreaterThan(0);
    expect(optimizedPlan.steps[0]?.type).toBe('lookup');

    // Materialize the triples
    const entities = materializeTriples(readTriples);
    expect(entities.length).toBe(1);
    expect(entities[0]?.name).toBe('E2E Test User');
    // INT64 values are stored as bigint
    expect(entities[0]?.age).toBe(42n);
  });
});
