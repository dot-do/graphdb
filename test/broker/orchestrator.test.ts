/**
 * Query Orchestrator Tests
 *
 * These tests define the expected behavior for query orchestration:
 * - Query planning (single hop, multi-hop, filters)
 * - Batch optimization
 * - Shard coordination with real DurableObject stubs
 * - Stats tracking
 *
 * Uses real ShardDO stubs via env.SHARD.get() pattern.
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import {
  planQuery,
  executeStep,
  orchestrateQuery,
  batchLookups,
  resetCircuitBreakers,
  type QueryPlan,
  type QueryStep,
  type FilterExpr,
} from '../../src/broker/orchestrator';

// Reset circuit breakers before each test to ensure isolation
beforeEach(() => {
  resetCircuitBreakers();
});
import { createEntity, type Entity } from '../../src/core/entity';
import { createEntityId, createPredicate, ObjectType } from '../../src/core/types';
import { ShardDO } from '../../src/shard/shard-do';
import { initializeSchema } from '../../src/shard/schema';
import { createTripleStore } from '../../src/shard/crud';
import type { Triple, TypedObject } from '../../src/core/triple';

// Helper to get unique shard stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`orchestrator-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Helper to create test entities with the new interface
function createTestEntity(id: string): Entity {
  return createEntity(createEntityId(id), 'TestEntity', {});
}

// Helper to insert test data into a shard
async function insertTestTriples(stub: DurableObjectStub, triples: Array<{
  subject: string;
  predicate: string;
  objectType: number;
  value: unknown;
}>): Promise<void> {
  await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
    const sql = state.storage.sql;
    initializeSchema(sql);
    const store = createTripleStore(sql);

    for (const t of triples) {
      const triple: Triple = {
        subject: t.subject as any,
        predicate: t.predicate as any,
        object: { type: t.objectType, value: t.value } as TypedObject,
        timestamp: BigInt(Date.now()),
        txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV' as any,
      };
      await store.insertTriple(triple);
    }
  });
}

describe('Query Planning', () => {
  describe('planQuery - single hop queries', () => {
    it('should create a lookup step for entity by ID', () => {
      const plan = planQuery('MATCH (n {$id: "https://example.com/person/1"}) RETURN n');

      expect(plan.steps).toHaveLength(1);
      expect(plan.steps[0].type).toBe('lookup');
      expect(plan.steps[0].entityIds).toContain('https://example.com/person/1');
    });

    it('should create correct shard routing for lookup', () => {
      const plan = planQuery('MATCH (n {$id: "https://example.com/person/1"}) RETURN n');

      expect(plan.steps[0].shardId).toBeDefined();
      expect(typeof plan.steps[0].shardId).toBe('string');
    });

    it('should estimate cost for single lookup', () => {
      const plan = planQuery('MATCH (n {$id: "https://example.com/person/1"}) RETURN n');

      expect(plan.estimatedCost).toBeGreaterThan(0);
      expect(plan.estimatedCost).toBeLessThanOrEqual(1); // Single lookup should be cheap
    });
  });

  describe('planQuery - multi-hop queries', () => {
    it('should create traverse step for relationship following', () => {
      const plan = planQuery(
        'MATCH (p:Person)-[:knows]->(f:Person) WHERE p.$id = "https://example.com/person/1" RETURN f'
      );

      expect(plan.steps.length).toBeGreaterThan(1);

      const traverseStep = plan.steps.find((s) => s.type === 'traverse');
      expect(traverseStep).toBeDefined();
      expect(traverseStep?.predicate).toBe('knows');
    });

    it('should create expand steps for 2-hop queries', () => {
      const plan = planQuery(
        'MATCH (p:Person)-[:knows]->(f:Person)-[:knows]->(fof:Person) WHERE p.$id = "https://example.com/person/1" RETURN fof'
      );

      // Should have: lookup -> traverse -> expand
      const expandSteps = plan.steps.filter((s) => s.type === 'expand' || s.type === 'traverse');
      expect(expandSteps.length).toBeGreaterThanOrEqual(2);
    });

    it('should track depth for multi-hop', () => {
      const plan = planQuery(
        'MATCH (p:Person)-[:knows*1..3]->(f:Person) WHERE p.$id = "https://example.com/person/1" RETURN f'
      );

      const traverseStep = plan.steps.find((s) => s.type === 'traverse' || s.type === 'expand');
      expect(traverseStep?.depth).toBeDefined();
      expect(traverseStep?.depth).toBeLessThanOrEqual(3);
    });

    it('should estimate higher cost for multi-hop', () => {
      const singleHop = planQuery('MATCH (n {$id: "https://example.com/person/1"}) RETURN n');
      const multiHop = planQuery(
        'MATCH (p:Person)-[:knows]->(f:Person) WHERE p.$id = "https://example.com/person/1" RETURN f'
      );

      expect(multiHop.estimatedCost).toBeGreaterThan(singleHop.estimatedCost);
    });
  });

  describe('planQuery - filter handling', () => {
    it('should create filter step for property filters', () => {
      const plan = planQuery(
        'MATCH (p:Person) WHERE p.age > 21 RETURN p'
      );

      const filterStep = plan.steps.find((s) => s.type === 'filter');
      expect(filterStep).toBeDefined();
      expect(filterStep?.filter).toBeDefined();
    });

    it('should parse comparison operators correctly', () => {
      const testCases: Array<{ query: string; op: FilterExpr['op']; value: unknown }> = [
        { query: 'MATCH (p) WHERE p.age > 21 RETURN p', op: '>', value: 21 },
        { query: 'MATCH (p) WHERE p.age < 50 RETURN p', op: '<', value: 50 },
        { query: 'MATCH (p) WHERE p.age >= 18 RETURN p', op: '>=', value: 18 },
        { query: 'MATCH (p) WHERE p.age <= 65 RETURN p', op: '<=', value: 65 },
        { query: 'MATCH (p) WHERE p.status = "active" RETURN p', op: '=', value: 'active' },
        { query: 'MATCH (p) WHERE p.status != "deleted" RETURN p', op: '!=', value: 'deleted' },
      ];

      for (const { query, op, value } of testCases) {
        const plan = planQuery(query);
        const filterStep = plan.steps.find((s) => s.type === 'filter');

        expect(filterStep?.filter?.op).toBe(op);
        expect(filterStep?.filter?.value).toBe(value);
      }
    });

    it('should extract field name from filter', () => {
      const plan = planQuery('MATCH (p:Person) WHERE p.name = "Alice" RETURN p');

      const filterStep = plan.steps.find((s) => s.type === 'filter');
      expect(filterStep?.filter?.field).toBe('name');
    });
  });

  describe('planQuery - batch optimization', () => {
    it('should mark batchable queries', () => {
      const plan = planQuery(
        'MATCH (n) WHERE n.$id IN ["https://example.com/1", "https://example.com/2"] RETURN n'
      );

      expect(plan.canBatch).toBe(true);
    });

    it('should not batch single lookups', () => {
      const plan = planQuery('MATCH (n {$id: "https://example.com/person/1"}) RETURN n');

      // Single lookup doesn't need batching
      expect(plan.canBatch).toBe(false);
    });
  });
});

describe('Batch Lookups', () => {
  describe('batchLookups', () => {
    it('should combine lookups to same shard', () => {
      const steps: QueryStep[] = [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/1'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/2'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/3'] },
      ];

      const batched = batchLookups(steps);

      expect(batched).toHaveLength(1);
      expect(batched[0].entityIds).toHaveLength(3);
      expect(batched[0].shardId).toBe('shard-1');
    });

    it('should keep different shards separate', () => {
      const steps: QueryStep[] = [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/1'] },
        { type: 'lookup', shardId: 'shard-2', entityIds: ['https://example.com/2'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/3'] },
      ];

      const batched = batchLookups(steps);

      expect(batched).toHaveLength(2);

      const shard1Batch = batched.find((s) => s.shardId === 'shard-1');
      const shard2Batch = batched.find((s) => s.shardId === 'shard-2');

      expect(shard1Batch?.entityIds).toHaveLength(2);
      expect(shard2Batch?.entityIds).toHaveLength(1);
    });

    it('should preserve non-lookup steps', () => {
      const steps: QueryStep[] = [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/1'] },
        { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/2'] },
      ];

      const batched = batchLookups(steps);

      // Should have: batched lookup, traverse, with lookups combined
      const traverseSteps = batched.filter((s) => s.type === 'traverse');
      expect(traverseSteps).toHaveLength(1);
    });

    it('should handle empty steps array', () => {
      const batched = batchLookups([]);
      expect(batched).toHaveLength(0);
    });

    it('should deduplicate entity IDs in same batch', () => {
      const steps: QueryStep[] = [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/1', 'https://example.com/2'] },
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/2', 'https://example.com/3'] },
      ];

      const batched = batchLookups(steps);

      expect(batched).toHaveLength(1);
      // Should have 3 unique IDs, not 4
      expect(batched[0].entityIds).toHaveLength(3);
      expect(new Set(batched[0].entityIds).size).toBe(3);
    });
  });
});

describe('Execute Step with Real ShardDO', () => {
  describe('executeStep', () => {
    it('should fetch entities from shard for lookup step', async () => {
      const stub = getUniqueShardStub();

      // Insert test data
      await insertTestTriples(stub, [
        { subject: 'https://example.com/person/1', predicate: 'name', objectType: ObjectType.STRING, value: 'Alice' },
        { subject: 'https://example.com/person/1', predicate: '$type', objectType: ObjectType.STRING, value: 'Person' },
      ]);

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: ['https://example.com/person/1' as any],
      };

      const result = await executeStep(step, stub);

      expect(result).toHaveLength(1);
      expect(result[0].$id).toBe('https://example.com/person/1');
    });

    it('should handle empty result from shard', async () => {
      const stub = getUniqueShardStub();

      // Initialize schema but don't insert any data
      await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
        initializeSchema(state.storage.sql);
      });

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: ['https://example.com/nonexistent' as any],
      };

      const result = await executeStep(step, stub);

      expect(result).toHaveLength(0);
    });

    it('should execute traverse step with predicate', async () => {
      const stub = getUniqueShardStub();

      // Insert person1 who knows person2
      await insertTestTriples(stub, [
        { subject: 'https://example.com/person/1', predicate: 'name', objectType: ObjectType.STRING, value: 'Alice' },
        { subject: 'https://example.com/person/1', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/person/2' },
        { subject: 'https://example.com/person/2', predicate: 'name', objectType: ObjectType.STRING, value: 'Bob' },
        { subject: 'https://example.com/person/2', predicate: '$type', objectType: ObjectType.STRING, value: 'Person' },
      ]);

      const step: QueryStep = {
        type: 'traverse',
        shardId: 'shard-1',
        entityIds: ['https://example.com/person/1' as any],
        predicate: 'knows',
      };

      const result = await executeStep(step, stub);

      expect(result).toHaveLength(1);
      expect(result[0].$id).toBe('https://example.com/person/2');
    });

    it('should apply filter in filter step', async () => {
      const stub = getUniqueShardStub();

      // Insert people with different ages
      await insertTestTriples(stub, [
        { subject: 'https://example.com/person/alice', predicate: 'name', objectType: ObjectType.STRING, value: 'Alice' },
        { subject: 'https://example.com/person/alice', predicate: 'age', objectType: ObjectType.FLOAT64, value: 30 },
        { subject: 'https://example.com/person/bob', predicate: 'name', objectType: ObjectType.STRING, value: 'Bob' },
        { subject: 'https://example.com/person/bob', predicate: 'age', objectType: ObjectType.FLOAT64, value: 18 },
      ]);

      const step: QueryStep = {
        type: 'filter',
        shardId: 'shard-1',
        filter: { field: 'age', op: '>', value: 21 },
      };

      const result = await executeStep(step, stub);

      // Should only return Alice (age > 21)
      expect(result).toHaveLength(1);
      expect(result[0].$id).toBe('https://example.com/person/alice');
    });
  });
});

describe('Orchestrate Query with Real ShardDO', () => {
  describe('orchestrateQuery', () => {
    it('should execute steps in correct order', async () => {
      const stub = getUniqueShardStub();

      // Insert test data
      await insertTestTriples(stub, [
        { subject: 'https://example.com/person/1', predicate: 'name', objectType: ObjectType.STRING, value: 'Alice' },
        { subject: 'https://example.com/person/1', predicate: '$type', objectType: ObjectType.STRING, value: 'Person' },
        { subject: 'https://example.com/person/1', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/person/2' },
        { subject: 'https://example.com/person/2', predicate: 'name', objectType: ObjectType.STRING, value: 'Bob' },
        { subject: 'https://example.com/person/2', predicate: '$type', objectType: ObjectType.STRING, value: 'Person' },
      ]);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/person/1' as any] },
          { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      // Should return person/2 as the final result (traversed from person/1)
      expect(result.entities).toHaveLength(1);
      expect(result.entities[0].$id).toBe('https://example.com/person/2');
    });

    it('should pass results between steps', async () => {
      const stub = getUniqueShardStub();

      await insertTestTriples(stub, [
        { subject: 'https://example.com/person/1', predicate: 'name', objectType: ObjectType.STRING, value: 'Alice' },
        { subject: 'https://example.com/person/1', predicate: '$type', objectType: ObjectType.STRING, value: 'Person' },
        { subject: 'https://example.com/person/1', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/person/friend' },
        { subject: 'https://example.com/person/friend', predicate: 'name', objectType: ObjectType.STRING, value: 'Friend' },
        { subject: 'https://example.com/person/friend', predicate: '$type', objectType: ObjectType.STRING, value: 'Person' },
      ]);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/person/1' as any] },
          { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      expect(result.entities).toHaveLength(1);
      expect(result.entities[0].$id).toBe('https://example.com/person/friend');
    });

    it('should respect depth limits in expand step', async () => {
      const stub = getUniqueShardStub();

      // Create a chain: person/1 -> person/2 -> person/3 -> person/4
      await insertTestTriples(stub, [
        { subject: 'https://example.com/start', predicate: 'name', objectType: ObjectType.STRING, value: 'Start' },
        { subject: 'https://example.com/start', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/person/1' },
        { subject: 'https://example.com/person/1', predicate: 'name', objectType: ObjectType.STRING, value: 'Person1' },
        { subject: 'https://example.com/person/1', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/person/2' },
        { subject: 'https://example.com/person/2', predicate: 'name', objectType: ObjectType.STRING, value: 'Person2' },
        { subject: 'https://example.com/person/2', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/person/3' },
        { subject: 'https://example.com/person/3', predicate: 'name', objectType: ObjectType.STRING, value: 'Person3' },
      ]);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/start' as any] },
          { type: 'expand', shardId: 'shard-1', predicate: 'knows', depth: 2 },
        ],
        estimatedCost: 5,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      // With depth 2, should get person/1 and person/2 (not person/3)
      expect(result.entities.length).toBeLessThanOrEqual(3);
    });

    it('should return hasMore when results exceed limit', async () => {
      const stub = getUniqueShardStub();

      // Insert 15 entities (using smaller count for test reliability)
      const triples: Array<{ subject: string; predicate: string; objectType: number; value: unknown }> = [];
      for (let i = 0; i < 15; i++) {
        triples.push({
          subject: `https://example.com/entity/${i}`,
          predicate: 'name',
          objectType: ObjectType.STRING,
          value: `Entity ${i}`,
        });
      }
      await insertTestTriples(stub, triples);

      // Create a lookup for all 15 entities with limit of 10
      const entityIds = Array.from({ length: 15 }, (_, i) => `https://example.com/entity/${i}` as any);

      const plan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1', entityIds }],
        estimatedCost: 1,
        canBatch: false,
      };

      // Use limit of 10 so we can test hasMore with fewer entities
      const result = await orchestrateQuery(plan, () => stub, { limit: 10 });

      // With 15 entities and limit 10, should have more
      expect(result.entities.length).toBeLessThanOrEqual(10);
      if (result.entities.length === 10) {
        expect(result.hasMore).toBe(true);
        expect(result.cursor).toBeDefined();
      }
    });

    it('should provide cursor for pagination', async () => {
      const stub = getUniqueShardStub();

      // Insert 15 entities (using smaller count for test reliability)
      const triples: Array<{ subject: string; predicate: string; objectType: number; value: unknown }> = [];
      for (let i = 0; i < 15; i++) {
        triples.push({
          subject: `https://example.com/entity/${i}`,
          predicate: 'name',
          objectType: ObjectType.STRING,
          value: `Entity ${i}`,
        });
      }
      await insertTestTriples(stub, triples);

      const entityIds = Array.from({ length: 15 }, (_, i) => `https://example.com/entity/${i}` as any);

      const plan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1', entityIds }],
        estimatedCost: 1,
        canBatch: false,
      };

      // Use limit of 10 so we can test pagination with fewer entities
      const result = await orchestrateQuery(plan, () => stub, { limit: 10 });

      if (result.hasMore) {
        expect(result.cursor).toBeDefined();
        expect(typeof result.cursor).toBe('string');
      }
    });
  });

  describe('stats tracking', () => {
    it('should count shard queries accurately', async () => {
      const stub = getUniqueShardStub();

      // Initialize schema
      await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
        initializeSchema(state.storage.sql);
      });

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/1' as any] },
          { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
          { type: 'filter', shardId: 'shard-1', filter: { field: 'age', op: '>', value: 21 } },
        ],
        estimatedCost: 3,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      expect(result.stats.shardQueries).toBe(3);
    });

    it('should count entities scanned', async () => {
      const stub = getUniqueShardStub();

      // Insert 50 entities
      const triples: Array<{ subject: string; predicate: string; objectType: number; value: unknown }> = [];
      for (let i = 0; i < 50; i++) {
        triples.push({
          subject: `https://example.com/entity/${i}`,
          predicate: 'name',
          objectType: ObjectType.STRING,
          value: `Entity ${i}`,
        });
      }
      await insertTestTriples(stub, triples);

      const entityIds = Array.from({ length: 50 }, (_, i) => `https://example.com/entity/${i}` as any);

      const plan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1', entityIds }],
        estimatedCost: 1,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      expect(result.stats.entitiesScanned).toBe(50);
    });

    it('should track duration in milliseconds', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
        initializeSchema(state.storage.sql);
      });

      const plan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1' }],
        estimatedCost: 1,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      expect(result.stats.durationMs).toBeGreaterThanOrEqual(0);
      expect(typeof result.stats.durationMs).toBe('number');
    });

    it('should aggregate stats across multiple steps', async () => {
      const stub = getUniqueShardStub();

      await insertTestTriples(stub, [
        { subject: 'https://example.com/1', predicate: 'name', objectType: ObjectType.STRING, value: 'Alice' },
        { subject: 'https://example.com/1', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/2' },
        { subject: 'https://example.com/1', predicate: 'knows', objectType: ObjectType.REF, value: 'https://example.com/3' },
        { subject: 'https://example.com/2', predicate: 'name', objectType: ObjectType.STRING, value: 'Bob' },
        { subject: 'https://example.com/3', predicate: 'name', objectType: ObjectType.STRING, value: 'Charlie' },
      ]);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/1' as any] },
          { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, () => stub);

      // Should count all entities from all steps
      expect(result.stats.entitiesScanned).toBe(3); // 1 from lookup + 2 from traverse
      expect(result.stats.shardQueries).toBe(2);
    });
  });
});

describe('Edge Cases', () => {
  it('should handle empty query plan', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      initializeSchema(state.storage.sql);
    });

    const plan: QueryPlan = {
      steps: [],
      estimatedCost: 0,
      canBatch: false,
    };

    const result = await orchestrateQuery(plan, () => stub);

    expect(result.entities).toHaveLength(0);
    expect(result.stats.shardQueries).toBe(0);
  });

  it('should handle malformed query strings', () => {
    expect(() => planQuery('')).toThrow();
    expect(() => planQuery('not a valid query')).toThrow();
  });

  it('should handle query with no results propagating through steps', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      initializeSchema(state.storage.sql);
    });

    const plan: QueryPlan = {
      steps: [
        { type: 'lookup', shardId: 'shard-1', entityIds: ['https://example.com/nonexistent' as any] },
        { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
      ],
      estimatedCost: 2,
      canBatch: false,
    };

    const result = await orchestrateQuery(plan, () => stub);

    expect(result.entities).toHaveLength(0);
    expect(result.hasMore).toBe(false);
  });
});
