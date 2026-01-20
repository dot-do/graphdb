/**
 * ShardDO SQLiteIndexStore Integration Tests (TDD RED Phase)
 *
 * These tests verify that SQLiteIndexStore is properly wired into ShardDO
 * so that write operations automatically populate the appropriate indexes.
 *
 * Tests cover:
 * 1. Write Path Integration - Writing triples also populates indexes
 * 2. Query Integration - Queries use the appropriate indexes
 * 3. Lifecycle Tests - Indexes persist across hibernation and restarts
 *
 * NOTE: These tests are expected to FAIL because ShardDO doesn't have
 * SQLiteIndexStore wired in yet. This is the RED phase of TDD.
 *
 * @see src/index/sqlite-index-store.ts for the SQLiteIndexStore implementation
 * @see src/shard/shard-do.ts for the ShardDO implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { SQLiteIndexStore } from '../../src/index/sqlite-index-store.js';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types.js';
import type { EntityId, Predicate, TransactionId } from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';
import { typedObjectToJson } from '../../src/core/type-converters.js';

// ============================================================================
// Test Helpers
// ============================================================================

// Helper to get fresh DO stubs for each test
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-index-integration-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Valid ULID for transactions
const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV';
const VALID_TX_ID_2 = '01ARZ3NDEKTSV4RRFFQ69G5FAW';

/**
 * Create a test triple with the given parameters
 */
function createTestTriple(
  subjectSuffix: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
  txIdSuffix = VALID_TX_ID
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
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
}

/**
 * Convert triple to JSON-safe format for HTTP API
 * Uses the official typedObjectToJson converter for consistency
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

// ============================================================================
// 1. WRITE PATH INTEGRATION TESTS
// ============================================================================

describe('Write Path Integration', () => {
  describe('POS Index Population', () => {
    it('should populate POS index when writing a triple via HTTP POST', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write a STRING triple via HTTP
        const triple = createTestTriple('person1', 'name', ObjectType.STRING, 'John Doe');
        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );

        expect(response.status).toBe(201);

        // Verify the POS index was populated
        // This test will FAIL until SQLiteIndexStore is wired into ShardDO
        const indexStore = new SQLiteIndexStore(sql);
        const subjects = await indexStore.getByPredicateValue('name' as Predicate, 'John Doe');

        expect(subjects).toContain(triple.subject);
      });
    });

    it('should populate POS index for INT64 values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write an INT64 triple
        const triple = createTestTriple('person1', 'age', ObjectType.INT64, 30n);
        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );

        expect(response.status).toBe(201);

        // Verify the POS index was populated
        const indexStore = new SQLiteIndexStore(sql);
        const subjects = await indexStore.getByPredicateValue('age' as Predicate, 30);

        expect(subjects).toContain(triple.subject);
      });
    });
  });

  describe('OSP Index Population (Reverse Lookup)', () => {
    it('should populate OSP index when writing a REF triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write a REF triple (person1 knows person2)
        const targetId = createEntityId('https://example.com/entity/person2');
        const triple = createTestTriple('person1', 'knows', ObjectType.REF, targetId);

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );

        expect(response.status).toBe(201);

        // Verify the OSP index was populated (reverse lookup)
        // This test will FAIL until SQLiteIndexStore is wired into ShardDO
        const indexStore = new SQLiteIndexStore(sql);
        const referencingSubjects = await indexStore.getReferencesTo(targetId);

        expect(referencingSubjects).toContain(triple.subject);
      });
    });

    it('should populate OSP index for multiple REF triples pointing to same target', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        const targetId = createEntityId('https://example.com/entity/company1');

        // Write two REF triples pointing to the same target
        const triple1 = createTestTriple('employee1', 'worksAt', ObjectType.REF, targetId);
        const triple2 = createTestTriple('employee2', 'worksAt', ObjectType.REF, targetId);

        const triples = [tripleToHttpBody(triple1), tripleToHttpBody(triple2)];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples),
          })
        );

        expect(response.status).toBe(201);

        // Verify the OSP index contains both subjects
        const indexStore = new SQLiteIndexStore(sql);
        const referencingSubjects = await indexStore.getReferencesTo(targetId);

        expect(referencingSubjects).toContain(triple1.subject);
        expect(referencingSubjects).toContain(triple2.subject);
        expect(referencingSubjects.length).toBeGreaterThanOrEqual(2);
      });
    });
  });

  describe('FTS Index Population (Full-Text Search)', () => {
    it('should populate FTS index when writing a STRING triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write a STRING triple with searchable content
        const triple = createTestTriple('article1', 'content', ObjectType.STRING, 'The quick brown fox jumps over the lazy dog');

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );

        expect(response.status).toBe(201);

        // Verify the FTS index was populated
        // This test will FAIL until SQLiteIndexStore is wired into ShardDO
        const indexStore = new SQLiteIndexStore(sql);
        const results = await indexStore.search('quick brown fox');

        expect(results.length).toBeGreaterThan(0);
        expect(results.map(r => r.entityId)).toContain(triple.subject);
      });
    });

    it('should support searching in specific predicate via FTS', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write STRING triples with different predicates
        const triple1 = createTestTriple('doc1', 'title', ObjectType.STRING, 'Introduction to GraphDB');
        const triple2 = createTestTriple('doc1', 'body', ObjectType.STRING, 'GraphDB is a powerful database');

        const triples = [tripleToHttpBody(triple1), tripleToHttpBody(triple2)];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples),
          })
        );

        expect(response.status).toBe(201);

        // Verify the FTS index allows predicate-specific search
        const indexStore = new SQLiteIndexStore(sql);
        const titleResults = await indexStore.searchInPredicate('title' as Predicate, 'GraphDB');
        const bodyResults = await indexStore.searchInPredicate('body' as Predicate, 'GraphDB');

        expect(titleResults.length).toBeGreaterThan(0);
        expect(bodyResults.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Geo Index Population', () => {
    it('should populate geo index when writing a GEO_POINT triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write a GEO_POINT triple (San Francisco coordinates)
        const location = { lat: 37.7749, lng: -122.4194 };
        const triple = createTestTriple('place1', 'location', ObjectType.GEO_POINT, location);

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );

        expect(response.status).toBe(201);

        // Verify the geo index was populated
        // This test will FAIL until SQLiteIndexStore is wired into ShardDO
        const indexStore = new SQLiteIndexStore(sql);

        // Query a bounding box that includes San Francisco
        const results = await indexStore.queryGeoBBox(
          37.7, // minLat
          -122.5, // minLng
          37.9, // maxLat
          -122.3 // maxLng
        );

        expect(results).toContain(triple.subject);
      });
    });

    it('should find entities within radius using geo index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write multiple GEO_POINT triples
        const sf = { lat: 37.7749, lng: -122.4194 };
        const oakland = { lat: 37.8044, lng: -122.2712 };
        const losAngeles = { lat: 34.0522, lng: -118.2437 };

        const triple1 = createTestTriple('sf', 'location', ObjectType.GEO_POINT, sf);
        const triple2 = createTestTriple('oakland', 'location', ObjectType.GEO_POINT, oakland);
        const triple3 = createTestTriple('la', 'location', ObjectType.GEO_POINT, losAngeles);

        const triples = [tripleToHttpBody(triple1), tripleToHttpBody(triple2), tripleToHttpBody(triple3)];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples),
          })
        );

        expect(response.status).toBe(201);

        // Query within 50km of SF (should include Oakland but not LA)
        const indexStore = new SQLiteIndexStore(sql);
        const results = await indexStore.queryGeoRadius(37.7749, -122.4194, 50);

        expect(results).toContain(triple1.subject);
        // Oakland is about 15km from SF, should be included
        expect(results).toContain(triple2.subject);
        // LA is about 560km from SF, should NOT be included
        expect(results).not.toContain(triple3.subject);
      });
    });
  });

  describe('Batch Write Integration', () => {
    it('should update all indexes on batch write', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Create a batch of diverse triples
        const refTarget = createEntityId('https://example.com/entity/company1');
        const triples = [
          createTestTriple('emp1', 'name', ObjectType.STRING, 'Alice Smith'),
          createTestTriple('emp1', 'age', ObjectType.INT64, 28n),
          createTestTriple('emp1', 'worksAt', ObjectType.REF, refTarget),
          createTestTriple('emp1', 'office', ObjectType.GEO_POINT, { lat: 37.7749, lng: -122.4194 }),
        ];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        expect(response.status).toBe(201);
        const result = await response.json() as { count: number };
        expect(result.count).toBe(4);

        // Verify all indexes were populated
        const indexStore = new SQLiteIndexStore(sql);

        // POS index
        const byName = await indexStore.getByPredicateValue('name' as Predicate, 'Alice Smith');
        expect(byName).toContain(triples[0].subject);

        // OSP index
        const refsToCompany = await indexStore.getReferencesTo(refTarget);
        expect(refsToCompany).toContain(triples[0].subject);

        // FTS index
        const ftsResults = await indexStore.search('Alice Smith');
        expect(ftsResults.map(r => r.entityId)).toContain(triples[0].subject);

        // Geo index
        const geoResults = await indexStore.queryGeoBBox(37.7, -122.5, 37.9, -122.3);
        expect(geoResults).toContain(triples[0].subject);
      });
    });
  });

  describe('Index Stats', () => {
    it('should reflect written data in index stats', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Get initial stats
        const indexStore = new SQLiteIndexStore(sql);
        const initialStats = indexStore.getStats();

        // Write some triples
        const triples = [
          createTestTriple('e1', 'name', ObjectType.STRING, 'Entity 1'),
          createTestTriple('e2', 'name', ObjectType.STRING, 'Entity 2'),
          createTestTriple('e1', 'knows', ObjectType.REF, createEntityId('https://example.com/entity/e2')),
          createTestTriple('e1', 'location', ObjectType.GEO_POINT, { lat: 40.7128, lng: -74.0060 }),
        ];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        expect(response.status).toBe(201);

        // Get updated stats
        const updatedStats = indexStore.getStats();

        // Verify stats increased
        // This test will FAIL until SQLiteIndexStore is wired into ShardDO
        expect(updatedStats.posEntryCount).toBeGreaterThan(initialStats.posEntryCount);
        expect(updatedStats.ospEntryCount).toBeGreaterThan(initialStats.ospEntryCount);
        expect(updatedStats.ftsDocumentCount).toBeGreaterThan(initialStats.ftsDocumentCount);
        expect(updatedStats.geoCellCount).toBeGreaterThan(initialStats.geoCellCount);
      });
    });
  });
});

// ============================================================================
// 2. QUERY INTEGRATION TESTS
// ============================================================================

describe('Query Integration', () => {
  describe('POS Index Queries', () => {
    it('should query by predicate value using POS index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // First, write some triples
        const triples = [
          createTestTriple('person1', 'status', ObjectType.STRING, 'active'),
          createTestTriple('person2', 'status', ObjectType.STRING, 'inactive'),
          createTestTriple('person3', 'status', ObjectType.STRING, 'active'),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Query using the index
        // This test will FAIL until index query endpoints are added to ShardDO
        const indexStore = new SQLiteIndexStore(sql);
        const activeEntities = await indexStore.getByPredicateValue('status' as Predicate, 'active');

        expect(activeEntities.length).toBe(2);
        expect(activeEntities).toContain(triples[0].subject);
        expect(activeEntities).toContain(triples[2].subject);
        expect(activeEntities).not.toContain(triples[1].subject);
      });
    });

    it('should query by predicate only using POS index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write triples with various predicates
        const triples = [
          createTestTriple('e1', 'email', ObjectType.STRING, 'e1@example.com'),
          createTestTriple('e2', 'email', ObjectType.STRING, 'e2@example.com'),
          createTestTriple('e3', 'phone', ObjectType.STRING, '555-1234'),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Query all entities with 'email' predicate
        const indexStore = new SQLiteIndexStore(sql);
        const entitiesWithEmail = await indexStore.getByPredicate('email' as Predicate);

        expect(entitiesWithEmail.length).toBe(2);
        expect(entitiesWithEmail).toContain(triples[0].subject);
        expect(entitiesWithEmail).toContain(triples[1].subject);
      });
    });
  });

  describe('OSP Index Queries (References)', () => {
    it('should query for references using OSP index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        const popularEntity = createEntityId('https://example.com/entity/popular');

        // Write REF triples pointing to the popular entity
        const triples = [
          createTestTriple('follower1', 'follows', ObjectType.REF, popularEntity),
          createTestTriple('follower2', 'follows', ObjectType.REF, popularEntity),
          createTestTriple('follower3', 'likes', ObjectType.REF, popularEntity),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Query all entities that reference the popular entity
        const indexStore = new SQLiteIndexStore(sql);
        const referencingEntities = await indexStore.getReferencesTo(popularEntity);

        expect(referencingEntities.length).toBe(3);
        expect(referencingEntities).toContain(triples[0].subject);
        expect(referencingEntities).toContain(triples[1].subject);
        expect(referencingEntities).toContain(triples[2].subject);
      });
    });

    it('should query references by specific predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        const targetEntity = createEntityId('https://example.com/entity/target');

        // Write REF triples with different predicates
        const triples = [
          createTestTriple('src1', 'cites', ObjectType.REF, targetEntity),
          createTestTriple('src2', 'cites', ObjectType.REF, targetEntity),
          createTestTriple('src3', 'mentions', ObjectType.REF, targetEntity),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Query only 'cites' references
        const indexStore = new SQLiteIndexStore(sql);
        const citingEntities = await indexStore.getReferencesToByPredicate(targetEntity, 'cites' as Predicate);

        expect(citingEntities.length).toBe(2);
        expect(citingEntities).toContain(triples[0].subject);
        expect(citingEntities).toContain(triples[1].subject);
        expect(citingEntities).not.toContain(triples[2].subject);
      });
    });
  });

  describe('FTS Index Queries', () => {
    it('should perform full-text search using FTS index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write searchable content
        const triples = [
          createTestTriple('doc1', 'content', ObjectType.STRING, 'GraphDB provides distributed graph storage'),
          createTestTriple('doc2', 'content', ObjectType.STRING, 'Cloudflare Workers enable edge computing'),
          createTestTriple('doc3', 'content', ObjectType.STRING, 'Graph databases use edge traversal'),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Search for 'graph'
        const indexStore = new SQLiteIndexStore(sql);
        const results = await indexStore.search('graph');

        // Should find doc1 and doc3 (both contain 'graph')
        expect(results.length).toBe(2);
        const entityIds = results.map(r => r.entityId);
        expect(entityIds).toContain(triples[0].subject);
        expect(entityIds).toContain(triples[2].subject);
        expect(entityIds).not.toContain(triples[1].subject);
      });
    });

    it('should return relevance scores from FTS search', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write content with varying keyword density
        const triples = [
          createTestTriple('doc1', 'content', ObjectType.STRING, 'GraphDB GraphDB GraphDB'),
          createTestTriple('doc2', 'content', ObjectType.STRING, 'This mentions GraphDB once'),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Search and verify scores
        const indexStore = new SQLiteIndexStore(sql);
        const results = await indexStore.search('GraphDB');

        expect(results.length).toBe(2);

        // Results should have scores (BM25)
        for (const result of results) {
          expect(typeof result.score).toBe('number');
          expect(result.score).toBeGreaterThan(0);
        }
      });
    });
  });

  describe('Geo Index Queries', () => {
    it('should query geo bbox using geo index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Write locations in different regions
        const triples = [
          createTestTriple('nyc', 'location', ObjectType.GEO_POINT, { lat: 40.7128, lng: -74.0060 }),
          createTestTriple('boston', 'location', ObjectType.GEO_POINT, { lat: 42.3601, lng: -71.0589 }),
          createTestTriple('london', 'location', ObjectType.GEO_POINT, { lat: 51.5074, lng: -0.1278 }),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        // Query US East Coast bounding box
        const indexStore = new SQLiteIndexStore(sql);
        const usEastCoast = await indexStore.queryGeoBBox(
          38.0, // minLat
          -76.0, // minLng
          44.0, // maxLat
          -70.0 // maxLng
        );

        // Should find NYC and Boston, not London
        expect(usEastCoast).toContain(triples[0].subject);
        expect(usEastCoast).toContain(triples[1].subject);
        expect(usEastCoast).not.toContain(triples[2].subject);
      });
    });
  });
});

// ============================================================================
// 3. LIFECYCLE TESTS
// ============================================================================

describe('Lifecycle Tests', () => {
  describe('Hibernation Persistence', () => {
    it('should survive DO hibernation (write, hibernate, wake, verify)', async () => {
      const stub = getUniqueShardStub();

      // Phase 1: Write data
      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const triples = [
          createTestTriple('persistent1', 'name', ObjectType.STRING, 'Persisted Entity'),
          createTestTriple('persistent1', 'category', ObjectType.STRING, 'test'),
        ];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        expect(response.status).toBe(201);
      });

      // Simulate hibernation by accessing the stub again
      // (vitest-pool-workers simulates hibernation between runInDurableObject calls)

      // Phase 2: Wake and verify indexes are still populated
      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Verify POS index survived hibernation
        const byCategory = await indexStore.getByPredicateValue('category' as Predicate, 'test');
        expect(byCategory).toContain(createEntityId('https://example.com/entity/persistent1'));

        // Verify FTS index survived hibernation
        const ftsResults = await indexStore.search('Persisted Entity');
        expect(ftsResults.length).toBeGreaterThan(0);
        expect(ftsResults.map(r => r.entityId)).toContain(
          createEntityId('https://example.com/entity/persistent1')
        );
      });
    });

    it('should maintain index consistency after multiple hibernation cycles', async () => {
      const stub = getUniqueShardStub();
      const expectedEntityId = createEntityId('https://example.com/entity/multi1');

      // Cycle 1: Initial write
      await runInDurableObject(stub, async (instance: ShardDO, _state: DurableObjectState) => {
        const triple = createTestTriple('multi1', 'version', ObjectType.INT64, 1n);
        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );
      });

      // Cycle 2: Update
      await runInDurableObject(stub, async (instance: ShardDO, _state: DurableObjectState) => {
        const triple = createTestTriple('multi1', 'version', ObjectType.INT64, 2n);
        triple.txId = createTransactionId(VALID_TX_ID_2);
        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(tripleToHttpBody(triple)),
          })
        );
      });

      // Cycle 3: Verify
      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index should reflect both writes
        const results = await indexStore.getByPredicate('version' as Predicate);
        expect(results).toContain(expectedEntityId);
      });
    });
  });

  describe('Restart Persistence', () => {
    it('should persist indexes across restarts', async () => {
      // Use a fixed name to simulate restart
      const shardName = `restart-test-${Date.now()}`;
      const shardId = env.SHARD.idFromName(shardName);

      // Phase 1: Write data with first stub
      const stub1 = env.SHARD.get(shardId);
      await runInDurableObject(stub1, async (instance: ShardDO, state: DurableObjectState) => {
        const refTarget = createEntityId('https://example.com/entity/restart-target');
        const triples = [
          createTestTriple('restart1', 'title', ObjectType.STRING, 'Restart Test Document'),
          createTestTriple('restart1', 'linksTo', ObjectType.REF, refTarget),
          createTestTriple('restart1', 'coords', ObjectType.GEO_POINT, { lat: 48.8566, lng: 2.3522 }),
        ];

        const response = await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        expect(response.status).toBe(201);
      });

      // Phase 2: Get a new stub (simulating restart) and verify
      const stub2 = env.SHARD.get(shardId);
      await runInDurableObject(stub2, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const expectedEntityId = createEntityId('https://example.com/entity/restart1');
        const refTarget = createEntityId('https://example.com/entity/restart-target');

        // Verify POS index persisted
        const byTitle = await indexStore.getByPredicateValue('title' as Predicate, 'Restart Test Document');
        expect(byTitle).toContain(expectedEntityId);

        // Verify OSP index persisted
        const refs = await indexStore.getReferencesTo(refTarget);
        expect(refs).toContain(expectedEntityId);

        // Verify FTS index persisted
        const ftsResults = await indexStore.search('Restart Test');
        expect(ftsResults.map(r => r.entityId)).toContain(expectedEntityId);

        // Verify Geo index persisted (Paris coordinates)
        const geoResults = await indexStore.queryGeoBBox(48.0, 2.0, 49.0, 3.0);
        expect(geoResults).toContain(expectedEntityId);
      });
    });
  });

  describe('Index Stats Persistence', () => {
    it('should persist index stats across hibernation', async () => {
      const stub = getUniqueShardStub();

      let initialStats: { posEntryCount: number; ospEntryCount: number; ftsDocumentCount: number };

      // Phase 1: Write data and capture stats
      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        const triples = [
          createTestTriple('stats1', 'field', ObjectType.STRING, 'value1'),
          createTestTriple('stats2', 'field', ObjectType.STRING, 'value2'),
        ];

        await instance.fetch(
          new Request('https://shard-do/triples', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(triples.map(tripleToHttpBody)),
          })
        );

        const indexStore = new SQLiteIndexStore(sql);
        initialStats = indexStore.getStats();
      });

      // Phase 2: Verify stats persisted
      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);
        const currentStats = indexStore.getStats();

        // Stats should match what was captured before hibernation
        expect(currentStats.posEntryCount).toBe(initialStats.posEntryCount);
        expect(currentStats.ftsDocumentCount).toBe(initialStats.ftsDocumentCount);
      });
    });
  });
});
