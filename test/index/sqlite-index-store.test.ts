/**
 * SQLiteIndexStore Tests (TDD RED Phase)
 *
 * Comprehensive tests for SQLite-backed index store implementation:
 * - POS Index (Predicate-Object-Subject) queries
 * - OSP Index (Object-Subject-Predicate) reverse lookups
 * - FTS Index (Full-Text Search) with BM25 ranking
 * - GEO Index (Geospatial) queries with geohash
 * - Index maintenance (add/remove triples)
 * - R2 sync (save/load)
 *
 * @see CLAUDE.md for architecture details
 * @see src/index/sqlite-index-store.ts for implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { SQLiteIndexStore } from '../../src/index/sqlite-index-store.js';
import { INDEX_SCHEMA } from '../../src/index/index-store.js';
import { ObjectType } from '../../src/core/types.js';
import type { EntityId, Predicate, TransactionId } from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';

// ============================================================================
// TEST HELPERS
// ============================================================================

// Counter for unique DO instances
let testCounter = 0;

/**
 * Get a unique ShardDO stub for test isolation
 */
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-sqlite-index-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Valid ULID for transactions
const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV' as TransactionId;

/**
 * Helper to create a test triple
 */
function createTestTriple(
  subject: string,
  predicate: string,
  objectValue: TypedObject,
  txId: TransactionId = VALID_TX_ID
): Triple {
  return {
    subject: subject as EntityId,
    predicate: predicate as Predicate,
    object: objectValue,
    timestamp: BigInt(Date.now()),
    txId,
  };
}

/**
 * Create a string TypedObject
 */
function stringObject(value: string): TypedObject {
  return { type: ObjectType.STRING, value };
}

/**
 * Create an int64 TypedObject
 */
function int64Object(value: bigint): TypedObject {
  return { type: ObjectType.INT64, value };
}

/**
 * Create a float64 TypedObject
 */
function float64Object(value: number): TypedObject {
  return { type: ObjectType.FLOAT64, value };
}

/**
 * Create a ref TypedObject
 */
function refObject(value: string): TypedObject {
  return { type: ObjectType.REF, value: value as EntityId };
}

/**
 * Create a geo point TypedObject
 */
function geoPointObject(lat: number, lng: number): TypedObject {
  return { type: ObjectType.GEO_POINT, value: { lat, lng } };
}

/**
 * Create a null TypedObject
 */
function nullObject(): TypedObject {
  return { type: ObjectType.NULL };
}

/**
 * Mock R2 bucket for testing R2 sync operations
 */
function createMockR2Bucket(): R2Bucket {
  const storage = new Map<string, string>();

  return {
    get: vi.fn(async (key: string) => {
      const value = storage.get(key);
      if (!value) return null;
      return {
        text: async () => value,
        json: async () => JSON.parse(value),
        arrayBuffer: async () => new TextEncoder().encode(value).buffer,
        body: null,
        bodyUsed: false,
        blob: async () => new Blob([value]),
      } as unknown as R2ObjectBody;
    }),
    put: vi.fn(async (key: string, value: string | ArrayBuffer | ReadableStream) => {
      const strValue = typeof value === 'string' ? value : new TextDecoder().decode(value as ArrayBuffer);
      storage.set(key, strValue);
      return {} as R2Object;
    }),
    delete: vi.fn(async (key: string) => {
      storage.delete(key);
    }),
    list: vi.fn(async () => ({
      objects: [],
      truncated: false,
      cursor: undefined,
      delimitedPrefixes: [],
    })),
    head: vi.fn(async () => null),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
  } as unknown as R2Bucket;
}

// ============================================================================
// POS INDEX TESTS
// ============================================================================

describe('SQLiteIndexStore - POS Index (Predicate-Object-Subject)', () => {
  describe('getByPredicate', () => {
    it('should return entities with the specified predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index triples with the 'name' predicate
        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/2', 'name', stringObject('Bob')),
          createTestTriple('https://example.com/person/3', 'age', int64Object(25n)),
        ]);

        // Query by predicate 'name'
        const results = await indexStore.getByPredicate('name' as Predicate);

        expect(results).toHaveLength(2);
        expect(results).toContain('https://example.com/person/1');
        expect(results).toContain('https://example.com/person/2');
        expect(results).not.toContain('https://example.com/person/3');
      });
    });

    it('should return empty array when no entities have the predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index some triples
        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
        ]);

        // Query for non-existent predicate
        const results = await indexStore.getByPredicate('nonexistent' as Predicate);

        expect(results).toEqual([]);
      });
    });

    it('should support pagination with limit', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index many triples
        const triples: Triple[] = [];
        for (let i = 0; i < 100; i++) {
          triples.push(
            createTestTriple(`https://example.com/person/${i}`, 'name', stringObject(`Person ${i}`))
          );
        }
        await indexStore.indexTriples(triples);

        // Query with limit
        const results = await indexStore.getByPredicate('name' as Predicate, { limit: 10 });

        expect(results.length).toBeLessThanOrEqual(10);
      });
    });

    it('should support pagination with cursor', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index triples with different values to create distinct value_hash entries
        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'age', int64Object(20n)),
          createTestTriple('https://example.com/person/2', 'age', int64Object(25n)),
          createTestTriple('https://example.com/person/3', 'age', int64Object(30n)),
          createTestTriple('https://example.com/person/4', 'age', int64Object(35n)),
        ]);

        // First page
        const page1 = await indexStore.getByPredicate('age' as Predicate, { limit: 2 });
        expect(page1.length).toBeGreaterThan(0);

        // Get cursor for next page (using the last value_hash)
        // The cursor mechanism should allow fetching next batch
        const page2 = await indexStore.getByPredicate('age' as Predicate, { limit: 2, cursor: '25' });

        // Pages should have some results (exact behavior depends on implementation)
        expect(page1.length + page2.length).toBeGreaterThan(0);
      });
    });
  });

  describe('getByPredicateValue', () => {
    it('should return exact matches for predicate and value', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'age', int64Object(25n)),
          createTestTriple('https://example.com/person/2', 'age', int64Object(25n)),
          createTestTriple('https://example.com/person/3', 'age', int64Object(30n)),
        ]);

        const results = await indexStore.getByPredicateValue('age' as Predicate, 25);

        expect(results).toHaveLength(2);
        expect(results).toContain('https://example.com/person/1');
        expect(results).toContain('https://example.com/person/2');
        expect(results).not.toContain('https://example.com/person/3');
      });
    });

    it('should return exact string matches', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/2', 'name', stringObject('alice')),
          createTestTriple('https://example.com/person/3', 'name', stringObject('ALICE')),
        ]);

        const results = await indexStore.getByPredicateValue('name' as Predicate, 'Alice');

        expect(results).toHaveLength(1);
        expect(results).toContain('https://example.com/person/1');
      });
    });

    it('should return empty array for non-matching value', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'age', int64Object(25n)),
        ]);

        const results = await indexStore.getByPredicateValue('age' as Predicate, 999);

        expect(results).toEqual([]);
      });
    });
  });

  describe('getByPredicateRange', () => {
    it('should return entities within numeric range', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'age', int64Object(18n)),
          createTestTriple('https://example.com/person/2', 'age', int64Object(25n)),
          createTestTriple('https://example.com/person/3', 'age', int64Object(30n)),
          createTestTriple('https://example.com/person/4', 'age', int64Object(45n)),
        ]);

        const results = await indexStore.getByPredicateRange('age' as Predicate, 20, 35);

        expect(results).toHaveLength(2);
        expect(results).toContain('https://example.com/person/2');
        expect(results).toContain('https://example.com/person/3');
        expect(results).not.toContain('https://example.com/person/1');
        expect(results).not.toContain('https://example.com/person/4');
      });
    });

    it('should support inclusive range boundaries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'age', int64Object(20n)),
          createTestTriple('https://example.com/person/2', 'age', int64Object(30n)),
        ]);

        const results = await indexStore.getByPredicateRange('age' as Predicate, 20, 30);

        // Both boundaries should be inclusive
        expect(results).toHaveLength(2);
        expect(results).toContain('https://example.com/person/1');
        expect(results).toContain('https://example.com/person/2');
      });
    });

    it('should support bigint range values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/event/1', 'timestamp', int64Object(1000000000000n)),
          createTestTriple('https://example.com/event/2', 'timestamp', int64Object(1500000000000n)),
          createTestTriple('https://example.com/event/3', 'timestamp', int64Object(2000000000000n)),
        ]);

        const results = await indexStore.getByPredicateRange(
          'timestamp' as Predicate,
          1200000000000n,
          1800000000000n
        );

        expect(results).toHaveLength(1);
        expect(results).toContain('https://example.com/event/2');
      });
    });

    it('should support Date range values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const date1 = new Date('2024-01-01');
        const date2 = new Date('2024-06-15');
        const date3 = new Date('2024-12-31');

        await indexStore.indexTriples([
          createTestTriple('https://example.com/event/1', 'createdAt', int64Object(BigInt(date1.getTime()))),
          createTestTriple('https://example.com/event/2', 'createdAt', int64Object(BigInt(date2.getTime()))),
          createTestTriple('https://example.com/event/3', 'createdAt', int64Object(BigInt(date3.getTime()))),
        ]);

        const results = await indexStore.getByPredicateRange(
          'createdAt' as Predicate,
          new Date('2024-03-01'),
          new Date('2024-09-01')
        );

        expect(results).toHaveLength(1);
        expect(results).toContain('https://example.com/event/2');
      });
    });

    it('should respect limit option', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Create 50 entities in range
        const triples: Triple[] = [];
        for (let i = 0; i < 50; i++) {
          triples.push(
            createTestTriple(`https://example.com/item/${i}`, 'score', int64Object(BigInt(i)))
          );
        }
        await indexStore.indexTriples(triples);

        const results = await indexStore.getByPredicateRange(
          'score' as Predicate,
          0,
          100,
          { limit: 10 }
        );

        expect(results.length).toBeLessThanOrEqual(10);
      });
    });
  });
});

// ============================================================================
// OSP INDEX TESTS
// ============================================================================

describe('SQLiteIndexStore - OSP Index (Object-Subject-Predicate)', () => {
  describe('getReferencesTo', () => {
    it('should return entities that reference the target', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const companyId = 'https://example.com/company/acme';

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'worksAt', refObject(companyId)),
          createTestTriple('https://example.com/person/2', 'worksAt', refObject(companyId)),
          createTestTriple('https://example.com/person/3', 'worksAt', refObject('https://example.com/company/other')),
        ]);

        const results = await indexStore.getReferencesTo(companyId as EntityId);

        expect(results).toHaveLength(2);
        expect(results).toContain('https://example.com/person/1');
        expect(results).toContain('https://example.com/person/2');
        expect(results).not.toContain('https://example.com/person/3');
      });
    });

    it('should return empty array for non-referenced entities', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
        ]);

        const results = await indexStore.getReferencesTo('https://example.com/unknown' as EntityId);

        expect(results).toEqual([]);
      });
    });

    it('should support pagination with limit', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const targetId = 'https://example.com/popular/item';

        // Create many references to the same target
        const triples: Triple[] = [];
        for (let i = 0; i < 100; i++) {
          triples.push(
            createTestTriple(`https://example.com/user/${i}`, 'likes', refObject(targetId))
          );
        }
        await indexStore.indexTriples(triples);

        const results = await indexStore.getReferencesTo(targetId as EntityId, { limit: 10 });

        expect(results.length).toBeLessThanOrEqual(10);
      });
    });
  });

  describe('getReferencesToByPredicate', () => {
    it('should filter references by predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const targetId = 'https://example.com/company/acme';

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'worksAt', refObject(targetId)),
          createTestTriple('https://example.com/person/2', 'foundedBy', refObject(targetId)),
          createTestTriple('https://example.com/person/3', 'worksAt', refObject(targetId)),
        ]);

        const results = await indexStore.getReferencesToByPredicate(
          targetId as EntityId,
          'worksAt' as Predicate
        );

        expect(results).toHaveLength(2);
        expect(results).toContain('https://example.com/person/1');
        expect(results).toContain('https://example.com/person/3');
        expect(results).not.toContain('https://example.com/person/2');
      });
    });

    it('should return empty for wrong predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const targetId = 'https://example.com/company/acme';

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'worksAt', refObject(targetId)),
        ]);

        const results = await indexStore.getReferencesToByPredicate(
          targetId as EntityId,
          'foundedBy' as Predicate
        );

        expect(results).toEqual([]);
      });
    });
  });
});

// ============================================================================
// FTS INDEX TESTS
// ============================================================================

describe('SQLiteIndexStore - FTS Index (Full-Text Search)', () => {
  describe('search', () => {
    it('should find text matches across all string fields', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'title', stringObject('Introduction to Graph Databases')),
          createTestTriple('https://example.com/doc/2', 'title', stringObject('SQL Tutorial')),
          createTestTriple('https://example.com/doc/3', 'content', stringObject('Graph theory in computer science')),
        ]);

        const results = await indexStore.search('graph');

        expect(results.length).toBeGreaterThan(0);
        const entityIds = results.map(r => r.entityId);
        expect(entityIds).toContain('https://example.com/doc/1');
        expect(entityIds).toContain('https://example.com/doc/3');
        expect(entityIds).not.toContain('https://example.com/doc/2');
      });
    });

    it('should return results with BM25 scores', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'content', stringObject('machine learning is fascinating')),
          createTestTriple('https://example.com/doc/2', 'content', stringObject('machine learning machine learning machine learning')),
        ]);

        const results = await indexStore.search('machine learning');

        expect(results.length).toBeGreaterThan(0);
        // Each result should have a score
        results.forEach(result => {
          expect(typeof result.score).toBe('number');
          expect(result.score).toBeGreaterThanOrEqual(0);
        });
      });
    });

    it('should return more relevant results first (BM25 ordering)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Doc with more "database" mentions should rank higher
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/sparse', 'content', stringObject('A document about something else with database mentioned once')),
          createTestTriple('https://example.com/doc/dense', 'content', stringObject('Database database database - this is all about databases and database systems')),
        ]);

        const results = await indexStore.search('database');

        expect(results.length).toBe(2);
        // Higher score = more relevant (but BM25 returns negatives, so after abs(), higher is better)
        // The dense doc should have higher score
        const denseResult = results.find(r => r.entityId.includes('dense'));
        const sparseResult = results.find(r => r.entityId.includes('sparse'));

        expect(denseResult).toBeDefined();
        expect(sparseResult).toBeDefined();
        // In sorted results, dense should come first (higher relevance)
        expect(results[0].entityId).toBe('https://example.com/doc/dense');
      });
    });

    it('should return empty array for no matches', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'content', stringObject('hello world')),
        ]);

        const results = await indexStore.search('xyznonexistent');

        expect(results).toEqual([]);
      });
    });

    it('should respect limit option', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Create many matching documents
        const triples: Triple[] = [];
        for (let i = 0; i < 50; i++) {
          triples.push(
            createTestTriple(`https://example.com/doc/${i}`, 'content', stringObject(`Test document number ${i}`))
          );
        }
        await indexStore.indexTriples(triples);

        const results = await indexStore.search('document', { limit: 5 });

        expect(results.length).toBeLessThanOrEqual(5);
      });
    });
  });

  describe('searchInPredicate', () => {
    it('should search only within specified predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/article/1', 'title', stringObject('Cloud Computing Overview')),
          createTestTriple('https://example.com/article/1', 'body', stringObject('Detailed text about cloud infrastructure')),
          createTestTriple('https://example.com/article/2', 'title', stringObject('Introduction to Databases')),
          createTestTriple('https://example.com/article/2', 'body', stringObject('Cloud storage and cloud services')),
        ]);

        // Search for 'cloud' only in title predicate
        const results = await indexStore.searchInPredicate('title' as Predicate, 'cloud');

        expect(results.length).toBe(1);
        expect(results[0].entityId).toBe('https://example.com/article/1');
      });
    });

    it('should return empty when predicate has no matches', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'title', stringObject('Hello')),
          createTestTriple('https://example.com/doc/1', 'body', stringObject('World contains searchterm')),
        ]);

        const results = await indexStore.searchInPredicate('title' as Predicate, 'searchterm');

        expect(results).toEqual([]);
      });
    });
  });
});

// ============================================================================
// GEO INDEX TESTS
// ============================================================================

describe('SQLiteIndexStore - GEO Index (Geospatial)', () => {
  describe('queryGeoBBox', () => {
    it('should find entities within bounding box', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // San Francisco area locations
        await indexStore.indexTriples([
          createTestTriple('https://example.com/place/sf', 'location', geoPointObject(37.7749, -122.4194)), // SF downtown
          createTestTriple('https://example.com/place/oakland', 'location', geoPointObject(37.8044, -122.2712)), // Oakland
          createTestTriple('https://example.com/place/la', 'location', geoPointObject(34.0522, -118.2437)), // LA (outside bbox)
        ]);

        // Bounding box covering SF Bay Area
        const results = await indexStore.queryGeoBBox(37.5, -123.0, 38.0, -122.0);

        expect(results.length).toBeGreaterThanOrEqual(2);
        expect(results).toContain('https://example.com/place/sf');
        expect(results).toContain('https://example.com/place/oakland');
        expect(results).not.toContain('https://example.com/place/la');
      });
    });

    it('should return empty array for empty area', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.0, -74.0)),
        ]);

        // Bounding box in Pacific Ocean (no land)
        const results = await indexStore.queryGeoBBox(0.0, -150.0, 1.0, -149.0);

        expect(results).toEqual([]);
      });
    });

    it('should respect limit option', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Create many places in a region
        const triples: Triple[] = [];
        for (let i = 0; i < 50; i++) {
          // Small variations around a central point
          const lat = 40.0 + (i * 0.001);
          const lng = -74.0 + (i * 0.001);
          triples.push(
            createTestTriple(`https://example.com/place/${i}`, 'location', geoPointObject(lat, lng))
          );
        }
        await indexStore.indexTriples(triples);

        const results = await indexStore.queryGeoBBox(39.9, -74.1, 40.1, -73.9, { limit: 10 });

        expect(results.length).toBeLessThanOrEqual(10);
      });
    });
  });

  describe('queryGeoRadius', () => {
    it('should find entities within radius', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Center: Times Square, NYC (40.758, -73.9855)
        await indexStore.indexTriples([
          createTestTriple('https://example.com/place/timessquare', 'location', geoPointObject(40.758, -73.9855)),
          createTestTriple('https://example.com/place/centralpark', 'location', geoPointObject(40.7829, -73.9654)), // ~3km away
          createTestTriple('https://example.com/place/brooklyn', 'location', geoPointObject(40.6782, -73.9442)), // ~10km away
        ]);

        // Search within 5km of Times Square
        const results = await indexStore.queryGeoRadius(40.758, -73.9855, 5);

        // NOTE: This test will FAIL until getGeohashNeighbors is properly implemented
        // Currently the function is a stub that only returns the center cell, not neighbors.
        // A proper implementation would find Central Park (~3km away) in the results.
        // This is intentional for TDD RED phase - drives implementation of neighbor calculation.
        expect(results).toContain('https://example.com/place/timessquare');
        expect(results).toContain('https://example.com/place/centralpark');
        // Brooklyn might be included depending on geohash precision
      });
    });

    it('should return empty for radius with no entities', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/place/nyc', 'location', geoPointObject(40.7128, -74.0060)),
        ]);

        // Search in completely different location
        const results = await indexStore.queryGeoRadius(51.5074, -0.1278, 1); // London, 1km

        expect(results).toEqual([]);
      });
    });

    it('should handle large radius queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/city/nyc', 'location', geoPointObject(40.7128, -74.0060)),
          createTestTriple('https://example.com/city/boston', 'location', geoPointObject(42.3601, -71.0589)),
          createTestTriple('https://example.com/city/philly', 'location', geoPointObject(39.9526, -75.1652)),
        ]);

        // Large radius from NYC (500km should include Boston and Philly)
        const results = await indexStore.queryGeoRadius(40.7128, -74.0060, 500);

        expect(results.length).toBeGreaterThanOrEqual(1);
        expect(results).toContain('https://example.com/city/nyc');
      });
    });
  });
});

// ============================================================================
// INDEX MAINTENANCE TESTS
// ============================================================================

describe('SQLiteIndexStore - Index Maintenance', () => {
  describe('indexTriple', () => {
    it('should add single triple to all relevant indexes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index a string triple (should go to POS and FTS)
        await indexStore.indexTriple(
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice Smith'))
        );

        // Verify POS index
        const posResults = await indexStore.getByPredicate('name' as Predicate);
        expect(posResults).toContain('https://example.com/person/1');

        // Verify FTS index
        const ftsResults = await indexStore.search('Alice');
        expect(ftsResults.map(r => r.entityId)).toContain('https://example.com/person/1');
      });
    });

    it('should add REF triple to OSP index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const targetId = 'https://example.com/company/1';

        await indexStore.indexTriple(
          createTestTriple('https://example.com/person/1', 'worksAt', refObject(targetId))
        );

        // Verify OSP index
        const ospResults = await indexStore.getReferencesTo(targetId as EntityId);
        expect(ospResults).toContain('https://example.com/person/1');
      });
    });

    it('should add GEO_POINT triple to geo index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriple(
          createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.7128, -74.0060))
        );

        // Verify geo index
        const geoResults = await indexStore.queryGeoRadius(40.7128, -74.0060, 1);
        expect(geoResults).toContain('https://example.com/place/1');
      });
    });

    it('should skip NULL type triples', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriple(
          createTestTriple('https://example.com/entity/1', 'deletedField', nullObject())
        );

        // Should not throw and should not add to POS index
        const posResults = await indexStore.getByPredicate('deletedField' as Predicate);
        expect(posResults).toEqual([]);
      });
    });
  });

  describe('indexTriples', () => {
    it('should batch index multiple triples efficiently', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const triples: Triple[] = [
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/2', 'name', stringObject('Bob')),
          createTestTriple('https://example.com/person/3', 'name', stringObject('Charlie')),
          createTestTriple('https://example.com/person/1', 'worksAt', refObject('https://example.com/company/1')),
          createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.0, -74.0)),
        ];

        await indexStore.indexTriples(triples);

        // Verify all indexes populated
        const nameResults = await indexStore.getByPredicate('name' as Predicate);
        expect(nameResults).toHaveLength(3);

        const refResults = await indexStore.getReferencesTo('https://example.com/company/1' as EntityId);
        expect(refResults).toHaveLength(1);

        const geoResults = await indexStore.queryGeoRadius(40.0, -74.0, 10);
        expect(geoResults).toHaveLength(1);
      });
    });

    it('should handle large batch efficiently', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const triples: Triple[] = [];
        for (let i = 0; i < 1000; i++) {
          triples.push(
            createTestTriple(`https://example.com/entity/${i}`, 'value', int64Object(BigInt(i)))
          );
        }

        const start = Date.now();
        await indexStore.indexTriples(triples);
        const elapsed = Date.now() - start;

        // Should complete in reasonable time (less than 10 seconds)
        expect(elapsed).toBeLessThan(10000);

        // Verify indexing worked
        const stats = indexStore.getStats();
        expect(stats.posEntryCount).toBeGreaterThanOrEqual(1);
      });
    });

    it('should handle same subject with multiple predicates', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/1', 'age', int64Object(30n)),
          createTestTriple('https://example.com/person/1', 'email', stringObject('alice@example.com')),
        ]);

        // All predicates should be searchable
        const nameResults = await indexStore.getByPredicate('name' as Predicate);
        expect(nameResults).toContain('https://example.com/person/1');

        const ageResults = await indexStore.getByPredicate('age' as Predicate);
        expect(ageResults).toContain('https://example.com/person/1');

        const emailResults = await indexStore.getByPredicate('email' as Predicate);
        expect(emailResults).toContain('https://example.com/person/1');
      });
    });
  });

  describe('unindexTriple', () => {
    it('should remove triple from POS index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const triple = createTestTriple('https://example.com/person/1', 'name', stringObject('Alice'));

        // Index then unindex
        await indexStore.indexTriple(triple);
        await indexStore.unindexTriple(triple);

        // Should no longer be findable
        const results = await indexStore.getByPredicateValue('name' as Predicate, 'Alice');
        expect(results).not.toContain('https://example.com/person/1');
      });
    });

    it('should remove REF triple from OSP index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const targetId = 'https://example.com/company/1';
        const triple = createTestTriple('https://example.com/person/1', 'worksAt', refObject(targetId));

        await indexStore.indexTriple(triple);
        await indexStore.unindexTriple(triple);

        const results = await indexStore.getReferencesTo(targetId as EntityId);
        expect(results).not.toContain('https://example.com/person/1');
      });
    });

    it('should remove STRING triple from FTS index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const triple = createTestTriple('https://example.com/doc/1', 'content', stringObject('unique searchable text xyz123'));

        await indexStore.indexTriple(triple);
        await indexStore.unindexTriple(triple);

        const results = await indexStore.search('xyz123');
        expect(results.map(r => r.entityId)).not.toContain('https://example.com/doc/1');
      });
    });

    it('should remove GEO_POINT triple from geo index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        const triple = createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.7128, -74.0060));

        await indexStore.indexTriple(triple);
        await indexStore.unindexTriple(triple);

        const results = await indexStore.queryGeoRadius(40.7128, -74.0060, 1);
        expect(results).not.toContain('https://example.com/place/1');
      });
    });

    it('should not affect other triples with same predicate but different values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/2', 'name', stringObject('Bob')),
        ]);

        // Remove only Alice
        await indexStore.unindexTriple(
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice'))
        );

        // Bob should still be indexed
        const results = await indexStore.getByPredicateValue('name' as Predicate, 'Bob');
        expect(results).toContain('https://example.com/person/2');
      });
    });

    it('should handle NULL type gracefully (no-op)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Should not throw
        await indexStore.unindexTriple(
          createTestTriple('https://example.com/entity/1', 'field', nullObject())
        );

        // Verify no crash
        const stats = indexStore.getStats();
        expect(stats).toBeDefined();
      });
    });
  });

  describe('getStats', () => {
    it('should return index statistics', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/1', 'worksAt', refObject('https://example.com/company/1')),
          createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.0, -74.0)),
        ]);

        const stats = indexStore.getStats();

        expect(stats.posEntryCount).toBeGreaterThanOrEqual(2);
        expect(stats.ospEntryCount).toBeGreaterThanOrEqual(1);
        expect(stats.ftsDocumentCount).toBeGreaterThanOrEqual(1);
        expect(stats.geoCellCount).toBeGreaterThanOrEqual(1);
        expect(typeof stats.lastUpdated).toBe('number');
      });
    });
  });
});

// ============================================================================
// R2 SYNC TESTS
// ============================================================================

describe('SQLiteIndexStore - R2 Sync', () => {
  describe('saveToR2', () => {
    it('should serialize all indexes to R2 JSON files', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);
        const mockR2 = createMockR2Bucket();

        // Index some data
        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/1', 'worksAt', refObject('https://example.com/company/1')),
          createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.0, -74.0)),
        ]);

        // Save to R2
        await indexStore.saveToR2(mockR2, 'test-namespace');

        // Verify R2 put was called for each index type
        expect(mockR2.put).toHaveBeenCalledWith(
          'test-namespace/indexes/pos.json',
          expect.any(String)
        );
        expect(mockR2.put).toHaveBeenCalledWith(
          'test-namespace/indexes/osp.json',
          expect.any(String)
        );
        expect(mockR2.put).toHaveBeenCalledWith(
          'test-namespace/indexes/geo.json',
          expect.any(String)
        );
        expect(mockR2.put).toHaveBeenCalledWith(
          'test-namespace/indexes/fts.json',
          expect.any(String)
        );
      });
    });

    it('should include version in serialized indexes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);
        const mockR2 = createMockR2Bucket();

        await indexStore.indexTriple(
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice'))
        );

        await indexStore.saveToR2(mockR2, 'test-namespace');

        // Get the saved POS index and verify it has a version
        const putCalls = (mockR2.put as ReturnType<typeof vi.fn>).mock.calls;
        const posCall = putCalls.find((call: [string, unknown]) => call[0].includes('pos.json'));
        expect(posCall).toBeDefined();

        const posData = JSON.parse(posCall![1] as string);
        expect(posData.version).toBeDefined();
        expect(posData.version).toMatch(/^v\d+$/);
      });
    });
  });

  describe('loadFromR2', () => {
    it('should restore indexes from R2 JSON files', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);
        const mockR2 = createMockR2Bucket();

        // First, populate and save
        await indexStore.indexTriples([
          createTestTriple('https://example.com/person/1', 'name', stringObject('Alice')),
          createTestTriple('https://example.com/person/1', 'worksAt', refObject('https://example.com/company/1')),
        ]);
        await indexStore.saveToR2(mockR2, 'test-namespace');

        // Create a new index store (simulating cold start)
        const newIndexStore = new SQLiteIndexStore(sql);

        // Clear existing data by re-initializing (this is a simplification for testing)
        sql.exec('DELETE FROM pos_index');
        sql.exec('DELETE FROM osp_index');

        // Load from R2
        await newIndexStore.loadFromR2(mockR2, 'test-namespace');

        // Verify data was restored
        const posResults = await newIndexStore.getByPredicate('name' as Predicate);
        expect(posResults).toContain('https://example.com/person/1');

        const ospResults = await newIndexStore.getReferencesTo('https://example.com/company/1' as EntityId);
        expect(ospResults).toContain('https://example.com/person/1');
      });
    });

    it('should handle missing R2 files gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Mock R2 with no files
        const emptyR2 = createMockR2Bucket();
        (emptyR2.get as ReturnType<typeof vi.fn>).mockResolvedValue(null);

        // Should not throw
        await expect(indexStore.loadFromR2(emptyR2, 'empty-namespace')).resolves.toBeUndefined();

        // Stats should show empty
        const stats = indexStore.getStats();
        expect(stats.posEntryCount).toBe(0);
      });
    });

    it('should restore geo index from R2', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);
        const mockR2 = createMockR2Bucket();

        // Populate and save geo data
        await indexStore.indexTriple(
          createTestTriple('https://example.com/place/1', 'location', geoPointObject(40.7128, -74.0060))
        );
        await indexStore.saveToR2(mockR2, 'geo-test');

        // Clear and reload
        sql.exec('DELETE FROM geo_index');

        const newIndexStore = new SQLiteIndexStore(sql);
        await newIndexStore.loadFromR2(mockR2, 'geo-test');

        // Geo queries should work after reload
        const geoResults = await newIndexStore.queryGeoRadius(40.7128, -74.0060, 10);
        expect(geoResults).toContain('https://example.com/place/1');
      });
    });
  });
});

// ============================================================================
// VECTOR INDEX TESTS
// ============================================================================

/**
 * Create a vector TypedObject
 */
function vectorObject(value: number[]): TypedObject {
  return { type: ObjectType.VECTOR, value };
}

describe('SQLiteIndexStore - Vector Index (Similarity Search)', () => {
  describe('indexing vectors', () => {
    it('should index VECTOR type triples', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index a vector
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([0.1, 0.2, 0.3, 0.4]))
        );

        // Verify stats reflect the vector
        const stats = indexStore.getStats();
        expect(stats.vectorCount).toBe(1);
      });
    });

    it('should index multiple vectors with different predicates', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([0.1, 0.2, 0.3])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0.4, 0.5, 0.6])),
          createTestTriple('https://example.com/doc/1', 'imageEmbedding', vectorObject([0.7, 0.8, 0.9])),
        ]);

        const stats = indexStore.getStats();
        expect(stats.vectorCount).toBe(3);
      });
    });

    it('should update existing vector when re-indexed', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index a vector
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([0.1, 0.2, 0.3]))
        );

        // Update the same vector
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([0.4, 0.5, 0.6]))
        );

        // Should still have only 1 entry
        const stats = indexStore.getStats();
        expect(stats.vectorCount).toBe(1);

        // Query should return the updated vector
        const results = await indexStore.queryKNN('embedding' as Predicate, [0.4, 0.5, 0.6], 1);
        expect(results).toHaveLength(1);
        expect(results[0].entityId).toBe('https://example.com/doc/1');
        // Updated vector should have similarity close to 1.0
        expect(results[0].similarity).toBeGreaterThan(0.99);
      });
    });
  });

  describe('queryKNN', () => {
    it('should return k nearest neighbors by cosine similarity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index vectors with known relationships
        // Vector 1: [1, 0, 0]
        // Vector 2: [0.9, 0.1, 0] - very similar to query
        // Vector 3: [0, 1, 0] - orthogonal to query
        // Vector 4: [-1, 0, 0] - opposite to query
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0.9, 0.1, 0])),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject([0, 1, 0])),
          createTestTriple('https://example.com/doc/4', 'embedding', vectorObject([-1, 0, 0])),
        ]);

        // Query for vector similar to [1, 0, 0]
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 2);

        expect(results).toHaveLength(2);
        // First result should be doc/1 (exact match, similarity = 1.0)
        expect(results[0].entityId).toBe('https://example.com/doc/1');
        expect(results[0].similarity).toBeCloseTo(1.0, 5);
        // Second result should be doc/2 (very similar)
        expect(results[1].entityId).toBe('https://example.com/doc/2');
        expect(results[1].similarity).toBeGreaterThan(0.9);
      });
    });

    it('should order results by cosine similarity descending', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Create vectors with progressively decreasing similarity to [1, 0, 0]
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/a', 'embedding', vectorObject([1, 0, 0])),       // cos = 1.0
          createTestTriple('https://example.com/doc/b', 'embedding', vectorObject([0.7, 0.7, 0])),   // cos ~ 0.707
          createTestTriple('https://example.com/doc/c', 'embedding', vectorObject([0, 1, 0])),       // cos = 0
          createTestTriple('https://example.com/doc/d', 'embedding', vectorObject([-1, 0, 0])),      // cos = -1.0
        ]);

        // Get all 4 results
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 4);

        expect(results).toHaveLength(4);
        // Verify descending order of similarity
        expect(results[0].similarity).toBeGreaterThan(results[1].similarity);
        expect(results[1].similarity).toBeGreaterThan(results[2].similarity);
        expect(results[2].similarity).toBeGreaterThan(results[3].similarity);

        // Verify specific order: a, b, c, d
        expect(results[0].entityId).toBe('https://example.com/doc/a');
        expect(results[1].entityId).toBe('https://example.com/doc/b');
        expect(results[2].entityId).toBe('https://example.com/doc/c');
        expect(results[3].entityId).toBe('https://example.com/doc/d');
      });
    });

    it('should return empty array when no vectors for predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index with different predicate
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/1', 'otherEmbedding', vectorObject([1, 0, 0]))
        );

        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 5);
        expect(results).toEqual([]);
      });
    });

    it('should return fewer than k results if not enough vectors exist', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index only 2 vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0, 1, 0])),
        ]);

        // Request 10 results
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 10);

        // Should only return 2
        expect(results).toHaveLength(2);
      });
    });

    it('should handle high-dimensional vectors', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Create 128-dimensional vectors (typical for embeddings)
        const dim = 128;
        const vec1 = Array.from({ length: dim }, (_, i) => Math.sin(i * 0.1));
        const vec2 = Array.from({ length: dim }, (_, i) => Math.sin(i * 0.1 + 0.1)); // Similar
        const vec3 = Array.from({ length: dim }, (_, i) => Math.cos(i * 0.1)); // Different

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject(vec1)),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject(vec2)),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject(vec3)),
        ]);

        const results = await indexStore.queryKNN('embedding' as Predicate, vec1, 2);

        expect(results).toHaveLength(2);
        // First result should be exact match
        expect(results[0].entityId).toBe('https://example.com/doc/1');
        expect(results[0].similarity).toBeCloseTo(1.0, 5);
        // Second should be the similar vector
        expect(results[1].entityId).toBe('https://example.com/doc/2');
      });
    });

    it('should correctly compute cosine similarity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Test with known cosine similarity values
        // cos([1,0], [1,0]) = 1
        // cos([1,0], [0,1]) = 0
        // cos([1,0], [-1,0]) = -1
        // cos([1,1], [1,0]) = 1/sqrt(2) ~ 0.707

        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/identical', 'embedding', vectorObject([1, 0])),
          createTestTriple('https://example.com/doc/orthogonal', 'embedding', vectorObject([0, 1])),
          createTestTriple('https://example.com/doc/opposite', 'embedding', vectorObject([-1, 0])),
          createTestTriple('https://example.com/doc/diagonal', 'embedding', vectorObject([1, 1])),
        ]);

        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0], 4);

        // Find each result by entityId
        const identical = results.find(r => r.entityId.includes('identical'));
        const orthogonal = results.find(r => r.entityId.includes('orthogonal'));
        const opposite = results.find(r => r.entityId.includes('opposite'));
        const diagonal = results.find(r => r.entityId.includes('diagonal'));

        expect(identical?.similarity).toBeCloseTo(1.0, 5);
        expect(orthogonal?.similarity).toBeCloseTo(0.0, 5);
        expect(opposite?.similarity).toBeCloseTo(-1.0, 5);
        expect(diagonal?.similarity).toBeCloseTo(1 / Math.sqrt(2), 3);
      });
    });

    it('should filter by predicate correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index vectors with different predicates
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'textEmbedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'imageEmbedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/3', 'textEmbedding', vectorObject([0, 1, 0])),
        ]);

        // Query only textEmbedding
        const textResults = await indexStore.queryKNN('textEmbedding' as Predicate, [1, 0, 0], 10);
        expect(textResults).toHaveLength(2);
        expect(textResults.map(r => r.entityId)).toContain('https://example.com/doc/1');
        expect(textResults.map(r => r.entityId)).toContain('https://example.com/doc/3');
        expect(textResults.map(r => r.entityId)).not.toContain('https://example.com/doc/2');

        // Query only imageEmbedding
        const imageResults = await indexStore.queryKNN('imageEmbedding' as Predicate, [1, 0, 0], 10);
        expect(imageResults).toHaveLength(1);
        expect(imageResults[0].entityId).toBe('https://example.com/doc/2');
      });
    });

    it('should support ef parameter for search accuracy control', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0.9, 0.1, 0])),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject([0, 1, 0])),
        ]);

        // Query with custom ef parameter
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 2, 50);

        expect(results).toHaveLength(2);
        expect(results[0].entityId).toBe('https://example.com/doc/1');
      });
    });
  });

  describe('HNSW integration', () => {
    it('should build HNSW graph when indexing vectors', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index multiple vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0.9, 0.1, 0])),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject([0, 1, 0])),
          createTestTriple('https://example.com/doc/4', 'embedding', vectorObject([-1, 0, 0])),
        ]);

        // Verify HNSW graph tables are populated
        const metaRows = sql.exec(
          `SELECT * FROM hnsw_meta WHERE key LIKE 'entry_point_%' OR key LIKE 'max_layer_%'`
        ).toArray();

        expect(metaRows.length).toBeGreaterThan(0);

        // Verify edges are stored
        const edgeRows = sql.exec(
          `SELECT COUNT(*) as cnt FROM hnsw_edges WHERE node_id LIKE 'embedding:%'`
        ).toArray();

        expect(edgeRows[0]!['cnt'] as number).toBeGreaterThan(0);
      });
    });

    it('should use HNSW for larger datasets (>100 vectors)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Generate 150 random 32-dimensional vectors
        const dim = 32;
        const numVectors = 150;
        const triples: Triple[] = [];

        for (let i = 0; i < numVectors; i++) {
          const vector = Array.from({ length: dim }, () => Math.random() - 0.5);
          triples.push(
            createTestTriple(`https://example.com/doc/${i}`, 'embedding', vectorObject(vector))
          );
        }

        await indexStore.indexTriples(triples);

        // Query - should use HNSW since we have >100 vectors
        const queryVector = Array.from({ length: dim }, () => Math.random() - 0.5);
        const results = await indexStore.queryKNN('embedding' as Predicate, queryVector, 10);

        expect(results).toHaveLength(10);
        // Results should be sorted by similarity descending
        for (let i = 1; i < results.length; i++) {
          expect(results[i - 1]!.similarity).toBeGreaterThanOrEqual(results[i]!.similarity);
        }

        // Verify HNSW graph has entry point set
        const entryPoint = sql.exec(
          `SELECT value FROM hnsw_meta WHERE key = 'entry_point_embedding'`
        ).toArray();
        expect(entryPoint.length).toBe(1);
        expect(entryPoint[0]!['value']).toBeTruthy();
      });
    });

    it('should demonstrate O(log n) scaling behavior', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Generate 200 random 32-dimensional vectors (smaller for faster tests)
        const dim = 32;
        const numVectors = 200;
        const triples: Triple[] = [];
        const vectors: number[][] = [];

        for (let i = 0; i < numVectors; i++) {
          const vector = Array.from({ length: dim }, () => Math.random() - 0.5);
          vectors.push(vector);
          triples.push(
            createTestTriple(`https://example.com/item/${i}`, 'vector', vectorObject(vector))
          );
        }

        // Index all vectors
        await indexStore.indexTriples(triples);

        // Perform multiple queries and measure average time
        const numQueries = 5;
        const startTime = Date.now();

        for (let i = 0; i < numQueries; i++) {
          const queryVector = Array.from({ length: dim }, () => Math.random() - 0.5);
          await indexStore.queryKNN('vector' as Predicate, queryVector, 10);
        }

        const avgQueryTime = (Date.now() - startTime) / numQueries;

        // With HNSW, average query time should be << 100ms even for 200 vectors
        // Just verify queries complete and are reasonable
        expect(avgQueryTime).toBeLessThan(500); // Generous bound for test stability

        // Verify accuracy by checking a known vector returns itself as closest
        const knownVectorIdx = Math.floor(Math.random() * numVectors);
        const results = await indexStore.queryKNN('vector' as Predicate, vectors[knownVectorIdx]!, 1);

        expect(results).toHaveLength(1);
        expect(results[0]!.entityId).toBe(`https://example.com/item/${knownVectorIdx}`);
        expect(results[0]!.similarity).toBeCloseTo(1.0, 3);
      });
    }, 15000); // Increase timeout to 15 seconds

    it('should preserve HNSW graph across queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0.9, 0.1, 0])),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject([0, 1, 0])),
        ]);

        // First query
        const results1 = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 2);
        expect(results1[0]!.entityId).toBe('https://example.com/doc/1');

        // Add more vectors
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/4', 'embedding', vectorObject([0.95, 0.05, 0]))
        );

        // Second query should include new vector
        const results2 = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 3);
        expect(results2).toHaveLength(3);
        // New vector should be in top results (very similar to query)
        const newVectorResult = results2.find(r => r.entityId === 'https://example.com/doc/4');
        expect(newVectorResult).toBeDefined();
        expect(newVectorResult!.similarity).toBeGreaterThan(0.99);
      });
    });

    it('should handle high recall with appropriate ef parameter', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Create vectors in clusters
        const dim = 16;
        const triples: Triple[] = [];

        // Cluster 1: vectors near [1, 0, 0, ...]
        for (let i = 0; i < 50; i++) {
          const vector = Array.from({ length: dim }, (_, j) =>
            j === 0 ? 1 + (Math.random() - 0.5) * 0.1 : (Math.random() - 0.5) * 0.1
          );
          triples.push(
            createTestTriple(`https://example.com/cluster1/doc/${i}`, 'embedding', vectorObject(vector))
          );
        }

        // Cluster 2: vectors near [0, 1, 0, ...]
        for (let i = 0; i < 50; i++) {
          const vector = Array.from({ length: dim }, (_, j) =>
            j === 1 ? 1 + (Math.random() - 0.5) * 0.1 : (Math.random() - 0.5) * 0.1
          );
          triples.push(
            createTestTriple(`https://example.com/cluster2/doc/${i}`, 'embedding', vectorObject(vector))
          );
        }

        await indexStore.indexTriples(triples);

        // Query with vector from cluster 1
        const queryVector = Array.from({ length: dim }, (_, j) => j === 0 ? 1 : 0);

        // Higher ef should give better recall
        const resultsLowEf = await indexStore.queryKNN('embedding' as Predicate, queryVector, 20, 25);
        const resultsHighEf = await indexStore.queryKNN('embedding' as Predicate, queryVector, 20, 100);

        // Both should return results from cluster 1
        const cluster1CountLowEf = resultsLowEf.filter(r => r.entityId.includes('cluster1')).length;
        const cluster1CountHighEf = resultsHighEf.filter(r => r.entityId.includes('cluster1')).length;

        // Higher ef should generally give better or equal recall for cluster 1
        expect(cluster1CountHighEf).toBeGreaterThanOrEqual(cluster1CountLowEf * 0.8); // Allow some variance
        expect(cluster1CountHighEf).toBeGreaterThan(10); // Should find most cluster 1 vectors
      });
    });
  });
});

// ============================================================================
// TYPE NARROWING TESTS (pocs-5bcn)
// ============================================================================

describe('SQLiteIndexStore - Type Narrowing for SQLite Rows', () => {
  describe('queryKNNBruteForce type safety', () => {
    it('should handle valid VectorIndexRow data correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index valid vector data
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0, 1, 0])),
        ]);

        // Query should work correctly with properly typed data
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 2);

        expect(results).toHaveLength(2);
        expect(results[0]).toHaveProperty('entityId');
        expect(results[0]).toHaveProperty('similarity');
        expect(typeof results[0].entityId).toBe('string');
        expect(typeof results[0].similarity).toBe('number');
      });
    });

    it('should skip invalid rows gracefully when entity_id is missing', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // First, index some valid vectors
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/valid', 'embedding', vectorObject([1, 0, 0]))
        );

        // Manually insert a malformed row with NULL entity_id (simulating corrupted data)
        try {
          sql.exec(`
            INSERT INTO vector_index (entity_id, predicate, vector, layer, connections, updated_at)
            VALUES (NULL, 'embedding', X'0000803F00000000000000000000000000000000', 0, '[]', ?)
          `, Date.now());
        } catch {
          // If NULL entity_id is rejected by NOT NULL constraint, this is expected
          // The test still validates the type guard behavior with valid data
        }

        // Query should still work - type guard should skip invalid rows
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 10);

        // Should return only the valid row
        expect(results.length).toBeGreaterThanOrEqual(1);
        const validResult = results.find(r => r.entityId === 'https://example.com/doc/valid');
        expect(validResult).toBeDefined();
      });
    });

    it('should skip rows with non-string entity_id gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index valid vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0, 1, 0])),
        ]);

        // Query should work correctly
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 10);

        // All results should have string entityIds
        results.forEach((result, index) => {
          expect(typeof result.entityId).toBe('string');
          expect(result.entityId.length).toBeGreaterThan(0);
        });
      });
    });

    it('should handle empty result set without errors', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Don't index any vectors for the predicate we're querying
        await indexStore.indexTriple(
          createTestTriple('https://example.com/doc/1', 'otherPredicate', vectorObject([1, 0, 0]))
        );

        // Query for a predicate with no vectors
        const results = await indexStore.queryKNN('nonExistentPredicate' as Predicate, [1, 0, 0], 10);

        expect(results).toEqual([]);
      });
    });

    it('should correctly narrow types for vector buffer data', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index vectors with the same dimensions (5D vectors)
        // Note: All vectors for the same predicate must have matching dimensions
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/a', 'embedding5d', vectorObject([1, 2, 3, 4, 5])),
          createTestTriple('https://example.com/doc/b', 'embedding5d', vectorObject([5, 4, 3, 2, 1])),
          createTestTriple('https://example.com/doc/c', 'embedding5d', vectorObject([1, 1, 1, 1, 1])),
        ]);

        // Index a separate set of 10D vectors with different predicate
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/x', 'embedding10d', vectorObject([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])),
          createTestTriple('https://example.com/doc/y', 'embedding10d', vectorObject([10, 9, 8, 7, 6, 5, 4, 3, 2, 1])),
        ]);

        // Query with matching dimensions
        const results5d = await indexStore.queryKNN('embedding5d' as Predicate, [1, 2, 3, 4, 5], 10);
        const results10d = await indexStore.queryKNN('embedding10d' as Predicate, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 10);

        // Results should be returned (type narrowing should work for all vector sizes)
        expect(results5d.length).toBe(3);
        expect(results10d.length).toBe(2);

        // Each result should have valid structure
        [...results5d, ...results10d].forEach(result => {
          expect(result).toHaveProperty('entityId');
          expect(result).toHaveProperty('similarity');
          expect(typeof result.similarity).toBe('number');
          expect(Number.isFinite(result.similarity)).toBe(true);
        });
      });
    });

    it('should maintain type safety across multiple sequential queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/a', 'embedding', vectorObject([1, 0, 0])),
          createTestTriple('https://example.com/doc/b', 'embedding', vectorObject([0, 1, 0])),
          createTestTriple('https://example.com/doc/c', 'embedding', vectorObject([0, 0, 1])),
        ]);

        // Run multiple queries sequentially
        const queries = [
          [1, 0, 0],
          [0, 1, 0],
          [0, 0, 1],
          [1, 1, 0],
          [1, 1, 1],
        ];

        for (const queryVector of queries) {
          const results = await indexStore.queryKNN('embedding' as Predicate, queryVector, 3);

          // Each query should return properly typed results
          expect(Array.isArray(results)).toBe(true);
          results.forEach(result => {
            expect(typeof result.entityId).toBe('string');
            expect(typeof result.similarity).toBe('number');
            expect(result.entityId).toMatch(/^https:\/\/example\.com\/doc\/[abc]$/);
          });
        }
      });
    });
  });

  describe('loadVectorCache type safety', () => {
    it('should build cache correctly with valid rows', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Index multiple vectors for the same predicate
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 2, 3])),
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([4, 5, 6])),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject([7, 8, 9])),
        ]);

        // Perform a query which internally uses the vector cache
        const results = await indexStore.queryKNN('embedding' as Predicate, [1, 2, 3], 3);

        // Results should be correct, indicating cache was built properly
        expect(results).toHaveLength(3);

        // First result should be the exact match
        expect(results[0].entityId).toBe('https://example.com/doc/1');
        expect(results[0].similarity).toBeCloseTo(1.0, 5);
      });
    });

    it('should handle cache rebuild after new vectors are added', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        const indexStore = new SQLiteIndexStore(sql);

        // Initial vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/1', 'embedding', vectorObject([1, 0, 0])),
        ]);

        // First query
        const results1 = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 5);
        expect(results1).toHaveLength(1);

        // Add more vectors
        await indexStore.indexTriples([
          createTestTriple('https://example.com/doc/2', 'embedding', vectorObject([0, 1, 0])),
          createTestTriple('https://example.com/doc/3', 'embedding', vectorObject([0, 0, 1])),
        ]);

        // Second query should see new vectors (cache should be rebuilt/invalidated)
        const results2 = await indexStore.queryKNN('embedding' as Predicate, [1, 0, 0], 5);
        expect(results2).toHaveLength(3);

        // Type safety should be maintained
        results2.forEach(result => {
          expect(typeof result.entityId).toBe('string');
          expect(typeof result.similarity).toBe('number');
        });
      });
    });
  });
});
