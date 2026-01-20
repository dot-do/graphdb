/**
 * Triple CRUD Operations Tests
 *
 * @deprecated These tests use the deprecated crud.ts module which relies on
 * individual SQLite rows. The triples table has been removed in BLOB-only
 * architecture (schema v3). These tests are SKIPPED.
 *
 * For current tests, see test/shard/chunk-store.test.ts
 *
 * Tests for GraphDB Triple CRUD operations:
 * - insertTriple stores all ObjectType values correctly
 * - insertTriples batch insert works
 * - getTriple retrieves correct triple
 * - getTriples returns all predicates for subject
 * - updateTriple creates new version (MVCC)
 * - getLatestTriple returns most recent version
 * - deleteTriple creates tombstone
 * - deleteEntity removes all triples for subject
 * - exists returns true/false correctly
 * - Predicate must not contain colons
 *
 * @see CLAUDE.md for architecture details
 * @see test/shard/chunk-store.test.ts for current BLOB-only tests
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import {
  createTripleStore,
  tripleToRow,
  rowToTriple,
  type TripleStore,
} from '../../src/shard/crud.js';
import { initializeSchema } from '../../src/shard/schema.js';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  isPredicate,
} from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-crud-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test data helpers
const createTestTriple = (
  subjectSuffix: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
  txIdSuffix = '01ARZ3NDEKTSV4RRFFQ69G5FAV'
): Triple => {
  const object: TypedObject = { type: objectType };

  switch (objectType) {
    case ObjectType.NULL:
      break;
    case ObjectType.BOOL:
      object.value = value as boolean;
      break;
    case ObjectType.INT32:
    case ObjectType.INT64:
      object.value = value as bigint;
      break;
    case ObjectType.FLOAT64:
      object.value = value as number;
      break;
    case ObjectType.STRING:
      object.value = value as string;
      break;
    case ObjectType.BINARY:
      object.value = value as Uint8Array;
      break;
    case ObjectType.TIMESTAMP:
      object.value = value as bigint;
      break;
    case ObjectType.DATE:
      object.value = value as number;
      break;
    case ObjectType.DURATION:
      object.value = value as string;
      break;
    case ObjectType.REF:
      object.value = value as any;
      break;
    case ObjectType.REF_ARRAY:
      object.value = value as any[];
      break;
    case ObjectType.JSON:
      object.value = value;
      break;
    case ObjectType.GEO_POINT:
      object.value = value as any;
      break;
    case ObjectType.GEO_POLYGON:
      object.value = value as any;
      break;
    case ObjectType.GEO_LINESTRING:
      object.value = value as any;
      break;
    case ObjectType.URL:
      object.value = value as string;
      break;
  }

  return {
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
};

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('TripleStore CRUD Operations', () => {
  describe('createTripleStore', () => {
    it('should create a TripleStore from SqlStorage', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createTripleStore(sql);
        expect(store).toBeDefined();
        expect(typeof store.insertTriple).toBe('function');
        expect(typeof store.insertTriples).toBe('function');
        expect(typeof store.getTriple).toBe('function');
        expect(typeof store.getTriples).toBe('function');
        expect(typeof store.getTriplesByPredicate).toBe('function');
        expect(typeof store.updateTriple).toBe('function');
        expect(typeof store.deleteTriple).toBe('function');
        expect(typeof store.deleteEntity).toBe('function');
        expect(typeof store.exists).toBe('function');
        expect(typeof store.getLatestTriple).toBe('function');
      });
    });
  });

  describe('insertTriple', () => {
    it('should insert a STRING type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John Doe');

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.subject).toBe(triple.subject);
        expect(result!.predicate).toBe(triple.predicate);
        expect(result!.object.type).toBe(ObjectType.STRING);
        expect(result!.object.value).toBe('John Doe');
      });
    });

    it('should insert a INT64 type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'age', ObjectType.INT64, 30n);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.INT64);
        expect(result!.object.value).toBe(30n);
      });
    });

    it('should insert a FLOAT64 type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'score', ObjectType.FLOAT64, 3.14159);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.FLOAT64);
        expect(result!.object.value).toBeCloseTo(3.14159, 5);
      });
    });

    it('should insert a BOOL type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'active', ObjectType.BOOL, true);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.BOOL);
        expect(result!.object.value).toBe(true);
      });
    });

    it('should insert a REF type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const value = createEntityId('https://example.com/person/2');
        const triple = createTestTriple('1', 'knows', ObjectType.REF, value);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.REF);
        expect(result!.object.value).toBe(value);
      });
    });

    it('should insert a GEO_POINT type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const value = { lat: 37.7749, lng: -122.4194 };
        const triple = createTestTriple('1', 'location', ObjectType.GEO_POINT, value);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.GEO_POINT);
        expect(result!.object.value!.lat).toBeCloseTo(37.7749, 4);
        expect(result!.object.value!.lng).toBeCloseTo(-122.4194, 4);
      });
    });

    it('should insert a TIMESTAMP type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const timestamp = BigInt(Date.now());
        const triple = createTestTriple('1', 'createdAt', ObjectType.TIMESTAMP, timestamp);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.TIMESTAMP);
        expect(result!.object.value).toBe(timestamp);
      });
    });

    it('should insert a NULL type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'middleName', ObjectType.NULL, null);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.NULL);
      });
    });

    it('should insert a JSON type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const value = { nested: { key: 'value' }, array: [1, 2, 3] };
        const triple = createTestTriple('1', 'metadata', ObjectType.JSON, value);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.JSON);
        expect(result!.object.value).toEqual(value);
      });
    });

    it('should insert a BINARY type triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const value = new Uint8Array([1, 2, 3, 4, 5]);
        const triple = createTestTriple('1', 'avatar', ObjectType.BINARY, value);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.type).toBe(ObjectType.BINARY);
        // SQLite returns ArrayBuffer, convert to Uint8Array for comparison
        const resultBinary = result!.object.value instanceof ArrayBuffer
          ? new Uint8Array(result!.object.value)
          : result!.object.value;
        expect(resultBinary).toEqual(value);
      });
    });

    it('should reject predicate containing colon', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Attempting to use a predicate with colon should throw
        expect(() => createPredicate('schema:name')).toThrow();

        // Also verify isPredicate rejects colons
        expect(isPredicate('schema:name')).toBe(false);
        expect(isPredicate('name')).toBe(true);
      });
    });
  });

  describe('insertTriples (batch)', () => {
    it('should insert multiple triples in batch', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'John'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
          createTestTriple('1', 'active', ObjectType.BOOL, true),
        ];

        await store.insertTriples(triples);

        // Verify all triples were inserted
        const subject = createEntityId('https://example.com/entity/1');
        const results = await store.getTriples(subject);
        expect(results.length).toBe(3);
      });
    });

    it('should handle empty batch gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Should not throw
        await store.insertTriples([]);
      });
    });
  });

  describe('getTriple', () => {
    it('should return null for non-existent triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/nonexistent');
        const predicate = createPredicate('name');

        const result = await store.getTriple(subject, predicate);
        expect(result).toBeNull();
      });
    });

    it('should retrieve the correct triple by subject and predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert multiple triples with different predicates
        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'John'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
        ];
        await store.insertTriples(triples);

        const subject = createEntityId('https://example.com/entity/1');
        const result = await store.getTriple(subject, createPredicate('age'));

        expect(result).not.toBeNull();
        expect(result!.predicate).toBe('age');
        expect(result!.object.value).toBe(30n);
      });
    });
  });

  describe('getTriples', () => {
    it('should return all triples for a subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'John'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
          createTestTriple('1', 'email', ObjectType.STRING, 'john@example.com'),
          createTestTriple('2', 'name', ObjectType.STRING, 'Jane'), // Different subject
        ];
        await store.insertTriples(triples);

        const subject = createEntityId('https://example.com/entity/1');
        const results = await store.getTriples(subject);

        expect(results.length).toBe(3);
        const predicates = results.map((t) => t.predicate);
        expect(predicates).toContain('name');
        expect(predicates).toContain('age');
        expect(predicates).toContain('email');
      });
    });

    it('should return empty array for non-existent subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/nonexistent');
        const results = await store.getTriples(subject);

        expect(results).toEqual([]);
      });
    });
  });

  describe('getTriplesByPredicate', () => {
    it('should return all triples with a given predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'John'),
          createTestTriple('2', 'name', ObjectType.STRING, 'Jane'),
          createTestTriple('3', 'name', ObjectType.STRING, 'Bob'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
        ];
        await store.insertTriples(triples);

        const results = await store.getTriplesByPredicate(createPredicate('name'));

        expect(results.length).toBe(3);
        const names = results.map((t) => t.object.value);
        expect(names).toContain('John');
        expect(names).toContain('Jane');
        expect(names).toContain('Bob');
      });
    });
  });

  describe('updateTriple (MVCC)', () => {
    it('should create a new version when updating', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert initial triple
        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await store.insertTriple(triple);

        // Update with new value and new transaction
        const newTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        const newValue: TypedObject = { type: ObjectType.STRING, value: 'Johnny' };

        await store.updateTriple(triple.subject, triple.predicate, newValue, newTxId);

        // Both versions should exist in the database (MVCC)
        // getLatestTriple should return the newest version
        const latest = await store.getLatestTriple(triple.subject, triple.predicate);
        expect(latest).not.toBeNull();
        expect(latest!.object.value).toBe('Johnny');
        expect(latest!.txId).toBe(newTxId);
      });
    });

    it('should preserve old version for MVCC time-travel', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert and then update
        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await store.insertTriple(triple);

        const newTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        const newValue: TypedObject = { type: ObjectType.STRING, value: 'Johnny' };
        await store.updateTriple(triple.subject, triple.predicate, newValue, newTxId);

        // Query the database directly to verify MVCC
        const result = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE subject = ? AND predicate = ?`,
          triple.subject,
          triple.predicate
        );
        const rows = [...result];
        expect(rows[0].count).toBe(2); // Both versions should exist
      });
    });
  });

  describe('getLatestTriple', () => {
    it('should return the most recent version based on timestamp', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/1');
        const predicate = createPredicate('name');

        // Insert multiple versions with different timestamps
        const tx1 = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
        const tx2 = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        const tx3 = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAX');

        // Insert them in order (note: use numbers for SQLite, not bigint)
        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          subject,
          predicate,
          ObjectType.STRING,
          'First',
          1000,
          tx1
        );

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          subject,
          predicate,
          ObjectType.STRING,
          'Latest',
          3000,
          tx3
        );

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          subject,
          predicate,
          ObjectType.STRING,
          'Second',
          2000,
          tx2
        );

        const latest = await store.getLatestTriple(subject, predicate);
        expect(latest).not.toBeNull();
        expect(latest!.object.value).toBe('Latest');
        expect(latest!.txId).toBe(tx3);
      });
    });

    it('should return null for non-existent triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/nonexistent');
        const predicate = createPredicate('name');

        const latest = await store.getLatestTriple(subject, predicate);
        expect(latest).toBeNull();
      });
    });
  });

  describe('deleteTriple (soft delete via tombstone)', () => {
    it('should create a tombstone for deleted triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert a triple
        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await store.insertTriple(triple);

        // Delete it
        const deleteTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        await store.deleteTriple(triple.subject, triple.predicate, deleteTxId);

        // getLatestTriple should return the tombstone (NULL type indicates deletion)
        const latest = await store.getLatestTriple(triple.subject, triple.predicate);
        expect(latest).not.toBeNull();
        expect(latest!.object.type).toBe(ObjectType.NULL);
        expect(latest!.txId).toBe(deleteTxId);
      });
    });

    it('should preserve history after delete (MVCC)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await store.insertTriple(triple);

        const deleteTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        await store.deleteTriple(triple.subject, triple.predicate, deleteTxId);

        // Both original and tombstone should exist
        const result = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE subject = ? AND predicate = ?`,
          triple.subject,
          triple.predicate
        );
        const rows = [...result];
        expect(rows[0].count).toBe(2);
      });
    });
  });

  describe('deleteEntity', () => {
    it('should delete all triples for an entity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert multiple triples for one entity
        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'John'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
          createTestTriple('1', 'email', ObjectType.STRING, 'john@example.com'),
          createTestTriple('2', 'name', ObjectType.STRING, 'Jane'), // Different entity
        ];
        await store.insertTriples(triples);

        // Delete all triples for entity 1
        const deleteTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        const subject = createEntityId('https://example.com/entity/1');
        await store.deleteEntity(subject, deleteTxId);

        // All predicates for entity 1 should have tombstones
        const entity1Triples = await store.getTriples(subject);
        // After deleteEntity, the latest versions should all be tombstones
        const latestName = await store.getLatestTriple(subject, createPredicate('name'));
        const latestAge = await store.getLatestTriple(subject, createPredicate('age'));
        const latestEmail = await store.getLatestTriple(subject, createPredicate('email'));

        expect(latestName!.object.type).toBe(ObjectType.NULL);
        expect(latestAge!.object.type).toBe(ObjectType.NULL);
        expect(latestEmail!.object.type).toBe(ObjectType.NULL);

        // Entity 2 should be unaffected
        const entity2Name = await store.getLatestTriple(
          createEntityId('https://example.com/entity/2'),
          createPredicate('name')
        );
        expect(entity2Name!.object.value).toBe('Jane');
      });
    });
  });

  describe('exists', () => {
    it('should return true for existing entity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await store.insertTriple(triple);

        const exists = await store.exists(triple.subject);
        expect(exists).toBe(true);
      });
    });

    it('should return false for non-existent entity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/nonexistent');
        const exists = await store.exists(subject);
        expect(exists).toBe(false);
      });
    });

    it('should return false for deleted entity (only tombstones)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert and then delete
        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await store.insertTriple(triple);

        const deleteTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
        await store.deleteEntity(triple.subject, deleteTxId);

        // exists should return false because all latest versions are tombstones
        const exists = await store.exists(triple.subject);
        expect(exists).toBe(false);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('tripleToRow / rowToTriple conversion', () => {
  describe('tripleToRow', () => {
    it('should convert STRING triple to row', () => {
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      const row = tripleToRow(triple);

      expect(row.subject).toBe(triple.subject);
      expect(row.predicate).toBe(triple.predicate);
      expect(row.obj_type).toBe(ObjectType.STRING);
      expect(row.obj_string).toBe('John');
      // timestamp is converted to number for SQLite compatibility
      expect(row.timestamp).toBe(Number(triple.timestamp));
      expect(row.tx_id).toBe(triple.txId);
    });

    it('should convert INT64 triple to row', () => {
      const triple = createTestTriple('1', 'age', ObjectType.INT64, 30n);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.INT64);
      // BigInt is converted to number for SQLite compatibility
      expect(row.obj_int64).toBe(30);
    });

    it('should convert FLOAT64 triple to row', () => {
      const triple = createTestTriple('1', 'score', ObjectType.FLOAT64, 3.14);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.FLOAT64);
      expect(row.obj_float64).toBe(3.14);
    });

    it('should convert BOOL triple to row', () => {
      const triple = createTestTriple('1', 'active', ObjectType.BOOL, true);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.BOOL);
      expect(row.obj_bool).toBe(1);
    });

    it('should convert REF triple to row', () => {
      const value = createEntityId('https://example.com/person/2');
      const triple = createTestTriple('1', 'knows', ObjectType.REF, value);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.REF);
      expect(row.obj_ref).toBe(value);
    });

    it('should convert GEO_POINT triple to row', () => {
      const value = { lat: 37.7749, lng: -122.4194 };
      const triple = createTestTriple('1', 'location', ObjectType.GEO_POINT, value);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.GEO_POINT);
      expect(row.obj_lat).toBe(37.7749);
      expect(row.obj_lng).toBe(-122.4194);
    });

    it('should convert TIMESTAMP triple to row', () => {
      const timestamp = BigInt(1705320000000);
      const triple = createTestTriple('1', 'createdAt', ObjectType.TIMESTAMP, timestamp);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.TIMESTAMP);
      // BigInt is converted to number for SQLite compatibility
      expect(row.obj_timestamp).toBe(1705320000000);
    });

    it('should convert JSON triple to row', () => {
      const value = { key: 'value' };
      const triple = createTestTriple('1', 'metadata', ObjectType.JSON, value);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.JSON);
      expect(row.obj_binary).toBeInstanceOf(Uint8Array);
      // The JSON should be serialized to binary
    });

    it('should convert BINARY triple to row', () => {
      const value = new Uint8Array([1, 2, 3]);
      const triple = createTestTriple('1', 'data', ObjectType.BINARY, value);
      const row = tripleToRow(triple);

      expect(row.obj_type).toBe(ObjectType.BINARY);
      expect(row.obj_binary).toEqual(value);
    });
  });

  describe('rowToTriple', () => {
    it('should convert STRING row to triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const subject = 'https://example.com/entity/1';
        const predicate = 'name';
        const txId = '01ARZ3NDEKTSV4RRFFQ69G5FAV';
        const timestamp = Date.now();

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          subject,
          predicate,
          ObjectType.STRING,
          'John',
          timestamp,
          txId
        );

        const result = sql.exec(`SELECT * FROM triples LIMIT 1`);
        const rows = [...result];
        const triple = rowToTriple(rows[0]);

        expect(triple.subject).toBe(subject);
        expect(triple.predicate).toBe(predicate);
        expect(triple.object.type).toBe(ObjectType.STRING);
        expect(triple.object.value).toBe('John');
        expect(triple.txId).toBe(txId);
      });
    });

    it('should convert INT64 row to triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_int64, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/entity/1',
          'age',
          ObjectType.INT64,
          30,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec(`SELECT * FROM triples LIMIT 1`);
        const rows = [...result];
        const triple = rowToTriple(rows[0]);

        expect(triple.object.type).toBe(ObjectType.INT64);
        expect(triple.object.value).toBe(30n);
      });
    });

    it('should convert GEO_POINT row to triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_lat, obj_lng, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          'https://example.com/place/1',
          'location',
          ObjectType.GEO_POINT,
          37.7749,
          -122.4194,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec(`SELECT * FROM triples LIMIT 1`);
        const rows = [...result];
        const triple = rowToTriple(rows[0]);

        expect(triple.object.type).toBe(ObjectType.GEO_POINT);
        expect(triple.object.value).toBeDefined();
        expect(triple.object.value!.lat).toBeCloseTo(37.7749, 4);
        expect(triple.object.value!.lng).toBeCloseTo(-122.4194, 4);
      });
    });

    it('should convert BOOL row to triple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_bool, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/entity/1',
          'active',
          ObjectType.BOOL,
          1,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec(`SELECT * FROM triples LIMIT 1`);
        const rows = [...result];
        const triple = rowToTriple(rows[0]);

        expect(triple.object.type).toBe(ObjectType.BOOL);
        expect(triple.object.value).toBe(true);
      });
    });
  });
});

// Note: HTTP endpoint tests may cause isolated storage issues due to async cleanup
// in the Cloudflare vitest-pool-workers. This is a known infrastructure issue.
// @see https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
// These tests are skipped for now but demonstrate the intended API.
describe('ShardDO CRUD Endpoints', () => {
  describe('POST /triples', () => {
    it('should insert a single triple via HTTP', async () => {
      const stub = getUniqueShardStub();

      const triple = {
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: Date.now(),
        txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const response = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triple),
      });

      expect(response.status).toBe(201);
      const result = await response.json();
      expect(result.success).toBe(true);
    });

    it('should insert multiple triples via HTTP', async () => {
      const stub = getUniqueShardStub();

      const triples = [
        {
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          object: { type: ObjectType.STRING, value: 'John' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        },
        {
          subject: 'https://example.com/entity/1',
          predicate: 'age',
          object: { type: ObjectType.INT64, value: '30' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        },
      ];

      const response = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triples),
      });

      expect(response.status).toBe(201);
      const result = await response.json();
      expect(result.success).toBe(true);
      expect(result.count).toBe(2);
    });
  });

  describe('GET /triples/:subject', () => {
    it('should get all triples for a subject', async () => {
      const stub = getUniqueShardStub();

      // First insert some triples
      const triples = [
        {
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          object: { type: ObjectType.STRING, value: 'John' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        },
        {
          subject: 'https://example.com/entity/1',
          predicate: 'age',
          object: { type: ObjectType.INT64, value: '30' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        },
      ];

      const insertResponse = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triples),
      });
      // Consume response body to ensure DO operations complete
      await insertResponse.json();

      // Then get them
      const subject = encodeURIComponent('https://example.com/entity/1');
      const response = await stub.fetch(`http://localhost/triples/${subject}`);

      expect(response.status).toBe(200);
      const result = await response.json();
      expect(result.triples.length).toBe(2);
    });

    it('should return empty array for non-existent subject', async () => {
      const stub = getUniqueShardStub();

      const subject = encodeURIComponent('https://example.com/entity/nonexistent');
      const response = await stub.fetch(`http://localhost/triples/${subject}`);

      expect(response.status).toBe(200);
      const result = await response.json();
      expect(result.triples).toEqual([]);
    });
  });

  describe('GET /triples/:subject/:predicate', () => {
    it('should get a specific triple', async () => {
      const stub = getUniqueShardStub();

      // Insert a triple
      const triple = {
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: Date.now(),
        txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const insertResponse = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triple),
      });
      // Consume response body to ensure DO operations complete
      await insertResponse.json();

      // Get it
      const subject = encodeURIComponent('https://example.com/entity/1');
      const response = await stub.fetch(`http://localhost/triples/${subject}/name`);

      expect(response.status).toBe(200);
      const result = await response.json();
      expect(result.triple).not.toBeNull();
      expect(result.triple.object.value).toBe('John');
    });

    it('should return 404 for non-existent triple', async () => {
      const stub = getUniqueShardStub();

      const subject = encodeURIComponent('https://example.com/entity/nonexistent');
      const response = await stub.fetch(`http://localhost/triples/${subject}/name`);

      expect(response.status).toBe(404);
      // Consume response body to ensure DO operations complete
      await response.text();
    });
  });

  describe('PUT /triples/:subject/:predicate', () => {
    it('should update a triple (MVCC)', async () => {
      const stub = getUniqueShardStub();

      // Insert initial triple
      const triple = {
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: Date.now(),
        txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const insertResponse = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triple),
      });
      // Consume response body to ensure DO operations complete
      await insertResponse.json();

      // Update it
      const subject = encodeURIComponent('https://example.com/entity/1');
      const updateBody = {
        object: { type: ObjectType.STRING, value: 'Johnny' },
        txId: '01ARZ3NDEKTSV4RRFFQ69G5FAW',
      };

      const response = await stub.fetch(`http://localhost/triples/${subject}/name`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updateBody),
      });

      expect(response.status).toBe(200);
      // Consume response body to ensure DO operations complete
      await response.json();

      // Verify the update
      const getResponse = await stub.fetch(`http://localhost/triples/${subject}/name`);
      const result = await getResponse.json();
      expect(result.triple.object.value).toBe('Johnny');
    });
  });

  describe('DELETE /triples/:subject/:predicate', () => {
    it('should delete a triple (soft delete)', async () => {
      const stub = getUniqueShardStub();

      // Insert a triple
      const triple = {
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: Date.now(),
        txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const insertResponse = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triple),
      });
      // Consume response body to ensure DO operations complete
      await insertResponse.json();

      // Delete it
      const subject = encodeURIComponent('https://example.com/entity/1');
      const response = await stub.fetch(`http://localhost/triples/${subject}/name?txId=01ARZ3NDEKTSV4RRFFQ69G5FAW`, {
        method: 'DELETE',
      });

      expect(response.status).toBe(200);
      // Consume response body to ensure DO operations complete
      await response.json();

      // Verify deletion (latest should be tombstone)
      const getResponse = await stub.fetch(`http://localhost/triples/${subject}/name`);
      const result = await getResponse.json();
      expect(result.triple.object.type).toBe(ObjectType.NULL);
    });
  });

  describe('DELETE /entities/:subject', () => {
    it('should delete all triples for an entity', async () => {
      const stub = getUniqueShardStub();

      // Insert multiple triples
      const triples = [
        {
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          object: { type: ObjectType.STRING, value: 'John' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        },
        {
          subject: 'https://example.com/entity/1',
          predicate: 'age',
          object: { type: ObjectType.INT64, value: '30' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        },
      ];

      const insertResponse = await stub.fetch('http://localhost/triples', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(triples),
      });
      // Consume response body to ensure DO operations complete
      await insertResponse.json();

      // Delete the entity
      const subject = encodeURIComponent('https://example.com/entity/1');
      const response = await stub.fetch(`http://localhost/entities/${subject}?txId=01ARZ3NDEKTSV4RRFFQ69G5FAW`, {
        method: 'DELETE',
      });

      expect(response.status).toBe(200);
      // Consume response body to ensure DO operations complete
      await response.json();

      // Verify all triples are tombstoned
      const getResponse = await stub.fetch(`http://localhost/triples/${subject}`);
      const result = await getResponse.json();

      // All latest triples should be tombstones (NULL type)
      for (const triple of result.triples) {
        // The triples returned should show the latest versions which are tombstones
        expect(triple.object.type).toBe(ObjectType.NULL);
      }
    });
  });
});
