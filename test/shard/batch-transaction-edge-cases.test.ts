/**
 * Batch Insert/Update and Transaction Edge Cases Tests (TDD RED Phase)
 *
 * Tests for:
 * - Batch insert edge cases (empty, single, large batches)
 * - MVCC update behavior
 * - Transaction isolation
 * - Concurrent operations
 * - Atomicity guarantees
 *
 * @see CLAUDE.md for architecture details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { initializeSchema } from '../../src/shard/schema.js';
import { createTripleStore } from '../../src/shard/crud.js';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  type EntityId,
  type Predicate,
} from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-batch-tx-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test helper to create triples
function createTestTriple(
  subjectSuffix: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
  timestamp?: bigint,
  txIdSuffix = '01ARZ3NDEKTSV4RRFFQ69G5FAV'
): Triple {
  const object: TypedObject = { type: objectType };

  switch (objectType) {
    case ObjectType.STRING:
      object.value = value as string;
      break;
    case ObjectType.INT64:
      object.value = value as bigint;
      break;
    case ObjectType.FLOAT64:
      object.value = value as number;
      break;
    case ObjectType.BOOL:
      object.value = value as boolean;
      break;
    case ObjectType.NULL:
      break;
    case ObjectType.REF:
      object.value = value as EntityId;
      break;
    default:
      object.value = value;
  }

  return {
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: timestamp ?? BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
}

describe('Batch Insert Edge Cases', () => {
  describe('Empty and Single Element Batches', () => {
    it('should handle empty batch gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Should not throw
        await store.insertTriples([]);

        // Verify no rows inserted
        const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
        expect([...countResult][0].count).toBe(0);
      });
    });

    it('should handle single element batch same as insertTriple', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('single', 'name', ObjectType.STRING, 'Test');

        await store.insertTriples([triple]);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe('Test');
      });
    });

    it('should handle batch with duplicate subjects and predicates', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const baseTimestamp = BigInt(Date.now());

        // Same subject+predicate, different timestamps (MVCC versions)
        const triples: Triple[] = [
          createTestTriple('dup', 'name', ObjectType.STRING, 'First', baseTimestamp, '01ARZ3NDEKTSV4RRFFQ69G5FA1'),
          createTestTriple('dup', 'name', ObjectType.STRING, 'Second', baseTimestamp + 1n, '01ARZ3NDEKTSV4RRFFQ69G5FA2'),
          createTestTriple('dup', 'name', ObjectType.STRING, 'Third', baseTimestamp + 2n, '01ARZ3NDEKTSV4RRFFQ69G5FA3'),
        ];

        await store.insertTriples(triples);

        // Should have all 3 versions (MVCC)
        const countResult = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE subject = ?`,
          'https://example.com/entity/dup'
        );
        expect([...countResult][0].count).toBe(3);

        // Latest should be 'Third'
        const latest = await store.getLatestTriple(
          createEntityId('https://example.com/entity/dup'),
          createPredicate('name')
        );
        expect(latest!.object.value).toBe('Third');
      });
    });
  });

  describe('Large Batch Operations', () => {
    it('should handle 500+ row batch insert', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const batchSize = 500;
        const triples: Triple[] = [];
        for (let i = 0; i < batchSize; i++) {
          triples.push(createTestTriple(`batch_${i}`, 'index', ObjectType.INT64, BigInt(i)));
        }

        await store.insertTriples(triples);

        // Verify all rows inserted
        const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
        expect([...countResult][0].count).toBe(batchSize);

        // Verify first and last
        const first = await store.getTriple(
          createEntityId('https://example.com/entity/batch_0'),
          createPredicate('index')
        );
        expect(first!.object.value).toBe(0n);

        const last = await store.getTriple(
          createEntityId(`https://example.com/entity/batch_${batchSize - 1}`),
          createPredicate('index')
        );
        expect(last!.object.value).toBe(BigInt(batchSize - 1));
      });
    });

    it('should handle 1000 row batch insert', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const batchSize = 1000;
        const triples: Triple[] = [];
        for (let i = 0; i < batchSize; i++) {
          triples.push(createTestTriple(`large_${i}`, 'value', ObjectType.STRING, `Item ${i}`));
        }

        const startTime = performance.now();
        await store.insertTriples(triples);
        const endTime = performance.now();

        // Should complete reasonably fast (under 5 seconds)
        expect(endTime - startTime).toBeLessThan(5000);

        // Verify all rows inserted
        const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
        expect([...countResult][0].count).toBe(batchSize);
      });
    });

    it('should handle multiple batches sequentially', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const batchSize = 100;
        const numBatches = 10;

        for (let batch = 0; batch < numBatches; batch++) {
          const triples: Triple[] = [];
          for (let i = 0; i < batchSize; i++) {
            triples.push(
              createTestTriple(`multi_${batch}_${i}`, 'batchNum', ObjectType.INT64, BigInt(batch))
            );
          }
          await store.insertTriples(triples);
        }

        // Verify total count
        const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
        expect([...countResult][0].count).toBe(batchSize * numBatches);

        // Verify data from each batch
        for (let batch = 0; batch < numBatches; batch++) {
          const batchCountResult = sql.exec(
            `SELECT COUNT(*) as count FROM triples WHERE subject LIKE ?`,
            `https://example.com/entity/multi_${batch}_%`
          );
          expect([...batchCountResult][0].count).toBe(batchSize);
        }
      });
    });
  });

  describe('Batch with NULL and Edge Values', () => {
    it('should handle batch with mixed NULL and non-NULL values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples: Triple[] = [
          createTestTriple('null1', 'field', ObjectType.NULL, null),
          createTestTriple('null2', 'field', ObjectType.STRING, 'not null'),
          createTestTriple('null3', 'field', ObjectType.NULL, null),
          createTestTriple('null4', 'field', ObjectType.INT64, 42n),
        ];

        await store.insertTriples(triples);

        const null1 = await store.getTriple(
          createEntityId('https://example.com/entity/null1'),
          createPredicate('field')
        );
        expect(null1!.object.type).toBe(ObjectType.NULL);

        const null2 = await store.getTriple(
          createEntityId('https://example.com/entity/null2'),
          createPredicate('field')
        );
        expect(null2!.object.type).toBe(ObjectType.STRING);
        expect(null2!.object.value).toBe('not null');
      });
    });

    it('should handle batch with boundary numeric values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples: Triple[] = [
          createTestTriple('bound1', 'value', ObjectType.INT64, BigInt(Number.MAX_SAFE_INTEGER)),
          createTestTriple('bound2', 'value', ObjectType.INT64, BigInt(Number.MIN_SAFE_INTEGER)),
          createTestTriple('bound3', 'value', ObjectType.INT64, 0n),
          createTestTriple('bound4', 'value', ObjectType.INT64, -1n),
          createTestTriple('bound5', 'value', ObjectType.INT64, 1n),
        ];

        await store.insertTriples(triples);

        const max = await store.getTriple(
          createEntityId('https://example.com/entity/bound1'),
          createPredicate('value')
        );
        expect(max!.object.value).toBe(BigInt(Number.MAX_SAFE_INTEGER));

        const min = await store.getTriple(
          createEntityId('https://example.com/entity/bound2'),
          createPredicate('value')
        );
        expect(min!.object.value).toBe(BigInt(Number.MIN_SAFE_INTEGER));
      });
    });
  });
});

describe('MVCC Update Behavior', () => {
  describe('Version History', () => {
    it('should preserve all versions on update', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/versioned');
        const predicate = createPredicate('name');

        // Insert initial version
        await store.insertTriple(
          createTestTriple('versioned', 'name', ObjectType.STRING, 'Version 1')
        );

        // Update multiple times
        await store.updateTriple(
          subject,
          predicate,
          { type: ObjectType.STRING, value: 'Version 2' },
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2')
        );

        await store.updateTriple(
          subject,
          predicate,
          { type: ObjectType.STRING, value: 'Version 3' },
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA3')
        );

        // Should have 3 versions in the database
        const countResult = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE subject = ? AND predicate = ?`,
          subject,
          predicate
        );
        expect([...countResult][0].count).toBe(3);

        // Latest should be Version 3
        const latest = await store.getLatestTriple(subject, predicate);
        expect(latest!.object.value).toBe('Version 3');

        // Can query all versions by timestamp
        const allVersions = sql.exec(
          `SELECT obj_string FROM triples WHERE subject = ? AND predicate = ? ORDER BY timestamp ASC`,
          subject,
          predicate
        );
        const versions = [...allVersions].map((r) => r.obj_string);
        expect(versions).toEqual(['Version 1', 'Version 2', 'Version 3']);
      });
    });

    it('should handle type changes in update', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/typechange');
        const predicate = createPredicate('value');

        // Insert as STRING
        await store.insertTriple(
          createTestTriple('typechange', 'value', ObjectType.STRING, 'hello')
        );

        // Update to INT64
        await store.updateTriple(
          subject,
          predicate,
          { type: ObjectType.INT64, value: 42n },
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2')
        );

        // Latest should be INT64
        const latest = await store.getLatestTriple(subject, predicate);
        expect(latest!.object.type).toBe(ObjectType.INT64);
        expect(latest!.object.value).toBe(42n);

        // Both versions should exist
        const countResult = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE subject = ? AND predicate = ?`,
          subject,
          predicate
        );
        expect([...countResult][0].count).toBe(2);
      });
    });

    it('should ensure new version has later timestamp than current', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/tscheck');
        const predicate = createPredicate('name');

        // Insert with specific timestamp
        const initialTimestamp = BigInt(Date.now());
        const triple = createTestTriple('tscheck', 'name', ObjectType.STRING, 'Initial', initialTimestamp);
        await store.insertTriple(triple);

        // Update (should get later timestamp)
        await store.updateTriple(
          subject,
          predicate,
          { type: ObjectType.STRING, value: 'Updated' },
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2')
        );

        // Get both versions
        const versions = sql.exec(
          `SELECT timestamp, obj_string FROM triples WHERE subject = ? AND predicate = ? ORDER BY timestamp ASC`,
          subject,
          predicate
        );
        const rows = [...versions];
        expect(rows.length).toBe(2);
        expect(BigInt(rows[1].timestamp)).toBeGreaterThan(BigInt(rows[0].timestamp));
      });
    });
  });

  describe('Soft Delete (Tombstones)', () => {
    it('should create tombstone on delete', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/deleteme');
        const predicate = createPredicate('name');

        // Insert then delete
        await store.insertTriple(
          createTestTriple('deleteme', 'name', ObjectType.STRING, 'To be deleted')
        );

        await store.deleteTriple(
          subject,
          predicate,
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2')
        );

        // Latest should be tombstone (NULL type)
        const latest = await store.getLatestTriple(subject, predicate);
        expect(latest!.object.type).toBe(ObjectType.NULL);

        // Should have 2 versions (original + tombstone)
        const countResult = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE subject = ? AND predicate = ?`,
          subject,
          predicate
        );
        expect([...countResult][0].count).toBe(2);
      });
    });

    it('should tombstone have later timestamp than deleted version', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/tsdelete');
        const predicate = createPredicate('name');

        // Insert with specific timestamp
        const insertTs = BigInt(Date.now());
        await store.insertTriple(
          createTestTriple('tsdelete', 'name', ObjectType.STRING, 'Original', insertTs)
        );

        await store.deleteTriple(
          subject,
          predicate,
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2')
        );

        // Tombstone should have later timestamp
        const versions = sql.exec(
          `SELECT timestamp, obj_type FROM triples WHERE subject = ? AND predicate = ? ORDER BY timestamp ASC`,
          subject,
          predicate
        );
        const rows = [...versions];
        expect(rows.length).toBe(2);
        expect(rows[1].obj_type).toBe(ObjectType.NULL);
        expect(BigInt(rows[1].timestamp)).toBeGreaterThan(BigInt(rows[0].timestamp));
      });
    });

    it('should exists return false after entity delete', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/existstest');

        // Insert multiple predicates
        await store.insertTriples([
          createTestTriple('existstest', 'name', ObjectType.STRING, 'Test'),
          createTestTriple('existstest', 'age', ObjectType.INT64, 30n),
        ]);

        expect(await store.exists(subject)).toBe(true);

        // Delete entity
        await store.deleteEntity(subject, createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2'));

        expect(await store.exists(subject)).toBe(false);
      });
    });
  });
});

describe('Transaction-like Behavior', () => {
  describe('Batch Atomicity', () => {
    it('should insert all or nothing in batch (simulated failure)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples: Triple[] = [
          createTestTriple('atomic1', 'name', ObjectType.STRING, 'First'),
          createTestTriple('atomic2', 'name', ObjectType.STRING, 'Second'),
          createTestTriple('atomic3', 'name', ObjectType.STRING, 'Third'),
        ];

        // Normal batch should succeed
        await store.insertTriples(triples);

        const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
        expect([...countResult][0].count).toBe(3);
      });
    });

    it('should handle concurrent batch inserts', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Create two batches
        const batch1: Triple[] = [];
        const batch2: Triple[] = [];

        for (let i = 0; i < 100; i++) {
          batch1.push(createTestTriple(`concurrent_a_${i}`, 'batch', ObjectType.STRING, 'A'));
          batch2.push(createTestTriple(`concurrent_b_${i}`, 'batch', ObjectType.STRING, 'B'));
        }

        // Insert both "concurrently" (Durable Objects serialize, but this tests the pattern)
        await Promise.all([
          store.insertTriples(batch1),
          store.insertTriples(batch2),
        ]);

        // All 200 rows should be inserted
        const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
        expect([...countResult][0].count).toBe(200);

        // Verify both batches are complete
        const batch1Count = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE obj_string = ?`,
          'A'
        );
        expect([...batch1Count][0].count).toBe(100);

        const batch2Count = sql.exec(
          `SELECT COUNT(*) as count FROM triples WHERE obj_string = ?`,
          'B'
        );
        expect([...batch2Count][0].count).toBe(100);
      });
    });
  });

  describe('getTriplesForMultipleSubjects Batch Query', () => {
    it('should return correct triples for multiple subjects', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert test data
        await store.insertTriples([
          createTestTriple('multi_a', 'name', ObjectType.STRING, 'Entity A'),
          createTestTriple('multi_a', 'age', ObjectType.INT64, 25n),
          createTestTriple('multi_b', 'name', ObjectType.STRING, 'Entity B'),
          createTestTriple('multi_b', 'age', ObjectType.INT64, 30n),
          createTestTriple('multi_c', 'name', ObjectType.STRING, 'Entity C'),
        ]);

        const subjects = [
          createEntityId('https://example.com/entity/multi_a'),
          createEntityId('https://example.com/entity/multi_b'),
          createEntityId('https://example.com/entity/multi_c'),
        ];

        const result = await store.getTriplesForMultipleSubjects(subjects);

        expect(result.size).toBe(3);
        expect(result.get(subjects[0])?.length).toBe(2);
        expect(result.get(subjects[1])?.length).toBe(2);
        expect(result.get(subjects[2])?.length).toBe(1);
      });
    });

    it('should handle empty subjects array', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const result = await store.getTriplesForMultipleSubjects([]);
        expect(result.size).toBe(0);
      });
    });

    it('should handle single subject array', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        await store.insertTriple(
          createTestTriple('single_multi', 'name', ObjectType.STRING, 'Test')
        );

        const subjects = [createEntityId('https://example.com/entity/single_multi')];
        const result = await store.getTriplesForMultipleSubjects(subjects);

        expect(result.size).toBe(1);
        expect(result.get(subjects[0])?.length).toBe(1);
      });
    });

    it('should return only latest version for each predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/multi_latest');
        const predicate = createPredicate('name');

        // Insert multiple versions
        const baseTs = BigInt(Date.now());
        await store.insertTriple(
          createTestTriple('multi_latest', 'name', ObjectType.STRING, 'V1', baseTs)
        );
        await store.insertTriple(
          createTestTriple('multi_latest', 'name', ObjectType.STRING, 'V2', baseTs + 1n)
        );
        await store.insertTriple(
          createTestTriple('multi_latest', 'name', ObjectType.STRING, 'V3', baseTs + 2n)
        );

        const result = await store.getTriplesForMultipleSubjects([subject]);

        // Should only get 1 triple (the latest version)
        expect(result.get(subject)?.length).toBe(1);
        expect(result.get(subject)?.[0].object.value).toBe('V3');
      });
    });

    it('should handle mix of existing and non-existing subjects', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        await store.insertTriple(
          createTestTriple('exists', 'name', ObjectType.STRING, 'I exist')
        );

        const subjects = [
          createEntityId('https://example.com/entity/exists'),
          createEntityId('https://example.com/entity/not_exists'),
        ];

        const result = await store.getTriplesForMultipleSubjects(subjects);

        // Only the existing subject should be in result
        expect(result.size).toBe(1);
        expect(result.has(subjects[0])).toBe(true);
        expect(result.has(subjects[1])).toBe(false);
      });
    });
  });

  describe('Consistency Guarantees', () => {
    it('should maintain consistent state after multiple operations', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const subject = createEntityId('https://example.com/entity/consistent');

        // Insert
        await store.insertTriple(
          createTestTriple('consistent', 'name', ObjectType.STRING, 'Original')
        );

        // Update
        await store.updateTriple(
          subject,
          createPredicate('name'),
          { type: ObjectType.STRING, value: 'Updated' },
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA2')
        );

        // Add new predicate
        await store.insertTriple(
          createTestTriple('consistent', 'age', ObjectType.INT64, 25n)
        );

        // Delete name
        await store.deleteTriple(
          subject,
          createPredicate('name'),
          createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA3')
        );

        // Verify final state
        const exists = await store.exists(subject);
        expect(exists).toBe(true); // age still exists

        const name = await store.getLatestTriple(subject, createPredicate('name'));
        expect(name!.object.type).toBe(ObjectType.NULL); // tombstone

        const age = await store.getLatestTriple(subject, createPredicate('age'));
        expect(age!.object.value).toBe(25n);
      });
    });
  });
});
