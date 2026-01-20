/**
 * Triple Index Tests
 *
 * @deprecated These tests use the deprecated triple-indexes.ts module which relies on
 * individual SQLite rows. The triples table has been removed in BLOB-only
 * architecture (schema v3). These tests are SKIPPED.
 *
 * For current tests, see test/shard/chunk-store.test.ts
 *
 * Tests for SPO, POS, OSP index query helpers:
 * - SPO: Forward traversal (get all predicates/values for entity X)
 * - POS: Predicate queries (get all entities with predicate P having value V)
 * - OSP: Reverse lookups (who references entity X?)
 *
 * @see CLAUDE.md for architecture details
 * @see test/shard/chunk-store.test.ts for current BLOB-only tests
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { initializeSchema } from '../../src/shard/schema.js';
import { ObjectType, createEntityId, createPredicate } from '../../src/core/types.js';
import type { EntityId, Predicate } from '../../src/core/types.js';
import type { Triple } from '../../src/core/triple.js';
import {
  querySPO,
  queryPOS,
  queryOSP,
  batchQuerySPO,
  batchQueryOSP,
  type SPOQuery,
  type POSQuery,
  type OSPQuery,
} from '../../src/index/triple-indexes.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-index-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test data helpers
const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV';

function insertTriple(
  sql: SqlStorage,
  subject: string,
  predicate: string,
  objType: ObjectType,
  value: { ref?: string; string?: string; int64?: number; float64?: number; bool?: number }
): void {
  sql.exec(
    `INSERT INTO triples (subject, predicate, obj_type, obj_ref, obj_string, obj_int64, obj_float64, obj_bool, timestamp, tx_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    subject,
    predicate,
    objType,
    value.ref ?? null,
    value.string ?? null,
    value.int64 ?? null,
    value.float64 ?? null,
    value.bool ?? null,
    Date.now(),
    VALID_TX_ID
  );
}

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('SPO Index - Forward Traversal', () => {
  describe('querySPO', () => {
    it('should return all triples for a given subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert test data for a single subject
        const subjectId = 'https://example.com/person/1';
        insertTriple(sql, subjectId, 'name', ObjectType.STRING, { string: 'John Doe' });
        insertTriple(sql, subjectId, 'age', ObjectType.INT64, { int64: 30 });
        insertTriple(sql, subjectId, 'active', ObjectType.BOOL, { bool: 1 });

        // Insert data for another subject (should not be returned)
        insertTriple(sql, 'https://example.com/person/2', 'name', ObjectType.STRING, { string: 'Jane Doe' });

        const result = await querySPO(sql, {
          subject: subjectId as EntityId,
        });

        expect(result.triples.length).toBe(3);
        expect(result.hasMore).toBe(false);

        // Verify all returned triples have the correct subject
        for (const triple of result.triples) {
          expect(triple.subject).toBe(subjectId);
        }

        // Verify predicate diversity
        const predicates = result.triples.map((t) => t.predicate);
        expect(predicates).toContain('name');
        expect(predicates).toContain('age');
        expect(predicates).toContain('active');
      });
    });

    it('should filter by predicate when specified', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const subjectId = 'https://example.com/person/1';
        insertTriple(sql, subjectId, 'name', ObjectType.STRING, { string: 'John Doe' });
        insertTriple(sql, subjectId, 'age', ObjectType.INT64, { int64: 30 });
        insertTriple(sql, subjectId, 'email', ObjectType.STRING, { string: 'john@example.com' });

        const result = await querySPO(sql, {
          subject: subjectId as EntityId,
          predicate: 'name' as Predicate,
        });

        expect(result.triples.length).toBe(1);
        expect(result.triples[0].predicate).toBe('name');
        expect(result.hasMore).toBe(false);
      });
    });

    it('should support pagination with limit and cursor', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const subjectId = 'https://example.com/person/1';

        // Insert 10 triples
        for (let i = 0; i < 10; i++) {
          insertTriple(sql, subjectId, `prop${i}`, ObjectType.STRING, { string: `value${i}` });
        }

        // First page
        const page1 = await querySPO(sql, {
          subject: subjectId as EntityId,
          limit: 3,
        });

        expect(page1.triples.length).toBe(3);
        expect(page1.hasMore).toBe(true);
        expect(page1.cursor).toBeDefined();

        // Second page
        const page2 = await querySPO(sql, {
          subject: subjectId as EntityId,
          limit: 3,
          cursor: page1.cursor,
        });

        expect(page2.triples.length).toBe(3);
        expect(page2.hasMore).toBe(true);
        expect(page2.cursor).toBeDefined();

        // Verify no overlap between pages
        const page1Predicates = page1.triples.map((t) => t.predicate);
        const page2Predicates = page2.triples.map((t) => t.predicate);
        const overlap = page1Predicates.filter((p) => page2Predicates.includes(p));
        expect(overlap.length).toBe(0);
      });
    });

    it('should return empty array for non-existent subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const result = await querySPO(sql, {
          subject: 'https://example.com/nonexistent' as EntityId,
        });

        expect(result.triples.length).toBe(0);
        expect(result.hasMore).toBe(false);
        expect(result.cursor).toBeUndefined();
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('POS Index - Predicate Queries', () => {
  describe('queryPOS', () => {
    it('should return all subjects with a given predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert multiple subjects with 'name' predicate
        for (let i = 0; i < 5; i++) {
          insertTriple(sql, `https://example.com/person/${i}`, 'name', ObjectType.STRING, { string: `Person ${i}` });
        }

        // Insert one with different predicate
        insertTriple(sql, 'https://example.com/person/99', 'age', ObjectType.INT64, { int64: 25 });

        const result = await queryPOS(sql, {
          predicate: 'name' as Predicate,
        });

        expect(result.triples.length).toBe(5);
        expect(result.hasMore).toBe(false);

        // Verify all returned triples have 'name' predicate
        for (const triple of result.triples) {
          expect(triple.predicate).toBe('name');
        }
      });
    });

    it('should filter by exact value when specified', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        insertTriple(sql, 'https://example.com/person/1', 'name', ObjectType.STRING, { string: 'John Doe' });
        insertTriple(sql, 'https://example.com/person/2', 'name', ObjectType.STRING, { string: 'Jane Doe' });
        insertTriple(sql, 'https://example.com/person/3', 'name', ObjectType.STRING, { string: 'John Doe' });

        const result = await queryPOS(sql, {
          predicate: 'name' as Predicate,
          value: 'John Doe',
          valueOp: '=',
        });

        expect(result.triples.length).toBe(2);
        for (const triple of result.triples) {
          expect(triple.object.value).toBe('John Doe');
        }
      });
    });

    it('should support comparison operators for numeric values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        for (let i = 0; i < 10; i++) {
          insertTriple(sql, `https://example.com/person/${i}`, 'age', ObjectType.INT64, { int64: i * 10 });
        }

        // Test greater than
        const gtResult = await queryPOS(sql, {
          predicate: 'age' as Predicate,
          value: 50,
          valueOp: '>',
        });

        // Should return ages 60, 70, 80, 90
        expect(gtResult.triples.length).toBe(4);
        for (const triple of gtResult.triples) {
          expect(triple.object.value).toBeGreaterThan(50n);
        }

        // Test less than or equal
        const lteResult = await queryPOS(sql, {
          predicate: 'age' as Predicate,
          value: 30,
          valueOp: '<=',
        });

        // Should return ages 0, 10, 20, 30
        expect(lteResult.triples.length).toBe(4);
        for (const triple of lteResult.triples) {
          expect(triple.object.value).toBeLessThanOrEqual(30n);
        }
      });
    });

    it('should support pagination', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert 10 subjects with 'type' predicate
        for (let i = 0; i < 10; i++) {
          insertTriple(sql, `https://example.com/entity/${i}`, 'type', ObjectType.STRING, { string: 'Entity' });
        }

        const page1 = await queryPOS(sql, {
          predicate: 'type' as Predicate,
          limit: 4,
        });

        expect(page1.triples.length).toBe(4);
        expect(page1.hasMore).toBe(true);
        expect(page1.cursor).toBeDefined();

        const page2 = await queryPOS(sql, {
          predicate: 'type' as Predicate,
          limit: 4,
          cursor: page1.cursor,
        });

        expect(page2.triples.length).toBe(4);
        expect(page2.hasMore).toBe(true);
      });
    });

    it('should return empty for non-existent predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        insertTriple(sql, 'https://example.com/person/1', 'name', ObjectType.STRING, { string: 'John' });

        const result = await queryPOS(sql, {
          predicate: 'nonexistent' as Predicate,
        });

        expect(result.triples.length).toBe(0);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('OSP Index - Reverse Lookups', () => {
  describe('queryOSP', () => {
    it('should return all triples referencing a target entity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targetId = 'https://example.com/person/target';

        // Multiple entities referencing the target
        for (let i = 0; i < 5; i++) {
          insertTriple(sql, `https://example.com/person/${i}`, 'knows', ObjectType.REF, { ref: targetId });
        }

        // A different reference (should not be returned)
        insertTriple(sql, 'https://example.com/person/99', 'knows', ObjectType.REF, {
          ref: 'https://example.com/person/other',
        });

        const result = await queryOSP(sql, {
          objectRef: targetId as EntityId,
        });

        expect(result.triples.length).toBe(5);
        expect(result.hasMore).toBe(false);

        // Verify all returned triples reference the target
        for (const triple of result.triples) {
          expect(triple.object.value).toBe(targetId);
          expect(triple.object.type).toBe(ObjectType.REF);
        }
      });
    });

    it('should filter by subject when specified', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targetId = 'https://example.com/person/target';
        const sourceId = 'https://example.com/person/source1';

        insertTriple(sql, sourceId, 'knows', ObjectType.REF, { ref: targetId });
        insertTriple(sql, sourceId, 'follows', ObjectType.REF, { ref: targetId });
        insertTriple(sql, 'https://example.com/person/source2', 'knows', ObjectType.REF, { ref: targetId });

        const result = await queryOSP(sql, {
          objectRef: targetId as EntityId,
          subject: sourceId as EntityId,
        });

        expect(result.triples.length).toBe(2);
        for (const triple of result.triples) {
          expect(triple.subject).toBe(sourceId);
        }
      });
    });

    it('should filter by predicate when specified', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targetId = 'https://example.com/person/target';

        insertTriple(sql, 'https://example.com/person/1', 'knows', ObjectType.REF, { ref: targetId });
        insertTriple(sql, 'https://example.com/person/2', 'follows', ObjectType.REF, { ref: targetId });
        insertTriple(sql, 'https://example.com/person/3', 'knows', ObjectType.REF, { ref: targetId });

        const result = await queryOSP(sql, {
          objectRef: targetId as EntityId,
          predicate: 'knows' as Predicate,
        });

        expect(result.triples.length).toBe(2);
        for (const triple of result.triples) {
          expect(triple.predicate).toBe('knows');
        }
      });
    });

    it('should only work with REF type triples', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targetId = 'https://example.com/person/target';

        // Insert a REF triple
        insertTriple(sql, 'https://example.com/person/1', 'knows', ObjectType.REF, { ref: targetId });

        // Insert a STRING triple with similar content (should NOT be returned)
        insertTriple(sql, 'https://example.com/person/2', 'description', ObjectType.STRING, { string: targetId });

        const result = await queryOSP(sql, {
          objectRef: targetId as EntityId,
        });

        // Only the REF triple should be returned
        expect(result.triples.length).toBe(1);
        expect(result.triples[0].object.type).toBe(ObjectType.REF);
      });
    });

    it('should support pagination', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targetId = 'https://example.com/entity/target';

        // Insert 10 references to target
        for (let i = 0; i < 10; i++) {
          insertTriple(sql, `https://example.com/entity/${i}`, 'references', ObjectType.REF, { ref: targetId });
        }

        const page1 = await queryOSP(sql, {
          objectRef: targetId as EntityId,
          limit: 3,
        });

        expect(page1.triples.length).toBe(3);
        expect(page1.hasMore).toBe(true);
        expect(page1.cursor).toBeDefined();

        const page2 = await queryOSP(sql, {
          objectRef: targetId as EntityId,
          limit: 3,
          cursor: page1.cursor,
        });

        expect(page2.triples.length).toBe(3);
        expect(page2.hasMore).toBe(true);
      });
    });

    it('should return empty for non-referenced entity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const result = await queryOSP(sql, {
          objectRef: 'https://example.com/nonexistent' as EntityId,
        });

        expect(result.triples.length).toBe(0);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Batch Operations', () => {
  describe('batchQuerySPO', () => {
    it('should efficiently query multiple subjects', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert data for multiple subjects
        const subjects: EntityId[] = [];
        for (let i = 0; i < 5; i++) {
          const subjectId = `https://example.com/person/${i}` as EntityId;
          subjects.push(subjectId);
          insertTriple(sql, subjectId, 'name', ObjectType.STRING, { string: `Person ${i}` });
          insertTriple(sql, subjectId, 'age', ObjectType.INT64, { int64: 20 + i });
        }

        const results = await batchQuerySPO(sql, subjects);

        expect(results.size).toBe(5);

        for (const subjectId of subjects) {
          const triples = results.get(subjectId);
          expect(triples).toBeDefined();
          expect(triples!.length).toBe(2);

          for (const triple of triples!) {
            expect(triple.subject).toBe(subjectId);
          }
        }
      });
    });

    it('should return empty arrays for non-existent subjects', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const existingSubject = 'https://example.com/person/1' as EntityId;
        const nonExistentSubject = 'https://example.com/person/nonexistent' as EntityId;

        insertTriple(sql, existingSubject, 'name', ObjectType.STRING, { string: 'John' });

        const results = await batchQuerySPO(sql, [existingSubject, nonExistentSubject]);

        expect(results.size).toBe(2);
        expect(results.get(existingSubject)!.length).toBe(1);
        expect(results.get(nonExistentSubject)!.length).toBe(0);
      });
    });

    it('should handle empty subject list', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const results = await batchQuerySPO(sql, []);

        expect(results.size).toBe(0);
      });
    });
  });

  describe('batchQueryOSP', () => {
    it('should efficiently query references to multiple targets', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targets: EntityId[] = [];
        for (let i = 0; i < 3; i++) {
          const targetId = `https://example.com/target/${i}` as EntityId;
          targets.push(targetId);

          // Each target is referenced by 2 entities
          insertTriple(sql, `https://example.com/source/${i}a`, 'knows', ObjectType.REF, { ref: targetId });
          insertTriple(sql, `https://example.com/source/${i}b`, 'follows', ObjectType.REF, { ref: targetId });
        }

        const results = await batchQueryOSP(sql, targets);

        expect(results.size).toBe(3);

        for (const targetId of targets) {
          const triples = results.get(targetId);
          expect(triples).toBeDefined();
          expect(triples!.length).toBe(2);

          for (const triple of triples!) {
            expect(triple.object.value).toBe(targetId);
          }
        }
      });
    });

    it('should return empty arrays for non-referenced targets', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const referencedTarget = 'https://example.com/target/1' as EntityId;
        const nonReferencedTarget = 'https://example.com/target/nonexistent' as EntityId;

        insertTriple(sql, 'https://example.com/source/1', 'knows', ObjectType.REF, { ref: referencedTarget });

        const results = await batchQueryOSP(sql, [referencedTarget, nonReferencedTarget]);

        expect(results.size).toBe(2);
        expect(results.get(referencedTarget)!.length).toBe(1);
        expect(results.get(nonReferencedTarget)!.length).toBe(0);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Index Usage Verification', () => {
  describe('EXPLAIN QUERY PLAN', () => {
    it('should use idx_spo for SPO queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert some data to have statistics
        for (let i = 0; i < 20; i++) {
          insertTriple(sql, `https://example.com/entity/${i}`, 'name', ObjectType.STRING, { string: `Entity ${i}` });
        }

        // Check query plan for SPO query
        const plan = sql.exec(
          `EXPLAIN QUERY PLAN SELECT * FROM triples WHERE subject = ? AND predicate = ?`,
          'https://example.com/entity/5',
          'name'
        );
        const planRows = [...plan];

        // Verify the plan uses idx_spo
        const planText = planRows.map((r) => String(r.detail)).join(' ');
        expect(planText).toMatch(/idx_spo|USING INDEX/i);
      });
    });

    it('should use idx_pos for POS queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert some data
        for (let i = 0; i < 20; i++) {
          insertTriple(sql, `https://example.com/entity/${i}`, 'type', ObjectType.STRING, { string: 'Entity' });
        }

        // Check query plan for POS query
        const plan = sql.exec(`EXPLAIN QUERY PLAN SELECT * FROM triples WHERE predicate = ? AND obj_type = ?`, 'type', ObjectType.STRING);
        const planRows = [...plan];

        const planText = planRows.map((r) => String(r.detail)).join(' ');
        expect(planText).toMatch(/idx_pos|USING INDEX/i);
      });
    });

    it('should use idx_osp for OSP queries (REF type)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const targetId = 'https://example.com/target';

        // Insert REF triples
        for (let i = 0; i < 20; i++) {
          insertTriple(sql, `https://example.com/source/${i}`, 'knows', ObjectType.REF, { ref: targetId });
        }

        // Check query plan for OSP query
        const plan = sql.exec(
          `EXPLAIN QUERY PLAN SELECT * FROM triples WHERE obj_ref = ? AND obj_type = ?`,
          targetId,
          ObjectType.REF
        );
        const planRows = [...plan];

        const planText = planRows.map((r) => String(r.detail)).join(' ');
        expect(planText).toMatch(/idx_osp|USING INDEX/i);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Triple Conversion', () => {
  it('should return proper Triple objects with all fields', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const subjectId = 'https://example.com/person/1';
      insertTriple(sql, subjectId, 'name', ObjectType.STRING, { string: 'John Doe' });

      const result = await querySPO(sql, {
        subject: subjectId as EntityId,
      });

      expect(result.triples.length).toBe(1);
      const triple = result.triples[0];

      // Verify Triple structure
      expect(triple.subject).toBe(subjectId);
      expect(triple.predicate).toBe('name');
      expect(triple.object).toBeDefined();
      expect(triple.object.type).toBe(ObjectType.STRING);
      expect(triple.object.value).toBe('John Doe');
      expect(triple.timestamp).toBeDefined();
      expect(typeof triple.timestamp).toBe('bigint');
      expect(triple.txId).toBe(VALID_TX_ID);
    });
  });

  it('should correctly convert REF type objects', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const subjectId = 'https://example.com/person/1';
      const targetId = 'https://example.com/person/2';
      insertTriple(sql, subjectId, 'knows', ObjectType.REF, { ref: targetId });

      const result = await querySPO(sql, {
        subject: subjectId as EntityId,
      });

      const triple = result.triples[0];
      expect(triple.object.type).toBe(ObjectType.REF);
      expect(triple.object.value).toBe(targetId);
    });
  });

  it('should correctly convert INT64 type objects', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const subjectId = 'https://example.com/person/1';
      insertTriple(sql, subjectId, 'age', ObjectType.INT64, { int64: 30 });

      const result = await querySPO(sql, {
        subject: subjectId as EntityId,
      });

      const triple = result.triples[0];
      expect(triple.object.type).toBe(ObjectType.INT64);
      expect(triple.object.value).toBe(30n);
    });
  });

  it('should correctly convert BOOL type objects', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const subjectId = 'https://example.com/person/1';
      insertTriple(sql, subjectId, 'active', ObjectType.BOOL, { bool: 1 });

      const result = await querySPO(sql, {
        subject: subjectId as EntityId,
      });

      const triple = result.triples[0];
      expect(triple.object.type).toBe(ObjectType.BOOL);
      expect(triple.object.value).toBe(true);
    });
  });

  it('should correctly convert FLOAT64 type objects', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const subjectId = 'https://example.com/sensor/1';
      insertTriple(sql, subjectId, 'temperature', ObjectType.FLOAT64, { float64: 98.6 });

      const result = await querySPO(sql, {
        subject: subjectId as EntityId,
      });

      const triple = result.triples[0];
      expect(triple.object.type).toBe(ObjectType.FLOAT64);
      expect(triple.object.value).toBeCloseTo(98.6, 1);
    });
  });
});
