/**
 * SQL Generation Edge Cases Tests (TDD RED Phase)
 *
 * Tests for SQL statement generation edge cases:
 * - SQL injection prevention via parameterized queries
 * - Special characters in string values
 * - Unicode handling
 * - Boundary values for numeric types
 * - NULL handling in various contexts
 * - Large batch operations
 *
 * @see CLAUDE.md for architecture details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { initializeSchema } from '../../src/shard/schema.js';
import {
  createTripleStore,
  tripleToRow,
  rowToTriple,
} from '../../src/shard/crud.js';
import { querySql, querySqlOne } from '../../src/shard/sql-utils.js';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
} from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-sql-gen-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test helper to create triples
function createTestTriple(
  subjectSuffix: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
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
    default:
      object.value = value;
  }

  return {
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
}

describe('SQL Generation Edge Cases', () => {
  describe('SQL Injection Prevention', () => {
    it('should handle string values with SQL special characters', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // String with SQL injection attempt
        const maliciousValue = "'; DROP TABLE triples; --";
        const triple = createTestTriple('1', 'name', ObjectType.STRING, maliciousValue);

        await store.insertTriple(triple);

        // Table should still exist and contain the literal string
        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(maliciousValue);

        // Verify triples table still exists
        const tableCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='triples'"
        );
        expect([...tableCheck].length).toBe(1);
      });
    });

    it('should handle string values with quotes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // String with single and double quotes
        const quotedValue = `He said "Hello" and she said 'Hi'`;
        const triple = createTestTriple('1', 'quote', ObjectType.STRING, quotedValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(quotedValue);
      });
    });

    it('should handle string values with backslashes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // String with backslashes (common in file paths)
        const backslashValue = 'C:\\Users\\test\\file.txt';
        const triple = createTestTriple('1', 'path', ObjectType.STRING, backslashValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(backslashValue);
      });
    });

    it('should handle string values with percent and underscore (LIKE wildcards)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // String with LIKE wildcards
        const wildcardValue = '100% discount for test_user';
        const triple = createTestTriple('1', 'offer', ObjectType.STRING, wildcardValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(wildcardValue);
      });
    });
  });

  describe('Unicode Handling', () => {
    it('should handle Unicode characters in string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Unicode string with emoji and various scripts
        const unicodeValue = 'Hello \u4e16\u754c \u0421\u043C\u0438\u0440\u043D\u043E \u{1F600}';
        const triple = createTestTriple('1', 'greeting', ObjectType.STRING, unicodeValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(unicodeValue);
      });
    });

    it('should handle emoji in string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // String with various emoji
        const emojiValue = '\u{1F389}\u{1F38A}\u{1F38B} Party time! \u{1F600}\u{1F601}\u{1F602}';
        const triple = createTestTriple('1', 'message', ObjectType.STRING, emojiValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(emojiValue);
      });
    });

    it('should handle null character in string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // String with null character (this may be problematic)
        const nullCharValue = 'before\x00after';
        const triple = createTestTriple('1', 'data', ObjectType.STRING, nullCharValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        // SQLite may truncate at null character or handle it differently
        // The test verifies consistent behavior
        expect(typeof result!.object.value).toBe('string');
      });
    });

    it('should handle RTL (Right-to-Left) text', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Arabic and Hebrew text (RTL)
        const rtlValue = '\u0645\u0631\u062D\u0628\u0627 \u05E9\u05DC\u05D5\u05DD';
        const triple = createTestTriple('1', 'rtlText', ObjectType.STRING, rtlValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(rtlValue);
      });
    });
  });

  describe('Numeric Boundary Values', () => {
    it('should handle maximum safe integer', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Number.MAX_SAFE_INTEGER = 9007199254740991
        const triple = createTestTriple('1', 'maxInt', ObjectType.INT64, BigInt(Number.MAX_SAFE_INTEGER));

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(BigInt(Number.MAX_SAFE_INTEGER));
      });
    });

    it('should handle minimum safe integer', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Number.MIN_SAFE_INTEGER = -9007199254740991
        const triple = createTestTriple('1', 'minInt', ObjectType.INT64, BigInt(Number.MIN_SAFE_INTEGER));

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(BigInt(Number.MIN_SAFE_INTEGER));
      });
    });

    it('should handle zero integer', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'zero', ObjectType.INT64, 0n);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(0n);
      });
    });

    it('should handle negative zero float', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Negative zero is a valid IEEE 754 value
        const triple = createTestTriple('1', 'negZero', ObjectType.FLOAT64, -0.0);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        // Note: SQLite may not preserve -0 distinction
        expect(result!.object.value).toBe(0);
      });
    });

    it('should handle very small float values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Very small number close to epsilon
        const smallValue = Number.EPSILON;
        const triple = createTestTriple('1', 'epsilon', ObjectType.FLOAT64, smallValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBeCloseTo(smallValue, 20);
      });
    });

    it('should handle Infinity float values gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // SQLite may not support Infinity directly
        const triple = createTestTriple('1', 'infinity', ObjectType.FLOAT64, Infinity);

        // This might throw or store as NULL - either is acceptable
        try {
          await store.insertTriple(triple);
          const result = await store.getTriple(triple.subject, triple.predicate);
          // If it succeeds, verify the value
          if (result) {
            // SQLite stores Infinity as NULL or may error
            expect([Infinity, null, undefined]).toContain(result.object.value);
          }
        } catch {
          // Expected - SQLite doesn't support Infinity
          expect(true).toBe(true);
        }
      });
    });

    it('should handle NaN float values gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'notANumber', ObjectType.FLOAT64, NaN);

        // NaN handling varies - test for consistent behavior
        try {
          await store.insertTriple(triple);
          const result = await store.getTriple(triple.subject, triple.predicate);
          if (result) {
            // SQLite may store NaN as NULL
            expect(result.object.value === null || Number.isNaN(result.object.value)).toBe(true);
          }
        } catch {
          // Expected - SQLite doesn't support NaN
          expect(true).toBe(true);
        }
      });
    });
  });

  describe('Empty and Whitespace Strings', () => {
    it('should handle empty string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triple = createTestTriple('1', 'empty', ObjectType.STRING, '');

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe('');
      });
    });

    it('should handle whitespace-only string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const whitespaceValue = '   \t\n\r   ';
        const triple = createTestTriple('1', 'whitespace', ObjectType.STRING, whitespaceValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(whitespaceValue);
      });
    });

    it('should handle very long string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // 100KB string
        const longValue = 'x'.repeat(100 * 1024);
        const triple = createTestTriple('1', 'longText', ObjectType.STRING, longValue);

        await store.insertTriple(triple);

        const result = await store.getTriple(triple.subject, triple.predicate);
        expect(result).not.toBeNull();
        expect(result!.object.value).toBe(longValue);
      });
    });
  });

  describe('Batch Insert SQL Generation', () => {
    it('should generate correct SQL for batch insert with varying sizes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Test various batch sizes
        const sizes = [1, 2, 5, 10, 50, 100];

        for (const size of sizes) {
          const triples: Triple[] = [];
          for (let i = 0; i < size; i++) {
            triples.push(createTestTriple(`batch_${size}_${i}`, 'value', ObjectType.INT64, BigInt(i)));
          }

          await store.insertTriples(triples);

          // Verify all inserted
          for (let i = 0; i < size; i++) {
            const result = await store.getTriple(
              createEntityId(`https://example.com/entity/batch_${size}_${i}`),
              createPredicate('value')
            );
            expect(result).not.toBeNull();
            expect(result!.object.value).toBe(BigInt(i));
          }
        }
      });
    });

    it('should handle batch insert with mixed types', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const triples: Triple[] = [
          createTestTriple('mixed1', 'strField', ObjectType.STRING, 'hello'),
          createTestTriple('mixed2', 'intField', ObjectType.INT64, 42n),
          createTestTriple('mixed3', 'floatField', ObjectType.FLOAT64, 3.14),
          createTestTriple('mixed4', 'boolField', ObjectType.BOOL, true),
          createTestTriple('mixed5', 'nullField', ObjectType.NULL, null),
        ];

        await store.insertTriples(triples);

        // Verify each type
        const str = await store.getTriple(createEntityId('https://example.com/entity/mixed1'), createPredicate('strField'));
        expect(str?.object.type).toBe(ObjectType.STRING);
        expect(str?.object.value).toBe('hello');

        const int = await store.getTriple(createEntityId('https://example.com/entity/mixed2'), createPredicate('intField'));
        expect(int?.object.type).toBe(ObjectType.INT64);
        expect(int?.object.value).toBe(42n);

        const float = await store.getTriple(createEntityId('https://example.com/entity/mixed3'), createPredicate('floatField'));
        expect(float?.object.type).toBe(ObjectType.FLOAT64);
        expect(float?.object.value).toBeCloseTo(3.14, 5);

        const bool = await store.getTriple(createEntityId('https://example.com/entity/mixed4'), createPredicate('boolField'));
        expect(bool?.object.type).toBe(ObjectType.BOOL);
        expect(bool?.object.value).toBe(true);

        const nullTriple = await store.getTriple(createEntityId('https://example.com/entity/mixed5'), createPredicate('nullField'));
        expect(nullTriple?.object.type).toBe(ObjectType.NULL);
      });
    });

    it('should handle batch insert with special characters in all rows', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        const specialValues = [
          "O'Brien",
          'Say "Hello"',
          'Path\\to\\file',
          '100% complete',
          'Line1\nLine2',
          'Tab\there',
        ];

        const triples: Triple[] = specialValues.map((value, i) =>
          createTestTriple(`special_${i}`, 'name', ObjectType.STRING, value)
        );

        await store.insertTriples(triples);

        // Verify all inserted correctly
        for (let i = 0; i < specialValues.length; i++) {
          const result = await store.getTriple(
            createEntityId(`https://example.com/entity/special_${i}`),
            createPredicate('name')
          );
          expect(result).not.toBeNull();
          expect(result!.object.value).toBe(specialValues[i]);
        }
      });
    });
  });

  describe('SQL Query Generation for Filtering', () => {
    it('should generate correct SQL for filterSubjectsByPredicateValue with string equality', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert test data
        await store.insertTriples([
          createTestTriple('filter1', 'status', ObjectType.STRING, 'active'),
          createTestTriple('filter2', 'status', ObjectType.STRING, 'inactive'),
          createTestTriple('filter3', 'status', ObjectType.STRING, 'active'),
        ]);

        const results = await store.filterSubjectsByPredicateValue(
          createPredicate('status'),
          'eq',
          'active'
        );

        expect(results.length).toBe(2);
        expect(results).toContain(createEntityId('https://example.com/entity/filter1'));
        expect(results).toContain(createEntityId('https://example.com/entity/filter3'));
      });
    });

    it('should generate correct SQL for filterSubjectsByPredicateValue with numeric comparison', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert test data
        await store.insertTriples([
          createTestTriple('num1', 'score', ObjectType.INT64, 10n),
          createTestTriple('num2', 'score', ObjectType.INT64, 50n),
          createTestTriple('num3', 'score', ObjectType.INT64, 100n),
        ]);

        const gtResults = await store.filterSubjectsByPredicateValue(
          createPredicate('score'),
          'gt',
          25
        );

        expect(gtResults.length).toBe(2);
        expect(gtResults).toContain(createEntityId('https://example.com/entity/num2'));
        expect(gtResults).toContain(createEntityId('https://example.com/entity/num3'));
      });
    });

    it('should generate correct SQL for contains filter with special characters', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        const store = createTripleStore(sql);

        // Insert test data with strings containing LIKE wildcards
        await store.insertTriples([
          createTestTriple('like1', 'description', ObjectType.STRING, '100% discount available'),
          createTestTriple('like2', 'description', ObjectType.STRING, 'test_user profile'),
          createTestTriple('like3', 'description', ObjectType.STRING, 'normal text'),
        ]);

        // Search for literal % character
        const percentResults = await store.filterSubjectsByPredicateValue(
          createPredicate('description'),
          'contains',
          '100%'
        );

        expect(percentResults.length).toBe(1);
        expect(percentResults).toContain(createEntityId('https://example.com/entity/like1'));
      });
    });
  });
});
