/**
 * SQL Utility Functions Tests
 *
 * Tests for type-safe wrappers around SqlStorage operations:
 * - querySql<T>() - returns typed array
 * - querySqlOne<T>() - returns single result or null
 * - Edge cases: empty results, multiple rows, null handling
 *
 * @see src/shard/sql-utils.ts for implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { querySql, querySqlOne, SqlRowValidationError, type SqlStorageValue } from '../../src/shard/sql-utils.js';
import { initializeSchema } from '../../src/shard/schema.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-sql-utils-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test row types
interface TestUserRow {
  id: number;
  name: string;
  email: string | null;
  age: number | null;
  active: number; // SQLite stores boolean as 0/1
}

interface TestSimpleRow {
  count: number;
}

interface TestNullableRow {
  value: string | null;
}

describe('SQL Utility Functions', () => {
  describe('querySql<T>()', () => {
    it('should return typed array of results', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Create test table
        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            active INTEGER NOT NULL DEFAULT 1
          )
        `);

        // Insert test data
        sql.exec(
          'INSERT INTO test_users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)',
          1,
          'Alice',
          'alice@example.com',
          30,
          1
        );
        sql.exec(
          'INSERT INTO test_users (id, name, email, age, active) VALUES (?, ?, ?, ?, ?)',
          2,
          'Bob',
          'bob@example.com',
          25,
          1
        );

        // Query using querySql
        const users = querySql<TestUserRow>(sql, 'SELECT * FROM test_users ORDER BY id');

        // Verify results
        expect(users).toHaveLength(2);
        expect(users[0]!.id).toBe(1);
        expect(users[0]!.name).toBe('Alice');
        expect(users[0]!.email).toBe('alice@example.com');
        expect(users[0]!.age).toBe(30);
        expect(users[0]!.active).toBe(1);
        expect(users[1]!.id).toBe(2);
        expect(users[1]!.name).toBe('Bob');
      });
    });

    it('should return empty array when no results', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Create empty test table
        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_empty (
            id INTEGER PRIMARY KEY,
            value TEXT
          )
        `);

        // Query empty table
        const results = querySql<{ id: number; value: string }>(
          sql,
          'SELECT * FROM test_empty'
        );

        expect(results).toHaveLength(0);
        expect(results).toEqual([]);
      });
    });

    it('should support parameterized queries with string params', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_params (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT
          )
        `);

        sql.exec('INSERT INTO test_params (id, name, category) VALUES (?, ?, ?)', 1, 'Item A', 'cat1');
        sql.exec('INSERT INTO test_params (id, name, category) VALUES (?, ?, ?)', 2, 'Item B', 'cat2');
        sql.exec('INSERT INTO test_params (id, name, category) VALUES (?, ?, ?)', 3, 'Item C', 'cat1');

        const results = querySql<{ id: number; name: string; category: string }>(
          sql,
          'SELECT * FROM test_params WHERE category = ? ORDER BY id',
          'cat1'
        );

        expect(results).toHaveLength(2);
        expect(results[0]!.name).toBe('Item A');
        expect(results[1]!.name).toBe('Item C');
      });
    });

    it('should support parameterized queries with number params', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_nums (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_nums (id, value) VALUES (?, ?)', 1, 100);
        sql.exec('INSERT INTO test_nums (id, value) VALUES (?, ?)', 2, 200);
        sql.exec('INSERT INTO test_nums (id, value) VALUES (?, ?)', 3, 150);

        const results = querySql<{ id: number; value: number }>(
          sql,
          'SELECT * FROM test_nums WHERE value > ? ORDER BY id',
          120
        );

        expect(results).toHaveLength(2);
        expect(results[0]!.value).toBe(200);
        expect(results[1]!.value).toBe(150);
      });
    });

    it('should support parameterized queries with null params', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_nulls (
            id INTEGER PRIMARY KEY,
            optional_value TEXT
          )
        `);

        sql.exec('INSERT INTO test_nulls (id, optional_value) VALUES (?, ?)', 1, 'has value');
        sql.exec('INSERT INTO test_nulls (id, optional_value) VALUES (?, ?)', 2, null);
        sql.exec('INSERT INTO test_nulls (id, optional_value) VALUES (?, ?)', 3, null);

        const results = querySql<{ id: number; optional_value: string | null }>(
          sql,
          'SELECT * FROM test_nulls WHERE optional_value IS NULL ORDER BY id'
        );

        expect(results).toHaveLength(2);
        expect(results[0]!.id).toBe(2);
        expect(results[0]!.optional_value).toBeNull();
        expect(results[1]!.id).toBe(3);
      });
    });

    it('should support multiple parameters', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_multi (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            score INTEGER NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_multi (id, name, score) VALUES (?, ?, ?)', 1, 'Alice', 85);
        sql.exec('INSERT INTO test_multi (id, name, score) VALUES (?, ?, ?)', 2, 'Bob', 92);
        sql.exec('INSERT INTO test_multi (id, name, score) VALUES (?, ?, ?)', 3, 'Charlie', 78);

        const results = querySql<{ id: number; name: string; score: number }>(
          sql,
          'SELECT * FROM test_multi WHERE name = ? AND score > ?',
          'Bob',
          80
        );

        expect(results).toHaveLength(1);
        expect(results[0]!.name).toBe('Bob');
        expect(results[0]!.score).toBe(92);
      });
    });

    it('should handle aggregate queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_agg (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_agg (id, value) VALUES (?, ?)', 1, 10);
        sql.exec('INSERT INTO test_agg (id, value) VALUES (?, ?)', 2, 20);
        sql.exec('INSERT INTO test_agg (id, value) VALUES (?, ?)', 3, 30);

        const results = querySql<TestSimpleRow>(sql, 'SELECT COUNT(*) as count FROM test_agg');

        expect(results).toHaveLength(1);
        expect(results[0]!.count).toBe(3);
      });
    });

    it('should handle bigint values in results', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_bigint (
            id INTEGER PRIMARY KEY,
            big_value INTEGER NOT NULL
          )
        `);

        // SQLite stores integers, query results may return bigint for large values
        // Use a value within safe integer range for insertion
        const value = 9007199254740000;
        sql.exec('INSERT INTO test_bigint (id, big_value) VALUES (?, ?)', 1, value);

        const results = querySql<{ id: number; big_value: number | bigint }>(
          sql,
          'SELECT * FROM test_bigint WHERE id = ?',
          1
        );

        expect(results).toHaveLength(1);
        expect(results[0]!.id).toBe(1);
        // Value should be retrieved correctly
        expect(Number(results[0]!.big_value)).toBe(value);
      });
    });

    it('should handle Uint8Array parameters', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_binary (
            id INTEGER PRIMARY KEY,
            data BLOB NOT NULL
          )
        `);

        const binaryData = new Uint8Array([1, 2, 3, 4, 5]);
        sql.exec('INSERT INTO test_binary (id, data) VALUES (?, ?)', 1, binaryData);

        const results = querySql<{ id: number; data: ArrayBuffer }>(
          sql,
          'SELECT * FROM test_binary WHERE id = ?',
          1
        );

        expect(results).toHaveLength(1);
        const resultData = new Uint8Array(results[0]!.data);
        expect(resultData).toEqual(binaryData);
      });
    });
  });

  describe('querySqlOne<T>()', () => {
    it('should return single typed result', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_one (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER
          )
        `);

        sql.exec('INSERT INTO test_one (id, name, value) VALUES (?, ?, ?)', 1, 'Test Item', 42);

        const result = querySqlOne<{ id: number; name: string; value: number }>(
          sql,
          'SELECT * FROM test_one WHERE id = ?',
          1
        );

        expect(result).not.toBeNull();
        expect(result!.id).toBe(1);
        expect(result!.name).toBe('Test Item');
        expect(result!.value).toBe(42);
      });
    });

    it('should return null when no results', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_none (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
          )
        `);

        const result = querySqlOne<{ id: number; name: string }>(
          sql,
          'SELECT * FROM test_none WHERE id = ?',
          999
        );

        expect(result).toBeNull();
      });
    });

    it('should return first row when multiple results exist', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_many (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            name TEXT NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_many (id, category, name) VALUES (?, ?, ?)', 1, 'A', 'First');
        sql.exec('INSERT INTO test_many (id, category, name) VALUES (?, ?, ?)', 2, 'A', 'Second');
        sql.exec('INSERT INTO test_many (id, category, name) VALUES (?, ?, ?)', 3, 'A', 'Third');

        // Without ORDER BY, returns first inserted
        const result = querySqlOne<{ id: number; category: string; name: string }>(
          sql,
          'SELECT * FROM test_many WHERE category = ? ORDER BY id',
          'A'
        );

        expect(result).not.toBeNull();
        expect(result!.id).toBe(1);
        expect(result!.name).toBe('First');
      });
    });

    it('should handle null values in result columns', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_null_cols (
            id INTEGER PRIMARY KEY,
            required_field TEXT NOT NULL,
            optional_field TEXT
          )
        `);

        sql.exec(
          'INSERT INTO test_null_cols (id, required_field, optional_field) VALUES (?, ?, ?)',
          1,
          'has value',
          null
        );

        const result = querySqlOne<{
          id: number;
          required_field: string;
          optional_field: string | null;
        }>(sql, 'SELECT * FROM test_null_cols WHERE id = ?', 1);

        expect(result).not.toBeNull();
        expect(result!.id).toBe(1);
        expect(result!.required_field).toBe('has value');
        expect(result!.optional_field).toBeNull();
      });
    });

    it('should work with aggregate queries returning single row', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_agg_one (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_agg_one (id, value) VALUES (?, ?)', 1, 10);
        sql.exec('INSERT INTO test_agg_one (id, value) VALUES (?, ?)', 2, 20);
        sql.exec('INSERT INTO test_agg_one (id, value) VALUES (?, ?)', 3, 30);

        const result = querySqlOne<{ total: number; avg_value: number }>(
          sql,
          'SELECT SUM(value) as total, AVG(value) as avg_value FROM test_agg_one'
        );

        expect(result).not.toBeNull();
        expect(result!.total).toBe(60);
        expect(result!.avg_value).toBe(20);
      });
    });

    it('should handle empty table with aggregate returning null', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_empty_agg (
            id INTEGER PRIMARY KEY,
            value INTEGER NOT NULL
          )
        `);

        // SUM/AVG on empty table returns a row with NULL values
        const result = querySqlOne<{ total: number | null }>(
          sql,
          'SELECT SUM(value) as total FROM test_empty_agg'
        );

        // SQLite returns a row with NULL for aggregate on empty table
        expect(result).not.toBeNull();
        expect(result!.total).toBeNull();
      });
    });

    it('should support lookup by primary key pattern', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_pk (
            entity_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            data TEXT
          )
        `);

        sql.exec(
          'INSERT INTO test_pk (entity_id, name, data) VALUES (?, ?, ?)',
          'https://example.com/entity/123',
          'Entity 123',
          '{"key": "value"}'
        );

        const result = querySqlOne<{ entity_id: string; name: string; data: string }>(
          sql,
          'SELECT * FROM test_pk WHERE entity_id = ?',
          'https://example.com/entity/123'
        );

        expect(result).not.toBeNull();
        expect(result!.entity_id).toBe('https://example.com/entity/123');
        expect(result!.name).toBe('Entity 123');
        expect(result!.data).toBe('{"key": "value"}');
      });
    });
  });

  describe('Runtime Type Validation', () => {
    it('should validate that query results are proper row objects', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Create test table and insert data
        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_validation (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER
          )
        `);

        sql.exec('INSERT INTO test_validation (id, name, value) VALUES (?, ?, ?)', 1, 'Test', 42);

        // Query should succeed and return validated rows
        const results = querySql<{ id: number; name: string; value: number }>(
          sql,
          'SELECT * FROM test_validation'
        );

        expect(results).toHaveLength(1);
        expect(results[0]).toBeDefined();
        expect(typeof results[0]).toBe('object');
        expect(results[0]!.id).toBe(1);
        expect(results[0]!.name).toBe('Test');
        expect(results[0]!.value).toBe(42);
      });
    });

    it('should return rows with correct structure from complex queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_complex_validation (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            amount REAL NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_complex_validation (id, category, amount) VALUES (?, ?, ?)', 1, 'A', 10.5);
        sql.exec('INSERT INTO test_complex_validation (id, category, amount) VALUES (?, ?, ?)', 2, 'A', 20.5);
        sql.exec('INSERT INTO test_complex_validation (id, category, amount) VALUES (?, ?, ?)', 3, 'B', 30.5);

        // Aggregate query should return a valid row object
        const result = querySqlOne<{ category: string; total: number; count: number }>(
          sql,
          'SELECT category, SUM(amount) as total, COUNT(*) as count FROM test_complex_validation GROUP BY category ORDER BY category LIMIT 1'
        );

        expect(result).not.toBeNull();
        expect(typeof result).toBe('object');
        expect(result!.category).toBe('A');
        expect(result!.total).toBe(31);
        expect(result!.count).toBe(2);
      });
    });

    it('should export SqlRowValidationError for error handling', () => {
      // Verify the error class is properly exported and can be used for instanceof checks
      const error = new SqlRowValidationError('Test error', 0, 'object', 'invalid');

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(SqlRowValidationError);
      expect(error.name).toBe('SqlRowValidationError');
      expect(error.rowIndex).toBe(0);
      expect(error.expectedType).toBe('object');
      expect(error.actualValue).toBe('invalid');
      expect(error.message).toBe('Test error');
    });

    it('should validate each row in multi-row results', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_multi_validation (
            id INTEGER PRIMARY KEY,
            data TEXT NOT NULL
          )
        `);

        // Insert multiple rows
        for (let i = 1; i <= 100; i++) {
          sql.exec('INSERT INTO test_multi_validation (id, data) VALUES (?, ?)', i, `Data ${i}`);
        }

        const results = querySql<{ id: number; data: string }>(
          sql,
          'SELECT * FROM test_multi_validation ORDER BY id'
        );

        expect(results).toHaveLength(100);

        // Verify all rows are proper objects
        results.forEach((row, index) => {
          expect(typeof row).toBe('object');
          expect(row).not.toBeNull();
          expect(row.id).toBe(index + 1);
          expect(row.data).toBe(`Data ${index + 1}`);
        });
      });
    });

    it('should handle rows with null column values correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_null_validation (
            id INTEGER PRIMARY KEY,
            optional_text TEXT,
            optional_int INTEGER
          )
        `);

        sql.exec('INSERT INTO test_null_validation (id, optional_text, optional_int) VALUES (?, ?, ?)', 1, null, null);

        const result = querySqlOne<{ id: number; optional_text: string | null; optional_int: number | null }>(
          sql,
          'SELECT * FROM test_null_validation WHERE id = ?',
          1
        );

        // Row should be a valid object even with null column values
        expect(result).not.toBeNull();
        expect(typeof result).toBe('object');
        expect(result!.id).toBe(1);
        expect(result!.optional_text).toBeNull();
        expect(result!.optional_int).toBeNull();
      });
    });

    it('should handle binary data columns correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_binary_validation (
            id INTEGER PRIMARY KEY,
            blob_data BLOB
          )
        `);

        const binaryData = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05]);
        sql.exec('INSERT INTO test_binary_validation (id, blob_data) VALUES (?, ?)', 1, binaryData);

        const result = querySqlOne<{ id: number; blob_data: ArrayBuffer }>(
          sql,
          'SELECT * FROM test_binary_validation WHERE id = ?',
          1
        );

        // Row should be validated as an object, even with binary column values
        expect(result).not.toBeNull();
        expect(typeof result).toBe('object');
        expect(result!.id).toBe(1);
        // Binary data is returned as ArrayBuffer
        const resultArray = new Uint8Array(result!.blob_data);
        expect(resultArray).toEqual(binaryData);
      });
    });
  });

  describe('Edge Cases', () => {
    it('should handle queries with no parameters', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_no_params (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_no_params (id, value) VALUES (1, "test")');

        // querySql with no params
        const results = querySql<{ id: number; value: string }>(
          sql,
          'SELECT * FROM test_no_params'
        );
        expect(results).toHaveLength(1);

        // querySqlOne with no params
        const result = querySqlOne<{ id: number; value: string }>(
          sql,
          'SELECT * FROM test_no_params'
        );
        expect(result).not.toBeNull();
      });
    });

    it('should handle complex WHERE conditions', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_complex (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            score INTEGER NOT NULL,
            created_at INTEGER NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_complex (id, status, score, created_at) VALUES (?, ?, ?, ?)', 1, 'active', 85, 1000);
        sql.exec('INSERT INTO test_complex (id, status, score, created_at) VALUES (?, ?, ?, ?)', 2, 'inactive', 90, 2000);
        sql.exec('INSERT INTO test_complex (id, status, score, created_at) VALUES (?, ?, ?, ?)', 3, 'active', 75, 3000);
        sql.exec('INSERT INTO test_complex (id, status, score, created_at) VALUES (?, ?, ?, ?)', 4, 'active', 95, 4000);

        const results = querySql<{ id: number; status: string; score: number; created_at: number }>(
          sql,
          'SELECT * FROM test_complex WHERE status = ? AND score >= ? AND created_at > ? ORDER BY score DESC',
          'active',
          80,
          500
        );

        expect(results).toHaveLength(2);
        expect(results[0]!.id).toBe(4); // highest score
        expect(results[1]!.id).toBe(1);
      });
    });

    it('should handle LIKE queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_like (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_like (id, name) VALUES (?, ?)', 1, 'Alice Smith');
        sql.exec('INSERT INTO test_like (id, name) VALUES (?, ?)', 2, 'Bob Jones');
        sql.exec('INSERT INTO test_like (id, name) VALUES (?, ?)', 3, 'Alice Johnson');

        const results = querySql<{ id: number; name: string }>(
          sql,
          'SELECT * FROM test_like WHERE name LIKE ? ORDER BY id',
          'Alice%'
        );

        expect(results).toHaveLength(2);
        expect(results[0]!.name).toBe('Alice Smith');
        expect(results[1]!.name).toBe('Alice Johnson');
      });
    });

    it('should handle JOIN queries', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_users_join (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
          )
        `);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_users_join (id, name) VALUES (?, ?)', 1, 'Alice');
        sql.exec('INSERT INTO test_users_join (id, name) VALUES (?, ?)', 2, 'Bob');
        sql.exec('INSERT INTO test_orders (id, user_id, amount) VALUES (?, ?, ?)', 1, 1, 100);
        sql.exec('INSERT INTO test_orders (id, user_id, amount) VALUES (?, ?, ?)', 2, 1, 200);
        sql.exec('INSERT INTO test_orders (id, user_id, amount) VALUES (?, ?, ?)', 3, 2, 150);

        const results = querySql<{ name: string; total_amount: number }>(
          sql,
          `SELECT u.name, SUM(o.amount) as total_amount
           FROM test_users_join u
           JOIN test_orders o ON u.id = o.user_id
           GROUP BY u.id
           ORDER BY total_amount DESC`
        );

        expect(results).toHaveLength(2);
        expect(results[0]!.name).toBe('Alice');
        expect(results[0]!.total_amount).toBe(300);
        expect(results[1]!.name).toBe('Bob');
        expect(results[1]!.total_amount).toBe(150);
      });
    });

    it('should handle EXISTS subquery', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_parent (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
          )
        `);

        sql.exec(`
          CREATE TABLE IF NOT EXISTS test_child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL,
            value TEXT NOT NULL
          )
        `);

        sql.exec('INSERT INTO test_parent (id, name) VALUES (?, ?)', 1, 'Parent A');
        sql.exec('INSERT INTO test_parent (id, name) VALUES (?, ?)', 2, 'Parent B');
        sql.exec('INSERT INTO test_child (id, parent_id, value) VALUES (?, ?, ?)', 1, 1, 'Child 1');

        // Find parents that have children
        const result = querySqlOne<{ id: number; name: string }>(
          sql,
          `SELECT * FROM test_parent p
           WHERE EXISTS (SELECT 1 FROM test_child c WHERE c.parent_id = p.id)`
        );

        expect(result).not.toBeNull();
        expect(result!.name).toBe('Parent A');
      });
    });
  });
});
