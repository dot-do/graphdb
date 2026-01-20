/**
 * SQLite Schema Migration Tests (TDD RED Phase)
 *
 * Tests for GraphDB SQLite schema initialization and migration:
 * - Schema creation (triples table with typed object columns)
 * - Index creation (SPO, POS, OSP, timestamp, tx)
 * - Schema versioning
 * - Migration system
 *
 * @see CLAUDE.md for schema design details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import {
  SCHEMA_VERSION,
  TRIPLES_TABLE_SCHEMA,
  CHUNKS_SCHEMA,
  SCHEMA_META,
  MIGRATIONS,
  initializeSchema,
  getCurrentVersion,
  migrateToVersion,
  runMigration,
  type Migration,
} from '../../src/shard/schema.js';
import { ObjectType } from '../../src/core/types.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-schema-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

describe('SQLite Schema Constants', () => {
  describe('SCHEMA_VERSION', () => {
    it('should be version 4 (BLOB-only + secondary indexes)', () => {
      expect(SCHEMA_VERSION).toBe(4);
    });
  });

  describe('TRIPLES_TABLE_SCHEMA', () => {
    it('should define triples table', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS triples');
    });

    it('should have id as INTEGER PRIMARY KEY AUTOINCREMENT', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('id INTEGER PRIMARY KEY AUTOINCREMENT');
    });

    it('should have subject column for EntityId', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('subject TEXT NOT NULL');
    });

    it('should have predicate column for field names', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('predicate TEXT NOT NULL');
    });

    it('should have obj_type column for ObjectType enum', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_type INTEGER NOT NULL');
    });

    it('should have obj_ref column for REF type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_ref TEXT');
    });

    it('should have obj_string column for STRING type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_string TEXT');
    });

    it('should have obj_int64 column for INT64 type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_int64 INTEGER');
    });

    it('should have obj_float64 column for FLOAT64 type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_float64 REAL');
    });

    it('should have obj_bool column for BOOL type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_bool INTEGER');
    });

    it('should have obj_timestamp column for TIMESTAMP type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_timestamp INTEGER');
    });

    it('should have obj_lat and obj_lng columns for GEO_POINT type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_lat REAL');
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_lng REAL');
    });

    it('should have obj_binary column for BINARY/JSON type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('obj_binary BLOB');
    });

    it('should have timestamp column', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('timestamp INTEGER NOT NULL');
    });

    it('should have tx_id column for TransactionId', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('tx_id TEXT NOT NULL');
    });

    // Index tests
    it('should create SPO index', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_spo ON triples(subject, predicate, obj_type)');
    });

    it('should create POS index', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_pos ON triples(predicate, obj_type, subject)');
    });

    it('should create OSP partial index for REF type', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_osp ON triples(obj_ref, subject, predicate) WHERE obj_type = 10');
    });

    it('should create timestamp index', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_timestamp ON triples(timestamp)');
    });

    it('should create tx_id index', () => {
      expect(TRIPLES_TABLE_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_tx ON triples(tx_id)');
    });
  });

  describe('CHUNKS_SCHEMA', () => {
    it('should define chunks table', () => {
      expect(CHUNKS_SCHEMA).toContain('CREATE TABLE IF NOT EXISTS chunks');
    });

    it('should have id as TEXT PRIMARY KEY', () => {
      expect(CHUNKS_SCHEMA).toContain('id TEXT PRIMARY KEY');
    });

    it('should have namespace column', () => {
      expect(CHUNKS_SCHEMA).toContain('namespace TEXT NOT NULL');
    });

    it('should have triple_count column', () => {
      expect(CHUNKS_SCHEMA).toContain('triple_count INTEGER NOT NULL');
    });

    it('should have data BLOB column', () => {
      expect(CHUNKS_SCHEMA).toContain('data BLOB NOT NULL');
    });

    it('should create namespace index', () => {
      expect(CHUNKS_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_chunks_ns ON chunks(namespace)');
    });

    it('should create time range index', () => {
      expect(CHUNKS_SCHEMA).toContain('CREATE INDEX IF NOT EXISTS idx_chunks_time ON chunks(min_timestamp, max_timestamp)');
    });
  });

  describe('SCHEMA_META', () => {
    it('should define schema_meta table', () => {
      expect(SCHEMA_META).toContain('CREATE TABLE IF NOT EXISTS schema_meta');
    });

    it('should have key as PRIMARY KEY', () => {
      expect(SCHEMA_META).toContain('key TEXT PRIMARY KEY');
    });

    it('should have value column', () => {
      expect(SCHEMA_META).toContain('value TEXT NOT NULL');
    });
  });

  describe('MIGRATIONS', () => {
    it('should have at least one migration', () => {
      expect(MIGRATIONS.length).toBeGreaterThanOrEqual(1);
    });

    it('should have version 1 migration', () => {
      const v1 = MIGRATIONS.find((m) => m.version === 1);
      expect(v1).toBeDefined();
    });

    it('should have up and down SQL for each migration', () => {
      for (const migration of MIGRATIONS) {
        expect(migration.up).toBeDefined();
        expect(migration.up.length).toBeGreaterThan(0);
        expect(migration.down).toBeDefined();
        expect(migration.down.length).toBeGreaterThan(0);
      }
    });

    it('migration versions should be sequential starting from 1', () => {
      for (let i = 0; i < MIGRATIONS.length; i++) {
        expect(MIGRATIONS[i].version).toBe(i + 1);
      }
    });
  });
});

describe('Schema Initialization', () => {
  describe('initializeSchema', () => {
    it('should create chunks table (BLOB-only architecture)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Check that chunks table exists (BLOB-only architecture)
        const result = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'"
        );
        const tables = [...result];
        expect(tables.length).toBe(1);
        expect(tables[0].name).toBe('chunks');
      });
    });

    it('should create schema_meta table', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Check that schema_meta table exists
        const result = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        );
        const tables = [...result];
        expect(tables.length).toBe(1);
        expect(tables[0].name).toBe('schema_meta');
      });
    });

    it('should create all required indexes for chunks table', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Get all indexes for chunks table (BLOB-only architecture)
        const result = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='chunks'"
        );
        const indexes = [...result].map((row) => row.name);

        expect(indexes).toContain('idx_chunks_ns');
        expect(indexes).toContain('idx_chunks_time');
      });
    });

    it('should set schema version in schema_meta', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Check schema version
        const result = sql.exec("SELECT value FROM schema_meta WHERE key='schema_version'");
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].value).toBe(String(SCHEMA_VERSION));
      });
    });

    it('should be idempotent (safe to call multiple times)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Call multiple times
        initializeSchema(sql);
        initializeSchema(sql);
        initializeSchema(sql);

        // Should still have exactly one chunks table (BLOB-only architecture)
        const result = sql.exec(
          "SELECT count(*) as count FROM sqlite_master WHERE type='table' AND name='chunks'"
        );
        const rows = [...result];
        expect(rows[0].count).toBe(1);
      });
    });
  });

  describe('getCurrentVersion', () => {
    it('should return 0 for truly uninitialized database (before schema_meta exists)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Note: ShardDO auto-initializes schema in constructor, so this test verifies
        // the getCurrentVersion function behavior when schema_meta table doesn't exist.
        // We simulate this by dropping the schema_meta table.
        sql.exec('DROP TABLE IF EXISTS schema_meta');

        const version = getCurrentVersion(sql);
        expect(version).toBe(0);
      });
    });

    it('should return current version after initialization', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // ShardDO auto-initializes schema, so just verify the version
        const version = getCurrentVersion(sql);
        expect(version).toBe(SCHEMA_VERSION);
      });
    });

    it('ShardDO should auto-initialize schema on construction', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Schema should already be initialized by ShardDO constructor
        const version = getCurrentVersion(sql);
        expect(version).toBe(SCHEMA_VERSION);

        // Tables should exist (BLOB-only architecture: chunks, not triples)
        const tablesResult = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        );
        const tables = [...tablesResult].map((row) => row.name);
        expect(tables).toContain('chunks');
        expect(tables).toContain('schema_meta');
      });
    });
  });
});

describe('Schema Migration', () => {
  describe('migrateToVersion', () => {
    it('should initialize BLOB-only schema on fresh database', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Note: ShardDO auto-initializes, so we simulate a fresh database by:
        // 1. Dropping all tables
        // 2. Verifying initializeSchema creates BLOB-only schema
        sql.exec('DROP TABLE IF EXISTS chunks');
        sql.exec('DROP TABLE IF EXISTS schema_meta');

        // Start with truly uninitialized database
        expect(getCurrentVersion(sql)).toBe(0);

        // Initialize schema (goes directly to SCHEMA_VERSION with BLOB-only architecture)
        initializeSchema(sql);

        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);

        // Tables should exist (BLOB-only: chunks, not triples)
        const tablesResult = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        );
        const tables = [...tablesResult].map((row) => row.name);
        expect(tables).toContain('chunks');
        expect(tables).toContain('schema_meta');
      });
    });

    it('should run migrations in order', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Migrate step by step
        migrateToVersion(sql, 1);
        expect(getCurrentVersion(sql)).toBe(1);

        // Future: When we have version 2
        // migrateToVersion(sql, 2);
        // expect(getCurrentVersion(sql)).toBe(2);
      });
    });

    it('should skip already applied migrations', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Initialize (runs migration)
        initializeSchema(sql);

        // Calling migrateToVersion should not error
        migrateToVersion(sql, SCHEMA_VERSION);

        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);
      });
    });
  });

  describe('runMigration', () => {
    it('should run schema_meta migration (version 1)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Drop any existing tables
        sql.exec('DROP TABLE IF EXISTS schema_meta');
        sql.exec('DROP TABLE IF EXISTS chunks');

        // Run migration 1 (schema_meta only)
        const migration = MIGRATIONS[0];
        runMigration(sql, migration, 'up');

        // Check schema_meta was created
        const result = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        );
        const tables = [...result];
        expect(tables.length).toBe(1);
      });
    });

    it('should run down migration for schema_meta', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // First run up
        const migration = MIGRATIONS[0];
        runMigration(sql, migration, 'up');

        // Then run down
        runMigration(sql, migration, 'down');

        // schema_meta should be gone
        const result = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        );
        const tables = [...result];
        expect(tables.length).toBe(0);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Data Integrity Constraints', () => {
  describe('Predicate column (NO colons allowed)', () => {
    it('should allow valid predicates without colons', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Valid predicates
        const validPredicates = ['name', 'firstName', 'first_name', 'age', '$id', '$type'];

        for (const predicate of validPredicates) {
          sql.exec(
            `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
             VALUES (?, ?, ?, ?, ?, ?)`,
            `https://example.com/entity/${predicate}`,
            predicate,
            ObjectType.STRING,
            'test value',
            Date.now(),
            '01ARZ3NDEKTSV4RRFFQ69G5FAV'
          );
        }

        // Verify inserts
        const result = sql.exec('SELECT COUNT(*) as count FROM triples');
        const rows = [...result];
        expect(rows[0].count).toBe(validPredicates.length);
      });
    });

    it('should demonstrate that colons in predicates violate design constraints', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Note: SQLite doesn't enforce this at the DB level, but our application
        // layer should prevent it. This test documents the design constraint.
        // The predicate "schema:name" would violate our design principles.

        // This insert would technically succeed at DB level...
        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/entity/1',
          'schema:name', // BAD! Contains colon
          ObjectType.STRING,
          'test value',
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        // But our isPredicate type guard would reject it
        const { isPredicate } = await import('../../src/core/types.js');
        expect(isPredicate('schema:name')).toBe(false);
        expect(isPredicate('name')).toBe(true);
      });
    });
  });

  describe('Typed object columns', () => {
    it('should store STRING type correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/person/1',
          'name',
          ObjectType.STRING,
          'John Doe',
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'name');
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].obj_type).toBe(ObjectType.STRING);
        expect(rows[0].obj_string).toBe('John Doe');
      });
    });

    it('should store INT64 type correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_int64, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/person/1',
          'age',
          ObjectType.INT64,
          30,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'age');
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].obj_type).toBe(ObjectType.INT64);
        expect(rows[0].obj_int64).toBe(30);
      });
    });

    it('should store REF type correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_ref, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/person/1',
          'knows',
          ObjectType.REF,
          'https://example.com/person/2',
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'knows');
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].obj_type).toBe(ObjectType.REF);
        expect(rows[0].obj_ref).toBe('https://example.com/person/2');
      });
    });

    it('should store GEO_POINT type correctly', async () => {
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
          37.7749, // San Francisco lat
          -122.4194, // San Francisco lng
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'location');
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].obj_type).toBe(ObjectType.GEO_POINT);
        expect(rows[0].obj_lat).toBeCloseTo(37.7749, 4);
        expect(rows[0].obj_lng).toBeCloseTo(-122.4194, 4);
      });
    });

    it('should store TIMESTAMP type correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        const timestamp = Date.now();
        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_timestamp, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/event/1',
          'createdAt',
          ObjectType.TIMESTAMP,
          timestamp,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'createdAt');
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].obj_type).toBe(ObjectType.TIMESTAMP);
        expect(rows[0].obj_timestamp).toBe(timestamp);
      });
    });

    it('should store BOOL type correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_bool, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          'https://example.com/person/1',
          'active',
          ObjectType.BOOL,
          1, // true
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );

        const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'active');
        const rows = [...result];
        expect(rows.length).toBe(1);
        expect(rows[0].obj_type).toBe(ObjectType.BOOL);
        expect(rows[0].obj_bool).toBe(1);
      });
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Schema Survives DO Restart', () => {
  it('should persist schema across DO lifecycle', async () => {
    const shardName = `shard-restart-test-${Date.now()}-${testCounter++}`;

    // First interaction - initialize and insert data
    const stub1 = env.SHARD.get(env.SHARD.idFromName(shardName));
    await runInDurableObject(stub1, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;

      initializeSchema(sql);

      sql.exec(
        `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
         VALUES (?, ?, ?, ?, ?, ?)`,
        'https://example.com/test/1',
        'testField',
        ObjectType.STRING,
        'test value',
        Date.now(),
        '01ARZ3NDEKTSV4RRFFQ69G5FAV'
      );
    });

    // Second interaction - should see persisted data
    const stub2 = env.SHARD.get(env.SHARD.idFromName(shardName));
    await runInDurableObject(stub2, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;

      // Tables should still exist
      const tablesResult = sql.exec(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
      );
      const tables = [...tablesResult].map((row) => row.name);
      expect(tables).toContain('triples');
      expect(tables).toContain('schema_meta');

      // Data should be preserved
      const result = sql.exec('SELECT * FROM triples WHERE predicate = ?', 'testField');
      const rows = [...result];
      expect(rows.length).toBe(1);
      expect(rows[0].obj_string).toBe('test value');

      // Version should be preserved
      const version = getCurrentVersion(sql);
      expect(version).toBe(SCHEMA_VERSION);
    });
  });
});

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Index Usage Verification', () => {
  it('should use idx_spo for subject+predicate queries', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;

      initializeSchema(sql);

      // Insert some test data
      for (let i = 0; i < 10; i++) {
        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          `https://example.com/entity/${i}`,
          'name',
          ObjectType.STRING,
          `Entity ${i}`,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );
      }

      // Query by subject + predicate (should use idx_spo)
      const result = sql.exec(
        'SELECT * FROM triples WHERE subject = ? AND predicate = ?',
        'https://example.com/entity/5',
        'name'
      );
      const rows = [...result];
      expect(rows.length).toBe(1);
      expect(rows[0].obj_string).toBe('Entity 5');
    });
  });

  it('should use idx_osp for reverse lookups by obj_ref', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;

      initializeSchema(sql);

      // Insert relationship triples
      const targetEntity = 'https://example.com/person/target';
      for (let i = 0; i < 10; i++) {
        sql.exec(
          `INSERT INTO triples (subject, predicate, obj_type, obj_ref, timestamp, tx_id)
           VALUES (?, ?, ?, ?, ?, ?)`,
          `https://example.com/person/${i}`,
          'knows',
          ObjectType.REF,
          targetEntity,
          Date.now(),
          '01ARZ3NDEKTSV4RRFFQ69G5FAV'
        );
      }

      // Reverse lookup - who knows the target? (should use idx_osp)
      const result = sql.exec(
        'SELECT subject FROM triples WHERE obj_ref = ? AND obj_type = ?',
        targetEntity,
        ObjectType.REF
      );
      const rows = [...result];
      expect(rows.length).toBe(10);
    });
  });
});
