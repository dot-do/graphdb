/**
 * Schema Migration Edge Cases Tests (TDD RED Phase)
 *
 * Tests for schema migration edge cases:
 * - Downgrade migrations
 * - Partial migrations (interrupted)
 * - Re-running migrations
 * - Migration with existing data
 * - Version jumps (skip versions)
 * - Concurrent migration attempts
 *
 * @see CLAUDE.md for architecture details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import {
  SCHEMA_VERSION,
  MIGRATIONS,
  SCHEMA_META,
  CHUNKS_SCHEMA,
  TRIPLES_TABLE_SCHEMA,
  initializeSchema,
  getCurrentVersion,
  migrateToVersion,
  runMigration,
} from '../../src/shard/schema.js';
import { ObjectType } from '../../src/core/types.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-migration-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

describe('Schema Migration Edge Cases', () => {
  describe('Downgrade Migrations', () => {
    it('should support downgrade from version 4 to version 3', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Initialize to version 4
        initializeSchema(sql);
        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);

        // Downgrade to version 3
        migrateToVersion(sql, 3);
        expect(getCurrentVersion(sql)).toBe(3);

        // Version 3 should NOT have triples table (BLOB-only)
        const triplesCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='triples'"
        );
        expect([...triplesCheck].length).toBe(0);

        // But should have chunks table
        const chunksCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'"
        );
        expect([...chunksCheck].length).toBe(1);
      });
    });

    it('should support downgrade from version 3 to version 2', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Initialize and downgrade
        initializeSchema(sql);
        migrateToVersion(sql, 2);
        expect(getCurrentVersion(sql)).toBe(2);

        // Version 2 should have chunks table
        const chunksCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'"
        );
        expect([...chunksCheck].length).toBe(1);
      });
    });

    it('should support downgrade all the way to version 1', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Initialize and downgrade to v1
        initializeSchema(sql);
        migrateToVersion(sql, 1);
        expect(getCurrentVersion(sql)).toBe(1);

        // Version 1 only has schema_meta
        const schemaMetaCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        );
        expect([...schemaMetaCheck].length).toBe(1);
      });
    });

    it('should be able to upgrade again after downgrade', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Initialize, downgrade, then upgrade
        initializeSchema(sql);
        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);

        migrateToVersion(sql, 2);
        expect(getCurrentVersion(sql)).toBe(2);

        migrateToVersion(sql, SCHEMA_VERSION);
        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);

        // All tables should exist after re-upgrade
        const triplesCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='triples'"
        );
        expect([...triplesCheck].length).toBe(1);
      });
    });
  });

  describe('Migration with Existing Data', () => {
    it('should preserve chunks data during version 3 to 4 upgrade', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Start with full schema then downgrade to v3 (BLOB-only)
        initializeSchema(sql);
        migrateToVersion(sql, 3);

        // Insert some chunk data at version 3
        sql.exec(
          `INSERT INTO chunks (id, namespace, triple_count, min_timestamp, max_timestamp, data, size_bytes, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          'chunk_test_1',
          'https://example.com/',
          100,
          1000,
          2000,
          new Uint8Array([1, 2, 3, 4]),
          4,
          Date.now()
        );

        // Upgrade to version 4
        migrateToVersion(sql, SCHEMA_VERSION);

        // Chunk data should still exist
        const chunkResult = sql.exec(`SELECT * FROM chunks WHERE id = ?`, 'chunk_test_1');
        const chunks = [...chunkResult];
        expect(chunks.length).toBe(1);
        expect(chunks[0].triple_count).toBe(100);
      });
    });

    it('should handle upgrade with schema_meta data intact', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Initialize schema
        initializeSchema(sql);

        // Add custom metadata
        sql.exec(
          `INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)`,
          'custom_key',
          'custom_value'
        );

        // Force a re-migration by downgrading then upgrading
        migrateToVersion(sql, 2);
        migrateToVersion(sql, SCHEMA_VERSION);

        // Custom metadata should be preserved
        const metaResult = sql.exec(`SELECT value FROM schema_meta WHERE key = ?`, 'custom_key');
        const rows = [...metaResult];
        expect(rows.length).toBe(1);
        expect(rows[0].value).toBe('custom_value');
      });
    });
  });

  describe('Re-running Migrations (Idempotency)', () => {
    it('should be safe to run initializeSchema multiple times', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Run multiple times
        initializeSchema(sql);
        initializeSchema(sql);
        initializeSchema(sql);

        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);

        // Count tables - should be same as single init
        const tableCount = sql.exec(
          "SELECT COUNT(*) as count FROM sqlite_master WHERE type='table'"
        );
        const expectedTableCount = [...tableCount][0].count;

        // Run again
        initializeSchema(sql);

        const newTableCount = sql.exec(
          "SELECT COUNT(*) as count FROM sqlite_master WHERE type='table'"
        );
        expect([...newTableCount][0].count).toBe(expectedTableCount);
      });
    });

    it('should be safe to migrateToVersion to same version', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        initializeSchema(sql);

        // Migrate to same version multiple times
        migrateToVersion(sql, SCHEMA_VERSION);
        migrateToVersion(sql, SCHEMA_VERSION);
        migrateToVersion(sql, SCHEMA_VERSION);

        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);
      });
    });

    it('should be safe to run individual migrations multiple times', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Run version 1 migration multiple times
        const migration1 = MIGRATIONS[0]!;
        runMigration(sql, migration1, 'up');
        runMigration(sql, migration1, 'up');
        runMigration(sql, migration1, 'up');

        // schema_meta should exist (once)
        const tableCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
        );
        expect([...tableCheck].length).toBe(1);
      });
    });
  });

  describe('Version Jump Scenarios', () => {
    it('should handle jump from version 1 to version 4', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Start with just version 1
        sql.exec(SCHEMA_META);
        sql.exec(
          "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', '1')"
        );

        expect(getCurrentVersion(sql)).toBe(1);

        // Jump to version 4
        migrateToVersion(sql, SCHEMA_VERSION);

        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);

        // All tables should exist
        const tables = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        );
        const tableNames = [...tables].map((t) => t.name);
        expect(tableNames).toContain('schema_meta');
        expect(tableNames).toContain('chunks');
        expect(tableNames).toContain('triples');
      });
    });

    it('should handle jump from version 2 to version 4', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Start with version 2 (schema_meta + chunks)
        sql.exec(SCHEMA_META);

        // Parse and execute CHUNKS_SCHEMA statements
        const chunksStatements = CHUNKS_SCHEMA
          .split(';')
          .map((s) => s.trim())
          .filter((s) => s.length > 0);
        for (const stmt of chunksStatements) {
          sql.exec(stmt);
        }

        sql.exec(
          "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', '2')"
        );

        expect(getCurrentVersion(sql)).toBe(2);

        // Jump to version 4
        migrateToVersion(sql, SCHEMA_VERSION);

        expect(getCurrentVersion(sql)).toBe(SCHEMA_VERSION);
      });
    });
  });

  describe('Error Handling', () => {
    it('should handle getCurrentVersion on corrupted schema_meta', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Create schema_meta with non-numeric version
        sql.exec(SCHEMA_META);
        // Use INSERT OR REPLACE since schema_meta has unique key constraint
        sql.exec(
          "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', 'not_a_number')"
        );

        // Should handle gracefully (NaN becomes 0 via parseInt fallback)
        const version = getCurrentVersion(sql);
        expect(Number.isNaN(version) || version === 0).toBe(true);
      });
    });

    it('should handle migrateToVersion with invalid target', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Migrate to version that doesn't exist (beyond latest)
        const veryHighVersion = 999;
        migrateToVersion(sql, veryHighVersion);

        // Should only migrate to the highest available
        // (SCHEMA_VERSION or stay at current if no migration path)
        const currentVersion = getCurrentVersion(sql);
        expect(currentVersion).toBeLessThanOrEqual(SCHEMA_VERSION);
      });
    });

    it('should handle migrateToVersion with negative version', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Migrate to negative version
        migrateToVersion(sql, -1);

        // Should not crash and version should be valid
        const currentVersion = getCurrentVersion(sql);
        expect(currentVersion).toBeGreaterThanOrEqual(0);
      });
    });

    it('should handle migrateToVersion with zero version', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Migrate to version 0 (before any migrations)
        migrateToVersion(sql, 0);

        // Version should be 0 (all down migrations run)
        const currentVersion = getCurrentVersion(sql);
        expect(currentVersion).toBe(0);
      });
    });
  });

  describe('Migration SQL Statement Parsing', () => {
    it('should correctly split multi-statement migration SQL', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Test with version 4 migration which has multiple statements
        const migration4 = MIGRATIONS.find((m) => m.version === 4);
        expect(migration4).toBeDefined();

        // The up SQL should have multiple statements (triples table + indexes + index tables)
        const upStatements = migration4!.up
          .split(';')
          .map((s) => s.trim())
          .filter((s) => s.length > 0);

        // Should have at least triples table + indexes
        expect(upStatements.length).toBeGreaterThan(1);

        // Run the migration
        runMigration(sql, migration4!, 'up');

        // Verify tables and indexes exist
        const triplesCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='triples'"
        );
        expect([...triplesCheck].length).toBe(1);

        const indexCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_spo'"
        );
        expect([...indexCheck].length).toBe(1);
      });
    });

    it('should handle migration SQL with trailing semicolons', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Create a test migration with extra semicolons
        const testMigration = {
          version: 999,
          up: `CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY);;;`,
          down: `DROP TABLE IF EXISTS test_table;;;`,
        };

        // Should not throw on extra semicolons
        runMigration(sql, testMigration, 'up');

        const tableCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
        );
        expect([...tableCheck].length).toBe(1);

        // Clean up
        runMigration(sql, testMigration, 'down');
      });
    });

    it('should handle migration SQL with comments (if any)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;

        // Test migration with SQL comments
        const testMigration = {
          version: 998,
          up: `
            -- Create test table for comment handling
            CREATE TABLE IF NOT EXISTS test_comments (
              id INTEGER PRIMARY KEY
            );
          `,
          down: `DROP TABLE IF EXISTS test_comments;`,
        };

        // Should handle comments gracefully
        runMigration(sql, testMigration, 'up');

        const tableCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='test_comments'"
        );
        expect([...tableCheck].length).toBe(1);

        // Clean up
        runMigration(sql, testMigration, 'down');
      });
    });
  });

  describe('Index Tables Creation', () => {
    it('should create all secondary index tables in version 4', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Check for all index tables
        const tables = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        );
        const tableNames = [...tables].map((t) => t.name);

        // Version 4 should have these index tables
        expect(tableNames).toContain('pos_index');
        expect(tableNames).toContain('osp_index');
        expect(tableNames).toContain('fts_index');
        expect(tableNames).toContain('geo_index');
        expect(tableNames).toContain('vector_index');
      });
    });

    it('should create FTS5 virtual table correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Check FTS table is a virtual table
        const ftsCheck = sql.exec(
          "SELECT type FROM sqlite_master WHERE name='fts_index'"
        );
        const rows = [...ftsCheck];
        expect(rows.length).toBe(1);
        expect(rows[0].type).toBe('table'); // Virtual tables show as 'table'

        // Try inserting into FTS to verify it works
        sql.exec(
          `INSERT INTO fts_index (entity_id, predicate, content) VALUES (?, ?, ?)`,
          'https://example.com/entity/1',
          'name',
          'John Doe'
        );

        // Search should work
        const searchResult = sql.exec(
          `SELECT entity_id FROM fts_index WHERE fts_index MATCH ?`,
          'John'
        );
        expect([...searchResult].length).toBe(1);
      });
    });
  });
});
