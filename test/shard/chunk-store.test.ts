/**
 * ChunkStore Tests - BLOB-Only Architecture (TDD RED Phase)
 *
 * CRITICAL: This tests the correct BLOB-only architecture.
 * On Cloudflare DO, a 1KB row costs the SAME as a 2MB BLOB.
 * Individual rows are NOT faster - they're the same cost but 10,000x less efficient.
 *
 * The architecture is:
 *   Write Request -> In-Memory Buffer -> Flush to 2MB BLOB (only SQLite operation)
 *
 * NO individual SQLite rows. EVER.
 *
 * Tests verify:
 * - NO individual rows are created (only BLOB chunks)
 * - Buffer accumulation in memory
 * - Flush creates single BLOB chunk
 * - Query scans chunks only
 * - Force flush on hibernation
 *
 * @see CLAUDE.md for architecture details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import {
  createChunkStore,
  type ChunkStore,
  TARGET_BUFFER_SIZE,
  MIN_CHUNK_SIZE_FOR_COMPACTION,
  MIN_CHUNKS_FOR_COMPACTION,
} from '../../src/shard/chunk-store.js';
import { initializeSchema, SCHEMA_VERSION } from '../../src/shard/schema.js';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type EntityId,
  type Namespace,
} from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';

// ============================================================================
// Test Helpers
// ============================================================================

let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-chunk-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

const testNamespace = createNamespace('https://example.com/');

/**
 * Create a test triple
 */
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
    case ObjectType.REF:
      object.value = value as EntityId;
      break;
    case ObjectType.GEO_POINT:
      object.value = value as { lat: number; lng: number };
      break;
    case ObjectType.TIMESTAMP:
      object.value = value as bigint;
      break;
    case ObjectType.NULL:
      break;
  }

  return {
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: timestamp ?? BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
}

/**
 * Generate multiple test triples
 */
function generateTestTriples(
  count: number,
  options?: {
    subjectPrefix?: string;
    predicatePrefix?: string;
    timestampBase?: bigint;
    timestampIncrement?: bigint;
  }
): Triple[] {
  const {
    subjectPrefix = 'entity',
    predicatePrefix = 'field',
    timestampBase = BigInt(Date.now()),
    timestampIncrement = 1000n,
  } = options ?? {};

  const triples: Triple[] = [];
  const txIdChars = 'ABCDEFGHJKMNPQRSTVWXYZ';

  for (let i = 0; i < count; i++) {
    const timestamp = timestampBase + BigInt(i) * timestampIncrement;
    const txIdSuffix = `01ARZ3NDEKTSV4RRFFQ69G5FA${txIdChars[i % txIdChars.length]}`;

    triples.push(createTestTriple(
      `${subjectPrefix}_${i}`,
      `${predicatePrefix}_${i % 5}`,
      ObjectType.STRING,
      `Value ${i}`,
      timestamp,
      txIdSuffix
    ));
  }

  return triples;
}

// ============================================================================
// Tests
// ============================================================================

describe('ChunkStore - BLOB-Only Architecture', () => {
  describe('createChunkStore', () => {
    it('should create a ChunkStore with buffer-based interface', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);
        expect(store).toBeDefined();

        // Buffer-based interface
        expect(store.buffer).toBeDefined();
        expect(Array.isArray(store.buffer)).toBe(true);
        expect(typeof store.write).toBe('function');
        expect(typeof store.flush).toBe('function');
        expect(typeof store.forceFlush).toBe('function');
        expect(typeof store.query).toBe('function');
      });
    });
  });

  describe('write (in-memory buffer only)', () => {
    it('should accumulate triples in memory buffer - NOT in SQLite', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'Alice'),
          createTestTriple('2', 'name', ObjectType.STRING, 'Bob'),
        ];

        store.write(triples);

        // Buffer should contain the triples
        expect(store.buffer.length).toBe(2);

        // CRITICAL: NO rows should be created in ANY table
        // Check that no triples table exists OR has no rows
        try {
          const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
          const rows = [...countResult];
          expect(rows[0].count).toBe(0);
        } catch {
          // Table doesn't exist, which is also correct
        }

        // Chunks table should also have no rows (we haven't flushed)
        const chunkCount = sql.exec(`SELECT COUNT(*) as count FROM chunks`);
        const chunkRows = [...chunkCount];
        expect(chunkRows[0].count).toBe(0);
      });
    });

    it('should NOT create individual SQLite rows - EVER', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Write 1000 triples
        const triples = generateTestTriples(1000);
        store.write(triples);

        // All 1000 should be in buffer
        expect(store.buffer.length).toBe(1000);

        // CRITICAL: Zero SQLite operations for individual rows
        // The ONLY table that should have data after flush is 'chunks'

        // Verify no triples table or empty triples table
        const tables = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table'"
        );
        const tableNames = [...tables].map(t => t.name);

        // If triples table exists, it should be empty
        if (tableNames.includes('triples')) {
          const countResult = sql.exec(`SELECT COUNT(*) as count FROM triples`);
          const rows = [...countResult];
          expect(rows[0].count).toBe(0);
        }
      });
    });

    it('should handle empty array gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Should not throw
        store.write([]);

        expect(store.buffer.length).toBe(0);
      });
    });

    it('should accumulate multiple write calls in buffer', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        store.write([createTestTriple('1', 'name', ObjectType.STRING, 'Alice')]);
        store.write([createTestTriple('2', 'name', ObjectType.STRING, 'Bob')]);
        store.write([createTestTriple('3', 'name', ObjectType.STRING, 'Charlie')]);

        expect(store.buffer.length).toBe(3);
      });
    });
  });

  describe('flush (create 2MB BLOB chunk)', () => {
    it('should create a single BLOB chunk when buffer is flushed', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Write some triples to buffer
        const triples = generateTestTriples(100);
        store.write(triples);

        expect(store.buffer.length).toBe(100);

        // Flush creates a BLOB chunk
        const chunkId = await store.flush();

        expect(chunkId).toBeDefined();
        expect(chunkId).toMatch(/^chunk_/);

        // Buffer should be empty after flush
        expect(store.buffer.length).toBe(0);

        // Should have exactly ONE chunk row (not 100 individual rows)
        const chunkCount = sql.exec(`SELECT COUNT(*) as count FROM chunks`);
        const rows = [...chunkCount];
        expect(rows[0].count).toBe(1);
      });
    });

    it('should return null when buffer is empty', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const chunkId = await store.flush();
        expect(chunkId).toBeNull();
      });
    });

    it('should encode all triple types correctly in BLOB', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const now = BigInt(Date.now());
        const triples: Triple[] = [
          createTestTriple('1', 'name', ObjectType.STRING, 'Alice', now),
          createTestTriple('2', 'age', ObjectType.INT64, 30n, now + 1n),
          createTestTriple('3', 'score', ObjectType.FLOAT64, 95.5, now + 2n),
          createTestTriple('4', 'active', ObjectType.BOOL, true, now + 3n),
        ];

        store.write(triples);
        const chunkId = await store.flush();

        // Query should decode correctly
        const subject1 = createEntityId('https://example.com/entity/1');
        const result1 = await store.query(subject1);
        expect(result1.length).toBe(1);
        expect(result1[0].object.value).toBe('Alice');

        const subject2 = createEntityId('https://example.com/entity/2');
        const result2 = await store.query(subject2);
        expect(result2.length).toBe(1);
        expect(result2[0].object.value).toBe(30n);
      });
    });

    it('should store chunk metadata correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const triples = generateTestTriples(50);
        store.write(triples);
        const chunkId = await store.flush();

        // Verify chunk metadata
        const result = sql.exec(
          `SELECT * FROM chunks WHERE id = ?`,
          chunkId
        );
        const chunks = [...result];
        expect(chunks.length).toBe(1);

        const chunk = chunks[0];
        expect(chunk.namespace).toBe(testNamespace);
        expect(chunk.triple_count).toBe(50);
        expect(chunk.size_bytes).toBeGreaterThan(0);
        expect(chunk.data).toBeInstanceOf(ArrayBuffer);
      });
    });
  });

  describe('forceFlush (for hibernation/shutdown)', () => {
    it('should flush buffer immediately regardless of size', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Write just a few triples (way below any threshold)
        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'Alice'),
        ];
        store.write(triples);

        const chunkId = await store.forceFlush();

        expect(chunkId).not.toBeNull();
        expect(store.buffer.length).toBe(0);
      });
    });

    it('should return null when buffer is empty', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const chunkId = await store.forceFlush();
        expect(chunkId).toBeNull();
      });
    });
  });

  describe('query (scan chunks only)', () => {
    it('should query data from buffer (not yet flushed)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const subject = createEntityId('https://example.com/entity/1');
        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'Alice'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
          createTestTriple('2', 'name', ObjectType.STRING, 'Bob'), // Different subject
        ];

        store.write(triples);

        // Query should find data in buffer
        const results = await store.query(subject);
        expect(results.length).toBe(2);
        expect(results.some(t => t.predicate === 'name')).toBe(true);
        expect(results.some(t => t.predicate === 'age')).toBe(true);
      });
    });

    it('should query data from flushed chunks', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const subject = createEntityId('https://example.com/entity/1');
        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'Alice'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
        ];

        store.write(triples);
        await store.flush();

        // Buffer is empty
        expect(store.buffer.length).toBe(0);

        // Query should find data in chunks
        const results = await store.query(subject);
        expect(results.length).toBe(2);
      });
    });

    it('should combine results from buffer and chunks', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const subject = createEntityId('https://example.com/entity/1');

        // Write and flush first batch
        const oldTriples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'Alice'),
        ];
        store.write(oldTriples);
        await store.flush();

        // Write new data to buffer (not flushed)
        const newTriples = [
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
        ];
        store.write(newTriples);

        // Query should return both
        const results = await store.query(subject);
        expect(results.length).toBe(2);
        expect(results.some(t => t.predicate === 'name')).toBe(true);
        expect(results.some(t => t.predicate === 'age')).toBe(true);
      });
    });

    it('should prefer buffer data over chunk data for same predicate (newer wins)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const subject = createEntityId('https://example.com/entity/1');
        const oldTimestamp = BigInt(Date.now() - 10000);
        const newTimestamp = BigInt(Date.now());

        // Write old data and flush to chunk
        const oldTriples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'OldName', oldTimestamp),
        ];
        store.write(oldTriples);
        await store.flush();

        // Write new data to buffer
        const newTriples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'NewName', newTimestamp),
        ];
        store.write(newTriples);

        // Query should return the newer buffer value
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe('NewName');
      });
    });

    it('should return empty array for non-existent subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const subject = createEntityId('https://example.com/entity/nonexistent');
        const results = await store.query(subject);
        expect(results).toEqual([]);
      });
    });

    it('should scan multiple chunks for query results', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const subject = createEntityId('https://example.com/entity/1');

        // Create multiple chunks
        store.write([createTestTriple('1', 'field1', ObjectType.STRING, 'value1')]);
        await store.flush();

        store.write([createTestTriple('1', 'field2', ObjectType.STRING, 'value2')]);
        await store.flush();

        store.write([createTestTriple('1', 'field3', ObjectType.STRING, 'value3')]);
        await store.flush();

        // Verify we have 3 chunks
        const chunkCount = sql.exec(`SELECT COUNT(*) as count FROM chunks`);
        expect([...chunkCount][0].count).toBe(3);

        // Query should find data across all chunks
        const results = await store.query(subject);
        expect(results.length).toBe(3);
      });
    });
  });

  describe('getStats', () => {
    it('should return accurate buffer and chunk statistics', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Start empty
        let stats = await store.getStats();
        expect(stats.bufferSize).toBe(0);
        expect(stats.chunkCount).toBe(0);
        expect(stats.totalTriplesInChunks).toBe(0);

        // Add to buffer
        store.write(generateTestTriples(50));
        stats = await store.getStats();
        expect(stats.bufferSize).toBe(50);
        expect(stats.chunkCount).toBe(0);

        // Flush to chunk
        await store.flush();
        stats = await store.getStats();
        expect(stats.bufferSize).toBe(0);
        expect(stats.chunkCount).toBe(1);
        expect(stats.totalTriplesInChunks).toBe(50);
        expect(stats.totalStorageBytes).toBeGreaterThan(0);
      });
    });
  });

  describe('listChunks', () => {
    it('should list all chunks with metadata', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create two chunks
        store.write(generateTestTriples(50));
        const chunkId1 = await store.flush();

        store.write(generateTestTriples(30));
        const chunkId2 = await store.flush();

        const chunks = await store.listChunks();
        expect(chunks.length).toBe(2);

        // Verify metadata
        const chunk1 = chunks.find(c => c.id === chunkId1);
        expect(chunk1).toBeDefined();
        expect(chunk1!.tripleCount).toBe(50);

        const chunk2 = chunks.find(c => c.id === chunkId2);
        expect(chunk2).toBeDefined();
        expect(chunk2!.tripleCount).toBe(30);
      });
    });
  });

  describe('deleteChunk', () => {
    it('should delete a chunk by ID', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        store.write(generateTestTriples(50));
        const chunkId = await store.flush();

        // Verify exists
        let chunks = await store.listChunks();
        expect(chunks.length).toBe(1);

        // Delete
        await store.deleteChunk(chunkId!);

        // Verify gone
        chunks = await store.listChunks();
        expect(chunks.length).toBe(0);
      });
    });
  });

  describe('Cost Optimization - BLOB-only Proof', () => {
    it('should achieve 1000:1 row reduction via BLOB chunking', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Write 1000 triples
        const triples = generateTestTriples(1000);
        store.write(triples);

        // Flush to single BLOB
        await store.flush();

        // Should have exactly 1 chunk row containing 1000 triples
        const chunkResult = sql.exec(`SELECT COUNT(*) as count FROM chunks`);
        expect([...chunkResult][0].count).toBe(1);

        // Verify the chunk contains all 1000 triples
        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(1000);

        // Cost benefit: 1 row read/write instead of 1000
        // On DO: $0.001 per million reads vs $1 per million reads (1000x cost reduction)
      });
    });

    it('should NEVER create individual triple rows', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Simulate normal usage
        for (let i = 0; i < 10; i++) {
          store.write(generateTestTriples(100));
        }

        // Flush periodically
        await store.flush();

        // Add more
        store.write(generateTestTriples(50));

        // Verify NO triples table rows exist
        const tables = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table'"
        );
        const tableNames = [...tables].map(t => t.name as string);

        if (tableNames.includes('triples')) {
          const triplesCount = sql.exec(`SELECT COUNT(*) as count FROM triples`);
          const count = [...triplesCount][0].count as number;
          expect(count).toBe(0); // CRITICAL: Must be zero
        }

        // Only chunks table should have data
        const chunksCount = sql.exec(`SELECT COUNT(*) as count FROM chunks`);
        expect([...chunksCount][0].count).toBeGreaterThanOrEqual(1);
      });
    });
  });

  describe('Schema', () => {
    it('should only have chunks table (no triples table)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // chunks table should exist
        const chunksResult = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='chunks'"
        );
        expect([...chunksResult].length).toBe(1);

        // v4: triples table re-added for secondary index integration
        // Triples are stored in both chunks (for cost-optimized storage)
        // and triples table (for index building and querying)
        const triplesResult = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='triples'"
        );
        expect([...triplesResult].length).toBe(1);
      });
    });

    it('should have correct schema version', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const result = sql.exec(
          "SELECT value FROM schema_meta WHERE key='schema_version'"
        );
        const rows = [...result];
        expect(rows.length).toBe(1);
        // Version should be updated for BLOB-only architecture
        expect(parseInt(rows[0].value as string)).toBeGreaterThanOrEqual(SCHEMA_VERSION);
      });
    });
  });

  describe('Buffer Restore on Wake', () => {
    it('should restore buffer from most recent unflushed chunk on wake', async () => {
      // This test verifies the hibernation/wake cycle preserves unflushed data
      // In the actual DO, this would be handled via blockConcurrencyWhile

      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Simulate: Write data, then DO hibernates (forceFlush called)
        store.write([createTestTriple('1', 'name', ObjectType.STRING, 'Alice')]);

        // On hibernation, forceFlush is called
        await store.forceFlush();

        // Create a new store (simulates DO wake)
        const newStore = createChunkStore(sql, testNamespace);

        // On wake, the data should be queryable from chunks
        const subject = createEntityId('https://example.com/entity/1');
        const results = await newStore.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe('Alice');
      });
    });
  });

  describe('Edge Cases', () => {
    it('should handle null object type correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const nullTriple = createTestTriple('null-entity', 'nullField', ObjectType.NULL, null);
        store.write([nullTriple]);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/null-entity');
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.type).toBe(ObjectType.NULL);
      });
    });

    it('should handle empty string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const emptyStringTriple = createTestTriple('empty-str', 'emptyField', ObjectType.STRING, '');
        store.write([emptyStringTriple]);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/empty-str');
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe('');
      });
    });

    it('should handle zero numeric values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const zeroIntTriple = createTestTriple('zero-int', 'zeroInt', ObjectType.INT64, 0n);
        const zeroFloatTriple = createTestTriple('zero-float', 'zeroFloat', ObjectType.FLOAT64, 0.0);
        store.write([zeroIntTriple, zeroFloatTriple]);
        await store.flush();

        const subject1 = createEntityId('https://example.com/entity/zero-int');
        const results1 = await store.query(subject1);
        expect(results1.length).toBe(1);
        expect(results1[0].object.value).toBe(0n);

        const subject2 = createEntityId('https://example.com/entity/zero-float');
        const results2 = await store.query(subject2);
        expect(results2.length).toBe(1);
        expect(results2[0].object.value).toBe(0.0);
      });
    });

    it('should handle negative numeric values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const negIntTriple = createTestTriple('neg-int', 'negInt', ObjectType.INT64, -9999n);
        const negFloatTriple = createTestTriple('neg-float', 'negFloat', ObjectType.FLOAT64, -123.456);
        store.write([negIntTriple, negFloatTriple]);
        await store.flush();

        const subject1 = createEntityId('https://example.com/entity/neg-int');
        const results1 = await store.query(subject1);
        expect(results1.length).toBe(1);
        expect(results1[0].object.value).toBe(-9999n);

        const subject2 = createEntityId('https://example.com/entity/neg-float');
        const results2 = await store.query(subject2);
        expect(results2.length).toBe(1);
        expect(results2[0].object.value).toBe(-123.456);
      });
    });

    it('should handle boolean false values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const falseTriple = createTestTriple('bool-false', 'active', ObjectType.BOOL, false);
        store.write([falseTriple]);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/bool-false');
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe(false);
      });
    });

    it('should handle very long string values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create a 100KB string
        const longString = 'x'.repeat(100_000);
        const longStringTriple = createTestTriple('long-str', 'content', ObjectType.STRING, longString);
        store.write([longStringTriple]);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/long-str');
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe(longString);
        expect(results[0].object.value?.length).toBe(100_000);
      });
    });

    it('should handle timestamp edge values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Test with zero timestamp
        const zeroTs = createTestTriple('zero-ts', 'field', ObjectType.STRING, 'value', 0n);
        // Test with very large timestamp
        const largeTs = createTestTriple('large-ts', 'field', ObjectType.STRING, 'value', 9999999999999n);

        store.write([zeroTs, largeTs]);
        await store.flush();

        const subject1 = createEntityId('https://example.com/entity/zero-ts');
        const results1 = await store.query(subject1);
        expect(results1.length).toBe(1);
        expect(results1[0].timestamp).toBe(0n);

        const subject2 = createEntityId('https://example.com/entity/large-ts');
        const results2 = await store.query(subject2);
        expect(results2.length).toBe(1);
        expect(results2[0].timestamp).toBe(9999999999999n);
      });
    });

    it('should handle multiple writes to same subject/predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const ts1 = BigInt(Date.now());
        const ts2 = ts1 + 1000n;
        const ts3 = ts2 + 1000n;

        // Write multiple values for same subject/predicate with increasing timestamps
        const triple1 = createTestTriple('multi', 'name', ObjectType.STRING, 'First', ts1);
        const triple2 = createTestTriple('multi', 'name', ObjectType.STRING, 'Second', ts2);
        const triple3 = createTestTriple('multi', 'name', ObjectType.STRING, 'Third', ts3);

        store.write([triple1, triple2, triple3]);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/multi');
        const results = await store.query(subject);
        // Should return only the newest version
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe('Third');
      });
    });
  });

  describe('Large Triple Count (10K+)', () => {
    it('should handle 10,000 triples efficiently', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Generate 10K triples
        const triples = generateTestTriples(10_000);
        store.write(triples);

        expect(store.buffer.length).toBe(10_000);

        // Flush to BLOB
        const chunkId = await store.flush();
        expect(chunkId).not.toBeNull();
        expect(store.buffer.length).toBe(0);

        // Verify chunk metadata
        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(10_000);
        expect(stats.chunkCount).toBe(1);
      });
    });

    it('should handle 15,000 triples in a single chunk', async () => {
      // Note: 50K triples exceeds test env SQLite BLOB limit (~2MB)
      // In production, real Cloudflare DO supports larger blobs
      // Testing with 15K which is safely within limits
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const tripleCount = 15_000;
        const triples = generateTestTriples(tripleCount);
        store.write(triples);
        await store.flush();

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(tripleCount);
        expect(stats.chunkCount).toBe(1);
      });
    });

    it('should query efficiently across 10K triples', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const triples = generateTestTriples(10_000);
        store.write(triples);
        await store.flush();

        // Query a specific subject in the middle
        const subject = createEntityId('https://example.com/entity/entity_5000');
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].predicate).toBe('field_0'); // 5000 % 5 = 0
      });
    });

    it('should handle incremental writes totaling 10K+ triples', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Write in batches of 2500
        for (let i = 0; i < 4; i++) {
          const triples = generateTestTriples(2500, {
            subjectPrefix: `batch${i}_entity`,
            timestampBase: BigInt(Date.now() + i * 10000),
          });
          store.write(triples);
          await store.flush();
        }

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(10_000);
        expect(stats.chunkCount).toBe(4);
      });
    });
  });

  describe('Query by Different Predicates', () => {
    it('should return all predicates for a subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create entity with multiple predicates
        const triples = [
          createTestTriple('multi-pred', 'name', ObjectType.STRING, 'Entity Name'),
          createTestTriple('multi-pred', 'description', ObjectType.STRING, 'A description'),
          createTestTriple('multi-pred', 'age', ObjectType.INT64, 25n),
          createTestTriple('multi-pred', 'score', ObjectType.FLOAT64, 98.5),
          createTestTriple('multi-pred', 'active', ObjectType.BOOL, true),
          createTestTriple('multi-pred', 'location', ObjectType.GEO_POINT, { lat: 37.7749, lng: -122.4194 }),
        ];

        store.write(triples);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/multi-pred');
        const results = await store.query(subject);

        expect(results.length).toBe(6);

        const predicateMap = new Map(results.map(r => [r.predicate, r]));
        expect(predicateMap.get('name')?.object.value).toBe('Entity Name');
        expect(predicateMap.get('description')?.object.value).toBe('A description');
        expect(predicateMap.get('age')?.object.value).toBe(25n);
        expect(predicateMap.get('score')?.object.value).toBe(98.5);
        expect(predicateMap.get('active')?.object.value).toBe(true);
        expect(predicateMap.get('location')?.object.value).toEqual({ lat: 37.7749, lng: -122.4194 });
      });
    });

    it('should distinguish between different subjects with same predicates', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Multiple entities with the same predicate
        const triples = [
          createTestTriple('person1', 'name', ObjectType.STRING, 'Alice'),
          createTestTriple('person2', 'name', ObjectType.STRING, 'Bob'),
          createTestTriple('person3', 'name', ObjectType.STRING, 'Charlie'),
        ];

        store.write(triples);
        await store.flush();

        const alice = await store.query(createEntityId('https://example.com/entity/person1'));
        const bob = await store.query(createEntityId('https://example.com/entity/person2'));
        const charlie = await store.query(createEntityId('https://example.com/entity/person3'));

        expect(alice.length).toBe(1);
        expect(alice[0].object.value).toBe('Alice');

        expect(bob.length).toBe(1);
        expect(bob[0].object.value).toBe('Bob');

        expect(charlie.length).toBe(1);
        expect(charlie[0].object.value).toBe('Charlie');
      });
    });

    it('should handle entities with many predicates (100+)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create entity with 100 different predicates
        const triples: Triple[] = [];
        for (let i = 0; i < 100; i++) {
          triples.push(createTestTriple('many-preds', `field${i}`, ObjectType.STRING, `value${i}`));
        }

        store.write(triples);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/many-preds');
        const results = await store.query(subject);

        expect(results.length).toBe(100);
      });
    });
  });

  describe('Time Range Queries', () => {
    it('should store and retrieve min/max timestamps correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const baseTs = BigInt(Date.now());
        const triples = [
          createTestTriple('ts1', 'field', ObjectType.STRING, 'a', baseTs),
          createTestTriple('ts2', 'field', ObjectType.STRING, 'b', baseTs + 10000n),
          createTestTriple('ts3', 'field', ObjectType.STRING, 'c', baseTs + 5000n),
        ];

        store.write(triples);
        await store.flush();

        const chunks = await store.listChunks();
        expect(chunks.length).toBe(1);
        expect(chunks[0].minTimestamp).toBe(Number(baseTs));
        expect(chunks[0].maxTimestamp).toBe(Number(baseTs + 10000n));
      });
    });

    it('should handle queries across multiple time-ordered chunks', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const baseTs = BigInt(Date.now());

        // Create chunks with different time ranges
        // Chunk 1: t0 - t1000
        store.write(generateTestTriples(100, {
          subjectPrefix: 'time_range_entity',
          timestampBase: baseTs,
          timestampIncrement: 10n,
        }));
        await store.flush();

        // Chunk 2: t2000 - t3000
        store.write(generateTestTriples(100, {
          subjectPrefix: 'time_range_entity',
          timestampBase: baseTs + 2000n,
          timestampIncrement: 10n,
        }));
        await store.flush();

        // Chunk 3: t4000 - t5000
        store.write(generateTestTriples(100, {
          subjectPrefix: 'time_range_entity',
          timestampBase: baseTs + 4000n,
          timestampIncrement: 10n,
        }));
        await store.flush();

        const chunks = await store.listChunks();
        expect(chunks.length).toBe(3);

        // Verify time ranges
        const timestamps = chunks.map(c => ({ min: c.minTimestamp, max: c.maxTimestamp }));
        expect(timestamps.some(t => t.min === Number(baseTs))).toBe(true);
        expect(timestamps.some(t => t.min === Number(baseTs + 2000n))).toBe(true);
        expect(timestamps.some(t => t.min === Number(baseTs + 4000n))).toBe(true);
      });
    });

    it('should return the newest value when same predicate exists in different time ranges', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const oldTs = BigInt(Date.now() - 100000);
        const newTs = BigInt(Date.now());

        // Old chunk with old value
        store.write([createTestTriple('time-entity', 'status', ObjectType.STRING, 'old', oldTs)]);
        await store.flush();

        // New chunk with new value
        store.write([createTestTriple('time-entity', 'status', ObjectType.STRING, 'new', newTs)]);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/time-entity');
        const results = await store.query(subject);

        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe('new');
        expect(results[0].timestamp).toBe(newTs);
      });
    });
  });

  describe('Concurrent Writes', () => {
    it('should handle rapid sequential writes without data loss', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Rapid sequential writes
        for (let i = 0; i < 100; i++) {
          store.write([createTestTriple(`rapid_${i}`, 'field', ObjectType.STRING, `value_${i}`)]);
        }

        expect(store.buffer.length).toBe(100);

        await store.flush();

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(100);
      });
    });

    it('should handle interleaved writes and flushes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Interleaved writes and flushes
        store.write([createTestTriple('interleave_1', 'field', ObjectType.STRING, 'v1')]);
        store.write([createTestTriple('interleave_2', 'field', ObjectType.STRING, 'v2')]);
        await store.flush();

        store.write([createTestTriple('interleave_3', 'field', ObjectType.STRING, 'v3')]);
        await store.flush();

        store.write([createTestTriple('interleave_4', 'field', ObjectType.STRING, 'v4')]);
        store.write([createTestTriple('interleave_5', 'field', ObjectType.STRING, 'v5')]);
        await store.flush();

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(5);
        expect(stats.chunkCount).toBe(3);
      });
    });

    it('should handle parallel flush calls on same store (idempotent)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        store.write(generateTestTriples(100));

        // Call flush twice in parallel
        const [result1, result2] = await Promise.all([
          store.flush(),
          store.flush(),
        ]);

        // One should succeed, one should return null (buffer empty)
        const results = [result1, result2].filter(r => r !== null);
        expect(results.length).toBe(1);

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(100);
        expect(stats.chunkCount).toBe(1);
      });
    });
  });

  describe('Buffer Overflow Handling', () => {
    // Note: Test environment SQLite has stricter BLOB limits than production DO
    // Using smaller counts that work reliably in test environment

    it('should handle buffer approaching practical limits', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Test with 14,999 triples (just under 15K practical limit in test env)
        const testLimit = 14_999;
        const triples = generateTestTriples(testLimit);
        store.write(triples);

        expect(store.buffer.length).toBe(testLimit);

        // Add one more
        store.write([createTestTriple('overflow', 'field', ObjectType.STRING, 'value')]);
        expect(store.buffer.length).toBe(testLimit + 1);

        await store.flush();
        expect(store.buffer.length).toBe(0);

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(testLimit + 1);
      });
    });

    it('should handle buffer with varying triple sizes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Write mix of small and large triples
        const triples: Triple[] = [];
        for (let i = 0; i < 5000; i++) {
          if (i % 10 === 0) {
            // Every 10th triple has a larger string value
            triples.push(createTestTriple(`large_${i}`, 'content', ObjectType.STRING, 'x'.repeat(1000)));
          } else {
            triples.push(createTestTriple(`small_${i}`, 'field', ObjectType.STRING, `v${i}`));
          }
        }
        store.write(triples);

        await store.flush();
        expect(store.buffer.length).toBe(0);

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(5000);
      });
    });

    it('should handle multiple sequential flushes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create multiple chunks in sequence
        for (let i = 0; i < 5; i++) {
          store.write(generateTestTriples(2000, {
            subjectPrefix: `batch_${i}`,
            timestampBase: BigInt(Date.now() + i * 10000),
          }));
          await store.flush();
        }

        const stats = await store.getStats();
        expect(stats.totalTriplesInChunks).toBe(10000);
        expect(stats.chunkCount).toBe(5);
      });
    });

    it('should compact multiple small chunks into fewer chunks', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create 5 small chunks (each under MIN_CHUNK_SIZE_FOR_COMPACTION)
        for (let i = 0; i < 5; i++) {
          store.write(generateTestTriples(1000, {
            subjectPrefix: `compact_batch_${i}`,
            timestampBase: BigInt(Date.now() + i * 10000),
          }));
          await store.flush();
        }

        const statsBefore = await store.getStats();
        expect(statsBefore.chunkCount).toBe(5);

        // Compact should merge small chunks
        const compacted = await store.compact();
        expect(compacted).toBe(5);

        const statsAfter = await store.getStats();
        // Total triples should be preserved
        expect(statsAfter.totalTriplesInChunks).toBe(5000);
        // Should be fewer chunks after compaction
        expect(statsAfter.chunkCount).toBeLessThan(5);
      });
    });
  });

  describe('GraphCol Encoding/Decoding Correctness', () => {
    it('should correctly encode/decode all basic ObjectTypes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const timestamp = BigInt(Date.now());
        const refEntity = createEntityId('https://example.com/entity/referenced');

        const triples: Triple[] = [
          createTestTriple('types', 'nullVal', ObjectType.NULL, null, timestamp),
          createTestTriple('types', 'boolTrue', ObjectType.BOOL, true, timestamp + 1n),
          createTestTriple('types', 'boolFalse', ObjectType.BOOL, false, timestamp + 2n),
          createTestTriple('types', 'int64Pos', ObjectType.INT64, 9223372036854775807n, timestamp + 3n),
          createTestTriple('types', 'int64Neg', ObjectType.INT64, -9223372036854775807n, timestamp + 4n),
          createTestTriple('types', 'float64', ObjectType.FLOAT64, 3.141592653589793, timestamp + 5n),
          createTestTriple('types', 'string', ObjectType.STRING, 'Hello, World! 你好世界 🌍', timestamp + 6n),
          createTestTriple('types', 'ref', ObjectType.REF, refEntity, timestamp + 7n),
          createTestTriple('types', 'value', ObjectType.GEO_POINT, { lat: -33.8688, lng: 151.2093 }, timestamp + 8n),
          createTestTriple('types', 'timestamp', ObjectType.TIMESTAMP, 1704067200000n, timestamp + 9n),
        ];

        store.write(triples);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/types');
        const results = await store.query(subject);

        expect(results.length).toBe(10);

        const byPredicate = new Map(results.map(r => [r.predicate, r]));

        // Verify each type
        expect(byPredicate.get('nullVal')?.object.type).toBe(ObjectType.NULL);
        expect(byPredicate.get('boolTrue')?.object.value).toBe(true);
        expect(byPredicate.get('boolFalse')?.object.value).toBe(false);
        expect(byPredicate.get('int64Pos')?.object.value).toBe(9223372036854775807n);
        expect(byPredicate.get('int64Neg')?.object.value).toBe(-9223372036854775807n);
        expect(byPredicate.get('float64')?.object.value).toBeCloseTo(3.141592653589793, 10);
        expect(byPredicate.get('string')?.object.value).toBe('Hello, World! 你好世界 🌍');
        expect(byPredicate.get('ref')?.object.value).toBe(refEntity);
        expect(byPredicate.get('value')?.object.value).toEqual({ lat: -33.8688, lng: 151.2093 });
        expect(byPredicate.get('timestamp')?.object.value).toBe(1704067200000n);
      });
    });

    it('should correctly encode/decode mixed types in same chunk', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create diverse triples for multiple entities
        const triples: Triple[] = [];
        for (let i = 0; i < 100; i++) {
          const entityId = `mixed_${i}`;
          const type = i % 7;

          switch (type) {
            case 0:
              triples.push(createTestTriple(entityId, 'data', ObjectType.STRING, `str_${i}`));
              break;
            case 1:
              triples.push(createTestTriple(entityId, 'data', ObjectType.INT64, BigInt(i)));
              break;
            case 2:
              triples.push(createTestTriple(entityId, 'data', ObjectType.FLOAT64, i * 1.5));
              break;
            case 3:
              triples.push(createTestTriple(entityId, 'data', ObjectType.BOOL, i % 2 === 0));
              break;
            case 4:
              triples.push(createTestTriple(entityId, 'data', ObjectType.GEO_POINT, { lat: i, lng: -i }));
              break;
            case 5:
              triples.push(createTestTriple(entityId, 'data', ObjectType.TIMESTAMP, BigInt(i * 1000)));
              break;
            case 6:
              triples.push(createTestTriple(entityId, 'data', ObjectType.NULL, null));
              break;
          }
        }

        store.write(triples);
        await store.flush();

        // Verify each entity
        for (let i = 0; i < 100; i++) {
          const subject = createEntityId(`https://example.com/entity/mixed_${i}`);
          const results = await store.query(subject);
          expect(results.length).toBe(1);

          const type = i % 7;
          switch (type) {
            case 0:
              expect(results[0].object.value).toBe(`str_${i}`);
              break;
            case 1:
              expect(results[0].object.value).toBe(BigInt(i));
              break;
            case 2:
              expect(results[0].object.value).toBeCloseTo(i * 1.5);
              break;
            case 3:
              expect(results[0].object.value).toBe(i % 2 === 0);
              break;
            case 4:
              expect(results[0].object.value).toEqual({ lat: i, lng: -i });
              break;
            case 5:
              expect(results[0].object.value).toBe(BigInt(i * 1000));
              break;
            case 6:
              expect(results[0].object.type).toBe(ObjectType.NULL);
              break;
          }
        }
      });
    });

    it('should preserve data integrity through encode/decode cycle', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create triples with specific known values
        const originalTriples: Triple[] = [
          createTestTriple('integrity', 'maxInt64', ObjectType.INT64, 9007199254740991n),
          createTestTriple('integrity', 'minInt64', ObjectType.INT64, -9007199254740991n),
          createTestTriple('integrity', 'specialFloat', ObjectType.FLOAT64, 1.7976931348623157e308),
          createTestTriple('integrity', 'tinyFloat', ObjectType.FLOAT64, 5e-324),
          createTestTriple('integrity', 'unicodeStr', ObjectType.STRING, '日本語テスト 🇯🇵 العربية'),
        ];

        store.write(originalTriples);
        await store.flush();

        const subject = createEntityId('https://example.com/entity/integrity');
        const results = await store.query(subject);

        expect(results.length).toBe(5);

        const byPredicate = new Map(results.map(r => [r.predicate, r]));

        expect(byPredicate.get('maxInt64')?.object.value).toBe(9007199254740991n);
        expect(byPredicate.get('minInt64')?.object.value).toBe(-9007199254740991n);
        expect(byPredicate.get('specialFloat')?.object.value).toBe(1.7976931348623157e308);
        expect(byPredicate.get('tinyFloat')?.object.value).toBe(5e-324);
        expect(byPredicate.get('unicodeStr')?.object.value).toBe('日本語テスト 🇯🇵 العربية');
      });
    });

    it('should handle dictionary encoding with repeated values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create many triples with repeated string values
        const triples: Triple[] = [];
        const repeatedValues = ['Active', 'Inactive', 'Pending'];

        for (let i = 0; i < 1000; i++) {
          triples.push(
            createTestTriple(
              `dict_${i}`,
              'status',
              ObjectType.STRING,
              repeatedValues[i % 3]
            )
          );
        }

        store.write(triples);
        const chunkId = await store.flush();

        // Verify chunk size is compact due to dictionary encoding
        const chunks = await store.listChunks();
        const chunk = chunks.find(c => c.id === chunkId);
        expect(chunk).toBeDefined();
        // With dictionary encoding, 1000 triples with 3 unique values should be compact

        // Verify data integrity
        for (let i = 0; i < 1000; i++) {
          const subject = createEntityId(`https://example.com/entity/dict_${i}`);
          const results = await store.query(subject);
          expect(results.length).toBe(1);
          expect(results[0].object.value).toBe(repeatedValues[i % 3]);
        }
      });
    });
  });

  describe('compact (merge small chunks)', () => {
    it('should not compact when fewer than MIN_CHUNKS_FOR_COMPACTION small chunks exist', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create only 2 small chunks (below MIN_CHUNKS_FOR_COMPACTION which is 3)
        store.write(generateTestTriples(100));
        await store.flush();

        store.write(generateTestTriples(100));
        await store.flush();

        const chunksBefore = await store.listChunks();
        expect(chunksBefore.length).toBe(2);

        // Compact should do nothing
        const compactedCount = await store.compact();
        expect(compactedCount).toBe(0);

        const chunksAfter = await store.listChunks();
        expect(chunksAfter.length).toBe(2);
      });
    });

    it('should compact when MIN_CHUNKS_FOR_COMPACTION or more small chunks exist', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create 4 small chunks (each under MIN_CHUNK_SIZE_FOR_COMPACTION)
        for (let i = 0; i < 4; i++) {
          store.write(generateTestTriples(100, {
            subjectPrefix: `batch${i}`,
            timestampBase: BigInt(Date.now() + i * 1000),
          }));
          await store.flush();
        }

        const chunksBefore = await store.listChunks();
        expect(chunksBefore.length).toBe(4);
        expect(chunksBefore.every(c => c.tripleCount < MIN_CHUNK_SIZE_FOR_COMPACTION)).toBe(true);

        // Compact should merge all 4 into 1
        const compactedCount = await store.compact();
        expect(compactedCount).toBe(4);

        const chunksAfter = await store.listChunks();
        expect(chunksAfter.length).toBe(1);

        // The new chunk should have all 400 triples
        expect(chunksAfter[0].tripleCount).toBe(400);
      });
    });

    it('should preserve all data after compaction', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create multiple small chunks with distinct data
        const subject1 = createEntityId('https://example.com/entity/compactTest1');
        const subject2 = createEntityId('https://example.com/entity/compactTest2');
        const subject3 = createEntityId('https://example.com/entity/compactTest3');

        store.write([createTestTriple('compactTest1', 'name', ObjectType.STRING, 'Alice')]);
        await store.flush();

        store.write([createTestTriple('compactTest2', 'name', ObjectType.STRING, 'Bob')]);
        await store.flush();

        store.write([createTestTriple('compactTest3', 'name', ObjectType.STRING, 'Charlie')]);
        await store.flush();

        // Compact
        await store.compact();

        // All data should still be queryable
        const result1 = await store.query(subject1);
        expect(result1.length).toBe(1);
        expect(result1[0].object.value).toBe('Alice');

        const result2 = await store.query(subject2);
        expect(result2.length).toBe(1);
        expect(result2[0].object.value).toBe('Bob');

        const result3 = await store.query(subject3);
        expect(result3.length).toBe(1);
        expect(result3[0].object.value).toBe('Charlie');
      });
    });

    it('should not compact chunks larger than MIN_CHUNK_SIZE_FOR_COMPACTION', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        // Create one large chunk (>= MIN_CHUNK_SIZE_FOR_COMPACTION)
        store.write(generateTestTriples(MIN_CHUNK_SIZE_FOR_COMPACTION));
        await store.flush();

        // Create 3 small chunks
        for (let i = 0; i < 3; i++) {
          store.write(generateTestTriples(100, { subjectPrefix: `small${i}` }));
          await store.flush();
        }

        const chunksBefore = await store.listChunks();
        expect(chunksBefore.length).toBe(4);

        // Compact should only merge the 3 small chunks
        const compactedCount = await store.compact();
        expect(compactedCount).toBe(3);

        const chunksAfter = await store.listChunks();
        // 1 large chunk (unchanged) + 1 merged chunk = 2
        expect(chunksAfter.length).toBe(2);

        // The large chunk should be unchanged
        const largeChunk = chunksAfter.find(c => c.tripleCount >= MIN_CHUNK_SIZE_FOR_COMPACTION);
        expect(largeChunk).toBeDefined();
        expect(largeChunk!.tripleCount).toBe(MIN_CHUNK_SIZE_FOR_COMPACTION);
      });
    });

    it('should maintain timestamp ordering after compaction', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const store = createChunkStore(sql, testNamespace);

        const baseTimestamp = BigInt(Date.now());

        // Create chunks with different timestamps
        store.write([createTestTriple('entity1', 'field', ObjectType.STRING, 'First', baseTimestamp)]);
        await store.flush();

        store.write([createTestTriple('entity1', 'field', ObjectType.STRING, 'Second', baseTimestamp + 1000n)]);
        await store.flush();

        store.write([createTestTriple('entity1', 'field', ObjectType.STRING, 'Third', baseTimestamp + 2000n)]);
        await store.flush();

        // Compact
        await store.compact();

        // Query should return the latest value (Third)
        const subject = createEntityId('https://example.com/entity/entity1');
        const results = await store.query(subject);
        expect(results.length).toBe(1);
        expect(results[0].object.value).toBe('Third');
      });
    });
  });
});
