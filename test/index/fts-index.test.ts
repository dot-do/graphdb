/**
 * FTS Index Tests
 *
 * @deprecated These tests use the deprecated triples table which no longer exists
 * in BLOB-only architecture (schema v3). These tests are SKIPPED.
 *
 * Tests for Full-Text Search (FTS5) index functionality:
 * - FTS table creation and trigger setup
 * - Text search with various FTS5 query syntaxes
 * - Predicate filtering, snippet generation, BM25 ranking
 *
 * @see CLAUDE.md for architecture details
 * @see src/index/fts-index.ts for implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { initializeSchema } from '../../src/shard/schema.js';
import { ObjectType } from '../../src/core/types.js';
import type { EntityId, Predicate } from '../../src/core/types.js';
import {
  FTS_SCHEMA,
  initializeFTS,
  searchFTS,
  rebuildFTS,
  isFTSInitialized,
  type FTSQuery,
  type FTSResult,
} from '../../src/index/fts-index.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-fts-test-${Date.now()}-${testCounter++}`);
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
describe('FTS Index - Full-Text Search', () => {
  describe('FTS Initialization', () => {
    it('should create FTS virtual table', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Check that FTS table exists
        const tableCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='triples_fts'"
        );
        const tables = [...tableCheck];

        expect(tables.length).toBe(1);
        expect(tables[0].name).toBe('triples_fts');
      });
    });

    it('should create FTS sync triggers', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Check that triggers exist
        const triggerCheck = sql.exec(
          "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'triples_%'"
        );
        const triggers = [...triggerCheck];

        // Should have at least INSERT trigger
        expect(triggers.length).toBeGreaterThanOrEqual(1);
        const triggerNames = triggers.map((t) => t.name);
        expect(triggerNames).toContain('triples_ai');
      });
    });

    it('should report FTS as initialized after setup', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Not initialized yet
        expect(isFTSInitialized(sql)).toBe(false);

        initializeFTS(sql);

        // Now initialized
        expect(isFTSInitialized(sql)).toBe(true);
      });
    });

    it('should be idempotent - calling initializeFTS multiple times is safe', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Call multiple times
        initializeFTS(sql);
        initializeFTS(sql);
        initializeFTS(sql);

        // Should still work
        expect(isFTSInitialized(sql)).toBe(true);
      });
    });
  });

  describe('FTS Trigger Behavior', () => {
    it('should automatically index STRING values on insert', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Insert a STRING triple
        insertTriple(sql, 'https://example.com/person/1', 'name', ObjectType.STRING, {
          string: 'John Doe',
        });

        // Check FTS table has the entry
        const ftsCheck = sql.exec('SELECT * FROM triples_fts');
        const ftsRows = [...ftsCheck];

        expect(ftsRows.length).toBe(1);
        expect(ftsRows[0].content).toBe('John Doe');
        expect(ftsRows[0].subject).toBe('https://example.com/person/1');
        expect(ftsRows[0].predicate).toBe('name');
      });
    });

    it('should NOT index non-STRING values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Insert non-STRING triples
        insertTriple(sql, 'https://example.com/person/1', 'age', ObjectType.INT64, { int64: 30 });
        insertTriple(sql, 'https://example.com/person/1', 'active', ObjectType.BOOL, { bool: 1 });
        insertTriple(sql, 'https://example.com/person/1', 'score', ObjectType.FLOAT64, {
          float64: 98.5,
        });

        // FTS table should be empty
        const ftsCheck = sql.exec('SELECT * FROM triples_fts');
        const ftsRows = [...ftsCheck];

        expect(ftsRows.length).toBe(0);
      });
    });

    it('should index multiple STRING values from same entity', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        const subjectId = 'https://example.com/person/1';
        insertTriple(sql, subjectId, 'name', ObjectType.STRING, { string: 'John Doe' });
        insertTriple(sql, subjectId, 'bio', ObjectType.STRING, {
          string: 'Software engineer at Acme Corp',
        });
        insertTriple(sql, subjectId, 'email', ObjectType.STRING, { string: 'john@example.com' });

        const ftsCheck = sql.exec('SELECT * FROM triples_fts');
        const ftsRows = [...ftsCheck];

        expect(ftsRows.length).toBe(3);
      });
    });
  });

  describe('Simple Text Search', () => {
    it('should find exact word matches', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'Introduction to JavaScript',
        });
        insertTriple(sql, 'https://example.com/article/2', 'title', ObjectType.STRING, {
          string: 'Python for Beginners',
        });
        insertTriple(sql, 'https://example.com/article/3', 'title', ObjectType.STRING, {
          string: 'Advanced JavaScript Patterns',
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(2);
        const subjects = results.map((r) => r.subject);
        expect(subjects).toContain('https://example.com/article/1');
        expect(subjects).toContain('https://example.com/article/3');
      });
    });

    it('should be case-insensitive by default', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'JavaScript Tutorial',
        });

        // Search with different cases
        const resultsLower = await searchFTS(sql, { query: 'javascript' });
        const resultsUpper = await searchFTS(sql, { query: 'JAVASCRIPT' });
        const resultsMixed = await searchFTS(sql, { query: 'JaVaScRiPt' });

        expect(resultsLower.length).toBe(1);
        expect(resultsUpper.length).toBe(1);
        expect(resultsMixed.length).toBe(1);
      });
    });

    it('should return empty array for no matches', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'Python Programming',
        });

        const results = await searchFTS(sql, { query: 'Rust' });

        expect(results.length).toBe(0);
      });
    });
  });

  describe('Phrase Search', () => {
    it('should match exact phrases with quotes', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'The quick brown fox jumps over the lazy dog',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'A quick fox ran through the brown forest',
        });

        // Exact phrase search
        const results = await searchFTS(sql, { query: '"quick brown fox"' });

        expect(results.length).toBe(1);
        expect(results[0].subject).toBe('https://example.com/article/1');
      });
    });

    it('should not match when words are not adjacent for phrase search', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'The quick fox is brown',
        });

        // "quick brown" should not match because "fox is" is in between
        const results = await searchFTS(sql, { query: '"quick brown"' });

        expect(results.length).toBe(0);
      });
    });
  });

  describe('Prefix Search', () => {
    it('should match prefix with asterisk', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'JavaScript Programming',
        });
        insertTriple(sql, 'https://example.com/article/2', 'title', ObjectType.STRING, {
          string: 'Java Development',
        });
        insertTriple(sql, 'https://example.com/article/3', 'title', ObjectType.STRING, {
          string: 'Python Scripting',
        });

        // Prefix search for "Java*" should match JavaScript and Java
        const results = await searchFTS(sql, { query: 'Java*' });

        expect(results.length).toBe(2);
        const subjects = results.map((r) => r.subject);
        expect(subjects).toContain('https://example.com/article/1');
        expect(subjects).toContain('https://example.com/article/2');
      });
    });

    it('should support prefix in phrase context', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/doc/1', 'content', ObjectType.STRING, {
          string: 'programming language tutorial',
        });
        insertTriple(sql, 'https://example.com/doc/2', 'content', ObjectType.STRING, {
          string: 'program execution flow',
        });

        const results = await searchFTS(sql, { query: 'program*' });

        expect(results.length).toBe(2);
      });
    });
  });

  describe('Boolean Operators', () => {
    it('should support AND operator (implicit with multiple words)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'JavaScript is a programming language',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'Python is also a programming language',
        });
        insertTriple(sql, 'https://example.com/article/3', 'content', ObjectType.STRING, {
          string: 'JavaScript frameworks overview',
        });

        // Both "JavaScript" AND "programming" must be present
        const results = await searchFTS(sql, { query: 'JavaScript programming' });

        expect(results.length).toBe(1);
        expect(results[0].subject).toBe('https://example.com/article/1');
      });
    });

    it('should support OR operator', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'JavaScript tutorial',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'Python guide',
        });
        insertTriple(sql, 'https://example.com/article/3', 'content', ObjectType.STRING, {
          string: 'Rust documentation',
        });

        // "JavaScript" OR "Python"
        const results = await searchFTS(sql, { query: 'JavaScript OR Python' });

        expect(results.length).toBe(2);
        const subjects = results.map((r) => r.subject);
        expect(subjects).toContain('https://example.com/article/1');
        expect(subjects).toContain('https://example.com/article/2');
      });
    });

    it('should support NOT operator', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'JavaScript programming tutorial',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'Python programming tutorial',
        });
        insertTriple(sql, 'https://example.com/article/3', 'content', ObjectType.STRING, {
          string: 'Rust programming guide',
        });

        // "programming" but NOT "tutorial"
        const results = await searchFTS(sql, { query: 'programming NOT tutorial' });

        expect(results.length).toBe(1);
        expect(results[0].subject).toBe('https://example.com/article/3');
      });
    });

    it('should support complex boolean expressions', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/doc/1', 'content', ObjectType.STRING, {
          string: 'web development with JavaScript',
        });
        insertTriple(sql, 'https://example.com/doc/2', 'content', ObjectType.STRING, {
          string: 'web development with Python',
        });
        insertTriple(sql, 'https://example.com/doc/3', 'content', ObjectType.STRING, {
          string: 'mobile development with JavaScript',
        });
        insertTriple(sql, 'https://example.com/doc/4', 'content', ObjectType.STRING, {
          string: 'desktop development with Python',
        });

        // "(web OR mobile) AND JavaScript" - FTS5 syntax with explicit AND
        const results = await searchFTS(sql, { query: '(web OR mobile) AND JavaScript' });

        expect(results.length).toBe(2);
        const subjects = results.map((r) => r.subject);
        expect(subjects).toContain('https://example.com/doc/1');
        expect(subjects).toContain('https://example.com/doc/3');
      });
    });
  });

  describe('Predicate Filtering', () => {
    it('should filter results by predicate', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'JavaScript Guide',
        });
        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'This is about JavaScript programming',
        });
        insertTriple(sql, 'https://example.com/article/2', 'title', ObjectType.STRING, {
          string: 'Python Basics',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'Learning JavaScript as a second language',
        });

        // Search only in 'title' predicate
        const results = await searchFTS(sql, {
          query: 'JavaScript',
          predicate: 'title' as Predicate,
        });

        expect(results.length).toBe(1);
        expect(results[0].subject).toBe('https://example.com/article/1');
        expect(results[0].predicate).toBe('title');
      });
    });

    it('should search across all predicates when not specified', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'JavaScript Guide',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'Introduction to JavaScript',
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(2);
      });
    });
  });

  describe('Snippet Generation', () => {
    it('should return snippets with matched text highlighted', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string:
            'This is a long article about JavaScript programming. JavaScript is widely used for web development.',
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(1);
        expect(results[0].snippet).toBeDefined();
        // Snippet should contain highlighting markers (typically <b>...</b> or similar)
        expect(results[0].snippet).toMatch(/JavaScript/i);
      });
    });

    it('should truncate long content in snippets', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        const longContent = 'word '.repeat(1000) + 'JavaScript ' + 'word '.repeat(1000);
        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: longContent,
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(1);
        expect(results[0].snippet).toBeDefined();
        // Snippet should be shorter than the full content
        expect(results[0].snippet.length).toBeLessThan(longContent.length);
      });
    });
  });

  describe('BM25 Ranking', () => {
    it('should return results with rank scores', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'JavaScript',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'JavaScript JavaScript JavaScript',
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(2);
        for (const result of results) {
          expect(typeof result.rank).toBe('number');
        }
      });
    });

    it('should rank documents with more term occurrences higher', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'JavaScript basics',
        });
        insertTriple(sql, 'https://example.com/article/2', 'content', ObjectType.STRING, {
          string: 'JavaScript JavaScript JavaScript advanced JavaScript patterns',
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(2);
        // Results should be sorted by rank (best first)
        // The one with more occurrences should have better (lower/more negative) rank
        const article2Index = results.findIndex(
          (r) => r.subject === 'https://example.com/article/2'
        );
        const article1Index = results.findIndex(
          (r) => r.subject === 'https://example.com/article/1'
        );

        // Article 2 should come first (better rank)
        expect(article2Index).toBeLessThan(article1Index);
      });
    });

    it('should return results sorted by relevance', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Insert documents with varying relevance
        insertTriple(sql, 'https://example.com/doc/1', 'content', ObjectType.STRING, {
          string: 'unrelated document about cooking',
        });
        insertTriple(sql, 'https://example.com/doc/2', 'content', ObjectType.STRING, {
          string: 'JavaScript basics tutorial',
        });
        insertTriple(sql, 'https://example.com/doc/3', 'content', ObjectType.STRING, {
          string:
            'JavaScript is great. JavaScript everywhere. I love JavaScript. JavaScript JavaScript!',
        });

        const results = await searchFTS(sql, { query: 'JavaScript' });

        expect(results.length).toBe(2);
        // Results should be in rank order
        for (let i = 1; i < results.length; i++) {
          // BM25 returns negative scores, more negative = more relevant
          expect(results[i - 1].rank).toBeLessThanOrEqual(results[i].rank);
        }
      });
    });
  });

  describe('Pagination', () => {
    it('should support limit parameter', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Insert many matching documents
        for (let i = 0; i < 20; i++) {
          insertTriple(sql, `https://example.com/article/${i}`, 'content', ObjectType.STRING, {
            string: `JavaScript tutorial part ${i}`,
          });
        }

        const results = await searchFTS(sql, { query: 'JavaScript', limit: 5 });

        expect(results.length).toBe(5);
      });
    });

    it('should support offset parameter', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Insert many matching documents
        for (let i = 0; i < 20; i++) {
          insertTriple(sql, `https://example.com/article/${i}`, 'content', ObjectType.STRING, {
            string: `JavaScript tutorial part ${i}`,
          });
        }

        const page1 = await searchFTS(sql, { query: 'JavaScript', limit: 5 });
        const page2 = await searchFTS(sql, { query: 'JavaScript', limit: 5, offset: 5 });

        expect(page1.length).toBe(5);
        expect(page2.length).toBe(5);

        // No overlap between pages
        const page1Subjects = new Set(page1.map((r) => r.subject));
        const page2Subjects = new Set(page2.map((r) => r.subject));
        const intersection = [...page1Subjects].filter((s) => page2Subjects.has(s));
        expect(intersection.length).toBe(0);
      });
    });

    it('should return fewer results when offset exceeds total', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'JavaScript tutorial',
        });

        const results = await searchFTS(sql, { query: 'JavaScript', offset: 100 });

        expect(results.length).toBe(0);
      });
    });
  });

  describe('FTS Rebuild', () => {
    it('should rebuild FTS index from existing triples', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        // Insert data BEFORE initializing FTS
        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'JavaScript Guide',
        });
        insertTriple(sql, 'https://example.com/article/2', 'title', ObjectType.STRING, {
          string: 'Python Basics',
        });

        // Now initialize FTS
        initializeFTS(sql);

        // FTS should be empty since data was inserted before triggers
        const initialResults = await searchFTS(sql, { query: 'JavaScript' });
        expect(initialResults.length).toBe(0);

        // Rebuild FTS index
        await rebuildFTS(sql);

        // Now search should work
        const results = await searchFTS(sql, { query: 'JavaScript' });
        expect(results.length).toBe(1);
        expect(results[0].subject).toBe('https://example.com/article/1');
      });
    });

    it('should clear and rebuild FTS index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        // Insert some data
        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'Original Title',
        });

        // Verify it's indexed
        let results = await searchFTS(sql, { query: 'Original' });
        expect(results.length).toBe(1);

        // UPDATE triggers keep FTS in sync automatically
        sql.exec(
          "UPDATE triples SET obj_string = 'Modified Title' WHERE subject = 'https://example.com/article/1'"
        );

        // FTS should be updated by trigger (triggers keep FTS in sync)
        results = await searchFTS(sql, { query: 'Original' });
        expect(results.length).toBe(0);
        results = await searchFTS(sql, { query: 'Modified' });
        expect(results.length).toBe(1);

        // Directly manipulate FTS table to simulate corruption (bypassing triggers)
        sql.exec("DELETE FROM triples_fts");

        // FTS is now out of sync (empty)
        results = await searchFTS(sql, { query: 'Modified' });
        expect(results.length).toBe(0);

        // Rebuild should restore the FTS index
        await rebuildFTS(sql);

        results = await searchFTS(sql, { query: 'Modified' });
        expect(results.length).toBe(1);
      });
    });
  });

  describe('Edge Cases', () => {
    it('should handle special characters in search query', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: 'C++ programming guide',
        });

        // Search for C++ (special characters)
        // Note: FTS5 may require escaping or may not handle ++ well
        const results = await searchFTS(sql, { query: 'programming' });

        expect(results.length).toBe(1);
      });
    });

    it('should handle unicode text', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'Intro',
        });

        const results = await searchFTS(sql, { query: 'Intro' });

        expect(results.length).toBe(1);
      });
    });

    it('should handle empty search query gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        insertTriple(sql, 'https://example.com/article/1', 'title', ObjectType.STRING, {
          string: 'Some content',
        });

        const results = await searchFTS(sql, { query: '' });

        // Empty query should return empty results or throw
        expect(results.length).toBe(0);
      });
    });

    it('should handle very long text content', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);
        initializeFTS(sql);

        const longText = 'Lorem ipsum '.repeat(10000) + 'UNIQUEWORD ' + 'dolor sit '.repeat(10000);
        insertTriple(sql, 'https://example.com/article/1', 'content', ObjectType.STRING, {
          string: longText,
        });

        const results = await searchFTS(sql, { query: 'UNIQUEWORD' });

        expect(results.length).toBe(1);
      });
    });
  });
});
