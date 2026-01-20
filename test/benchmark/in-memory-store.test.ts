/**
 * In-Memory Triple Store Tests
 *
 * Tests for the InMemoryTripleStore class and helper functions:
 * - Triple insertion and indexing
 * - Query operations (getBySubject, getByPredicate, getValue)
 * - Filter, aggregate, and groupBy operations
 * - 1-hop traversal
 * - Helper functions (inferObjectType, rowToTriples)
 * - Benchmark query execution
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  InMemoryTripleStore,
  inferObjectType,
  rowToTriples,
  generateTestData,
  executeBenchQuery,
  BENCH_QUERIES,
  type BenchQuery,
} from '../../src/benchmark/in-memory-store.js';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';

// Helper to create test triples
function createTestTriple(
  subject: string,
  predicate: string,
  object: TypedObject,
  txId: string = '01ARZ3NDEKTSV4RRFFQ69G5FAV'
): Triple {
  return {
    subject: subject as any,
    predicate: predicate as any,
    object,
    timestamp: BigInt(Date.now()),
    txId: txId as any,
  };
}

describe('InMemoryTripleStore', () => {
  let store: InMemoryTripleStore;

  beforeEach(() => {
    store = new InMemoryTripleStore();
  });

  // ============================================================================
  // Basic Operations Tests
  // ============================================================================

  describe('insert', () => {
    it('should insert a single triple', () => {
      const triple = createTestTriple(
        'https://example.com/entity/1',
        'name',
        { type: ObjectType.STRING, value: 'Test Entity' }
      );

      store.insert(triple);

      expect(store.count()).toBe(1);
      expect(store.entityCount()).toBe(1);
    });

    it('should insert multiple triples for same subject', () => {
      const triple1 = createTestTriple(
        'https://example.com/entity/1',
        'name',
        { type: ObjectType.STRING, value: 'Test Entity' }
      );
      const triple2 = createTestTriple(
        'https://example.com/entity/1',
        'age',
        { type: ObjectType.INT64, value: 25n }
      );

      store.insert(triple1);
      store.insert(triple2);

      expect(store.count()).toBe(2);
      expect(store.entityCount()).toBe(1);
    });

    it('should insert triples for multiple subjects', () => {
      const triple1 = createTestTriple(
        'https://example.com/entity/1',
        'name',
        { type: ObjectType.STRING, value: 'Entity 1' }
      );
      const triple2 = createTestTriple(
        'https://example.com/entity/2',
        'name',
        { type: ObjectType.STRING, value: 'Entity 2' }
      );

      store.insert(triple1);
      store.insert(triple2);

      expect(store.count()).toBe(2);
      expect(store.entityCount()).toBe(2);
    });

    it('should allow multiple values for same predicate', () => {
      const triple1 = createTestTriple(
        'https://example.com/entity/1',
        'tag',
        { type: ObjectType.STRING, value: 'tag1' }
      );
      const triple2 = createTestTriple(
        'https://example.com/entity/1',
        'tag',
        { type: ObjectType.STRING, value: 'tag2' }
      );

      store.insert(triple1);
      store.insert(triple2);

      expect(store.count()).toBe(2);
      const triples = store.getBySubject('https://example.com/entity/1');
      expect(triples.length).toBe(2);
    });
  });

  describe('getBySubject', () => {
    it('should return empty array for unknown subject', () => {
      const result = store.getBySubject('https://example.com/nonexistent');
      expect(result).toEqual([]);
    });

    it('should return all triples for a subject', () => {
      const subject = 'https://example.com/entity/1';
      store.insert(createTestTriple(subject, 'name', { type: ObjectType.STRING, value: 'Test' }));
      store.insert(createTestTriple(subject, 'age', { type: ObjectType.INT64, value: 30n }));
      store.insert(createTestTriple(subject, 'active', { type: ObjectType.BOOL, value: true }));

      const result = store.getBySubject(subject);

      expect(result.length).toBe(3);
      expect(result.every((t) => t.subject === subject)).toBe(true);
    });

    it('should not return triples from other subjects', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Entity 1' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'name', { type: ObjectType.STRING, value: 'Entity 2' }));

      const result = store.getBySubject('https://example.com/entity/1');

      expect(result.length).toBe(1);
      expect(result[0]!.object.type).toBe(ObjectType.STRING);
      expect((result[0]!.object as any).value).toBe('Entity 1');
    });
  });

  describe('getByPredicate', () => {
    it('should return empty array for unknown predicate', () => {
      const result = store.getByPredicate('nonexistent');
      expect(result).toEqual([]);
    });

    it('should return all subjects with a predicate', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Entity 1' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'name', { type: ObjectType.STRING, value: 'Entity 2' }));
      store.insert(createTestTriple('https://example.com/entity/3', 'age', { type: ObjectType.INT64, value: 25n }));

      const result = store.getByPredicate('name');

      expect(result.length).toBe(2);
      expect(result).toContain('https://example.com/entity/1');
      expect(result).toContain('https://example.com/entity/2');
    });

    it('should return unique subjects even with multiple values', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'tag', { type: ObjectType.STRING, value: 'tag1' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'tag', { type: ObjectType.STRING, value: 'tag2' }));

      const result = store.getByPredicate('tag');

      expect(result.length).toBe(1);
      expect(result[0]).toBe('https://example.com/entity/1');
    });
  });

  describe('getValue', () => {
    it('should return undefined for unknown subject', () => {
      const result = store.getValue('https://example.com/nonexistent', 'name');
      expect(result).toBeUndefined();
    });

    it('should return undefined for unknown predicate', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Test' }));

      const result = store.getValue('https://example.com/entity/1', 'age');
      expect(result).toBeUndefined();
    });

    it('should return first value for subject+predicate', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'First' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Second' }));

      const result = store.getValue('https://example.com/entity/1', 'name');

      expect(result).toBeDefined();
      expect((result as any).value).toBe('First');
    });
  });

  describe('getAllSubjects', () => {
    it('should return empty array for empty store', () => {
      expect(store.getAllSubjects()).toEqual([]);
    });

    it('should return all unique subjects', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Test' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'name', { type: ObjectType.STRING, value: 'Test' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'age', { type: ObjectType.INT64, value: 30n }));

      const subjects = store.getAllSubjects();

      expect(subjects.length).toBe(2);
      expect(subjects).toContain('https://example.com/entity/1');
      expect(subjects).toContain('https://example.com/entity/2');
    });
  });

  // ============================================================================
  // Filter, Aggregate, GroupBy Tests
  // ============================================================================

  describe('filterByPredicateValue', () => {
    beforeEach(() => {
      store.insert(createTestTriple('https://example.com/entity/1', 'category', { type: ObjectType.STRING, value: 'A' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'category', { type: ObjectType.STRING, value: 'B' }));
      store.insert(createTestTriple('https://example.com/entity/3', 'category', { type: ObjectType.STRING, value: 'A' }));
    });

    it('should filter by predicate value', () => {
      const result = store.filterByPredicateValue('category', (obj) => (obj as any).value === 'A');

      expect(result.length).toBe(2);
      expect(result).toContain('https://example.com/entity/1');
      expect(result).toContain('https://example.com/entity/3');
    });

    it('should return empty array when no matches', () => {
      const result = store.filterByPredicateValue('category', (obj) => (obj as any).value === 'Z');

      expect(result).toEqual([]);
    });

    it('should work with numeric predicates', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'score', { type: ObjectType.INT64, value: 100n }));
      store.insert(createTestTriple('https://example.com/entity/2', 'score', { type: ObjectType.INT64, value: 50n }));

      const result = store.filterByPredicateValue('score', (obj) => Number((obj as any).value) >= 75);

      expect(result.length).toBe(1);
      expect(result[0]).toBe('https://example.com/entity/1');
    });
  });

  describe('aggregate', () => {
    it('should return zero for unknown predicate', () => {
      const result = store.aggregate('nonexistent');

      expect(result.count).toBe(0);
      expect(result.sum).toBe(0);
    });

    it('should aggregate numeric values', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'value', { type: ObjectType.INT64, value: 10n }));
      store.insert(createTestTriple('https://example.com/entity/2', 'value', { type: ObjectType.INT64, value: 20n }));
      store.insert(createTestTriple('https://example.com/entity/3', 'value', { type: ObjectType.INT64, value: 30n }));

      const result = store.aggregate('value');

      expect(result.count).toBe(3);
      expect(result.sum).toBe(60);
    });

    it('should handle float values', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'score', { type: ObjectType.FLOAT64, value: 10.5 }));
      store.insert(createTestTriple('https://example.com/entity/2', 'score', { type: ObjectType.FLOAT64, value: 20.5 }));

      const result = store.aggregate('score');

      expect(result.count).toBe(2);
      expect(result.sum).toBeCloseTo(31.0);
    });
  });

  describe('groupBy', () => {
    beforeEach(() => {
      store.insert(createTestTriple('https://example.com/entity/1', 'category', { type: ObjectType.STRING, value: 'A' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'category', { type: ObjectType.STRING, value: 'B' }));
      store.insert(createTestTriple('https://example.com/entity/3', 'category', { type: ObjectType.STRING, value: 'A' }));
      store.insert(createTestTriple('https://example.com/entity/4', 'category', { type: ObjectType.STRING, value: 'C' }));
    });

    it('should group subjects by predicate value', () => {
      const groups = store.groupBy('category');

      expect(groups.size).toBe(3);
      expect(groups.get('A')?.length).toBe(2);
      expect(groups.get('B')?.length).toBe(1);
      expect(groups.get('C')?.length).toBe(1);
    });

    it('should return empty map for unknown predicate', () => {
      const groups = store.groupBy('nonexistent');

      expect(groups.size).toBe(0);
    });

    it('should work with numeric grouping', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'year', { type: ObjectType.INT64, value: 2020n }));
      store.insert(createTestTriple('https://example.com/entity/2', 'year', { type: ObjectType.INT64, value: 2020n }));
      store.insert(createTestTriple('https://example.com/entity/3', 'year', { type: ObjectType.INT64, value: 2021n }));

      const groups = store.groupBy('year');

      expect(groups.size).toBe(2);
      expect(groups.get(2020n)?.length).toBe(2);
      expect(groups.get(2021n)?.length).toBe(1);
    });
  });

  // ============================================================================
  // Traversal Tests
  // ============================================================================

  describe('traverse1Hop', () => {
    it('should return entity triples for subject without refs', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Entity 1' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'age', { type: ObjectType.INT64, value: 30n }));

      const result = store.traverse1Hop('https://example.com/entity/1');

      expect(result.entity.length).toBe(2);
      expect(result.related.size).toBe(0);
    });

    it('should follow REF edges to related entities', () => {
      // Entity 1 references Entity 2
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Entity 1' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'friend', { type: ObjectType.REF, value: 'https://example.com/entity/2' as any }));
      store.insert(createTestTriple('https://example.com/entity/2', 'name', { type: ObjectType.STRING, value: 'Entity 2' }));

      const result = store.traverse1Hop('https://example.com/entity/1');

      expect(result.entity.length).toBe(2);
      expect(result.related.size).toBe(1);
      expect(result.related.has('https://example.com/entity/2')).toBe(true);
    });

    it('should follow multiple REF edges', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'name', { type: ObjectType.STRING, value: 'Entity 1' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'friend', { type: ObjectType.REF, value: 'https://example.com/entity/2' as any }));
      store.insert(createTestTriple('https://example.com/entity/1', 'colleague', { type: ObjectType.REF, value: 'https://example.com/entity/3' as any }));
      store.insert(createTestTriple('https://example.com/entity/2', 'name', { type: ObjectType.STRING, value: 'Entity 2' }));
      store.insert(createTestTriple('https://example.com/entity/3', 'name', { type: ObjectType.STRING, value: 'Entity 3' }));

      const result = store.traverse1Hop('https://example.com/entity/1');

      expect(result.related.size).toBe(2);
    });

    it('should handle refs to non-existent entities', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'friend', { type: ObjectType.REF, value: 'https://example.com/nonexistent' as any }));

      const result = store.traverse1Hop('https://example.com/entity/1');

      // REF exists but target entity doesn't, so related should be empty
      expect(result.related.size).toBe(0);
    });
  });

  // ============================================================================
  // Count Tests
  // ============================================================================

  describe('count and entityCount', () => {
    it('should start at zero', () => {
      expect(store.count()).toBe(0);
      expect(store.entityCount()).toBe(0);
    });

    it('should track triple count accurately', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'a', { type: ObjectType.STRING, value: '1' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'b', { type: ObjectType.STRING, value: '2' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'a', { type: ObjectType.STRING, value: '3' }));

      expect(store.count()).toBe(3);
    });

    it('should track entity count accurately', () => {
      store.insert(createTestTriple('https://example.com/entity/1', 'a', { type: ObjectType.STRING, value: '1' }));
      store.insert(createTestTriple('https://example.com/entity/1', 'b', { type: ObjectType.STRING, value: '2' }));
      store.insert(createTestTriple('https://example.com/entity/2', 'a', { type: ObjectType.STRING, value: '3' }));

      expect(store.entityCount()).toBe(2);
    });
  });
});

// ============================================================================
// Helper Function Tests
// ============================================================================

describe('inferObjectType', () => {
  it('should infer NULL for null and undefined', () => {
    expect(inferObjectType(null)).toBe(ObjectType.NULL);
    expect(inferObjectType(undefined)).toBe(ObjectType.NULL);
  });

  it('should infer BOOL for boolean', () => {
    expect(inferObjectType(true)).toBe(ObjectType.BOOL);
    expect(inferObjectType(false)).toBe(ObjectType.BOOL);
  });

  it('should infer INT64 for bigint', () => {
    expect(inferObjectType(42n)).toBe(ObjectType.INT64);
    expect(inferObjectType(BigInt(1000000))).toBe(ObjectType.INT64);
  });

  it('should infer INT64 for integer numbers', () => {
    expect(inferObjectType(42)).toBe(ObjectType.INT64);
    expect(inferObjectType(0)).toBe(ObjectType.INT64);
    expect(inferObjectType(-100)).toBe(ObjectType.INT64);
  });

  it('should infer FLOAT64 for non-integer numbers', () => {
    expect(inferObjectType(3.14)).toBe(ObjectType.FLOAT64);
    expect(inferObjectType(0.5)).toBe(ObjectType.FLOAT64);
    expect(inferObjectType(-2.5)).toBe(ObjectType.FLOAT64);
  });

  it('should infer STRING for strings', () => {
    expect(inferObjectType('hello')).toBe(ObjectType.STRING);
    expect(inferObjectType('')).toBe(ObjectType.STRING);
  });

  it('should infer TIMESTAMP for Date objects', () => {
    expect(inferObjectType(new Date())).toBe(ObjectType.TIMESTAMP);
  });

  it('should infer JSON for objects and arrays', () => {
    expect(inferObjectType({ foo: 'bar' })).toBe(ObjectType.JSON);
    expect(inferObjectType([1, 2, 3])).toBe(ObjectType.JSON);
    expect(inferObjectType({})).toBe(ObjectType.JSON);
  });
});

describe('rowToTriples', () => {
  it('should create triples from a data row', () => {
    const row = { name: 'Test', value: 42 };
    const triples = rowToTriples(row, 'test', 0, '01ARZ3NDEKTSV4RRFFQ69G5FAV', BigInt(Date.now()));

    // Should have $type triple + 2 field triples
    expect(triples.length).toBe(3);
  });

  it('should create $type triple first', () => {
    const row = { name: 'Test' };
    const triples = rowToTriples(row, 'myDataset', 5, '01ARZ3NDEKTSV4RRFFQ69G5FAV', BigInt(Date.now()));

    const typeTriple = triples.find((t) => t.predicate === '$type');
    expect(typeTriple).toBeDefined();
    expect(typeTriple!.object.type).toBe(ObjectType.URL);
    expect((typeTriple!.object as any).value).toBe('https://schema.workers.do/myDataset');
  });

  it('should use correct entity ID format', () => {
    const row = { name: 'Test' };
    const triples = rowToTriples(row, 'dataset', 123, '01ARZ3NDEKTSV4RRFFQ69G5FAV', BigInt(Date.now()));

    expect(triples[0]!.subject).toBe('https://graph.workers.do/dataset/123');
  });

  it('should skip null and undefined values', () => {
    const row = { name: 'Test', missing: null, also: undefined };
    const triples = rowToTriples(row, 'test', 0, '01ARZ3NDEKTSV4RRFFQ69G5FAV', BigInt(Date.now()));

    // Should have $type + name only (null/undefined skipped)
    expect(triples.length).toBe(2);
    expect(triples.some((t) => t.predicate === 'missing')).toBe(false);
    expect(triples.some((t) => t.predicate === 'also')).toBe(false);
  });

  it('should infer correct types for values', () => {
    const row = {
      strField: 'hello',
      intField: 42,
      floatField: 3.14,
      boolField: true,
    };
    const triples = rowToTriples(row, 'test', 0, '01ARZ3NDEKTSV4RRFFQ69G5FAV', BigInt(Date.now()));

    const strTriple = triples.find((t) => t.predicate === 'strField');
    const intTriple = triples.find((t) => t.predicate === 'intField');
    const floatTriple = triples.find((t) => t.predicate === 'floatField');
    const boolTriple = triples.find((t) => t.predicate === 'boolField');

    expect(strTriple!.object.type).toBe(ObjectType.STRING);
    expect(intTriple!.object.type).toBe(ObjectType.INT64);
    expect(floatTriple!.object.type).toBe(ObjectType.FLOAT64);
    expect(boolTriple!.object.type).toBe(ObjectType.BOOL);
  });
});

describe('generateTestData', () => {
  it('should generate specified number of rows', () => {
    const data = generateTestData(100);
    expect(data.length).toBe(100);
  });

  it('should generate rows with expected fields', () => {
    const data = generateTestData(1);
    const row = data[0]!;

    expect(row).toHaveProperty('id');
    expect(row).toHaveProperty('name');
    expect(row).toHaveProperty('category');
    expect(row).toHaveProperty('value');
    expect(row).toHaveProperty('score');
    expect(row).toHaveProperty('active');
  });

  it('should generate sequential IDs', () => {
    const data = generateTestData(5);

    expect(data[0]!.id).toBe(0);
    expect(data[1]!.id).toBe(1);
    expect(data[2]!.id).toBe(2);
    expect(data[3]!.id).toBe(3);
    expect(data[4]!.id).toBe(4);
  });

  it('should cycle through categories A-E', () => {
    const data = generateTestData(10);
    const categories = data.map((r) => r.category);

    expect(categories[0]).toBe('A');
    expect(categories[1]).toBe('B');
    expect(categories[2]).toBe('C');
    expect(categories[3]).toBe('D');
    expect(categories[4]).toBe('E');
    expect(categories[5]).toBe('A');
  });

  it('should generate random values and scores', () => {
    const data = generateTestData(100);

    // Check values are within expected range
    expect(data.every((r) => (r.value as number) >= 0 && (r.value as number) < 1000)).toBe(true);
    expect(data.every((r) => (r.score as number) >= 0 && (r.score as number) < 100)).toBe(true);
  });

  it('should alternate active boolean', () => {
    const data = generateTestData(4);

    expect(data[0]!.active).toBe(true);
    expect(data[1]!.active).toBe(false);
    expect(data[2]!.active).toBe(true);
    expect(data[3]!.active).toBe(false);
  });
});

// ============================================================================
// Benchmark Query Tests
// ============================================================================

describe('BENCH_QUERIES', () => {
  it('should have queries for test dataset', () => {
    expect(BENCH_QUERIES['test']).toBeDefined();
    expect(BENCH_QUERIES['test']!.length).toBeGreaterThan(0);
  });

  it('should have queries for onet dataset', () => {
    expect(BENCH_QUERIES['onet']).toBeDefined();
    expect(BENCH_QUERIES['onet']!.length).toBeGreaterThan(0);
  });

  it('should have queries for imdb dataset', () => {
    expect(BENCH_QUERIES['imdb']).toBeDefined();
    expect(BENCH_QUERIES['imdb']!.length).toBeGreaterThan(0);
  });

  it('should have required query types for test dataset', () => {
    const testQueries = BENCH_QUERIES['test']!;
    const types = testQueries.map((q) => q.type);

    expect(types).toContain('count');
    expect(types).toContain('filter');
    expect(types).toContain('group_by');
    expect(types).toContain('aggregate');
    expect(types).toContain('point_lookup');
    expect(types).toContain('traversal');
  });
});

describe('executeBenchQuery', () => {
  let store: InMemoryTripleStore;

  beforeEach(() => {
    store = new InMemoryTripleStore();
    // Populate with test data
    const testData = generateTestData(100);
    const txId = '01ARZ3NDEKTSV4RRFFQ69G5FAV';
    const timestamp = BigInt(Date.now());

    for (let i = 0; i < testData.length; i++) {
      const triples = rowToTriples(testData[i]!, 'test', i, txId, timestamp);
      for (const triple of triples) {
        store.insert(triple);
      }
    }
  });

  it('should execute count query', () => {
    const query: BenchQuery = { id: 'Q0', name: 'Count', type: 'count' };
    const result = executeBenchQuery(store, query, 'test');

    expect(result.rowCount).toBe(100);
    expect((result.data as any).count).toBe(100);
    expect((result.data as any).tripleCount).toBeGreaterThan(100);
  });

  it('should execute filter query', () => {
    const query: BenchQuery = { id: 'Q1', name: 'Filter', type: 'filter' };
    const result = executeBenchQuery(store, query, 'test');

    // Should filter by category 'A' (20 of 100 entities)
    expect(result.rowCount).toBe(20);
  });

  it('should execute group_by query', () => {
    const query: BenchQuery = { id: 'Q2', name: 'GroupBy', type: 'group_by' };
    const result = executeBenchQuery(store, query, 'test');

    // Should group by category (5 categories: A, B, C, D, E)
    expect(result.rowCount).toBe(5);
  });

  it('should execute aggregate query', () => {
    const query: BenchQuery = { id: 'Q3', name: 'Aggregate', type: 'aggregate' };
    const result = executeBenchQuery(store, query, 'test');

    expect(result.rowCount).toBe(100);
    expect((result.data as any).count).toBe(100);
    expect((result.data as any).sum).toBeGreaterThan(0);
  });

  it('should execute point_lookup query', () => {
    const query: BenchQuery = { id: 'Q4', name: 'PointLookup', type: 'point_lookup' };
    const result = executeBenchQuery(store, query, 'test');

    expect(result.rowCount).toBeGreaterThan(0);
    expect((result.data as any).entityId).toBeDefined();
  });

  it('should execute traversal query', () => {
    const query: BenchQuery = { id: 'Q5', name: 'Traversal', type: 'traversal' };
    const result = executeBenchQuery(store, query, 'test');

    // Result should include at least the starting entity
    expect(result.rowCount).toBeGreaterThanOrEqual(1);
    expect((result.data as any).startEntity).toBeDefined();
  });

  it('should return zero for unknown query type', () => {
    const query: BenchQuery = { id: 'QX', name: 'Unknown', type: 'unknown' as any };
    const result = executeBenchQuery(store, query, 'test');

    expect(result.rowCount).toBe(0);
  });

  it('should handle empty store gracefully', () => {
    const emptyStore = new InMemoryTripleStore();
    const query: BenchQuery = { id: 'Q0', name: 'Count', type: 'count' };
    const result = executeBenchQuery(emptyStore, query, 'test');

    expect(result.rowCount).toBe(0);
  });
});
