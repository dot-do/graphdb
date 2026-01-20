/**
 * In-Memory Triple Store for Benchmarking
 *
 * A lightweight in-memory triple store implementation used by the /bench endpoint
 * for jsonbench-compatible benchmarking. This store provides SPO and POS indexes
 * for efficient querying patterns.
 */

import { ObjectType } from '../core/types.js';
import type { Triple, TypedObject } from '../core/triple.js';

/**
 * In-memory triple store for /bench endpoint (jsonbench-compatible)
 *
 * Provides basic graph operations:
 * - SPO index: subject -> predicate -> objects
 * - POS index: predicate -> object type -> subjects
 *
 * Supports queries:
 * - getBySubject: Retrieve all triples for an entity
 * - getByPredicate: Find all subjects with a predicate
 * - getValue: Point lookup for a specific subject+predicate
 * - filterByPredicateValue: Filter subjects by predicate value
 * - aggregate: Sum/count for numeric predicates
 * - groupBy: Group subjects by predicate value
 * - traverse1Hop: 1-hop graph traversal following REF edges
 */
export class InMemoryTripleStore {
  private spo: Map<string, Map<string, TypedObject[]>> = new Map();
  private pos: Map<string, Map<number, Set<string>>> = new Map();
  private tripleCount = 0;

  insert(triple: Triple): void {
    const { subject, predicate, object } = triple;

    // SPO index
    if (!this.spo.has(subject)) {
      this.spo.set(subject, new Map());
    }
    const predicates = this.spo.get(subject)!;
    if (!predicates.has(predicate)) {
      predicates.set(predicate, []);
    }
    predicates.get(predicate)!.push(object);

    // POS index
    if (!this.pos.has(predicate)) {
      this.pos.set(predicate, new Map());
    }
    const objTypes = this.pos.get(predicate)!;
    if (!objTypes.has(object.type)) {
      objTypes.set(object.type, new Set());
    }
    objTypes.get(object.type)!.add(subject);

    this.tripleCount++;
  }

  getBySubject(subject: string): Triple[] {
    const predicates = this.spo.get(subject);
    if (!predicates) return [];

    const results: Triple[] = [];
    for (const [predicate, objects] of predicates) {
      for (const obj of objects) {
        results.push({
          subject: subject as any,
          predicate: predicate as any,
          object: obj,
          timestamp: 0n,
          txId: '' as any,
        });
      }
    }
    return results;
  }

  getByPredicate(predicate: string): string[] {
    const objTypes = this.pos.get(predicate);
    if (!objTypes) return [];

    const subjects = new Set<string>();
    for (const subjectSet of objTypes.values()) {
      for (const subject of subjectSet) {
        subjects.add(subject);
      }
    }
    return Array.from(subjects);
  }

  getValue(subject: string, predicate: string): TypedObject | undefined {
    const predicates = this.spo.get(subject);
    if (!predicates) return undefined;

    const objects = predicates.get(predicate);
    if (!objects || objects.length === 0) return undefined;

    return objects[0];
  }

  getAllSubjects(): string[] {
    return Array.from(this.spo.keys());
  }

  count(): number {
    return this.tripleCount;
  }

  entityCount(): number {
    return this.spo.size;
  }

  filterByPredicateValue(
    predicate: string,
    filterFn: (obj: TypedObject) => boolean
  ): string[] {
    const subjects = this.getByPredicate(predicate);
    return subjects.filter((subject) => {
      const value = this.getValue(subject, predicate);
      return value !== undefined && filterFn(value);
    });
  }

  aggregate(predicate: string): { count: number; sum: number } {
    const subjects = this.getByPredicate(predicate);
    let sum = 0;

    for (const subject of subjects) {
      const obj = this.getValue(subject, predicate);
      if (obj) {
        if (typeof (obj as any).value === 'number') {
          sum += (obj as any).value;
        } else if (typeof (obj as any).value === 'bigint') {
          sum += Number((obj as any).value);
        }
      }
    }

    return { count: subjects.length, sum };
  }

  groupBy(predicate: string): Map<unknown, string[]> {
    const groups = new Map<unknown, string[]>();
    const subjects = this.getByPredicate(predicate);

    for (const subject of subjects) {
      const obj = this.getValue(subject, predicate);
      if (obj) {
        const key = (obj as any).value;
        if (!groups.has(key)) {
          groups.set(key, []);
        }
        groups.get(key)!.push(subject);
      }
    }

    return groups;
  }

  traverse1Hop(subject: string): {
    entity: Triple[];
    related: Map<string, Triple[]>;
  } {
    const entity = this.getBySubject(subject);
    const related = new Map<string, Triple[]>();

    for (const triple of entity) {
      if (triple.object.type === ObjectType.REF && typeof (triple.object as any).value === 'string') {
        const relatedTriples = this.getBySubject((triple.object as any).value);
        if (relatedTriples.length > 0) {
          related.set((triple.object as any).value, relatedTriples);
        }
      }
    }

    return { entity, related };
  }
}

/**
 * Infer ObjectType from a JavaScript value
 */
export function inferObjectType(value: unknown): ObjectType {
  if (value === null || value === undefined) return ObjectType.NULL;
  if (typeof value === 'boolean') return ObjectType.BOOL;
  if (typeof value === 'bigint') return ObjectType.INT64;
  if (typeof value === 'number') {
    return Number.isInteger(value) ? ObjectType.INT64 : ObjectType.FLOAT64;
  }
  if (typeof value === 'string') return ObjectType.STRING;
  if (value instanceof Date) return ObjectType.TIMESTAMP;
  if (typeof value === 'object') return ObjectType.JSON;
  return ObjectType.STRING;
}

/**
 * Convert a data row to triples for ingestion into the triple store
 */
export function rowToTriples(
  row: Record<string, unknown>,
  datasetId: string,
  rowIndex: number,
  txId: string,
  timestamp: bigint
): Triple[] {
  const triples: Triple[] = [];
  const entityId = `https://graph.workers.do/${datasetId}/${rowIndex}` as any;

  triples.push({
    subject: entityId,
    predicate: '$type' as any,
    object: { type: ObjectType.URL, value: `https://schema.workers.do/${datasetId}` },
    timestamp,
    txId: txId as any,
  });

  for (const [key, value] of Object.entries(row)) {
    if (value === null || value === undefined) continue;

    const objType = inferObjectType(value);
    triples.push({
      subject: entityId,
      predicate: key as any,
      object: { type: objType, value: value as any } as TypedObject,
      timestamp,
      txId: txId as any,
    });
  }

  return triples;
}

/**
 * Benchmark query definition
 */
export interface BenchQuery {
  id: string;
  name: string;
  type: 'count' | 'filter' | 'group_by' | 'aggregate' | 'traversal' | 'point_lookup';
}

/**
 * Predefined benchmark queries for different datasets
 */
export const BENCH_QUERIES: Record<string, BenchQuery[]> = {
  test: [
    { id: 'Q0_COUNT', name: 'Count entities', type: 'count' },
    { id: 'Q1_FILTER', name: 'Filter by category', type: 'filter' },
    { id: 'Q2_GROUPBY', name: 'Group by category', type: 'group_by' },
    { id: 'Q3_AGG', name: 'Aggregate values', type: 'aggregate' },
    { id: 'Q4_LOOKUP', name: 'Point lookup', type: 'point_lookup' },
    { id: 'Q5_TRAVERSE', name: '1-hop traversal', type: 'traversal' },
  ],
  onet: [
    { id: 'Q0_COUNT', name: 'Count entities', type: 'count' },
    { id: 'Q1_FILTER', name: 'Filter by job zone', type: 'filter' },
    { id: 'Q2_GROUPBY', name: 'Group by job zone', type: 'group_by' },
  ],
  imdb: [
    { id: 'Q0_COUNT', name: 'Count entities', type: 'count' },
    { id: 'Q1_FILTER', name: 'Filter by genre', type: 'filter' },
    { id: 'Q2_GROUPBY', name: 'Group by year', type: 'group_by' },
  ],
};

/**
 * Execute a benchmark query on the triple store
 */
export function executeBenchQuery(
  store: InMemoryTripleStore,
  query: BenchQuery,
  datasetId: string
): { rowCount: number; data?: unknown } {
  switch (query.type) {
    case 'count':
      return {
        rowCount: store.entityCount(),
        data: { count: store.entityCount(), tripleCount: store.count() },
      };

    case 'filter':
      const filterPred = datasetId === 'test' ? 'category' : datasetId === 'onet' ? 'jobZone' : 'genre';
      const filtered = store.filterByPredicateValue(filterPred, (obj) => {
        if (datasetId === 'test') {
          return (obj as any).value === 'A';
        }
        if (datasetId === 'onet') {
          const val = (obj as any).value;
          return (typeof val === 'number' || typeof val === 'bigint') && Number(val) >= 4;
        }
        return typeof (obj as any).value === 'string' && (obj as any).value.includes('Drama');
      });
      return { rowCount: filtered.length, data: filtered.slice(0, 20) };

    case 'group_by':
      const groupPred = datasetId === 'test' ? 'category' : datasetId === 'onet' ? 'jobZone' : 'year';
      const groups = store.groupBy(groupPred);
      const groupResult: Array<{ key: unknown; count: number }> = [];
      for (const [key, subjects] of groups) {
        groupResult.push({ key, count: subjects.length });
      }
      groupResult.sort((a, b) => b.count - a.count);
      return { rowCount: groupResult.length, data: groupResult.slice(0, 20) };

    case 'aggregate':
      const aggPred = datasetId === 'test' ? 'value' : datasetId === 'onet' ? 'jobZone' : 'numVotes';
      const agg = store.aggregate(aggPred);
      return {
        rowCount: agg.count,
        data: { count: agg.count, sum: agg.sum, avg: agg.count > 0 ? agg.sum / agg.count : 0 },
      };

    case 'point_lookup': {
      const subjects = store.getAllSubjects();
      if (subjects.length > 0) {
        const randomSubject = subjects[Math.floor(Math.random() * subjects.length)] as string;
        const triples = store.getBySubject(randomSubject);
        return { rowCount: triples.length, data: { entityId: randomSubject, tripleCount: triples.length } };
      }
      return { rowCount: 0 };
    }

    case 'traversal': {
      const allSubjects = store.getAllSubjects();
      if (allSubjects.length > 0) {
        const randomSubject = allSubjects[Math.floor(Math.random() * allSubjects.length)] as string;
        const traversal = store.traverse1Hop(randomSubject);
        return {
          rowCount: 1 + traversal.related.size,
          data: { startEntity: randomSubject, relatedCount: traversal.related.size },
        };
      }
      return { rowCount: 0 };
    }

    default:
      return { rowCount: 0 };
  }
}

/**
 * Simple data generator for /bench endpoint
 */
export function generateTestData(rows: number): Array<Record<string, unknown>> {
  const data: Array<Record<string, unknown>> = [];
  const categories = ['A', 'B', 'C', 'D', 'E'];

  for (let i = 0; i < rows; i++) {
    data.push({
      id: i,
      name: `Entity ${i}`,
      category: categories[i % categories.length],
      value: Math.floor(Math.random() * 1000),
      score: Math.random() * 100,
      active: i % 2 === 0,
    });
  }

  return data;
}
