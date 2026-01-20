/**
 * GraphDB Result Materializer Tests (E7.3: RED)
 *
 * Tests for materializing query results from triples to entities.
 *
 * TDD RED phase: These tests define the expected behavior.
 * All tests should fail until implementation is complete.
 */

import { describe, it, expect, vi } from 'vitest';
import {
  materializeTriples,
  groupBySubject,
  expandRefs,
  projectFields,
  formatResult,
  type MaterializeOptions,
  type EntityResolver,
} from '../../src/query/materializer';
import { Triple, createTriple } from '../../src/core/triple';
import { Entity, createEntity } from '../../src/core/entity';
import { ObjectType, createEntityId, createTransactionId } from '../../src/core/types';
import type { ExecutionResult } from '../../src/query/executor';

// ============================================================================
// Test Fixtures
// ============================================================================

const TX_ID = createTransactionId('01HQWV8X3KJ2M5N6P7Q8R9S0T1');

function createTestTriple(
  subject: string,
  predicate: string,
  value: unknown,
  objType?: ObjectType
): Triple {
  const entityId = createEntityId(subject);
  const triple = createTriple(entityId, predicate, value, TX_ID);
  if (objType !== undefined) {
    triple.object.type = objType;
  }
  return triple;
}

function createRefTriple(subject: string, predicate: string, refId: string): Triple {
  const entityId = createEntityId(subject);
  const refEntityId = createEntityId(refId);
  return {
    subject: entityId,
    predicate,
    object: {
      type: ObjectType.REF,
      value: refEntityId,
    },
    timestamp: BigInt(Date.now()),
    txId: TX_ID,
  };
}

// ============================================================================
// materializeTriples Tests
// ============================================================================

describe('materializeTriples', () => {
  it('should return empty array for empty triples', () => {
    const result = materializeTriples([]);
    expect(result).toEqual([]);
  });

  it('should group triples by subject into single entity', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createTestTriple('https://example.com/user/1', 'email', 'alice@example.com'),
      createTestTriple('https://example.com/user/1', 'age', 30),
    ];

    const result = materializeTriples(triples);

    expect(result).toHaveLength(1);
    expect(result[0].$id).toBe('https://example.com/user/1');
    expect(result[0].name).toBe('Alice');
    expect(result[0].email).toBe('alice@example.com');
    expect(result[0].age).toBe(30);
  });

  it('should create multiple entities from triples with different subjects', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createTestTriple('https://example.com/user/2', 'name', 'Bob'),
      createTestTriple('https://example.com/user/1', 'role', 'admin'),
      createTestTriple('https://example.com/user/2', 'role', 'user'),
    ];

    const result = materializeTriples(triples);

    expect(result).toHaveLength(2);

    const alice = result.find((e) => e.$id === 'https://example.com/user/1');
    const bob = result.find((e) => e.$id === 'https://example.com/user/2');

    expect(alice).toBeDefined();
    expect(alice?.name).toBe('Alice');
    expect(alice?.role).toBe('admin');

    expect(bob).toBeDefined();
    expect(bob?.name).toBe('Bob');
    expect(bob?.role).toBe('user');
  });

  it('should handle multiple values for same predicate as array', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createTestTriple('https://example.com/user/1', 'tag', 'vip'),
      createTestTriple('https://example.com/user/1', 'tag', 'premium'),
      createTestTriple('https://example.com/user/1', 'tag', 'verified'),
    ];

    const result = materializeTriples(triples);

    expect(result).toHaveLength(1);
    expect(result[0].tag).toEqual(['vip', 'premium', 'verified']);
  });

  it('should extract $type from triples', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', '$type', 'Person'),
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
    ];

    const result = materializeTriples(triples);

    expect(result).toHaveLength(1);
    expect(result[0].$type).toBe('Person');
  });

  it('should handle REF type values correctly', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createRefTriple(
        'https://example.com/user/1',
        'manager',
        'https://example.com/user/2'
      ),
    ];

    const result = materializeTriples(triples);

    expect(result).toHaveLength(1);
    expect(result[0].manager).toEqual({ '@ref': 'https://example.com/user/2' });
  });

  it('should handle various typed object values', () => {
    const entityId = createEntityId('https://example.com/item/1');
    const triples: Triple[] = [
      {
        subject: entityId,
        predicate: 'active',
        object: { type: ObjectType.BOOL, value: true },
        timestamp: BigInt(Date.now()),
        txId: TX_ID,
      },
      {
        subject: entityId,
        predicate: 'count',
        object: { type: ObjectType.INT64, value: BigInt(42) },
        timestamp: BigInt(Date.now()),
        txId: TX_ID,
      },
      {
        subject: entityId,
        predicate: 'price',
        object: { type: ObjectType.FLOAT64, value: 19.99 },
        timestamp: BigInt(Date.now()),
        txId: TX_ID,
      },
      {
        subject: entityId,
        predicate: 'createdAt',
        object: { type: ObjectType.TIMESTAMP, value: BigInt(1705680000000) },
        timestamp: BigInt(Date.now()),
        txId: TX_ID,
      },
    ];

    const result = materializeTriples(triples);

    expect(result).toHaveLength(1);
    expect(result[0].active).toBe(true);
    expect(result[0].count).toBe(BigInt(42));
    expect(result[0].price).toBe(19.99);
    expect(result[0].createdAt).toBe(BigInt(1705680000000));
  });
});

// ============================================================================
// groupBySubject Tests
// ============================================================================

describe('groupBySubject', () => {
  it('should return empty map for empty triples', () => {
    const result = groupBySubject([]);
    expect(result.size).toBe(0);
  });

  it('should group all triples for same subject together', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createTestTriple('https://example.com/user/1', 'email', 'alice@example.com'),
    ];

    const result = groupBySubject(triples);

    expect(result.size).toBe(1);
    expect(result.has('https://example.com/user/1')).toBe(true);
    expect(result.get('https://example.com/user/1')).toHaveLength(2);
  });

  it('should separate triples by subject', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createTestTriple('https://example.com/user/2', 'name', 'Bob'),
      createTestTriple('https://example.com/user/3', 'name', 'Charlie'),
    ];

    const result = groupBySubject(triples);

    expect(result.size).toBe(3);
    expect(result.get('https://example.com/user/1')).toHaveLength(1);
    expect(result.get('https://example.com/user/2')).toHaveLength(1);
    expect(result.get('https://example.com/user/3')).toHaveLength(1);
  });

  it('should preserve order within groups', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'first', 'value1'),
      createTestTriple('https://example.com/user/1', 'second', 'value2'),
      createTestTriple('https://example.com/user/1', 'third', 'value3'),
    ];

    const result = groupBySubject(triples);
    const group = result.get('https://example.com/user/1')!;

    expect(group[0].predicate).toBe('first');
    expect(group[1].predicate).toBe('second');
    expect(group[2].predicate).toBe('third');
  });
});

// ============================================================================
// expandRefs Tests
// ============================================================================

describe('expandRefs', () => {
  it('should return entity unchanged when no refs present', async () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', age: 30 }
    );

    const resolver: EntityResolver = vi.fn().mockResolvedValue(null);
    const result = await expandRefs(entity, resolver);

    expect(result.name).toBe('Alice');
    expect(result.age).toBe(30);
    expect(resolver).not.toHaveBeenCalled();
  });

  it('should expand single ref to nested entity', async () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', manager: { '@ref': 'https://example.com/user/2' } }
    );

    const managerEntity = createEntity(
      createEntityId('https://example.com/user/2'),
      'Person',
      { name: 'Bob', title: 'Manager' }
    );

    const resolver: EntityResolver = vi.fn().mockResolvedValue(managerEntity);
    const result = await expandRefs(entity, resolver);

    expect(result.manager).toBeDefined();
    expect((result.manager as Entity).name).toBe('Bob');
    expect((result.manager as Entity).title).toBe('Manager');
  });

  it('should expand nested refs up to maxDepth', async () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', manager: { '@ref': 'https://example.com/user/2' } }
    );

    const manager = createEntity(
      createEntityId('https://example.com/user/2'),
      'Person',
      { name: 'Bob', manager: { '@ref': 'https://example.com/user/3' } }
    );

    const executive = createEntity(
      createEntityId('https://example.com/user/3'),
      'Person',
      { name: 'Carol', title: 'CEO' }
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      if (id === 'https://example.com/user/2') return manager;
      if (id === 'https://example.com/user/3') return executive;
      return null;
    });

    const result = await expandRefs(entity, resolver, { maxDepth: 2 });

    expect((result.manager as Entity).name).toBe('Bob');
    expect(((result.manager as Entity).manager as Entity).name).toBe('Carol');
  });

  it('should stop expansion at maxDepth', async () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', manager: { '@ref': 'https://example.com/user/2' } }
    );

    const manager = createEntity(
      createEntityId('https://example.com/user/2'),
      'Person',
      { name: 'Bob', manager: { '@ref': 'https://example.com/user/3' } }
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      if (id === 'https://example.com/user/2') return manager;
      return null;
    });

    const result = await expandRefs(entity, resolver, { maxDepth: 1 });

    expect((result.manager as Entity).name).toBe('Bob');
    // Manager's manager should remain as ref (not expanded due to depth limit)
    expect((result.manager as Entity).manager).toEqual({
      '@ref': 'https://example.com/user/3',
    });
  });

  it('should handle null resolution gracefully', async () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', manager: { '@ref': 'https://example.com/user/999' } }
    );

    const resolver: EntityResolver = vi.fn().mockResolvedValue(null);
    const result = await expandRefs(entity, resolver);

    // Should keep original ref if resolution fails
    expect(result.manager).toEqual({ '@ref': 'https://example.com/user/999' });
  });

  it('should expand refs in arrays', async () => {
    const entity = createEntity(
      createEntityId('https://example.com/team/1'),
      'Team',
      {
        name: 'Engineering',
        members: [
          { '@ref': 'https://example.com/user/1' },
          { '@ref': 'https://example.com/user/2' },
        ],
      }
    );

    const user1 = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice' }
    );

    const user2 = createEntity(
      createEntityId('https://example.com/user/2'),
      'Person',
      { name: 'Bob' }
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      if (id === 'https://example.com/user/1') return user1;
      if (id === 'https://example.com/user/2') return user2;
      return null;
    });

    const result = await expandRefs(entity, resolver);

    expect(Array.isArray(result.members)).toBe(true);
    const members = result.members as Entity[];
    expect(members[0].name).toBe('Alice');
    expect(members[1].name).toBe('Bob');
  });
});

// ============================================================================
// projectFields Tests
// ============================================================================

describe('projectFields', () => {
  it('should always include $id, $type, $context', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', email: 'alice@example.com', age: 30 }
    );

    const result = projectFields(entity, ['name']);

    expect(result.$id).toBe('https://example.com/user/1');
    expect(result.$type).toBe('Person');
    expect(result.$context).toBeDefined();
  });

  it('should include only specified fields', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', email: 'alice@example.com', age: 30, role: 'admin' }
    );

    const result = projectFields(entity, ['name', 'email']);

    expect(result.name).toBe('Alice');
    expect(result.email).toBe('alice@example.com');
    expect(result.age).toBeUndefined();
    expect(result.role).toBeUndefined();
  });

  it('should handle empty fields array (return only metadata)', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', email: 'alice@example.com' }
    );

    const result = projectFields(entity, []);

    expect(result.$id).toBe('https://example.com/user/1');
    expect(result.$type).toBe('Person');
    expect(result.name).toBeUndefined();
    expect(result.email).toBeUndefined();
  });

  it('should ignore non-existent fields', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice' }
    );

    const result = projectFields(entity, ['name', 'nonExistent']);

    expect(result.name).toBe('Alice');
    expect(result).not.toHaveProperty('nonExistent');
  });

  it('should handle metadata fields (_namespace, _localId)', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice' }
    );

    const result = projectFields(entity, ['name', '_namespace', '_localId']);

    expect(result.name).toBe('Alice');
    expect(result._namespace).toBe('https://example.com');
    expect(result._localId).toBe('1');
  });
});

// ============================================================================
// formatResult Tests
// ============================================================================

describe('formatResult', () => {
  it('should format execution result with entities', () => {
    const entity1 = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice' }
    );

    const entity2 = createEntity(
      createEntityId('https://example.com/user/2'),
      'Person',
      { name: 'Bob' }
    );

    const executionResult: ExecutionResult = {
      entities: [entity1, entity2],
      triples: [],
      hasMore: false,
      stats: {
        shardQueries: 2,
        entitiesScanned: 10,
        durationMs: 25,
      },
    };

    const result = formatResult(executionResult);

    expect(result.data).toHaveLength(2);
    expect(result.data[0].name).toBe('Alice');
    expect(result.data[1].name).toBe('Bob');
  });

  it('should include pagination info when cursor present', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice' }
    );

    const executionResult: ExecutionResult = {
      entities: [entity],
      triples: [],
      cursor: 'eyJvZmZzZXQiOjEwfQ==',
      hasMore: true,
      stats: {
        shardQueries: 1,
        entitiesScanned: 10,
        durationMs: 15,
      },
    };

    const result = formatResult(executionResult);

    expect(result.pagination).toBeDefined();
    expect(result.pagination?.cursor).toBe('eyJvZmZzZXQiOjEwfQ==');
    expect(result.pagination?.hasMore).toBe(true);
  });

  it('should not include pagination when no more results', () => {
    const executionResult: ExecutionResult = {
      entities: [],
      triples: [],
      hasMore: false,
      stats: {
        shardQueries: 1,
        entitiesScanned: 0,
        durationMs: 5,
      },
    };

    const result = formatResult(executionResult);

    expect(result.pagination).toBeUndefined();
  });

  it('should include meta with duration and shardQueries', () => {
    const executionResult: ExecutionResult = {
      entities: [],
      triples: [],
      hasMore: false,
      stats: {
        shardQueries: 5,
        entitiesScanned: 100,
        durationMs: 42,
      },
    };

    const result = formatResult(executionResult);

    expect(result.meta).toBeDefined();
    expect(result.meta?.duration).toBe(42);
    expect(result.meta?.shardQueries).toBe(5);
  });

  it('should project fields when specified in options', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice', email: 'alice@example.com', age: 30 }
    );

    const executionResult: ExecutionResult = {
      entities: [entity],
      triples: [],
      hasMore: false,
      stats: {
        shardQueries: 1,
        entitiesScanned: 1,
        durationMs: 10,
      },
    };

    const options: MaterializeOptions = {
      fields: ['name'],
    };

    const result = formatResult(executionResult, options);

    expect(result.data[0].name).toBe('Alice');
    expect(result.data[0].email).toBeUndefined();
    expect(result.data[0].age).toBeUndefined();
  });

  it('should exclude metadata when includeMetadata is false', () => {
    const entity = createEntity(
      createEntityId('https://example.com/user/1'),
      'Person',
      { name: 'Alice' }
    );

    const executionResult: ExecutionResult = {
      entities: [entity],
      triples: [],
      hasMore: false,
      stats: {
        shardQueries: 1,
        entitiesScanned: 1,
        durationMs: 10,
      },
    };

    const options: MaterializeOptions = {
      includeMetadata: false,
    };

    const result = formatResult(executionResult, options);

    expect(result.data[0]._namespace).toBeUndefined();
    expect(result.data[0]._localId).toBeUndefined();
    expect(result.data[0].$id).toBeDefined(); // $id should always be present
  });

  it('should materialize triples when entities array is empty', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/user/1', 'name', 'Alice'),
      createTestTriple('https://example.com/user/1', 'email', 'alice@example.com'),
    ];

    const executionResult: ExecutionResult = {
      entities: [],
      triples,
      hasMore: false,
      stats: {
        shardQueries: 1,
        entitiesScanned: 1,
        durationMs: 10,
      },
    };

    const result = formatResult(executionResult);

    expect(result.data).toHaveLength(1);
    expect(result.data[0].name).toBe('Alice');
    expect(result.data[0].email).toBe('alice@example.com');
  });
});
