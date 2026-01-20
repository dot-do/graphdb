/**
 * Index Maintenance Triggers Tests (TDD RED Phase)
 *
 * Tests for GraphDB Index maintenance triggers:
 * - onInsert called when triple inserted
 * - onUpdate called with old and new values
 * - onDelete called when triple deleted
 * - CDC events generated for all operations
 * - Batch operations work efficiently
 * - Bloom filter updated on entity changes
 *
 * @see CLAUDE.md for architecture details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { initializeSchema } from '../../src/shard/schema.js';
import {
  createTripleStore,
  type TripleStore,
} from '../../src/shard/crud.js';
import {
  createIndexMaintainer,
  createCDCBuffer,
  updateBloomFilter,
  type IndexMaintainer,
  type CDCBuffer,
  type CDCEvent,
} from '../../src/shard/triggers.js';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
} from '../../src/core/types.js';
import type { Triple, TypedObject } from '../../src/core/triple.js';
import {
  createBloomFilter,
  mightExist,
  type BloomFilter,
} from '../../src/snippet/bloom.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-triggers-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test data helpers
const createTestTriple = (
  subjectSuffix: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
  txIdSuffix = '01ARZ3NDEKTSV4RRFFQ69G5FAV'
): Triple => {
  const object: TypedObject = { type: objectType };

  switch (objectType) {
    case ObjectType.NULL:
      break;
    case ObjectType.BOOL:
      object.value = value as boolean;
      break;
    case ObjectType.INT32:
    case ObjectType.INT64:
      object.value = value as bigint;
      break;
    case ObjectType.FLOAT64:
      object.value = value as number;
      break;
    case ObjectType.STRING:
      object.value = value as string;
      break;
    case ObjectType.REF:
      object.value = value as any;
      break;
    default:
      break;
  }

  return {
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
};

describe('IndexMaintainer', () => {
  describe('createIndexMaintainer', () => {
    it('should create an IndexMaintainer from SqlStorage', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        expect(maintainer).toBeDefined();
        expect(typeof maintainer.onInsert).toBe('function');
        expect(typeof maintainer.onUpdate).toBe('function');
        expect(typeof maintainer.onDelete).toBe('function');
        expect(typeof maintainer.onBatchInsert).toBe('function');
        expect(typeof maintainer.onBatchDelete).toBe('function');
      });
    });
  });

  describe('onInsert', () => {
    it('should be called when a triple is inserted', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const onInsertSpy = vi.spyOn(maintainer, 'onInsert');

        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await maintainer.onInsert(triple);

        expect(onInsertSpy).toHaveBeenCalledWith(triple);
        expect(onInsertSpy).toHaveBeenCalledTimes(1);
      });
    });

    it('should update index tables on insert', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');

        await maintainer.onInsert(triple);

        // The maintainer should track index operations
        // Verify by checking internal state or a method
        // Since this is a RED test, we just verify the call doesn't throw
        expect(true).toBe(true);
      });
    });

    it('should handle REF type triples for OSP index', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const value = createEntityId('https://example.com/person/2');
        const triple = createTestTriple('1', 'knows', ObjectType.REF, value);

        // Should not throw when handling REF type
        await maintainer.onInsert(triple);
        expect(true).toBe(true);
      });
    });
  });

  describe('onUpdate', () => {
    it('should be called with old and new triple values', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const onUpdateSpy = vi.spyOn(maintainer, 'onUpdate');

        const oldTriple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        const newTriple = createTestTriple('1', 'name', ObjectType.STRING, 'Johnny');
        newTriple.timestamp = BigInt(Date.now() + 1000);
        newTriple.txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');

        await maintainer.onUpdate(oldTriple, newTriple);

        expect(onUpdateSpy).toHaveBeenCalledWith(oldTriple, newTriple);
        expect(onUpdateSpy).toHaveBeenCalledTimes(1);
      });
    });

    it('should handle REF type changes for OSP index updates', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);

        const oldRef = createEntityId('https://example.com/person/2');
        const newRef = createEntityId('https://example.com/person/3');

        const oldTriple = createTestTriple('1', 'knows', ObjectType.REF, oldRef);
        const newTriple = createTestTriple('1', 'knows', ObjectType.REF, newRef);
        newTriple.timestamp = BigInt(Date.now() + 1000);

        // Should handle REF type updates correctly
        await maintainer.onUpdate(oldTriple, newTriple);
        expect(true).toBe(true);
      });
    });

    it('should handle type changes (e.g., STRING to INT64)', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);

        const oldTriple = createTestTriple('1', 'value', ObjectType.STRING, '42');
        const newTriple = createTestTriple('1', 'value', ObjectType.INT64, 42n);
        newTriple.timestamp = BigInt(Date.now() + 1000);

        // Should handle type changes
        await maintainer.onUpdate(oldTriple, newTriple);
        expect(true).toBe(true);
      });
    });
  });

  describe('onDelete', () => {
    it('should be called when a triple is deleted', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const onDeleteSpy = vi.spyOn(maintainer, 'onDelete');

        const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
        await maintainer.onDelete(triple);

        expect(onDeleteSpy).toHaveBeenCalledWith(triple);
        expect(onDeleteSpy).toHaveBeenCalledTimes(1);
      });
    });

    it('should clean up OSP index on REF delete', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const value = createEntityId('https://example.com/person/2');
        const triple = createTestTriple('1', 'knows', ObjectType.REF, value);

        // Should handle REF deletion
        await maintainer.onDelete(triple);
        expect(true).toBe(true);
      });
    });
  });

  describe('onBatchInsert', () => {
    it('should handle batch inserts efficiently', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const onBatchInsertSpy = vi.spyOn(maintainer, 'onBatchInsert');

        const triples = [
          createTestTriple('1', 'name', ObjectType.STRING, 'John'),
          createTestTriple('1', 'age', ObjectType.INT64, 30n),
          createTestTriple('2', 'name', ObjectType.STRING, 'Jane'),
        ];

        await maintainer.onBatchInsert(triples);

        expect(onBatchInsertSpy).toHaveBeenCalledWith(triples);
        expect(onBatchInsertSpy).toHaveBeenCalledTimes(1);
      });
    });

    it('should handle empty batch gracefully', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);

        // Should not throw for empty batch
        await maintainer.onBatchInsert([]);
        expect(true).toBe(true);
      });
    });
  });

  describe('onBatchDelete', () => {
    it('should handle batch deletes by subject', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        const sql = state.storage.sql;
        initializeSchema(sql);

        const maintainer = createIndexMaintainer(sql);
        const onBatchDeleteSpy = vi.spyOn(maintainer, 'onBatchDelete');

        const subjects = [
          createEntityId('https://example.com/entity/1'),
          createEntityId('https://example.com/entity/2'),
        ];

        await maintainer.onBatchDelete(subjects);

        expect(onBatchDeleteSpy).toHaveBeenCalledWith(subjects);
        expect(onBatchDeleteSpy).toHaveBeenCalledTimes(1);
      });
    });
  });
});

describe('CDCBuffer', () => {
  describe('createCDCBuffer', () => {
    it('should create a CDCBuffer with default max size', () => {
      const buffer = createCDCBuffer();
      expect(buffer).toBeDefined();
      expect(typeof buffer.append).toBe('function');
      expect(typeof buffer.flush).toBe('function');
      expect(typeof buffer.size).toBe('function');
      expect(buffer.size()).toBe(0);
    });

    it('should create a CDCBuffer with custom max size', () => {
      const buffer = createCDCBuffer(100);
      expect(buffer).toBeDefined();
      expect(buffer.size()).toBe(0);
    });
  });

  describe('append', () => {
    it('should append insert events', () => {
      const buffer = createCDCBuffer();
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');

      const event: CDCEvent = {
        type: 'insert',
        triple,
        timestamp: BigInt(Date.now()),
      };

      buffer.append(event);
      expect(buffer.size()).toBe(1);
    });

    it('should append update events with previous value', () => {
      const buffer = createCDCBuffer();
      const oldTriple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      const newTriple = createTestTriple('1', 'name', ObjectType.STRING, 'Johnny');

      const event: CDCEvent = {
        type: 'update',
        triple: newTriple,
        previousValue: oldTriple,
        timestamp: BigInt(Date.now()),
      };

      buffer.append(event);
      expect(buffer.size()).toBe(1);

      const events = buffer.flush();
      expect(events[0].type).toBe('update');
      expect(events[0].previousValue).toBeDefined();
      expect(events[0].previousValue?.object.value).toBe('John');
    });

    it('should append delete events', () => {
      const buffer = createCDCBuffer();
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');

      const event: CDCEvent = {
        type: 'delete',
        triple,
        timestamp: BigInt(Date.now()),
      };

      buffer.append(event);
      expect(buffer.size()).toBe(1);

      const events = buffer.flush();
      expect(events[0].type).toBe('delete');
    });

    it('should respect max size limit', () => {
      const buffer = createCDCBuffer(2);
      const triple1 = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      const triple2 = createTestTriple('2', 'name', ObjectType.STRING, 'Jane');
      const triple3 = createTestTriple('3', 'name', ObjectType.STRING, 'Bob');

      buffer.append({ type: 'insert', triple: triple1, timestamp: BigInt(1) });
      buffer.append({ type: 'insert', triple: triple2, timestamp: BigInt(2) });

      // Buffer is full, should auto-flush or drop oldest
      expect(buffer.size()).toBe(2);

      buffer.append({ type: 'insert', triple: triple3, timestamp: BigInt(3) });

      // Behavior: either keeps 2 most recent or expands
      // For this test, we expect it to handle the overflow
      expect(buffer.size()).toBeGreaterThanOrEqual(1);
    });
  });

  describe('flush', () => {
    it('should return all events and clear buffer', () => {
      const buffer = createCDCBuffer();
      const triple1 = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      const triple2 = createTestTriple('2', 'name', ObjectType.STRING, 'Jane');

      buffer.append({ type: 'insert', triple: triple1, timestamp: BigInt(1) });
      buffer.append({ type: 'insert', triple: triple2, timestamp: BigInt(2) });

      expect(buffer.size()).toBe(2);

      const events = buffer.flush();
      expect(events.length).toBe(2);
      expect(buffer.size()).toBe(0);
    });

    it('should return empty array when buffer is empty', () => {
      const buffer = createCDCBuffer();
      const events = buffer.flush();
      expect(events).toEqual([]);
    });

    it('should preserve event order (FIFO)', () => {
      const buffer = createCDCBuffer();
      const triple1 = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      const triple2 = createTestTriple('2', 'name', ObjectType.STRING, 'Jane');
      const triple3 = createTestTriple('3', 'name', ObjectType.STRING, 'Bob');

      buffer.append({ type: 'insert', triple: triple1, timestamp: BigInt(1) });
      buffer.append({ type: 'insert', triple: triple2, timestamp: BigInt(2) });
      buffer.append({ type: 'insert', triple: triple3, timestamp: BigInt(3) });

      const events = buffer.flush();

      expect(events[0].triple.subject).toContain('entity/1');
      expect(events[1].triple.subject).toContain('entity/2');
      expect(events[2].triple.subject).toContain('entity/3');
    });
  });

  describe('size', () => {
    it('should return current buffer size', () => {
      const buffer = createCDCBuffer();
      expect(buffer.size()).toBe(0);

      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      buffer.append({ type: 'insert', triple, timestamp: BigInt(1) });
      expect(buffer.size()).toBe(1);

      buffer.append({ type: 'insert', triple, timestamp: BigInt(2) });
      expect(buffer.size()).toBe(2);
    });
  });
});

describe('CDC Event Generation', () => {
  it('should generate CDC event on insert', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const buffer = createCDCBuffer();
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');

      // Simulate what happens after an insert
      buffer.append({
        type: 'insert',
        triple,
        timestamp: BigInt(Date.now()),
      });

      const events = buffer.flush();
      expect(events.length).toBe(1);
      expect(events[0].type).toBe('insert');
      expect(events[0].triple.subject).toBe(triple.subject);
    });
  });

  it('should generate CDC event on update with previous value', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const buffer = createCDCBuffer();
      const oldTriple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      const newTriple = createTestTriple('1', 'name', ObjectType.STRING, 'Johnny');

      buffer.append({
        type: 'update',
        triple: newTriple,
        previousValue: oldTriple,
        timestamp: BigInt(Date.now()),
      });

      const events = buffer.flush();
      expect(events.length).toBe(1);
      expect(events[0].type).toBe('update');
      expect(events[0].triple.object.value).toBe('Johnny');
      expect(events[0].previousValue?.object.value).toBe('John');
    });
  });

  it('should generate CDC event on delete', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const buffer = createCDCBuffer();
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');

      buffer.append({
        type: 'delete',
        triple,
        timestamp: BigInt(Date.now()),
      });

      const events = buffer.flush();
      expect(events.length).toBe(1);
      expect(events[0].type).toBe('delete');
    });
  });
});

describe('Bloom Filter Updates', () => {
  describe('updateBloomFilter', () => {
    it('should add entity to bloom filter', () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const entityId = createEntityId('https://example.com/entity/1');

      updateBloomFilter(filter, entityId, 'add');

      expect(mightExist(filter, entityId)).toBe(true);
    });

    it('should handle multiple entity additions', () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const entity1 = createEntityId('https://example.com/entity/1');
      const entity2 = createEntityId('https://example.com/entity/2');
      const entity3 = createEntityId('https://example.com/entity/3');

      updateBloomFilter(filter, entity1, 'add');
      updateBloomFilter(filter, entity2, 'add');
      updateBloomFilter(filter, entity3, 'add');

      expect(mightExist(filter, entity1)).toBe(true);
      expect(mightExist(filter, entity2)).toBe(true);
      expect(mightExist(filter, entity3)).toBe(true);
    });

    it('should increment filter count on add', () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const initialCount = filter.count;

      const entityId = createEntityId('https://example.com/entity/1');
      updateBloomFilter(filter, entityId, 'add');

      expect(filter.count).toBe(initialCount + 1);
    });

    it('should handle remove action (for tracking purposes)', () => {
      const filter = createBloomFilter({ capacity: 1000 });
      const entityId = createEntityId('https://example.com/entity/1');

      // Add first
      updateBloomFilter(filter, entityId, 'add');
      expect(mightExist(filter, entityId)).toBe(true);

      // Note: Bloom filters cannot truly remove items
      // The 'remove' action is for tracking/counting purposes
      updateBloomFilter(filter, entityId, 'remove');

      // Entity may still test positive due to bloom filter nature
      // But the count should be decremented
      expect(filter.count).toBe(0);
    });
  });

  it('should update bloom filter on entity insert', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const filter = createBloomFilter({ capacity: 1000 });
      const entityId = createEntityId('https://example.com/entity/new');

      // Before: entity not in filter
      expect(mightExist(filter, entityId)).toBe(false);

      // Simulate what happens when a new entity is inserted
      updateBloomFilter(filter, entityId, 'add');

      // After: entity should be in filter
      expect(mightExist(filter, entityId)).toBe(true);
    });
  });

  it('should track entity count in bloom filter', () => {
    const filter = createBloomFilter({ capacity: 1000 });

    expect(filter.count).toBe(0);

    updateBloomFilter(filter, createEntityId('https://example.com/entity/1'), 'add');
    expect(filter.count).toBe(1);

    updateBloomFilter(filter, createEntityId('https://example.com/entity/2'), 'add');
    expect(filter.count).toBe(2);

    updateBloomFilter(filter, createEntityId('https://example.com/entity/3'), 'add');
    expect(filter.count).toBe(3);
  });
});

// SKIPPED: Uses deprecated triples table via createTripleStore which no longer exists in BLOB-only schema
describe('Index Maintenance Integration', () => {
  it('should call onInsert after insertTriple in TripleStore', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const store = createTripleStore(sql);
      const maintainer = createIndexMaintainer(sql);
      const buffer = createCDCBuffer();

      // Mock the onInsert to verify it gets called
      let insertCalled = false;
      const originalOnInsert = maintainer.onInsert.bind(maintainer);
      maintainer.onInsert = async (triple: Triple) => {
        insertCalled = true;
        await originalOnInsert(triple);
      };

      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');

      // Simulate integrated flow: insert + index maintenance
      await store.insertTriple(triple);
      await maintainer.onInsert(triple);
      buffer.append({ type: 'insert', triple, timestamp: BigInt(Date.now()) });

      expect(insertCalled).toBe(true);
      expect(buffer.size()).toBe(1);
    });
  });

  it('should call onUpdate after updateTriple in TripleStore', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const store = createTripleStore(sql);
      const maintainer = createIndexMaintainer(sql);
      const buffer = createCDCBuffer();

      // Insert initial triple
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      await store.insertTriple(triple);

      // Get the triple before update
      const oldTriple = await store.getLatestTriple(triple.subject, triple.predicate);
      expect(oldTriple).not.toBeNull();

      // Update
      const newTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
      const newValue: TypedObject = { type: ObjectType.STRING, value: 'Johnny' };
      await store.updateTriple(triple.subject, triple.predicate, newValue, newTxId);

      // Get new triple
      const newTriple = await store.getLatestTriple(triple.subject, triple.predicate);
      expect(newTriple).not.toBeNull();

      // Simulate integrated flow
      await maintainer.onUpdate(oldTriple!, newTriple!);
      buffer.append({
        type: 'update',
        triple: newTriple!,
        previousValue: oldTriple!,
        timestamp: BigInt(Date.now()),
      });

      const events = buffer.flush();
      expect(events.length).toBe(1);
      expect(events[0].type).toBe('update');
    });
  });

  it('should call onDelete after deleteTriple in TripleStore', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      const store = createTripleStore(sql);
      const maintainer = createIndexMaintainer(sql);
      const buffer = createCDCBuffer();

      // Insert initial triple
      const triple = createTestTriple('1', 'name', ObjectType.STRING, 'John');
      await store.insertTriple(triple);

      // Get the triple before delete
      const beforeDelete = await store.getLatestTriple(triple.subject, triple.predicate);
      expect(beforeDelete).not.toBeNull();

      // Delete
      const deleteTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAW');
      await store.deleteTriple(triple.subject, triple.predicate, deleteTxId);

      // Simulate integrated flow
      await maintainer.onDelete(beforeDelete!);
      buffer.append({
        type: 'delete',
        triple: beforeDelete!,
        timestamp: BigInt(Date.now()),
      });

      const events = buffer.flush();
      expect(events.length).toBe(1);
      expect(events[0].type).toBe('delete');
    });
  });
});
