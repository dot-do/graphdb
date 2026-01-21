/**
 * Graph API Protocol Tests
 *
 * Tests for capnweb RPC integration including:
 * - GraphAPI methods work via capnweb
 * - Entity CRUD operations
 * - Traversal operations
 * - Query execution
 * - Batch operations
 * - Error handling
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  GraphAPITarget,
  type GraphAPI,
  type TraversalOptions,
  type BatchResult,
  type Triple,
  MAX_BATCH_SIZE,
  BatchSizeLimitError,
} from '../../src/protocol/graph-api';
import type { Entity } from '../../src/core/entity';
import { ObjectType, createEntityId } from '../../src/core/types';

describe('GraphAPITarget', () => {
  let api: GraphAPITarget;

  beforeEach(() => {
    // Use test mode to enable in-memory store for isolated testing
    api = new GraphAPITarget({ mode: 'test' });
  });

  // ==========================================================================
  // Entity CRUD Operations
  // ==========================================================================

  describe('Entity CRUD Operations', () => {
    describe('createEntity', () => {
      it('should create an entity with $id and $type', async () => {
        const entity: Entity = {
          $id: createEntityId('https://example.com/user/1'),
          $type: 'User',
          $context: 'https://example.com/user',
          _namespace: 'https://example.com' as any,
          _localId: '1',
          name: 'Alice',
          email: 'alice@example.com',
        };

        await api.createEntity(entity);

        const retrieved = await api.getEntity('https://example.com/user/1');
        expect(retrieved).not.toBeNull();
        expect(retrieved?.$id).toBe('https://example.com/user/1');
        expect(retrieved?.$type).toBe('User');
        expect(retrieved?.name).toBe('Alice');
        expect(retrieved?.email).toBe('alice@example.com');
      });

      it('should throw if entity has no $id', async () => {
        const entity = {
          $type: 'User',
          name: 'Alice',
        } as Entity;

        await expect(api.createEntity(entity)).rejects.toThrow('Entity creation failed: missing required field(s): $id');
      });

      it('should throw if entity has no $type', async () => {
        const entity = {
          $id: createEntityId('https://example.com/user/1'),
          name: 'Alice',
        } as Entity;

        await expect(api.createEntity(entity)).rejects.toThrow('Entity creation failed: missing required field(s): $type');
      });

      it('should throw if entity already exists', async () => {
        const entity: Entity = {
          $id: createEntityId('https://example.com/user/1'),
          $type: 'User',
          $context: 'https://example.com/user',
          _namespace: 'https://example.com' as any,
          _localId: '1',
          name: 'Alice',
        };

        await api.createEntity(entity);

        await expect(api.createEntity(entity)).rejects.toThrow('already exists');
      });
    });

    describe('getEntity', () => {
      it('should return null for non-existent entity', async () => {
        const result = await api.getEntity('https://example.com/user/nonexistent');
        expect(result).toBeNull();
      });

      it('should return the entity if it exists', async () => {
        const entity: Entity = {
          $id: createEntityId('https://example.com/user/1'),
          $type: 'User',
          $context: 'https://example.com/user',
          _namespace: 'https://example.com' as any,
          _localId: '1',
          name: 'Bob',
        };

        await api.createEntity(entity);

        const retrieved = await api.getEntity('https://example.com/user/1');
        expect(retrieved).not.toBeNull();
        expect(retrieved?.name).toBe('Bob');
      });
    });

    describe('updateEntity', () => {
      it('should update existing entity properties', async () => {
        const entity: Entity = {
          $id: createEntityId('https://example.com/user/1'),
          $type: 'User',
          $context: 'https://example.com/user',
          _namespace: 'https://example.com' as any,
          _localId: '1',
          name: 'Alice',
          age: 25,
        };

        await api.createEntity(entity);
        await api.updateEntity('https://example.com/user/1', { age: 26, city: 'NYC' });

        const updated = await api.getEntity('https://example.com/user/1');
        expect(updated?.age).toBe(26);
        expect(updated?.city).toBe('NYC');
        expect(updated?.name).toBe('Alice'); // Unchanged
      });

      it('should throw if entity does not exist', async () => {
        await expect(
          api.updateEntity('https://example.com/user/nonexistent', { name: 'Test' })
        ).rejects.toThrow('not found');
      });
    });

    describe('deleteEntity', () => {
      it('should delete an existing entity', async () => {
        const entity: Entity = {
          $id: createEntityId('https://example.com/user/1'),
          $type: 'User',
          $context: 'https://example.com/user',
          _namespace: 'https://example.com' as any,
          _localId: '1',
          name: 'Alice',
        };

        await api.createEntity(entity);
        await api.deleteEntity('https://example.com/user/1');

        const retrieved = await api.getEntity('https://example.com/user/1');
        expect(retrieved).toBeNull();
      });

      it('should throw if entity does not exist', async () => {
        await expect(api.deleteEntity('https://example.com/user/nonexistent')).rejects.toThrow(
          'not found'
        );
      });
    });
  });

  // ==========================================================================
  // Traversal Operations
  // ==========================================================================

  describe('Traversal Operations', () => {
    beforeEach(async () => {
      // Create a graph: Alice -> [friends] -> Bob, Charlie
      //                 Bob -> [friends] -> Diana
      //                 Alice -> [posts] -> Post1
      const alice: Entity = {
        $id: createEntityId('https://example.com/user/alice'),
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'https://example.com' as any,
        _localId: 'alice',
        name: 'Alice',
        friends: 'https://example.com/user/bob',
      };

      const bob: Entity = {
        $id: createEntityId('https://example.com/user/bob'),
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'https://example.com' as any,
        _localId: 'bob',
        name: 'Bob',
        friends: 'https://example.com/user/diana',
      };

      const charlie: Entity = {
        $id: createEntityId('https://example.com/user/charlie'),
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'https://example.com' as any,
        _localId: 'charlie',
        name: 'Charlie',
      };

      const diana: Entity = {
        $id: createEntityId('https://example.com/user/diana'),
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'https://example.com' as any,
        _localId: 'diana',
        name: 'Diana',
      };

      const post1: Entity = {
        $id: createEntityId('https://example.com/post/1'),
        $type: 'Post',
        $context: 'https://example.com/post',
        _namespace: 'https://example.com' as any,
        _localId: '1',
        title: 'Hello World',
        author: 'https://example.com/user/alice',
      };

      await api.createEntity(alice);
      await api.createEntity(bob);
      await api.createEntity(charlie);
      await api.createEntity(diana);
      await api.createEntity(post1);
    });

    describe('traverse', () => {
      it('should traverse forward relationships', async () => {
        const friends = await api.traverse('https://example.com/user/alice', 'friends');
        expect(friends.length).toBe(1);
        expect(friends[0].name).toBe('Bob');
      });

      it('should return empty array for non-existent relationship', async () => {
        const followers = await api.traverse('https://example.com/user/alice', 'followers');
        expect(followers.length).toBe(0);
      });

      it('should respect limit option', async () => {
        // Add more friends to alice
        const eve: Entity = {
          $id: createEntityId('https://example.com/user/eve'),
          $type: 'User',
          $context: 'https://example.com/user',
          _namespace: 'https://example.com' as any,
          _localId: 'eve',
          name: 'Eve',
        };
        await api.createEntity(eve);
        await api.updateEntity('https://example.com/user/alice', {
          moreFriends: 'https://example.com/user/eve',
        });

        const friends = await api.traverse('https://example.com/user/alice', 'friends', {
          limit: 1,
        });
        expect(friends.length).toBe(1);
      });
    });

    describe('reverseTraverse', () => {
      it('should traverse reverse relationships', async () => {
        const authors = await api.reverseTraverse(
          'https://example.com/user/alice',
          'author'
        );
        expect(authors.length).toBe(1);
        expect(authors[0].$type).toBe('Post');
        expect(authors[0].title).toBe('Hello World');
      });

      it('should return empty array for non-existent reverse relationship', async () => {
        const result = await api.reverseTraverse(
          'https://example.com/user/bob',
          'author'
        );
        expect(result.length).toBe(0);
      });
    });

    describe('pathTraverse', () => {
      it('should traverse multi-hop paths', async () => {
        // Alice -> friends -> Bob -> friends -> Diana
        const result = await api.pathTraverse('https://example.com/user/alice', [
          'friends',
          'friends',
        ]);
        expect(result.length).toBe(1);
        expect(result[0].name).toBe('Diana');
      });

      it('should return starting entity for empty path', async () => {
        const result = await api.pathTraverse('https://example.com/user/alice', []);
        expect(result.length).toBe(1);
        expect(result[0].name).toBe('Alice');
      });

      it('should return empty array if path leads nowhere', async () => {
        const result = await api.pathTraverse('https://example.com/user/diana', [
          'friends',
        ]);
        expect(result.length).toBe(0);
      });

      it('should respect maxDepth option', async () => {
        // Limit to 1 hop even though path has 2
        const result = await api.pathTraverse(
          'https://example.com/user/alice',
          ['friends', 'friends'],
          { maxDepth: 1 }
        );
        // With maxDepth=1, we only follow the first predicate
        expect(result.length).toBe(1);
        expect(result[0].name).toBe('Bob');
      });
    });
  });

  // ==========================================================================
  // Query Operations
  // ==========================================================================

  describe('Query Operations', () => {
    beforeEach(async () => {
      // Query format is "entity:id.predicate" - use simple ID for testing
      const user: Entity = {
        $id: createEntityId('https://example.com/user/test'),
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'https://example.com' as any,
        _localId: 'test',
        name: 'Test User',
      };
      await api.createEntity(user);

      // Also create a second user for query tests
      const secondUser: Entity = {
        $id: createEntityId('https://example.com/user/test2'),
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'example.com' as any,
        _localId: 'test2',
        name: 'Second User',
      };
      await api.createEntity(secondUser);
    });

    describe('query', () => {
      // Note: These tests use the mock GraphAPITarget which has simplified query parsing
      // Real query tests are in e2e/full-flow.test.ts with the orchestrator

      it('should execute simple entity lookup query', async () => {
        // Query with full URL
        const result = await api.query('https://example.com/user/test');
        expect(result.entities.length).toBe(1);
        expect(result.entities[0].name).toBe('Test User');
        expect(result.hasMore).toBe(false);
      });

      it('should return empty results for non-existent entity', async () => {
        // Query for non-existent entity
        const result = await api.query('https://example.com/user/nonexistent');
        expect(result.entities.length).toBe(0);
        expect(result.hasMore).toBe(false);
      });

      it('should include execution time in stats', async () => {
        // This test only verifies stats structure, doesn't depend on entity matching
        const result = await api.query('anything');
        expect(result.stats).toBeDefined();
        expect(result.stats.durationMs).toBeGreaterThanOrEqual(0);
      });
    });
  });

  // ==========================================================================
  // Batch Operations
  // ==========================================================================

  describe('Batch Operations', () => {
    beforeEach(async () => {
      // Create multiple entities
      for (let i = 1; i <= 5; i++) {
        const entity: Entity = {
          $id: createEntityId(`https://example.com/item/${i}`),
          $type: 'Item',
          $context: 'https://example.com/item',
          _namespace: 'https://example.com' as any,
          _localId: String(i),
          value: i * 10,
        };
        await api.createEntity(entity);
      }
    });

    describe('batchGet', () => {
      it('should get multiple entities in one call', async () => {
        const result = await api.batchGet([
          'https://example.com/item/1',
          'https://example.com/item/3',
          'https://example.com/item/5',
        ]);

        expect(result.successCount).toBe(3);
        expect(result.errorCount).toBe(0);
        expect(result.results.length).toBe(3);
        expect(result.results[0]?.value).toBe(10);
        expect(result.results[1]?.value).toBe(30);
        expect(result.results[2]?.value).toBe(50);
      });

      it('should return null for non-existent entities in batch', async () => {
        const result = await api.batchGet([
          'https://example.com/item/1',
          'https://example.com/item/nonexistent',
          'https://example.com/item/3',
        ]);

        expect(result.results.length).toBe(3);
        expect(result.results[0]).not.toBeNull();
        expect(result.results[1]).toBeNull();
        expect(result.results[2]).not.toBeNull();
      });
    });

    describe('batchCreate', () => {
      it('should create multiple entities in one call', async () => {
        const entities: Entity[] = [
          {
            $id: createEntityId('https://example.com/item/100'),
            $type: 'Item',
            $context: 'https://example.com/item',
            _namespace: 'https://example.com' as any,
            _localId: '100',
            value: 1000,
          },
          {
            $id: createEntityId('https://example.com/item/101'),
            $type: 'Item',
            $context: 'https://example.com/item',
            _namespace: 'https://example.com' as any,
            _localId: '101',
            value: 1010,
          },
        ];

        const result = await api.batchCreate(entities);

        expect(result.successCount).toBe(2);
        expect(result.errorCount).toBe(0);

        const item100 = await api.getEntity('https://example.com/item/100');
        const item101 = await api.getEntity('https://example.com/item/101');
        expect(item100?.value).toBe(1000);
        expect(item101?.value).toBe(1010);
      });

      it('should report errors for duplicate entities in batch', async () => {
        const entities: Entity[] = [
          {
            $id: createEntityId('https://example.com/item/1'), // Already exists
            $type: 'Item',
            $context: 'https://example.com/item',
            _namespace: 'https://example.com' as any,
            _localId: '1',
            value: 999,
          },
          {
            $id: createEntityId('https://example.com/item/200'),
            $type: 'Item',
            $context: 'https://example.com/item',
            _namespace: 'https://example.com' as any,
            _localId: '200',
            value: 2000,
          },
        ];

        const result = await api.batchCreate(entities);

        expect(result.errorCount).toBe(1);
        expect(result.successCount).toBe(1);
        expect(result.errors[0].index).toBe(0);
        expect(result.errors[0].error).toContain('already exists');
      });
    });

    describe('batchExecute', () => {
      it('should execute mixed operations in batch', async () => {
        const result = await api.batchExecute([
          { type: 'get', id: 'https://example.com/item/1' },
          {
            type: 'create',
            entity: {
              $id: createEntityId('https://example.com/item/300'),
              $type: 'Item',
              $context: 'https://example.com/item',
              _namespace: 'https://example.com' as any,
              _localId: '300',
              value: 3000,
            },
          },
          { type: 'update', id: 'https://example.com/item/2', props: { value: 999 } },
          { type: 'delete', id: 'https://example.com/item/5' },
          { type: 'get', id: 'https://example.com/item/2' },
        ]);

        expect(result.successCount).toBe(5);
        expect(result.errorCount).toBe(0);

        // Verify operations
        const item1 = result.results[0] as Entity;
        expect(item1?.value).toBe(10);

        const item300 = await api.getEntity('https://example.com/item/300');
        expect(item300?.value).toBe(3000);

        const item2 = result.results[4] as Entity;
        expect(item2?.value).toBe(999);

        const item5 = await api.getEntity('https://example.com/item/5');
        expect(item5).toBeNull();
      });

      it('should report errors for failed operations', async () => {
        const result = await api.batchExecute([
          { type: 'get', id: 'https://example.com/item/1' },
          { type: 'delete', id: 'https://example.com/item/nonexistent' },
          { type: 'update', id: 'https://example.com/item/nonexistent', props: { value: 1 } },
        ]);

        expect(result.successCount).toBe(1);
        expect(result.errorCount).toBe(2);
        expect(result.errors[0].index).toBe(1);
        expect(result.errors[1].index).toBe(2);
      });
    });
  });

  // ==========================================================================
  // Error Handling
  // ==========================================================================

  describe('Error Handling', () => {
    it('should handle missing required fields gracefully', async () => {
      const invalidEntity = {} as Entity;
      await expect(api.createEntity(invalidEntity)).rejects.toThrow();
    });

    it('should handle operations on non-existent entities', async () => {
      await expect(
        api.updateEntity('https://example.com/nonexistent', {})
      ).rejects.toThrow('not found');

      await expect(api.deleteEntity('https://example.com/nonexistent')).rejects.toThrow(
        'not found'
      );
    });

    it('should handle duplicate entity creation', async () => {
      const entity: Entity = {
        $id: createEntityId('https://example.com/dup/1'),
        $type: 'Test',
        $context: 'https://example.com/dup',
        _namespace: 'https://example.com' as any,
        _localId: '1',
      };

      await api.createEntity(entity);
      await expect(api.createEntity(entity)).rejects.toThrow('already exists');
    });
  });

  // ==========================================================================
  // Type Inference
  // ==========================================================================

  describe('Type Inference', () => {
    it('should infer REF type for URL-like strings', async () => {
      const entity: Entity = {
        $id: createEntityId('https://example.com/test/1'),
        $type: 'Test',
        $context: 'https://example.com/test',
        _namespace: 'https://example.com' as any,
        _localId: '1',
        reference: 'https://example.com/other/2',
      };

      await api.createEntity(entity);

      // The reference field should be stored as a REF type triple
      // and be traversable
      const other: Entity = {
        $id: createEntityId('https://example.com/other/2'),
        $type: 'Other',
        $context: 'https://example.com/other',
        _namespace: 'https://example.com' as any,
        _localId: '2',
        name: 'Other Entity',
      };
      await api.createEntity(other);

      const result = await api.traverse('https://example.com/test/1', 'reference');
      expect(result.length).toBe(1);
      expect(result[0].name).toBe('Other Entity');
    });

    it('should infer INT64 type for integer numbers', async () => {
      const entity: Entity = {
        $id: createEntityId('https://example.com/test/int'),
        $type: 'Test',
        $context: 'https://example.com/test',
        _namespace: 'https://example.com' as any,
        _localId: 'int',
        count: 42,
      };

      await api.createEntity(entity);
      const retrieved = await api.getEntity('https://example.com/test/int');
      expect(retrieved?.count).toBe(42);
    });

    it('should infer FLOAT64 type for decimal numbers', async () => {
      const entity: Entity = {
        $id: createEntityId('https://example.com/test/float'),
        $type: 'Test',
        $context: 'https://example.com/test',
        _namespace: 'https://example.com' as any,
        _localId: 'float',
        price: 19.99,
      };

      await api.createEntity(entity);
      const retrieved = await api.getEntity('https://example.com/test/float');
      expect(retrieved?.price).toBe(19.99);
    });

    it('should infer BOOL type for boolean values', async () => {
      const entity: Entity = {
        $id: createEntityId('https://example.com/test/bool'),
        $type: 'Test',
        $context: 'https://example.com/test',
        _namespace: 'https://example.com' as any,
        _localId: 'bool',
        active: true,
      };

      await api.createEntity(entity);
      const retrieved = await api.getEntity('https://example.com/test/bool');
      expect(retrieved?.active).toBe(true);
    });

    it('should infer GEO_POINT type for lat/lng objects', async () => {
      const entity: Entity = {
        $id: createEntityId('https://example.com/test/geo'),
        $type: 'Test',
        $context: 'https://example.com/test',
        _namespace: 'https://example.com' as any,
        _localId: 'geo',
        location: { lat: 37.7749, lng: -122.4194 },
      };

      await api.createEntity(entity);
      const retrieved = await api.getEntity('https://example.com/test/geo');
      expect(retrieved?.location).toEqual({ lat: 37.7749, lng: -122.4194 });
    });
  });

  // ==========================================================================
  // Batch Size Limits (DoS Prevention)
  // ==========================================================================

  describe('Batch Size Limits', () => {
    it('should export MAX_BATCH_SIZE constant', () => {
      expect(MAX_BATCH_SIZE).toBe(1000);
    });

    it('should export BatchSizeLimitError class', () => {
      const error = new BatchSizeLimitError(1500);
      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe('BatchSizeLimitError');
      expect(error.message).toContain('1500');
      expect(error.message).toContain('1000');
      expect(error.message).toContain('Split your request');
    });

    describe('batchGet', () => {
      it('should reject batch exceeding MAX_BATCH_SIZE', async () => {
        // Create an array with MAX_BATCH_SIZE + 1 IDs
        const oversizedIds = Array.from(
          { length: MAX_BATCH_SIZE + 1 },
          (_, i) => `https://example.com/item/${i}`
        );

        await expect(api.batchGet(oversizedIds)).rejects.toThrow(BatchSizeLimitError);
        await expect(api.batchGet(oversizedIds)).rejects.toThrow(/1001.*exceeds.*1000/);
      });

      it('should allow batch at exactly MAX_BATCH_SIZE', async () => {
        // Create an array with exactly MAX_BATCH_SIZE IDs
        const maxIds = Array.from(
          { length: MAX_BATCH_SIZE },
          (_, i) => `https://example.com/item/${i}`
        );

        // Should not throw - all items will be null since they don't exist
        const result = await api.batchGet(maxIds);
        expect(result.results.length).toBe(MAX_BATCH_SIZE);
      });

      it('should allow small batches', async () => {
        const smallIds = ['https://example.com/item/1', 'https://example.com/item/2'];
        const result = await api.batchGet(smallIds);
        expect(result.results.length).toBe(2);
      });
    });

    describe('batchCreate', () => {
      it('should reject batch exceeding MAX_BATCH_SIZE', async () => {
        // Create an array with MAX_BATCH_SIZE + 1 entities
        const oversizedEntities: Entity[] = Array.from({ length: MAX_BATCH_SIZE + 1 }, (_, i) => ({
          $id: createEntityId(`https://example.com/batch-item/${i}`),
          $type: 'Item',
          $context: 'https://example.com/batch-item',
          _namespace: 'https://example.com' as any,
          _localId: String(i),
          value: i,
        }));

        await expect(api.batchCreate(oversizedEntities)).rejects.toThrow(BatchSizeLimitError);
        await expect(api.batchCreate(oversizedEntities)).rejects.toThrow(/1001.*exceeds.*1000/);
      });

      it('should allow batch at exactly MAX_BATCH_SIZE', async () => {
        // Note: This test creates 1000 entities - we just verify it doesn't throw
        // We won't actually create them as that would be slow
        const maxEntities: Entity[] = Array.from({ length: MAX_BATCH_SIZE }, (_, i) => ({
          $id: createEntityId(`https://example.com/max-batch/${i}`),
          $type: 'Item',
          $context: 'https://example.com/max-batch',
          _namespace: 'https://example.com' as any,
          _localId: String(i),
          value: i,
        }));

        // Should not throw the batch size error
        // (may throw other errors during actual creation, but not BatchSizeLimitError)
        const result = await api.batchCreate(maxEntities);
        expect(result.successCount + result.errorCount).toBe(MAX_BATCH_SIZE);
      });

      it('should allow small batches', async () => {
        const smallEntities: Entity[] = [
          {
            $id: createEntityId('https://example.com/small-batch/1'),
            $type: 'Item',
            $context: 'https://example.com/small-batch',
            _namespace: 'https://example.com' as any,
            _localId: '1',
            value: 1,
          },
          {
            $id: createEntityId('https://example.com/small-batch/2'),
            $type: 'Item',
            $context: 'https://example.com/small-batch',
            _namespace: 'https://example.com' as any,
            _localId: '2',
            value: 2,
          },
        ];

        const result = await api.batchCreate(smallEntities);
        expect(result.successCount).toBe(2);
      });
    });

    describe('batchExecute', () => {
      it('should reject batch exceeding MAX_BATCH_SIZE', async () => {
        // Create an array with MAX_BATCH_SIZE + 1 operations
        const oversizedOperations = Array.from({ length: MAX_BATCH_SIZE + 1 }, (_, i) => ({
          type: 'get' as const,
          id: `https://example.com/item/${i}`,
        }));

        await expect(api.batchExecute(oversizedOperations)).rejects.toThrow(BatchSizeLimitError);
        await expect(api.batchExecute(oversizedOperations)).rejects.toThrow(/1001.*exceeds.*1000/);
      });

      it('should allow batch at exactly MAX_BATCH_SIZE', async () => {
        // Create an array with exactly MAX_BATCH_SIZE operations
        const maxOperations = Array.from({ length: MAX_BATCH_SIZE }, (_, i) => ({
          type: 'get' as const,
          id: `https://example.com/item/${i}`,
        }));

        // Should not throw - operations will return null for non-existent entities
        const result = await api.batchExecute(maxOperations);
        expect(result.results.length).toBe(MAX_BATCH_SIZE);
      });

      it('should allow small batches', async () => {
        const smallOperations = [
          { type: 'get' as const, id: 'https://example.com/item/1' },
          { type: 'get' as const, id: 'https://example.com/item/2' },
        ];
        const result = await api.batchExecute(smallOperations);
        expect(result.results.length).toBe(2);
      });
    });

    describe('error message quality', () => {
      it('should include actual size in error message', async () => {
        const size = 1500;
        const ids = Array.from({ length: size }, (_, i) => `https://example.com/item/${i}`);

        try {
          await api.batchGet(ids);
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BatchSizeLimitError);
          expect((e as Error).message).toContain('1500');
        }
      });

      it('should include max size in error message', async () => {
        const ids = Array.from({ length: 2000 }, (_, i) => `https://example.com/item/${i}`);

        try {
          await api.batchGet(ids);
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BatchSizeLimitError);
          expect((e as Error).message).toContain('1000');
        }
      });

      it('should provide helpful guidance in error message', async () => {
        const ids = Array.from({ length: 1001 }, (_, i) => `https://example.com/item/${i}`);

        try {
          await api.batchGet(ids);
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BatchSizeLimitError);
          expect((e as Error).message).toContain('Split your request');
        }
      });
    });
  });
});
