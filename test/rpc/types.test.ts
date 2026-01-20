import { describe, it, expect, expectTypeOf } from 'vitest';
import type {
  RpcEntity,
  Edge,
  TraversalResult,
  TraversalStats,
  TraversalApi,
  TraversalApiOptions,
  TraversalContext,
  RpcCallMessage,
  RpcMethodName,
} from '../../src/rpc/types';
import {
  validateRpcCall,
  isValidRpcMethod,
} from '../../src/rpc/types';
import type { EntityId, Namespace } from '../../src/core/types';

/**
 * Tests for RPC types module (src/rpc/types.ts)
 *
 * Since this module primarily exports TypeScript interfaces and types,
 * these tests verify:
 * 1. Type exports are correct and usable
 * 2. Interface structures match expected shapes
 * 3. Type compatibility with core types
 */

describe('RpcEntity interface', () => {
  it('should accept valid RpcEntity objects with required fields', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/users/123' as EntityId,
      $type: 'User',
      $context: 'https://example.com/users',
    };

    expect(entity.$id).toBe('https://example.com/users/123');
    expect(entity.$type).toBe('User');
    expect(entity.$context).toBe('https://example.com/users');
  });

  it('should accept RpcEntity with array of types', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/users/123' as EntityId,
      $type: ['User', 'Employee', 'Person'],
      $context: 'https://example.com/users',
    };

    expect(Array.isArray(entity.$type)).toBe(true);
    expect(entity.$type).toEqual(['User', 'Employee', 'Person']);
  });

  it('should accept RpcEntity with optional _namespace and _localId', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/users/123' as EntityId,
      $type: 'User',
      $context: 'https://example.com/users',
      _namespace: 'https://example.com/' as Namespace,
      _localId: 'users/123',
    };

    expect(entity._namespace).toBe('https://example.com/');
    expect(entity._localId).toBe('users/123');
  });

  it('should accept RpcEntity with arbitrary properties', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/users/123' as EntityId,
      $type: 'User',
      $context: 'https://example.com/users',
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      active: true,
      metadata: { createdAt: '2024-01-01' },
    };

    expect(entity.name).toBe('Alice');
    expect(entity.email).toBe('alice@example.com');
    expect(entity.age).toBe(30);
    expect(entity.active).toBe(true);
    expect(entity.metadata).toEqual({ createdAt: '2024-01-01' });
  });

  it('should have $id typed as EntityId', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/entity/1' as EntityId,
      $type: 'TestType',
      $context: 'https://example.com/entity',
    };

    // Type assertion check - $id should be compatible with EntityId
    const id: EntityId = entity.$id;
    expect(id).toBe('https://example.com/entity/1');
  });
});

describe('Edge interface', () => {
  it('should accept valid Edge objects with required fields', () => {
    const edge: Edge = {
      source: 'https://example.com/users/123',
      predicate: 'follows',
      target: 'https://example.com/users/456',
    };

    expect(edge.source).toBe('https://example.com/users/123');
    expect(edge.predicate).toBe('follows');
    expect(edge.target).toBe('https://example.com/users/456');
  });

  it('should accept Edge with optional weight', () => {
    const edge: Edge = {
      source: 'https://example.com/users/123',
      predicate: 'follows',
      target: 'https://example.com/users/456',
      weight: 0.85,
    };

    expect(edge.weight).toBe(0.85);
  });

  it('should accept Edge with optional metadata', () => {
    const edge: Edge = {
      source: 'https://example.com/users/123',
      predicate: 'follows',
      target: 'https://example.com/users/456',
      metadata: {
        since: '2024-01-01',
        mutualFriends: 5,
        verified: true,
      },
    };

    expect(edge.metadata).toEqual({
      since: '2024-01-01',
      mutualFriends: 5,
      verified: true,
    });
  });

  it('should accept Edge with optional timestamp', () => {
    const timestamp = Date.now();
    const edge: Edge = {
      source: 'https://example.com/users/123',
      predicate: 'follows',
      target: 'https://example.com/users/456',
      timestamp,
    };

    expect(edge.timestamp).toBe(timestamp);
  });

  it('should accept Edge with optional txId', () => {
    const edge: Edge = {
      source: 'https://example.com/users/123',
      predicate: 'follows',
      target: 'https://example.com/users/456',
      txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
    };

    expect(edge.txId).toBe('01ARZ3NDEKTSV4RRFFQ69G5FAV');
  });

  it('should accept Edge with all optional fields', () => {
    const edge: Edge = {
      source: 'https://example.com/users/123',
      predicate: 'follows',
      target: 'https://example.com/users/456',
      weight: 1.0,
      metadata: { category: 'social' },
      timestamp: 1704067200000,
      txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
    };

    expect(edge.source).toBeDefined();
    expect(edge.predicate).toBeDefined();
    expect(edge.target).toBeDefined();
    expect(edge.weight).toBe(1.0);
    expect(edge.metadata).toBeDefined();
    expect(edge.timestamp).toBeDefined();
    expect(edge.txId).toBeDefined();
  });
});

describe('TraversalStats interface', () => {
  it('should accept valid TraversalStats with required fields', () => {
    const stats: TraversalStats = {
      nodesVisited: 100,
      edgesTraversed: 250,
      durationMs: 15.4,
      shardQueries: 3,
    };

    expect(stats.nodesVisited).toBe(100);
    expect(stats.edgesTraversed).toBe(250);
    expect(stats.durationMs).toBe(15.4);
    expect(stats.shardQueries).toBe(3);
  });

  it('should accept TraversalStats with optional cache fields', () => {
    const stats: TraversalStats = {
      nodesVisited: 100,
      edgesTraversed: 250,
      durationMs: 15.4,
      shardQueries: 3,
      cacheHits: 5,
      cacheMisses: 2,
    };

    expect(stats.cacheHits).toBe(5);
    expect(stats.cacheMisses).toBe(2);
  });

  it('should accept TraversalStats with optional r2LatencyMs', () => {
    const stats: TraversalStats = {
      nodesVisited: 100,
      edgesTraversed: 250,
      durationMs: 15.4,
      shardQueries: 3,
      r2LatencyMs: 45.2,
    };

    expect(stats.r2LatencyMs).toBe(45.2);
  });

  it('should accept TraversalStats with all optional fields', () => {
    const stats: TraversalStats = {
      nodesVisited: 500,
      edgesTraversed: 1200,
      durationMs: 89.5,
      shardQueries: 10,
      cacheHits: 8,
      cacheMisses: 2,
      r2LatencyMs: 120.3,
    };

    expect(stats.nodesVisited).toBe(500);
    expect(stats.edgesTraversed).toBe(1200);
    expect(stats.durationMs).toBe(89.5);
    expect(stats.shardQueries).toBe(10);
    expect(stats.cacheHits).toBe(8);
    expect(stats.cacheMisses).toBe(2);
    expect(stats.r2LatencyMs).toBe(120.3);
  });
});

describe('TraversalResult interface', () => {
  it('should accept valid TraversalResult with required fields', () => {
    const result: TraversalResult = {
      entities: [
        'https://example.com/users/456',
        'https://example.com/users/789',
      ],
      stats: {
        nodesVisited: 10,
        edgesTraversed: 25,
        durationMs: 5.2,
        shardQueries: 1,
      },
    };

    expect(result.entities).toHaveLength(2);
    expect(result.stats.nodesVisited).toBe(10);
  });

  it('should accept TraversalResult with empty entities array', () => {
    const result: TraversalResult = {
      entities: [],
      stats: {
        nodesVisited: 0,
        edgesTraversed: 0,
        durationMs: 0.5,
        shardQueries: 1,
      },
    };

    expect(result.entities).toHaveLength(0);
  });

  it('should accept TraversalResult with optional edges array', () => {
    const result: TraversalResult = {
      entities: ['https://example.com/users/456'],
      edges: [
        {
          source: 'https://example.com/users/123',
          predicate: 'follows',
          target: 'https://example.com/users/456',
        },
      ],
      stats: {
        nodesVisited: 2,
        edgesTraversed: 1,
        durationMs: 2.1,
        shardQueries: 1,
      },
    };

    expect(result.edges).toBeDefined();
    expect(result.edges).toHaveLength(1);
    expect(result.edges![0].predicate).toBe('follows');
  });

  it('should accept TraversalResult with optional cursor', () => {
    const result: TraversalResult = {
      entities: ['https://example.com/users/456'],
      stats: {
        nodesVisited: 10,
        edgesTraversed: 25,
        durationMs: 5.2,
        shardQueries: 1,
      },
      cursor: 'eyJvZmZzZXQiOjEwMH0=',
    };

    expect(result.cursor).toBe('eyJvZmZzZXQiOjEwMH0=');
  });

  it('should accept TraversalResult with optional hasMore', () => {
    const result: TraversalResult = {
      entities: ['https://example.com/users/456'],
      stats: {
        nodesVisited: 10,
        edgesTraversed: 25,
        durationMs: 5.2,
        shardQueries: 1,
      },
      hasMore: true,
    };

    expect(result.hasMore).toBe(true);
  });

  it('should accept TraversalResult with pagination fields for paginated queries', () => {
    const result: TraversalResult = {
      entities: Array.from({ length: 100 }, (_, i) => `https://example.com/users/${i}`),
      stats: {
        nodesVisited: 100,
        edgesTraversed: 250,
        durationMs: 45.3,
        shardQueries: 5,
      },
      cursor: 'next_page_token',
      hasMore: true,
    };

    expect(result.entities).toHaveLength(100);
    expect(result.cursor).toBe('next_page_token');
    expect(result.hasMore).toBe(true);
  });
});

describe('TraversalApiOptions interface', () => {
  it('should accept valid TraversalApiOptions with required colo', () => {
    const options: TraversalApiOptions = {
      colo: 'SJC',
    };

    expect(options.colo).toBe('SJC');
  });

  it('should accept TraversalApiOptions with optional measureR2Latency function', () => {
    const options: TraversalApiOptions = {
      colo: 'IAD',
      measureR2Latency: async () => 25.5,
    };

    expect(options.colo).toBe('IAD');
    expect(options.measureR2Latency).toBeDefined();
    expect(typeof options.measureR2Latency).toBe('function');
  });

  it('should accept TraversalApiOptions with optional getShardStub function', () => {
    const mockStub = {} as DurableObjectStub;
    const options: TraversalApiOptions = {
      colo: 'AMS',
      getShardStub: (_shardId: string) => mockStub,
    };

    expect(options.getShardStub).toBeDefined();
    expect(typeof options.getShardStub).toBe('function');
  });

  it('should accept TraversalApiOptions with optional r2Bucket', () => {
    const mockBucket = {} as R2Bucket;
    const options: TraversalApiOptions = {
      colo: 'SYD',
      r2Bucket: mockBucket,
    };

    expect(options.r2Bucket).toBeDefined();
  });

  it('should accept TraversalApiOptions with all optional fields', () => {
    const mockStub = {} as DurableObjectStub;
    const mockBucket = {} as R2Bucket;
    const options: TraversalApiOptions = {
      colo: 'NRT',
      measureR2Latency: async () => 30.0,
      getShardStub: (_shardId: string) => mockStub,
      r2Bucket: mockBucket,
    };

    expect(options.colo).toBe('NRT');
    expect(options.measureR2Latency).toBeDefined();
    expect(options.getShardStub).toBeDefined();
    expect(options.r2Bucket).toBeDefined();
  });
});

describe('TraversalContext interface', () => {
  it('should accept valid TraversalContext with required fields', () => {
    const context: TraversalContext = {
      timestamp: Date.now(),
      requestId: 'req-123-abc',
    };

    expect(context.timestamp).toBeGreaterThan(0);
    expect(context.requestId).toBe('req-123-abc');
  });

  it('should accept TraversalContext with optional originColo', () => {
    const context: TraversalContext = {
      timestamp: 1704067200000,
      requestId: 'req-456-def',
      originColo: 'SJC',
    };

    expect(context.originColo).toBe('SJC');
  });

  it('should accept TraversalContext for cross-colo requests', () => {
    const context: TraversalContext = {
      timestamp: Date.now(),
      requestId: 'cross-colo-request-789',
      originColo: 'IAD', // Request originated from IAD
    };

    expect(context.originColo).toBe('IAD');
    expect(context.requestId).toContain('cross-colo');
  });
});

describe('TraversalApi interface structure', () => {
  it('should have getColo method returning string', () => {
    // Type-level test using a mock implementation
    const mockApi: TraversalApi = {
      getColo: () => 'SJC',
      getR2Latency: async () => 25.0,
      lookup: async (_entityId: string) => null,
      batchLookup: async (_entityIds: string[]) => [],
      traverse: async (_startId: string, _depth: number) => [],
      traverseWithStats: async (_startId: string, _depth: number) => ({
        entities: [],
        stats: {
          nodesVisited: 0,
          edgesTraversed: 0,
          durationMs: 0,
          shardQueries: 0,
        },
      }),
    };

    const colo = mockApi.getColo();
    expect(typeof colo).toBe('string');
    expect(colo).toBe('SJC');
  });

  it('should have getR2Latency method returning Promise<number>', async () => {
    const mockApi: TraversalApi = {
      getColo: () => 'SJC',
      getR2Latency: async () => 45.5,
      lookup: async () => null,
      batchLookup: async () => [],
      traverse: async () => [],
      traverseWithStats: async () => ({
        entities: [],
        stats: { nodesVisited: 0, edgesTraversed: 0, durationMs: 0, shardQueries: 0 },
      }),
    };

    const latency = await mockApi.getR2Latency();
    expect(typeof latency).toBe('number');
    expect(latency).toBe(45.5);
  });

  it('should have lookup method returning Promise<RpcEntity | null>', async () => {
    const testEntity: RpcEntity = {
      $id: 'https://example.com/users/123' as EntityId,
      $type: 'User',
      $context: 'https://example.com/users',
      name: 'Alice',
    };

    const mockApi: TraversalApi = {
      getColo: () => 'SJC',
      getR2Latency: async () => 25.0,
      lookup: async (entityId: string) => {
        if (entityId === 'https://example.com/users/123') {
          return testEntity;
        }
        return null;
      },
      batchLookup: async () => [],
      traverse: async () => [],
      traverseWithStats: async () => ({
        entities: [],
        stats: { nodesVisited: 0, edgesTraversed: 0, durationMs: 0, shardQueries: 0 },
      }),
    };

    const found = await mockApi.lookup('https://example.com/users/123');
    expect(found).not.toBeNull();
    expect(found!.$id).toBe('https://example.com/users/123');
    expect(found!.name).toBe('Alice');

    const notFound = await mockApi.lookup('https://example.com/users/999');
    expect(notFound).toBeNull();
  });

  it('should have batchLookup method returning Promise<(RpcEntity | null)[]>', async () => {
    const testEntities: RpcEntity[] = [
      {
        $id: 'https://example.com/users/1' as EntityId,
        $type: 'User',
        $context: 'https://example.com/users',
      },
      {
        $id: 'https://example.com/users/2' as EntityId,
        $type: 'User',
        $context: 'https://example.com/users',
      },
    ];

    const mockApi: TraversalApi = {
      getColo: () => 'SJC',
      getR2Latency: async () => 25.0,
      lookup: async () => null,
      batchLookup: async (entityIds: string[]) => {
        return entityIds.map((id) => {
          const found = testEntities.find((e) => e.$id === id);
          return found || null;
        });
      },
      traverse: async () => [],
      traverseWithStats: async () => ({
        entities: [],
        stats: { nodesVisited: 0, edgesTraversed: 0, durationMs: 0, shardQueries: 0 },
      }),
    };

    const results = await mockApi.batchLookup([
      'https://example.com/users/1',
      'https://example.com/users/999',
      'https://example.com/users/2',
    ]);

    expect(results).toHaveLength(3);
    expect(results[0]).not.toBeNull();
    expect(results[0]!.$id).toBe('https://example.com/users/1');
    expect(results[1]).toBeNull(); // Not found
    expect(results[2]).not.toBeNull();
    expect(results[2]!.$id).toBe('https://example.com/users/2');
  });

  it('should have traverse method returning Promise<string[]>', async () => {
    const mockApi: TraversalApi = {
      getColo: () => 'SJC',
      getR2Latency: async () => 25.0,
      lookup: async () => null,
      batchLookup: async () => [],
      traverse: async (startId: string, depth: number) => {
        // Mock traversal - returns neighbors up to depth
        if (depth === 0) return [];
        return [
          'https://example.com/users/2',
          'https://example.com/users/3',
        ];
      },
      traverseWithStats: async () => ({
        entities: [],
        stats: { nodesVisited: 0, edgesTraversed: 0, durationMs: 0, shardQueries: 0 },
      }),
    };

    const neighbors = await mockApi.traverse('https://example.com/users/1', 1);
    expect(Array.isArray(neighbors)).toBe(true);
    expect(neighbors).toHaveLength(2);
    expect(neighbors[0]).toBe('https://example.com/users/2');
  });

  it('should have traverseWithStats method returning Promise<TraversalResult>', async () => {
    const mockApi: TraversalApi = {
      getColo: () => 'SJC',
      getR2Latency: async () => 25.0,
      lookup: async () => null,
      batchLookup: async () => [],
      traverse: async () => [],
      traverseWithStats: async (startId: string, depth: number) => {
        return {
          entities: ['https://example.com/users/2', 'https://example.com/users/3'],
          edges: [
            {
              source: startId,
              predicate: 'follows',
              target: 'https://example.com/users/2',
            },
            {
              source: startId,
              predicate: 'follows',
              target: 'https://example.com/users/3',
            },
          ],
          stats: {
            nodesVisited: 3,
            edgesTraversed: 2,
            durationMs: 12.5,
            shardQueries: 1,
            cacheHits: 1,
            cacheMisses: 0,
          },
        };
      },
    };

    const result = await mockApi.traverseWithStats('https://example.com/users/1', 1);

    expect(result.entities).toHaveLength(2);
    expect(result.edges).toHaveLength(2);
    expect(result.stats.nodesVisited).toBe(3);
    expect(result.stats.edgesTraversed).toBe(2);
    expect(result.stats.durationMs).toBe(12.5);
    expect(result.stats.cacheHits).toBe(1);
  });
});

describe('Type compatibility with core types', () => {
  it('should use EntityId type for RpcEntity.$id', () => {
    // This test verifies at compile time that EntityId is properly used
    const entity: RpcEntity = {
      $id: 'https://example.com/entity/1' as EntityId,
      $type: 'Test',
      $context: 'https://example.com/entity',
    };

    // The $id should be assignable to EntityId
    const entityId: EntityId = entity.$id;
    expect(entityId).toBe('https://example.com/entity/1');
  });

  it('should use Namespace type for RpcEntity._namespace', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/entity/1' as EntityId,
      $type: 'Test',
      $context: 'https://example.com/entity',
      _namespace: 'https://example.com/' as Namespace,
    };

    // The _namespace should be assignable to Namespace
    const namespace: Namespace | undefined = entity._namespace;
    expect(namespace).toBe('https://example.com/');
  });
});

// ============================================================================
// RPC Parameter Validation Tests
// ============================================================================

describe('isValidRpcMethod', () => {
  it('should return true for valid method names', () => {
    const validMethods: RpcMethodName[] = [
      'getEntity',
      'createEntity',
      'updateEntity',
      'deleteEntity',
      'traverse',
      'reverseTraverse',
      'pathTraverse',
      'query',
      'batchGet',
      'batchCreate',
      'batchExecute',
    ];

    for (const method of validMethods) {
      expect(isValidRpcMethod(method)).toBe(true);
    }
  });

  it('should return false for invalid method names', () => {
    expect(isValidRpcMethod('invalidMethod')).toBe(false);
    expect(isValidRpcMethod('')).toBe(false);
    expect(isValidRpcMethod('GET_ENTITY')).toBe(false);
    expect(isValidRpcMethod('getentity')).toBe(false);
  });
});

describe('validateRpcCall', () => {
  describe('getEntity', () => {
    it('should validate valid getEntity call', () => {
      const call: RpcCallMessage = {
        method: 'getEntity',
        args: ['https://example.com/entity/1'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params).toBeDefined();
      expect(result.params?.method).toBe('getEntity');
      expect(result.params?.args).toEqual(['https://example.com/entity/1']);
    });

    it('should reject getEntity with missing id', () => {
      const call: RpcCallMessage = {
        method: 'getEntity',
        args: [],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
      expect(result.error).toContain('non-empty string id');
    });

    it('should reject getEntity with empty string id', () => {
      const call: RpcCallMessage = {
        method: 'getEntity',
        args: [''],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
    });
  });

  describe('createEntity', () => {
    it('should validate valid createEntity call', () => {
      const call: RpcCallMessage = {
        method: 'createEntity',
        args: [{ $id: 'https://example.com/entity/1', $type: 'TestEntity', name: 'Test' }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('createEntity');
    });

    it('should reject createEntity without $id', () => {
      const call: RpcCallMessage = {
        method: 'createEntity',
        args: [{ $type: 'TestEntity' }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
      expect(result.error).toContain('valid entity');
    });

    it('should reject createEntity without $type', () => {
      const call: RpcCallMessage = {
        method: 'createEntity',
        args: [{ $id: 'https://example.com/entity/1' }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
    });
  });

  describe('updateEntity', () => {
    it('should validate valid updateEntity call', () => {
      const call: RpcCallMessage = {
        method: 'updateEntity',
        args: ['https://example.com/entity/1', { name: 'Updated' }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('updateEntity');
    });

    it('should reject updateEntity with missing props', () => {
      const call: RpcCallMessage = {
        method: 'updateEntity',
        args: ['https://example.com/entity/1'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
    });
  });

  describe('deleteEntity', () => {
    it('should validate valid deleteEntity call', () => {
      const call: RpcCallMessage = {
        method: 'deleteEntity',
        args: ['https://example.com/entity/1'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('deleteEntity');
    });
  });

  describe('traverse', () => {
    it('should validate valid traverse call', () => {
      const call: RpcCallMessage = {
        method: 'traverse',
        args: ['https://example.com/entity/1', 'friends'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('traverse');
    });

    it('should validate traverse with options', () => {
      const call: RpcCallMessage = {
        method: 'traverse',
        args: ['https://example.com/entity/1', 'friends', { limit: 10, maxDepth: 3 }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
    });

    it('should reject traverse with invalid options', () => {
      const call: RpcCallMessage = {
        method: 'traverse',
        args: ['https://example.com/entity/1', 'friends', { limit: 'invalid' }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
      expect(result.error).toContain('TraversalOptions');
    });
  });

  describe('reverseTraverse', () => {
    it('should validate valid reverseTraverse call', () => {
      const call: RpcCallMessage = {
        method: 'reverseTraverse',
        args: ['https://example.com/entity/1', 'friends'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('reverseTraverse');
    });
  });

  describe('pathTraverse', () => {
    it('should validate valid pathTraverse call', () => {
      const call: RpcCallMessage = {
        method: 'pathTraverse',
        args: ['https://example.com/entity/1', ['friends', 'posts']],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('pathTraverse');
    });

    it('should reject pathTraverse with non-string path elements', () => {
      const call: RpcCallMessage = {
        method: 'pathTraverse',
        args: ['https://example.com/entity/1', ['friends', 123]],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
      expect(result.error).toContain('path: string[]');
    });
  });

  describe('query', () => {
    it('should validate valid query call', () => {
      const call: RpcCallMessage = {
        method: 'query',
        args: ['user:1.friends.posts'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('query');
    });

    it('should validate query with options', () => {
      const call: RpcCallMessage = {
        method: 'query',
        args: ['user:1.friends', { limit: 50, cursor: 'abc123' }],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
    });
  });

  describe('batch operations', () => {
    it('should validate batchGet', () => {
      const call: RpcCallMessage = {
        method: 'batchGet',
        args: [['https://example.com/1', 'https://example.com/2']],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('batchGet');
    });

    it('should reject batchGet with non-string ids', () => {
      const call: RpcCallMessage = {
        method: 'batchGet',
        args: [['https://example.com/1', 123]],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
    });

    it('should validate batchCreate', () => {
      const call: RpcCallMessage = {
        method: 'batchCreate',
        args: [[
          { $id: 'https://example.com/1', $type: 'Test' },
          { $id: 'https://example.com/2', $type: 'Test' },
        ]],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('batchCreate');
    });

    it('should validate batchExecute', () => {
      const call: RpcCallMessage = {
        method: 'batchExecute',
        args: [[
          { type: 'get', id: 'https://example.com/1' },
          { type: 'create', entity: { $id: 'https://example.com/2', $type: 'Test' } },
          { type: 'update', id: 'https://example.com/1', props: { name: 'Updated' } },
          { type: 'delete', id: 'https://example.com/3' },
        ]],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
      expect(result.params?.method).toBe('batchExecute');
    });

    it('should reject batchExecute with invalid operation type', () => {
      const call: RpcCallMessage = {
        method: 'batchExecute',
        args: [[{ type: 'invalid', id: 'https://example.com/1' }]],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
    });
  });

  describe('error cases', () => {
    it('should reject unknown method', () => {
      const call: RpcCallMessage = {
        method: 'unknownMethod',
        args: [],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
      expect(result.error).toContain('Unknown RPC method');
    });

    it('should reject empty method name', () => {
      const call: RpcCallMessage = {
        method: '',
        args: [],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(false);
      expect(result.error).toContain('non-empty string');
    });

    it('should handle call with id field', () => {
      const call: RpcCallMessage = {
        id: 'req-123',
        method: 'getEntity',
        args: ['https://example.com/entity/1'],
      };

      const result = validateRpcCall(call);

      expect(result.valid).toBe(true);
    });

    it('should handle call without args (defaults to empty array)', () => {
      const call: RpcCallMessage = {
        method: 'getEntity',
      };

      const result = validateRpcCall(call);

      // Should fail because getEntity requires an id
      expect(result.valid).toBe(false);
    });
  });
});

describe('Edge cases and boundary conditions', () => {
  it('should handle RpcEntity with empty string values', () => {
    const entity: RpcEntity = {
      $id: 'https://example.com/entity/1' as EntityId,
      $type: '',
      $context: '',
    };

    expect(entity.$type).toBe('');
    expect(entity.$context).toBe('');
  });

  it('should handle Edge with zero weight', () => {
    const edge: Edge = {
      source: 'https://example.com/a',
      predicate: 'linked',
      target: 'https://example.com/b',
      weight: 0,
    };

    expect(edge.weight).toBe(0);
  });

  it('should handle TraversalStats with zero values', () => {
    const stats: TraversalStats = {
      nodesVisited: 0,
      edgesTraversed: 0,
      durationMs: 0,
      shardQueries: 0,
      cacheHits: 0,
      cacheMisses: 0,
      r2LatencyMs: 0,
    };

    expect(stats.nodesVisited).toBe(0);
    expect(stats.durationMs).toBe(0);
  });

  it('should handle TraversalResult with large entity counts', () => {
    const largeEntityArray = Array.from(
      { length: 10000 },
      (_, i) => `https://example.com/entity/${i}`
    );

    const result: TraversalResult = {
      entities: largeEntityArray,
      stats: {
        nodesVisited: 10000,
        edgesTraversed: 50000,
        durationMs: 1500.5,
        shardQueries: 100,
      },
    };

    expect(result.entities).toHaveLength(10000);
    expect(result.stats.nodesVisited).toBe(10000);
  });

  it('should handle Edge with complex metadata', () => {
    const edge: Edge = {
      source: 'https://example.com/a',
      predicate: 'related',
      target: 'https://example.com/b',
      metadata: {
        nested: {
          deeply: {
            value: 42,
          },
        },
        array: [1, 2, 3],
        nullValue: null,
        undefinedValue: undefined,
      },
    };

    expect(edge.metadata!.nested).toBeDefined();
    expect(edge.metadata!.array).toEqual([1, 2, 3]);
  });
});
