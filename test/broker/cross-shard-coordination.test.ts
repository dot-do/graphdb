/**
 * Cross-Shard Coordination Tests
 *
 * TDD RED phase - tests for coordinating queries across multiple shards.
 * These tests define expected behavior for:
 * - Fan-out queries to multiple shards
 * - Result merging strategies (union, intersection, ordering)
 * - Deduplication across shard boundaries
 * - Consistency models (read-your-writes, eventual)
 * - Scatter-gather patterns
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  planQuery,
  orchestrateQuery,
  batchLookups,
  type QueryPlan,
  type QueryStep,
} from '../../src/broker/orchestrator';
import { createEntityId } from '../../src/core/types';

// Helper to create mock shard stubs with specific data
function createShardStub(data: Array<{ $id: string; $type: string; $context: string; [key: string]: unknown }>): DurableObjectStub {
  return {
    fetch: vi.fn(async (request: Request) => {
      const url = new URL(request.url);

      // Handle lookup requests
      if (url.pathname === '/lookup') {
        const idsParam = url.searchParams.get('ids') ?? '';
        const requestedIds = idsParam.split(',').filter(Boolean);

        const results = data.filter(item =>
          requestedIds.length === 0 || requestedIds.includes(item.$id)
        );

        return new Response(JSON.stringify(results), { status: 200 });
      }

      // Handle traverse requests
      if (url.pathname === '/traverse') {
        const fromId = url.searchParams.get('from');
        const predicate = url.searchParams.get('predicate');

        const source = data.find(item => item.$id === fromId);
        if (!source) {
          return new Response(JSON.stringify([]), { status: 200 });
        }

        const targetIds = source[predicate as string];
        if (!targetIds) {
          return new Response(JSON.stringify([]), { status: 200 });
        }

        const targets = Array.isArray(targetIds)
          ? data.filter(item => targetIds.includes(item.$id))
          : data.filter(item => item.$id === targetIds);

        return new Response(JSON.stringify(targets), { status: 200 });
      }

      // Handle filter requests
      if (url.pathname === '/filter') {
        const field = url.searchParams.get('field');
        const op = url.searchParams.get('op');
        const value = url.searchParams.get('value');

        const numValue = Number(value);
        const results = data.filter(item => {
          const fieldValue = item[field as string];
          switch (op) {
            case '>': return fieldValue > numValue;
            case '<': return fieldValue < numValue;
            case '>=': return fieldValue >= numValue;
            case '<=': return fieldValue <= numValue;
            case '=': return fieldValue === value || fieldValue === numValue;
            case '!=': return fieldValue !== value && fieldValue !== numValue;
            default: return true;
          }
        });

        return new Response(JSON.stringify(results), { status: 200 });
      }

      return new Response(JSON.stringify([]), { status: 200 });
    }),
  } as unknown as DurableObjectStub;
}

describe('Fan-Out Queries', () => {
  describe('Parallel shard execution', () => {
    it('should execute lookups to multiple shards in parallel', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string }>> = {
        'shard-0': [{ $id: 'https://example.com/user/1', $type: 'User', $context: 'test' }],
        'shard-1': [{ $id: 'https://example.com/user/2', $type: 'User', $context: 'test' }],
        'shard-2': [{ $id: 'https://example.com/user/3', $type: 'User', $context: 'test' }],
      };

      const callOrder: string[] = [];
      const callTimes: Record<string, number> = {};

      const getStub = (shardId: string) => {
        const stub = createShardStub(shardData[shardId] ?? []);
        const originalFetch = stub.fetch as ReturnType<typeof vi.fn>;

        stub.fetch = vi.fn(async (request: Request) => {
          callOrder.push(shardId);
          callTimes[shardId] = Date.now();
          // Simulate varying response times
          await new Promise(resolve => setTimeout(resolve, Math.random() * 10));
          return originalFetch(request);
        });

        return stub;
      };

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0', entityIds: [createEntityId('https://example.com/user/1')] },
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/user/2')] },
          { type: 'lookup', shardId: 'shard-2', entityIds: [createEntityId('https://example.com/user/3')] },
        ],
        estimatedCost: 3,
        canBatch: true,
      };

      const result = await orchestrateQuery(plan, getStub, { parallel: true });

      // All three shards should have been called
      expect(callOrder).toContain('shard-0');
      expect(callOrder).toContain('shard-1');
      expect(callOrder).toContain('shard-2');

      // All three entities should be returned
      expect(result.entities).toHaveLength(3);

      // Calls should have started at roughly the same time (within 50ms)
      const times = Object.values(callTimes);
      const minTime = Math.min(...times);
      const maxTime = Math.max(...times);
      expect(maxTime - minTime).toBeLessThan(50);
    });

    it('should limit concurrent shard requests', async () => {
      const shardCount = 20;
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      const getStub = () => {
        return {
          fetch: vi.fn(async () => {
            currentConcurrent++;
            maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
            await new Promise(resolve => setTimeout(resolve, 10));
            currentConcurrent--;
            return new Response(JSON.stringify([]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const steps: QueryStep[] = Array.from({ length: shardCount }, (_, i) => ({
        type: 'lookup' as const,
        shardId: `shard-${i}`,
        entityIds: [createEntityId(`https://example.com/${i}`)],
      }));

      const plan: QueryPlan = {
        steps,
        estimatedCost: shardCount,
        canBatch: true,
      };

      await orchestrateQuery(plan, getStub, { parallel: true, maxConcurrency: 5 });

      // Should not exceed max concurrency limit
      expect(maxConcurrent).toBeLessThanOrEqual(5);
    });

    it('should preserve result order when executing in parallel', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string; sortOrder: number }>> = {
        'shard-0': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', sortOrder: 1 }],
        'shard-1': [{ $id: 'https://example.com/2', $type: 'Entity', $context: 'test', sortOrder: 2 }],
        'shard-2': [{ $id: 'https://example.com/3', $type: 'Entity', $context: 'test', sortOrder: 3 }],
      };

      const getStub = (shardId: string) => {
        // Add varying delays to test order preservation
        const delay = shardId === 'shard-0' ? 50 : shardId === 'shard-1' ? 10 : 30;

        return {
          fetch: vi.fn(async () => {
            await new Promise(resolve => setTimeout(resolve, delay));
            return new Response(JSON.stringify(shardData[shardId] ?? []), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/2')] },
          { type: 'lookup', shardId: 'shard-2', entityIds: [createEntityId('https://example.com/3')] },
        ],
        estimatedCost: 3,
        canBatch: true,
      };

      const result = await orchestrateQuery(plan, getStub, { parallel: true, preserveOrder: true });

      // Results should be in the order of the steps, not completion order
      expect(result.entities[0].$id).toBe('https://example.com/1');
      expect(result.entities[1].$id).toBe('https://example.com/2');
      expect(result.entities[2].$id).toBe('https://example.com/3');
    });
  });
});

describe('Result Merging', () => {
  describe('Union merge strategy', () => {
    it('should combine results from multiple shards', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string }>> = {
        'shard-0': [
          { $id: 'https://example.com/user/1', $type: 'User', $context: 'test' },
          { $id: 'https://example.com/user/2', $type: 'User', $context: 'test' },
        ],
        'shard-1': [
          { $id: 'https://example.com/user/3', $type: 'User', $context: 'test' },
          { $id: 'https://example.com/user/4', $type: 'User', $context: 'test' },
        ],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0' },
          { type: 'lookup', shardId: 'shard-1' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, { mergeStrategy: 'union' });

      expect(result.entities).toHaveLength(4);
      const ids = result.entities.map(e => e.$id);
      expect(ids).toContain('https://example.com/user/1');
      expect(ids).toContain('https://example.com/user/2');
      expect(ids).toContain('https://example.com/user/3');
      expect(ids).toContain('https://example.com/user/4');
    });
  });

  describe('Intersection merge strategy', () => {
    it('should return only entities present in all shards', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string }>> = {
        'shard-0': [
          { $id: 'https://example.com/common', $type: 'Entity', $context: 'test' },
          { $id: 'https://example.com/only-in-0', $type: 'Entity', $context: 'test' },
        ],
        'shard-1': [
          { $id: 'https://example.com/common', $type: 'Entity', $context: 'test' },
          { $id: 'https://example.com/only-in-1', $type: 'Entity', $context: 'test' },
        ],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0' },
          { type: 'lookup', shardId: 'shard-1' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, { mergeStrategy: 'intersection' });

      expect(result.entities).toHaveLength(1);
      expect(result.entities[0].$id).toBe('https://example.com/common');
    });
  });

  describe('Ordered merge strategy', () => {
    it('should merge results maintaining global sort order', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string; score: number }>> = {
        'shard-0': [
          { $id: 'https://example.com/1', $type: 'Entity', $context: 'test', score: 90 },
          { $id: 'https://example.com/3', $type: 'Entity', $context: 'test', score: 70 },
        ],
        'shard-1': [
          { $id: 'https://example.com/2', $type: 'Entity', $context: 'test', score: 85 },
          { $id: 'https://example.com/4', $type: 'Entity', $context: 'test', score: 60 },
        ],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0' },
          { type: 'lookup', shardId: 'shard-1' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, {
        mergeStrategy: 'ordered',
        orderBy: 'score',
        orderDirection: 'desc',
      });

      const scores = result.entities.map(e => (e as unknown as { score: number }).score);
      expect(scores).toEqual([90, 85, 70, 60]);
    });

    it('should handle ascending sort order', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string; timestamp: number }>> = {
        'shard-0': [
          { $id: 'https://example.com/1', $type: 'Entity', $context: 'test', timestamp: 100 },
          { $id: 'https://example.com/3', $type: 'Entity', $context: 'test', timestamp: 300 },
        ],
        'shard-1': [
          { $id: 'https://example.com/2', $type: 'Entity', $context: 'test', timestamp: 200 },
          { $id: 'https://example.com/4', $type: 'Entity', $context: 'test', timestamp: 400 },
        ],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0' },
          { type: 'lookup', shardId: 'shard-1' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, {
        mergeStrategy: 'ordered',
        orderBy: 'timestamp',
        orderDirection: 'asc',
      });

      const timestamps = result.entities.map(e => (e as unknown as { timestamp: number }).timestamp);
      expect(timestamps).toEqual([100, 200, 300, 400]);
    });
  });
});

describe('Deduplication', () => {
  describe('Cross-shard deduplication', () => {
    it('should deduplicate entities by $id across shards', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string }>> = {
        'shard-0': [
          { $id: 'https://example.com/user/1', $type: 'User', $context: 'test' },
          { $id: 'https://example.com/shared', $type: 'Entity', $context: 'test' },
        ],
        'shard-1': [
          { $id: 'https://example.com/user/2', $type: 'User', $context: 'test' },
          { $id: 'https://example.com/shared', $type: 'Entity', $context: 'test' }, // Duplicate
        ],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0' },
          { type: 'lookup', shardId: 'shard-1' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, { deduplicate: true });

      expect(result.entities).toHaveLength(3);
      const ids = result.entities.map(e => e.$id);
      expect(new Set(ids).size).toBe(3); // All unique
    });

    it('should prefer newer version when deduplicating', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string; version: number }>> = {
        'shard-0': [
          { $id: 'https://example.com/doc', $type: 'Document', $context: 'test', version: 1 },
        ],
        'shard-1': [
          { $id: 'https://example.com/doc', $type: 'Document', $context: 'test', version: 2 }, // Newer
        ],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0' },
          { type: 'lookup', shardId: 'shard-1' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, {
        deduplicate: true,
        deduplicateBy: 'version',
        preferNewer: true,
      });

      expect(result.entities).toHaveLength(1);
      expect((result.entities[0] as unknown as { version: number }).version).toBe(2);
    });
  });
});

describe('Consistency Models', () => {
  describe('Read-your-writes consistency', () => {
    it('should wait for pending writes before reading', async () => {
      const writeComplete = { value: false };

      const getStub = () => {
        return {
          fetch: vi.fn(async (request: Request) => {
            const url = new URL(request.url);

            if (url.pathname === '/lookup') {
              // If write hasn't completed, return stale data
              if (!writeComplete.value) {
                return new Response(JSON.stringify([
                  { $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'stale' },
                ]), { status: 200 });
              }
              return new Response(JSON.stringify([
                { $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'fresh' },
              ]), { status: 200 });
            }

            return new Response(JSON.stringify([]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0', entityIds: [createEntityId('https://example.com/1')] },
        ],
        estimatedCost: 1,
        canBatch: false,
      };

      // Simulate pending write
      const pendingWriteId = 'write-123';
      writeComplete.value = false;

      // Start a write operation that will complete after 50ms
      setTimeout(() => {
        writeComplete.value = true;
      }, 50);

      const result = await orchestrateQuery(plan, getStub, {
        consistency: 'read-your-writes',
        awaitPendingWrite: pendingWriteId,
      });

      // Should have waited for write and returned fresh data
      expect((result.entities[0] as unknown as { value: string }).value).toBe('fresh');
    });
  });

  describe('Quorum reads', () => {
    it('should require majority of shards to agree', async () => {
      const shardResponses: Record<string, Array<{ $id: string; $type: string; $context: string; value: string }>> = {
        'shard-0': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'A' }],
        'shard-1': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'A' }],
        'shard-2': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'B' }], // Divergent
      };

      const getStub = (shardId: string) => createShardStub(shardResponses[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-2', entityIds: [createEntityId('https://example.com/1')] },
        ],
        estimatedCost: 3,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, {
        consistency: 'quorum',
        quorumSize: 2,
      });

      // Should return the value that 2 out of 3 shards agreed on
      expect(result.entities).toHaveLength(1);
      expect((result.entities[0] as unknown as { value: string }).value).toBe('A');
    });

    it('should fail if quorum not reached', async () => {
      const shardResponses: Record<string, Array<{ $id: string; $type: string; $context: string; value: string }>> = {
        'shard-0': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'A' }],
        'shard-1': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'B' }],
        'shard-2': [{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test', value: 'C' }], // All different
      };

      const getStub = (shardId: string) => createShardStub(shardResponses[shardId] ?? []);

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-0', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-2', entityIds: [createEntityId('https://example.com/1')] },
        ],
        estimatedCost: 3,
        canBatch: false,
      };

      await expect(
        orchestrateQuery(plan, getStub, {
          consistency: 'quorum',
          quorumSize: 2,
        })
      ).rejects.toThrow('Quorum not reached');
    });
  });
});

describe('Scatter-Gather Patterns', () => {
  describe('Broadcast queries', () => {
    it('should broadcast query to all known shards', async () => {
      const allShardIds = ['shard-0', 'shard-1', 'shard-2', 'shard-3'];
      const calledShards: string[] = [];

      const getStub = (shardId: string) => {
        return {
          fetch: vi.fn(async () => {
            calledShards.push(shardId);
            return new Response(JSON.stringify([
              { $id: `https://example.com/${shardId}/entity`, $type: 'Entity', $context: 'test' },
            ]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: allShardIds.map(shardId => ({
          type: 'lookup' as const,
          shardId,
        })),
        estimatedCost: allShardIds.length,
        canBatch: true,
      };

      const result = await orchestrateQuery(plan, getStub, { broadcast: true });

      // All shards should have been queried
      expect(calledShards.sort()).toEqual(allShardIds.sort());
      expect(result.entities).toHaveLength(4);
    });

    it('should aggregate results from scatter-gather', async () => {
      const shardData: Record<string, Array<{ $id: string; $type: string; $context: string; count: number }>> = {
        'shard-0': [{ $id: 'https://example.com/stats', $type: 'Stats', $context: 'test', count: 100 }],
        'shard-1': [{ $id: 'https://example.com/stats', $type: 'Stats', $context: 'test', count: 200 }],
        'shard-2': [{ $id: 'https://example.com/stats', $type: 'Stats', $context: 'test', count: 150 }],
      };

      const getStub = (shardId: string) => createShardStub(shardData[shardId] ?? []);

      const plan: QueryPlan = {
        steps: Object.keys(shardData).map(shardId => ({
          type: 'lookup' as const,
          shardId,
        })),
        estimatedCost: 3,
        canBatch: true,
      };

      const result = await orchestrateQuery(plan, getStub, {
        aggregation: {
          type: 'sum',
          field: 'count',
        },
      });

      // Should aggregate counts from all shards
      expect(result.stats.aggregatedValue).toBe(450); // 100 + 200 + 150
    });
  });

  describe('Early termination', () => {
    it('should stop querying shards once limit is reached', async () => {
      const calledShards: string[] = [];

      const getStub = (shardId: string) => {
        return {
          fetch: vi.fn(async () => {
            calledShards.push(shardId);
            // Each shard returns 10 entities
            return new Response(JSON.stringify(
              Array.from({ length: 10 }, (_, i) => ({
                $id: `https://example.com/${shardId}/${i}`,
                $type: 'Entity',
                $context: 'test',
              }))
            ), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: Array.from({ length: 10 }, (_, i) => ({
          type: 'lookup' as const,
          shardId: `shard-${i}`,
        })),
        estimatedCost: 10,
        canBatch: true,
      };

      const result = await orchestrateQuery(plan, getStub, {
        limit: 25,
        earlyTermination: true,
      });

      // Should only query 3 shards (10 + 10 + 10 > 25)
      expect(calledShards.length).toBeLessThanOrEqual(3);
      expect(result.entities.length).toBeLessThanOrEqual(25);
    });
  });
});

describe('Shard Health Awareness', () => {
  describe('Routing around unhealthy shards', () => {
    it('should skip unhealthy shards and use replicas', async () => {
      const healthyShardCalled = { value: false };
      const unhealthyShardCalled = { value: false };

      const getStub = (shardId: string) => {
        if (shardId === 'shard-primary') {
          return {
            fetch: vi.fn(async () => {
              unhealthyShardCalled.value = true;
              throw new Error('Shard is down');
            }),
          } as unknown as DurableObjectStub;
        }
        return {
          fetch: vi.fn(async () => {
            healthyShardCalled.value = true;
            return new Response(JSON.stringify([
              { $id: 'https://example.com/1', $type: 'Entity', $context: 'test' },
            ]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: [
          {
            type: 'lookup',
            shardId: 'shard-primary',
            entityIds: [createEntityId('https://example.com/1')],
          },
        ],
        estimatedCost: 1,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, {
        replicaShards: {
          'shard-primary': 'shard-replica',
        },
        useReplicaOnFailure: true,
      });

      expect(result.entities).toHaveLength(1);
      expect(healthyShardCalled.value).toBe(true);
    });

    it('should report shard health in stats', async () => {
      const getStub = (shardId: string) => {
        if (shardId === 'shard-slow') {
          return {
            fetch: vi.fn(async () => {
              await new Promise(resolve => setTimeout(resolve, 100));
              return new Response(JSON.stringify([]), { status: 200 });
            }),
          } as unknown as DurableObjectStub;
        }
        return {
          fetch: vi.fn(async () => {
            return new Response(JSON.stringify([
              { $id: `https://example.com/${shardId}/1`, $type: 'Entity', $context: 'test' },
            ]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-fast', entityIds: [createEntityId('https://example.com/shard-fast/1')] },
          { type: 'lookup', shardId: 'shard-slow', entityIds: [createEntityId('https://example.com/shard-slow/1')] },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, { trackShardHealth: true });

      expect(result.stats.shardLatencies).toBeDefined();
      expect(result.stats.shardLatencies!['shard-slow']).toBeGreaterThan(
        result.stats.shardLatencies!['shard-fast']
      );
    });
  });
});
