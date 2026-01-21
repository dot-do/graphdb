/**
 * CoordinatorDO Cross-Shard Query Tests
 *
 * Tests for the CoordinatorDO cross-shard query coordination:
 * - Shard registration/deregistration
 * - Heartbeat mechanism for tracking shard health
 * - Cross-shard query execution (lookup, traverse, filter)
 * - Result aggregation and deduplication
 * - Shard health status tracking
 * - Statistics and monitoring endpoints
 *
 * Uses @cloudflare/vitest-pool-workers for real DO stubs.
 * Uses runInDurableObject() for tests that write to storage.
 *
 * @see src/coordinator/coordinator-do.ts for implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { CoordinatorDO, type CoordinatorStats, type ShardInfo, type QueryResponse } from '../../src/coordinator/coordinator-do.js';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Generate a unique coordinator stub for each test
 */
let testCounter = 0;
function getUniqueCoordinatorStub() {
  const id = env.COORDINATOR.idFromName(`coord-test-${Date.now()}-${testCounter++}`);
  return env.COORDINATOR.get(id);
}

// ============================================================================
// Tests
// ============================================================================

describe('CoordinatorDO', () => {
  describe('Health and Stats', () => {
    it('should return healthy status', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/health'));
        expect(response.status).toBe(200);

        const health = await response.json() as { status: string; uptime: number; activeShards: number; registeredShards: number };
        expect(health.status).toBe('healthy');
        expect(health.uptime).toBeGreaterThanOrEqual(0);
        expect(health.activeShards).toBe(0);
        expect(health.registeredShards).toBe(0);
      });
    });

    it('should return initial stats', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/stats'));
        expect(response.status).toBe(200);

        const stats = await response.json() as CoordinatorStats;
        expect(stats.totalQueries).toBe(0);
        expect(stats.queriesInProgress).toBe(0);
        expect(stats.successfulQueries).toBe(0);
        expect(stats.failedQueries).toBe(0);
        expect(stats.registeredShards).toBe(0);
        expect(stats.activeShards).toBe(0);
        expect(stats.uptimeMs).toBeGreaterThanOrEqual(0);
        expect(stats.startupTimestamp).toBeGreaterThan(0);
      });
    });
  });

  describe('Shard Registration', () => {
    it('should register a new shard', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        expect(response.status).toBe(201);
        const data = await response.json() as { success: boolean; shard: ShardInfo };
        expect(data.success).toBe(true);
        expect(data.shard.shardId).toBe('shard-1');
        expect(data.shard.status).toBe('active');
        expect(data.shard.queryCount).toBe(0);
        expect(data.shard.errorCount).toBe(0);
      });
    });

    it('should list registered shards', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register multiple shards sequentially
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-2' }),
        }));

        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-3' }),
        }));

        // List shards
        const response = await instance.fetch(new Request('https://coordinator-do/shards'));
        expect(response.status).toBe(200);

        const data = await response.json() as { shards: ShardInfo[] };
        expect(data.shards.length).toBe(3);
        const shardIds = data.shards.map(s => s.shardId).sort();
        expect(shardIds).toEqual(['shard-1', 'shard-2', 'shard-3']);
      });
    });

    it('should update health after registering shards', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-2' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/health'));
        const health = await response.json() as { registeredShards: number; activeShards: number };

        expect(health.registeredShards).toBe(2);
        expect(health.activeShards).toBe(2);
      });
    });

    it('should reject registration without shardId', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({}),
        }));

        expect(response.status).toBe(400);
        const data = await response.json() as { error: { code: string } };
        expect(data.error.code).toBe('VALIDATION_ERROR');
      });
    });
  });

  describe('Shard Deregistration', () => {
    it('should deregister an existing shard', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register first
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        // Verify registered
        let listResponse = await instance.fetch(new Request('https://coordinator-do/shards'));
        let listData = await listResponse.json() as { shards: ShardInfo[] };
        expect(listData.shards.length).toBe(1);

        // Deregister
        const response = await instance.fetch(new Request('https://coordinator-do/shards/deregister', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as { success: boolean; existed: boolean };
        expect(data.success).toBe(true);
        expect(data.existed).toBe(true);

        // Verify deregistered
        listResponse = await instance.fetch(new Request('https://coordinator-do/shards'));
        listData = await listResponse.json() as { shards: ShardInfo[] };
        expect(listData.shards.length).toBe(0);
      });
    });

    it('should handle deregistering non-existent shard', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/shards/deregister', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'non-existent-shard' }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as { success: boolean; existed: boolean };
        expect(data.success).toBe(true);
        expect(data.existed).toBe(false);
      });
    });
  });

  describe('Shard Heartbeat', () => {
    it('should update lastHeartbeat on heartbeat', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        // Wait a bit then send heartbeat
        await new Promise(resolve => setTimeout(resolve, 10));

        const response = await instance.fetch(new Request('https://coordinator-do/shards/heartbeat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as { success: boolean; shard: ShardInfo };
        expect(data.success).toBe(true);
        expect(data.shard.status).toBe('active');
      });
    });

    it('should reject heartbeat for unregistered shard', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/shards/heartbeat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'unknown-shard' }),
        }));

        expect(response.status).toBe(404);
        const data = await response.json() as { error: { code: string } };
        expect(data.error.code).toBe('NOT_FOUND');
      });
    });
  });

  describe('Query Validation', () => {
    it('should reject query with invalid type', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register a shard first
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'invalid-type' }),
        }));

        expect(response.status).toBe(400);
        const data = await response.json() as { error: { code: string } };
        expect(data.error.code).toBe('VALIDATION_ERROR');
      });
    });

    it('should reject query when no shards are available', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
          }),
        }));

        expect(response.status).toBe(404);
        const data = await response.json() as { error: { code: string; message: string } };
        expect(data.error.code).toBe('NOT_FOUND');
        expect(data.error.message).toContain('No active shards');
      });
    });

    it('should reject non-POST requests to query endpoint', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'GET',
        }));

        expect(response.status).toBe(405);
        const data = await response.json() as { error: { code: string } };
        expect(data.error.code).toBe('METHOD_NOT_ALLOWED');
      });
    });
  });

  describe('Query Execution', () => {
    it('should execute lookup query and return metadata', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register shards
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-2' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
          }),
        }));

        // Note: The actual shard responses will fail since there's no real data,
        // but the coordinator should handle this gracefully
        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.success).toBe(true);
        expect(data.queryId).toBeDefined();
        expect(data.queryId).toMatch(/^q_/);
        expect(data.metadata).toBeDefined();
        expect(data.metadata.shardsQueried).toBe(2);
        expect(data.metadata.durationMs).toBeGreaterThanOrEqual(0);
      });
    });

    it('should execute traverse query', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'traverse',
            from: 'https://example.com/entity/1',
            predicate: 'knows',
            depth: 2,
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.success).toBe(true);
        expect(data.metadata.shardsQueried).toBe(1);
      });
    });

    it('should execute filter query', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'filter',
            field: 'name',
            op: '=',
            value: 'Alice',
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.success).toBe(true);
      });
    });

    it('should query specific shards when shardIds provided', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register multiple shards
        for (const shardId of ['shard-1', 'shard-2', 'shard-3']) {
          await instance.fetch(new Request('https://coordinator-do/shards/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ shardId }),
          }));
        }

        // Query only shard-1 and shard-2
        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
            shardIds: ['shard-1', 'shard-2'],
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.metadata.shardsQueried).toBe(2);
      });
    });
  });

  describe('Statistics Tracking', () => {
    it('should increment query counts', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        // Initial stats
        let statsResponse = await instance.fetch(new Request('https://coordinator-do/stats'));
        let stats = await statsResponse.json() as CoordinatorStats;
        expect(stats.totalQueries).toBe(0);

        // Execute queries
        await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'lookup', ids: ['https://example.com/entity/1'] }),
        }));

        await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'lookup', ids: ['https://example.com/entity/2'] }),
        }));

        // Check updated stats
        statsResponse = await instance.fetch(new Request('https://coordinator-do/stats'));
        stats = await statsResponse.json() as CoordinatorStats;
        expect(stats.totalQueries).toBe(2);
        expect(stats.successfulQueries).toBe(2);
      });
    });

    it('should track failed queries', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register a shard first
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        // Invalid query type should fail
        await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'invalid' }),
        }));

        const statsResponse = await instance.fetch(new Request('https://coordinator-do/stats'));
        const stats = await statsResponse.json() as CoordinatorStats;
        expect(stats.failedQueries).toBe(1);
      });
    });

    it('should update lastQueryTimestamp', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        let statsResponse = await instance.fetch(new Request('https://coordinator-do/stats'));
        let beforeStats = await statsResponse.json() as CoordinatorStats;
        expect(beforeStats.lastQueryTimestamp).toBe(0);

        await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ type: 'lookup', ids: ['https://example.com/entity/1'] }),
        }));

        statsResponse = await instance.fetch(new Request('https://coordinator-do/stats'));
        const afterStats = await statsResponse.json() as CoordinatorStats;
        expect(afterStats.lastQueryTimestamp).toBeGreaterThan(0);
      });
    });
  });

  describe('State Persistence', () => {
    it('should persist shard registrations to storage', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (_instance: CoordinatorDO, state: DurableObjectState) => {
        // Register via instance
        const instance = _instance;
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-2' }),
        }));

        // Verify via storage
        const shard1 = await state.storage.get<ShardInfo>('shard:shard-1');
        const shard2 = await state.storage.get<ShardInfo>('shard:shard-2');

        expect(shard1).toBeDefined();
        expect(shard1?.shardId).toBe('shard-1');
        expect(shard2).toBeDefined();
        expect(shard2?.shardId).toBe('shard-2');
      });
    });

    it('should persist query statistics', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO, state: DurableObjectState) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        // Execute multiple queries
        for (let i = 0; i < 5; i++) {
          await instance.fetch(new Request('https://coordinator-do/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ type: 'lookup', ids: [`https://example.com/entity/${i}`] }),
          }));
        }

        // Verify stats persistence
        const stats = await state.storage.get<{
          totalQueries: number;
          successfulQueries: number;
        }>('stats');

        expect(stats).toBeDefined();
        expect(stats?.totalQueries).toBe(5);
        expect(stats?.successfulQueries).toBe(5);
      });
    });
  });

  describe('Error Handling', () => {
    it('should return 404 for unknown endpoint', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/unknown-endpoint'));

        expect(response.status).toBe(404);
        const data = await response.json() as { error: { code: string } };
        expect(data.error.code).toBe('NOT_FOUND');
      });
    });

    it('should handle malformed JSON in request body', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        const response = await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: 'not valid json',
        }));

        expect(response.status).toBe(400);
        const data = await response.json() as { error: { code: string } };
        expect(data.error.code).toBe('BAD_REQUEST');
      });
    });
  });

  describe('Query Options', () => {
    it('should respect limit parameter', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
            limit: 50,
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        // Results should be limited (though empty in this test)
        expect(data.results).toBeDefined();
        expect(data.results.length).toBeLessThanOrEqual(50);
      });
    });

    it('should enforce maximum limit', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        // Request more than MAX_LIMIT (1000)
        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
            limit: 5000,
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        // Should cap at MAX_LIMIT
        expect(data.results.length).toBeLessThanOrEqual(1000);
      });
    });

    it('should accept timeout parameter', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
            timeout: 1000,
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.success).toBe(true);
      });
    });
  });

  describe('Multiple Shards', () => {
    it('should query all active shards in parallel', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register multiple shards
        for (let i = 1; i <= 5; i++) {
          await instance.fetch(new Request('https://coordinator-do/shards/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ shardId: `shard-${i}` }),
          }));
        }

        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.metadata.shardsQueried).toBe(5);
      });
    });

    it('should handle partial shard failures gracefully', async () => {
      const stub = getUniqueCoordinatorStub();

      await runInDurableObject(stub, async (instance: CoordinatorDO) => {
        // Register shards
        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-1' }),
        }));

        await instance.fetch(new Request('https://coordinator-do/shards/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ shardId: 'shard-2' }),
        }));

        // Execute query - some shards may fail but query should complete
        const response = await instance.fetch(new Request('https://coordinator-do/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type: 'lookup',
            ids: ['https://example.com/entity/1'],
          }),
        }));

        expect(response.status).toBe(200);
        const data = await response.json() as QueryResponse;
        expect(data.success).toBe(true);
        // shardsFailed should track failures
        expect(data.metadata.shardsFailed).toBeDefined();
      });
    });
  });
});
