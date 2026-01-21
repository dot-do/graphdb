/**
 * Retry and Error Recovery Tests for Query Orchestration
 *
 * TDD RED phase - tests for retry logic in the orchestrator.
 * These tests define expected behavior for:
 * - Automatic retry on transient failures
 * - Exponential backoff
 * - Circuit breaker patterns
 * - Partial failure handling
 * - Timeout recovery
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  planQuery,
  executeStep,
  orchestrateQuery,
  type QueryPlan,
  type QueryStep,
} from '../../src/broker/orchestrator';
import { createEntityId } from '../../src/core/types';

// Mock shard stub for testing retry behavior
function createMockShardStub(options: {
  failCount?: number;
  failWithTimeout?: boolean;
  failWithNetworkError?: boolean;
  responseDelay?: number;
  responses?: unknown[];
}): DurableObjectStub {
  let callCount = 0;
  const { failCount = 0, failWithTimeout = false, failWithNetworkError = false, responseDelay = 0, responses = [] } = options;

  return {
    fetch: vi.fn(async (request: Request) => {
      callCount++;

      // Simulate delay
      if (responseDelay > 0) {
        await new Promise(resolve => setTimeout(resolve, responseDelay));
      }

      // Simulate failures for first N calls
      if (callCount <= failCount) {
        if (failWithTimeout) {
          throw new Error('Request timed out');
        }
        if (failWithNetworkError) {
          throw new Error('Network error: connection refused');
        }
        return new Response(JSON.stringify({
          success: false,
          error: { code: 'SHARD_UNAVAILABLE', message: 'Temporary failure' }
        }), { status: 503 });
      }

      // Return response from responses array or default empty array
      const responseData = responses[callCount - failCount - 1] ?? [];
      return new Response(JSON.stringify(responseData), { status: 200 });
    }),
  } as unknown as DurableObjectStub;
}

describe('Retry Logic', () => {
  describe('Automatic retry on transient failures', () => {
    it('should retry on HTTP 503 Service Unavailable', async () => {
      const mockStub = createMockShardStub({
        failCount: 2,
        responses: [[{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test' }]],
      });

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      const result = await executeStep(step, mockStub);

      // Should have retried and eventually succeeded
      expect(mockStub.fetch).toHaveBeenCalledTimes(3); // 2 failures + 1 success
      expect(result).toHaveLength(1);
    });

    it('should retry on timeout errors', async () => {
      const mockStub = createMockShardStub({
        failCount: 1,
        failWithTimeout: true,
        responses: [[{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test' }]],
      });

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      const result = await executeStep(step, mockStub);

      expect(mockStub.fetch).toHaveBeenCalledTimes(2);
      expect(result).toHaveLength(1);
    });

    it('should retry on network errors', async () => {
      const mockStub = createMockShardStub({
        failCount: 1,
        failWithNetworkError: true,
        responses: [[{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test' }]],
      });

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      const result = await executeStep(step, mockStub);

      expect(mockStub.fetch).toHaveBeenCalledTimes(2);
      expect(result).toHaveLength(1);
    });

    it('should NOT retry on HTTP 400 Bad Request', async () => {
      const mockStub = {
        fetch: vi.fn(async () => {
          return new Response(JSON.stringify({
            success: false,
            error: { code: 'INVALID_REQUEST', message: 'Bad request format' },
          }), { status: 400 });
        }),
      } as unknown as DurableObjectStub;

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      await expect(executeStep(step, mockStub)).rejects.toThrow();
      expect(mockStub.fetch).toHaveBeenCalledTimes(1); // No retry on 4xx
    });

    it('should NOT retry on HTTP 404 Not Found', async () => {
      const mockStub = {
        fetch: vi.fn(async () => {
          return new Response(JSON.stringify({
            success: false,
            error: { code: 'NOT_FOUND', message: 'Entity not found' },
          }), { status: 404 });
        }),
      } as unknown as DurableObjectStub;

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      await expect(executeStep(step, mockStub)).rejects.toThrow();
      expect(mockStub.fetch).toHaveBeenCalledTimes(1);
    });
  });

  describe('Retry limits', () => {
    it('should fail after max retries exceeded', async () => {
      const mockStub = createMockShardStub({
        failCount: 10, // More failures than max retries
      });

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      await expect(executeStep(step, mockStub)).rejects.toThrow();

      // Should have tried initial + max retries (default 3)
      expect(mockStub.fetch).toHaveBeenCalledTimes(4); // 1 initial + 3 retries
    });

    it('should respect custom retry count', async () => {
      const mockStub = createMockShardStub({
        failCount: 5,
        responses: [[{ $id: 'https://example.com/1', $type: 'Entity', $context: 'test' }]],
      });

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      // With maxRetries: 5, should succeed after 5 failures
      const result = await executeStep(step, mockStub, { maxRetries: 5 });

      expect(mockStub.fetch).toHaveBeenCalledTimes(6); // 5 failures + 1 success
      expect(result).toHaveLength(1);
    });
  });

  describe('Exponential backoff', () => {
    it('should apply exponential backoff between retries', async () => {
      const timestamps: number[] = [];
      const mockStub = {
        fetch: vi.fn(async () => {
          timestamps.push(Date.now());
          if (timestamps.length < 4) {
            return new Response(JSON.stringify({
              success: false,
              error: { code: 'SHARD_UNAVAILABLE', message: 'Retry later' },
            }), { status: 503 });
          }
          return new Response(JSON.stringify([]), { status: 200 });
        }),
      } as unknown as DurableObjectStub;

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      await executeStep(step, mockStub);

      // Verify delays increase exponentially
      // Expected pattern: 0ms, ~100ms, ~200ms, ~400ms (with jitter)
      const delays = [];
      for (let i = 1; i < timestamps.length; i++) {
        delays.push(timestamps[i] - timestamps[i - 1]);
      }

      // Each delay should be roughly double the previous (allowing for jitter)
      expect(delays[1]).toBeGreaterThan(delays[0] * 1.5);
      expect(delays[2]).toBeGreaterThan(delays[1] * 1.5);
    });

    it('should cap backoff at maximum delay', async () => {
      const timestamps: number[] = [];
      const mockStub = {
        fetch: vi.fn(async () => {
          timestamps.push(Date.now());
          if (timestamps.length < 6) {
            return new Response(JSON.stringify({
              success: false,
              error: { code: 'SHARD_UNAVAILABLE', message: 'Retry later' },
            }), { status: 503 });
          }
          return new Response(JSON.stringify([]), { status: 200 });
        }),
      } as unknown as DurableObjectStub;

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      await executeStep(step, mockStub, { maxRetries: 5, maxBackoffMs: 500 });

      const delays = [];
      for (let i = 1; i < timestamps.length; i++) {
        delays.push(timestamps[i] - timestamps[i - 1]);
      }

      // All delays should be capped at maxBackoffMs (+ some tolerance for jitter)
      for (const delay of delays) {
        expect(delay).toBeLessThanOrEqual(600); // 500ms + 100ms tolerance
      }
    });
  });
});

describe('Circuit Breaker', () => {
  describe('Circuit opening', () => {
    it('should open circuit after consecutive failures threshold', async () => {
      let callCount = 0;
      const mockStub = {
        fetch: vi.fn(async () => {
          callCount++;
          return new Response(JSON.stringify({
            success: false,
            error: { code: 'SHARD_UNAVAILABLE', message: 'Service down' },
          }), { status: 503 });
        }),
      } as unknown as DurableObjectStub;

      const getStub = () => mockStub;

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] },
        ],
        estimatedCost: 1,
        canBatch: false,
      };

      // Execute multiple queries to trigger circuit breaker
      const errors: Error[] = [];
      for (let i = 0; i < 10; i++) {
        try {
          await orchestrateQuery(plan, getStub);
        } catch (e) {
          errors.push(e as Error);
        }
      }

      // After circuit opens, subsequent calls should fail fast
      // without actually calling the shard
      const lastError = errors[errors.length - 1];
      expect(lastError?.message).toContain('Circuit breaker open');
    });

    it('should track failures per shard independently', async () => {
      const shard1Calls = { count: 0 };
      const shard2Calls = { count: 0 };

      const getStub = (shardId: string) => {
        if (shardId === 'shard-1') {
          return {
            fetch: vi.fn(async () => {
              shard1Calls.count++;
              return new Response(JSON.stringify({
                success: false,
                error: { code: 'SHARD_UNAVAILABLE', message: 'Shard 1 down' },
              }), { status: 503 });
            }),
          } as unknown as DurableObjectStub;
        }
        return {
          fetch: vi.fn(async () => {
            shard2Calls.count++;
            return new Response(JSON.stringify([{ $id: 'test', $type: 'Entity', $context: 'test' }]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      // Queries to failing shard-1
      const failingPlan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] }],
        estimatedCost: 1,
        canBatch: false,
      };

      // Queries to healthy shard-2
      const healthyPlan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-2', entityIds: [createEntityId('https://example.com/2')] }],
        estimatedCost: 1,
        canBatch: false,
      };

      // Trip circuit for shard-1
      for (let i = 0; i < 5; i++) {
        try {
          await orchestrateQuery(failingPlan, getStub);
        } catch { /* expected */ }
      }

      // Shard-2 should still work
      const result = await orchestrateQuery(healthyPlan, getStub);
      expect(result.entities).toHaveLength(1);
    });
  });

  describe('Circuit closing (half-open state)', () => {
    it('should allow test request after cooldown period', async () => {
      vi.useFakeTimers();

      let callCount = 0;
      const mockStub = {
        fetch: vi.fn(async () => {
          callCount++;
          if (callCount <= 5) {
            return new Response(JSON.stringify({
              success: false,
              error: { code: 'SHARD_UNAVAILABLE', message: 'Service down' },
            }), { status: 503 });
          }
          // Recover after 5 failures
          return new Response(JSON.stringify([{ $id: 'test', $type: 'Entity', $context: 'test' }]), { status: 200 });
        }),
      } as unknown as DurableObjectStub;

      const getStub = () => mockStub;

      const plan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] }],
        estimatedCost: 1,
        canBatch: false,
      };

      // Trip circuit
      for (let i = 0; i < 5; i++) {
        try {
          await orchestrateQuery(plan, getStub);
        } catch { /* expected */ }
      }

      // Advance time past cooldown
      vi.advanceTimersByTime(30000); // 30 seconds

      // Next request should be allowed (half-open)
      const result = await orchestrateQuery(plan, getStub);
      expect(result.entities).toHaveLength(1);

      vi.useRealTimers();
    });

    it('should close circuit on successful half-open request', async () => {
      vi.useFakeTimers();

      let callCount = 0;
      const mockStub = {
        fetch: vi.fn(async () => {
          callCount++;
          if (callCount <= 5) {
            return new Response(JSON.stringify({
              success: false,
              error: { code: 'SHARD_UNAVAILABLE', message: 'Service down' },
            }), { status: 503 });
          }
          return new Response(JSON.stringify([{ $id: 'test', $type: 'Entity', $context: 'test' }]), { status: 200 });
        }),
      } as unknown as DurableObjectStub;

      const getStub = () => mockStub;

      const plan: QueryPlan = {
        steps: [{ type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] }],
        estimatedCost: 1,
        canBatch: false,
      };

      // Trip circuit
      for (let i = 0; i < 5; i++) {
        try {
          await orchestrateQuery(plan, getStub);
        } catch { /* expected */ }
      }

      // Advance time past cooldown
      vi.advanceTimersByTime(30000);

      // Successful half-open request
      await orchestrateQuery(plan, getStub);

      // Subsequent requests should work immediately (circuit closed)
      const startCount = callCount;
      const result = await orchestrateQuery(plan, getStub);

      expect(result.entities).toHaveLength(1);
      // No delay should have been imposed
      expect(callCount).toBe(startCount + 1);

      vi.useRealTimers();
    });
  });
});

describe('Partial Failure Handling', () => {
  describe('Multi-shard queries', () => {
    it('should return partial results when one shard fails', async () => {
      const getStub = (shardId: string) => {
        if (shardId === 'shard-fail') {
          return {
            fetch: vi.fn(async () => {
              return new Response(JSON.stringify({
                success: false,
                error: { code: 'SHARD_UNAVAILABLE', message: 'Shard failed' },
              }), { status: 503 });
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
          { type: 'lookup', shardId: 'shard-ok', entityIds: [createEntityId('https://example.com/shard-ok/1')] },
          { type: 'lookup', shardId: 'shard-fail', entityIds: [createEntityId('https://example.com/shard-fail/1')] },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, { allowPartialResults: true });

      expect(result.entities).toHaveLength(1);
      expect(result.stats.partialFailure).toBe(true);
      expect(result.stats.failedShards).toContain('shard-fail');
    });

    it('should fail entirely when partial results disabled', async () => {
      const getStub = (shardId: string) => {
        if (shardId === 'shard-fail') {
          return {
            fetch: vi.fn(async () => {
              return new Response(JSON.stringify({
                success: false,
                error: { code: 'SHARD_UNAVAILABLE', message: 'Shard failed' },
              }), { status: 503 });
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
          { type: 'lookup', shardId: 'shard-ok', entityIds: [createEntityId('https://example.com/shard-ok/1')] },
          { type: 'lookup', shardId: 'shard-fail', entityIds: [createEntityId('https://example.com/shard-fail/1')] },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      await expect(
        orchestrateQuery(plan, getStub, { allowPartialResults: false })
      ).rejects.toThrow();
    });

    it('should include failure details in result stats', async () => {
      const getStub = (shardId: string) => {
        if (shardId === 'shard-fail') {
          return {
            fetch: vi.fn(async () => {
              return new Response(JSON.stringify({
                success: false,
                error: {
                  code: 'SHARD_OVERLOADED',
                  message: 'Too many concurrent requests',
                  shardId: 'shard-fail',
                },
              }), { status: 503 });
            }),
          } as unknown as DurableObjectStub;
        }
        return {
          fetch: vi.fn(async () => {
            return new Response(JSON.stringify([]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-ok', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-fail', entityIds: [createEntityId('https://example.com/2')] },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const result = await orchestrateQuery(plan, getStub, { allowPartialResults: true });

      expect(result.stats.partialFailure).toBe(true);
      expect(result.stats.failedShards).toHaveLength(1);
      expect(result.stats.errors).toContainEqual(
        expect.objectContaining({
          shardId: 'shard-fail',
          code: 'SHARD_OVERLOADED',
        })
      );
    });
  });
});

describe('Timeout Handling', () => {
  describe('Per-step timeout', () => {
    it('should timeout individual steps', async () => {
      vi.useFakeTimers();

      const mockStub = {
        fetch: vi.fn(async () => {
          // Never resolves - simulates hung request
          return new Promise(() => {});
        }),
      } as unknown as DurableObjectStub;

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      const executePromise = executeStep(step, mockStub, { timeoutMs: 5000 });

      vi.advanceTimersByTime(5001);

      await expect(executePromise).rejects.toThrow('Step execution timed out');

      vi.useRealTimers();
    });

    it('should use default timeout when not specified', async () => {
      vi.useFakeTimers();

      const mockStub = {
        fetch: vi.fn(async () => {
          return new Promise(() => {});
        }),
      } as unknown as DurableObjectStub;

      const step: QueryStep = {
        type: 'lookup',
        shardId: 'shard-1',
        entityIds: [createEntityId('https://example.com/1')],
      };

      const executePromise = executeStep(step, mockStub);

      // Default timeout should be 30 seconds
      vi.advanceTimersByTime(30001);

      await expect(executePromise).rejects.toThrow('Step execution timed out');

      vi.useRealTimers();
    });
  });

  describe('Query-level timeout', () => {
    it('should timeout entire query orchestration', async () => {
      vi.useFakeTimers();

      const mockStub = {
        fetch: vi.fn(async () => {
          return new Promise(() => {});
        }),
      } as unknown as DurableObjectStub;

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'traverse', shardId: 'shard-1', predicate: 'knows' },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const queryPromise = orchestrateQuery(plan, () => mockStub, { totalTimeoutMs: 10000 });

      vi.advanceTimersByTime(10001);

      await expect(queryPromise).rejects.toThrow('Query execution timed out');

      vi.useRealTimers();
    });

    it('should cancel remaining steps when timeout reached', async () => {
      vi.useFakeTimers();

      let step2Called = false;
      const getStub = (shardId: string) => {
        if (shardId === 'shard-1') {
          return {
            fetch: vi.fn(async () => {
              await new Promise(resolve => setTimeout(resolve, 8000));
              return new Response(JSON.stringify([{ $id: 'test', $type: 'Entity', $context: 'test' }]), { status: 200 });
            }),
          } as unknown as DurableObjectStub;
        }
        return {
          fetch: vi.fn(async () => {
            step2Called = true;
            return new Response(JSON.stringify([]), { status: 200 });
          }),
        } as unknown as DurableObjectStub;
      };

      const plan: QueryPlan = {
        steps: [
          { type: 'lookup', shardId: 'shard-1', entityIds: [createEntityId('https://example.com/1')] },
          { type: 'lookup', shardId: 'shard-2', entityIds: [createEntityId('https://example.com/2')] },
        ],
        estimatedCost: 2,
        canBatch: false,
      };

      const queryPromise = orchestrateQuery(plan, getStub, { totalTimeoutMs: 5000 });

      vi.advanceTimersByTime(5001);

      await expect(queryPromise).rejects.toThrow();
      expect(step2Called).toBe(false); // Second step should not have been called

      vi.useRealTimers();
    });
  });
});
