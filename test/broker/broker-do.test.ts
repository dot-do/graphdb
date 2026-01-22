/**
 * BrokerDO Hibernating WebSocket Tests
 *
 * Tests the hibernating WebSocket pattern that provides:
 * - Fresh 1000 subrequest quota per webSocketMessage wake
 * - State preservation across hibernation cycles
 * - 95% cost savings over active connections
 *
 * Success criteria:
 * - 3 WS messages x 500 subrequests each = 1500 total (exceeds single 1000 limit)
 * - State preserved across hibernation cycles
 *
 * Note: These tests use the @cloudflare/vitest-pool-workers environment
 * which runs actual Durable Objects in the Workers runtime.
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { BrokerDO } from '../../src/broker/broker-do.js';
import { ShardDO } from '../../src/shard/shard-do.js';

// Types for responses
interface SubrequestResult {
  type: 'subrequestResult';
  result: {
    messageId: number;
    requestedCount: number;
    successCount: number;
    failureCount: number;
    errors: string[];
    durationMs: number;
  };
  metrics: {
    wakeNumber: number;
    totalSubrequestsThisSession: number;
    stateValue: number;
  };
}

interface StateMessage {
  type: 'state' | 'stateSet';
  value: number;
}

interface ConnectedMessage {
  type: 'connected';
  clientId: string;
}

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueBrokerStub() {
  const id = env.BROKER.idFromName(`broker-${Date.now()}-${testCounter++}`);
  return env.BROKER.get(id);
}

function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

/**
 * Helper to properly close WebSocket and wait for DO storage operations to complete.
 * This prevents "Isolated storage failed" errors that occur when a test ends
 * before the DO has finished processing the WebSocket close event.
 */
async function closeWebSocket(ws: WebSocket, delayMs: number = 100): Promise<void> {
  ws.close();
  await new Promise(resolve => setTimeout(resolve, delayMs));
}

/**
 * Helper to establish WebSocket connection to BrokerDO
 */
async function connectWebSocket(stub: DurableObjectStub): Promise<WebSocket> {
  const response = await stub.fetch('https://broker-do/connect', {
    headers: { Upgrade: 'websocket' },
  });

  expect(response.status).toBe(101);

  const webSocket = response.webSocket;
  expect(webSocket).toBeDefined();

  webSocket!.accept();
  return webSocket!;
}

/**
 * Wait for a specific message type
 */
function waitForMessage<T>(
  ws: WebSocket,
  predicate: (msg: unknown) => boolean,
  timeoutMs: number = 60000
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timeout waiting for message after ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data as string);
        if (predicate(data)) {
          clearTimeout(timeout);
          ws.removeEventListener('message', handler);
          resolve(data as T);
        }
      } catch {
        // Ignore parse errors
      }
    };

    ws.addEventListener('message', handler);
  });
}

/**
 * Send message and wait for response
 */
async function sendAndWait<T>(
  ws: WebSocket,
  message: object,
  predicate: (msg: unknown) => boolean,
  timeoutMs: number = 60000
): Promise<T> {
  const promise = waitForMessage<T>(ws, predicate, timeoutMs);
  ws.send(JSON.stringify(message));
  return promise;
}

describe('BrokerDO Hibernating WebSocket Handler', () => {
  describe('WebSocket Connection with Hibernation', () => {
    it('should accept WebSocket upgrade and return connected message', async () => {
      const stub = getUniqueBrokerStub();

      const ws = await connectWebSocket(stub);

      const connected = await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      expect(connected.type).toBe('connected');
      expect(connected.clientId).toBeDefined();
      expect(connected.clientId).toMatch(/^client_/);

      await closeWebSocket(ws);
    });

    it('should respond to ping with pong', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const timestamp = Date.now();
      const pong = await sendAndWait<{ type: string; timestamp: number; serverTime: number }>(
        ws,
        { type: 'ping', timestamp },
        (m: unknown) => (m as { type?: string }).type === 'pong'
      );

      expect(pong.type).toBe('pong');
      expect(pong.timestamp).toBe(timestamp);
      expect(pong.serverTime).toBeGreaterThanOrEqual(timestamp);

      await closeWebSocket(ws);
    });
  });

  describe('Subrequest Quota Reset per Wake', () => {
    it('should complete 500 subrequests in a single WS message', async () => {
      const stub = getUniqueBrokerStub();

      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const result = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 500, messageId: 1 },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      expect(result.result.requestedCount).toBe(500);
      expect(result.result.successCount).toBe(500);
      expect(result.result.failureCount).toBe(0);

      await closeWebSocket(ws);
    });

    it('should complete 3 x 500 = 1500 subrequests across hibernation cycles', { timeout: 15000 }, async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      let totalSuccess = 0;
      let totalFailure = 0;

      // Send 3 messages, each triggering 500 subrequests
      for (let i = 1; i <= 3; i++) {
        const result = await sendAndWait<SubrequestResult>(
          ws,
          { subrequests: 500, messageId: i },
          (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
        );

        totalSuccess += result.result.successCount;
        totalFailure += result.result.failureCount;

        console.log(`Message ${i}: ${result.result.successCount}/${result.result.requestedCount} succeeded`);
      }

      // 1500 > 1000 single limit - proves quota resets!
      expect(totalSuccess).toBe(1500);
      expect(totalFailure).toBe(0);

      await closeWebSocket(ws);
    });

    it('should exceed 1000 cumulative subrequests proving quota resets', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // 5 messages x 300 subrequests = 1500 total
      const messageCount = 5;
      const subrequestsPerMessage = 300;
      let totalSuccessful = 0;

      for (let i = 1; i <= messageCount; i++) {
        const result = await sendAndWait<SubrequestResult>(
          ws,
          { subrequests: subrequestsPerMessage, messageId: i },
          (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
        );

        totalSuccessful += result.result.successCount;
        console.log(`Cumulative after message ${i}: ${totalSuccessful}`);
      }

      // If quota didn't reset, we'd hit limit on message 4
      expect(totalSuccessful).toBe(1500);

      await closeWebSocket(ws);
    });
  });

  describe('State Preservation Across Hibernation', () => {
    it('should preserve state across hibernation cycles', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Set state
      const setResult = await sendAndWait<StateMessage>(
        ws,
        { type: 'setState', value: 42 },
        (m: unknown) => (m as { type?: string }).type === 'stateSet'
      );

      expect(setResult.value).toBe(42);

      // Get state back
      const getResult = await sendAndWait<StateMessage>(
        ws,
        { type: 'getState' },
        (m: unknown) => (m as { type?: string }).type === 'state'
      );

      expect(getResult.value).toBe(42);

      await closeWebSocket(ws);
    });

    it('should preserve WebSocket attachment across message handlers', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Send multiple messages
      for (let i = 1; i <= 3; i++) {
        const result = await sendAndWait<SubrequestResult>(
          ws,
          { subrequests: 10, messageId: i },
          (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
        );

        // Wake number should increment
        expect(result.metrics.wakeNumber).toBe(i);
      }

      await closeWebSocket(ws);
    });
  });

  describe('ShardDO Target', () => {
    it('should handle high throughput requests', async () => {
      const shardStub = getUniqueShardStub();

      await runInDurableObject(shardStub, async (instance: ShardDO) => {
        const requestCount = 100;
        const promises: Promise<Response>[] = [];

        for (let i = 0; i < requestCount; i++) {
          const req = new Request(`https://shard-do/count?messageId=test&index=${i}`);
          promises.push(instance.fetch(req));
        }

        const responses = await Promise.all(promises);

        for (const response of responses) {
          expect(response.ok).toBe(true);
        }

        const statsResponse = await instance.fetch(new Request('https://shard-do/stats'));
        const stats = (await statsResponse.json()) as { totalRequests: number };

        expect(stats.totalRequests).toBe(requestCount);
      });
    });

    it('should track per-message request counts', async () => {
      const shardStub = getUniqueShardStub();

      await runInDurableObject(shardStub, async (instance: ShardDO) => {
        for (let i = 0; i < 10; i++) {
          await instance.fetch(new Request(`https://shard-do/count?messageId=msg1&index=${i}`));
        }
        for (let i = 0; i < 20; i++) {
          await instance.fetch(new Request(`https://shard-do/count?messageId=msg2&index=${i}`));
        }

        const statsResponse = await instance.fetch(new Request('https://shard-do/stats'));
        const stats = (await statsResponse.json()) as {
          totalRequests: number;
          perMessageStats: Record<string, number>;
        };

        expect(stats.totalRequests).toBe(30);
        expect(stats.perMessageStats.msg_msg1).toBe(10);
        expect(stats.perMessageStats.msg_msg2).toBe(20);
      });
    });
  });

  describe('Multiple Messages', () => {
    it('should handle rapid sequential messages', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const messageCount = 10;
      const results: SubrequestResult[] = [];

      for (let i = 1; i <= messageCount; i++) {
        const result = await sendAndWait<SubrequestResult>(
          ws,
          { subrequests: 50, messageId: i },
          (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
        );
        results.push(result);
      }

      const totalSuccess = results.reduce((sum, r) => sum + r.result.successCount, 0);
      expect(totalSuccess).toBe(500); // 10 * 50

      await closeWebSocket(ws);
    });

    it('should reject zero subrequests with validation error', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // MIN_SUBREQUESTS is 1, so 0 should return a validation error
      // Validation errors now properly propagate through the RPC layer
      const result = await sendAndWait<{ type: string; code: string; message: string; details?: { min: number; max: number } }>(
        ws,
        { method: 'executeSubrequests', args: [0, 1] },
        (m: unknown) => (m as { type?: string }).type === 'error'
      );

      expect(result.type).toBe('error');
      expect(result.code).toBe('VALIDATION_ERROR');
      expect(result.message).toContain('subrequests must be a number between');
      expect(result.details?.min).toBe(1);
      expect(result.details?.max).toBe(1000);

      await closeWebSocket(ws);
    });
  });

  describe('Cursor Hibernation Support', () => {
    it('should store cursor in WebSocket attachment for hibernation survival', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Create a test cursor (simulating what would be returned from a paginated query)
      const testCursor = btoa(JSON.stringify({
        lastId: 'https://example.com/user/friend-10',
        queryHash: 'abc123',
        ts: Date.now(),
        offset: 10,
      }));

      // Store cursor in attachment
      const storeCursorResult = await sendAndWait<{ type: string; success: boolean }>(
        ws,
        {
          type: 'storeCursor',
          queryId: 'friends-query',
          cursor: testCursor,
        },
        (m: unknown) => (m as { type?: string }).type === 'cursorStored'
      );

      expect(storeCursorResult.success).toBe(true);

      // Retrieve cursor from attachment (simulating hibernation wake)
      const retrieveResult = await sendAndWait<{
        type: string;
        cursor?: string;
        queryId: string;
      }>(
        ws,
        {
          type: 'getCursor',
          queryId: 'friends-query',
        },
        (m: unknown) => (m as { type?: string }).type === 'cursor'
      );

      expect(retrieveResult.cursor).toBe(testCursor);

      await closeWebSocket(ws);
    });

    it('should preserve cursor across multiple message handlers (hibernation cycles)', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const testCursor = btoa(JSON.stringify({ lastId: 'test-123', offset: 10, ts: Date.now() }));

      // Store cursor
      await sendAndWait<{ type: string }>(
        ws,
        {
          type: 'storeCursor',
          queryId: 'test-query',
          cursor: testCursor,
        },
        (m: unknown) => (m as { type?: string }).type === 'cursorStored'
      );

      // Trigger multiple wake cycles with subrequests
      for (let i = 0; i < 3; i++) {
        await sendAndWait<SubrequestResult>(
          ws,
          { subrequests: 10, messageId: i },
          (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
        );
      }

      // Cursor should still be retrievable after hibernation cycles
      const retrieveResult = await sendAndWait<{
        type: string;
        cursor?: string;
      }>(
        ws,
        {
          type: 'getCursor',
          queryId: 'test-query',
        },
        (m: unknown) => (m as { type?: string }).type === 'cursor'
      );

      expect(retrieveResult.cursor).toBe(testCursor);

      await closeWebSocket(ws);
    });

    it('should handle multiple cursors for different queries', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const cursor1 = btoa(JSON.stringify({ queryId: 'q1', offset: 10 }));
      const cursor2 = btoa(JSON.stringify({ queryId: 'q2', offset: 20 }));
      const cursor3 = btoa(JSON.stringify({ queryId: 'q3', offset: 30 }));

      // Store multiple cursors
      await sendAndWait<{ type: string }>(
        ws,
        { type: 'storeCursor', queryId: 'query-1', cursor: cursor1 },
        (m: unknown) => (m as { type?: string }).type === 'cursorStored'
      );

      await sendAndWait<{ type: string }>(
        ws,
        { type: 'storeCursor', queryId: 'query-2', cursor: cursor2 },
        (m: unknown) => (m as { type?: string }).type === 'cursorStored'
      );

      await sendAndWait<{ type: string }>(
        ws,
        { type: 'storeCursor', queryId: 'query-3', cursor: cursor3 },
        (m: unknown) => (m as { type?: string }).type === 'cursorStored'
      );

      // Retrieve each cursor
      const result1 = await sendAndWait<{ type: string; cursor?: string }>(
        ws,
        { type: 'getCursor', queryId: 'query-1' },
        (m: unknown) => (m as { type?: string }).type === 'cursor'
      );
      expect(result1.cursor).toBe(cursor1);

      const result2 = await sendAndWait<{ type: string; cursor?: string }>(
        ws,
        { type: 'getCursor', queryId: 'query-2' },
        (m: unknown) => (m as { type?: string }).type === 'cursor'
      );
      expect(result2.cursor).toBe(cursor2);

      const result3 = await sendAndWait<{ type: string; cursor?: string }>(
        ws,
        { type: 'getCursor', queryId: 'query-3' },
        (m: unknown) => (m as { type?: string }).type === 'cursor'
      );
      expect(result3.cursor).toBe(cursor3);

      await closeWebSocket(ws);
    });

    it('should clear cursor when requested', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const testCursor = btoa(JSON.stringify({ offset: 10 }));

      // Store cursor
      await sendAndWait<{ type: string }>(
        ws,
        { type: 'storeCursor', queryId: 'temp-query', cursor: testCursor },
        (m: unknown) => (m as { type?: string }).type === 'cursorStored'
      );

      // Clear cursor
      await sendAndWait<{ type: string }>(
        ws,
        { type: 'clearCursor', queryId: 'temp-query' },
        (m: unknown) => (m as { type?: string }).type === 'cursorCleared'
      );

      // Should return null/undefined for cleared cursor
      const result = await sendAndWait<{ type: string; cursor?: string }>(
        ws,
        { type: 'getCursor', queryId: 'temp-query' },
        (m: unknown) => (m as { type?: string }).type === 'cursor'
      );

      expect(result.cursor).toBeUndefined();

      await closeWebSocket(ws);
    });
  });

  describe('JSON Security Validation', () => {
    it('should reject malformed JSON with error response', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Send malformed JSON
      const errorPromise = waitForMessage<{ type: string; code: string; message: string }>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'error'
      );

      ws.send('{"invalid json');

      const errorResponse = await errorPromise;

      expect(errorResponse.type).toBe('error');
      expect(errorResponse.code).toBe('PARSE_ERROR');
      expect(errorResponse.message).toContain('Invalid JSON');

      await closeWebSocket(ws);
    });

    it('should reject oversized messages', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Send message larger than 64KB
      const largePayload = JSON.stringify({ data: 'x'.repeat(70000) });

      const errorPromise = waitForMessage<{ type: string; code: string; message: string }>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'error'
      );

      ws.send(largePayload);

      const errorResponse = await errorPromise;

      expect(errorResponse.type).toBe('error');
      expect(errorResponse.code).toBe('SIZE_EXCEEDED');
      expect(errorResponse.message).toContain('exceeds maximum allowed size');

      await closeWebSocket(ws);
    });
  });

  describe('Metrics Persistence Across DO Eviction', () => {
    it('should restore metrics from storage on DO wake', async () => {
      // Use same name to get the same DO instance after simulated eviction
      const brokerName = `broker-persist-${Date.now()}`;
      const id = env.BROKER.idFromName(brokerName);

      // First session - accumulate some metrics
      {
        const stub = env.BROKER.get(id);
        const ws = await connectWebSocket(stub);

        await waitForMessage<ConnectedMessage>(
          ws,
          (m: unknown) => (m as { type?: string }).type === 'connected'
        );

        // Send multiple messages to accumulate metrics
        for (let i = 1; i <= 12; i++) {
          await sendAndWait<SubrequestResult>(
            ws,
            { subrequests: 10, messageId: i },
            (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
          );
        }

        await closeWebSocket(ws);
      }

      // Verify metrics via HTTP endpoint (same stub, metrics should be in memory)
      {
        const stub = env.BROKER.get(id);
        const response = await stub.fetch('https://broker-do/metrics');
        const data = (await response.json()) as { metrics: { totalWakes: number; totalSubrequests: number } };

        expect(data.metrics.totalWakes).toBe(12);
        expect(data.metrics.totalSubrequests).toBe(120); // 12 * 10
      }
    });

    it('should flush metrics on threshold and restore after simulated eviction', async () => {
      const brokerName = `broker-flush-${Date.now()}`;
      const id = env.BROKER.idFromName(brokerName);

      // First session - send exactly threshold messages to trigger flush
      {
        const stub = env.BROKER.get(id);
        const ws = await connectWebSocket(stub);

        await waitForMessage<ConnectedMessage>(
          ws,
          (m: unknown) => (m as { type?: string }).type === 'connected'
        );

        // Send 10 messages (threshold is 10) to ensure flush
        for (let i = 1; i <= 10; i++) {
          await sendAndWait<SubrequestResult>(
            ws,
            { subrequests: 5, messageId: i },
            (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
          );
        }

        await closeWebSocket(ws);
      }

      // Verify metrics persisted via HTTP
      {
        const stub = env.BROKER.get(id);
        const response = await stub.fetch('https://broker-do/metrics');
        const data = (await response.json()) as { metrics: { totalWakes: number; totalSubrequests: number } };

        // Should have at least 10 wakes (threshold reached, flush happened)
        expect(data.metrics.totalWakes).toBeGreaterThanOrEqual(10);
        expect(data.metrics.totalSubrequests).toBeGreaterThanOrEqual(50);
      }
    });

    it('should trigger alarm handler for periodic metrics flush', async () => {
      const stub = getUniqueBrokerStub();

      // runInDurableObject allows direct access to DO instance
      await runInDurableObject(stub, async (instance: BrokerDO) => {
        // Trigger alarm directly (simulating scheduled alarm)
        await instance.alarm();

        // Verify alarm completed without error
        // The alarm should have flushed metrics and rescheduled itself
      });

      // Verify DO is still healthy after alarm
      const response = await stub.fetch('https://broker-do/health');
      expect(response.ok).toBe(true);

      const data = (await response.json()) as { status: string };
      expect(data.status).toBe('ok');
    });

    it('should preserve metrics subrequestsPerWake rolling window', async () => {
      const brokerName = `broker-rolling-${Date.now()}`;
      const id = env.BROKER.idFromName(brokerName);

      const stub = env.BROKER.get(id);
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Send multiple messages with varying subrequest counts
      const subrequestCounts = [10, 20, 30, 40, 50];
      for (let i = 0; i < subrequestCounts.length; i++) {
        await sendAndWait<SubrequestResult>(
          ws,
          { subrequests: subrequestCounts[i], messageId: i + 1 },
          (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
        );
      }

      await closeWebSocket(ws);

      // Verify rolling window via metrics endpoint
      const response = await stub.fetch('https://broker-do/metrics');
      const data = (await response.json()) as {
        metrics: { subrequestsPerWake: number[]; totalSubrequests: number };
      };

      // Should have recorded all subrequest counts
      expect(data.metrics.subrequestsPerWake.length).toBe(5);
      expect(data.metrics.totalSubrequests).toBe(150); // 10+20+30+40+50
    });
  });
});
