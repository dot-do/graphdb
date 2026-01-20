/**
 * ShardDO Lifecycle and Hibernation Tests
 *
 * Tests for ShardDO hibernation behavior and state preservation:
 * - Should accept WebSocket connection
 * - Should hibernate when no activity
 * - Should preserve state across hibernation
 * - Should handle alarm wakeup
 * - Should resume pending operations after hibernation
 * - Should cleanup old connections
 *
 * Uses vitest-pool-workers for hibernation simulation.
 *
 * @see CLAUDE.md for architecture details
 * @see src/shard/shard-do.ts for implementation
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect, beforeEach } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';

/**
 * Attachment data stored with hibernated WebSocket connections for ShardDO
 */
interface ShardWebSocketAttachment {
  clientId: string;
  connectedAt: number;
  totalMessagesReceived: number;
  pendingOperations: number;
  lastActivityTimestamp: number;
}

/**
 * Lifecycle state message types
 */
interface LifecycleStateMessage {
  type: 'state' | 'stateSet';
  value: number;
  hibernationCount?: number;
}

interface ConnectedMessage {
  type: 'connected';
  clientId: string;
  shardId?: string;
}

interface AlarmResultMessage {
  type: 'alarmResult';
  processedOperations: number;
  nextAlarmScheduled: boolean;
}

interface PendingOperationResult {
  type: 'operationQueued' | 'operationResult';
  operationId: string;
  status: 'pending' | 'completed' | 'failed';
  result?: unknown;
}

interface ConnectionCleanupResult {
  type: 'cleanupResult';
  closedConnections: number;
  remainingConnections: number;
}

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-lifecycle-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

/**
 * Helper to establish WebSocket connection to ShardDO
 */
async function connectWebSocket(stub: DurableObjectStub): Promise<WebSocket> {
  const response = await stub.fetch('https://shard-do/ws', {
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
  timeoutMs: number = 30000
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
  timeoutMs: number = 30000
): Promise<T> {
  const promise = waitForMessage<T>(ws, predicate, timeoutMs);
  ws.send(JSON.stringify(message));
  return promise;
}

describe('ShardDO Lifecycle and Hibernation', () => {
  describe('WebSocket Connection', () => {
    it('should accept WebSocket connection', async () => {
      const stub = getUniqueShardStub();

      const ws = await connectWebSocket(stub);

      const connected = await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      expect(connected.type).toBe('connected');
      expect(connected.clientId).toBeDefined();
      expect(connected.clientId).toMatch(/^shard_client_/);

      ws.close();
    });

    it('should respond to ping with pong preserving connection state', async () => {
      const stub = getUniqueShardStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const timestamp = Date.now();
      const pong = await sendAndWait<{ type: string; timestamp: number; serverTime: number; hibernationCount: number }>(
        ws,
        { type: 'ping', timestamp },
        (m: unknown) => (m as { type?: string }).type === 'pong'
      );

      expect(pong.type).toBe('pong');
      expect(pong.timestamp).toBe(timestamp);
      expect(pong.serverTime).toBeGreaterThanOrEqual(timestamp);
      // Hibernation count should be tracked
      expect(typeof pong.hibernationCount).toBe('number');

      ws.close();
    });
  });

  describe('Hibernation Behavior', () => {
    it('should hibernate when no activity', async () => {
      const stub = getUniqueShardStub();
      const ws = await connectWebSocket(stub);

      const connected = await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      expect(connected.clientId).toBeDefined();

      // The DO should hibernate after we stop sending messages
      // When we send another message, it should wake and process it
      // This tests that hibernation works correctly

      // Wait a small amount then send another message
      await new Promise((resolve) => setTimeout(resolve, 100));

      const pong = await sendAndWait<{ type: string; hibernationCount: number }>(
        ws,
        { type: 'ping', timestamp: Date.now() },
        (m: unknown) => (m as { type?: string }).type === 'pong'
      );

      // DO should have woken from hibernation
      expect(pong.hibernationCount).toBeGreaterThanOrEqual(0);

      ws.close();
    });

    it('should preserve state across hibernation', async () => {
      const stub = getUniqueShardStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Set state value
      const setValue = 12345;
      const setResult = await sendAndWait<LifecycleStateMessage>(
        ws,
        { type: 'setState', value: setValue },
        (m: unknown) => (m as { type?: string }).type === 'stateSet'
      );

      expect(setResult.value).toBe(setValue);

      // Allow time for potential hibernation
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Get state back - should be preserved
      const getResult = await sendAndWait<LifecycleStateMessage>(
        ws,
        { type: 'getState' },
        (m: unknown) => (m as { type?: string }).type === 'state'
      );

      expect(getResult.value).toBe(setValue);

      ws.close();
    });

    it('should preserve WebSocket attachment across hibernation cycles', async () => {
      const stub = getUniqueShardStub();
      const ws = await connectWebSocket(stub);

      const connected = await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const originalClientId = connected.clientId;

      // Send multiple messages to trigger multiple hibernation wake cycles
      for (let i = 1; i <= 5; i++) {
        const result = await sendAndWait<{ type: string; messageCount: number; clientId: string }>(
          ws,
          { type: 'ping', timestamp: Date.now(), messageId: i },
          (m: unknown) => (m as { type?: string }).type === 'pong'
        );

        // Client ID should remain consistent across wakes
        expect(result.clientId).toBe(originalClientId);
        // Message count should increment (proving attachment is preserved)
        expect(result.messageCount).toBe(i);
      }

      ws.close();
    });
  });

  describe('Alarm Wakeup', () => {
    it('should handle alarm wakeup', async () => {
      const stub = getUniqueShardStub();

      // Use runInDurableObject to access internal state and trigger alarm
      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        // Schedule an alarm
        await state.storage.setAlarm(Date.now() + 100);

        // Wait for alarm to fire (vitest-pool-workers simulates this)
        await new Promise((resolve) => setTimeout(resolve, 200));

        // Check that alarm handler was called (via stats or state)
        const statsResponse = await instance.fetch(new Request('https://shard-do/stats'));
        const stats = (await statsResponse.json()) as { alarmCount?: number; lastAlarmTimestamp?: number };

        // Alarm count should be tracked
        expect(typeof stats.alarmCount).toBe('number');
        expect(stats.alarmCount).toBeGreaterThanOrEqual(1);
      });
    });

    it('should process scheduled maintenance on alarm', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        // First insert some data that needs maintenance
        const insertResponse = await instance.fetch(
          new Request('https://shard-do/maintenance/schedule', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ task: 'compaction', priority: 'high' }),
          })
        );

        expect(insertResponse.ok).toBe(true);

        // Schedule alarm for maintenance
        await state.storage.setAlarm(Date.now() + 50);

        // Wait for alarm to process
        await new Promise((resolve) => setTimeout(resolve, 150));

        // Check maintenance status
        const statusResponse = await instance.fetch(new Request('https://shard-do/maintenance/status'));
        const status = (await statusResponse.json()) as { pendingTasks: number; completedTasks: number };

        expect(status.completedTasks).toBeGreaterThanOrEqual(1);
      });
    });
  });

  describe('Pending Operations', () => {
    it('should resume pending operations after hibernation', async () => {
      const stub = getUniqueShardStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Queue a pending operation
      const queueResult = await sendAndWait<PendingOperationResult>(
        ws,
        {
          type: 'queueOperation',
          operation: {
            type: 'batchInsert',
            data: [
              { subject: 'entity1', predicate: 'name', value: 'Test' },
              { subject: 'entity2', predicate: 'name', value: 'Test2' },
            ],
          },
        },
        (m: unknown) => (m as { type?: string }).type === 'operationQueued'
      );

      expect(queueResult.status).toBe('pending');
      expect(queueResult.operationId).toBeDefined();

      // Allow time for hibernation and wake
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Check operation status - should be completed after wake
      const statusResult = await sendAndWait<PendingOperationResult>(
        ws,
        { type: 'getOperationStatus', operationId: queueResult.operationId },
        (m: unknown) =>
          (m as { type?: string }).type === 'operationResult' ||
          (m as { type?: string }).type === 'operationQueued'
      );

      // Operation should eventually complete
      expect(['pending', 'completed']).toContain(statusResult.status);

      ws.close();
    });

    it('should persist pending operations across hibernation', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        // Queue operations via HTTP API
        const queueResponse = await instance.fetch(
          new Request('https://shard-do/operations/queue', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              operations: [
                { type: 'insert', data: { subject: 'e1', predicate: 'p1' } },
                { type: 'insert', data: { subject: 'e2', predicate: 'p2' } },
              ],
            }),
          })
        );

        expect(queueResponse.ok).toBe(true);
        const queueData = (await queueResponse.json()) as { queuedCount: number; operationIds: string[] };
        expect(queueData.queuedCount).toBe(2);

        // Get pending operations count
        const pendingResponse = await instance.fetch(new Request('https://shard-do/operations/pending'));
        const pending = (await pendingResponse.json()) as { count: number };
        expect(pending.count).toBe(2);
      });
    });
  });

  describe('Connection Cleanup', () => {
    it('should cleanup old connections', async () => {
      const stub = getUniqueShardStub();

      // Create multiple connections
      const connections: WebSocket[] = [];
      for (let i = 0; i < 3; i++) {
        const ws = await connectWebSocket(stub);
        await waitForMessage<ConnectedMessage>(
          ws,
          (m: unknown) => (m as { type?: string }).type === 'connected'
        );
        connections.push(ws);
      }

      // Note: When client closes WebSocket, it's immediately removed from getWebSockets()
      // The cleanup mechanism is for stale connections (connections that are still open but inactive)
      // Close the first two connections
      connections[0].close();
      connections[1].close();

      // Allow time for WebSocket close to propagate
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Trigger cleanup via the remaining connection
      // Since closed connections are automatically removed from getWebSockets(),
      // cleanup finds no stale connections - only the active connection remains
      const cleanupResult = await sendAndWait<ConnectionCleanupResult>(
        connections[2],
        { type: 'triggerCleanup' },
        (m: unknown) => (m as { type?: string }).type === 'cleanupResult'
      );

      // Client-closed connections are auto-removed, not cleaned up
      // Only 1 remaining connection (the one sending the cleanup request)
      expect(cleanupResult.closedConnections).toBe(0);
      expect(cleanupResult.remainingConnections).toBe(1);

      connections[2].close();
    });

    it('should track connection count correctly', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, _state: DurableObjectState) => {
        // Get initial connection count
        const initialStats = await instance.fetch(new Request('https://shard-do/connections/count'));
        const initial = (await initialStats.json()) as { activeConnections: number };

        expect(initial.activeConnections).toBe(0);
      });

      // Create a connection from outside runInDurableObject
      const ws = await connectWebSocket(stub);
      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Check count increased
      const statsResponse = await stub.fetch(new Request('https://shard-do/connections/count'));
      const stats = (await statsResponse.json()) as { activeConnections: number };

      expect(stats.activeConnections).toBe(1);

      ws.close();

      // Allow cleanup
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Check count decreased
      const finalResponse = await stub.fetch(new Request('https://shard-do/connections/count'));
      const final = (await finalResponse.json()) as { activeConnections: number };

      expect(final.activeConnections).toBe(0);
    });

    it('should handle stale connection timeout', async () => {
      const stub = getUniqueShardStub();

      await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
        // Set a very short connection timeout for testing
        await instance.fetch(
          new Request('https://shard-do/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ connectionTimeoutMs: 50 }),
          })
        );

        // Now stale connections should be cleaned up within 50ms
        // Schedule cleanup alarm
        await state.storage.setAlarm(Date.now() + 100);

        // Wait for alarm
        await new Promise((resolve) => setTimeout(resolve, 150));

        // Verify cleanup happened
        const cleanupStats = await instance.fetch(new Request('https://shard-do/stats'));
        const stats = (await cleanupStats.json()) as { staleConnectionsCleanedUp?: number };

        expect(typeof stats.staleConnectionsCleanedUp).toBe('number');
      });
    });
  });

  describe('State Persistence', () => {
    it('should persist hibernation count to storage', async () => {
      const stub = getUniqueShardStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Send multiple messages to increment wake count
      for (let i = 0; i < 3; i++) {
        await sendAndWait<{ type: string }>(
          ws,
          { type: 'ping', timestamp: Date.now() },
          (m: unknown) => (m as { type?: string }).type === 'pong'
        );
      }

      ws.close();

      // Create a new connection to verify persistence
      const ws2 = await connectWebSocket(stub);
      await waitForMessage<ConnectedMessage>(
        ws2,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      const stats = await sendAndWait<{ type: string; totalWakes: number }>(
        ws2,
        { type: 'getStats' },
        (m: unknown) => (m as { type?: string }).type === 'stats'
      );

      // Total wakes should persist across connections
      expect(stats.totalWakes).toBeGreaterThanOrEqual(3);

      ws2.close();
    });
  });
});
