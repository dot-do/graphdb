/**
 * Multi-Shard Routing Tests for BrokerDO
 *
 * Tests that BrokerDO correctly routes requests to different shards
 * based on FNV-1a hash of the subject/entity ID, rather than using
 * a hardcoded shard name.
 *
 * TDD RED phase - verifies hash-based routing behavior.
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { BrokerDO } from '../../src/broker/broker-do.js';
import { getShardId, routeEntity } from '../../src/snippet/router.js';
import { createEntityId, createNamespace } from '../../src/core/types.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueBrokerStub() {
  const id = env.BROKER.idFromName(`broker-routing-${Date.now()}-${testCounter++}`);
  return env.BROKER.get(id);
}

interface ConnectedMessage {
  type: 'connected';
  clientId: string;
}

interface SubrequestResult {
  type: 'subrequestResult';
  result: {
    messageId: number;
    requestedCount: number;
    successCount: number;
    failureCount: number;
    errors: string[];
    durationMs: number;
    shardId?: string; // Should include the shard ID that was targeted
  };
  metrics: {
    wakeNumber: number;
    totalSubrequestsThisSession: number;
    stateValue: number;
  };
}

/**
 * Helper to properly close WebSocket and wait for DO storage operations to complete.
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

describe('BrokerDO Multi-Shard Routing', () => {
  describe('Hash-based shard routing', () => {
    it('should route requests to shard determined by FNV-1a hash of subject', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Request with a specific subject/entity ID
      const subject = 'https://example.com/users/123';
      const expectedShardId = getShardId(createNamespace('https://example.com/users/'));

      const result = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 1, subject },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // The result should indicate which shard was used
      expect(result.result.shardId).toBe(expectedShardId);

      await closeWebSocket(ws);
    });

    it('should route different namespaces to different shards', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Two subjects with different namespaces
      const subject1 = 'https://example.com/users/123';
      const subject2 = 'https://other.com/users/456';

      const expectedShard1 = getShardId(createNamespace('https://example.com/users/'));
      const expectedShard2 = getShardId(createNamespace('https://other.com/users/'));

      // Verify the two namespaces hash to different shards
      expect(expectedShard1).not.toBe(expectedShard2);

      // Send request for first subject
      const result1 = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 1, subject: subject1 },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // Send request for second subject
      const result2 = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 2, subject: subject2 },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // Each request should route to its own shard
      expect(result1.result.shardId).toBe(expectedShard1);
      expect(result2.result.shardId).toBe(expectedShard2);

      await closeWebSocket(ws);
    });

    it('should route same namespace entities to the same shard', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Two subjects with the same namespace but different IDs
      const subject1 = 'https://example.com/users/123';
      const subject2 = 'https://example.com/users/456';

      const expectedShardId = getShardId(createNamespace('https://example.com/users/'));

      // Send request for first subject
      const result1 = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 1, subject: subject1 },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // Send request for second subject
      const result2 = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 2, subject: subject2 },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // Both requests should route to the same shard (same namespace)
      expect(result1.result.shardId).toBe(expectedShardId);
      expect(result2.result.shardId).toBe(expectedShardId);

      await closeWebSocket(ws);
    });

    it('should use routeEntity for entity-based routing', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Use the routeEntity function to get expected shard
      const entityId = createEntityId('https://api.example.com/v1/resources/abc');
      const routeInfo = routeEntity(entityId);

      const result = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 1, subject: entityId },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      expect(result.result.shardId).toBe(routeInfo.shardId);

      await closeWebSocket(ws);
    });

    it('should NOT use hardcoded shard-node-1', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Use a subject that should definitely not hash to 'shard-node-1'
      const subject = 'https://example.com/users/123';

      const result = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 1, subject },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // The shard ID should follow the pattern from getShardId
      expect(result.result.shardId).toMatch(/^shard-\d+-[a-f0-9]+$/);
      expect(result.result.shardId).not.toBe('shard-node-1');

      await closeWebSocket(ws);
    });
  });

  describe('Default routing behavior', () => {
    it('should use default shard when no subject is provided', async () => {
      const stub = getUniqueBrokerStub();
      const ws = await connectWebSocket(stub);

      await waitForMessage<ConnectedMessage>(
        ws,
        (m: unknown) => (m as { type?: string }).type === 'connected'
      );

      // Request without a subject - should use a default namespace
      const result = await sendAndWait<SubrequestResult>(
        ws,
        { subrequests: 1, messageId: 1 },
        (m: unknown) => (m as { type?: string }).type === 'subrequestResult'
      );

      // Should still have a valid shard ID (using default namespace)
      expect(result.result.shardId).toBeDefined();
      expect(result.result.shardId).toMatch(/^shard-\d+-[a-f0-9]+$/);

      await closeWebSocket(ws);
    });
  });
});
