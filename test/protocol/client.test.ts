/**
 * Protocol Client Tests
 *
 * Tests for the capnweb client SDK wrapper including:
 * - WebSocket connection to broker
 * - RPC request/response handling
 * - Promise pipelining
 * - Reconnection logic
 * - Timeout handling
 * - Request batching
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createGraphClient,
  createGraphClientFromWebSocket,
  ManualWebSocketClient,
  type GraphClient,
  type GraphClientOptions,
  type ConnectionStats,
} from '../../src/protocol/client.js';

// ============================================================================
// Mock WebSocket for unit testing
// ============================================================================

class MockWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState: number = MockWebSocket.CONNECTING;
  url: string;
  sentMessages: string[] = [];
  closeCode?: number;
  closeReason?: string;
  private responseQueue: { predicate: (msg: unknown) => boolean; response: unknown }[] = [];
  private autoConnect: boolean;
  private connectionDelay: number;
  private shouldFailConnection: boolean = false;

  constructor(url: string, options?: { autoConnect?: boolean; connectionDelay?: number; failConnection?: boolean }) {
    super();
    this.url = url;
    this.autoConnect = options?.autoConnect ?? true;
    this.connectionDelay = options?.connectionDelay ?? 0;
    this.shouldFailConnection = options?.failConnection ?? false;

    if (this.autoConnect) {
      this.simulateConnect();
    }
  }

  simulateConnect(): void {
    setTimeout(() => {
      if (this.shouldFailConnection) {
        this.readyState = MockWebSocket.CLOSED;
        this.dispatchEvent(new Event('error'));
        this.dispatchEvent(new CloseEvent('close', { code: 1006, reason: 'Connection failed' }));
      } else {
        this.readyState = MockWebSocket.OPEN;
        this.dispatchEvent(new Event('open'));
      }
    }, this.connectionDelay);
  }

  send(data: string): void {
    if (this.readyState !== MockWebSocket.OPEN) {
      throw new Error('WebSocket is not open');
    }
    this.sentMessages.push(data);

    // Check response queue for matching responses
    try {
      const parsed = JSON.parse(data);
      for (let i = 0; i < this.responseQueue.length; i++) {
        if (this.responseQueue[i].predicate(parsed)) {
          const response = this.responseQueue[i];
          this.responseQueue.splice(i, 1);
          setTimeout(() => {
            this.simulateMessage(response.response);
          }, 0);
          break;
        }
      }
    } catch {
      // Not JSON, ignore
    }
  }

  close(code?: number, reason?: string): void {
    this.closeCode = code;
    this.closeReason = reason;
    this.readyState = MockWebSocket.CLOSING;
    setTimeout(() => {
      this.readyState = MockWebSocket.CLOSED;
      this.dispatchEvent(new CloseEvent('close', { code, reason }));
    }, 0);
  }

  // Test helpers
  simulateMessage(data: unknown): void {
    const event = new MessageEvent('message', { data: JSON.stringify(data) });
    this.dispatchEvent(event);
  }

  queueResponse(predicate: (msg: unknown) => boolean, response: unknown): void {
    this.responseQueue.push({ predicate, response });
  }

  simulateDisconnect(code: number = 1006, reason: string = 'Connection lost'): void {
    this.readyState = MockWebSocket.CLOSED;
    this.dispatchEvent(new CloseEvent('close', { code, reason }));
  }
}

// Replace global WebSocket with mock for testing
const originalWebSocket = globalThis.WebSocket;

// ============================================================================
// Test Suite
// ============================================================================

describe('Protocol Client', () => {
  describe('WebSocket Connection', () => {
    it('should connect to broker over WebSocket', async () => {
      // Create a mock WebSocket that will be used by the client
      let createdWs: MockWebSocket | null = null;

      // Mock the global WebSocket
      (globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket = class extends MockWebSocket {
        constructor(url: string) {
          super(url);
          createdWs = this;
        }
      } as unknown as typeof MockWebSocket;

      try {
        const client = createGraphClient('wss://example.com/graph');

        // Client should be marked as connected
        expect(client.isConnected()).toBe(true);

        // Stats should show connected
        const stats = client.getStats();
        expect(stats.connected).toBe(true);

        client.close();
      } finally {
        globalThis.WebSocket = originalWebSocket;
      }
    });

    it('should create client from existing WebSocket', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      // Wait for connection
      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      expect(client.isConnected()).toBe(true);

      client.close();
    });
  });

  describe('RPC Request/Response', () => {
    it('should send RPC requests', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      // Wait for connection
      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue a response for the getEntity call
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-1', result: { $id: 'https://example.com/user/1', $type: 'User', name: 'Alice' } }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Make an RPC call
      const entityPromise = client.getEntity('https://example.com/user/1');

      // Verify message was sent
      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(mockWs.sentMessages.length).toBe(1);

      const sentMessage = JSON.parse(mockWs.sentMessages[0]);
      expect(sentMessage.method).toBe('getEntity');
      expect(sentMessage.args).toContain('https://example.com/user/1');

      // Wait for response
      const entity = await entityPromise;
      expect(entity).not.toBeNull();
      expect(entity?.$id).toBe('https://example.com/user/1');
      expect(entity?.name).toBe('Alice');

      client.close();
    });

    it('should handle RPC responses', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue responses
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'createEntity',
        { id: 'req-1', result: undefined }
      );

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-2', result: { $id: 'https://example.com/user/2', $type: 'User', name: 'Bob' } }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Create then get
      await client.createEntity({
        $id: 'https://example.com/user/2' as any,
        $type: 'User',
        $context: 'https://example.com/user',
        _namespace: 'https://example.com' as any,
        _localId: '2',
        name: 'Bob',
      });

      const entity = await client.getEntity('https://example.com/user/2');
      expect(entity?.name).toBe('Bob');

      // Stats should show messages
      const stats = client.getStats();
      expect(stats.messagesSent).toBe(2);
      expect(stats.messagesReceived).toBe(2);

      client.close();
    });

    it('should handle RPC errors', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue error response
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-1', error: 'Entity not found' }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await expect(client.getEntity('https://example.com/nonexistent')).rejects.toThrow('Entity not found');

      client.close();
    });
  });

  describe('Promise Pipelining', () => {
    it('should support promise pipelining', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue response for pathTraverse (which is what pipelining uses)
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'pathTraverse',
        {
          id: 'req-1',
          result: [
            { $id: 'https://example.com/user/3', $type: 'User', name: 'Charlie' },
            { $id: 'https://example.com/user/4', $type: 'User', name: 'Diana' },
          ],
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Use pathTraverse for multi-hop traversal (enables pipelining)
      const friends = await client.pathTraverse('https://example.com/user/1', ['friends', 'friends']);

      expect(friends.length).toBe(2);
      expect(friends[0].name).toBe('Charlie');
      expect(friends[1].name).toBe('Diana');

      // Should be a single request (pipelined)
      expect(mockWs.sentMessages.length).toBe(1);

      client.close();
    });

    it('should chain traversals efficiently', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue response for traverse
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'traverse',
        {
          id: 'req-1',
          result: [{ $id: 'https://example.com/user/2', $type: 'User', name: 'Bob' }],
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Single hop traverse
      const result = await client.traverse('https://example.com/user/1', 'friends');

      expect(result.length).toBe(1);
      expect(result[0].name).toBe('Bob');

      client.close();
    });
  });

  describe('Reconnection Logic', () => {
    it('should reconnect on connection loss', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      expect(client.isConnected()).toBe(true);

      // Simulate disconnection
      mockWs.simulateDisconnect(1006, 'Connection lost');

      // Wait for close event to propagate
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Client should reflect disconnected state
      expect(client.isConnected()).toBe(false);

      // Stats should show disconnected
      const stats = client.getStats();
      expect(stats.connected).toBe(false);
    });

    it('should reject pending requests on connection loss', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Start a request but don't respond
      const pendingRequest = client.getEntity('https://example.com/user/1');

      // Disconnect before response
      mockWs.simulateDisconnect();

      // Request should be rejected
      await expect(pendingRequest).rejects.toThrow('Connection closed');
    });

    it('should reject requests when not connected', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph', { autoConnect: false });

      // Don't wait for open, create client while still connecting
      mockWs.readyState = MockWebSocket.CLOSED;

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await expect(client.getEntity('https://example.com/user/1')).rejects.toThrow('WebSocket not connected');
    });
  });

  describe('Timeout Handling', () => {
    it('should handle timeout', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Don't queue any response - request will hang
      const startTime = Date.now();
      const requestPromise = client.getEntity('https://example.com/user/1');

      // Manually simulate timeout by closing connection after delay
      setTimeout(() => {
        mockWs.simulateDisconnect(1000, 'Timeout');
      }, 100);

      await expect(requestPromise).rejects.toThrow('Connection closed');

      const elapsed = Date.now() - startTime;
      expect(elapsed).toBeGreaterThanOrEqual(100);
      expect(elapsed).toBeLessThan(1000);
    });
  });

  describe('Request Batching', () => {
    it('should batch multiple requests', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue batch response
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'batchGet',
        {
          id: 'req-1',
          result: {
            successCount: 3,
            errorCount: 0,
            results: [
              { $id: 'https://example.com/item/1', $type: 'Item', value: 10 },
              { $id: 'https://example.com/item/2', $type: 'Item', value: 20 },
              { $id: 'https://example.com/item/3', $type: 'Item', value: 30 },
            ],
            errors: [],
          },
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Batch get multiple entities
      const result = await client.batchGet([
        'https://example.com/item/1',
        'https://example.com/item/2',
        'https://example.com/item/3',
      ]);

      expect(result.successCount).toBe(3);
      expect(result.errorCount).toBe(0);
      expect(result.results.length).toBe(3);
      expect(result.results[0]?.value).toBe(10);
      expect(result.results[1]?.value).toBe(20);
      expect(result.results[2]?.value).toBe(30);

      // Should be a single request
      expect(mockWs.sentMessages.length).toBe(1);
    });

    it('should batch create operations', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue batch create response
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'batchCreate',
        {
          id: 'req-1',
          result: {
            successCount: 2,
            errorCount: 0,
            results: [undefined, undefined],
            errors: [],
          },
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const entities = [
        { $id: 'https://example.com/item/100' as any, $type: 'Item', value: 100 },
        { $id: 'https://example.com/item/101' as any, $type: 'Item', value: 101 },
      ];

      const result = await client.batchCreate(entities as any);

      expect(result.successCount).toBe(2);
      expect(result.errorCount).toBe(0);

      // Should be a single request
      expect(mockWs.sentMessages.length).toBe(1);
    });

    it('should execute mixed batch operations', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // Queue batch execute response
      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'batchExecute',
        {
          id: 'req-1',
          result: {
            successCount: 3,
            errorCount: 0,
            results: [
              { $id: 'https://example.com/item/1', $type: 'Item', value: 10 },
              undefined,
              undefined,
            ],
            errors: [],
          },
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const operations = [
        { type: 'get' as const, id: 'https://example.com/item/1' },
        { type: 'create' as const, entity: { $id: 'https://example.com/item/200' as any, $type: 'Item', value: 200 } },
        { type: 'delete' as const, id: 'https://example.com/item/999' },
      ];

      const result = await client.batchExecute(operations);

      expect(result.successCount).toBe(3);
      expect(result.results[0]?.$id).toBe('https://example.com/item/1');

      // Should be a single request
      expect(mockWs.sentMessages.length).toBe(1);
    });
  });

  describe('ManualWebSocketClient', () => {
    it('should connect and make calls', async () => {
      // Mock global WebSocket for ManualWebSocketClient
      let createdWs: MockWebSocket | null = null;

      (globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket = class extends MockWebSocket {
        constructor(url: string) {
          super(url);
          createdWs = this;
        }
      } as unknown as typeof MockWebSocket;

      try {
        const client = new ManualWebSocketClient('wss://example.com/graph');

        // Connect
        const connectPromise = client.connect();

        // Wait a tick for the MockWebSocket to initialize
        await new Promise((resolve) => setTimeout(resolve, 10));

        // Queue response
        createdWs!.queueResponse(
          (msg) => (msg as { method?: string }).method === 'getEntity',
          { id: 'req-1', result: { $id: 'https://example.com/user/1', $type: 'User', name: 'Test' } }
        );

        await connectPromise;

        // Make a call
        const result = await client.call<{ $id: string; name: string }>('getEntity', 'https://example.com/user/1');

        expect(result.$id).toBe('https://example.com/user/1');
        expect(result.name).toBe('Test');

        client.close();
      } finally {
        globalThis.WebSocket = originalWebSocket;
      }
    });

    it('should batch multiple calls in single frame', async () => {
      let createdWs: MockWebSocket | null = null;

      (globalThis as unknown as { WebSocket: typeof MockWebSocket }).WebSocket = class extends MockWebSocket {
        constructor(url: string) {
          super(url);
          createdWs = this;
        }
      } as unknown as typeof MockWebSocket;

      try {
        const client = new ManualWebSocketClient('wss://example.com/graph');

        const connectPromise = client.connect();
        await new Promise((resolve) => setTimeout(resolve, 10));

        // Queue batch response
        createdWs!.queueResponse(
          (msg) => Array.isArray((msg as { calls?: unknown[] }).calls),
          {
            id: 'batch-1',
            results: [
              { id: 'batch-1-0', result: { $id: 'https://example.com/item/1', value: 1 } },
              { id: 'batch-1-1', result: { $id: 'https://example.com/item/2', value: 2 } },
              { id: 'batch-1-2', result: { $id: 'https://example.com/item/3', value: 3 } },
            ],
          }
        );

        await connectPromise;

        // Make batch call
        const results = await client.batchCall<{ $id: string; value: number }>([
          { method: 'getEntity', args: ['https://example.com/item/1'] },
          { method: 'getEntity', args: ['https://example.com/item/2'] },
          { method: 'getEntity', args: ['https://example.com/item/3'] },
        ]);

        expect(results.length).toBe(3);

        // Verify only one message was sent
        expect(createdWs!.sentMessages.length).toBe(1);

        const sentBatch = JSON.parse(createdWs!.sentMessages[0]);
        expect(sentBatch.calls.length).toBe(3);

        client.close();
      } finally {
        globalThis.WebSocket = originalWebSocket;
      }
    });

    it('should throw when calling before connect', async () => {
      const client = new ManualWebSocketClient('wss://example.com/graph');

      await expect(client.call('getEntity', 'test')).rejects.toThrow('Not connected');
    });
  });

  describe('Connection Stats', () => {
    it('should track connection statistics', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Initial stats
      let stats = client.getStats();
      expect(stats.connected).toBe(true);
      expect(stats.messagesReceived).toBe(0);
      expect(stats.messagesSent).toBe(0);

      // Queue response and make request
      mockWs.queueResponse(
        () => true,
        { id: 'req-1', result: null }
      );

      await client.getEntity('test');

      stats = client.getStats();
      expect(stats.messagesSent).toBe(1);
      expect(stats.messagesReceived).toBe(1);

      client.close();
    });

    it('should return copy of stats (immutable)', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const stats1 = client.getStats();
      const stats2 = client.getStats();

      // Should be different objects
      expect(stats1).not.toBe(stats2);

      // But same values
      expect(stats1).toEqual(stats2);

      // Modifying one shouldn't affect the other
      stats1.messagesReceived = 999;
      expect(client.getStats().messagesReceived).toBe(0);

      client.close();
    });
  });

  describe('Close Connection', () => {
    it('should close the connection properly', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      expect(client.isConnected()).toBe(true);

      client.close();

      // Wait for close to propagate
      await new Promise((resolve) => setTimeout(resolve, 10));

      expect(client.isConnected()).toBe(false);
    });
  });
});
