/**
 * Client SDK Tests
 *
 * Tests for the GraphDB client SDK including:
 * - Connection management
 * - CRUD operations (insert, query, update, delete)
 * - Traversal operations
 * - Batch operations
 * - Connection state management
 */

import { describe, it, expect, afterEach, vi } from 'vitest';
import {
  createGraphClient,
  createGraphClientFromWebSocket,
  type GraphClient,
  type ConnectionState,
} from '../../src/client/index.js';

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
  private responseQueue: Array<{
    predicate: (msg: unknown) => boolean;
    response: unknown;
  }> = [];

  constructor(url: string) {
    super();
    this.url = url;
    // Auto-connect immediately
    setTimeout(() => {
      this.readyState = MockWebSocket.OPEN;
      this.dispatchEvent(new Event('open'));
    }, 0);
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
        const response = this.responseQueue[i];
        if (response && response.predicate(parsed)) {
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

  close(): void {
    this.readyState = MockWebSocket.CLOSING;
    setTimeout(() => {
      this.readyState = MockWebSocket.CLOSED;
      this.dispatchEvent(new CloseEvent('close', { code: 1000 }));
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

// Store original WebSocket for cleanup
const originalWebSocket = globalThis.WebSocket;

// ============================================================================
// Test Suite
// ============================================================================

describe('Client SDK', () => {
  afterEach(() => {
    globalThis.WebSocket = originalWebSocket;
  });

  describe('createGraphClientFromWebSocket', () => {
    it('should create client from existing WebSocket', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      // Wait for connection
      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      expect(client.isConnected()).toBe(true);
      expect(client.getState()).toBe('connected');

      client.close();
    });
  });

  describe('CRUD Operations', () => {
    it('should insert an entity', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'createEntity',
        { id: 'req-1', result: undefined }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await client.insert({
        $id: 'https://example.com/user/1',
        $type: 'User',
        name: 'Alice',
        email: 'alice@example.com',
      });

      expect(mockWs.sentMessages.length).toBe(1);
      const sent = JSON.parse(mockWs.sentMessages[0]!);
      expect(sent.method).toBe('createEntity');
      expect(sent.args[0].$id).toBe('https://example.com/user/1');
      expect(sent.args[0].name).toBe('Alice');

      client.close();
    });

    it('should query an entity by URL', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        {
          id: 'req-1',
          result: {
            $id: 'https://example.com/user/1',
            $type: 'User',
            $context: 'https://example.com/user',
            _namespace: 'https://example.com',
            _localId: '1',
            name: 'Alice',
          },
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const user = await client.query('https://example.com/user/1');

      expect(user).not.toBeNull();
      expect((user as { name: string }).name).toBe('Alice');

      client.close();
    });

    it('should update an entity', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'updateEntity',
        { id: 'req-1', result: undefined }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await client.update('https://example.com/user/1', {
        name: 'Alice Smith',
      });

      const sent = JSON.parse(mockWs.sentMessages[0]!);
      expect(sent.method).toBe('updateEntity');
      expect(sent.args[0]).toBe('https://example.com/user/1');
      expect(sent.args[1].name).toBe('Alice Smith');

      client.close();
    });

    it('should delete an entity', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'deleteEntity',
        { id: 'req-1', result: undefined }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await client.delete('https://example.com/user/1');

      const sent = JSON.parse(mockWs.sentMessages[0]!);
      expect(sent.method).toBe('deleteEntity');
      expect(sent.args[0]).toBe('https://example.com/user/1');

      client.close();
    });
  });

  describe('Traversal Operations', () => {
    it('should traverse forward relationships', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse((msg) => (msg as { method?: string }).method === 'traverse', {
        id: 'req-1',
        result: [
          { $id: 'https://example.com/user/2', $type: 'User', name: 'Bob' },
          { $id: 'https://example.com/user/3', $type: 'User', name: 'Charlie' },
        ],
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const friends = await client.traverse('https://example.com/user/1', 'friends');

      expect(friends.length).toBe(2);
      expect(friends[0]!.name).toBe('Bob');
      expect(friends[1]!.name).toBe('Charlie');

      client.close();
    });

    it('should perform path traversal', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'pathTraverse',
        {
          id: 'req-1',
          result: [
            { $id: 'https://example.com/post/1', $type: 'Post', title: 'Hello World' },
          ],
        }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const posts = await client.pathTraverse('https://example.com/user/1', [
        'friends',
        'posts',
      ]);

      expect(posts.length).toBe(1);
      expect(posts[0]!.title).toBe('Hello World');

      client.close();
    });
  });

  describe('Batch Operations', () => {
    it('should batch get entities', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse((msg) => (msg as { method?: string }).method === 'batchGet', {
        id: 'req-1',
        result: {
          successCount: 2,
          errorCount: 0,
          results: [
            { $id: 'https://example.com/user/1', $type: 'User', name: 'Alice' },
            { $id: 'https://example.com/user/2', $type: 'User', name: 'Bob' },
          ],
          errors: [],
        },
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const result = await client.batchGet([
        'https://example.com/user/1',
        'https://example.com/user/2',
      ]);

      expect(result.successCount).toBe(2);
      expect(result.results.length).toBe(2);
      expect(result.results[0]?.name).toBe('Alice');

      client.close();
    });

    it('should batch insert entities', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

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

      const result = await client.batchInsert([
        { $id: 'https://example.com/user/1', $type: 'User', name: 'Alice' },
        { $id: 'https://example.com/user/2', $type: 'User', name: 'Bob' },
      ]);

      expect(result.successCount).toBe(2);
      expect(result.errorCount).toBe(0);

      client.close();
    });
  });

  describe('Connection Management', () => {
    it('should track connection stats', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const stats = client.getStats();

      expect(stats.state).toBe('connected');
      expect(stats.connected).toBe(true);
      expect(stats.messagesSent).toBe(0);
      expect(stats.messagesReceived).toBe(0);

      client.close();
    });

    it('should return immutable stats copies', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      const stats1 = client.getStats();
      const stats2 = client.getStats();

      expect(stats1).not.toBe(stats2);
      expect(stats1).toEqual(stats2);

      stats1.messagesSent = 999;
      expect(client.getStats().messagesSent).toBe(0);

      client.close();
    });

    it('should handle disconnection', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      let disconnectReason: string | undefined;

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket, {
        onDisconnect: (reason) => {
          disconnectReason = reason;
        },
      });

      expect(client.isConnected()).toBe(true);

      mockWs.simulateDisconnect(1006, 'Connection lost');

      await new Promise((resolve) => setTimeout(resolve, 10));

      expect(client.isConnected()).toBe(false);
      expect(client.getState()).toBe('disconnected');
      expect(disconnectReason).toBe('Connection lost');

      client.close();
    });

    it('should reject pending requests on disconnection', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      // Start a request but don't respond
      const pendingRequest = client.query('https://example.com/user/1');

      // Disconnect before response
      mockWs.simulateDisconnect();

      await expect(pendingRequest).rejects.toThrow('Connection closed');

      client.close();
    });
  });

  describe('Error Handling', () => {
    it('should handle RPC errors', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-1', error: 'Entity not found' }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await expect(client.query('https://example.com/nonexistent')).rejects.toThrow(
        'Entity not found'
      );

      client.close();
    });

    it('should reject requests that cannot reconnect from external WebSocket', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await expect(client.reconnect()).rejects.toThrow(
        'Cannot reconnect: WebSocket was provided externally'
      );

      client.close();
    });
  });

  describe('Query Method', () => {
    it('should use getEntity for URL lookups', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-1', result: { $id: 'https://example.com/user/1', name: 'Alice' } }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await client.query('https://example.com/user/1');

      const sent = JSON.parse(mockWs.sentMessages[0]!);
      expect(sent.method).toBe('getEntity');

      client.close();
    });

    it('should use query for path expressions', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse((msg) => (msg as { method?: string }).method === 'query', {
        id: 'req-1',
        result: {
          entities: [{ $id: 'https://example.com/user/1', name: 'Alice' }],
          hasMore: false,
          stats: { shardQueries: 1, entitiesScanned: 1, durationMs: 5 },
        },
      });

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket);

      await client.query('user:1.friends');

      const sent = JSON.parse(mockWs.sentMessages[0]!);
      expect(sent.method).toBe('query');

      client.close();
    });
  });

  describe('Retry Logic', () => {
    it('should retry on transient errors when enableRetry is true', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      let callCount = 0;
      mockWs.queueResponse(
        (msg) => {
          callCount++;
          return (msg as { method?: string }).method === 'getEntity' && callCount === 1;
        },
        { id: 'req-1', error: 'Connection timeout' }
      );

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-2', result: { $id: 'https://example.com/user/1', name: 'Alice' } }
      );

      const onRetry = vi.fn();
      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket, {
        enableRetry: true,
        retryConfig: {
          maxRetries: 3,
          baseDelayMs: 10,
          maxDelayMs: 100,
          jitterFactor: 0,
        },
        onRetry,
      });

      const user = await client.query('https://example.com/user/1');

      expect(user).not.toBeNull();
      expect((user as { name: string }).name).toBe('Alice');
      expect(mockWs.sentMessages.length).toBe(2);
      expect(onRetry).toHaveBeenCalledWith('getEntity', 1, expect.any(Error), expect.any(Number));

      client.close();
    });

    it('should not retry non-idempotent operations', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'createEntity',
        { id: 'req-1', error: 'Connection timeout' }
      );

      const onRetry = vi.fn();
      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket, {
        enableRetry: true,
        retryConfig: {
          maxRetries: 3,
          baseDelayMs: 10,
          maxDelayMs: 100,
          jitterFactor: 0,
        },
        onRetry,
      });

      await expect(
        client.insert({
          $id: 'https://example.com/user/1',
          $type: 'User',
          name: 'Alice',
        })
      ).rejects.toThrow('Connection timeout');

      expect(mockWs.sentMessages.length).toBe(1);
      expect(onRetry).not.toHaveBeenCalled();

      client.close();
    });

    it('should not retry when enableRetry is false', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-1', error: 'Connection timeout' }
      );

      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket, {
        enableRetry: false,
      });

      await expect(client.query('https://example.com/user/1')).rejects.toThrow(
        'Connection timeout'
      );

      expect(mockWs.sentMessages.length).toBe(1);

      client.close();
    });

    it('should not retry non-transient errors even when enableRetry is true', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      mockWs.queueResponse(
        (msg) => (msg as { method?: string }).method === 'getEntity',
        { id: 'req-1', error: 'Entity not found' }
      );

      const onRetry = vi.fn();
      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket, {
        enableRetry: true,
        retryConfig: {
          maxRetries: 3,
          baseDelayMs: 10,
        },
        onRetry,
      });

      await expect(client.query('https://example.com/user/1')).rejects.toThrow(
        'Entity not found'
      );

      expect(mockWs.sentMessages.length).toBe(1);
      expect(onRetry).not.toHaveBeenCalled();

      client.close();
    });

    it('should fail after exhausting all retries', async () => {
      const mockWs = new MockWebSocket('wss://example.com/graph');

      await new Promise<void>((resolve) => {
        mockWs.addEventListener('open', () => resolve());
      });

      // All requests will return timeout error
      for (let i = 0; i < 4; i++) {
        mockWs.queueResponse(
          (msg) => (msg as { method?: string }).method === 'getEntity',
          { id: `req-${i + 1}`, error: 'Connection timeout' }
        );
      }

      const onRetry = vi.fn();
      const client = createGraphClientFromWebSocket(mockWs as unknown as WebSocket, {
        enableRetry: true,
        retryConfig: {
          maxRetries: 3,
          baseDelayMs: 10,
          maxDelayMs: 50,
          jitterFactor: 0,
        },
        onRetry,
      });

      await expect(client.query('https://example.com/user/1')).rejects.toThrow(
        'Connection timeout'
      );

      expect(mockWs.sentMessages.length).toBe(4); // 1 initial + 3 retries
      expect(onRetry).toHaveBeenCalledTimes(3);

      client.close();
    });
  });
});
