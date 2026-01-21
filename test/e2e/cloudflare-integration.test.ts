/**
 * E2E Integration Tests Against Real Cloudflare Environment
 *
 * These tests run against a real deployed GraphDB worker on Cloudflare.
 * They are skipped by default if credentials are not present.
 *
 * ## Environment Variables Required
 *
 * - CLOUDFLARE_API_TOKEN: Cloudflare API token with Workers permissions
 * - CLOUDFLARE_ACCOUNT_ID: Cloudflare account ID
 * - GRAPHDB_E2E_URL: WebSocket URL of deployed GraphDB worker
 *   (default: wss://graphdb.workers.do/v1)
 *
 * ## Running E2E Tests
 *
 * 1. Set up environment variables:
 *    ```bash
 *    export CLOUDFLARE_API_TOKEN="your-api-token"
 *    export CLOUDFLARE_ACCOUNT_ID="your-account-id"
 *    export GRAPHDB_E2E_URL="wss://graphdb.workers.do/v1"
 *    ```
 *
 * 2. Run E2E tests only:
 *    ```bash
 *    npx vitest run test/e2e/cloudflare-integration.test.ts
 *    ```
 *
 * 3. Run with verbose output:
 *    ```bash
 *    npx vitest run test/e2e/cloudflare-integration.test.ts --reporter=verbose
 *    ```
 *
 * ## CI Configuration
 *
 * In CI, these tests will be automatically skipped unless the required
 * environment variables are set. Add secrets to your CI configuration:
 *
 * GitHub Actions example:
 * ```yaml
 * env:
 *   CLOUDFLARE_API_TOKEN: ${{ secrets.CLOUDFLARE_API_TOKEN }}
 *   CLOUDFLARE_ACCOUNT_ID: ${{ secrets.CLOUDFLARE_ACCOUNT_ID }}
 *   GRAPHDB_E2E_URL: ${{ secrets.GRAPHDB_E2E_URL }}
 * ```
 *
 * ## Test Isolation
 *
 * Each test uses unique entity IDs with timestamps to prevent conflicts
 * when running tests concurrently or repeatedly. Tests clean up after
 * themselves by deleting created entities.
 *
 * ## Timeouts
 *
 * Network operations have longer timeouts (30s) to account for cold starts
 * and network latency in real Cloudflare environments.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';

// ============================================================================
// Environment Detection
// ============================================================================

const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;
const CLOUDFLARE_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const GRAPHDB_E2E_URL = process.env.GRAPHDB_E2E_URL || 'wss://graphdb.workers.do/v1';

/**
 * Check if E2E credentials are available.
 * Tests will be skipped if any required credential is missing.
 */
function hasE2ECredentials(): boolean {
  return !!(CLOUDFLARE_API_TOKEN && CLOUDFLARE_ACCOUNT_ID);
}

/**
 * Skip message for missing credentials.
 */
const SKIP_MESSAGE = 'E2E tests skipped: Missing CLOUDFLARE_API_TOKEN or CLOUDFLARE_ACCOUNT_ID environment variables';

// ============================================================================
// Test Utilities
// ============================================================================

/**
 * Generate a unique test ID to prevent conflicts between test runs.
 */
function generateTestId(): string {
  return `e2e-${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;
}

/**
 * Generate a unique entity URL for test isolation.
 */
function generateEntityUrl(prefix: string, testId: string): string {
  return `https://e2e-test.graphdb.workers.do/${prefix}/${testId}`;
}

/**
 * Sleep for a specified number of milliseconds.
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// WebSocket Client for E2E Tests
// ============================================================================

interface RpcMessage {
  id: string;
  method: string;
  args: unknown[];
}

interface RpcResponse {
  id: string;
  result?: unknown;
  error?: string;
}

/**
 * Simple WebSocket RPC client for E2E testing.
 * This is a minimal implementation for testing purposes.
 */
class E2EClient {
  private ws: WebSocket | null = null;
  private requestId = 0;
  private pendingRequests = new Map<
    string,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();
  private connectionPromise: Promise<void> | null = null;
  private connected = false;

  constructor(private url: string) {}

  /**
   * Connect to the GraphDB WebSocket endpoint.
   */
  async connect(timeoutMs = 30000): Promise<void> {
    if (this.connected && this.ws?.readyState === WebSocket.OPEN) {
      return;
    }

    if (this.connectionPromise) {
      return this.connectionPromise;
    }

    this.connectionPromise = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Connection timeout after ${timeoutMs}ms`));
      }, timeoutMs);

      // Use native WebSocket (Node.js 18+ or browser)
      this.ws = new WebSocket(this.url);

      this.ws.onopen = () => {
        clearTimeout(timeout);
        this.connected = true;
        this.connectionPromise = null;
        resolve();
      };

      this.ws.onerror = (event) => {
        clearTimeout(timeout);
        this.connectionPromise = null;
        reject(new Error(`WebSocket error: ${event}`));
      };

      this.ws.onclose = (event) => {
        this.connected = false;
        this.connectionPromise = null;
        // Reject all pending requests
        for (const [, pending] of this.pendingRequests) {
          pending.reject(new Error(`Connection closed: ${event.code} ${event.reason}`));
        }
        this.pendingRequests.clear();
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string) as RpcResponse;
          const pending = this.pendingRequests.get(data.id);
          if (pending) {
            this.pendingRequests.delete(data.id);
            if (data.error) {
              pending.reject(new Error(data.error));
            } else {
              pending.resolve(data.result);
            }
          }
        } catch (e) {
          // Ignore parse errors for non-RPC messages
        }
      };
    });

    return this.connectionPromise;
  }

  /**
   * Send an RPC call and wait for response.
   */
  async call<T>(method: string, ...args: unknown[]): Promise<T> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      await this.connect();
    }

    const id = `req-${++this.requestId}`;

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`Request timeout after 30000ms for method: ${method}`));
      }, 30000);

      this.pendingRequests.set(id, {
        resolve: (value) => {
          clearTimeout(timeout);
          resolve(value as T);
        },
        reject: (error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });

      const message: RpcMessage = { id, method, args };
      this.ws!.send(JSON.stringify(message));
    });
  }

  /**
   * Close the connection.
   */
  close(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.connected = false;
  }

  /**
   * Check if connected.
   */
  isConnected(): boolean {
    return this.connected && this.ws?.readyState === WebSocket.OPEN;
  }
}

// ============================================================================
// Entity Types for E2E Tests
// ============================================================================

interface E2EEntity {
  $id: string;
  $type: string | string[];
  $context?: string;
  _namespace?: string;
  _localId?: string;
  [key: string]: unknown;
}

// ============================================================================
// E2E Test Suite
// ============================================================================

describe.skipIf(!hasE2ECredentials())('E2E: Cloudflare Integration', () => {
  let client: E2EClient;
  let testId: string;
  const createdEntityIds: string[] = [];

  beforeAll(async () => {
    console.log(`\n[E2E] Connecting to: ${GRAPHDB_E2E_URL}`);
    console.log(`[E2E] Account ID: ${CLOUDFLARE_ACCOUNT_ID?.substring(0, 8)}...`);

    client = new E2EClient(GRAPHDB_E2E_URL);
    await client.connect();

    console.log('[E2E] Connected successfully');
  }, 60000); // 60s timeout for connection (cold start)

  afterAll(async () => {
    // Clean up all created entities
    console.log(`\n[E2E] Cleaning up ${createdEntityIds.length} test entities...`);

    for (const entityId of createdEntityIds) {
      try {
        await client.call('deleteEntity', entityId);
      } catch (e) {
        // Ignore deletion errors during cleanup
        console.log(`[E2E] Warning: Failed to delete ${entityId}`);
      }
    }

    client.close();
    console.log('[E2E] Cleanup complete');
  }, 60000);

  beforeEach(() => {
    testId = generateTestId();
  });

  // --------------------------------------------------------------------------
  // Connection Tests
  // --------------------------------------------------------------------------

  describe('Connection', () => {
    it('should maintain WebSocket connection', () => {
      expect(client.isConnected()).toBe(true);
    });

    it('should handle ping/pong', async () => {
      // The broker should respond to ping messages
      // This tests basic RPC connectivity
      const start = Date.now();

      try {
        // Try a simple getEntity call that should return quickly (even if entity doesn't exist)
        await client.call('getEntity', 'https://nonexistent.test/entity');
      } catch (e) {
        // Entity not found is expected, but the RPC round-trip should work
      }

      const latency = Date.now() - start;
      console.log(`[E2E] RPC round-trip latency: ${latency}ms`);

      // Should complete within reasonable time
      expect(latency).toBeLessThan(10000);
    }, 30000);
  });

  // --------------------------------------------------------------------------
  // CRUD Operations
  // --------------------------------------------------------------------------

  describe('CRUD Operations', () => {
    it('should create a new entity', async () => {
      const entityId = generateEntityUrl('user', testId);
      createdEntityIds.push(entityId);

      const entity: E2EEntity = {
        $id: entityId,
        $type: 'User',
        name: 'E2E Test User',
        email: `e2e-${testId}@test.graphdb.workers.do`,
        createdAt: new Date().toISOString(),
      };

      // Create entity
      await client.call('createEntity', entity);

      // Verify creation by reading it back
      const retrieved = await client.call<E2EEntity | null>('getEntity', entityId);

      expect(retrieved).not.toBeNull();
      expect(retrieved!.$id).toBe(entityId);
      expect(retrieved!.name).toBe('E2E Test User');
      expect(retrieved!.email).toBe(`e2e-${testId}@test.graphdb.workers.do`);
    }, 30000);

    it('should read an existing entity', async () => {
      const entityId = generateEntityUrl('user', testId);
      createdEntityIds.push(entityId);

      // Create entity first
      await client.call('createEntity', {
        $id: entityId,
        $type: 'User',
        name: 'Read Test User',
      });

      // Read it back
      const entity = await client.call<E2EEntity | null>('getEntity', entityId);

      expect(entity).not.toBeNull();
      expect(entity!.$id).toBe(entityId);
      expect(entity!.name).toBe('Read Test User');
    }, 30000);

    it('should return null for non-existent entity', async () => {
      const nonExistentId = generateEntityUrl('nonexistent', testId);

      const entity = await client.call<E2EEntity | null>('getEntity', nonExistentId);

      expect(entity).toBeNull();
    }, 30000);

    it('should update an existing entity', async () => {
      const entityId = generateEntityUrl('user', testId);
      createdEntityIds.push(entityId);

      // Create entity
      await client.call('createEntity', {
        $id: entityId,
        $type: 'User',
        name: 'Original Name',
        status: 'active',
      });

      // Update entity
      await client.call('updateEntity', entityId, {
        name: 'Updated Name',
        updatedAt: new Date().toISOString(),
      });

      // Verify update
      const updated = await client.call<E2EEntity | null>('getEntity', entityId);

      expect(updated).not.toBeNull();
      expect(updated!.name).toBe('Updated Name');
      expect(updated!.status).toBe('active'); // Original field preserved
      expect(updated!.updatedAt).toBeDefined();
    }, 30000);

    it('should delete an entity', async () => {
      const entityId = generateEntityUrl('user', testId);
      // Don't add to createdEntityIds since we're deleting it

      // Create entity
      await client.call('createEntity', {
        $id: entityId,
        $type: 'User',
        name: 'To Be Deleted',
      });

      // Verify it exists
      const beforeDelete = await client.call<E2EEntity | null>('getEntity', entityId);
      expect(beforeDelete).not.toBeNull();

      // Delete entity
      await client.call('deleteEntity', entityId);

      // Verify deletion
      const afterDelete = await client.call<E2EEntity | null>('getEntity', entityId);
      expect(afterDelete).toBeNull();
    }, 30000);
  });

  // --------------------------------------------------------------------------
  // Graph Traversal Tests
  // --------------------------------------------------------------------------

  describe('Graph Traversal', () => {
    it('should traverse forward relationships', async () => {
      const aliceId = generateEntityUrl('user/alice', testId);
      const bobId = generateEntityUrl('user/bob', testId);
      const charlieId = generateEntityUrl('user/charlie', testId);
      createdEntityIds.push(aliceId, bobId, charlieId);

      // Create users
      await client.call('createEntity', {
        $id: aliceId,
        $type: 'User',
        name: 'Alice',
        friends: [bobId, charlieId],
      });

      await client.call('createEntity', {
        $id: bobId,
        $type: 'User',
        name: 'Bob',
      });

      await client.call('createEntity', {
        $id: charlieId,
        $type: 'User',
        name: 'Charlie',
      });

      // Traverse friends
      const friends = await client.call<E2EEntity[]>('traverse', aliceId, 'friends');

      expect(Array.isArray(friends)).toBe(true);
      expect(friends.length).toBe(2);

      const friendNames = friends.map((f) => f.name).sort();
      expect(friendNames).toEqual(['Bob', 'Charlie']);
    }, 60000);

    it('should handle multi-hop traversal', async () => {
      const user1Id = generateEntityUrl('user/1', testId);
      const user2Id = generateEntityUrl('user/2', testId);
      const user3Id = generateEntityUrl('user/3', testId);
      const postId = generateEntityUrl('post/1', testId);
      createdEntityIds.push(user1Id, user2Id, user3Id, postId);

      // Create graph: user1 -> user2 -> user3 -> post
      await client.call('createEntity', {
        $id: user1Id,
        $type: 'User',
        name: 'User 1',
        follows: [user2Id],
      });

      await client.call('createEntity', {
        $id: user2Id,
        $type: 'User',
        name: 'User 2',
        follows: [user3Id],
      });

      await client.call('createEntity', {
        $id: user3Id,
        $type: 'User',
        name: 'User 3',
        posts: [postId],
      });

      await client.call('createEntity', {
        $id: postId,
        $type: 'Post',
        title: 'Test Post',
        content: 'E2E test content',
      });

      // 3-hop traversal: user1 -> follows -> follows -> posts
      const posts = await client.call<E2EEntity[]>(
        'pathTraverse',
        user1Id,
        ['follows', 'follows', 'posts']
      );

      expect(Array.isArray(posts)).toBe(true);
      expect(posts.length).toBe(1);
      expect(posts[0]!.title).toBe('Test Post');
    }, 60000);

    it('should handle reverse traversal', async () => {
      const targetId = generateEntityUrl('user/target', testId);
      const follower1Id = generateEntityUrl('user/follower1', testId);
      const follower2Id = generateEntityUrl('user/follower2', testId);
      createdEntityIds.push(targetId, follower1Id, follower2Id);

      // Create target user
      await client.call('createEntity', {
        $id: targetId,
        $type: 'User',
        name: 'Target User',
      });

      // Create followers pointing to target
      await client.call('createEntity', {
        $id: follower1Id,
        $type: 'User',
        name: 'Follower 1',
        follows: [targetId],
      });

      await client.call('createEntity', {
        $id: follower2Id,
        $type: 'User',
        name: 'Follower 2',
        follows: [targetId],
      });

      // Reverse traverse to find followers
      const followers = await client.call<E2EEntity[]>('reverseTraverse', targetId, 'follows');

      expect(Array.isArray(followers)).toBe(true);
      expect(followers.length).toBe(2);

      const followerNames = followers.map((f) => f.name).sort();
      expect(followerNames).toEqual(['Follower 1', 'Follower 2']);
    }, 60000);
  });

  // --------------------------------------------------------------------------
  // Query Tests
  // --------------------------------------------------------------------------

  describe('Query Operations', () => {
    it('should execute simple query', async () => {
      const entityId = generateEntityUrl('doc', testId);
      createdEntityIds.push(entityId);

      // Create document
      await client.call('createEntity', {
        $id: entityId,
        $type: 'Document',
        title: 'E2E Query Test',
        content: 'This is a test document for E2E query testing.',
        tags: ['e2e', 'test', 'query'],
      });

      // Query for the entity by ID
      const result = await client.call<E2EEntity | null>('getEntity', entityId);

      expect(result).not.toBeNull();
      expect(result!.title).toBe('E2E Query Test');
      expect(result!.tags).toEqual(['e2e', 'test', 'query']);
    }, 30000);

    it('should handle batch get', async () => {
      const entity1Id = generateEntityUrl('item/1', testId);
      const entity2Id = generateEntityUrl('item/2', testId);
      const entity3Id = generateEntityUrl('item/nonexistent', testId);
      createdEntityIds.push(entity1Id, entity2Id);

      // Create entities
      await client.call('createEntity', {
        $id: entity1Id,
        $type: 'Item',
        name: 'Item 1',
      });

      await client.call('createEntity', {
        $id: entity2Id,
        $type: 'Item',
        name: 'Item 2',
      });

      // Batch get including a non-existent entity
      const result = await client.call<{
        results: (E2EEntity | null)[];
        successCount: number;
        errorCount: number;
      }>('batchGet', [entity1Id, entity2Id, entity3Id]);

      expect(result.successCount).toBe(2);
      expect(result.results[0]?.name).toBe('Item 1');
      expect(result.results[1]?.name).toBe('Item 2');
      expect(result.results[2]).toBeNull();
    }, 30000);
  });

  // --------------------------------------------------------------------------
  // Full Flow Integration Test
  // --------------------------------------------------------------------------

  describe('Full Flow Integration', () => {
    it('should complete full CRUD cycle: create -> traverse -> query -> delete', async () => {
      console.log(`\n[E2E] Starting full flow test with ID: ${testId}`);

      // Step 1: Create a user with posts
      const userId = generateEntityUrl('user/full-flow', testId);
      const post1Id = generateEntityUrl('post/1', testId);
      const post2Id = generateEntityUrl('post/2', testId);
      createdEntityIds.push(userId, post1Id, post2Id);

      console.log('[E2E] Step 1: Creating entities...');

      await client.call('createEntity', {
        $id: userId,
        $type: 'User',
        name: 'Full Flow User',
        email: 'fullflow@test.graphdb.workers.do',
        posts: [post1Id, post2Id],
        createdAt: new Date().toISOString(),
      });

      await client.call('createEntity', {
        $id: post1Id,
        $type: 'Post',
        title: 'First Post',
        content: 'Content of first post',
        author: userId,
      });

      await client.call('createEntity', {
        $id: post2Id,
        $type: 'Post',
        title: 'Second Post',
        content: 'Content of second post',
        author: userId,
      });

      console.log('[E2E] Step 1 complete: Created 3 entities');

      // Step 2: Traverse to get user's posts
      console.log('[E2E] Step 2: Traversing relationships...');

      const posts = await client.call<E2EEntity[]>('traverse', userId, 'posts');

      expect(posts.length).toBe(2);
      const postTitles = posts.map((p) => p.title).sort();
      expect(postTitles).toEqual(['First Post', 'Second Post']);

      console.log('[E2E] Step 2 complete: Found 2 posts via traversal');

      // Step 3: Query and verify user
      console.log('[E2E] Step 3: Querying user...');

      const user = await client.call<E2EEntity | null>('getEntity', userId);

      expect(user).not.toBeNull();
      expect(user!.name).toBe('Full Flow User');
      expect(user!.email).toBe('fullflow@test.graphdb.workers.do');

      console.log('[E2E] Step 3 complete: User query verified');

      // Step 4: Update user
      console.log('[E2E] Step 4: Updating user...');

      await client.call('updateEntity', userId, {
        name: 'Updated Full Flow User',
        bio: 'Added bio field',
      });

      const updatedUser = await client.call<E2EEntity | null>('getEntity', userId);
      expect(updatedUser!.name).toBe('Updated Full Flow User');
      expect(updatedUser!.bio).toBe('Added bio field');

      console.log('[E2E] Step 4 complete: User updated');

      // Step 5: Delete entities (in reverse order to avoid orphan references)
      console.log('[E2E] Step 5: Deleting entities...');

      await client.call('deleteEntity', post2Id);
      await client.call('deleteEntity', post1Id);
      await client.call('deleteEntity', userId);

      // Remove from cleanup list since we deleted them
      const idx1 = createdEntityIds.indexOf(userId);
      if (idx1 > -1) createdEntityIds.splice(idx1, 1);
      const idx2 = createdEntityIds.indexOf(post1Id);
      if (idx2 > -1) createdEntityIds.splice(idx2, 1);
      const idx3 = createdEntityIds.indexOf(post2Id);
      if (idx3 > -1) createdEntityIds.splice(idx3, 1);

      // Verify deletion
      const deletedUser = await client.call<E2EEntity | null>('getEntity', userId);
      expect(deletedUser).toBeNull();

      console.log('[E2E] Step 5 complete: All entities deleted');
      console.log('[E2E] Full flow test completed successfully!');
    }, 120000); // 2 minute timeout for full flow
  });

  // --------------------------------------------------------------------------
  // Error Handling Tests
  // --------------------------------------------------------------------------

  describe('Error Handling', () => {
    it('should handle update on non-existent entity', async () => {
      const nonExistentId = generateEntityUrl('nonexistent', testId);

      await expect(
        client.call('updateEntity', nonExistentId, { name: 'Should Fail' })
      ).rejects.toThrow();
    }, 30000);

    it('should handle delete on non-existent entity', async () => {
      const nonExistentId = generateEntityUrl('nonexistent', testId);

      // Delete on non-existent might throw or be idempotent depending on implementation
      try {
        await client.call('deleteEntity', nonExistentId);
        // If it succeeds, that's also valid (idempotent delete)
      } catch (e) {
        // Error is expected
        expect(e).toBeInstanceOf(Error);
      }
    }, 30000);

    it('should handle duplicate entity creation', async () => {
      const entityId = generateEntityUrl('duplicate', testId);
      createdEntityIds.push(entityId);

      // Create entity
      await client.call('createEntity', {
        $id: entityId,
        $type: 'Test',
        name: 'Original',
      });

      // Try to create duplicate
      await expect(
        client.call('createEntity', {
          $id: entityId,
          $type: 'Test',
          name: 'Duplicate',
        })
      ).rejects.toThrow();
    }, 30000);
  });

  // --------------------------------------------------------------------------
  // Performance Baseline Tests
  // --------------------------------------------------------------------------

  describe('Performance Baseline', () => {
    it('should complete single entity CRUD within acceptable latency', async () => {
      const entityId = generateEntityUrl('perf', testId);
      createdEntityIds.push(entityId);

      const timings: Record<string, number> = {};

      // Create
      let start = Date.now();
      await client.call('createEntity', {
        $id: entityId,
        $type: 'PerfTest',
        name: 'Performance Test',
        data: { nested: { value: 42 } },
      });
      timings.create = Date.now() - start;

      // Read
      start = Date.now();
      await client.call('getEntity', entityId);
      timings.read = Date.now() - start;

      // Update
      start = Date.now();
      await client.call('updateEntity', entityId, { updatedAt: new Date().toISOString() });
      timings.update = Date.now() - start;

      // Delete
      start = Date.now();
      await client.call('deleteEntity', entityId);
      timings.delete = Date.now() - start;

      // Remove from cleanup list
      const idx = createdEntityIds.indexOf(entityId);
      if (idx > -1) createdEntityIds.splice(idx, 1);

      console.log('[E2E] CRUD Latencies:', timings);

      // Assert reasonable latencies (adjust based on expected performance)
      // These are generous limits to account for network variability
      expect(timings.create).toBeLessThan(5000);
      expect(timings.read).toBeLessThan(3000);
      expect(timings.update).toBeLessThan(5000);
      expect(timings.delete).toBeLessThan(5000);
    }, 60000);
  });
});

// ============================================================================
// Skip notification
// ============================================================================

if (!hasE2ECredentials()) {
  console.log(`
================================================================================
${SKIP_MESSAGE}

To run E2E tests against real Cloudflare:

1. Set environment variables:
   export CLOUDFLARE_API_TOKEN="your-api-token"
   export CLOUDFLARE_ACCOUNT_ID="your-account-id"
   export GRAPHDB_E2E_URL="wss://graphdb.workers.do/v1"  # optional

2. Run tests:
   npx vitest run test/e2e/cloudflare-integration.test.ts
================================================================================
`);
}
