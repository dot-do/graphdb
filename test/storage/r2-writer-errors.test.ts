/**
 * R2 CDC Writer Error Handling Tests
 *
 * Tests for proper error handling in the R2Writer flush interval:
 * - Should throw on R2 write failure
 * - Should retry on transient errors (with backoff)
 * - Should emit error event on permanent failure
 * - Should not lose data on flush failure
 * - Should log flush errors with context
 *
 * Data integrity is critical - never silently lose writes.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createR2Writer,
  type R2Writer,
  type R2WriterConfig,
  type R2WriterErrorEvent,
} from '../../src/storage/r2-writer';
import { type CDCEvent } from '../../src/shard/triggers';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type TransactionId,
} from '../../src/core/types';
import { type Triple } from '../../src/core/triple';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Generate a valid ULID-format transaction ID for testing
 */
function generateTestTxId(index: number): TransactionId {
  const base = '01ARZ3NDEKTSV4RRFFQ69G5FA';
  const lastChar = 'ABCDEFGHJKMNPQRSTVWXYZ'[index % 22];
  return createTransactionId(base + lastChar);
}

/**
 * Create a test triple
 */
function createTestTriple(
  subjectId: number,
  predicateName: string,
  value: string,
  timestamp: bigint,
  txId: TransactionId
): Triple {
  return {
    subject: createEntityId(`https://example.com/entity/${subjectId}`),
    predicate: createPredicate(predicateName),
    object: { type: ObjectType.STRING, value: value },
    timestamp,
    txId,
  };
}

/**
 * Create a test CDC event
 */
function createTestCDCEvent(
  subjectId: number,
  value: string,
  timestamp: bigint
): CDCEvent {
  return {
    type: 'insert',
    triple: createTestTriple(subjectId, 'name', value, timestamp, generateTestTxId(subjectId % 22)),
    timestamp,
  };
}

/**
 * Create multiple test CDC events
 */
function createTestCDCEvents(count: number, baseTimestamp: bigint): CDCEvent[] {
  return Array.from({ length: count }, (_, i) =>
    createTestCDCEvent(i, `User ${i}`, baseTimestamp + BigInt(i * 1000))
  );
}

// ============================================================================
// Mock R2 Bucket with Error Simulation
// ============================================================================

interface MockR2Object {
  key: string;
  data: Uint8Array;
  size: number;
  etag: string;
  uploaded: Date;
}

/**
 * R2 Error types for testing
 */
class R2TransientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'R2TransientError';
  }
}

class R2PermanentError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'R2PermanentError';
  }
}

/**
 * Create a mock R2Bucket that can simulate failures
 */
function createFailingR2Bucket(options: {
  failureMode?: 'transient' | 'permanent' | 'none';
  failUntilAttempt?: number; // For transient: succeed after N attempts
  failureMessage?: string;
}): R2Bucket & {
  _storage: Map<string, MockR2Object>;
  _putAttempts: number;
  _setFailureMode: (mode: 'transient' | 'permanent' | 'none') => void;
} {
  const storage = new Map<string, MockR2Object>();
  let putAttempts = 0;
  let failureMode = options.failureMode ?? 'none';
  const failUntilAttempt = options.failUntilAttempt ?? 3;
  const failureMessage = options.failureMessage ?? 'R2 write failed';

  return {
    _storage: storage,
    _putAttempts: putAttempts,
    _setFailureMode: (mode: 'transient' | 'permanent' | 'none') => {
      failureMode = mode;
      putAttempts = 0; // Reset attempt counter
    },

    get putAttempts() {
      return putAttempts;
    },

    async put(key: string, value: ArrayBuffer | Uint8Array | string | ReadableStream | Blob | null): Promise<R2Object> {
      putAttempts++;

      // Simulate failure based on mode
      if (failureMode === 'permanent') {
        throw new R2PermanentError(failureMessage);
      }

      if (failureMode === 'transient' && putAttempts < failUntilAttempt) {
        throw new R2TransientError(`${failureMessage} (attempt ${putAttempts})`);
      }

      // Success path
      let data: Uint8Array;
      if (value instanceof Uint8Array) {
        data = value;
      } else if (value instanceof ArrayBuffer) {
        data = new Uint8Array(value);
      } else if (typeof value === 'string') {
        data = new TextEncoder().encode(value);
      } else {
        throw new Error('Unsupported value type');
      }

      const obj: MockR2Object = {
        key,
        data,
        size: data.length,
        etag: `etag-${Date.now()}-${Math.random()}`,
        uploaded: new Date(),
      };
      storage.set(key, obj);

      return {
        key,
        size: obj.size,
        etag: obj.etag,
        httpEtag: `"${obj.etag}"`,
        uploaded: obj.uploaded,
        checksums: {},
        customMetadata: {},
        httpMetadata: {},
        writeHttpMetadata: () => {},
        storageClass: 'Standard',
      } as unknown as R2Object;
    },

    async get(key: string): Promise<R2ObjectBody | null> {
      const obj = storage.get(key);
      if (!obj) return null;

      return {
        key: obj.key,
        size: obj.size,
        etag: obj.etag,
        httpEtag: `"${obj.etag}"`,
        uploaded: obj.uploaded,
        checksums: {},
        customMetadata: {},
        httpMetadata: {},
        writeHttpMetadata: () => {},
        storageClass: 'Standard',
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(obj.data);
            controller.close();
          },
        }),
        bodyUsed: false,
        arrayBuffer: async () => obj.data.buffer.slice(obj.data.byteOffset, obj.data.byteOffset + obj.data.byteLength),
        text: async () => new TextDecoder().decode(obj.data),
        json: async () => JSON.parse(new TextDecoder().decode(obj.data)),
        blob: async () => new Blob([obj.data]),
      } as unknown as R2ObjectBody;
    },

    async head(key: string): Promise<R2Object | null> {
      const obj = storage.get(key);
      if (!obj) return null;

      return {
        key: obj.key,
        size: obj.size,
        etag: obj.etag,
        httpEtag: `"${obj.etag}"`,
        uploaded: obj.uploaded,
        checksums: {},
        customMetadata: {},
        httpMetadata: {},
        writeHttpMetadata: () => {},
        storageClass: 'Standard',
      } as unknown as R2Object;
    },

    async delete(keys: string | string[]): Promise<void> {
      const keyArray = Array.isArray(keys) ? keys : [keys];
      for (const key of keyArray) {
        storage.delete(key);
      }
    },

    async list(listOptions?: R2ListOptions): Promise<R2Objects> {
      const prefix = listOptions?.prefix ?? '';
      const objects: R2Object[] = [];

      for (const [key, obj] of storage) {
        if (key.startsWith(prefix)) {
          objects.push({
            key: obj.key,
            size: obj.size,
            etag: obj.etag,
            httpEtag: `"${obj.etag}"`,
            uploaded: obj.uploaded,
            checksums: {},
            customMetadata: {},
            httpMetadata: {},
            writeHttpMetadata: () => {},
            storageClass: 'Standard',
          } as unknown as R2Object);
        }
      }

      return {
        objects,
        truncated: false,
        delimitedPrefixes: [],
      };
    },

    async createMultipartUpload(): Promise<R2MultipartUpload> {
      throw new Error('Not implemented');
    },

    async resumeMultipartUpload(): Promise<R2MultipartUpload> {
      throw new Error('Not implemented');
    },
  } as unknown as R2Bucket & {
    _storage: Map<string, MockR2Object>;
    _putAttempts: number;
    _setFailureMode: (mode: 'transient' | 'permanent' | 'none') => void;
  };
}

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('R2Writer Error Handling', () => {
  const testNamespace = createNamespace('https://example.com/crm/acme');

  describe('Explicit flush error propagation', () => {
    it('should throw on R2 write failure during explicit flush', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
        failureMessage: 'R2 service unavailable',
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0, // Disable interval
        maxRetries: 0, // No retries for this test
      });

      const events = createTestCDCEvents(5, BigInt(Date.now()));
      await writer.write(events);

      // Explicit flush should throw
      await expect(writer.flush()).rejects.toThrow('R2 service unavailable');

      writer.close();
    });

    it('should include context in error message', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
        failureMessage: 'Connection timeout',
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 0,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      try {
        await writer.flush();
        expect.fail('Expected flush to throw');
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        // Error should have contextual information
        const errorMessage = (error as Error).message;
        expect(errorMessage).toContain('Connection timeout');
      }

      writer.close();
    });
  });

  describe('Retry with exponential backoff', () => {
    let bucket: ReturnType<typeof createFailingR2Bucket>;

    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should retry on transient errors with backoff', async () => {
      bucket = createFailingR2Bucket({
        failureMode: 'transient',
        failUntilAttempt: 3, // Fail twice, succeed on 3rd
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 3,
        retryBackoffMs: 100,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      // Flush should eventually succeed after retries
      const flushPromise = writer.flush();

      // Advance timers for backoff delays
      await vi.advanceTimersByTimeAsync(100); // First retry delay
      await vi.advanceTimersByTimeAsync(200); // Second retry delay (exponential)
      await vi.advanceTimersByTimeAsync(400); // Extra buffer

      await flushPromise;

      // Should have succeeded after 3 attempts
      expect(bucket._storage.size).toBe(1);

      writer.close();
    });

    it('should respect maxRetries limit', async () => {
      bucket = createFailingR2Bucket({
        failureMode: 'permanent', // Always fails
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 2,
        retryBackoffMs: 50,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      const flushPromise = writer.flush();

      // Advance timers for all retry delays
      await vi.advanceTimersByTimeAsync(50); // First retry
      await vi.advanceTimersByTimeAsync(100); // Second retry
      await vi.advanceTimersByTimeAsync(200); // Extra buffer

      // Should fail after exhausting retries
      await expect(flushPromise).rejects.toThrow();

      writer.close();
    });

    it('should use exponential backoff (100ms, 200ms, 400ms)', async () => {
      const delays: number[] = [];
      const originalSetTimeout = globalThis.setTimeout;

      vi.spyOn(globalThis, 'setTimeout').mockImplementation((fn, delay) => {
        if (delay && delay > 0) {
          delays.push(delay);
        }
        return originalSetTimeout(fn, delay);
      });

      bucket = createFailingR2Bucket({
        failureMode: 'transient',
        failUntilAttempt: 4, // Need 3 retries
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 3,
        retryBackoffMs: 100,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      const flushPromise = writer.flush();

      // Advance through all backoff periods
      for (let i = 0; i < 3; i++) {
        await vi.advanceTimersByTimeAsync(100 * Math.pow(2, i) + 50);
      }

      await flushPromise;

      // Verify exponential backoff pattern
      expect(delays).toContain(100); // First retry
      expect(delays).toContain(200); // Second retry
      expect(delays).toContain(400); // Third retry

      writer.close();
    });
  });

  describe('Error event emission', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should emit error event on permanent failure', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
        failureMessage: 'Bucket not found',
      });

      const errorEvents: R2WriterErrorEvent[] = [];

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 100,
        maxRetries: 1,
        retryBackoffMs: 10,
        onError: (event) => errorEvents.push(event),
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      // Trigger interval flush
      await vi.advanceTimersByTimeAsync(150);

      // Allow retries to complete
      await vi.advanceTimersByTimeAsync(100);

      // Should have emitted error event
      expect(errorEvents.length).toBeGreaterThan(0);
      expect(errorEvents[0].error.message).toContain('Bucket not found');
      expect(errorEvents[0].eventCount).toBe(5);

      writer.close();
    });

    it('should include buffered event count in error event', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
      });

      const errorEvents: R2WriterErrorEvent[] = [];

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 0,
        onError: (event) => errorEvents.push(event),
      });

      // Write specific number of events
      await writer.write(createTestCDCEvents(7, BigInt(Date.now())));

      try {
        await writer.flush();
      } catch {
        // Expected to throw
      }

      expect(errorEvents.length).toBe(1);
      expect(errorEvents[0].eventCount).toBe(7);

      writer.close();
    });

    it('should include retry attempt count in error event', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
      });

      const errorEvents: R2WriterErrorEvent[] = [];

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 2,
        retryBackoffMs: 10,
        onError: (event) => errorEvents.push(event),
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      const flushPromise = writer.flush();

      // Advance through retries
      await vi.advanceTimersByTimeAsync(100);

      try {
        await flushPromise;
      } catch {
        // Expected
      }

      expect(errorEvents.length).toBe(1);
      expect(errorEvents[0].attempts).toBe(3); // Initial + 2 retries

      writer.close();
    });
  });

  describe('Data preservation on failure', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should not lose data on flush failure - buffer preserved for retry', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'transient',
        failUntilAttempt: 2,
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 3,
        retryBackoffMs: 50,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      // First flush attempt fails
      const flushPromise = writer.flush();

      // Advance to allow retry
      await vi.advanceTimersByTimeAsync(100);

      await flushPromise;

      // Data should eventually be written
      expect(bucket._storage.size).toBe(1);

      // Verify all 5 events were written
      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(5);

      writer.close();
    });

    it('should keep data in buffer after all retries exhausted', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 2,
        retryBackoffMs: 10,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      const flushPromise = writer.flush();
      await vi.advanceTimersByTimeAsync(100);

      try {
        await flushPromise;
      } catch {
        // Expected to fail
      }

      // Buffer should still have data
      const pendingCount = writer.getPendingEventCount();
      expect(pendingCount).toBe(5);

      // Now fix the bucket and retry
      bucket._setFailureMode('none');

      await writer.flush();

      expect(bucket._storage.size).toBe(1);
      expect(writer.getPendingEventCount()).toBe(0);

      writer.close();
    });

    it('should recover gracefully after transient failures', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'transient',
        failUntilAttempt: 3,
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 5,
        retryBackoffMs: 20,
      });

      await writer.write(createTestCDCEvents(10, BigInt(Date.now())));

      const flushPromise = writer.flush();

      // Advance timers to allow retries
      for (let i = 0; i < 5; i++) {
        await vi.advanceTimersByTimeAsync(50);
      }

      await flushPromise;

      // Should have recovered
      expect(bucket._storage.size).toBe(1);

      const stats = writer.getStats();
      expect(stats.eventsWritten).toBe(10);
      expect(stats.flushCount).toBe(1);

      writer.close();
    });
  });

  describe('Logging', () => {
    let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      vi.useFakeTimers();
    });

    afterEach(() => {
      consoleErrorSpy.mockRestore();
      vi.useRealTimers();
    });

    it('should log flush errors with context', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
        failureMessage: 'Network error',
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 100,
        maxRetries: 0,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      // Trigger interval flush
      await vi.advanceTimersByTimeAsync(150);

      // Should have logged the error
      expect(consoleErrorSpy).toHaveBeenCalled();

      const lastCall = consoleErrorSpy.mock.calls[consoleErrorSpy.mock.calls.length - 1];
      const loggedMessage = lastCall[0];

      expect(loggedMessage).toContain('R2Writer');
      expect(loggedMessage).toContain('flush');

      writer.close();
    });

    it('should log structured error with namespace and event count', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
        failureMessage: 'Write quota exceeded',
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 100,
        maxRetries: 0,
      });

      await writer.write(createTestCDCEvents(3, BigInt(Date.now())));

      await vi.advanceTimersByTimeAsync(150);

      expect(consoleErrorSpy).toHaveBeenCalled();

      // Check that structured logging includes context
      const calls = consoleErrorSpy.mock.calls;
      const hasContext = calls.some(call => {
        const args = call.join(' ');
        return args.includes('example.com') || args.includes('crm/acme') || args.includes('3');
      });

      expect(hasContext).toBe(true);

      writer.close();
    });

    it('should log retry attempts', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'transient',
        failUntilAttempt: 3,
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 0,
        maxRetries: 3,
        retryBackoffMs: 10,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      const flushPromise = writer.flush();

      // Advance through retries
      for (let i = 0; i < 5; i++) {
        await vi.advanceTimersByTimeAsync(50);
      }

      await flushPromise;

      // Should have logged retry attempts
      const retryCalls = consoleErrorSpy.mock.calls.filter(call =>
        call.some(arg => typeof arg === 'string' && arg.toLowerCase().includes('retry'))
      );

      expect(retryCalls.length).toBeGreaterThan(0);

      writer.close();
    });
  });

  describe('Interval flush error handling', () => {
    let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      vi.useFakeTimers();
    });

    afterEach(() => {
      consoleErrorSpy.mockRestore();
      vi.useRealTimers();
    });

    it('should not crash on interval flush error', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'permanent',
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 50,
        maxRetries: 0,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      // Multiple interval triggers should not crash
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(100);

      // Writer should still be functional
      expect(() => writer.getStats()).not.toThrow();

      writer.close();
    });

    it('should continue trying on subsequent intervals after failure', async () => {
      const bucket = createFailingR2Bucket({
        failureMode: 'transient',
        failUntilAttempt: 5,
      });

      const writer = createR2Writer({
        bucket,
        namespace: testNamespace,
        flushIntervalMs: 50,
        maxRetries: 1,
        retryBackoffMs: 10,
      });

      await writer.write(createTestCDCEvents(5, BigInt(Date.now())));

      // First few intervals will fail
      for (let i = 0; i < 10; i++) {
        await vi.advanceTimersByTimeAsync(100);
      }

      // Eventually should succeed
      expect(bucket._storage.size).toBe(1);

      writer.close();
    });
  });
});
