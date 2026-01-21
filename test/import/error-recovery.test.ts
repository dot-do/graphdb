/**
 * Tests for Error Recovery in GraphDB Import Module
 *
 * TDD RED phase: Tests for error recovery scenarios
 *
 * Tests cover:
 * - Storage failures during batch writes
 * - Partial batch recovery
 * - Connection interruptions during streaming
 * - Corrupted checkpoint recovery
 * - Transaction rollback scenarios
 * - Retry exhaustion handling
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createStreamingLineReader,
  type StreamingLineReader,
} from '../../src/import/streaming-reader';
import {
  createBatchedTripleWriter,
  type BatchedTripleWriter,
} from '../../src/import/batched-writer';
import {
  createResumableImportState,
  type ResumableImportState,
  type ImportCheckpoint,
} from '../../src/import/resumable-state';
import {
  createRangeFetcher,
  type RangeFetcher,
} from '../../src/import/range-fetcher';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import type { Triple } from '../../src/core/triple';

// ============================================================================
// Test Helpers
// ============================================================================

function makeTestTriple(id: number): Triple {
  return {
    subject: createEntityId(`https://example.com/entity/${id}`),
    predicate: createPredicate('name'),
    object: { type: ObjectType.STRING, value: `Entity ${id}` },
    timestamp: BigInt(Date.now()),
    txId: createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
  };
}

function createMockR2Bucket(options?: {
  failAfter?: number;
  failOnKeys?: string[];
  failureType?: 'throw' | 'timeout';
}): R2Bucket & {
  puts: Map<string, Uint8Array>;
  putCount: number;
  reset: () => void;
} {
  const puts = new Map<string, Uint8Array>();
  let putCount = 0;

  return {
    puts,
    get putCount() { return putCount; },
    reset() {
      puts.clear();
      putCount = 0;
    },
    async put(key: string, value: ArrayBufferLike | ArrayBuffer | ReadableStream | string | null) {
      putCount++;

      // Simulate failure after N writes
      if (options?.failAfter !== undefined && putCount > options.failAfter) {
        if (options?.failureType === 'timeout') {
          await new Promise((_, reject) =>
            setTimeout(() => reject(new Error('R2 write timeout')), 100)
          );
        }
        throw new Error('R2 write failed: simulated storage failure');
      }

      // Simulate failure on specific keys
      if (options?.failOnKeys?.some(pattern => key.includes(pattern))) {
        throw new Error(`R2 write failed for key: ${key}`);
      }

      if (value instanceof Uint8Array) {
        puts.set(key, value);
      } else if (typeof value === 'string') {
        puts.set(key, new TextEncoder().encode(value));
      }
      return {} as R2Object;
    },
    async get() { return null; },
    async head() { return null; },
    async delete() {},
    async list() { return { objects: [], truncated: false } as R2Objects; },
    async createMultipartUpload() { return {} as R2MultipartUpload; },
    async resumeMultipartUpload() { return {} as R2MultipartUpload; },
  } as unknown as R2Bucket & { puts: Map<string, Uint8Array>; putCount: number; reset: () => void };
}

function createMockDOStorage(options?: {
  failOnSave?: boolean;
  corruptData?: boolean;
}): DurableObjectStorage {
  const data = new Map<string, unknown>();

  return {
    async get<T>(key: string): Promise<T | undefined> {
      const value = data.get(key);
      if (options?.corruptData && value) {
        // Return corrupted data
        return { corrupted: true } as T;
      }
      return value as T | undefined;
    },
    async put(key: string, value: unknown): Promise<void> {
      if (options?.failOnSave) {
        throw new Error('DO storage write failed');
      }
      data.set(key, value);
    },
    async delete(key: string | string[]): Promise<boolean> {
      if (Array.isArray(key)) {
        let deleted = false;
        for (const k of key) {
          if (data.delete(k)) deleted = true;
        }
        return deleted;
      }
      return data.delete(key);
    },
    async list(options?: { prefix?: string }): Promise<Map<string, unknown>> {
      const result = new Map<string, unknown>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value);
        }
      }
      return result;
    },
  } as unknown as DurableObjectStorage;
}

// ============================================================================
// Storage Failure Tests
// ============================================================================

describe('Error Recovery: Storage Failures', () => {
  describe('BatchedTripleWriter storage errors', () => {
    it('should throw error when R2 write fails', async () => {
      const r2 = createMockR2Bucket({ failAfter: 0 });
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      await writer.addTriple(makeTestTriple(1));

      // This should trigger a flush and fail
      await expect(writer.addTriple(makeTestTriple(2))).rejects.toThrow('R2 write failed');
    });

    it('should preserve state before failure for recovery', async () => {
      const r2 = createMockR2Bucket({ failAfter: 1 });
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      // First batch should succeed
      await writer.addTriples([makeTestTriple(1), makeTestTriple(2)]);

      const stateBeforeFailure = writer.getState();
      expect(stateBeforeFailure.chunksUploaded).toBe(1);
      expect(stateBeforeFailure.triplesWritten).toBe(2);

      // Second batch should fail
      await writer.addTriple(makeTestTriple(3));
      await expect(writer.addTriple(makeTestTriple(4))).rejects.toThrow();

      // State should still reflect successful writes
      const stateAfterFailure = writer.getState();
      expect(stateAfterFailure.chunksUploaded).toBe(1);
    });

    it('should allow retry after transient storage failure', async () => {
      let failCount = 0;
      const r2 = createMockR2Bucket();

      // Override put to fail first time, succeed second time
      const originalPut = r2.put.bind(r2);
      r2.put = async (key: string, value: any) => {
        failCount++;
        if (failCount === 1) {
          throw new Error('Transient R2 failure');
        }
        return originalPut(key, value);
      };

      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 2,
      });

      await writer.addTriple(makeTestTriple(1));

      // First attempt fails
      await expect(writer.addTriple(makeTestTriple(2))).rejects.toThrow('Transient R2 failure');

      // Retry with new triples should succeed
      await writer.addTriples([makeTestTriple(3), makeTestTriple(4)]);
      expect(r2.puts.size).toBe(1);
    });
  });

  describe('ResumableImportState storage errors', () => {
    it('should throw when checkpoint save fails', async () => {
      const storage = createMockDOStorage({ failOnSave: true });
      const importState = createResumableImportState(storage);

      const checkpoint: ImportCheckpoint = {
        jobId: 'test-job',
        sourceUrl: 'https://example.com/data.json',
        byteOffset: 1000,
        linesProcessed: 50,
        triplesWritten: 200,
        lineReaderState: { bytesProcessed: 1000, linesEmitted: 50, partialLine: '' },
        batchWriterState: {
          triplesWritten: 200,
          chunksUploaded: 2,
          bytesUploaded: 5000,
          chunkInfos: [],
          bloomState: { filter: '', k: 7, m: 1000, version: 'v1', meta: { count: 0, capacity: 1000, targetFpr: 0.01, expectedFpr: 0.01, sizeBytes: 125 } },
        },
        checkpointedAt: new Date().toISOString(),
      };

      await expect(importState.saveCheckpoint(checkpoint)).rejects.toThrow('DO storage write failed');
    });

    it('should handle corrupted checkpoint data gracefully', async () => {
      const storage = createMockDOStorage({ corruptData: true });
      const importState = createResumableImportState(storage);

      // First save without corruption
      const normalStorage = createMockDOStorage();
      const normalImportState = createResumableImportState(normalStorage);

      const checkpoint: ImportCheckpoint = {
        jobId: 'test-job',
        sourceUrl: 'https://example.com/data.json',
        byteOffset: 1000,
        linesProcessed: 50,
        triplesWritten: 200,
        lineReaderState: { bytesProcessed: 1000, linesEmitted: 50, partialLine: '' },
        batchWriterState: {
          triplesWritten: 200,
          chunksUploaded: 2,
          bytesUploaded: 5000,
          chunkInfos: [],
          bloomState: { filter: '', k: 7, m: 1000, version: 'v1', meta: { count: 0, capacity: 1000, targetFpr: 0.01, expectedFpr: 0.01, sizeBytes: 125 } },
        },
        checkpointedAt: new Date().toISOString(),
      };
      await normalImportState.saveCheckpoint(checkpoint);

      // Load returns corrupted data - should validate and reject
      const loaded = await importState.loadCheckpoint('test-job');

      // Expected behavior: either return null or throw validation error
      // The implementation should validate checkpoint structure
      expect(loaded === null || !('jobId' in loaded)).toBe(true);
    });
  });
});

// ============================================================================
// Streaming Interruption Tests
// ============================================================================

describe('Error Recovery: Streaming Interruptions', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  describe('Connection drops mid-stream', () => {
    it('should handle connection drop during chunk fetch', async () => {
      let chunkCount = 0;
      const content = 'AAAABBBBCCCCDDDD';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        chunkCount++;

        // Fail on third chunk
        if (chunkCount === 3) {
          throw new Error('Connection reset by peer');
        }

        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
        maxRetries: 1,
        baseDelayMs: 10,
      });

      const chunks: string[] = [];
      let error: Error | null = null;

      try {
        for await (const { data } of fetcher.chunks(0)) {
          chunks.push(new TextDecoder().decode(data));
        }
      } catch (e) {
        error = e as Error;
      }

      // Should have received first two chunks before failure
      expect(chunks.length).toBe(2);
      expect(chunks).toEqual(['AAAA', 'BBBB']);
      expect(error?.message).toBe('Connection reset by peer');
    });

    it('should allow resuming from last successful offset after interruption', async () => {
      const content = 'AAAABBBBCCCCDDDD';
      let failOnChunk = 3;

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);

          // Calculate which chunk this is
          const chunkIndex = Math.floor(start / 4) + 1;
          if (chunkIndex === failOnChunk) {
            failOnChunk = -1; // Only fail once
            throw new Error('Network timeout');
          }

          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
        maxRetries: 1,
        baseDelayMs: 10,
      });

      // First attempt - should get 2 chunks then fail
      const firstAttemptChunks: { data: string; end: number }[] = [];
      let lastSuccessfulOffset = 0;

      try {
        for await (const { data, end } of fetcher.chunks(0)) {
          firstAttemptChunks.push({ data: new TextDecoder().decode(data), end });
          lastSuccessfulOffset = end;
        }
      } catch {
        // Expected
      }

      expect(firstAttemptChunks.length).toBe(2);
      expect(lastSuccessfulOffset).toBe(8);

      // Resume from last successful offset
      const resumeChunks: string[] = [];
      for await (const { data } of fetcher.chunks(lastSuccessfulOffset)) {
        resumeChunks.push(new TextDecoder().decode(data));
      }

      expect(resumeChunks).toEqual(['CCCC', 'DDDD']);
    });
  });

  describe('StreamingLineReader interruption recovery', () => {
    it('should preserve partial line state on interruption', async () => {
      const reader = createStreamingLineReader();

      // Process chunk that ends mid-line
      const chunk1 = new TextEncoder().encode('line1\nline2\npartial li');
      const lines: string[] = [];

      for await (const line of reader.processChunk(chunk1)) {
        lines.push(line);
      }

      expect(lines).toEqual(['line1', 'line2']);

      // Simulate interruption - save state
      const savedState = reader.getState();
      expect(savedState.partialLine).toBe('partial li');

      // Create new reader and restore (simulating process restart)
      const newReader = createStreamingLineReader();
      newReader.restoreState(savedState);

      // Continue processing
      const chunk2 = new TextEncoder().encode('ne\nline4\n');
      const moreLines: string[] = [];

      for await (const line of newReader.processChunk(chunk2)) {
        moreLines.push(line);
      }

      expect(moreLines).toEqual(['partial line', 'line4']);
    });
  });
});

// ============================================================================
// Complex Resume Scenarios
// ============================================================================

describe('Error Recovery: Complex Resume Scenarios', () => {
  it('should resume multi-stage import after failure', async () => {
    const storage = createMockDOStorage();
    const importState = createResumableImportState(storage);

    // Stage 1: Process first half, save checkpoint
    const r2First = createMockR2Bucket();
    const readerFirst = createStreamingLineReader();
    const writerFirst = createBatchedTripleWriter(r2First, 'https://example.com/data/', {
      batchSize: 10,
    });

    const totalRecords = 100;
    const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

    // Process first 50 records
    for (let i = 0; i < 50; i++) {
      const line = `{"id": ${i}, "data": "test"}`;
      const chunk = new TextEncoder().encode(line + '\n');

      for await (const parsedLine of readerFirst.processChunk(chunk)) {
        const record = JSON.parse(parsedLine);
        await writerFirst.addTriple({
          subject: createEntityId(`https://example.com/entity/${record.id}`),
          predicate: createPredicate('data'),
          object: { type: ObjectType.STRING, value: record.data },
          timestamp: BigInt(Date.now()),
          txId,
        });
      }
    }

    // Simulate checkpoint save at failure point
    await writerFirst.flush();
    await importState.saveCheckpoint({
      jobId: 'multi-stage-import',
      sourceUrl: 'https://example.com/data.json',
      byteOffset: 50 * 25, // Approximate
      linesProcessed: 50,
      triplesWritten: 50,
      lineReaderState: readerFirst.getState(),
      batchWriterState: writerFirst.getState(),
      checkpointedAt: new Date().toISOString(),
    });

    // Stage 2: Resume from checkpoint
    const checkpoint = await importState.loadCheckpoint('multi-stage-import');
    expect(checkpoint).not.toBeNull();

    const r2Second = createMockR2Bucket();
    const readerSecond = createStreamingLineReader();
    readerSecond.restoreState(checkpoint!.lineReaderState);

    const writerSecond = createBatchedTripleWriter(r2Second, 'https://example.com/data/', {
      batchSize: 10,
    });
    writerSecond.restoreState(checkpoint!.batchWriterState);

    // Process remaining 50 records
    for (let i = 50; i < totalRecords; i++) {
      const line = `{"id": ${i}, "data": "test"}`;
      const chunk = new TextEncoder().encode(line + '\n');

      for await (const parsedLine of readerSecond.processChunk(chunk)) {
        const record = JSON.parse(parsedLine);
        await writerSecond.addTriple({
          subject: createEntityId(`https://example.com/entity/${record.id}`),
          predicate: createPredicate('data'),
          object: { type: ObjectType.STRING, value: record.data },
          timestamp: BigInt(Date.now()),
          txId,
        });
      }
    }

    const result = await writerSecond.finalize();

    // Total should be 100 (50 restored + 50 new)
    expect(result.totalTriples).toBe(totalRecords);
  });

  it('should handle checkpoint with uncommitted buffer data', async () => {
    const storage = createMockDOStorage();
    const importState = createResumableImportState(storage);
    const r2 = createMockR2Bucket();

    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 10,
    });

    // Add 15 triples (10 will flush, 5 in buffer)
    for (let i = 0; i < 15; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    // State should show 10 written, buffer has 5 uncommitted
    const state = writer.getState();
    expect(state.triplesWritten).toBe(10);
    expect(r2.puts.size).toBe(1);

    // Save checkpoint - note: buffer content is lost!
    await importState.saveCheckpoint({
      jobId: 'uncommitted-test',
      sourceUrl: 'https://example.com/data.json',
      byteOffset: 15 * 25,
      linesProcessed: 15,
      triplesWritten: state.triplesWritten, // Only committed writes
      lineReaderState: { bytesProcessed: 15 * 25, linesEmitted: 15, partialLine: '' },
      batchWriterState: state,
      checkpointedAt: new Date().toISOString(),
    });

    // On resume, we should re-process from line 10, not 15
    // This tests that checkpoint tracks committed state correctly
    const loaded = await importState.loadCheckpoint('uncommitted-test');
    expect(loaded!.batchWriterState.triplesWritten).toBe(10);
  });
});

// ============================================================================
// Retry Exhaustion Tests
// ============================================================================

describe('Error Recovery: Retry Exhaustion', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('should provide detailed error after all retries fail', async () => {
    const errors: string[] = [];
    let attemptCount = 0;

    globalThis.fetch = vi.fn().mockImplementation(async () => {
      attemptCount++;
      const errorMsg = `Attempt ${attemptCount} failed: Service unavailable`;
      errors.push(errorMsg);
      throw new Error(errorMsg);
    });

    const fetcher = createRangeFetcher('https://example.com/data.txt', {
      maxRetries: 3,
      baseDelayMs: 10,
    });

    try {
      await fetcher.fetchRange(0, 100);
      expect.fail('Should have thrown');
    } catch (error) {
      // Should have attempted 3 times
      expect(attemptCount).toBe(3);
      // Error should be from last attempt
      expect((error as Error).message).toContain('Attempt 3');
    }
  });

  it('should track partial progress before retry exhaustion', async () => {
    const content = 'AAAABBBBCCCCDDDD';
    let fetchCount = 0;

    globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
      fetchCount++;
      const rangeHeader = (options?.headers as Record<string, string>)?.Range;

      if (rangeHeader && rangeHeader.startsWith('bytes=')) {
        const [start] = rangeHeader.slice(6).split('-').map(Number);

        // First two chunks succeed, then permanent failure
        if (start >= 8) {
          throw new Error('Permanent failure');
        }

        const end = Math.min(start + 3, content.length - 1);
        const slice = content.slice(start, end + 1);

        return new Response(slice, {
          status: 206,
          headers: {
            'Content-Range': `bytes ${start}-${end}/${content.length}`,
          },
        });
      }
      return new Response('', { status: 400 });
    });

    const fetcher = createRangeFetcher('https://example.com/data.txt', {
      chunkSize: 4,
      maxRetries: 2,
      baseDelayMs: 10,
    });

    const successfulChunks: string[] = [];
    let failedAtOffset = 0;

    try {
      for await (const { data, end } of fetcher.chunks(0)) {
        successfulChunks.push(new TextDecoder().decode(data));
        failedAtOffset = end;
      }
    } catch {
      // Expected
    }

    expect(successfulChunks).toEqual(['AAAA', 'BBBB']);
    expect(failedAtOffset).toBe(8);
  });
});

// ============================================================================
// Batch Import Edge Cases
// ============================================================================

describe('Error Recovery: Batch Import Edge Cases', () => {
  it('should handle exactly batch size writes', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 5,
    });

    // Add exactly 5 triples
    for (let i = 0; i < 5; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    // Should have flushed exactly once
    expect(r2.puts.size).toBe(1);

    const state = writer.getState();
    expect(state.triplesWritten).toBe(5);
  });

  it('should handle single triple batch', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 1,
    });

    await writer.addTriple(makeTestTriple(1));
    expect(r2.puts.size).toBe(1);

    await writer.addTriple(makeTestTriple(2));
    expect(r2.puts.size).toBe(2);

    await writer.addTriple(makeTestTriple(3));
    expect(r2.puts.size).toBe(3);

    const result = await writer.finalize();
    expect(result.totalTriples).toBe(3);
    expect(result.totalChunks).toBe(3);
  });

  it('should handle very large batch sizes', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 100000, // Very large batch
    });

    // Add 1000 triples (less than batch size)
    for (let i = 0; i < 1000; i++) {
      await writer.addTriple(makeTestTriple(i));
    }

    // Should not have flushed yet
    expect(r2.puts.size).toBe(0);

    // Finalize should flush the remaining
    const result = await writer.finalize();
    expect(result.totalTriples).toBe(1000);
    expect(result.totalChunks).toBe(1);
  });

  it('should handle empty finalize', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 100,
    });

    // Finalize without adding any triples
    const result = await writer.finalize();

    expect(result.totalTriples).toBe(0);
    expect(result.totalChunks).toBe(0);
    expect(result.chunks).toHaveLength(0);
    expect(r2.puts.size).toBe(0);
  });

  it('should handle multiple finalizes', async () => {
    const r2 = createMockR2Bucket();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
      batchSize: 100,
    });

    await writer.addTriple(makeTestTriple(1));

    const result1 = await writer.finalize();
    expect(result1.totalTriples).toBe(1);

    // Second finalize should be idempotent
    const result2 = await writer.finalize();
    expect(result2.totalTriples).toBe(1);
    expect(r2.puts.size).toBe(1);
  });
});

// ============================================================================
// Data Validation During Recovery
// ============================================================================

describe('Error Recovery: Data Validation', () => {
  it('should validate checkpoint structure on load', async () => {
    const storage = createMockDOStorage();

    // Manually insert invalid checkpoint data
    await storage.put('checkpoint:invalid-job', {
      jobId: 'invalid-job',
      // Missing required fields
    });

    const importState = createResumableImportState(storage);

    // Should return null for invalid checkpoint structure
    const loaded = await importState.loadCheckpoint('invalid-job');

    // Validation should reject invalid checkpoints
    expect(loaded).toBeNull();
  });

  it('should return null for checkpoint missing lineReaderState', async () => {
    const storage = createMockDOStorage();

    // Missing lineReaderState
    await storage.put('checkpoint:partial-job', {
      jobId: 'partial-job',
      sourceUrl: 'https://example.com/data.json',
      byteOffset: 1000,
      linesProcessed: 50,
      triplesWritten: 200,
      // lineReaderState missing
      batchWriterState: {
        triplesWritten: 200,
        chunksUploaded: 2,
        bytesUploaded: 5000,
        chunkInfos: [],
        bloomState: { filter: '', k: 7, m: 1000, version: 'v1', meta: {} },
      },
      checkpointedAt: new Date().toISOString(),
    });

    const importState = createResumableImportState(storage);
    const loaded = await importState.loadCheckpoint('partial-job');

    expect(loaded).toBeNull();
  });

  it('should return null for checkpoint missing batchWriterState', async () => {
    const storage = createMockDOStorage();

    // Missing batchWriterState
    await storage.put('checkpoint:partial-job-2', {
      jobId: 'partial-job-2',
      sourceUrl: 'https://example.com/data.json',
      byteOffset: 1000,
      linesProcessed: 50,
      triplesWritten: 200,
      lineReaderState: { bytesProcessed: 1000, linesEmitted: 50, partialLine: '' },
      // batchWriterState missing
      checkpointedAt: new Date().toISOString(),
    });

    const importState = createResumableImportState(storage);
    const loaded = await importState.loadCheckpoint('partial-job-2');

    expect(loaded).toBeNull();
  });

  it('should accept valid checkpoint with all required fields', async () => {
    const storage = createMockDOStorage();

    const validCheckpoint = {
      jobId: 'valid-job',
      sourceUrl: 'https://example.com/data.json',
      byteOffset: 1000,
      linesProcessed: 50,
      triplesWritten: 200,
      lineReaderState: { bytesProcessed: 1000, linesEmitted: 50, partialLine: '' },
      batchWriterState: {
        triplesWritten: 200,
        chunksUploaded: 2,
        bytesUploaded: 5000,
        chunkInfos: [],
        bloomState: { filter: '', k: 7, m: 1000, version: 'v1', meta: {} },
      },
      checkpointedAt: new Date().toISOString(),
    };

    await storage.put('checkpoint:valid-job', validCheckpoint);

    const importState = createResumableImportState(storage);
    const loaded = await importState.loadCheckpoint('valid-job');

    expect(loaded).not.toBeNull();
    expect(loaded?.jobId).toBe('valid-job');
    expect(loaded?.sourceUrl).toBe('https://example.com/data.json');
  });

  it('should handle BigInt serialization in checkpoints', async () => {
    const storage = createMockDOStorage();
    const importState = createResumableImportState(storage);

    const checkpoint: ImportCheckpoint = {
      jobId: 'bigint-test',
      sourceUrl: 'https://example.com/data.json',
      byteOffset: 1000,
      linesProcessed: 50,
      triplesWritten: 200,
      lineReaderState: { bytesProcessed: 1000, linesEmitted: 50, partialLine: '' },
      batchWriterState: {
        triplesWritten: 200,
        chunksUploaded: 2,
        bytesUploaded: 5000,
        chunkInfos: [{
          id: 'chunk-1',
          tripleCount: 100,
          minTime: BigInt('9007199254740993'), // Larger than Number.MAX_SAFE_INTEGER
          maxTime: BigInt('9007199254740999'),
          bytes: 5000,
          path: '/test/chunk-1.gcol',
        }],
        bloomState: { filter: '', k: 7, m: 1000, version: 'v1', meta: { count: 0, capacity: 1000, targetFpr: 0.01, expectedFpr: 0.01, sizeBytes: 125 } },
      },
      checkpointedAt: new Date().toISOString(),
    };

    await importState.saveCheckpoint(checkpoint);
    const loaded = await importState.loadCheckpoint('bigint-test');

    // BigInt values should be preserved correctly
    expect(loaded?.batchWriterState.chunkInfos[0]?.minTime).toBe(BigInt('9007199254740993'));
  });
});
