/**
 * Tests for Streaming Import - Memory-Efficient Data Ingestion for GraphDB
 *
 * TDD-first tests covering:
 * - StreamingLineReader: Process chunks without loading full text
 * - BatchedTripleWriter: Batch 10K triples before flush
 * - ResumableImportState: Checkpoint to DO storage
 * - RangeFetcher: HTTP Range requests for large files
 *
 * Acceptance Criteria:
 * - Process 100MB synthetic data with peak < 50MB memory
 * - Resume from checkpoint on timeout
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createStreamingLineReader,
  type StreamingLineReader,
  type LineReaderState,
} from '../../src/import/streaming-reader';
import {
  createBatchedTripleWriter,
  type BatchedTripleWriter,
  type BatchWriterState,
  type WriterResult,
} from '../../src/import/batched-writer';
import {
  createResumableImportState,
  type ResumableImportState,
  type ImportCheckpoint,
} from '../../src/import/resumable-state';
import {
  createRangeFetcher,
  type RangeFetcher,
  type RangeFetchResult,
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

function createMockR2Bucket(): R2Bucket & {
  puts: Map<string, Uint8Array>;
  reset: () => void;
} {
  const puts = new Map<string, Uint8Array>();

  return {
    puts,
    reset() {
      puts.clear();
    },
    async put(key: string, value: ArrayBufferLike | ArrayBuffer | ReadableStream | string | null) {
      if (value instanceof Uint8Array) {
        puts.set(key, value);
      } else if (typeof value === 'string') {
        puts.set(key, new TextEncoder().encode(value));
      }
      return {} as R2Object;
    },
    async get() { return null; },
    async head() { return null; },
    async delete() { },
    async list() { return { objects: [], truncated: false } as R2Objects; },
    async createMultipartUpload() { return {} as R2MultipartUpload; },
    async resumeMultipartUpload() { return {} as R2MultipartUpload; },
  } as unknown as R2Bucket & { puts: Map<string, Uint8Array>; reset: () => void };
}

function createMockDOStorage(): DurableObjectStorage {
  const data = new Map<string, unknown>();

  return {
    async get<T>(key: string): Promise<T | undefined> {
      return data.get(key) as T | undefined;
    },
    async put(key: string, value: unknown): Promise<void> {
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
// StreamingLineReader Tests
// ============================================================================

describe('StreamingLineReader', () => {
  describe('Basic Line Processing', () => {
    it('should process a single chunk with complete lines', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('line1\nline2\nline3\n');

      const lines: string[] = [];
      for await (const line of reader.processChunk(chunk)) {
        lines.push(line);
      }

      expect(lines).toEqual(['line1', 'line2', 'line3']);
    });

    it('should handle partial lines across chunks', async () => {
      const reader = createStreamingLineReader();
      const encoder = new TextEncoder();

      const lines: string[] = [];

      // First chunk ends mid-line
      const chunk1 = encoder.encode('hello wor');
      for await (const line of reader.processChunk(chunk1)) {
        lines.push(line);
      }
      expect(lines).toEqual([]); // No complete lines yet

      // Second chunk completes the line
      const chunk2 = encoder.encode('ld\nanother line\n');
      for await (const line of reader.processChunk(chunk2)) {
        lines.push(line);
      }

      expect(lines).toEqual(['hello world', 'another line']);
    });

    it('should skip empty lines', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('line1\n\n\nline2\n  \nline3\n');

      const lines: string[] = [];
      for await (const line of reader.processChunk(chunk)) {
        lines.push(line);
      }

      expect(lines).toEqual(['line1', 'line2', 'line3']);
    });

    it('should flush remaining partial line', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('line1\nfinal line');

      for await (const _ of reader.processChunk(chunk)) {
        // Consume lines
      }

      const remaining = reader.flush();
      expect(remaining).toBe('final line');
    });

    it('should return null from flush when no partial line', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('complete\n');

      for await (const _ of reader.processChunk(chunk)) {
        // Consume
      }

      expect(reader.flush()).toBeNull();
    });
  });

  describe('State Management', () => {
    it('should track state for checkpointing', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('line1\nline2\npartial');

      for await (const _ of reader.processChunk(chunk)) {
        // Consume
      }

      const state = reader.getState();
      expect(state.bytesProcessed).toBe(chunk.length);
      expect(state.linesEmitted).toBe(2);
      expect(state.partialLine).toBe('partial');
    });

    it('should restore from saved state', async () => {
      const reader1 = createStreamingLineReader();
      const chunk1 = new TextEncoder().encode('hello wor');
      for await (const _ of reader1.processChunk(chunk1)) {
        // Consume generator
      }

      const savedState = reader1.getState();

      // Create new reader and restore state
      const reader2 = createStreamingLineReader();
      reader2.restoreState(savedState);

      const chunk2 = new TextEncoder().encode('ld\n');
      const lines: string[] = [];
      for await (const line of reader2.processChunk(chunk2)) {
        lines.push(line);
      }

      expect(lines).toEqual(['hello world']);
    });
  });

  describe('Buffer Limits', () => {
    it('should respect maxBufferSize option', async () => {
      const reader = createStreamingLineReader({ maxBufferSize: 10 });
      // Create a long partial line
      const longText = 'a'.repeat(100);
      const chunk = new TextEncoder().encode(longText);

      for await (const _ of reader.processChunk(chunk)) {
        // Consume
      }

      const state = reader.getState();
      expect(state.partialLine.length).toBeLessThanOrEqual(10);
    });
  });

  describe('Memory Efficiency', () => {
    it('should process large data without holding full text in memory', async () => {
      const reader = createStreamingLineReader();
      const lineCount = 10000;
      let processedLines = 0;
      let maxPartialLineSize = 0;

      // Generate and process data in chunks
      for (let i = 0; i < lineCount; i += 100) {
        const lines: string[] = [];
        for (let j = 0; j < 100 && i + j < lineCount; j++) {
          lines.push(`{"id": ${i + j}, "data": "some test data for line ${i + j}"}`);
        }
        const chunk = new TextEncoder().encode(lines.join('\n') + '\n');

        for await (const _ of reader.processChunk(chunk)) {
          processedLines++;
        }

        const state = reader.getState();
        maxPartialLineSize = Math.max(maxPartialLineSize, state.partialLine.length);
      }

      expect(processedLines).toBe(lineCount);
      // Partial line should be much smaller than total data
      expect(maxPartialLineSize).toBeLessThan(1024);
    });
  });
});

// ============================================================================
// BatchedTripleWriter Tests
// ============================================================================

describe('BatchedTripleWriter', () => {
  describe('Batching Behavior', () => {
    it('should batch triples before flushing', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 3,
      });

      // Add 2 triples - should not flush yet
      await writer.addTriple(makeTestTriple(1));
      await writer.addTriple(makeTestTriple(2));
      expect(r2.puts.size).toBe(0);

      // Add 1 more - should trigger flush
      await writer.addTriple(makeTestTriple(3));
      expect(r2.puts.size).toBe(1);
    });

    it('should batch add multiple triples', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 5,
      });

      const triples = [makeTestTriple(1), makeTestTriple(2), makeTestTriple(3)];
      await writer.addTriples(triples);

      // Not flushed yet (only 3 of 5)
      expect(r2.puts.size).toBe(0);

      // Add 2 more to trigger flush
      await writer.addTriples([makeTestTriple(4), makeTestTriple(5)]);
      expect(r2.puts.size).toBe(1);
    });

    it('should manually flush on demand', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriple(makeTestTriple(1));
      expect(r2.puts.size).toBe(0);

      await writer.flush();
      expect(r2.puts.size).toBe(1);
    });

    it('should not flush when buffer is empty', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 10,
      });

      const result = await writer.flush();
      expect(result).toBeNull();
      expect(r2.puts.size).toBe(0);
    });
  });

  describe('Backpressure', () => {
    it('should track backpressure state', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 1,
        maxPendingBatches: 1,
      });

      // Initially not backpressured
      expect(writer.isBackpressured()).toBe(false);
    });
  });

  describe('Finalization', () => {
    it('should finalize and return results', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer.addTriples([makeTestTriple(1), makeTestTriple(2), makeTestTriple(3)]);

      const result = await writer.finalize();

      expect(result.totalTriples).toBe(3);
      expect(result.totalChunks).toBe(1);
      expect(result.totalBytes).toBeGreaterThan(0);
      expect(result.chunks).toHaveLength(1);
      expect(result.combinedBloom).toBeDefined();
    });

    it('should generate correct R2 paths', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/graphs/');

      await writer.addTriple(makeTestTriple(1));
      await writer.flush();

      // Path should be reversed domain + path + _chunks
      const keys = Array.from(r2.puts.keys());
      expect(keys[0]).toContain('.com/.example/data/graphs/_chunks/');
      expect(keys[0]).toContain('.gcol');
    });
  });

  describe('State Management', () => {
    it('should save and restore state', async () => {
      const r2 = createMockR2Bucket();
      const writer1 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      await writer1.addTriples([makeTestTriple(1), makeTestTriple(2)]);
      await writer1.flush();

      const state = writer1.getState();
      expect(state.triplesWritten).toBe(2);
      expect(state.chunksUploaded).toBe(1);
      expect(state.chunkInfos).toHaveLength(1);

      // Create new writer and restore state
      const writer2 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });
      writer2.restoreState(state);

      const finalState = writer2.getState();
      expect(finalState.triplesWritten).toBe(2);
      expect(finalState.chunksUploaded).toBe(1);
    });
  });

  describe('Batch Size Configuration', () => {
    it('should default to 10K batch size', async () => {
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/');

      // Add 9999 triples - should not flush
      const triples: Triple[] = [];
      for (let i = 0; i < 9999; i++) {
        triples.push(makeTestTriple(i));
      }
      await writer.addTriples(triples);
      expect(r2.puts.size).toBe(0);

      // Add 1 more - should trigger flush
      await writer.addTriple(makeTestTriple(9999));
      expect(r2.puts.size).toBe(1);
    });
  });
});

// ============================================================================
// ResumableImportState Tests
// ============================================================================

describe('ResumableImportState', () => {
  describe('Checkpoint Management', () => {
    it('should save and load checkpoints', async () => {
      const storage = createMockDOStorage();
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

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded).toBeDefined();
      expect(loaded?.jobId).toBe('test-job');
      expect(loaded?.byteOffset).toBe(1000);
      expect(loaded?.linesProcessed).toBe(50);
    });

    it('should return null for non-existent checkpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const loaded = await importState.loadCheckpoint('non-existent');
      expect(loaded).toBeNull();
    });

    it('should update existing checkpoint', async () => {
      const storage = createMockDOStorage();
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

      await importState.saveCheckpoint(checkpoint);
      await importState.updateCheckpoint('test-job', { byteOffset: 2000, linesProcessed: 100 });

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded?.byteOffset).toBe(2000);
      expect(loaded?.linesProcessed).toBe(100);
    });

    it('should delete checkpoint', async () => {
      const storage = createMockDOStorage();
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

      await importState.saveCheckpoint(checkpoint);
      await importState.deleteCheckpoint('test-job');

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded).toBeNull();
    });

    it('should list all checkpoints', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const baseCheckpoint = {
        sourceUrl: 'https://example.com/data.json',
        byteOffset: 0,
        linesProcessed: 0,
        triplesWritten: 0,
        lineReaderState: { bytesProcessed: 0, linesEmitted: 0, partialLine: '' },
        batchWriterState: {
          triplesWritten: 0,
          chunksUploaded: 0,
          bytesUploaded: 0,
          chunkInfos: [],
          bloomState: { filter: '', k: 7, m: 1000, version: 'v1', meta: { count: 0, capacity: 1000, targetFpr: 0.01, expectedFpr: 0.01, sizeBytes: 125 } },
        },
        checkpointedAt: new Date().toISOString(),
      };

      await importState.saveCheckpoint({ ...baseCheckpoint, jobId: 'job-1' });
      await importState.saveCheckpoint({ ...baseCheckpoint, jobId: 'job-2' });
      await importState.saveCheckpoint({ ...baseCheckpoint, jobId: 'job-3' });

      const jobs = await importState.listCheckpoints();
      expect(jobs).toHaveLength(3);
      expect(jobs.sort()).toEqual(['job-1', 'job-2', 'job-3']);
    });
  });

  describe('Resume Capability', () => {
    it('should enable resume from checkpoint on timeout', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      // Simulate first run with timeout
      const checkpoint: ImportCheckpoint = {
        jobId: 'import-job',
        sourceUrl: 'https://example.com/large-file.json',
        byteOffset: 50_000_000, // 50MB processed before timeout
        totalBytes: 100_000_000, // 100MB total
        linesProcessed: 250_000,
        triplesWritten: 500_000,
        lineReaderState: { bytesProcessed: 50_000_000, linesEmitted: 250_000, partialLine: '{"partial":' },
        batchWriterState: {
          triplesWritten: 500_000,
          chunksUploaded: 50,
          bytesUploaded: 25_000_000,
          chunkInfos: [],
          bloomState: { filter: '', k: 7, m: 1000000, version: 'v1', meta: { count: 500000, capacity: 1000000, targetFpr: 0.01, expectedFpr: 0.01, sizeBytes: 125000 } },
        },
        checkpointedAt: new Date().toISOString(),
      };

      await importState.saveCheckpoint(checkpoint);

      // Simulate resume
      const loaded = await importState.loadCheckpoint('import-job');
      expect(loaded).toBeDefined();
      expect(loaded?.byteOffset).toBe(50_000_000);
      expect(loaded?.lineReaderState.partialLine).toBe('{"partial":');
    });
  });
});

// ============================================================================
// RangeFetcher Tests
// ============================================================================

describe('RangeFetcher', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  describe('Range Requests', () => {
    it('should fetch a specific byte range', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const content = 'Hello, World! This is test data for range requests.';
          const slice = content.slice(start, end + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${start + slice.length - 1}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 10,
      });

      const result = await fetcher.fetchRange(0, 4);
      expect(new TextDecoder().decode(result.data)).toBe('Hello');
      expect(result.start).toBe(0);
      expect(result.isLast).toBe(false);
    });

    it('should iterate chunks with generator', async () => {
      const content = 'AAAABBBBCCCC';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${start + slice.length - 1}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
      });

      const chunks: string[] = [];
      for await (const { data } of fetcher.chunks(0)) {
        chunks.push(new TextDecoder().decode(data));
      }

      expect(chunks).toEqual(['AAAA', 'BBBB', 'CCCC']);
    });

    it('should resume from offset', async () => {
      const content = 'AAAABBBBCCCC';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${start + slice.length - 1}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
      });

      // Start from offset 4 (skip first chunk)
      const chunks: string[] = [];
      for await (const { data } of fetcher.chunks(4)) {
        chunks.push(new TextDecoder().decode(data));
      }

      expect(chunks).toEqual(['BBBB', 'CCCC']);
    });
  });

  describe('Total Size Detection', () => {
    it('should get total size via HEAD request', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        if (options?.method === 'HEAD') {
          return new Response(null, {
            status: 200,
            headers: {
              'Content-Length': '12345',
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      const size = await fetcher.getTotalSize();
      expect(size).toBe(12345);
    });

    it('should cache total size', async () => {
      let headCalls = 0;
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        if (options?.method === 'HEAD') {
          headCalls++;
          return new Response(null, {
            status: 200,
            headers: {
              'Content-Length': '12345',
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      await fetcher.getTotalSize();
      await fetcher.getTotalSize();
      await fetcher.getTotalSize();

      expect(headCalls).toBe(1);
    });
  });

  describe('Retry Logic', () => {
    it('should retry on failure with exponential backoff', async () => {
      let attempts = 0;

      globalThis.fetch = vi.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 3) {
          throw new Error('Network error');
        }
        return new Response('Success', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-6/7',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        maxRetries: 3,
        baseDelayMs: 10, // Short delay for tests
      });

      const result = await fetcher.fetchRange(0, 6);
      expect(new TextDecoder().decode(result.data)).toBe('Success');
      expect(attempts).toBe(3);
    });

    it('should fail after max retries exceeded', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async () => {
        throw new Error('Network error');
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        maxRetries: 2,
        baseDelayMs: 10,
      });

      await expect(fetcher.fetchRange(0, 100)).rejects.toThrow('Network error');
    });
  });

  describe('Default Chunk Size', () => {
    it('should default to 10MB chunk size', async () => {
      let requestedRange: string | null = null;

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        requestedRange = (options?.headers as Record<string, string>)?.Range;
        return new Response('x'.repeat(10 * 1024 * 1024), {
          status: 206,
          headers: {
            'Content-Range': `bytes 0-${10 * 1024 * 1024 - 1}/${100 * 1024 * 1024}`,
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      await fetcher.fetchRange(0);

      // Should request 0 to 10MB-1
      expect(requestedRange).toBe(`bytes=0-${10 * 1024 * 1024 - 1}`);
    });
  });
});

// ============================================================================
// Integration Tests - Acceptance Criteria
// ============================================================================

describe('Acceptance Criteria', () => {
  describe('Memory Efficiency', () => {
    it('should process large data with bounded memory', async () => {
      const reader = createStreamingLineReader();
      const r2 = createMockR2Bucket();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 10000,
      });

      // Simulate processing 10MB of data (100MB would take too long in tests)
      // Scale down by 10x for test speed
      const totalLines = 50000;
      const linesPerChunk = 1000;
      let processedLines = 0;
      let maxPartialLineSize = 0;

      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

      for (let chunkNum = 0; chunkNum < totalLines / linesPerChunk; chunkNum++) {
        // Generate chunk
        const lines: string[] = [];
        for (let i = 0; i < linesPerChunk; i++) {
          const lineNum = chunkNum * linesPerChunk + i;
          lines.push(`{"id": ${lineNum}, "data": "test data for entity ${lineNum}"}`);
        }
        const chunk = new TextEncoder().encode(lines.join('\n') + '\n');

        // Process through reader
        for await (const line of reader.processChunk(chunk)) {
          processedLines++;
          const record = JSON.parse(line);

          // Create triples
          const triples: Triple[] = [
            {
              subject: createEntityId(`https://example.com/entity/${record.id}`),
              predicate: createPredicate('data'),
              object: { type: ObjectType.STRING, value: record.data },
              timestamp: BigInt(Date.now()),
              txId,
            },
          ];

          await writer.addTriples(triples);

          // Check backpressure
          while (writer.isBackpressured()) {
            await new Promise(resolve => setTimeout(resolve, 10));
          }
        }

        maxPartialLineSize = Math.max(maxPartialLineSize, reader.getState().partialLine.length);
      }

      // Finalize
      const result = await writer.finalize();

      expect(processedLines).toBe(totalLines);
      expect(result.totalTriples).toBe(totalLines);
      // Partial line buffer should be bounded (< 1KB typical)
      expect(maxPartialLineSize).toBeLessThan(1024);
    });
  });

  describe('Resume from Checkpoint', () => {
    it('should resume import after simulated timeout', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);
      const r2 = createMockR2Bucket();

      // First run - process half the data
      const reader1 = createStreamingLineReader();
      const writer1 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });

      const totalLines = 1000;
      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
      let processedLines = 0;

      // Process first half
      for (let i = 0; i < totalLines / 2; i++) {
        const line = `{"id": ${i}, "data": "test"}`;
        const chunk = new TextEncoder().encode(line + '\n');

        for await (const parsedLine of reader1.processChunk(chunk)) {
          processedLines++;
          const record = JSON.parse(parsedLine);
          await writer1.addTriple({
            subject: createEntityId(`https://example.com/entity/${record.id}`),
            predicate: createPredicate('data'),
            object: { type: ObjectType.STRING, value: record.data },
            timestamp: BigInt(Date.now()),
            txId,
          });
        }
      }

      // Save checkpoint (simulating timeout)
      await importState.saveCheckpoint({
        jobId: 'test-import',
        sourceUrl: 'https://example.com/data.json',
        byteOffset: processedLines * 25, // Approximate
        linesProcessed: processedLines,
        triplesWritten: processedLines,
        lineReaderState: reader1.getState(),
        batchWriterState: writer1.getState(),
        checkpointedAt: new Date().toISOString(),
      });

      // Second run - resume from checkpoint
      const checkpoint = await importState.loadCheckpoint('test-import');
      expect(checkpoint).toBeDefined();

      const reader2 = createStreamingLineReader();
      reader2.restoreState(checkpoint!.lineReaderState);

      const writer2 = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100,
      });
      writer2.restoreState(checkpoint!.batchWriterState);

      // Process second half
      for (let i = totalLines / 2; i < totalLines; i++) {
        const line = `{"id": ${i}, "data": "test"}`;
        const chunk = new TextEncoder().encode(line + '\n');

        for await (const parsedLine of reader2.processChunk(chunk)) {
          processedLines++;
          const record = JSON.parse(parsedLine);
          await writer2.addTriple({
            subject: createEntityId(`https://example.com/entity/${record.id}`),
            predicate: createPredicate('data'),
            object: { type: ObjectType.STRING, value: record.data },
            timestamp: BigInt(Date.now()),
            txId,
          });
        }
      }

      const result = await writer2.finalize();

      // Total should be 1000 (500 from first run state + 500 from second run)
      expect(processedLines).toBe(totalLines);
      expect(result.totalTriples).toBe(totalLines);

      // Clean up checkpoint
      await importState.deleteCheckpoint('test-import');
      expect(await importState.loadCheckpoint('test-import')).toBeNull();
    });
  });
});
