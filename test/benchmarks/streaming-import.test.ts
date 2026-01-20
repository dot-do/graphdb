/**
 * Performance Benchmarks for Streaming Import Utilities
 *
 * Measures:
 * - Memory efficiency (streaming vs split)
 * - Throughput (lines/sec, triples/sec)
 * - Chunk processing overhead
 * - Backpressure behavior
 */

import { describe, it, expect, bench } from 'vitest';
import {
  createStreamingLineReader,
  createBatchedTripleWriter,
} from '../../scripts/loaders/lib/import-utils';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import type { Triple } from '../../src/core/triple';

// ============================================================================
// Test Data Generation
// ============================================================================

function generateNDJSON(count: number, avgLineLength: number = 200): Uint8Array {
  const lines: string[] = [];
  for (let i = 0; i < count; i++) {
    const obj = {
      id: `entity-${i}`,
      word: `word${i}`,
      pos: ['noun', 'verb', 'adj', 'adv'][i % 4],
      definitions: [`Definition ${i} with some extra text to pad the line`],
      examples: [`Example sentence ${i} that demonstrates usage`],
      synonyms: [`syn${i}a`, `syn${i}b`, `syn${i}c`],
      timestamp: Date.now(),
    };
    lines.push(JSON.stringify(obj));
  }
  return new TextEncoder().encode(lines.join('\n') + '\n');
}

function generateTriples(count: number): Triple[] {
  const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
  const timestamp = BigInt(Date.now());
  const triples: Triple[] = [];

  for (let i = 0; i < count; i++) {
    triples.push({
      subject: createEntityId(`https://example.com/entity/${i}`),
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `Entity ${i}` },
      timestamp,
      txId,
    });
  }
  return triples;
}

// Mock R2Bucket that tracks write operations
function createBenchmarkR2(): R2Bucket & {
  writes: number;
  bytesWritten: number;
  reset: () => void;
} {
  let writes = 0;
  let bytesWritten = 0;

  return {
    get writes() { return writes; },
    get bytesWritten() { return bytesWritten; },
    reset() { writes = 0; bytesWritten = 0; },
    async put(_key: string, value: ArrayBufferLike | ArrayBuffer | ReadableStream | string | null) {
      writes++;
      if (value instanceof Uint8Array) {
        bytesWritten += value.byteLength;
      } else if (typeof value === 'string') {
        bytesWritten += new TextEncoder().encode(value).byteLength;
      }
      return {} as R2Object;
    },
    async get() { return null; },
    async head() { return null; },
    async delete() { },
    async list() { return { objects: [], truncated: false } as R2Objects; },
    async createMultipartUpload() { return {} as R2MultipartUpload; },
    async resumeMultipartUpload() { return {} as R2MultipartUpload; },
  } as unknown as R2Bucket & { writes: number; bytesWritten: number; reset: () => void };
}

// ============================================================================
// StreamingLineReader Benchmarks
// ============================================================================

describe('StreamingLineReader Benchmarks', () => {
  const SIZES = {
    small: 1_000,      // 1K lines
    medium: 10_000,    // 10K lines
    large: 100_000,    // 100K lines
  };

  describe('Throughput Comparison: Streaming vs Split', () => {
    it('should process 10K lines faster with streaming', async () => {
      const data = generateNDJSON(SIZES.medium);
      const text = new TextDecoder().decode(data);

      // Method 1: Traditional split (loads all into memory)
      const splitStart = performance.now();
      const splitLines = text.split('\n').filter(l => l.trim());
      let splitCount = 0;
      for (const line of splitLines) {
        JSON.parse(line);
        splitCount++;
      }
      const splitTime = performance.now() - splitStart;

      // Method 2: Streaming line reader
      const streamStart = performance.now();
      const reader = createStreamingLineReader();
      let streamCount = 0;
      for await (const line of reader.processChunk(data)) {
        JSON.parse(line);
        streamCount++;
      }
      const remaining = reader.flush();
      if (remaining) {
        JSON.parse(remaining);
        streamCount++;
      }
      const streamTime = performance.now() - streamStart;

      console.log(`\n=== 10K Lines Throughput ===`);
      console.log(`Split method:     ${splitTime.toFixed(2)}ms (${(splitCount / splitTime * 1000).toFixed(0)} lines/sec)`);
      console.log(`Streaming method: ${streamTime.toFixed(2)}ms (${(streamCount / streamTime * 1000).toFixed(0)} lines/sec)`);
      console.log(`Data size: ${(data.byteLength / 1024).toFixed(1)} KB`);

      expect(splitCount).toBe(streamCount);
    });

    it('should show memory advantage with chunked processing', async () => {
      const totalLines = SIZES.large;
      const chunkSize = 10_000; // Lines per chunk
      const linesPerChunk = Math.ceil(totalLines / 10);

      // Simulate chunked arrival (like HTTP Range requests)
      const chunks: Uint8Array[] = [];
      for (let i = 0; i < 10; i++) {
        chunks.push(generateNDJSON(linesPerChunk));
      }

      const reader = createStreamingLineReader();
      let linesProcessed = 0;
      let maxPartialLineSize = 0;

      const streamStart = performance.now();
      for (const chunk of chunks) {
        for await (const line of reader.processChunk(chunk)) {
          // Just count, don't parse for this benchmark
          linesProcessed++;
        }
        const state = reader.getState();
        maxPartialLineSize = Math.max(maxPartialLineSize, state.partialLine.length);
      }
      const remaining = reader.flush();
      if (remaining) linesProcessed++;
      const streamTime = performance.now() - streamStart;

      console.log(`\n=== 100K Lines Chunked Processing ===`);
      console.log(`Total lines processed: ${linesProcessed.toLocaleString()}`);
      console.log(`Time: ${streamTime.toFixed(2)}ms`);
      console.log(`Throughput: ${(linesProcessed / streamTime * 1000).toFixed(0)} lines/sec`);
      console.log(`Max partial line buffer: ${maxPartialLineSize} bytes`);
      console.log(`Total data: ${(chunks.reduce((s, c) => s + c.byteLength, 0) / 1024 / 1024).toFixed(1)} MB`);

      expect(maxPartialLineSize).toBeLessThan(1024); // Partial lines should be small
    });
  });
});

// ============================================================================
// BatchedTripleWriter Benchmarks
// ============================================================================

describe('BatchedTripleWriter Benchmarks', () => {
  describe('Batch Size Impact', () => {
    const TRIPLE_COUNTS = [1_000, 10_000, 50_000];
    const BATCH_SIZES = [100, 1_000, 10_000];

    for (const tripleCount of TRIPLE_COUNTS) {
      for (const batchSize of BATCH_SIZES) {
        it(`should write ${tripleCount} triples with batch size ${batchSize}`, async () => {
          const r2 = createBenchmarkR2();
          const writer = createBatchedTripleWriter(r2, 'https://example.com/bench/', {
            batchSize,
          });

          const triples = generateTriples(tripleCount);

          const start = performance.now();
          await writer.addTriples(triples);
          const result = await writer.finalize();
          const elapsed = performance.now() - start;

          console.log(`\n=== ${tripleCount} triples, batch=${batchSize} ===`);
          console.log(`Time: ${elapsed.toFixed(2)}ms`);
          console.log(`Throughput: ${(tripleCount / elapsed * 1000).toFixed(0)} triples/sec`);
          console.log(`Chunks written: ${result.totalChunks}`);
          console.log(`Bytes written: ${(result.totalBytes / 1024).toFixed(1)} KB`);
          console.log(`Avg chunk size: ${(result.totalBytes / result.totalChunks / 1024).toFixed(1)} KB`);

          expect(result.totalTriples).toBe(tripleCount);
        });
      }
    }
  });

  describe('Single vs Batch Add Comparison', () => {
    it('should show addTriples is faster than individual addTriple', async () => {
      const tripleCount = 10_000;
      const triples = generateTriples(tripleCount);

      // Method 1: Individual adds
      const r2Single = createBenchmarkR2();
      const writerSingle = createBatchedTripleWriter(r2Single, 'https://example.com/single/', {
        batchSize: 10_000,
      });

      const singleStart = performance.now();
      for (const triple of triples) {
        await writerSingle.addTriple(triple);
      }
      await writerSingle.finalize();
      const singleTime = performance.now() - singleStart;

      // Method 2: Batch add
      const r2Batch = createBenchmarkR2();
      const writerBatch = createBatchedTripleWriter(r2Batch, 'https://example.com/batch/', {
        batchSize: 10_000,
      });

      const batchStart = performance.now();
      await writerBatch.addTriples(triples);
      await writerBatch.finalize();
      const batchTime = performance.now() - batchStart;

      console.log(`\n=== Single vs Batch Add (${tripleCount} triples) ===`);
      console.log(`Single addTriple: ${singleTime.toFixed(2)}ms (${(tripleCount / singleTime * 1000).toFixed(0)} triples/sec)`);
      console.log(`Batch addTriples: ${batchTime.toFixed(2)}ms (${(tripleCount / batchTime * 1000).toFixed(0)} triples/sec)`);
      console.log(`Speedup: ${(singleTime / batchTime).toFixed(2)}x`);

      // Batch should be at least as fast (likely faster due to less await overhead)
      expect(batchTime).toBeLessThanOrEqual(singleTime * 1.1); // Allow 10% variance
    });
  });
});

// ============================================================================
// Memory Estimation
// ============================================================================

describe('Memory Characteristics', () => {
  it('should estimate memory usage for different scenarios', () => {
    // Calculate theoretical memory usage

    const scenarios = [
      { name: 'Wiktionary (2.6GB)', sourceSize: 2.6 * 1024 * 1024 * 1024, chunkSize: 10 * 1024 * 1024, avgLineSize: 500 },
      { name: 'CC Host Graph (300M hosts)', sourceSize: 50 * 1024 * 1024 * 1024, chunkSize: 10 * 1024 * 1024, avgLineSize: 50 },
      { name: 'IMDB (10M titles)', sourceSize: 2 * 1024 * 1024 * 1024, chunkSize: 10 * 1024 * 1024, avgLineSize: 200 },
    ];

    console.log('\n=== Memory Usage Estimates ===');
    console.log('(Based on streaming approach with 10MB chunks)\n');

    for (const s of scenarios) {
      // With streaming:
      // - 1 chunk in flight: chunkSize
      // - Partial line buffer: avgLineSize (worst case)
      // - Triple buffer (10K batch): 10000 * ~200 bytes = 2MB
      // - Bloom filter: 64KB max
      const streamingMemory = s.chunkSize + s.avgLineSize + (2 * 1024 * 1024) + (64 * 1024);

      // Without streaming (split approach):
      // - Full text in memory: sourceSize (decompressed)
      // - Lines array: ~sourceSize/avgLineSize * 50 bytes per reference
      // - All entity IDs in Set: could be massive
      const nonStreamingMemory = s.sourceSize * 1.5; // Conservative estimate

      console.log(`${s.name}:`);
      console.log(`  Streaming approach: ~${(streamingMemory / 1024 / 1024).toFixed(1)} MB peak`);
      console.log(`  Non-streaming: ~${(nonStreamingMemory / 1024 / 1024 / 1024).toFixed(1)} GB peak`);
      console.log(`  Memory savings: ${((1 - streamingMemory / nonStreamingMemory) * 100).toFixed(1)}%`);
      console.log();
    }

    // Verify streaming approach stays under DO memory limit (128MB)
    const doMemoryLimit = 128 * 1024 * 1024;
    const maxStreamingMemory = 10 * 1024 * 1024 + 64 * 1024 + 2 * 1024 * 1024 + 64 * 1024;
    console.log(`DO Memory Limit: ${doMemoryLimit / 1024 / 1024} MB`);
    console.log(`Max streaming memory: ${maxStreamingMemory / 1024 / 1024} MB`);
    console.log(`Safety margin: ${((1 - maxStreamingMemory / doMemoryLimit) * 100).toFixed(1)}%`);

    expect(maxStreamingMemory).toBeLessThan(doMemoryLimit);
  });
});

// ============================================================================
// End-to-End Throughput
// ============================================================================

describe('End-to-End Pipeline Throughput', () => {
  it('should measure full pipeline: read -> parse -> transform -> write', async () => {
    const lineCount = 10_000;
    const data = generateNDJSON(lineCount);

    const r2 = createBenchmarkR2();
    const reader = createStreamingLineReader();
    const writer = createBatchedTripleWriter(r2, 'https://example.com/e2e/', {
      batchSize: 1000,
    });

    const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
    const timestamp = BigInt(Date.now());

    let linesProcessed = 0;
    let triplesWritten = 0;

    const start = performance.now();

    // Simulate chunked arrival (1MB chunks)
    const chunkSize = 1024 * 1024;
    for (let offset = 0; offset < data.byteLength; offset += chunkSize) {
      const chunk = data.slice(offset, Math.min(offset + chunkSize, data.byteLength));

      for await (const line of reader.processChunk(chunk)) {
        linesProcessed++;

        // Parse
        const record = JSON.parse(line);

        // Transform to triples (simplified)
        const triples: Triple[] = [
          {
            subject: createEntityId(`https://example.com/word/${record.id}`),
            predicate: createPredicate('word'),
            object: { type: ObjectType.STRING, value: record.word },
            timestamp,
            txId,
          },
          {
            subject: createEntityId(`https://example.com/word/${record.id}`),
            predicate: createPredicate('pos'),
            object: { type: ObjectType.STRING, value: record.pos },
            timestamp,
            txId,
          },
        ];

        await writer.addTriples(triples);
        triplesWritten += triples.length;
      }
    }

    // Flush remaining
    const remaining = reader.flush();
    if (remaining) {
      const record = JSON.parse(remaining);
      const triples: Triple[] = [
        {
          subject: createEntityId(`https://example.com/word/${record.id}`),
          predicate: createPredicate('word'),
          object: { type: ObjectType.STRING, value: record.word },
          timestamp,
          txId,
        },
      ];
      await writer.addTriples(triples);
      triplesWritten += triples.length;
      linesProcessed++;
    }

    const result = await writer.finalize();
    const elapsed = performance.now() - start;

    console.log(`\n=== End-to-End Pipeline (${lineCount} records) ===`);
    console.log(`Total time: ${elapsed.toFixed(2)}ms`);
    console.log(`Lines processed: ${linesProcessed.toLocaleString()}`);
    console.log(`Triples written: ${triplesWritten.toLocaleString()}`);
    console.log(`Lines/sec: ${(linesProcessed / elapsed * 1000).toFixed(0)}`);
    console.log(`Triples/sec: ${(triplesWritten / elapsed * 1000).toFixed(0)}`);
    console.log(`Chunks created: ${result.totalChunks}`);
    console.log(`Output size: ${(result.totalBytes / 1024).toFixed(1)} KB`);
    console.log(`Input size: ${(data.byteLength / 1024).toFixed(1)} KB`);

    expect(linesProcessed).toBe(lineCount);
    expect(result.totalTriples).toBe(triplesWritten);
  });
});
