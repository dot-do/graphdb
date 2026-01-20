/**
 * Tests for GraphDB Import Utilities
 *
 * Tests TSV parsing, batch encoding, bloom filter generation,
 * and manifest generation.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  createTsvParser,
  createGzipDecompressor,
  createChunkBloom,
  createChunkBloomSerialized,
  generateManifest,
  serializeManifest,
  deserializeManifest,
  createBatchEncoder,
  generateTxId,
  makeTriple,
  parseRecordToTriples,
  createStreamingLineReader,
  createBatchedTripleWriter,
  createResumableImportState,
  createRangeFetcher,
  type ChunkInfo,
  type ChunkManifest,
  type StreamingLineReader,
  type BatchedTripleWriter,
  type ResumableImportState,
  type RangeFetcher,
  type LineReaderState,
  type BatchWriterState,
  type ImportCheckpoint,
} from '../../scripts/loaders/lib/import-utils';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import type { Triple } from '../../src/core/triple';
import { mightExist, deserializeFilter } from '../../src/snippet/bloom';

// ============================================================================
// TSV Parsing Tests
// ============================================================================

/**
 * Helper function to parse TSV data and collect all records
 * Uses pipeTo pattern which works better with Cloudflare Workers runtime
 */
async function parseTsv(
  tsv: string,
  options?: Parameters<typeof createTsvParser>[0]
): Promise<Record<string, string>[]> {
  const parser = createTsvParser(options);
  const encoder = new TextEncoder();
  const results: Record<string, string>[] = [];

  // Create a readable stream from the TSV data
  const inputStream = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(tsv));
      controller.close();
    },
  });

  // Pipe through the parser and collect results
  const outputStream = new WritableStream({
    write(record) {
      results.push(record);
    },
  });

  await inputStream.pipeThrough(parser).pipeTo(outputStream);

  return results;
}

describe('TSV Parser', () => {
  describe('createTsvParser', () => {
    it('should parse simple TSV with header row', async () => {
      const tsv = 'name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLondon\n';
      const results = await parseTsv(tsv);

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ name: 'Alice', age: '30', city: 'New York' });
      expect(results[1]).toEqual({ name: 'Bob', age: '25', city: 'London' });
    });

    it('should handle quoted fields with tabs', async () => {
      const tsv = 'name\tdescription\n"John Doe"\t"A field\twith\ttabs"\n';
      const results = await parseTsv(tsv);

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({
        name: 'John Doe',
        description: 'A field\twith\ttabs',
      });
    });

    it('should handle escaped quotes in quoted fields', async () => {
      const tsv = 'name\tquote\nAlice\t"She said ""hello"""\n';
      const results = await parseTsv(tsv);

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({
        name: 'Alice',
        quote: 'She said "hello"',
      });
    });

    it('should skip empty lines', async () => {
      const tsv = 'name\tage\n\nAlice\t30\n\n\nBob\t25\n';
      const results = await parseTsv(tsv);

      expect(results).toHaveLength(2);
    });

    it('should work without header row', async () => {
      const tsv = 'Alice\t30\nBob\t25\n';
      const results = await parseTsv(tsv, { hasHeader: false });

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ col0: 'Alice', col1: '30' });
    });

    it('should use custom column names', async () => {
      const tsv = 'Alice\t30\n';
      const results = await parseTsv(tsv, {
        columns: [{ name: 'firstName' }, { name: 'years' }],
        hasHeader: false,
      });

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ firstName: 'Alice', years: '30' });
    });

    it('should handle streaming chunks correctly', async () => {
      // Test using a chunked input stream
      const parser = createTsvParser();
      const encoder = new TextEncoder();
      const results: Record<string, string>[] = [];

      const chunks = ['name\ta', 'ge\nAlice\t3', '0\nBob\t25\n'];
      let chunkIndex = 0;

      const inputStream = new ReadableStream({
        pull(controller) {
          if (chunkIndex < chunks.length) {
            controller.enqueue(encoder.encode(chunks[chunkIndex]));
            chunkIndex++;
          } else {
            controller.close();
          }
        },
      });

      const outputStream = new WritableStream({
        write(record) {
          results.push(record);
        },
      });

      await inputStream.pipeThrough(parser).pipeTo(outputStream);

      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ name: 'Alice', age: '30' });
      expect(results[1]).toEqual({ name: 'Bob', age: '25' });
    });

    it('should use custom delimiter', async () => {
      const csv = 'name,age\nAlice,30\n';
      const results = await parseTsv(csv, { delimiter: ',' });

      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ name: 'Alice', age: '30' });
    });
  });
});

// ============================================================================
// Gzip Decompressor Tests
// ============================================================================

describe('Gzip Decompressor', () => {
  describe('createGzipDecompressor', () => {
    it('should decompress gzipped data', async () => {
      const decompressor = createGzipDecompressor();

      // Create gzipped data using CompressionStream
      const original = 'Hello, World!';
      const encoder = new TextEncoder();
      const compressor = new CompressionStream('gzip');

      const compressedWriter = compressor.writable.getWriter();
      await compressedWriter.write(encoder.encode(original));
      await compressedWriter.close();

      const compressedReader = compressor.readable.getReader();
      const compressedChunks: Uint8Array[] = [];
      let compResult = await compressedReader.read();
      while (!compResult.done) {
        compressedChunks.push(compResult.value);
        compResult = await compressedReader.read();
      }

      // Combine compressed chunks
      const compressed = new Uint8Array(
        compressedChunks.reduce((sum, c) => sum + c.length, 0)
      );
      let offset = 0;
      for (const chunk of compressedChunks) {
        compressed.set(chunk, offset);
        offset += chunk.length;
      }

      // Now decompress
      const decompWriter = decompressor.writable.getWriter();
      await decompWriter.write(compressed);
      await decompWriter.close();

      const decompReader = decompressor.readable.getReader();
      const decompressedChunks: Uint8Array[] = [];
      let decompResult = await decompReader.read();
      while (!decompResult.done) {
        decompressedChunks.push(decompResult.value);
        decompResult = await decompReader.read();
      }

      // Combine and decode
      const decompressed = new Uint8Array(
        decompressedChunks.reduce((sum, c) => sum + c.length, 0)
      );
      offset = 0;
      for (const chunk of decompressedChunks) {
        decompressed.set(chunk, offset);
        offset += chunk.length;
      }

      const decoder = new TextDecoder();
      expect(decoder.decode(decompressed)).toBe(original);
    });
  });
});

// ============================================================================
// Bloom Filter Tests
// ============================================================================

describe('Bloom Filter Generation', () => {
  function createTestTriples(): Triple[] {
    const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
    const timestamp = BigInt(Date.now());

    return [
      {
        subject: createEntityId('https://example.com/users/1'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Alice' },
        timestamp,
        txId,
      },
      {
        subject: createEntityId('https://example.com/users/2'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Bob' },
        timestamp,
        txId,
      },
      {
        subject: createEntityId('https://example.com/users/1'),
        predicate: createPredicate('knows'),
        object: { type: ObjectType.REF, value: createEntityId('https://example.com/users/2') },
        timestamp,
        txId,
      },
      {
        subject: createEntityId('https://example.com/users/2'),
        predicate: createPredicate('friends'),
        object: {
          type: ObjectType.REF_ARRAY,
          value: [
            createEntityId('https://example.com/users/1'),
            createEntityId('https://example.com/users/3'),
          ],
        },
        timestamp,
        txId,
      },
    ];
  }

  describe('createChunkBloom', () => {
    it('should create a bloom filter containing subjects', () => {
      const triples = createTestTriples();
      const bloomBytes = createChunkBloom(triples);

      // Parse the serialized filter
      const serialized = JSON.parse(new TextDecoder().decode(bloomBytes));
      const filter = deserializeFilter(serialized);

      // Subjects should be in the filter
      expect(mightExist(filter, 'https://example.com/users/1')).toBe(true);
      expect(mightExist(filter, 'https://example.com/users/2')).toBe(true);
    });

    it('should include REF objects in the filter', () => {
      const triples = createTestTriples();
      const bloomBytes = createChunkBloom(triples);
      const serialized = JSON.parse(new TextDecoder().decode(bloomBytes));
      const filter = deserializeFilter(serialized);

      // REF from triple 3
      expect(mightExist(filter, 'https://example.com/users/2')).toBe(true);
    });

    it('should include REF_ARRAY objects in the filter', () => {
      const triples = createTestTriples();
      const bloomBytes = createChunkBloom(triples);
      const serialized = JSON.parse(new TextDecoder().decode(bloomBytes));
      const filter = deserializeFilter(serialized);

      // REF_ARRAY entries
      expect(mightExist(filter, 'https://example.com/users/1')).toBe(true);
      expect(mightExist(filter, 'https://example.com/users/3')).toBe(true);
    });

    it('should return false for definitely non-existent entities', () => {
      const triples = createTestTriples();
      const bloomBytes = createChunkBloom(triples);
      const serialized = JSON.parse(new TextDecoder().decode(bloomBytes));
      const filter = deserializeFilter(serialized);

      // These definitely don't exist
      expect(mightExist(filter, 'https://example.com/users/999')).toBe(false);
      expect(mightExist(filter, 'https://other.com/entity/1')).toBe(false);
    });
  });

  describe('createChunkBloomSerialized', () => {
    it('should return a SerializedFilter object', () => {
      const triples = createTestTriples();
      const serialized = createChunkBloomSerialized(triples);

      expect(serialized).toHaveProperty('filter');
      expect(serialized).toHaveProperty('k');
      expect(serialized).toHaveProperty('m');
      expect(serialized).toHaveProperty('version');
      expect(serialized).toHaveProperty('meta');
      expect(serialized.meta).toHaveProperty('count');
      expect(serialized.meta).toHaveProperty('capacity');
    });

    it('should respect custom FPR', () => {
      const triples = createTestTriples();
      const serialized1 = createChunkBloomSerialized(triples, { targetFpr: 0.01 });
      const serialized2 = createChunkBloomSerialized(triples, { targetFpr: 0.001 });

      // Lower FPR should result in larger filter
      expect(serialized2.meta.sizeBytes).toBeGreaterThanOrEqual(serialized1.meta.sizeBytes);
    });
  });
});

// ============================================================================
// Manifest Generation Tests
// ============================================================================

describe('Manifest Generation', () => {
  function createTestChunks(): ChunkInfo[] {
    return [
      {
        id: 'chunk-001',
        tripleCount: 1000,
        minTime: BigInt(1000),
        maxTime: BigInt(2000),
        bytes: 50000,
        path: '.com/.example/data/_chunks/chunk-001.gcol',
      },
      {
        id: 'chunk-002',
        tripleCount: 1500,
        minTime: BigInt(2000),
        maxTime: BigInt(3000),
        bytes: 75000,
        path: '.com/.example/data/_chunks/chunk-002.gcol',
      },
    ];
  }

  describe('generateManifest', () => {
    it('should create manifest with correct namespace', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);

      expect(manifest.namespace).toBe('https://example.com/data/');
    });

    it('should calculate total triples correctly', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);

      expect(manifest.totalTriples).toBe(2500);
    });

    it('should include all chunks', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);

      expect(manifest.chunks).toHaveLength(2);
      expect(manifest.chunks[0].id).toBe('chunk-001');
      expect(manifest.chunks[1].id).toBe('chunk-002');
    });

    it('should preserve chunk metadata', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);

      expect(manifest.chunks[0].tripleCount).toBe(1000);
      expect(manifest.chunks[0].minTime).toBe(BigInt(1000));
      expect(manifest.chunks[0].maxTime).toBe(BigInt(2000));
      expect(manifest.chunks[0].bytes).toBe(50000);
      expect(manifest.chunks[0].path).toBe('.com/.example/data/_chunks/chunk-001.gcol');
    });

    it('should set createdAt timestamp', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);

      expect(manifest.createdAt).toBeDefined();
      // Should be a valid ISO date
      expect(new Date(manifest.createdAt).getTime()).toBeGreaterThan(0);
    });

    it('should generate version string', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);

      expect(manifest.version).toBeDefined();
      expect(manifest.version).toMatch(/^v[a-z0-9]+$/);
    });

    it('should use custom version if provided', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks, {
        version: 'v1.0.0',
      });

      expect(manifest.version).toBe('v1.0.0');
    });

    it('should include combined bloom filter if provided', () => {
      const chunks = createTestChunks();
      const bloom = { filter: 'abc', k: 7, m: 1000, version: 'v1', meta: { count: 10, capacity: 100, targetFpr: 0.01, expectedFpr: 0.01, sizeBytes: 125 } };
      const manifest = generateManifest('https://example.com/data/', chunks, {
        combinedBloom: bloom,
      });

      expect(manifest.combinedBloom).toBeDefined();
      expect(manifest.combinedBloom?.k).toBe(7);
    });
  });

  describe('serializeManifest', () => {
    it('should serialize bigints to strings', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);
      const serialized = serializeManifest(manifest);

      // Parse to verify JSON is valid
      const parsed = JSON.parse(serialized);
      expect(parsed.chunks[0].minTime).toBe('1000');
      expect(parsed.chunks[0].maxTime).toBe('2000');
    });

    it('should produce valid JSON', () => {
      const chunks = createTestChunks();
      const manifest = generateManifest('https://example.com/data/', chunks);
      const serialized = serializeManifest(manifest);

      expect(() => JSON.parse(serialized)).not.toThrow();
    });
  });

  describe('deserializeManifest', () => {
    it('should restore bigints from strings', () => {
      const chunks = createTestChunks();
      const original = generateManifest('https://example.com/data/', chunks);
      const serialized = serializeManifest(original);
      const restored = deserializeManifest(serialized);

      expect(typeof restored.chunks[0].minTime).toBe('bigint');
      expect(restored.chunks[0].minTime).toBe(BigInt(1000));
    });

    it('should preserve all properties', () => {
      const chunks = createTestChunks();
      const original = generateManifest('https://example.com/data/', chunks);
      const serialized = serializeManifest(original);
      const restored = deserializeManifest(serialized);

      expect(restored.namespace).toBe(original.namespace);
      expect(restored.totalTriples).toBe(original.totalTriples);
      expect(restored.version).toBe(original.version);
      expect(restored.chunks.length).toBe(original.chunks.length);
    });
  });
});

// ============================================================================
// Batch Encoder Tests
// ============================================================================

describe('Batch Encoder', () => {
  // Mock R2Bucket
  function createMockR2Bucket(): R2Bucket & { puts: { key: string; data: ArrayBuffer }[] } {
    const puts: { key: string; data: ArrayBuffer }[] = [];

    return {
      puts,
      async put(key: string, value: ArrayBufferLike | ArrayBuffer | ReadableStream | string | null) {
        if (value instanceof Uint8Array) {
          puts.push({ key, data: value.buffer });
        } else if (typeof value === 'string') {
          puts.push({ key, data: new TextEncoder().encode(value).buffer });
        }
        return {} as R2Object;
      },
      async get() { return null; },
      async head() { return null; },
      async delete() { },
      async list() { return { objects: [], truncated: false } as R2Objects; },
      async createMultipartUpload() { return {} as R2MultipartUpload; },
      async resumeMultipartUpload() { return {} as R2MultipartUpload; },
    } as unknown as R2Bucket & { puts: { key: string; data: ArrayBuffer }[] };
  }

  describe('createBatchEncoder', () => {
    it('should track statistics correctly', async () => {
      const r2 = createMockR2Bucket();
      const encoder = createBatchEncoder(r2, 'https://example.com/test/', {
        batchSize: 2,
      });

      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
      const timestamp = BigInt(Date.now());

      encoder.add({
        subject: createEntityId('https://example.com/e/1'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp,
        txId,
      });

      encoder.add({
        subject: createEntityId('https://example.com/e/2'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test2' },
        timestamp,
        txId,
      });

      await encoder.flush();

      const stats = encoder.getStats();
      expect(stats.chunks).toBe(1);
      expect(stats.triples).toBe(2);
      expect(stats.bytes).toBeGreaterThan(0);
    });

    it('should write chunks to R2', async () => {
      const r2 = createMockR2Bucket();
      const encoder = createBatchEncoder(r2, 'https://example.com/test/');

      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

      encoder.add({
        subject: createEntityId('https://example.com/e/1'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp: BigInt(Date.now()),
        txId,
      });

      await encoder.flush();

      expect(r2.puts.length).toBe(1);
      expect(r2.puts[0].key).toContain('.gcol');
    });

    it('should generate correct R2 paths', async () => {
      const r2 = createMockR2Bucket();
      const encoder = createBatchEncoder(r2, 'https://example.com/data/graphs/');

      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

      encoder.add({
        subject: createEntityId('https://example.com/e/1'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp: BigInt(Date.now()),
        txId,
      });

      await encoder.flush();

      // Path should be reversed domain + path + _chunks
      expect(r2.puts[0].key).toContain('.com/.example/data/graphs/_chunks/');
    });

    it('should finalize and write manifest', async () => {
      const r2 = createMockR2Bucket();
      const encoder = createBatchEncoder(r2, 'https://example.com/test/');

      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

      encoder.add({
        subject: createEntityId('https://example.com/e/1'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp: BigInt(Date.now()),
        txId,
      });

      const manifest = await encoder.finalize();

      // Should have chunk + manifest
      expect(r2.puts.length).toBe(2);
      expect(r2.puts.some(p => p.key.endsWith('_manifest.json'))).toBe(true);
      expect(manifest.totalTriples).toBe(1);
    });

    it('should not flush when buffer is empty', async () => {
      const r2 = createMockR2Bucket();
      const encoder = createBatchEncoder(r2, 'https://example.com/test/');

      await encoder.flush();

      expect(r2.puts.length).toBe(0);
    });
  });
});

// ============================================================================
// Helper Function Tests
// ============================================================================

describe('Helper Functions', () => {
  describe('generateTxId', () => {
    it('should generate valid ULID format', () => {
      const txId = generateTxId();

      // ULID is 26 characters, Crockford Base32
      expect(txId.length).toBe(26);
      expect(txId).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/);
    });

    it('should generate unique IDs', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        ids.add(generateTxId());
      }
      expect(ids.size).toBe(100);
    });
  });

  describe('makeTriple', () => {
    it('should create triple with string value', () => {
      const triple = makeTriple(
        'https://example.com/e/1',
        'name',
        'Test Value'
      );

      expect(triple.subject).toBe('https://example.com/e/1');
      expect(triple.predicate).toBe('name');
      expect(triple.object.type).toBe(ObjectType.STRING);
      expect((triple.object as { value: string }).value).toBe('Test Value');
    });

    it('should create triple with number value', () => {
      const triple = makeTriple(
        'https://example.com/e/1',
        'age',
        42
      );

      expect(triple.object.type).toBe(ObjectType.INT64);
      expect((triple.object as { value: bigint }).value).toBe(BigInt(42));
    });

    it('should create triple with float value', () => {
      const triple = makeTriple(
        'https://example.com/e/1',
        'rating',
        4.5
      );

      expect(triple.object.type).toBe(ObjectType.FLOAT64);
      expect((triple.object as { value: number }).value).toBe(4.5);
    });

    it('should create triple with explicit REF type', () => {
      const triple = makeTriple(
        'https://example.com/e/1',
        'knows',
        'https://example.com/e/2',
        ObjectType.REF
      );

      expect(triple.object.type).toBe(ObjectType.REF);
    });

    it('should use provided timestamp and txId', () => {
      const timestamp = BigInt(1234567890);
      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

      const triple = makeTriple(
        'https://example.com/e/1',
        'name',
        'Test',
        undefined,
        { timestamp, txId }
      );

      expect(triple.timestamp).toBe(timestamp);
      expect(triple.txId).toBe(txId);
    });

    it('should infer REF type from URL string', () => {
      const triple = makeTriple(
        'https://example.com/e/1',
        'knows',
        'https://example.com/e/2'
      );

      expect(triple.object.type).toBe(ObjectType.REF);
    });

    it('should infer GeoPoint from lat/lng object', () => {
      const triple = makeTriple(
        'https://example.com/e/1',
        'location',
        { lat: 37.7749, lng: -122.4194 }
      );

      expect(triple.object.type).toBe(ObjectType.GEO_POINT);
    });
  });

  describe('parseRecordToTriples', () => {
    it('should convert record fields to triples', () => {
      const record = { name: 'Alice', age: '30', city: 'NYC' };
      const triples = parseRecordToTriples(record, {
        getSubject: (r) => `https://example.com/users/${r.name}`,
      });

      expect(triples).toHaveLength(3);
      expect(triples.every(t => t.subject === 'https://example.com/users/Alice')).toBe(true);
      expect(triples.map(t => t.predicate).sort()).toEqual(['age', 'city', 'name']);
    });

    it('should skip empty values', () => {
      const record = { name: 'Alice', age: '', city: 'NYC' };
      const triples = parseRecordToTriples(record, {
        getSubject: () => 'https://example.com/users/1',
      });

      expect(triples).toHaveLength(2);
      expect(triples.map(t => t.predicate).sort()).toEqual(['city', 'name']);
    });

    it('should skip specified fields', () => {
      const record = { name: 'Alice', age: '30', id: '123' };
      const triples = parseRecordToTriples(record, {
        getSubject: () => 'https://example.com/users/1',
        skipFields: ['id'],
      });

      expect(triples).toHaveLength(2);
      expect(triples.map(t => t.predicate).sort()).toEqual(['age', 'name']);
    });

    it('should map predicate names', () => {
      const record = { firstName: 'Alice', lastName: 'Smith' };
      const triples = parseRecordToTriples(record, {
        getSubject: () => 'https://example.com/users/1',
        mapPredicate: (field) => {
          if (field === 'firstName') return 'givenName';
          if (field === 'lastName') return 'familyName';
          return field;
        },
      });

      expect(triples.map(t => t.predicate).sort()).toEqual(['familyName', 'givenName']);
    });

    it('should skip predicates with null mapping', () => {
      const record = { name: 'Alice', internal_id: '123' };
      const triples = parseRecordToTriples(record, {
        getSubject: () => 'https://example.com/users/1',
        mapPredicate: (field) => {
          if (field.startsWith('internal_')) return null;
          return field;
        },
      });

      expect(triples).toHaveLength(1);
      expect(triples[0].predicate).toBe('name');
    });

    it('should apply custom value mapping', () => {
      const record = { age: '30', isActive: 'true' };
      const triples = parseRecordToTriples(record, {
        getSubject: () => 'https://example.com/users/1',
        mapValue: (field, value) => {
          if (field === 'age') {
            return { type: ObjectType.INT64, value: BigInt(parseInt(value)) };
          }
          if (field === 'isActive') {
            return { type: ObjectType.BOOL, value: value === 'true' };
          }
          return { type: ObjectType.STRING, value };
        },
      });

      const ageTriple = triples.find(t => t.predicate === 'age');
      const activeTriple = triples.find(t => t.predicate === 'isActive');

      expect(ageTriple?.object.type).toBe(ObjectType.INT64);
      expect(activeTriple?.object.type).toBe(ObjectType.BOOL);
    });
  });
});

// ============================================================================
// StreamingLineReader Tests
// ============================================================================

describe('StreamingLineReader', () => {
  describe('createStreamingLineReader', () => {
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

    it('should track state for checkpointing', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('line1\nline2\npartial');

      const lines: string[] = [];
      for await (const line of reader.processChunk(chunk)) {
        lines.push(line);
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

    it('should skip empty lines', async () => {
      const reader = createStreamingLineReader();
      const chunk = new TextEncoder().encode('line1\n\n\nline2\n  \nline3\n');

      const lines: string[] = [];
      for await (const line of reader.processChunk(chunk)) {
        lines.push(line);
      }

      expect(lines).toEqual(['line1', 'line2', 'line3']);
    });

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
});

// ============================================================================
// BatchedTripleWriter Tests
// ============================================================================

describe('BatchedTripleWriter', () => {
  // Mock R2Bucket for tests
  function createMockR2(): R2Bucket & { puts: Map<string, Uint8Array> } {
    const puts = new Map<string, Uint8Array>();

    return {
      puts,
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
    } as unknown as R2Bucket & { puts: Map<string, Uint8Array> };
  }

  function makeTestTriple(id: number): Triple {
    return {
      subject: createEntityId(`https://example.com/entity/${id}`),
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `Entity ${id}` },
      timestamp: BigInt(Date.now()),
      txId: createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
    };
  }

  describe('createBatchedTripleWriter', () => {
    it('should batch triples before flushing', async () => {
      const r2 = createMockR2();
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
      const r2 = createMockR2();
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
      const r2 = createMockR2();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 100, // High batch size
      });

      await writer.addTriple(makeTestTriple(1));
      expect(r2.puts.size).toBe(0);

      await writer.flush();
      expect(r2.puts.size).toBe(1);
    });

    it('should track backpressure', async () => {
      const r2 = createMockR2();
      const writer = createBatchedTripleWriter(r2, 'https://example.com/data/', {
        batchSize: 1,
        maxPendingBatches: 1,
      });

      // Initially not backpressured
      expect(writer.isBackpressured()).toBe(false);
    });

    it('should finalize and return results', async () => {
      const r2 = createMockR2();
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

    it('should save and restore state', async () => {
      const r2 = createMockR2();
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
});

// ============================================================================
// ResumableImportState Tests
// ============================================================================

describe('ResumableImportState', () => {
  // Mock DurableObjectStorage
  function createMockStorage(): DurableObjectStorage {
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
      // Other methods not needed for tests
    } as unknown as DurableObjectStorage;
  }

  describe('createResumableImportState', () => {
    it('should save and load checkpoints', async () => {
      const storage = createMockStorage();
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
      const storage = createMockStorage();
      const importState = createResumableImportState(storage);

      const loaded = await importState.loadCheckpoint('non-existent');
      expect(loaded).toBeNull();
    });

    it('should update existing checkpoint', async () => {
      const storage = createMockStorage();
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
      const storage = createMockStorage();
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
      const storage = createMockStorage();
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
});

// ============================================================================
// RangeFetcher Tests
// ============================================================================

describe('RangeFetcher', () => {
  describe('createRangeFetcher', () => {
    it('should fetch a specific byte range', async () => {
      // Mock fetch for range requests
      const originalFetch = globalThis.fetch;
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

      try {
        const fetcher = createRangeFetcher('https://example.com/data.txt', {
          chunkSize: 10,
        });

        const result = await fetcher.fetchRange(0, 4);
        expect(new TextDecoder().decode(result.data)).toBe('Hello');
        expect(result.start).toBe(0);
        expect(result.isLast).toBe(false);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    it('should iterate chunks with generator', async () => {
      const originalFetch = globalThis.fetch;
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

      try {
        const fetcher = createRangeFetcher('https://example.com/data.txt', {
          chunkSize: 4,
        });

        const chunks: string[] = [];
        for await (const { data } of fetcher.chunks(0)) {
          chunks.push(new TextDecoder().decode(data));
        }

        expect(chunks).toEqual(['AAAA', 'BBBB', 'CCCC']);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    it('should get total size via HEAD request', async () => {
      const originalFetch = globalThis.fetch;

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

      try {
        const fetcher = createRangeFetcher('https://example.com/data.txt');
        const size = await fetcher.getTotalSize();
        expect(size).toBe(12345);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    it('should retry on failure with exponential backoff', async () => {
      const originalFetch = globalThis.fetch;
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

      try {
        const fetcher = createRangeFetcher('https://example.com/data.txt', {
          maxRetries: 3,
          baseDelayMs: 10, // Short delay for tests
        });

        const result = await fetcher.fetchRange(0, 6);
        expect(new TextDecoder().decode(result.data)).toBe('Success');
        expect(attempts).toBe(3);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });
  });
});
