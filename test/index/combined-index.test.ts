/**
 * Combined Index File Format Tests (TDD RED Phase)
 *
 * These tests cover the combined index file format which stores all secondary
 * indexes (POS, OSP, FTS, GEO, VEC) in a single file with an offset header.
 * This enables efficient HTTP Range requests to load only needed indexes.
 *
 * Test categories:
 * 1. Encoding Tests - validate binary encoding produces correct format
 * 2. Decoding Tests - validate parsing and round-trip correctness
 * 3. Range Request Tests - validate byte range planning and coalescing
 * 4. Quantized Vector Tests - validate vector compression and similarity
 *
 * @see src/index/combined-index.ts for implementation
 */

import { describe, it, expect, beforeEach, beforeAll, afterAll } from 'vitest';
import {
  // Constants
  GIDX_MAGIC,
  GIDX_MAGIC_FOOTER,
  GIDX_VERSION,
  HEADER_SIZE,
  DIRECTORY_ENTRY_SIZE,
  FOOTER_SIZE,
  QVEC_MAGIC,
  QVEC_HEADER_SIZE,
  // Enums
  IndexType,
  Compression,
  VectorQuantization,
  // Types
  type IndexDirectoryEntry,
  type CombinedIndexHeader,
  type CombinedIndexData,
  type IndexHeaderInfo,
  type QuantizedVectorHeader,
  type QuantizedVectorFile,
  // Encoding functions
  encodeCombinedIndex,
  // Decoding functions
  decodeIndexHeader,
  decodeIndexSection,
  decodeCombinedIndex,
  // Range request helpers
  getHeaderRange,
  planRangeRequests,
  coalesceRanges,
  // Quantized vector functions
  encodeQuantizedVectors,
  decodeQuantizedVectorHeader,
  decodeQuantizedVectors,
  getVectorFloat32,
  cosineSimilarity,
  hammingDistance,
} from '../../src/index/combined-index.js';
// Test utilities (internal, not part of public API)
import { suppressVectorIndexWarnings } from './test-utils.js';

// Suppress experimental warnings during tests
beforeAll(() => {
  suppressVectorIndexWarnings(true);
});

afterAll(() => {
  suppressVectorIndexWarnings(false);
});

import type { POSIndex, OSPIndex, FTSIndex, GeoIndex, VectorIndex, VectorIndexEntry } from '../../src/index/index-store.js';

// ============================================================================
// TEST FIXTURES
// ============================================================================

/**
 * Create a minimal POS index for testing
 */
function createTestPOSIndex(): POSIndex {
  return {
    version: 'v1',
    entries: {
      'name': ['https://example.com/person/1', 'https://example.com/person/2'],
      'age:25': ['https://example.com/person/1'],
      'age:30': ['https://example.com/person/2'],
    },
  };
}

/**
 * Create a minimal OSP index for testing
 */
function createTestOSPIndex(): OSPIndex {
  return {
    version: 'v1',
    entries: {
      'https://example.com/company/1': ['https://example.com/person/1', 'https://example.com/person/2'],
      'https://example.com/company/2': ['https://example.com/person/3'],
    },
  };
}

/**
 * Create a minimal FTS index for testing
 */
function createTestFTSIndex(): FTSIndex {
  return {
    version: 'v1',
    documentCount: 10,
    terms: {
      'hello': [
        { entityId: 'https://example.com/doc/1', predicate: 'content', score: 1.5 },
        { entityId: 'https://example.com/doc/2', predicate: 'title', score: 0.8 },
      ],
      'world': [
        { entityId: 'https://example.com/doc/1', predicate: 'content', score: 1.2 },
      ],
    },
  };
}

/**
 * Create a minimal GEO index for testing
 */
function createTestGeoIndex(): GeoIndex {
  return {
    version: 'v1',
    precision: 6,
    cells: {
      '9q8yyz': ['https://example.com/place/sf'],
      'u4pru': ['https://example.com/place/london'],
      'xn76ur': ['https://example.com/place/tokyo'],
    },
  };
}

/**
 * Create a minimal Vector index for testing
 */
function createTestVectorIndex(): VectorIndex {
  return {
    version: 'v1',
    dimensions: 4,
    m: 16,
    efConstruction: 200,
    entries: [
      {
        entityId: 'https://example.com/doc/1',
        predicate: 'embedding',
        vector: [0.1, 0.2, 0.3, 0.4],
        connections: [['https://example.com/doc/2']],
      },
      {
        entityId: 'https://example.com/doc/2',
        predicate: 'embedding',
        vector: [0.5, 0.6, 0.7, 0.8],
        connections: [['https://example.com/doc/1']],
      },
    ],
  };
}

/**
 * Create test vectors for quantization testing
 */
function createTestVectors(count: number, dimensions: number): Array<{ entityId: string; vector: number[] }> {
  const vectors: Array<{ entityId: string; vector: number[] }> = [];
  for (let i = 0; i < count; i++) {
    const vector: number[] = [];
    for (let j = 0; j < dimensions; j++) {
      // Create vectors with values between -1 and 1 (typical for embeddings)
      vector.push(Math.sin(i * dimensions + j) * 0.9);
    }
    vectors.push({
      entityId: `https://example.com/entity/${i}`,
      vector,
    });
  }
  return vectors;
}

// ============================================================================
// ENCODING TESTS
// ============================================================================

describe('Combined Index Encoding', () => {
  describe('encodeCombinedIndex', () => {
    it('should produce valid binary with correct magic bytes', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test-namespace', indexes);

      // Check that output is a Uint8Array
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(HEADER_SIZE + FOOTER_SIZE);

      // Check magic bytes at start
      const view = new DataView(encoded.buffer, encoded.byteOffset);
      const magic = view.getUint32(0, true);
      expect(magic).toBe(GIDX_MAGIC);

      // Check footer magic bytes
      const footerMagic = view.getUint32(encoded.length - 4, true);
      expect(footerMagic).toBe(GIDX_MAGIC_FOOTER);
    });

    it('should encode header with correct version', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test-namespace', indexes);
      const view = new DataView(encoded.buffer, encoded.byteOffset);

      const version = view.getUint16(4, true);
      expect(version).toBe(GIDX_VERSION);
    });

    it('should encode correct index count in header', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
        fts: createTestFTSIndex(),
      };

      const encoded = await encodeCombinedIndex('test-namespace', indexes);
      const view = new DataView(encoded.buffer, encoded.byteOffset);

      const indexCount = view.getUint32(8, true);
      expect(indexCount).toBe(3);
    });

    it('should encode total size correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test-namespace', indexes);
      const view = new DataView(encoded.buffer, encoded.byteOffset);

      const totalSize = Number(view.getBigUint64(12, true));
      expect(totalSize).toBe(encoded.length);
    });

    it('should encode namespace correctly', async () => {
      const namespace = 'my-test-namespace';
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex(namespace, indexes);
      const view = new DataView(encoded.buffer, encoded.byteOffset);

      // Namespace length is at offset 28 (after magic, version, flags, indexCount, totalSize, createdAt)
      const namespaceLength = view.getUint16(28, true);
      expect(namespaceLength).toBe(namespace.length);

      // Read namespace bytes (starts at HEADER_SIZE = 64)
      const namespaceBytes = encoded.subarray(HEADER_SIZE, HEADER_SIZE + namespaceLength);
      const decodedNamespace = new TextDecoder().decode(namespaceBytes);
      expect(decodedNamespace).toBe(namespace);
    });

    it('should have directory entries with correct offsets and sizes', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.directory.length).toBe(2);

      // Check that offsets are sequential and sizes are positive
      for (let i = 0; i < headerInfo.header.directory.length; i++) {
        const entry = headerInfo.header.directory[i]!;
        expect(entry.offset).toBeGreaterThanOrEqual(headerInfo.headerSize);
        expect(entry.compressedSize).toBeGreaterThan(0);

        // Each subsequent entry should start after the previous one
        if (i > 0) {
          const prevEntry = headerInfo.header.directory[i - 1]!;
          expect(entry.offset).toBe(prevEntry.offset + prevEntry.compressedSize);
        }
      }
    });

    it('should apply gzip compression and reduce size vs uncompressed', async () => {
      // Create a larger index that will compress well
      const posIndex: POSIndex = {
        version: 'v1',
        entries: {},
      };
      for (let i = 0; i < 100; i++) {
        posIndex.entries[`predicate_${i}`] = [
          `https://example.com/entity/${i}`,
          `https://example.com/entity/${i + 1}`,
          `https://example.com/entity/${i + 2}`,
        ];
      }

      const indexes: CombinedIndexData = { pos: posIndex };

      const compressed = await encodeCombinedIndex('test', indexes, { compression: Compression.GZIP });
      const uncompressed = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });

      expect(compressed.length).toBeLessThan(uncompressed.length);
    });

    it('should encode multiple index types in correct order', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
        fts: createTestFTSIndex(),
        geo: createTestGeoIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });
      const headerInfo = decodeIndexHeader(encoded);

      // Verify all index types are present
      const types = headerInfo.header.directory.map(e => e.type);
      expect(types).toContain(IndexType.POS);
      expect(types).toContain(IndexType.OSP);
      expect(types).toContain(IndexType.FTS);
      expect(types).toContain(IndexType.GEO);
      expect(types.length).toBe(4);
    });

    it('should exclude vector index by default', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        vec: createTestVectorIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      const types = headerInfo.header.directory.map(e => e.type);
      expect(types).toContain(IndexType.POS);
      expect(types).not.toContain(IndexType.VEC);
    });

    it('should include vector index when explicitly requested', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        vec: createTestVectorIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes, { includeVectors: true });
      const headerInfo = decodeIndexHeader(encoded);

      const types = headerInfo.header.directory.map(e => e.type);
      expect(types).toContain(IndexType.POS);
      expect(types).toContain(IndexType.VEC);
    });

    it('should calculate valid CRC32 in footer', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const view = new DataView(encoded.buffer, encoded.byteOffset);

      // CRC32 is at footer offset + 8
      const footerOffset = encoded.length - FOOTER_SIZE;
      const crc32 = view.getUint32(footerOffset + 8, true);

      // CRC should be non-zero (very unlikely to be zero for real data)
      expect(crc32).not.toBe(0);
    });

    it('should handle empty namespace', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.namespace).toBe('');
    });

    it('should handle long namespace with padding to 8-byte boundary', async () => {
      const namespace = 'this-is-a-long-namespace-for-testing-padding';
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex(namespace, indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.namespace).toBe(namespace);

      // Header size should be aligned properly
      const namespaceLength = new TextEncoder().encode(namespace).length;
      const expectedPaddedNamespace = Math.ceil(namespaceLength / 8) * 8;
      const expectedDirectoryStart = HEADER_SIZE + expectedPaddedNamespace;
      // Directory starts after header + padded namespace
      expect(headerInfo.header.directory[0]!.offset).toBeGreaterThanOrEqual(expectedDirectoryStart);
    });
  });
});

// ============================================================================
// DECODING TESTS
// ============================================================================

describe('Combined Index Decoding', () => {
  describe('decodeIndexHeader', () => {
    it('should parse header magic correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.magic).toBe(GIDX_MAGIC);
    });

    it('should parse version correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.version).toBe(GIDX_VERSION);
    });

    it('should parse index count correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
        fts: createTestFTSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.indexCount).toBe(3);
    });

    it('should parse total size correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.totalSize).toBe(encoded.length);
    });

    it('should parse namespace correctly', async () => {
      const namespace = 'my-special-namespace';
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex(namespace, indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.namespace).toBe(namespace);
    });

    it('should parse directory entries correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.directory.length).toBe(2);
      expect(headerInfo.header.directory[0]!.type).toBe(IndexType.POS);
      expect(headerInfo.header.directory[1]!.type).toBe(IndexType.OSP);
    });

    it('should build ranges map correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        fts: createTestFTSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.ranges.has(IndexType.POS)).toBe(true);
      expect(headerInfo.ranges.has(IndexType.FTS)).toBe(true);
      expect(headerInfo.ranges.has(IndexType.GEO)).toBe(false);

      const posRange = headerInfo.ranges.get(IndexType.POS)!;
      expect(posRange.offset).toBeGreaterThan(0);
      expect(posRange.length).toBeGreaterThan(0);
    });

    it('should calculate headerSize correctly', async () => {
      const namespace = 'test-namespace';
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
      };

      const encoded = await encodeCombinedIndex(namespace, indexes);
      const headerInfo = decodeIndexHeader(encoded);

      // Header size should include: HEADER_SIZE + padded namespace + directory
      const namespaceLength = new TextEncoder().encode(namespace).length;
      const paddedNamespace = Math.ceil(namespaceLength / 8) * 8;
      const directorySize = 2 * DIRECTORY_ENTRY_SIZE;
      const expectedSize = HEADER_SIZE + paddedNamespace + directorySize;

      expect(headerInfo.headerSize).toBe(expectedSize);
    });

    it('should throw error for invalid magic', () => {
      const invalidData = new Uint8Array(100);
      // Set invalid magic
      new DataView(invalidData.buffer).setUint32(0, 0x12345678, true);

      expect(() => decodeIndexHeader(invalidData)).toThrow(/Invalid magic/);
    });

    it('should parse createdAt timestamp', async () => {
      const beforeEncode = Date.now();
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const afterEncode = Date.now();
      const headerInfo = decodeIndexHeader(encoded);

      expect(headerInfo.header.createdAt).toBeGreaterThanOrEqual(beforeEncode);
      expect(headerInfo.header.createdAt).toBeLessThanOrEqual(afterEncode);
    });
  });

  describe('decodeIndexSection', () => {
    it('should extract POS index correctly', async () => {
      const posIndex = createTestPOSIndex();
      const indexes: CombinedIndexData = { pos: posIndex };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });
      const headerInfo = decodeIndexHeader(encoded);

      const posEntry = headerInfo.header.directory.find(e => e.type === IndexType.POS)!;
      const posData = encoded.subarray(posEntry.offset, posEntry.offset + posEntry.compressedSize);

      const decoded = await decodeIndexSection<POSIndex>(posData, posEntry);

      expect(decoded.version).toBe(posIndex.version);
      expect(decoded.entries).toEqual(posIndex.entries);
    });

    it('should extract compressed FTS index correctly', async () => {
      const ftsIndex = createTestFTSIndex();
      const indexes: CombinedIndexData = { fts: ftsIndex };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.GZIP });
      const headerInfo = decodeIndexHeader(encoded);

      const ftsEntry = headerInfo.header.directory.find(e => e.type === IndexType.FTS)!;
      const ftsData = encoded.subarray(ftsEntry.offset, ftsEntry.offset + ftsEntry.compressedSize);

      const decoded = await decodeIndexSection<FTSIndex>(ftsData, ftsEntry);

      expect(decoded.version).toBe(ftsIndex.version);
      expect(decoded.documentCount).toBe(ftsIndex.documentCount);
      expect(decoded.terms).toEqual(ftsIndex.terms);
    });

    it('should extract GEO index correctly', async () => {
      const geoIndex = createTestGeoIndex();
      const indexes: CombinedIndexData = { geo: geoIndex };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });
      const headerInfo = decodeIndexHeader(encoded);

      const geoEntry = headerInfo.header.directory.find(e => e.type === IndexType.GEO)!;
      const geoData = encoded.subarray(geoEntry.offset, geoEntry.offset + geoEntry.compressedSize);

      const decoded = await decodeIndexSection<GeoIndex>(geoData, geoEntry);

      expect(decoded.precision).toBe(geoIndex.precision);
      expect(decoded.cells).toEqual(geoIndex.cells);
    });
  });

  describe('decodeCombinedIndex', () => {
    it('should round-trip POS index correctly', async () => {
      const posIndex = createTestPOSIndex();
      const indexes: CombinedIndexData = { pos: posIndex };

      const encoded = await encodeCombinedIndex('test-namespace', indexes);
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.pos).toBeDefined();
      expect(decoded.indexes.pos!.version).toBe(posIndex.version);
      expect(decoded.indexes.pos!.entries).toEqual(posIndex.entries);
    });

    it('should round-trip all index types correctly', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
        fts: createTestFTSIndex(),
        geo: createTestGeoIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.pos).toEqual(indexes.pos);
      expect(decoded.indexes.osp).toEqual(indexes.osp);
      expect(decoded.indexes.fts).toEqual(indexes.fts);
      expect(decoded.indexes.geo).toEqual(indexes.geo);
    });

    it('should round-trip with gzip compression', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        fts: createTestFTSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.GZIP });
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.pos).toEqual(indexes.pos);
      expect(decoded.indexes.fts).toEqual(indexes.fts);
    });

    it('should round-trip vector index when included', async () => {
      const vecIndex = createTestVectorIndex();
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        vec: vecIndex,
      };

      const encoded = await encodeCombinedIndex('test', indexes, {
        compression: Compression.NONE,
        includeVectors: true,
      });
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.vec).toBeDefined();
      expect(decoded.indexes.vec!.dimensions).toBe(vecIndex.dimensions);
      expect(decoded.indexes.vec!.m).toBe(vecIndex.m);
      expect(decoded.indexes.vec!.entries.length).toBe(vecIndex.entries.length);
    });

    it('should round-trip HNSW graph connections correctly', async () => {
      // Create a vector index with explicit HNSW connections
      const vecIndex: VectorIndex = {
        version: 'v1',
        dimensions: 4,
        m: 16,
        efConstruction: 200,
        entries: [
          {
            entityId: 'https://example.com/doc/1',
            predicate: 'embedding',
            vector: [0.1, 0.2, 0.3, 0.4],
            connections: [
              ['https://example.com/doc/2', 'https://example.com/doc/3'], // Layer 0
              ['https://example.com/doc/2'], // Layer 1
            ],
          },
          {
            entityId: 'https://example.com/doc/2',
            predicate: 'embedding',
            vector: [0.5, 0.6, 0.7, 0.8],
            connections: [
              ['https://example.com/doc/1', 'https://example.com/doc/3'], // Layer 0
            ],
          },
          {
            entityId: 'https://example.com/doc/3',
            predicate: 'embedding',
            vector: [0.9, 1.0, 1.1, 1.2],
            connections: [
              ['https://example.com/doc/1', 'https://example.com/doc/2'], // Layer 0
              ['https://example.com/doc/1'], // Layer 1
              [], // Layer 2 (entry point might have higher layers with no connections)
            ],
          },
        ],
      };

      const indexes: CombinedIndexData = { vec: vecIndex };

      const encoded = await encodeCombinedIndex('test', indexes, {
        compression: Compression.NONE,
        includeVectors: true,
      });
      const decoded = await decodeCombinedIndex(encoded);

      // Verify all entries preserved
      expect(decoded.indexes.vec).toBeDefined();
      expect(decoded.indexes.vec!.entries.length).toBe(3);

      // Verify connections for each entry
      const entry1 = decoded.indexes.vec!.entries[0]!;
      expect(entry1.entityId).toBe('https://example.com/doc/1');
      expect(entry1.connections.length).toBe(2);
      expect(entry1.connections[0]).toEqual(['https://example.com/doc/2', 'https://example.com/doc/3']);
      expect(entry1.connections[1]).toEqual(['https://example.com/doc/2']);

      const entry2 = decoded.indexes.vec!.entries[1]!;
      expect(entry2.entityId).toBe('https://example.com/doc/2');
      expect(entry2.connections.length).toBe(1);
      expect(entry2.connections[0]).toEqual(['https://example.com/doc/1', 'https://example.com/doc/3']);

      const entry3 = decoded.indexes.vec!.entries[2]!;
      expect(entry3.entityId).toBe('https://example.com/doc/3');
      expect(entry3.connections.length).toBe(3);
      expect(entry3.connections[0]).toEqual(['https://example.com/doc/1', 'https://example.com/doc/2']);
      expect(entry3.connections[1]).toEqual(['https://example.com/doc/1']);
      expect(entry3.connections[2]).toEqual([]);
    });

    it('should preserve HNSW entry point (node with highest layer)', async () => {
      // In HNSW, the entry point is the node with the maximum layer
      // After round-trip, we should be able to identify it
      const vecIndex: VectorIndex = {
        version: 'v1',
        dimensions: 2,
        m: 16,
        efConstruction: 200,
        entries: [
          {
            entityId: 'https://example.com/node/a',
            predicate: 'embedding',
            vector: [0.1, 0.2],
            connections: [['https://example.com/node/b']], // Layer 0 only
          },
          {
            entityId: 'https://example.com/node/b',
            predicate: 'embedding',
            vector: [0.3, 0.4],
            connections: [
              ['https://example.com/node/a', 'https://example.com/node/c'], // Layer 0
              ['https://example.com/node/c'], // Layer 1
              ['https://example.com/node/c'], // Layer 2 - this is highest, likely entry point
            ],
          },
          {
            entityId: 'https://example.com/node/c',
            predicate: 'embedding',
            vector: [0.5, 0.6],
            connections: [
              ['https://example.com/node/a', 'https://example.com/node/b'], // Layer 0
              ['https://example.com/node/b'], // Layer 1
            ],
          },
        ],
      };

      const indexes: CombinedIndexData = { vec: vecIndex };

      const encoded = await encodeCombinedIndex('test', indexes, {
        compression: Compression.NONE,
        includeVectors: true,
      });
      const decoded = await decodeCombinedIndex(encoded);

      // Find the entry point (node with maximum layer count)
      const entryPoint = decoded.indexes.vec!.entries.reduce((max, entry) =>
        entry.connections.length > max.connections.length ? entry : max
      );

      expect(entryPoint.entityId).toBe('https://example.com/node/b');
      expect(entryPoint.connections.length).toBe(3); // 3 layers (0, 1, 2)
    });

    it('should round-trip vector index with gzip compression', async () => {
      const vecIndex: VectorIndex = {
        version: 'v1',
        dimensions: 8,
        m: 16,
        efConstruction: 200,
        entries: [
          {
            entityId: 'https://example.com/doc/1',
            predicate: 'embedding',
            vector: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
            connections: [
              ['https://example.com/doc/2', 'https://example.com/doc/3'],
            ],
          },
          {
            entityId: 'https://example.com/doc/2',
            predicate: 'embedding',
            vector: [0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6],
            connections: [
              ['https://example.com/doc/1'],
            ],
          },
          {
            entityId: 'https://example.com/doc/3',
            predicate: 'embedding',
            vector: [1.7, 1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4],
            connections: [
              ['https://example.com/doc/1'],
            ],
          },
        ],
      };

      const indexes: CombinedIndexData = { vec: vecIndex };

      const encoded = await encodeCombinedIndex('test', indexes, {
        compression: Compression.GZIP,
        includeVectors: true,
      });
      const decoded = await decodeCombinedIndex(encoded);

      // Verify complete round-trip with compression
      expect(decoded.indexes.vec!.entries.length).toBe(3);
      expect(decoded.indexes.vec!.entries[0]!.connections[0]).toEqual([
        'https://example.com/doc/2',
        'https://example.com/doc/3',
      ]);
    });

    it('should handle empty connections arrays', async () => {
      const vecIndex: VectorIndex = {
        version: 'v1',
        dimensions: 2,
        m: 16,
        efConstruction: 200,
        entries: [
          {
            entityId: 'https://example.com/isolated',
            predicate: 'embedding',
            vector: [0.1, 0.2],
            connections: [], // No connections at all
          },
        ],
      };

      const indexes: CombinedIndexData = { vec: vecIndex };

      const encoded = await encodeCombinedIndex('test', indexes, {
        compression: Compression.NONE,
        includeVectors: true,
      });
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.vec!.entries[0]!.connections).toEqual([]);
    });

    it('should handle layers with empty connection lists', async () => {
      const vecIndex: VectorIndex = {
        version: 'v1',
        dimensions: 2,
        m: 16,
        efConstruction: 200,
        entries: [
          {
            entityId: 'https://example.com/sparse',
            predicate: 'embedding',
            vector: [0.1, 0.2],
            connections: [
              ['https://example.com/other'], // Layer 0 has one connection
              [], // Layer 1 is empty
              [], // Layer 2 is empty
            ],
          },
        ],
      };

      const indexes: CombinedIndexData = { vec: vecIndex };

      const encoded = await encodeCombinedIndex('test', indexes, {
        compression: Compression.NONE,
        includeVectors: true,
      });
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.vec!.entries[0]!.connections.length).toBe(3);
      expect(decoded.indexes.vec!.entries[0]!.connections[0]).toEqual(['https://example.com/other']);
      expect(decoded.indexes.vec!.entries[0]!.connections[1]).toEqual([]);
      expect(decoded.indexes.vec!.entries[0]!.connections[2]).toEqual([]);
    });

    it('should preserve header information on round-trip', async () => {
      const namespace = 'my-namespace';
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const beforeEncode = Date.now();
      const encoded = await encodeCombinedIndex(namespace, indexes);
      const afterEncode = Date.now();
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.header.namespace).toBe(namespace);
      expect(decoded.header.version).toBe(GIDX_VERSION);
      expect(decoded.header.createdAt).toBeGreaterThanOrEqual(beforeEncode);
      expect(decoded.header.createdAt).toBeLessThanOrEqual(afterEncode);
    });

    it('should handle large indexes', async () => {
      // Create a larger index
      const posIndex: POSIndex = {
        version: 'v1',
        entries: {},
      };
      for (let i = 0; i < 1000; i++) {
        posIndex.entries[`predicate_${i}`] = Array.from(
          { length: 10 },
          (_, j) => `https://example.com/entity/${i * 10 + j}`
        );
      }

      const indexes: CombinedIndexData = { pos: posIndex };

      const encoded = await encodeCombinedIndex('test', indexes);
      const decoded = await decodeCombinedIndex(encoded);

      expect(decoded.indexes.pos!.entries).toEqual(posIndex.entries);
      expect(Object.keys(decoded.indexes.pos!.entries).length).toBe(1000);
    });
  });
});

// ============================================================================
// RANGE REQUEST TESTS
// ============================================================================

describe('Range Request Helpers', () => {
  describe('getHeaderRange', () => {
    it('should return appropriate size estimate with defaults', () => {
      const range = getHeaderRange();

      expect(range.offset).toBe(0);
      // Default: 256 byte namespace padded + 10 indexes
      const expectedNamespacePadded = Math.ceil(256 / 8) * 8;
      const expectedDirectorySize = 10 * DIRECTORY_ENTRY_SIZE;
      const expectedLength = HEADER_SIZE + expectedNamespacePadded + expectedDirectorySize;
      expect(range.length).toBe(expectedLength);
    });

    it('should respect custom maxNamespaceLength', () => {
      const range = getHeaderRange(64);

      expect(range.offset).toBe(0);
      const expectedNamespacePadded = Math.ceil(64 / 8) * 8;
      const expectedDirectorySize = 10 * DIRECTORY_ENTRY_SIZE;
      const expectedLength = HEADER_SIZE + expectedNamespacePadded + expectedDirectorySize;
      expect(range.length).toBe(expectedLength);
    });

    it('should respect custom maxIndexes', () => {
      const range = getHeaderRange(256, 5);

      expect(range.offset).toBe(0);
      const expectedNamespacePadded = Math.ceil(256 / 8) * 8;
      const expectedDirectorySize = 5 * DIRECTORY_ENTRY_SIZE;
      const expectedLength = HEADER_SIZE + expectedNamespacePadded + expectedDirectorySize;
      expect(range.length).toBe(expectedLength);
    });

    it('should handle edge case of 0 namespace length', () => {
      const range = getHeaderRange(0, 5);

      expect(range.offset).toBe(0);
      // 0 bytes namespace still pads to 0 (ceil(0/8)*8 = 0)
      const expectedDirectorySize = 5 * DIRECTORY_ENTRY_SIZE;
      const expectedLength = HEADER_SIZE + 0 + expectedDirectorySize;
      expect(range.length).toBe(expectedLength);
    });
  });

  describe('planRangeRequests', () => {
    it('should identify correct byte ranges for each index type', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
        fts: createTestFTSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });
      const headerInfo = decodeIndexHeader(encoded);

      const requests = planRangeRequests(headerInfo, [IndexType.POS, IndexType.FTS]);

      expect(requests.length).toBe(2);

      // Verify POS range
      const posRequest = requests.find(r => r.type === IndexType.POS);
      expect(posRequest).toBeDefined();
      expect(posRequest!.offset).toBe(headerInfo.ranges.get(IndexType.POS)!.offset);
      expect(posRequest!.length).toBe(headerInfo.ranges.get(IndexType.POS)!.length);

      // Verify FTS range
      const ftsRequest = requests.find(r => r.type === IndexType.FTS);
      expect(ftsRequest).toBeDefined();
      expect(ftsRequest!.offset).toBe(headerInfo.ranges.get(IndexType.FTS)!.offset);
      expect(ftsRequest!.length).toBe(headerInfo.ranges.get(IndexType.FTS)!.length);
    });

    it('should return empty array for non-existent index types', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      const requests = planRangeRequests(headerInfo, [IndexType.GEO, IndexType.VEC]);

      expect(requests.length).toBe(0);
    });

    it('should return results sorted by offset', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        osp: createTestOSPIndex(),
        fts: createTestFTSIndex(),
        geo: createTestGeoIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes, { compression: Compression.NONE });
      const headerInfo = decodeIndexHeader(encoded);

      // Request in reverse order
      const requests = planRangeRequests(headerInfo, [IndexType.GEO, IndexType.FTS, IndexType.OSP, IndexType.POS]);

      expect(requests.length).toBe(4);
      for (let i = 1; i < requests.length; i++) {
        expect(requests[i]!.offset).toBeGreaterThan(requests[i - 1]!.offset);
      }
    });

    it('should handle single index type request', async () => {
      const indexes: CombinedIndexData = {
        pos: createTestPOSIndex(),
        fts: createTestFTSIndex(),
      };

      const encoded = await encodeCombinedIndex('test', indexes);
      const headerInfo = decodeIndexHeader(encoded);

      const requests = planRangeRequests(headerInfo, [IndexType.FTS]);

      expect(requests.length).toBe(1);
      expect(requests[0]!.type).toBe(IndexType.FTS);
    });
  });

  describe('coalesceRanges', () => {
    it('should merge adjacent ranges within gap threshold', () => {
      const ranges = [
        { type: IndexType.POS, offset: 100, length: 50 },
        { type: IndexType.OSP, offset: 155, length: 50 }, // 5 byte gap
      ];

      const coalesced = coalesceRanges(ranges, 10);

      expect(coalesced.length).toBe(1);
      expect(coalesced[0]!.types).toContain(IndexType.POS);
      expect(coalesced[0]!.types).toContain(IndexType.OSP);
      expect(coalesced[0]!.offset).toBe(100);
      expect(coalesced[0]!.length).toBe(105); // 50 + 5 + 50
    });

    it('should keep ranges separate when gap exceeds threshold', () => {
      const ranges = [
        { type: IndexType.POS, offset: 100, length: 50 },
        { type: IndexType.FTS, offset: 200, length: 50 }, // 50 byte gap
      ];

      const coalesced = coalesceRanges(ranges, 10);

      expect(coalesced.length).toBe(2);
      expect(coalesced[0]!.types).toEqual([IndexType.POS]);
      expect(coalesced[1]!.types).toEqual([IndexType.FTS]);
    });

    it('should handle empty ranges array', () => {
      const coalesced = coalesceRanges([]);

      expect(coalesced.length).toBe(0);
    });

    it('should handle single range', () => {
      const ranges = [{ type: IndexType.POS, offset: 100, length: 50 }];

      const coalesced = coalesceRanges(ranges);

      expect(coalesced.length).toBe(1);
      expect(coalesced[0]!.types).toEqual([IndexType.POS]);
      expect(coalesced[0]!.offset).toBe(100);
      expect(coalesced[0]!.length).toBe(50);
    });

    it('should coalesce multiple adjacent ranges into one', () => {
      const ranges = [
        { type: IndexType.POS, offset: 100, length: 50 },
        { type: IndexType.OSP, offset: 150, length: 50 },   // 0 byte gap
        { type: IndexType.FTS, offset: 200, length: 50 },   // 0 byte gap
        { type: IndexType.GEO, offset: 250, length: 50 },   // 0 byte gap
      ];

      const coalesced = coalesceRanges(ranges, 0);

      expect(coalesced.length).toBe(1);
      expect(coalesced[0]!.types.length).toBe(4);
      expect(coalesced[0]!.offset).toBe(100);
      expect(coalesced[0]!.length).toBe(200);
    });

    it('should use default gap threshold of 4096 bytes', () => {
      const ranges = [
        { type: IndexType.POS, offset: 100, length: 50 },
        { type: IndexType.OSP, offset: 4100, length: 50 }, // 3950 byte gap (< 4096)
      ];

      const coalesced = coalesceRanges(ranges);

      expect(coalesced.length).toBe(1);
    });

    it('should sort unsorted input before coalescing', () => {
      const ranges = [
        { type: IndexType.FTS, offset: 200, length: 50 },
        { type: IndexType.POS, offset: 100, length: 50 },
      ];

      const coalesced = coalesceRanges(ranges, 100);

      expect(coalesced.length).toBe(1);
      expect(coalesced[0]!.types[0]).toBe(IndexType.POS); // POS comes first (lower offset)
      expect(coalesced[0]!.offset).toBe(100);
    });

    it('should handle overlapping ranges correctly', () => {
      const ranges = [
        { type: IndexType.POS, offset: 100, length: 100 },
        { type: IndexType.OSP, offset: 150, length: 100 }, // Overlaps with POS
      ];

      const coalesced = coalesceRanges(ranges, 0);

      expect(coalesced.length).toBe(1);
      expect(coalesced[0]!.types.length).toBe(2);
      expect(coalesced[0]!.offset).toBe(100);
      expect(coalesced[0]!.length).toBe(150); // Covers 100-250
    });
  });
});

// ============================================================================
// QUANTIZED VECTOR TESTS
// ============================================================================

describe('Quantized Vector Format', () => {
  describe('encodeQuantizedVectors', () => {
    it('should encode with Float32 full precision', () => {
      const vectors = createTestVectors(10, 8);

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);

      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(QVEC_HEADER_SIZE);

      // Check magic
      const view = new DataView(encoded.buffer, encoded.byteOffset);
      expect(view.getUint32(0, true)).toBe(QVEC_MAGIC);

      // Check quantization type
      expect(view.getUint8(6)).toBe(VectorQuantization.FLOAT32);
    });

    it('should encode with Int8 achieving 4x compression vs Float32', () => {
      const vectors = createTestVectors(100, 384);

      const float32Encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);
      const int8Encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);

      // Int8 should be roughly 4x smaller (1 byte vs 4 bytes per dimension)
      // Account for header and ID table overhead
      const vectorDataFloat32 = 100 * 384 * 4;
      const vectorDataInt8 = 100 * 384 * 1;

      expect(int8Encoded.length).toBeLessThan(float32Encoded.length);
      // The ratio should be closer to 4x as vector data dominates
      const compressionRatio = float32Encoded.length / int8Encoded.length;
      expect(compressionRatio).toBeGreaterThan(2.5); // At least 2.5x compression
    });

    it('should encode with Binary achieving 32x compression vs Float32', () => {
      const vectors = createTestVectors(100, 384);

      const float32Encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);
      const binaryEncoded = encodeQuantizedVectors(vectors, VectorQuantization.BINARY);

      // Binary should be roughly 32x smaller (1 bit vs 32 bits per dimension)
      expect(binaryEncoded.length).toBeLessThan(float32Encoded.length);

      const compressionRatio = float32Encoded.length / binaryEncoded.length;
      expect(compressionRatio).toBeGreaterThan(15); // At least 15x compression
    });

    it('should store correct dimensions in header', () => {
      const dimensions = 128;
      const vectors = createTestVectors(5, dimensions);

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);
      const header = decodeQuantizedVectorHeader(encoded);

      expect(header.dimensions).toBe(dimensions);
    });

    it('should store correct vector count in header', () => {
      const count = 25;
      const vectors = createTestVectors(count, 16);

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);
      const header = decodeQuantizedVectorHeader(encoded);

      expect(header.vectorCount).toBe(count);
    });

    it('should calculate scale and offset for Int8 quantization', () => {
      // Create vectors with known range
      const vectors = [
        { entityId: 'entity1', vector: [-1.0, 0.0, 1.0, 0.5] },
        { entityId: 'entity2', vector: [-0.5, 0.25, 0.75, -0.25] },
      ];

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);
      const header = decodeQuantizedVectorHeader(encoded);

      // Scale and offset should be set for the range [-1, 1]
      expect(header.scale).toBeGreaterThan(0);
      expect(typeof header.offset).toBe('number');
    });

    it('should throw error for empty vector array', () => {
      expect(() => encodeQuantizedVectors([], VectorQuantization.FLOAT32)).toThrow(/empty/i);
    });

    it('should preserve entity IDs correctly', () => {
      const vectors = [
        { entityId: 'https://example.com/entity/special-id-1', vector: [0.1, 0.2] },
        { entityId: 'https://example.com/entity/special-id-2', vector: [0.3, 0.4] },
      ];

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);
      const decoded = decodeQuantizedVectors(encoded);

      expect(decoded.ids).toEqual(vectors.map(v => v.entityId));
    });
  });

  describe('decodeQuantizedVectorHeader', () => {
    it('should parse magic correctly', () => {
      const vectors = createTestVectors(5, 8);
      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);

      const header = decodeQuantizedVectorHeader(encoded);

      expect(header.magic).toBe(QVEC_MAGIC);
    });

    it('should parse version correctly', () => {
      const vectors = createTestVectors(5, 8);
      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);

      const header = decodeQuantizedVectorHeader(encoded);

      expect(header.version).toBe(1);
    });

    it('should parse quantization type correctly', () => {
      const vectors = createTestVectors(5, 8);

      const float32Header = decodeQuantizedVectorHeader(
        encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32)
      );
      expect(float32Header.quantization).toBe(VectorQuantization.FLOAT32);

      const int8Header = decodeQuantizedVectorHeader(
        encodeQuantizedVectors(vectors, VectorQuantization.INT8)
      );
      expect(int8Header.quantization).toBe(VectorQuantization.INT8);

      const binaryHeader = decodeQuantizedVectorHeader(
        encodeQuantizedVectors(vectors, VectorQuantization.BINARY)
      );
      expect(binaryHeader.quantization).toBe(VectorQuantization.BINARY);
    });

    it('should throw error for invalid magic', () => {
      const invalidData = new Uint8Array(100);
      new DataView(invalidData.buffer).setUint32(0, 0x12345678, true);

      expect(() => decodeQuantizedVectorHeader(invalidData)).toThrow(/Invalid QVEC magic/);
    });
  });

  describe('decodeQuantizedVectors', () => {
    it('should round-trip Float32 vectors correctly', () => {
      const vectors = [
        { entityId: 'entity1', vector: [0.1, 0.2, 0.3, 0.4] },
        { entityId: 'entity2', vector: [0.5, 0.6, 0.7, 0.8] },
      ];

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);
      const decoded = decodeQuantizedVectors(encoded);

      expect(decoded.ids).toEqual(['entity1', 'entity2']);
      expect(decoded.header.quantization).toBe(VectorQuantization.FLOAT32);
      expect(decoded.vectors).toBeInstanceOf(Float32Array);
      expect(decoded.vectors.length).toBe(8); // 2 vectors * 4 dimensions
    });

    it('should round-trip Int8 vectors correctly', () => {
      const vectors = createTestVectors(10, 16);

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);
      const decoded = decodeQuantizedVectors(encoded);

      expect(decoded.ids.length).toBe(10);
      expect(decoded.header.quantization).toBe(VectorQuantization.INT8);
      expect(decoded.vectors).toBeInstanceOf(Int8Array);
      expect(decoded.vectors.length).toBe(160); // 10 vectors * 16 dimensions
    });

    it('should round-trip Binary vectors correctly', () => {
      const vectors = createTestVectors(10, 32);

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.BINARY);
      const decoded = decodeQuantizedVectors(encoded);

      expect(decoded.ids.length).toBe(10);
      expect(decoded.header.quantization).toBe(VectorQuantization.BINARY);
      expect(decoded.vectors).toBeInstanceOf(Uint8Array);
      expect(decoded.vectors.length).toBe(40); // 10 vectors * (32/8) bytes
    });

    it('should preserve all entity IDs', () => {
      const vectors = createTestVectors(100, 8);

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);
      const decoded = decodeQuantizedVectors(encoded);

      expect(decoded.ids).toEqual(vectors.map(v => v.entityId));
    });
  });

  describe('getVectorFloat32', () => {
    it('should return exact values for Float32 quantization', () => {
      const vectors = [
        { entityId: 'entity1', vector: [0.1, 0.2, 0.3, 0.4] },
        { entityId: 'entity2', vector: [0.5, 0.6, 0.7, 0.8] },
      ];

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.FLOAT32);
      const file = decodeQuantizedVectors(encoded);

      const vec0 = getVectorFloat32(file, 0);
      expect(vec0.length).toBe(4);
      expect(vec0[0]).toBeCloseTo(0.1, 5);
      expect(vec0[1]).toBeCloseTo(0.2, 5);
      expect(vec0[2]).toBeCloseTo(0.3, 5);
      expect(vec0[3]).toBeCloseTo(0.4, 5);

      const vec1 = getVectorFloat32(file, 1);
      expect(vec1[0]).toBeCloseTo(0.5, 5);
      expect(vec1[3]).toBeCloseTo(0.8, 5);
    });

    it('should dequantize Int8 correctly with reasonable precision', () => {
      const originalVectors = [
        { entityId: 'entity1', vector: [-0.9, -0.5, 0.0, 0.5, 0.9] },
      ];

      const encoded = encodeQuantizedVectors(originalVectors, VectorQuantization.INT8);
      const file = decodeQuantizedVectors(encoded);

      const dequantized = getVectorFloat32(file, 0);

      // Int8 quantization should preserve values within ~1% error
      expect(dequantized[0]).toBeCloseTo(-0.9, 1);
      expect(dequantized[1]).toBeCloseTo(-0.5, 1);
      expect(dequantized[2]).toBeCloseTo(0.0, 1);
      expect(dequantized[3]).toBeCloseTo(0.5, 1);
      expect(dequantized[4]).toBeCloseTo(0.9, 1);
    });

    it('should dequantize Binary vectors to -1/+1 values', () => {
      const vectors = [
        { entityId: 'entity1', vector: [-0.5, 0.5, -0.1, 0.1, -0.9, 0.9, -0.01, 0.01] },
      ];

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.BINARY);
      const file = decodeQuantizedVectors(encoded);

      const dequantized = getVectorFloat32(file, 0);

      // Binary quantization maps to -1 (negative) or +1 (positive/zero)
      expect(dequantized[0]).toBe(-1.0); // -0.5 -> -1
      expect(dequantized[1]).toBe(1.0);  // 0.5 -> +1
      expect(dequantized[2]).toBe(-1.0); // -0.1 -> -1
      expect(dequantized[3]).toBe(1.0);  // 0.1 -> +1
    });

    it('should return correct dimensions for all quantization types', () => {
      const dimensions = 64;
      const vectors = createTestVectors(3, dimensions);

      for (const quantization of [VectorQuantization.FLOAT32, VectorQuantization.INT8, VectorQuantization.BINARY]) {
        const encoded = encodeQuantizedVectors(vectors, quantization);
        const file = decodeQuantizedVectors(encoded);

        const vec = getVectorFloat32(file, 1);
        expect(vec.length).toBe(dimensions);
      }
    });
  });

  describe('cosineSimilarity', () => {
    it('should return 1.0 for identical vectors', () => {
      const a = [0.1, 0.2, 0.3, 0.4];
      const b = [0.1, 0.2, 0.3, 0.4];

      const similarity = cosineSimilarity(a, b);

      expect(similarity).toBeCloseTo(1.0, 5);
    });

    it('should return -1.0 for opposite vectors', () => {
      const a = [1.0, 0.0, 0.0];
      const b = [-1.0, 0.0, 0.0];

      const similarity = cosineSimilarity(a, b);

      expect(similarity).toBeCloseTo(-1.0, 5);
    });

    it('should return 0.0 for orthogonal vectors', () => {
      const a = [1.0, 0.0, 0.0];
      const b = [0.0, 1.0, 0.0];

      const similarity = cosineSimilarity(a, b);

      expect(similarity).toBeCloseTo(0.0, 5);
    });

    it('should compute correct values for arbitrary vectors', () => {
      // Known example: cos(theta) between [1,2,3] and [4,5,6]
      // dot product = 1*4 + 2*5 + 3*6 = 32
      // |a| = sqrt(1+4+9) = sqrt(14)
      // |b| = sqrt(16+25+36) = sqrt(77)
      // cosine = 32 / (sqrt(14) * sqrt(77)) = 32 / sqrt(1078) = ~0.9746
      const a = [1, 2, 3];
      const b = [4, 5, 6];

      const similarity = cosineSimilarity(a, b);

      expect(similarity).toBeCloseTo(0.9746, 3);
    });

    it('should work with Float32Array input', () => {
      const a = new Float32Array([0.5, 0.5, 0.5, 0.5]);
      const b = new Float32Array([0.5, 0.5, 0.5, 0.5]);

      const similarity = cosineSimilarity(a, b);

      expect(similarity).toBeCloseTo(1.0, 5);
    });

    it('should return 0 for zero vectors', () => {
      const a = [0, 0, 0];
      const b = [1, 2, 3];

      const similarity = cosineSimilarity(a, b);

      expect(similarity).toBe(0);
    });

    it('should handle normalized vectors correctly', () => {
      // Normalized vectors (unit length)
      const a = [1 / Math.sqrt(2), 1 / Math.sqrt(2), 0];
      const b = [1 / Math.sqrt(2), 0, 1 / Math.sqrt(2)];

      const similarity = cosineSimilarity(a, b);

      // dot product = 0.5 + 0 + 0 = 0.5
      // both have norm 1
      expect(similarity).toBeCloseTo(0.5, 5);
    });
  });

  describe('hammingDistance', () => {
    it('should return 0 for identical binary vectors', () => {
      const a = new Uint8Array([0b11110000, 0b10101010]);
      const b = new Uint8Array([0b11110000, 0b10101010]);

      const distance = hammingDistance(a, b);

      expect(distance).toBe(0);
    });

    it('should count all differing bits', () => {
      const a = new Uint8Array([0b11111111]);
      const b = new Uint8Array([0b00000000]);

      const distance = hammingDistance(a, b);

      expect(distance).toBe(8);
    });

    it('should compute correct distance for arbitrary vectors', () => {
      // 0b11110000 vs 0b11001100 -> 4 bits differ (positions 2,3,4,5)
      const a = new Uint8Array([0b11110000]);
      const b = new Uint8Array([0b11001100]);

      const distance = hammingDistance(a, b);

      expect(distance).toBe(4);
    });

    it('should handle multi-byte vectors', () => {
      const a = new Uint8Array([0b11111111, 0b00000000, 0b11111111]);
      const b = new Uint8Array([0b00000000, 0b00000000, 0b00000000]);

      const distance = hammingDistance(a, b);

      expect(distance).toBe(16); // 8 + 0 + 8 bits differ
    });

    it('should be symmetric', () => {
      const a = new Uint8Array([0b10101010, 0b01010101]);
      const b = new Uint8Array([0b11001100, 0b00110011]);

      expect(hammingDistance(a, b)).toBe(hammingDistance(b, a));
    });

    it('should compute correct distance for binary quantized vectors', () => {
      // Create vectors with known binary representation
      const vectors = [
        { entityId: 'entity1', vector: Array(8).fill(1) },   // All positive -> 0b11111111
        { entityId: 'entity2', vector: Array(8).fill(-1) },  // All negative -> 0b00000000
      ];

      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.BINARY);
      const file = decodeQuantizedVectors(encoded);

      const binaryVectors = file.vectors as Uint8Array;
      // Extract individual vectors (1 byte each for 8 dimensions)
      const vec1 = binaryVectors.subarray(0, 1);
      const vec2 = binaryVectors.subarray(1, 2);

      const distance = hammingDistance(vec1, vec2);

      expect(distance).toBe(8);
    });

    it('should handle empty vectors', () => {
      const a = new Uint8Array([]);
      const b = new Uint8Array([]);

      const distance = hammingDistance(a, b);

      expect(distance).toBe(0);
    });
  });

  describe('Integration: Vector Search Workflow', () => {
    it('should support typical vector search workflow', () => {
      // Create a dataset of vectors
      const vectors = createTestVectors(100, 128);

      // Encode with Int8 quantization for storage efficiency
      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);

      // Decode for searching
      const file = decodeQuantizedVectors(encoded);

      // Query vector (from user input)
      const queryVector = new Float32Array(128);
      for (let i = 0; i < 128; i++) {
        queryVector[i] = Math.sin(i * 0.1);
      }

      // Find k nearest neighbors
      const k = 5;
      const similarities: Array<{ index: number; similarity: number }> = [];

      for (let i = 0; i < file.header.vectorCount; i++) {
        const vec = getVectorFloat32(file, i);
        const sim = cosineSimilarity(queryVector, vec);
        similarities.push({ index: i, similarity: sim });
      }

      // Sort by similarity (descending)
      similarities.sort((a, b) => b.similarity - a.similarity);

      // Get top k results
      const topK = similarities.slice(0, k);

      expect(topK.length).toBe(k);
      for (const result of topK) {
        expect(result.similarity).toBeLessThanOrEqual(1.0);
        expect(result.similarity).toBeGreaterThanOrEqual(-1.0);
        expect(file.ids[result.index]).toBeDefined();
      }
    });

    it('should preserve relative ordering after Int8 quantization', () => {
      // Create vectors where we know the relative ordering
      const target = Array(32).fill(0).map((_, i) => Math.sin(i));
      const similar = Array(32).fill(0).map((_, i) => Math.sin(i) + 0.01);
      const different = Array(32).fill(0).map((_, i) => Math.cos(i));

      const vectors = [
        { entityId: 'similar', vector: similar },
        { entityId: 'different', vector: different },
      ];

      // Calculate similarities with Float32 (ground truth)
      const simSimilarFloat32 = cosineSimilarity(target, similar);
      const simDifferentFloat32 = cosineSimilarity(target, different);

      // Encode and decode with Int8
      const encoded = encodeQuantizedVectors(vectors, VectorQuantization.INT8);
      const file = decodeQuantizedVectors(encoded);

      // Calculate similarities with dequantized vectors
      const similarVec = getVectorFloat32(file, 0);
      const differentVec = getVectorFloat32(file, 1);

      const simSimilarInt8 = cosineSimilarity(target, similarVec);
      const simDifferentInt8 = cosineSimilarity(target, differentVec);

      // Relative ordering should be preserved
      expect(simSimilarFloat32 > simDifferentFloat32).toBe(simSimilarInt8 > simDifferentInt8);
    });
  });
});
