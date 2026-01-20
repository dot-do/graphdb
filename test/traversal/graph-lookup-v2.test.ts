/**
 * GraphLookup V2 Range Request Tests
 *
 * Tests for R2 Range request support for partial chunk reads:
 * - fetchFooter uses suffix range and parses correctly
 * - fetchEntityByRange fetches correct bytes
 * - lookupV2 uses cached footer on second call
 * - lookupV2 returns entity with correct data
 * - lookupV2 returns null for missing entity
 * - lookup auto-detects V2 and uses Range requests (integrated test)
 * - Stats track range vs full fetches
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  GraphLookup,
  fetchFooter,
  fetchEntityByRange,
  type CachedFooterInfo,
  type GraphLookupConfig,
} from '../../src/traversal/graph-lookup.js';
import {
  encodeGraphColV2,
  GCOL_FOOTER_SIZE,
  type GraphColFooter,
} from '../../src/storage/graphcol.js';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type TransactionId,
} from '../../src/core/types.js';
import { type Triple, type TypedObject } from '../../src/core/triple.js';

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
  subjectId: string,
  predicateName: string,
  objType: ObjectType,
  objValue: unknown,
  timestamp: bigint,
  txId: TransactionId
): Triple {
  const subject = createEntityId(subjectId);
  const predicate = createPredicate(predicateName);

  let object: TypedObject;
  switch (objType) {
    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: objValue as string };
      break;
    case ObjectType.INT64:
      object = { type: ObjectType.INT64, value: objValue as bigint };
      break;
    case ObjectType.FLOAT64:
      object = { type: ObjectType.FLOAT64, value: objValue as number };
      break;
    case ObjectType.BOOL:
      object = { type: ObjectType.BOOL, value: objValue as boolean };
      break;
    case ObjectType.REF:
      object = { type: ObjectType.REF, value: objValue as string };
      break;
    default:
      object = { type: ObjectType.NULL };
  }

  return {
    subject,
    predicate,
    object,
    timestamp,
    txId,
  };
}

/**
 * Generate test triples for multiple entities
 */
function generateMultiEntityTriples(entityCount: number, triplesPerEntity: number): Triple[] {
  const triples: Triple[] = [];
  const baseTime = BigInt(Date.now());

  for (let e = 0; e < entityCount; e++) {
    const entityId = `https://example.com/entity/${e.toString().padStart(4, '0')}`;
    for (let t = 0; t < triplesPerEntity; t++) {
      const txId = generateTestTxId((e * triplesPerEntity + t) % 22);
      const timestamp = baseTime + BigInt(e * 1000 + t);

      let triple: Triple;
      switch (t % 4) {
        case 0:
          triple = createTestTriple(entityId, 'name', ObjectType.STRING, `Entity ${e}`, timestamp, txId);
          break;
        case 1:
          triple = createTestTriple(entityId, 'age', ObjectType.INT64, BigInt(20 + e), timestamp, txId);
          break;
        case 2:
          triple = createTestTriple(entityId, 'score', ObjectType.FLOAT64, 75.5 + e, timestamp, txId);
          break;
        case 3:
          triple = createTestTriple(entityId, 'active', ObjectType.BOOL, e % 2 === 0, timestamp, txId);
          break;
        default:
          triple = createTestTriple(entityId, 'name', ObjectType.STRING, `Entity ${e}`, timestamp, txId);
      }
      triples.push(triple);
    }
  }

  return triples;
}

const testNamespace = createNamespace('https://example.com/');

// ============================================================================
// Mock R2 Bucket with Range Request Support
// ============================================================================

interface MockR2Store {
  [key: string]: Uint8Array;
}

interface R2RangeOption {
  offset?: number;
  length?: number;
  suffix?: number;
}

function createMockR2Bucket(store: MockR2Store = {}): R2Bucket & { _store: MockR2Store; _rangeRequests: { key: string; range?: R2RangeOption }[] } {
  const rangeRequests: { key: string; range?: R2RangeOption }[] = [];

  return {
    _store: store,
    _rangeRequests: rangeRequests,

    get: vi.fn(async (key: string, options?: { range?: R2RangeOption }): Promise<R2ObjectBody | null> => {
      const data = store[key];
      if (!data) return null;

      rangeRequests.push({ key, range: options?.range });

      let content: Uint8Array;
      if (options?.range) {
        if (options.range.suffix !== undefined) {
          // Suffix range: last N bytes
          const suffix = options.range.suffix;
          const start = Math.max(0, data.length - suffix);
          content = data.subarray(start);
        } else if (options.range.offset !== undefined && options.range.length !== undefined) {
          // Offset+length range
          const offset = options.range.offset;
          const length = options.range.length;
          content = data.subarray(offset, offset + length);
        } else {
          content = data;
        }
      } else {
        content = data;
      }

      return {
        key,
        version: '1',
        size: data.length, // Total file size, not range size
        etag: 'mock-etag',
        httpEtag: '"mock-etag"',
        checksums: {},
        uploaded: new Date(),
        httpMetadata: {},
        customMetadata: {},
        storageClass: 'Standard',
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(content);
            controller.close();
          },
        }),
        bodyUsed: false,
        arrayBuffer: async () => content.buffer.slice(content.byteOffset, content.byteOffset + content.byteLength) as ArrayBuffer,
        text: async () => new TextDecoder().decode(content),
        json: async () => JSON.parse(new TextDecoder().decode(content)),
        blob: async () => new Blob([content]),
        writeHttpMetadata: () => {},
      } as R2ObjectBody;
    }),

    put: vi.fn(async () => ({} as R2Object)),
    delete: vi.fn(async () => {}),
    head: vi.fn(async () => null),
    list: vi.fn(async () => ({
      objects: [],
      truncated: false,
      delimitedPrefixes: [],
    })),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
  } as unknown as R2Bucket & { _store: MockR2Store; _rangeRequests: { key: string; range?: R2RangeOption }[] };
}

// ============================================================================
// fetchFooter Tests
// ============================================================================

describe('fetchFooter', () => {
  it('should use suffix range request', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    const store: MockR2Store = { 'test.gcol': encoded };
    const r2 = createMockR2Bucket(store);

    await fetchFooter(r2, 'test.gcol');

    expect(r2._rangeRequests).toHaveLength(1);
    expect(r2._rangeRequests[0].range?.suffix).toBe(65536); // Default 64KB
  });

  it('should parse footer correctly', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    const store: MockR2Store = { 'test.gcol': encoded };
    const r2 = createMockR2Bucket(store);

    const result = await fetchFooter(r2, 'test.gcol');

    expect(result).not.toBeNull();
    expect(result!.footer.version).toBe(2);
    expect(result!.footer.entityCount).toBe(5);
    expect(result!.fileSize).toBe(encoded.length);
  });

  it('should include entity index', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    const store: MockR2Store = { 'test.gcol': encoded };
    const r2 = createMockR2Bucket(store);

    const result = await fetchFooter(r2, 'test.gcol');

    expect(result).not.toBeNull();
    expect(result!.index.entries).toHaveLength(5);
    expect(result!.index.version).toBe(1);
  });

  it('should return null for non-existent file', async () => {
    const r2 = createMockR2Bucket({});

    const result = await fetchFooter(r2, 'nonexistent.gcol');

    expect(result).toBeNull();
  });

  it('should return null for V1 file', async () => {
    // Import V1 encoder
    const { encodeGraphCol } = await import('../../src/storage/graphcol.js');
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphCol(triples, testNamespace);
    const store: MockR2Store = { 'v1.gcol': encoded };
    const r2 = createMockR2Bucket(store);

    const result = await fetchFooter(r2, 'v1.gcol');

    // V1 files don't have trailing magic, so parsing should fail
    expect(result).toBeNull();
  });

  it('should use custom footer size', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    const store: MockR2Store = { 'test.gcol': encoded };
    const r2 = createMockR2Bucket(store);

    const customSize = 1024;
    await fetchFooter(r2, 'test.gcol', customSize);

    expect(r2._rangeRequests).toHaveLength(1);
    expect(r2._rangeRequests[0].range?.suffix).toBe(customSize);
  });
});

// ============================================================================
// fetchEntityByRange Tests
// ============================================================================

describe('fetchEntityByRange', () => {
  it('should fetch correct bytes using offset+length range', async () => {
    const testData = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    const store: MockR2Store = { 'test.bin': testData };
    const r2 = createMockR2Bucket(store);

    const result = await fetchEntityByRange(r2, 'test.bin', 3, 4);

    expect(result).not.toBeNull();
    expect(Array.from(result!)).toEqual([3, 4, 5, 6]);
  });

  it('should make range request with correct parameters', async () => {
    const testData = new Uint8Array(100);
    const store: MockR2Store = { 'test.bin': testData };
    const r2 = createMockR2Bucket(store);

    await fetchEntityByRange(r2, 'test.bin', 10, 20);

    expect(r2._rangeRequests).toHaveLength(1);
    expect(r2._rangeRequests[0].range?.offset).toBe(10);
    expect(r2._rangeRequests[0].range?.length).toBe(20);
  });

  it('should return null for non-existent file', async () => {
    const r2 = createMockR2Bucket({});

    const result = await fetchEntityByRange(r2, 'nonexistent.bin', 0, 10);

    expect(result).toBeNull();
  });
});

// ============================================================================
// GraphLookup.lookupV2 Tests
// ============================================================================

describe('GraphLookup.lookupV2', () => {
  let r2: R2Bucket & { _store: MockR2Store; _rangeRequests: { key: string; range?: R2RangeOption }[] };
  let graphLookup: GraphLookup;

  beforeEach(() => {
    r2 = createMockR2Bucket({});
    graphLookup = new GraphLookup({ r2 });
  });

  it('should return entity with correct data', async () => {
    const entityId = 'https://example.com/entity/0002';
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2(entityId, 'chunks/test.gcol');

    expect(result.entity).not.toBeNull();
    expect(result.entity!.id).toBe(entityId);
    expect(result.stats.found).toBe(true);
  });

  it('should return null for missing entity', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2('https://example.com/entity/9999', 'chunks/test.gcol');

    expect(result.entity).toBeNull();
    expect(result.stats.found).toBe(false);
  });

  it('should cache footer on first call', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');

    const cache = graphLookup.getFooterCache();
    expect(cache.has('chunks/test.gcol')).toBe(true);
  });

  it('should use cached footer on second call', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    // First call - fetches footer
    const result1 = await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');
    expect(result1.stats.footerCacheHits).toBe(0);

    // Clear range requests tracker
    r2._rangeRequests.length = 0;

    // Second call - should use cache
    const result2 = await graphLookup.lookupV2('https://example.com/entity/0002', 'chunks/test.gcol');
    expect(result2.stats.footerCacheHits).toBe(1);

    // Should not have made a suffix range request for footer
    const suffixRequests = r2._rangeRequests.filter(r => r.range?.suffix !== undefined);
    expect(suffixRequests).toHaveLength(0);
  });

  it('should track rangeRequests in stats', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');

    expect(result.stats.rangeRequests).toBeGreaterThan(0);
  });

  it('should track fullFetches in stats', async () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');

    // Currently we fall back to full fetch for decoding
    expect(result.stats.fullFetches).toBeGreaterThan(0);
  });

  it('should return null for non-existent file', async () => {
    const result = await graphLookup.lookupV2('https://example.com/entity/0001', 'nonexistent.gcol');

    expect(result.entity).toBeNull();
    expect(result.stats.found).toBe(false);
  });

  it('should extract entity properties correctly', async () => {
    const entityId = 'https://example.com/entity/0002';
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2(entityId, 'chunks/test.gcol');

    expect(result.entity).not.toBeNull();
    expect(result.entity!.properties).toHaveProperty('name');
    expect(result.entity!.properties.name).toBe('Entity 2');
  });
});

// ============================================================================
// GraphLookup.clearCaches Tests
// ============================================================================

describe('GraphLookup.clearCaches', () => {
  it('should clear footer cache', async () => {
    const r2 = createMockR2Bucket({});
    const graphLookup = new GraphLookup({ r2 });

    // Populate cache
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');
    expect(graphLookup.getFooterCache().size).toBeGreaterThan(0);

    // Clear caches
    graphLookup.clearCaches();

    expect(graphLookup.getFooterCache().size).toBe(0);
  });
});

// ============================================================================
// Stats Tracking Tests
// ============================================================================

describe('Stats tracking', () => {
  it('should track timing in r2FetchMs', async () => {
    const r2 = createMockR2Bucket({});
    const graphLookup = new GraphLookup({ r2 });
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');

    expect(result.stats.r2FetchMs).toBeDefined();
    expect(result.stats.r2FetchMs).toBeGreaterThanOrEqual(0);
  });

  it('should track timing in decodeMs', async () => {
    const r2 = createMockR2Bucket({});
    const graphLookup = new GraphLookup({ r2 });
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');

    expect(result.stats.decodeMs).toBeDefined();
    expect(result.stats.decodeMs).toBeGreaterThanOrEqual(0);
  });

  it('should track total time in timeMs', async () => {
    const r2 = createMockR2Bucket({});
    const graphLookup = new GraphLookup({ r2 });
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);
    r2._store['chunks/test.gcol'] = encoded;

    const result = await graphLookup.lookupV2('https://example.com/entity/0001', 'chunks/test.gcol');

    expect(result.stats.timeMs).toBeDefined();
    expect(result.stats.timeMs).toBeGreaterThanOrEqual(0);
  });
});
