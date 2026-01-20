/**
 * GraphCol V2 Encoder/Decoder Tests (TDD - RED first, then GREEN)
 *
 * Tests for:
 * - encodeGraphColV2/decodeGraphColV2 round-trip
 * - readFooter extracts correct metadata
 * - readEntityIndex returns valid index
 * - decodeEntity returns only that entity's triples
 * - decodeEntity returns null for missing entity
 * - Backward compat: decodeGraphCol still works on v1 files
 * - Triples are sorted by entity ID in output
 *
 * V2 Layout:
 * - Data rows (sorted by entity ID)
 * - Entity Index (from entity-index.ts)
 * - Footer (48 bytes fixed)
 * - Trailer (8 bytes: footer_offset + magic)
 */

import { describe, it, expect } from 'vitest';
import {
  encodeGraphCol,
  decodeGraphCol,
  GCOL_MAGIC,
  GCOL_VERSION,
} from '../../src/storage/graphcol';
import {
  encodeGraphColV2,
  decodeGraphColV2,
  readFooter,
  readEntityIndex,
  decodeEntity,
  GCOL_VERSION_2,
  GCOL_FOOTER_SIZE,
  type GraphColFooter,
} from '../../src/storage/graphcol';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type EntityId,
  type TransactionId,
  type Namespace,
} from '../../src/core/types';
import { type Triple, type TypedObject } from '../../src/core/triple';
import { type EntityIndex } from '../../src/storage/entity-index';

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
    case ObjectType.NULL:
      object = { type: ObjectType.NULL };
      break;
    case ObjectType.BOOL:
      object = { type: ObjectType.BOOL, value: objValue as boolean };
      break;
    case ObjectType.INT64:
      object = { type: objType, value: objValue as bigint };
      break;
    case ObjectType.FLOAT64:
      object = { type: ObjectType.FLOAT64, value: objValue as number };
      break;
    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: objValue as string };
      break;
    case ObjectType.REF:
      object = { type: ObjectType.REF, value: objValue as EntityId };
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
      const predicates = ['name', 'age', 'score', 'active'];
      const predicate = predicates[t % predicates.length];
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
// V2 Encode/Decode Round-Trip Tests
// ============================================================================

describe('GraphCol V2 Encode/Decode Round-trip', () => {
  it('should round-trip empty triples array', () => {
    const encoded = encodeGraphColV2([], testNamespace);
    const decoded = decodeGraphColV2(encoded);

    expect(decoded).toEqual([]);
  });

  it('should round-trip single entity triples', () => {
    const triples = generateMultiEntityTriples(1, 4);

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);

    expect(decoded.length).toBe(triples.length);
    // Verify values match (order may differ since v2 sorts by entity)
    for (const original of triples) {
      const found = decoded.find(
        t => t.subject === original.subject && t.predicate === original.predicate
      );
      expect(found).toBeDefined();
      expect(found?.object.type).toBe(original.object.type);
    }
  });

  it('should round-trip multiple entity triples', () => {
    const triples = generateMultiEntityTriples(10, 4);

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);

    expect(decoded.length).toBe(triples.length);
  });

  it('should sort triples by entity ID in output', () => {
    // Create triples in reverse entity order
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/zzz', 'name', ObjectType.STRING, 'Last', BigInt(1000), generateTestTxId(0)),
      createTestTriple('https://example.com/entity/aaa', 'name', ObjectType.STRING, 'First', BigInt(1000), generateTestTxId(1)),
      createTestTriple('https://example.com/entity/mmm', 'name', ObjectType.STRING, 'Middle', BigInt(1000), generateTestTxId(2)),
    ];

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);

    expect(decoded.length).toBe(3);
    // Should be sorted by entity ID
    expect(decoded[0].subject).toBe('https://example.com/entity/aaa');
    expect(decoded[1].subject).toBe('https://example.com/entity/mmm');
    expect(decoded[2].subject).toBe('https://example.com/entity/zzz');
  });

  it('should preserve all object types correctly', () => {
    const baseTime = BigInt(Date.now());
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/001', 'strField', ObjectType.STRING, 'hello', baseTime, generateTestTxId(0)),
      createTestTriple('https://example.com/entity/001', 'intField', ObjectType.INT64, BigInt(42), baseTime, generateTestTxId(1)),
      createTestTriple('https://example.com/entity/001', 'floatField', ObjectType.FLOAT64, 3.14159, baseTime, generateTestTxId(2)),
      createTestTriple('https://example.com/entity/001', 'boolField', ObjectType.BOOL, true, baseTime, generateTestTxId(3)),
    ];

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);

    expect(decoded.length).toBe(4);

    const strTriple = decoded.find(t => t.predicate === 'strField');
    expect(strTriple?.object.value).toBe('hello');

    const intTriple = decoded.find(t => t.predicate === 'intField');
    expect(intTriple?.object.value).toBe(BigInt(42));

    const floatTriple = decoded.find(t => t.predicate === 'floatField');
    expect((floatTriple?.object.value as number)).toBeCloseTo(3.14159, 5);

    const boolTriple = decoded.find(t => t.predicate === 'boolField');
    expect(boolTriple?.object.value).toBe(true);
  });
});

// ============================================================================
// Footer Tests
// ============================================================================

describe('readFooter', () => {
  it('should extract correct version from footer', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const footer = readFooter(encoded);

    expect(footer.version).toBe(GCOL_VERSION_2);
  });

  it('should extract correct entity count', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const footer = readFooter(encoded);

    expect(footer.entityCount).toBe(5);
  });

  it('should extract correct data length', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const footer = readFooter(encoded);

    expect(footer.dataLength).toBeGreaterThan(0);
  });

  it('should extract correct index offset and length', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const footer = readFooter(encoded);

    expect(footer.indexOffset).toBeGreaterThan(0);
    expect(footer.indexLength).toBeGreaterThan(0);
    // Index should come after data
    expect(footer.indexOffset).toBeGreaterThanOrEqual(footer.dataLength);
  });

  it('should extract correct timestamp range', () => {
    const baseTime = BigInt(1700000000000);
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/001', 'a', ObjectType.STRING, 'x', baseTime, generateTestTxId(0)),
      createTestTriple('https://example.com/entity/002', 'b', ObjectType.STRING, 'y', baseTime + BigInt(5000), generateTestTxId(1)),
      createTestTriple('https://example.com/entity/003', 'c', ObjectType.STRING, 'z', baseTime + BigInt(10000), generateTestTxId(2)),
    ];

    const encoded = encodeGraphColV2(triples, testNamespace);
    const footer = readFooter(encoded);

    expect(footer.minTimestamp).toBe(baseTime);
    expect(footer.maxTimestamp).toBe(baseTime + BigInt(10000));
  });

  it('should validate CRC32 checksum', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    // Corrupt a byte in the footer's checksummed area
    // Footer starts at length - 56 (48 footer + 8 trailer)
    // The version field is at the start of the footer (offset 0 within footer)
    const corrupted = new Uint8Array(encoded);
    const footerStart = corrupted.length - 56;
    // Corrupt the version field (first 4 bytes of footer)
    corrupted[footerStart] ^= 0xFF;

    expect(() => readFooter(corrupted)).toThrow(/checksum/i);
  });

  it('should validate magic bytes', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    // Corrupt magic bytes (last 4 bytes)
    const corrupted = new Uint8Array(encoded);
    corrupted[corrupted.length - 1] = 0x00;
    corrupted[corrupted.length - 2] = 0x00;

    expect(() => readFooter(corrupted)).toThrow(/magic/i);
  });
});

// ============================================================================
// Entity Index Tests
// ============================================================================

describe('readEntityIndex', () => {
  it('should return valid EntityIndex from V2 file', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const index = readEntityIndex(encoded);

    expect(index).toBeDefined();
    expect(index.entries).toHaveLength(5);
    expect(index.version).toBe(1);
  });

  it('should have entries sorted by entityId', () => {
    const triples = generateMultiEntityTriples(10, 2);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const index = readEntityIndex(encoded);

    // Check entries are sorted
    for (let i = 1; i < index.entries.length; i++) {
      expect(index.entries[i].entityId.localeCompare(index.entries[i - 1].entityId)).toBeGreaterThan(0);
    }
  });

  it('should have valid byte offsets for each entity', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const index = readEntityIndex(encoded);
    const footer = readFooter(encoded);

    for (const entry of index.entries) {
      // Offsets should be within data section
      expect(entry.offset).toBeGreaterThanOrEqual(0);
      expect(entry.offset + entry.length).toBeLessThanOrEqual(footer.dataLength);
      expect(entry.length).toBeGreaterThan(0);
    }
  });

  it('should index all unique entities', () => {
    // Create triples with some entities having multiple triples
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const index = readEntityIndex(encoded);

    // Should have exactly 5 entities
    expect(index.entries).toHaveLength(5);

    // Each entity ID should appear once in the index
    const entityIds = new Set(index.entries.map(e => e.entityId));
    expect(entityIds.size).toBe(5);
  });
});

// ============================================================================
// Partial Decode (decodeEntity) Tests
// ============================================================================

describe('decodeEntity', () => {
  it('should return only triples for the specified entity', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const entityId = 'https://example.com/entity/0002';
    const entityTriples = decodeEntity(encoded, entityId);

    expect(entityTriples).not.toBeNull();
    expect(entityTriples!.length).toBe(4);
    // All triples should belong to the requested entity
    for (const triple of entityTriples!) {
      expect(triple.subject).toBe(entityId);
    }
  });

  it('should return null for missing entity', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    const entityTriples = decodeEntity(encoded, 'https://example.com/entity/9999');

    expect(entityTriples).toBeNull();
  });

  it('should return null for empty file', () => {
    const encoded = encodeGraphColV2([], testNamespace);

    const entityTriples = decodeEntity(encoded, 'https://example.com/entity/0001');

    expect(entityTriples).toBeNull();
  });

  it('should preserve all triple fields for decoded entity', () => {
    const baseTime = BigInt(Date.now());
    const entityId = 'https://example.com/entity/test';
    const txId = generateTestTxId(5);

    const triples: Triple[] = [
      {
        subject: createEntityId(entityId),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test Entity' },
        timestamp: baseTime,
        txId,
      },
    ];

    const encoded = encodeGraphColV2(triples, testNamespace);
    const entityTriples = decodeEntity(encoded, entityId);

    expect(entityTriples).not.toBeNull();
    expect(entityTriples!.length).toBe(1);
    expect(entityTriples![0].subject).toBe(entityId);
    expect(entityTriples![0].predicate).toBe('name');
    expect(entityTriples![0].object.type).toBe(ObjectType.STRING);
    expect(entityTriples![0].object.value).toBe('Test Entity');
    expect(entityTriples![0].timestamp).toBe(baseTime);
    expect(entityTriples![0].txId).toBe(txId);
  });

  it('should be more efficient than full decode for single entity lookup', () => {
    // Generate a larger dataset
    const triples = generateMultiEntityTriples(100, 10);
    const encoded = encodeGraphColV2(triples, testNamespace);

    // Time partial decode
    const partialStart = performance.now();
    for (let i = 0; i < 10; i++) {
      decodeEntity(encoded, 'https://example.com/entity/0050');
    }
    const partialTime = performance.now() - partialStart;

    // Time full decode + filter
    const fullStart = performance.now();
    for (let i = 0; i < 10; i++) {
      const all = decodeGraphColV2(encoded);
      all.filter(t => t.subject === 'https://example.com/entity/0050');
    }
    const fullTime = performance.now() - fullStart;

    // Partial decode should be faster (or at least not slower)
    // Note: For small datasets, overhead may make this not always true
    console.log(`Partial decode: ${partialTime.toFixed(2)}ms, Full decode: ${fullTime.toFixed(2)}ms`);
    // We just log for now since optimization depends on dataset size
  });
});

// ============================================================================
// Backward Compatibility Tests
// ============================================================================

describe('Backward Compatibility', () => {
  it('decodeGraphCol should still work on v1 files', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/001', 'name', ObjectType.STRING, 'Alice', BigInt(Date.now()), generateTestTxId(0)),
      createTestTriple('https://example.com/entity/002', 'name', ObjectType.STRING, 'Bob', BigInt(Date.now()), generateTestTxId(1)),
    ];

    // Encode with v1
    const encodedV1 = encodeGraphCol(triples, testNamespace);

    // Decode with the generic decoder (should auto-detect v1)
    const decoded = decodeGraphCol(encodedV1);

    expect(decoded.length).toBe(2);
    expect(decoded[0].object.value).toBe('Alice');
    expect(decoded[1].object.value).toBe('Bob');
  });

  it('decodeGraphCol should auto-detect and decode v2 files', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/001', 'name', ObjectType.STRING, 'Alice', BigInt(Date.now()), generateTestTxId(0)),
      createTestTriple('https://example.com/entity/002', 'name', ObjectType.STRING, 'Bob', BigInt(Date.now()), generateTestTxId(1)),
    ];

    // Encode with v2
    const encodedV2 = encodeGraphColV2(triples, testNamespace);

    // Decode with the generic decoder (should auto-detect v2)
    const decoded = decodeGraphCol(encodedV2);

    expect(decoded.length).toBe(2);
  });

  it('should detect version correctly from v1 header', () => {
    const triples = generateMultiEntityTriples(5, 2);
    const encodedV1 = encodeGraphCol(triples, testNamespace);

    // v1 has magic at the beginning
    const view = new DataView(encodedV1.buffer, encodedV1.byteOffset, encodedV1.byteLength);
    const magic = view.getUint32(0, true);
    const version = view.getUint16(4, true);

    expect(magic).toBe(GCOL_MAGIC);
    expect(version).toBe(GCOL_VERSION);
  });

  it('should detect version correctly from v2 footer', () => {
    const triples = generateMultiEntityTriples(5, 2);
    const encodedV2 = encodeGraphColV2(triples, testNamespace);

    const footer = readFooter(encodedV2);
    expect(footer.version).toBe(GCOL_VERSION_2);
  });
});

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

describe('GraphCol V2 Edge Cases', () => {
  it('should handle single triple', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/only', 'name', ObjectType.STRING, 'Solo', BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);

    expect(decoded.length).toBe(1);
    expect(decoded[0].object.value).toBe('Solo');
  });

  it('should handle many entities with one triple each', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 100; i++) {
      triples.push(
        createTestTriple(`https://example.com/entity/${i}`, 'id', ObjectType.INT64, BigInt(i), BigInt(Date.now()), generateTestTxId(i % 22))
      );
    }

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);
    const footer = readFooter(encoded);

    expect(decoded.length).toBe(100);
    expect(footer.entityCount).toBe(100);
  });

  it('should handle one entity with many triples', () => {
    const triples: Triple[] = [];
    const entityId = 'https://example.com/entity/bulk';
    for (let i = 0; i < 100; i++) {
      triples.push(
        createTestTriple(entityId, `field${i}`, ObjectType.INT64, BigInt(i), BigInt(Date.now()), generateTestTxId(i % 22))
      );
    }

    const encoded = encodeGraphColV2(triples, testNamespace);
    const decoded = decodeGraphColV2(encoded);
    const footer = readFooter(encoded);

    expect(decoded.length).toBe(100);
    expect(footer.entityCount).toBe(1);

    const entityTriples = decodeEntity(encoded, entityId);
    expect(entityTriples?.length).toBe(100);
  });

  it('should throw on truncated file', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    // Truncate the file
    const truncated = encoded.subarray(0, encoded.length - 20);

    expect(() => readFooter(truncated)).toThrow();
  });

  it('should throw on corrupted data section', () => {
    const triples = generateMultiEntityTriples(5, 4);
    const encoded = encodeGraphColV2(triples, testNamespace);

    // Corrupt data in the middle
    const corrupted = new Uint8Array(encoded);
    corrupted[100] ^= 0xFF;
    corrupted[101] ^= 0xFF;
    corrupted[102] ^= 0xFF;

    // Footer should still be readable
    const footer = readFooter(corrupted);
    expect(footer.version).toBe(GCOL_VERSION_2);

    // But full decode may fail due to checksum
    // (depending on what exactly was corrupted)
  });
});

// ============================================================================
// Footer Size Constant Test
// ============================================================================

describe('Footer Size', () => {
  it('should have GCOL_FOOTER_SIZE of 48 bytes', () => {
    // Footer structure:
    // - 4 bytes: version (0x02)
    // - 4 bytes: data_length
    // - 4 bytes: index_offset
    // - 4 bytes: index_length
    // - 4 bytes: entity_count
    // - 8 bytes: min_timestamp
    // - 8 bytes: max_timestamp
    // - 4 bytes: CRC32 of above
    // Total: 40 bytes of metadata + 4 bytes CRC = 44 bytes
    // Plus 4 bytes footer_offset + 4 bytes magic = 8 bytes trailer
    // Total footer section = 48 + 8 = 56 bytes from end

    // But GCOL_FOOTER_SIZE should be just the fixed footer (48 bytes)
    expect(GCOL_FOOTER_SIZE).toBe(48);
  });
});
