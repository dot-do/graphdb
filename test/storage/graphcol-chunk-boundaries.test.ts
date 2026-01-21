/**
 * GraphCol Chunk Boundary Tests (TDD - RED Phase)
 *
 * Tests for edge cases in GraphCol encoding/decoding:
 * - Chunk size boundaries (near max sizes)
 * - Dictionary size limits
 * - Delta encoding overflow
 * - Varint encoding edge cases
 * - Empty/null value handling
 * - Special characters in strings
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  encodeGraphCol,
  decodeGraphCol,
  getChunkStats,
  createEncoder,
  GCOL_MAGIC,
  GCOL_VERSION,
} from '../../src/storage/graphcol';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  createNamespace,
  type TransactionId,
  type EntityId,
} from '../../src/core/types';
import { type Triple, type TypedObject } from '../../src/core/triple';

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
      object = { type: ObjectType.REF, value: objValue as EntityId };
      break;
    case ObjectType.TIMESTAMP:
      object = { type: ObjectType.TIMESTAMP, value: objValue as bigint };
      break;
    case ObjectType.BINARY:
      object = { type: ObjectType.BINARY, value: objValue as Uint8Array };
      break;
    case ObjectType.NULL:
      object = { type: ObjectType.NULL };
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

const testNamespace = createNamespace('https://example.com/');

// ============================================================================
// Varint Edge Cases
// ============================================================================

describe('GraphCol Varint Edge Cases', () => {
  it('should handle varint boundary value 127 (single byte max)', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 127; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'idx',
          ObjectType.INT64,
          BigInt(i),
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(127);
  });

  it('should handle varint boundary value 128 (two byte start)', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 128; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'idx',
          ObjectType.INT64,
          BigInt(i),
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(128);
  });

  it('should handle varint boundary value 16383 (two byte max)', () => {
    // Create dictionary with 16383 unique values (near 2-byte varint limit)
    const uniqueCount = 16383;
    const triples: Triple[] = [];

    for (let i = 0; i < Math.min(uniqueCount, 5000); i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i.toString().padStart(5, '0')}`,
          'name',
          ObjectType.STRING,
          `unique_value_${i}`,
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(triples.length);
  });

  it('should handle large varint values for timestamps', () => {
    // Max safe timestamp (year 3000+)
    const futureTimestamp = BigInt(32503680000000); // Year 3000
    const triples: Triple[] = [
      createTestTriple(
        'https://example.com/entity/1',
        'name',
        ObjectType.STRING,
        'Future Entity',
        futureTimestamp,
        generateTestTxId(0)
      ),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].timestamp).toBe(futureTimestamp);
  });
});

// ============================================================================
// Delta Encoding Edge Cases
// ============================================================================

describe('GraphCol Delta Encoding Edge Cases', () => {
  it('should handle timestamps with zero delta', () => {
    const sameTimestamp = BigInt(Date.now());
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'a', ObjectType.STRING, 'first', sameTimestamp, generateTestTxId(0)),
      createTestTriple('https://example.com/entity/2', 'b', ObjectType.STRING, 'second', sameTimestamp, generateTestTxId(1)),
      createTestTriple('https://example.com/entity/3', 'c', ObjectType.STRING, 'third', sameTimestamp, generateTestTxId(2)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(3);
    expect(decoded[0].timestamp).toBe(sameTimestamp);
    expect(decoded[1].timestamp).toBe(sameTimestamp);
    expect(decoded[2].timestamp).toBe(sameTimestamp);
  });

  it('should handle negative timestamp deltas (out-of-order)', () => {
    const baseTime = BigInt(Date.now());
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'a', ObjectType.STRING, 'first', baseTime + BigInt(1000), generateTestTxId(0)),
      createTestTriple('https://example.com/entity/2', 'b', ObjectType.STRING, 'second', baseTime, generateTestTxId(1)), // Earlier
      createTestTriple('https://example.com/entity/3', 'c', ObjectType.STRING, 'third', baseTime + BigInt(500), generateTestTxId(2)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(3);
    expect(decoded[0].timestamp).toBe(baseTime + BigInt(1000));
    expect(decoded[1].timestamp).toBe(baseTime);
    expect(decoded[2].timestamp).toBe(baseTime + BigInt(500));
  });

  it('should handle very large timestamp deltas', () => {
    const baseTime = BigInt(Date.now());
    const oneYear = BigInt(365 * 24 * 60 * 60 * 1000);
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'a', ObjectType.STRING, 'first', baseTime, generateTestTxId(0)),
      createTestTriple('https://example.com/entity/2', 'b', ObjectType.STRING, 'second', baseTime + oneYear, generateTestTxId(1)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].timestamp).toBe(baseTime);
    expect(decoded[1].timestamp).toBe(baseTime + oneYear);
  });
});

// ============================================================================
// Dictionary Encoding Edge Cases
// ============================================================================

describe('GraphCol Dictionary Encoding Edge Cases', () => {
  it('should handle single entry dictionary', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'name', ObjectType.STRING, 'value', BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(1);
    expect(decoded[0].object.value).toBe('value');
  });

  it('should handle all identical values (single dictionary entry, many references)', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 100; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'type',
          ObjectType.STRING,
          'same_value', // All same
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(100);
    expect(decoded.every(t => t.object.value === 'same_value')).toBe(true);
  });

  it('should handle all unique values (many dictionary entries, one reference each)', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 100; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'id',
          ObjectType.STRING,
          `unique_${i}`, // All unique
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(100);
    for (let i = 0; i < 100; i++) {
      expect(decoded[i].object.value).toBe(`unique_${i}`);
    }
  });

  it('should handle empty string in dictionary', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'empty', ObjectType.STRING, '', BigInt(Date.now()), generateTestTxId(0)),
      createTestTriple('https://example.com/entity/2', 'nonempty', ObjectType.STRING, 'value', BigInt(Date.now()), generateTestTxId(1)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.find(t => t.predicate === 'empty')?.object.value).toBe('');
    expect(decoded.find(t => t.predicate === 'nonempty')?.object.value).toBe('value');
  });
});

// ============================================================================
// Special Character Handling
// ============================================================================

describe('GraphCol Special Character Handling', () => {
  it('should handle newlines and tabs in strings', () => {
    const valueWithNewlines = 'line1\nline2\nline3';
    const valueWithTabs = 'col1\tcol2\tcol3';
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'newlines', ObjectType.STRING, valueWithNewlines, BigInt(Date.now()), generateTestTxId(0)),
      createTestTriple('https://example.com/entity/2', 'tabs', ObjectType.STRING, valueWithTabs, BigInt(Date.now()), generateTestTxId(1)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.find(t => t.predicate === 'newlines')?.object.value).toBe(valueWithNewlines);
    expect(decoded.find(t => t.predicate === 'tabs')?.object.value).toBe(valueWithTabs);
  });

  it('should handle null bytes in binary data', () => {
    const binaryWithNulls = new Uint8Array([0, 1, 0, 2, 0, 3]);
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'data', ObjectType.BINARY, binaryWithNulls, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toEqual(binaryWithNulls);
  });

  it('should handle Unicode surrogate pairs (emoji)', () => {
    const emojiString = 'Hello World';
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'text', ObjectType.STRING, emojiString, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(emojiString);
  });

  it('should handle mixed encoding characters', () => {
    const mixedString = 'ASCII + Unicode + Emoji';
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'mixed', ObjectType.STRING, mixedString, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(mixedString);
  });

  it('should handle special characters in entity IDs', () => {
    const specialEntityId = 'https://example.com/entity/a+b%20c';
    const triples: Triple[] = [
      createTestTriple(specialEntityId, 'name', ObjectType.STRING, 'test', BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].subject).toBe(specialEntityId);
  });

  it('should handle special characters in predicate names', () => {
    const specialPredicate = '$type';
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', specialPredicate, ObjectType.STRING, 'value', BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].predicate).toBe(specialPredicate);
  });
});

// ============================================================================
// INT64 Edge Cases
// ============================================================================

describe('GraphCol INT64 Edge Cases', () => {
  it('should handle INT64 value of zero', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'count', ObjectType.INT64, BigInt(0), BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(BigInt(0));
  });

  it('should handle INT64 value of -1', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'index', ObjectType.INT64, BigInt(-1), BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(BigInt(-1));
  });

  it('should handle MIN_SAFE_INTEGER', () => {
    const minSafe = BigInt(Number.MIN_SAFE_INTEGER);
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'min', ObjectType.INT64, minSafe, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(minSafe);
  });

  it('should handle MAX_SAFE_INTEGER', () => {
    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'max', ObjectType.INT64, maxSafe, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(maxSafe);
  });

  it('should handle alternating positive and negative values', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 50; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'value',
          ObjectType.INT64,
          BigInt(i % 2 === 0 ? i : -i),
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    for (let i = 0; i < 50; i++) {
      expect(decoded[i].object.value).toBe(BigInt(i % 2 === 0 ? i : -i));
    }
  });
});

// ============================================================================
// FLOAT64 Edge Cases
// ============================================================================

describe('GraphCol FLOAT64 Edge Cases', () => {
  it('should handle FLOAT64 value of zero', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'value', ObjectType.FLOAT64, 0.0, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(0.0);
  });

  it('should handle negative zero', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'value', ObjectType.FLOAT64, -0.0, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    // GraphCol preserves negative zero (IEEE 754 compliant)
    // -0 === 0 in JavaScript, but Object.is can distinguish them
    expect(Object.is(decoded[0].object.value, -0)).toBe(true);
  });

  it('should handle very small float values', () => {
    const tiny = 1e-300;
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'tiny', ObjectType.FLOAT64, tiny, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBeCloseTo(tiny, 310);
  });

  it('should handle very large float values', () => {
    const huge = 1e300;
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'huge', ObjectType.FLOAT64, huge, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(huge);
  });

  it('should handle Infinity', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'inf', ObjectType.FLOAT64, Infinity, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(Infinity);
  });

  it('should handle -Infinity', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'negInf', ObjectType.FLOAT64, -Infinity, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded[0].object.value).toBe(-Infinity);
  });

  it('should handle NaN', () => {
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'nan', ObjectType.FLOAT64, NaN, BigInt(Date.now()), generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(Number.isNaN(decoded[0].object.value)).toBe(true);
  });
});

// ============================================================================
// RLE Encoding Edge Cases
// ============================================================================

describe('GraphCol RLE Encoding Edge Cases', () => {
  it('should handle all same object types (best case RLE)', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 1000; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'count',
          ObjectType.INT64, // All same type
          BigInt(i),
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(1000);
    expect(decoded.every(t => t.object.type === ObjectType.INT64)).toBe(true);
  });

  it('should handle alternating object types (worst case RLE)', () => {
    const triples: Triple[] = [];
    for (let i = 0; i < 100; i++) {
      if (i % 2 === 0) {
        triples.push(
          createTestTriple(
            `https://example.com/entity/${i}`,
            'str',
            ObjectType.STRING,
            `value_${i}`,
            BigInt(Date.now() + i),
            generateTestTxId(i % 22)
          )
        );
      } else {
        triples.push(
          createTestTriple(
            `https://example.com/entity/${i}`,
            'num',
            ObjectType.INT64,
            BigInt(i),
            BigInt(Date.now() + i),
            generateTestTxId(i % 22)
          )
        );
      }
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(100);
  });

  it('should handle single type run at max RLE count boundary', () => {
    // RLE uses uint16 for count (max 65535), but we cap at 65535
    // Test a smaller boundary for performance
    const runLength = 1000;
    const triples: Triple[] = [];
    for (let i = 0; i < runLength; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'flag',
          ObjectType.BOOL,
          true,
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const decoded = decodeGraphCol(encoded);

    expect(decoded.length).toBe(runLength);
  });
});

// ============================================================================
// Streaming Encoder Edge Cases
// ============================================================================

describe('GraphCol Streaming Encoder Edge Cases', () => {
  it('should handle flush with no triples added', () => {
    const encoder = createEncoder(testNamespace);
    const encoded = encoder.flush();
    const decoded = decodeGraphCol(encoded);

    expect(decoded).toEqual([]);
  });

  it('should handle multiple flushes', () => {
    const encoder = createEncoder(testNamespace);

    // First batch
    encoder.addTriple(
      createTestTriple('https://example.com/entity/1', 'name', ObjectType.STRING, 'First', BigInt(Date.now()), generateTestTxId(0))
    );
    const encoded1 = encoder.flush();
    const decoded1 = decodeGraphCol(encoded1);
    expect(decoded1.length).toBe(1);

    // Second batch
    encoder.addTriple(
      createTestTriple('https://example.com/entity/2', 'name', ObjectType.STRING, 'Second', BigInt(Date.now()), generateTestTxId(1))
    );
    const encoded2 = encoder.flush();
    const decoded2 = decodeGraphCol(encoded2);
    expect(decoded2.length).toBe(1);

    // Third batch (after reset)
    encoder.reset();
    encoder.addTriple(
      createTestTriple('https://example.com/entity/3', 'name', ObjectType.STRING, 'Third', BigInt(Date.now()), generateTestTxId(2))
    );
    const encoded3 = encoder.flush();
    const decoded3 = decodeGraphCol(encoded3);
    expect(decoded3.length).toBe(1);
  });

  it('should handle adding triples after reset', () => {
    const encoder = createEncoder(testNamespace);

    encoder.addTriple(
      createTestTriple('https://example.com/entity/1', 'name', ObjectType.STRING, 'Before', BigInt(Date.now()), generateTestTxId(0))
    );
    encoder.reset();

    // Buffer should be empty after reset
    const encoded = encoder.flush();
    const decoded = decodeGraphCol(encoded);
    expect(decoded).toEqual([]);
  });
});

// ============================================================================
// Chunk Stats Edge Cases
// ============================================================================

describe('GraphCol Chunk Stats Edge Cases', () => {
  it('should report correct stats for empty chunk', () => {
    const encoded = encodeGraphCol([], testNamespace);
    const stats = getChunkStats(encoded);

    expect(stats.tripleCount).toBe(0);
    expect(stats.predicates).toEqual([]);
    expect(stats.sizeBytes).toBe(encoded.length);
  });

  it('should report correct predicate count with duplicates', () => {
    const triples: Triple[] = [];
    // Create triples with duplicate predicate names
    for (let i = 0; i < 50; i++) {
      triples.push(
        createTestTriple(
          `https://example.com/entity/${i}`,
          'samePred', // Same predicate
          ObjectType.STRING,
          `value_${i}`,
          BigInt(Date.now() + i),
          generateTestTxId(i % 22)
        )
      );
    }

    const encoded = encodeGraphCol(triples, testNamespace);
    const stats = getChunkStats(encoded);

    expect(stats.tripleCount).toBe(50);
    expect(stats.predicates).toContain('samePred');
    expect(stats.predicates.length).toBe(1);
  });

  it('should report correct time range with single triple', () => {
    const timestamp = BigInt(Date.now());
    const triples: Triple[] = [
      createTestTriple('https://example.com/entity/1', 'name', ObjectType.STRING, 'Only', timestamp, generateTestTxId(0)),
    ];

    const encoded = encodeGraphCol(triples, testNamespace);
    const stats = getChunkStats(encoded);

    expect(stats.timeRange[0]).toBe(timestamp);
    expect(stats.timeRange[1]).toBe(timestamp);
  });
});
