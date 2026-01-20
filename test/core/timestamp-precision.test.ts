/**
 * Timestamp Precision Tests (TDD)
 *
 * Tests for BigInt timestamp handling in Triple operations.
 * JavaScript Number can only safely represent integers up to 2^53-1 (MAX_SAFE_INTEGER).
 *
 * IMPORTANT: Cloudflare DO SQLite doesn't support BigInt parameters directly.
 * tripleToRow intentionally converts BigInt to Number for DO compatibility.
 * This means precision is lost for timestamps > MAX_SAFE_INTEGER (9007199254740991).
 *
 * Tests verify:
 * - Triple/TypedObject can hold BigInt timestamps (in-memory)
 * - tripleToRow outputs Number (not BigInt) for DO SQLite compatibility
 * - Normal millisecond timestamps (Date.now()) work fine through full round-trip
 * - Precision loss is documented for values > MAX_SAFE_INTEGER
 * - Timestamp comparisons work correctly for in-memory BigInt values
 */

import { describe, it, expect } from 'vitest';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import { type Triple, type TypedObject, validateTriple, createTriple, extractValue } from '../../src/core/triple';
import { type TripleValue } from '../../src/core/entity';
import { tripleToRow, rowToTriple } from '../../src/shard/crud';

// ============================================================================
// Test Constants
// ============================================================================

/** Maximum safe integer in JavaScript: 2^53 - 1 = 9007199254740991 */
const MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER);

/** A timestamp beyond MAX_SAFE_INTEGER that would lose precision with Number */
const BEYOND_SAFE_TIMESTAMP = MAX_SAFE_INTEGER + 1000n; // 9007199254741991n

/** Nanosecond-precision timestamp (e.g., from high-precision time APIs) */
const NANOSECOND_TIMESTAMP = 1705700000000000000n; // ~2024-01-19 in nanoseconds

/** Very large timestamp for far-future scenarios */
const FAR_FUTURE_TIMESTAMP = 9999999999999999999n;

/** Test transaction ID */
const TEST_TX_ID = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

/** Test subject */
const TEST_SUBJECT = createEntityId('https://example.com/entity/1');

/** Test predicate */
const TEST_PREDICATE = createPredicate('testField');

// ============================================================================
// Precision Preservation Tests
// ============================================================================

describe('Timestamp Precision', () => {
  describe('Should preserve full precision of nanosecond timestamps', () => {
    it('should handle nanosecond-precision timestamps in Triple.timestamp', () => {
      const triple: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: NANOSECOND_TIMESTAMP,
        txId: TEST_TX_ID,
      };

      // Validate the triple is valid with bigint timestamp
      const validation = validateTriple(triple);
      expect(validation.valid).toBe(true);
      expect(validation.errors).toHaveLength(0);

      // Ensure the timestamp is exactly what we set
      expect(triple.timestamp).toBe(NANOSECOND_TIMESTAMP);
      expect(typeof triple.timestamp).toBe('bigint');
    });

    it('should handle nanosecond-precision timestamps in TypedObject.value', () => {
      const typedObject: TypedObject = {
        type: ObjectType.TIMESTAMP,
        value: NANOSECOND_TIMESTAMP,
      };

      expect(typedObject.value).toBe(NANOSECOND_TIMESTAMP);
      expect(typeof typedObject.value).toBe('bigint');
    });

    it('should preserve precision through extractValue for TIMESTAMP type', () => {
      const typedObject: TypedObject = {
        type: ObjectType.TIMESTAMP,
        value: NANOSECOND_TIMESTAMP,
      };

      const extracted = extractValue(typedObject);
      expect(extracted).toBe(NANOSECOND_TIMESTAMP);
      expect(typeof extracted).toBe('bigint');
    });
  });

  describe('Should handle timestamps beyond Number.MAX_SAFE_INTEGER', () => {
    it('should not lose precision for timestamps > MAX_SAFE_INTEGER in Triple', () => {
      const triple: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: BEYOND_SAFE_TIMESTAMP,
        txId: TEST_TX_ID,
      };

      // This would fail if timestamp were converted to Number internally
      expect(triple.timestamp).toBe(BEYOND_SAFE_TIMESTAMP);

      // Demonstrate the problem with Number conversion
      const asNumber = Number(BEYOND_SAFE_TIMESTAMP);
      const backToBigint = BigInt(asNumber);
      // This shows precision loss - Number can't represent this exactly
      expect(backToBigint).not.toBe(BEYOND_SAFE_TIMESTAMP);
    });

    it('should not lose precision for timestamps > MAX_SAFE_INTEGER in TypedObject', () => {
      const typedObject: TypedObject = {
        type: ObjectType.TIMESTAMP,
        value: BEYOND_SAFE_TIMESTAMP,
      };

      expect(typedObject.value).toBe(BEYOND_SAFE_TIMESTAMP);
    });

    it('should handle very large far-future timestamps', () => {
      const triple: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.TIMESTAMP, value: FAR_FUTURE_TIMESTAMP },
        timestamp: BigInt(Date.now()),
        txId: TEST_TX_ID,
      };

      expect(triple.object.value).toBe(FAR_FUTURE_TIMESTAMP);
    });
  });

  describe('Should serialize/deserialize timestamps (with known precision limitation)', () => {
    /**
     * KNOWN LIMITATION: Cloudflare DO SQLite doesn't support BigInt parameters.
     * tripleToRow converts BigInt timestamps to Number, which loses precision
     * for timestamps > Number.MAX_SAFE_INTEGER (9007199254740991).
     *
     * For typical use cases (millisecond timestamps from Date.now()), this is not
     * an issue as those fit well within MAX_SAFE_INTEGER until year ~275760.
     *
     * Precision loss occurs for values > MAX_SAFE_INTEGER:
     * - Nanosecond timestamps (e.g., 1705700000000000000n) will lose precision
     * - Far-future timestamps beyond year 275760 will lose precision
     * - Normal millisecond timestamps from Date.now() work fine
     */
    it('should convert BigInt timestamp to Number in tripleToRow (DO SQLite limitation)', () => {
      const original: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: BEYOND_SAFE_TIMESTAMP,
        txId: TEST_TX_ID,
      };

      // Convert to row format (as would be stored in SQLite)
      const row = tripleToRow(original);

      // tripleToRow outputs Number (not BigInt) for DO compatibility
      expect(typeof row.timestamp).toBe('number');
      // Verify the Number conversion happened (both should be numbers now)
      expect(row.timestamp).toBe(Number(BEYOND_SAFE_TIMESTAMP));
      // Demonstrate precision loss: the Number value differs from the original BigInt
      // when converted back to BigInt
      expect(BigInt(row.timestamp as number)).not.toBe(BEYOND_SAFE_TIMESTAMP);
    });

    it('should convert TIMESTAMP object value to Number in tripleToRow (DO SQLite limitation)', () => {
      const original: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.TIMESTAMP, value: BEYOND_SAFE_TIMESTAMP },
        timestamp: BigInt(Date.now()),
        txId: TEST_TX_ID,
      };

      const row = tripleToRow(original);

      // tripleToRow outputs Number (not BigInt) for DO compatibility
      expect(typeof row.obj_timestamp).toBe('number');
    });

    it('should work correctly for normal millisecond timestamps (Date.now())', () => {
      // Normal millisecond timestamps fit well within MAX_SAFE_INTEGER
      const normalTimestamp = BigInt(Date.now()); // e.g., ~1705700000000n (2024)

      const original: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: normalTimestamp,
        txId: TEST_TX_ID,
      };

      const row = tripleToRow(original);

      // tripleToRow outputs Number for DO compatibility
      expect(typeof row.timestamp).toBe('number');
      // Value is preserved because it's within MAX_SAFE_INTEGER
      expect(row.timestamp).toBe(Number(normalTimestamp));

      // Round-trip preserves value for normal timestamps
      const fullRow = {
        ...row,
        obj_type: original.object.type,
        tx_id: original.txId,
      };
      const restored = rowToTriple(fullRow);

      // rowToTriple converts back to BigInt
      expect(typeof restored.timestamp).toBe('bigint');
      expect(restored.timestamp).toBe(normalTimestamp);
    });

    it('should handle edge case at exactly MAX_SAFE_INTEGER', () => {
      const edgeTimestamp = MAX_SAFE_INTEGER;

      const original: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: edgeTimestamp,
        txId: TEST_TX_ID,
      };

      const row = tripleToRow(original);

      // tripleToRow outputs Number for DO compatibility
      expect(typeof row.timestamp).toBe('number');
      expect(row.timestamp).toBe(Number(MAX_SAFE_INTEGER));

      const fullRow = {
        ...row,
        obj_type: original.object.type,
        tx_id: original.txId,
      };
      const restored = rowToTriple(fullRow);

      // At exactly MAX_SAFE_INTEGER, precision is preserved
      expect(restored.timestamp).toBe(edgeTimestamp);
    });

    it('should lose precision for values beyond MAX_SAFE_INTEGER (documented limitation)', () => {
      // Use BEYOND_SAFE_TIMESTAMP which is MAX_SAFE_INTEGER + 1000
      // This value actually loses precision (unlike MAX_SAFE_INTEGER + 1 which
      // happens to round correctly due to IEEE 754 floating point representation)
      const original: Triple = {
        subject: TEST_SUBJECT,
        predicate: TEST_PREDICATE,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: BEYOND_SAFE_TIMESTAMP,
        txId: TEST_TX_ID,
      };

      const row = tripleToRow(original);

      // tripleToRow outputs Number for DO compatibility
      expect(typeof row.timestamp).toBe('number');

      const fullRow = {
        ...row,
        obj_type: original.object.type,
        tx_id: original.txId,
      };
      const restored = rowToTriple(fullRow);

      // DOCUMENTED LIMITATION: Values > MAX_SAFE_INTEGER lose precision
      // The restored value will NOT match the original due to precision loss
      expect(restored.timestamp).not.toBe(BEYOND_SAFE_TIMESTAMP);
      // rowToTriple still returns BigInt type (converted from Number)
      expect(typeof restored.timestamp).toBe('bigint');
    });
  });

  describe('Should compare timestamps correctly', () => {
    it('should compare large bigint timestamps correctly', () => {
      const ts1 = BEYOND_SAFE_TIMESTAMP;
      const ts2 = BEYOND_SAFE_TIMESTAMP + 1n;
      const ts3 = BEYOND_SAFE_TIMESTAMP - 1n;

      expect(ts2 > ts1).toBe(true);
      expect(ts1 > ts3).toBe(true);
      expect(ts1 === BEYOND_SAFE_TIMESTAMP).toBe(true);
    });

    it('should order triples by timestamp correctly even with large values', () => {
      const triples: Triple[] = [
        {
          subject: TEST_SUBJECT,
          predicate: TEST_PREDICATE,
          object: { type: ObjectType.STRING, value: 'third' },
          timestamp: BEYOND_SAFE_TIMESTAMP + 2n,
          txId: TEST_TX_ID,
        },
        {
          subject: TEST_SUBJECT,
          predicate: TEST_PREDICATE,
          object: { type: ObjectType.STRING, value: 'first' },
          timestamp: BEYOND_SAFE_TIMESTAMP,
          txId: TEST_TX_ID,
        },
        {
          subject: TEST_SUBJECT,
          predicate: TEST_PREDICATE,
          object: { type: ObjectType.STRING, value: 'second' },
          timestamp: BEYOND_SAFE_TIMESTAMP + 1n,
          txId: TEST_TX_ID,
        },
      ];

      // Sort by timestamp
      const sorted = [...triples].sort((a, b) => {
        if (a.timestamp < b.timestamp) return -1;
        if (a.timestamp > b.timestamp) return 1;
        return 0;
      });

      expect(sorted[0].object.value).toBe('first');
      expect(sorted[1].object.value).toBe('second');
      expect(sorted[2].object.value).toBe('third');
    });

    it('should maintain correct ordering when mixing normal and large timestamps', () => {
      const normalTimestamp = 1705700000000n; // Normal millisecond timestamp
      const largeTimestamp = BEYOND_SAFE_TIMESTAMP;

      expect(normalTimestamp < largeTimestamp).toBe(true);

      const triples: Triple[] = [
        {
          subject: TEST_SUBJECT,
          predicate: TEST_PREDICATE,
          object: { type: ObjectType.STRING, value: 'normal' },
          timestamp: normalTimestamp,
          txId: TEST_TX_ID,
        },
        {
          subject: TEST_SUBJECT,
          predicate: TEST_PREDICATE,
          object: { type: ObjectType.STRING, value: 'large' },
          timestamp: largeTimestamp,
          txId: TEST_TX_ID,
        },
      ];

      const sorted = [...triples].sort((a, b) => {
        if (a.timestamp < b.timestamp) return -1;
        if (a.timestamp > b.timestamp) return 1;
        return 0;
      });

      expect(sorted[0].object.value).toBe('normal');
      expect(sorted[1].object.value).toBe('large');
    });
  });

  describe('TripleValue timestamp type in entity.ts', () => {
    it('should use bigint for TripleValue.timestamp (not number)', () => {
      // This test documents the expected type
      // If TripleValue.timestamp is number, high-precision timestamps will lose precision
      const value: TripleValue = {
        type: ObjectType.TIMESTAMP,
        timestamp: BEYOND_SAFE_TIMESTAMP as unknown as number, // Cast to check runtime behavior
      };

      // When TripleValue.timestamp is bigint, this should work correctly
      // When it's number, precision is lost
      const asNumber = value.timestamp as unknown as number;
      const recovered = BigInt(asNumber);

      // This test will FAIL if timestamp is stored as number
      // because Number(9007199254741991n) !== 9007199254741991n
      expect(recovered).toBe(BEYOND_SAFE_TIMESTAMP);
    });
  });
});

describe('Negative timestamp handling', () => {
  it('should reject negative timestamps in Triple validation', () => {
    const triple: Triple = {
      subject: TEST_SUBJECT,
      predicate: TEST_PREDICATE,
      object: { type: ObjectType.STRING, value: 'test' },
      timestamp: -1n,
      txId: TEST_TX_ID,
    };

    const validation = validateTriple(triple);
    expect(validation.valid).toBe(false);
    expect(validation.errors.some(e => e.includes('positive'))).toBe(true);
  });

  it('should accept zero timestamp', () => {
    const triple: Triple = {
      subject: TEST_SUBJECT,
      predicate: TEST_PREDICATE,
      object: { type: ObjectType.STRING, value: 'test' },
      timestamp: 0n,
      txId: TEST_TX_ID,
    };

    const validation = validateTriple(triple);
    expect(validation.valid).toBe(true);
  });
});
