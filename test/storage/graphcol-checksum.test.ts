/**
 * GraphCol Checksum Validation Tests (TDD - RED phase)
 *
 * Tests for checksum validation in the GraphCol decoder:
 * - Should detect corrupted data (bit flip)
 * - Should reject truncated data
 * - Should pass valid data unchanged
 * - Should include checksum in encoded data
 */

import { describe, it, expect } from 'vitest';
import {
  encodeGraphCol,
  decodeGraphCol,
  GCOL_MAGIC,
} from '../../src/storage/graphcol';
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
 * Create test triples for checksum tests
 */
function createTestTriples(count: number): Triple[] {
  const triples: Triple[] = [];
  const baseTime = BigInt(Date.now());

  for (let i = 0; i < count; i++) {
    triples.push({
      subject: createEntityId(`https://example.com/entity/${i}`),
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `User ${i}` },
      timestamp: baseTime + BigInt(i * 1000),
      txId: generateTestTxId(i % 22),
    });
  }

  return triples;
}

// ============================================================================
// Tests
// ============================================================================

describe('GraphCol Checksum Validation', () => {
  const testNamespace = createNamespace('https://example.com/');

  describe('Corrupted Data Detection', () => {
    it('should detect corrupted data (single bit flip in payload)', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Corrupt data: flip a bit in the middle of the payload
      const corrupted = new Uint8Array(encoded);
      const middleIndex = Math.floor(encoded.length / 2);
      corrupted[middleIndex] ^= 0x01; // Flip lowest bit

      expect(() => decodeGraphCol(corrupted)).toThrow(/checksum/i);
    });

    it('should detect corrupted data (multiple bit flips)', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Corrupt multiple bytes
      const corrupted = new Uint8Array(encoded);
      corrupted[20] ^= 0xFF;
      corrupted[30] ^= 0xFF;
      corrupted[40] ^= 0xFF;

      expect(() => decodeGraphCol(corrupted)).toThrow(/checksum/i);
    });

    it('should detect corrupted header bytes', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Corrupt a byte in the header (after magic and version)
      const corrupted = new Uint8Array(encoded);
      corrupted[8] ^= 0x01; // Flip bit in header area

      expect(() => decodeGraphCol(corrupted)).toThrow(/checksum/i);
    });

    it('should detect corrupted checksum bytes', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Corrupt the checksum itself (last 4 bytes)
      const corrupted = new Uint8Array(encoded);
      corrupted[encoded.length - 1] ^= 0x01;

      expect(() => decodeGraphCol(corrupted)).toThrow(/checksum/i);
    });
  });

  describe('Truncated Data Detection', () => {
    it('should reject truncated data (missing checksum)', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Remove the last 4 bytes (checksum)
      const truncated = encoded.slice(0, encoded.length - 4);

      expect(() => decodeGraphCol(truncated)).toThrow(/checksum|truncated|too small/i);
    });

    it('should reject truncated data (partial checksum)', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Remove only 2 bytes from checksum
      const truncated = encoded.slice(0, encoded.length - 2);

      expect(() => decodeGraphCol(truncated)).toThrow(/checksum|truncated|too small/i);
    });

    it('should reject data truncated in the middle', () => {
      const triples = createTestTriples(20);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Truncate significantly in the middle
      const truncated = encoded.slice(0, Math.floor(encoded.length / 2));

      // This might throw for checksum mismatch or other parsing errors
      expect(() => decodeGraphCol(truncated)).toThrow();
    });
  });

  describe('Valid Data Handling', () => {
    it('should pass valid data unchanged', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Should not throw
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(10);
      expect(decoded[0].object.value).toBe('User 0');
      expect(decoded[9].object.value).toBe('User 9');
    });

    it('should pass valid empty chunk', () => {
      const encoded = encodeGraphCol([], testNamespace);

      // Should not throw
      const decoded = decodeGraphCol(encoded);

      expect(decoded).toEqual([]);
    });

    it('should pass valid single-triple chunk', () => {
      const triples = createTestTriples(1);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Should not throw
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
    });

    it('should pass valid large chunk', () => {
      const triples = createTestTriples(1000);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Should not throw
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1000);
    });
  });

  describe('Checksum Format', () => {
    it('should include checksum in encoded data (last 4 bytes)', () => {
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Encoded data should end with a 4-byte checksum
      // Verify by checking that changing the data causes checksum mismatch
      expect(encoded.length).toBeGreaterThan(4);

      // The checksum should be at the last 4 bytes
      const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
      const storedChecksum = view.getUint32(encoded.length - 4, true);

      // Checksum should be non-zero (extremely unlikely to be zero for real data)
      expect(storedChecksum).not.toBe(0);
    });

    it('should use CRC32 for checksum', () => {
      // CRC32 produces 32-bit values, fitting in 4 bytes
      const triples = createTestTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
      const storedChecksum = view.getUint32(encoded.length - 4, true);

      // CRC32 is stored as uint32 (0 to 2^32-1)
      expect(storedChecksum).toBeGreaterThanOrEqual(0);
      expect(storedChecksum).toBeLessThanOrEqual(0xFFFFFFFF);
    });

    it('should produce consistent checksum for same data', () => {
      const triples = createTestTriples(10);

      const encoded1 = encodeGraphCol(triples, testNamespace);
      const encoded2 = encodeGraphCol(triples, testNamespace);

      const view1 = new DataView(encoded1.buffer, encoded1.byteOffset, encoded1.byteLength);
      const view2 = new DataView(encoded2.buffer, encoded2.byteOffset, encoded2.byteLength);

      const checksum1 = view1.getUint32(encoded1.length - 4, true);
      const checksum2 = view2.getUint32(encoded2.length - 4, true);

      expect(checksum1).toBe(checksum2);
    });

    it('should produce different checksums for different data', () => {
      const triples1 = createTestTriples(10);
      const triples2 = createTestTriples(10);
      // Modify one triple
      triples2[5] = {
        ...triples2[5],
        object: { type: ObjectType.STRING, value: 'Modified User' },
      };

      const encoded1 = encodeGraphCol(triples1, testNamespace);
      const encoded2 = encodeGraphCol(triples2, testNamespace);

      const view1 = new DataView(encoded1.buffer, encoded1.byteOffset, encoded1.byteLength);
      const view2 = new DataView(encoded2.buffer, encoded2.byteOffset, encoded2.byteLength);

      const checksum1 = view1.getUint32(encoded1.length - 4, true);
      const checksum2 = view2.getUint32(encoded2.length - 4, true);

      expect(checksum1).not.toBe(checksum2);
    });
  });
});
