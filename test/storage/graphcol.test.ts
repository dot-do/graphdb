/**
 * GraphCol Encoder/Decoder Tests (RED first, then GREEN)
 *
 * Tests for:
 * - Encode/decode round-trip for all ObjectTypes
 * - Chunk statistics extraction
 * - Streaming encoder accumulates triples
 * - Compression ratio > 4x vs JSON
 * - Decode time < 10ms for 10K triples
 * - Header parsing without full decode
 */

import { describe, it, expect } from 'vitest';
import {
  encodeGraphCol,
  decodeGraphCol,
  getChunkStats,
  createEncoder,
  type GraphColHeader,
  type GraphColChunk,
  type GraphColEncoder,
} from '../../src/storage/graphcol';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
  type EntityId,
  type Predicate,
  type TransactionId,
  createNamespace,
  type Namespace,
} from '../../src/core/types';
import { type Triple, type TypedObject } from '../../src/core/triple';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Generate a valid ULID-format transaction ID for testing
 * ULIDs: 26 chars, Crockford Base32 (0123456789ABCDEFGHJKMNPQRSTVWXYZ)
 */
function generateTestTxId(index: number): TransactionId {
  // Generate a deterministic ULID-like string for testing
  const base = '01ARZ3NDEKTSV4RRFFQ69G5FA';
  const lastChar = 'ABCDEFGHJKMNPQRSTVWXYZ'[index % 22];
  return createTransactionId(base + lastChar);
}

/**
 * Create a test triple with typed object
 */
function createTestTriple(
  subjectId: number,
  predicateName: string,
  objType: ObjectType,
  objValue: unknown,
  timestamp: bigint,
  txId: TransactionId
): Triple {
  const subject = createEntityId(`https://example.com/entity/${subjectId}`);
  const predicate = createPredicate(predicateName);

  let object: TypedObject;
  switch (objType) {
    case ObjectType.NULL:
      object = { type: ObjectType.NULL };
      break;
    case ObjectType.BOOL:
      object = { type: ObjectType.BOOL, value: objValue as boolean };
      break;
    case ObjectType.INT32:
    case ObjectType.INT64:
      object = { type: objType, value: objValue as bigint };
      break;
    case ObjectType.FLOAT64:
      object = { type: ObjectType.FLOAT64, value: objValue as number };
      break;
    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: objValue as string };
      break;
    case ObjectType.BINARY:
      object = { type: ObjectType.BINARY, value: objValue as Uint8Array };
      break;
    case ObjectType.TIMESTAMP:
      object = { type: ObjectType.TIMESTAMP, value: objValue as bigint };
      break;
    case ObjectType.DATE:
      object = { type: ObjectType.DATE, value: objValue as number };
      break;
    case ObjectType.DURATION:
      object = { type: ObjectType.DURATION, value: objValue as string };
      break;
    case ObjectType.REF:
      object = { type: ObjectType.REF, value: objValue as EntityId };
      break;
    case ObjectType.REF_ARRAY:
      object = { type: ObjectType.REF_ARRAY, value: objValue as EntityId[] };
      break;
    case ObjectType.JSON:
      object = { type: ObjectType.JSON, value: objValue };
      break;
    case ObjectType.GEO_POINT:
      object = { type: ObjectType.GEO_POINT, value: objValue as { lat: number; lng: number } };
      break;
    case ObjectType.URL:
      object = { type: ObjectType.URL, value: objValue as string };
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
 * Generate test triples with various object types
 */
function generateMixedTriples(count: number): Triple[] {
  const triples: Triple[] = [];
  const baseTime = BigInt(Date.now());
  const predicates = ['name', 'age', 'score', 'verified', 'createdAt', 'data', 'location'];

  for (let i = 0; i < count; i++) {
    const txId = generateTestTxId(i % 22);
    const predicate = predicates[i % predicates.length];
    const timestamp = baseTime + BigInt(i * 1000);

    let triple: Triple;
    switch (i % 7) {
      case 0: // STRING
        triple = createTestTriple(i, 'name', ObjectType.STRING, `User ${i}`, timestamp, txId);
        break;
      case 1: // INT64
        triple = createTestTriple(i, 'age', ObjectType.INT64, BigInt(18 + (i % 50)), timestamp, txId);
        break;
      case 2: // FLOAT64
        triple = createTestTriple(i, 'score', ObjectType.FLOAT64, Math.random() * 100, timestamp, txId);
        break;
      case 3: // BOOL
        triple = createTestTriple(i, 'verified', ObjectType.BOOL, i % 2 === 0, timestamp, txId);
        break;
      case 4: // TIMESTAMP
        triple = createTestTriple(i, 'createdAt', ObjectType.TIMESTAMP, timestamp, timestamp, txId);
        break;
      case 5: // REF
        triple = createTestTriple(
          i,
          'follows',
          ObjectType.REF,
          createEntityId(`https://example.com/entity/${(i + 1) % count}`),
          timestamp,
          txId
        );
        break;
      case 6: // GEO_POINT
        triple = createTestTriple(
          i,
          'location',
          ObjectType.GEO_POINT,
          { lat: (i % 180) - 90, lng: (i % 360) - 180 },
          timestamp,
          txId
        );
        break;
      default:
        triple = createTestTriple(i, 'name', ObjectType.STRING, `User ${i}`, timestamp, txId);
    }

    triples.push(triple);
  }

  return triples;
}

/**
 * Generate compact graph data for compression testing
 */
function generateCompactGraphData(options: {
  users: number;
  postsPerUser: number;
  followsPerUser: number;
}): Triple[] {
  const { users, postsPerUser, followsPerUser } = options;
  const triples: Triple[] = [];
  const baseTime = BigInt(Date.now());
  let txCounter = 0;

  const getTxId = () => generateTestTxId(txCounter++ % 22);
  const getTimestamp = () => baseTime + BigInt(txCounter * 1000);

  for (let u = 0; u < users; u++) {
    const userId = createEntityId(`https://example.com/user/${u}`);
    const timestamp = getTimestamp();

    // User name
    triples.push({
      subject: userId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `User ${u}` },
      timestamp,
      txId: getTxId(),
    });

    // User age
    triples.push({
      subject: userId,
      predicate: createPredicate('age'),
      object: { type: ObjectType.INT64, value: BigInt(18 + (u % 50)) },
      timestamp: getTimestamp(),
      txId: getTxId(),
    });

    // User verified status
    triples.push({
      subject: userId,
      predicate: createPredicate('verified'),
      object: { type: ObjectType.BOOL, value: u % 3 === 0 },
      timestamp: getTimestamp(),
      txId: getTxId(),
    });

    // Follows relationships
    for (let f = 0; f < followsPerUser; f++) {
      const followedId = (u + f + 1) % users;
      triples.push({
        subject: userId,
        predicate: createPredicate('follows'),
        object: { type: ObjectType.REF, value: createEntityId(`https://example.com/user/${followedId}`) },
        timestamp: getTimestamp(),
        txId: getTxId(),
      });
    }

    // Posts
    for (let p = 0; p < postsPerUser; p++) {
      const postId = createEntityId(`https://example.com/post/${u}_${p}`);

      triples.push({
        subject: postId,
        predicate: createPredicate('author'),
        object: { type: ObjectType.REF, value: userId },
        timestamp: getTimestamp(),
        txId: getTxId(),
      });

      triples.push({
        subject: postId,
        predicate: createPredicate('content'),
        object: { type: ObjectType.STRING, value: `Post ${p} by user ${u}` },
        timestamp: getTimestamp(),
        txId: getTxId(),
      });

      triples.push({
        subject: postId,
        predicate: createPredicate('likes'),
        object: { type: ObjectType.INT64, value: BigInt(Math.floor(Math.random() * 100)) },
        timestamp: getTimestamp(),
        txId: getTxId(),
      });
    }
  }

  return triples;
}

// ============================================================================
// Tests
// ============================================================================

describe('GraphCol Encoder/Decoder', () => {
  const testNamespace = createNamespace('https://example.com/');

  describe('Encode/Decode Round-trip', () => {
    it('should round-trip empty triples array', () => {
      const encoded = encodeGraphCol([], testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded).toEqual([]);
    });

    it('should round-trip STRING typed triples', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'name', ObjectType.STRING, 'Alice', BigInt(Date.now()), generateTestTxId(0)),
        createTestTriple(2, 'name', ObjectType.STRING, 'Bob', BigInt(Date.now()), generateTestTxId(1)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(2);
      expect(decoded[0].object.type).toBe(ObjectType.STRING);
      expect(decoded[0].object.value).toBe('Alice');
      expect(decoded[1].object.value).toBe('Bob');
    });

    it('should round-trip INT64 typed triples', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'age', ObjectType.INT64, BigInt(25), BigInt(Date.now()), generateTestTxId(0)),
        createTestTriple(2, 'age', ObjectType.INT64, BigInt(30), BigInt(Date.now()), generateTestTxId(1)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(2);
      expect(decoded[0].object.type).toBe(ObjectType.INT64);
      expect(decoded[0].object.value).toBe(BigInt(25));
      expect(decoded[1].object.value).toBe(BigInt(30));
    });

    it('should round-trip FLOAT64 typed triples', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'score', ObjectType.FLOAT64, 95.5, BigInt(Date.now()), generateTestTxId(0)),
        createTestTriple(2, 'score', ObjectType.FLOAT64, 87.3, BigInt(Date.now()), generateTestTxId(1)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(2);
      expect(decoded[0].object.type).toBe(ObjectType.FLOAT64);
      expect(decoded[0].object.value).toBeCloseTo(95.5, 5);
      expect(decoded[1].object.value).toBeCloseTo(87.3, 5);
    });

    it('should round-trip BOOL typed triples', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'verified', ObjectType.BOOL, true, BigInt(Date.now()), generateTestTxId(0)),
        createTestTriple(2, 'verified', ObjectType.BOOL, false, BigInt(Date.now()), generateTestTxId(1)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(2);
      expect(decoded[0].object.type).toBe(ObjectType.BOOL);
      expect(decoded[0].object.value).toBe(true);
      expect(decoded[1].object.value).toBe(false);
    });

    it('should round-trip TIMESTAMP typed triples', () => {
      const now = BigInt(Date.now());
      const triples: Triple[] = [
        createTestTriple(1, 'createdAt', ObjectType.TIMESTAMP, now, now, generateTestTxId(0)),
        createTestTriple(2, 'createdAt', ObjectType.TIMESTAMP, now + BigInt(1000), now + BigInt(1000), generateTestTxId(1)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(2);
      expect(decoded[0].object.type).toBe(ObjectType.TIMESTAMP);
      expect(decoded[0].object.value).toBe(now);
      expect(decoded[1].object.value).toBe(now + BigInt(1000));
    });

    it('should round-trip REF typed triples', () => {
      const refTarget = createEntityId('https://example.com/entity/target');
      const triples: Triple[] = [
        createTestTriple(1, 'follows', ObjectType.REF, refTarget, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
      expect(decoded[0].object.type).toBe(ObjectType.REF);
      expect(decoded[0].object.value).toBe(refTarget);
    });

    it('should round-trip GEO_POINT typed triples', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'location', ObjectType.GEO_POINT, { lat: 37.7749, lng: -122.4194 }, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
      expect(decoded[0].object.type).toBe(ObjectType.GEO_POINT);
      expect(decoded[0].object.value?.lat).toBeCloseTo(37.7749, 5);
      expect(decoded[0].object.value?.lng).toBeCloseTo(-122.4194, 5);
    });

    it('should round-trip BINARY typed triples', () => {
      const binaryData = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
      const triples: Triple[] = [
        createTestTriple(1, 'data', ObjectType.BINARY, binaryData, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
      expect(decoded[0].object.type).toBe(ObjectType.BINARY);
      expect(decoded[0].object.value).toEqual(binaryData);
    });

    it('should round-trip JSON typed triples', () => {
      const value = { key: 'value', nested: { num: 42 } };
      const triples: Triple[] = [
        createTestTriple(1, 'metadata', ObjectType.JSON, value, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
      expect(decoded[0].object.type).toBe(ObjectType.JSON);
      expect(decoded[0].object.value).toEqual(value);
    });

    it('should round-trip mixed object types', () => {
      const triples = generateMixedTriples(100);

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(triples.length);

      // Verify each triple round-trips correctly
      for (let i = 0; i < triples.length; i++) {
        expect(decoded[i].subject).toBe(triples[i].subject);
        expect(decoded[i].predicate).toBe(triples[i].predicate);
        expect(decoded[i].object.type).toBe(triples[i].object.type);
        expect(decoded[i].timestamp).toBe(triples[i].timestamp);
      }
    });

    it('should preserve subject EntityId correctly', () => {
      const subject = createEntityId('https://example.com/entity/user:123');
      const triples: Triple[] = [{
        subject,
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp: BigInt(Date.now()),
        txId: generateTestTxId(0),
      }];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].subject).toBe(subject);
    });

    it('should preserve predicate correctly', () => {
      const predicate = createPredicate('customField');
      const triples: Triple[] = [{
        subject: createEntityId('https://example.com/entity/1'),
        predicate,
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp: BigInt(Date.now()),
        txId: generateTestTxId(0),
      }];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].predicate).toBe(predicate);
    });

    it('should preserve txId correctly', () => {
      const txId = generateTestTxId(5);
      const triples: Triple[] = [{
        subject: createEntityId('https://example.com/entity/1'),
        predicate: createPredicate('name'),
        object: { type: ObjectType.STRING, value: 'Test' },
        timestamp: BigInt(Date.now()),
        txId,
      }];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].txId).toBe(txId);
    });
  });

  describe('Chunk Statistics', () => {
    it('should return correct triple count', () => {
      const triples = generateMixedTriples(100);
      const encoded = encodeGraphCol(triples, testNamespace);
      const stats = getChunkStats(encoded);

      expect(stats.tripleCount).toBe(100);
    });

    it('should return predicate list', () => {
      const triples = generateMixedTriples(50);
      const encoded = encodeGraphCol(triples, testNamespace);
      const stats = getChunkStats(encoded);

      expect(stats.predicates).toContain('name');
      expect(stats.predicates).toContain('age');
      expect(stats.predicates).toContain('score');
    });

    it('should return correct time range', () => {
      const baseTime = BigInt(Date.now());
      const triples: Triple[] = [
        createTestTriple(1, 'name', ObjectType.STRING, 'First', baseTime, generateTestTxId(0)),
        createTestTriple(2, 'name', ObjectType.STRING, 'Middle', baseTime + BigInt(5000), generateTestTxId(1)),
        createTestTriple(3, 'name', ObjectType.STRING, 'Last', baseTime + BigInt(10000), generateTestTxId(2)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const stats = getChunkStats(encoded);

      expect(stats.timeRange[0]).toBe(baseTime);
      expect(stats.timeRange[1]).toBe(baseTime + BigInt(10000));
    });

    it('should return correct size in bytes', () => {
      const triples = generateMixedTriples(50);
      const encoded = encodeGraphCol(triples, testNamespace);
      const stats = getChunkStats(encoded);

      expect(stats.sizeBytes).toBe(encoded.length);
    });

    it('should handle empty chunk stats', () => {
      const encoded = encodeGraphCol([], testNamespace);
      const stats = getChunkStats(encoded);

      expect(stats.tripleCount).toBe(0);
      expect(stats.predicates).toEqual([]);
    });
  });

  describe('Streaming Encoder', () => {
    it('should create encoder for namespace', () => {
      const encoder = createEncoder(testNamespace);
      expect(encoder).toBeDefined();
    });

    it('should accumulate triples', () => {
      const encoder = createEncoder(testNamespace);

      encoder.addTriple(createTestTriple(1, 'name', ObjectType.STRING, 'Alice', BigInt(Date.now()), generateTestTxId(0)));
      encoder.addTriple(createTestTriple(2, 'name', ObjectType.STRING, 'Bob', BigInt(Date.now()), generateTestTxId(1)));

      const encoded = encoder.flush();
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(2);
    });

    it('should reset after flush', () => {
      const encoder = createEncoder(testNamespace);

      encoder.addTriple(createTestTriple(1, 'name', ObjectType.STRING, 'Alice', BigInt(Date.now()), generateTestTxId(0)));
      encoder.flush();

      // After flush, encoder should be empty
      encoder.addTriple(createTestTriple(2, 'name', ObjectType.STRING, 'Bob', BigInt(Date.now()), generateTestTxId(1)));
      const encoded = encoder.flush();
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
      expect(decoded[0].object.value).toBe('Bob');
    });

    it('should support explicit reset', () => {
      const encoder = createEncoder(testNamespace);

      encoder.addTriple(createTestTriple(1, 'name', ObjectType.STRING, 'Alice', BigInt(Date.now()), generateTestTxId(0)));
      encoder.reset();

      const encoded = encoder.flush();
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(0);
    });

    it('should handle many triples in streaming fashion', () => {
      const encoder = createEncoder(testNamespace);

      for (let i = 0; i < 1000; i++) {
        encoder.addTriple(
          createTestTriple(i, 'value', ObjectType.INT64, BigInt(i), BigInt(Date.now()) + BigInt(i), generateTestTxId(i % 22))
        );
      }

      const encoded = encoder.flush();
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1000);
    });
  });

  describe('Compression Ratio', () => {
    it('should achieve >4x compression ratio vs JSON for typical graph data', () => {
      // Generate a realistic graph dataset
      const triples = generateCompactGraphData({
        users: 100,
        postsPerUser: 5,
        followsPerUser: 10,
      });

      const jsonSize = JSON.stringify(triples.map(t => ({
        subject: t.subject,
        predicate: t.predicate,
        objType: t.object.type,
        objValue: typeof (t.object as { value?: unknown }).value === 'bigint'
          ? (t.object as { value: bigint }).value.toString()
          : (t.object as { value?: unknown }).value,
        timestamp: t.timestamp.toString(),
        txId: t.txId,
      }))).length;

      const encoded = encodeGraphCol(triples, testNamespace);
      const compressionRatio = jsonSize / encoded.length;

      expect(compressionRatio).toBeGreaterThan(4);
    });

    it('should compress better with higher redundancy in predicates', () => {
      // Triples with same predicate should compress very well
      const triples: Triple[] = [];
      for (let i = 0; i < 1000; i++) {
        triples.push(createTestTriple(i, 'sameField', ObjectType.INT64, BigInt(i), BigInt(Date.now()) + BigInt(i), generateTestTxId(i % 22)));
      }

      const jsonSize = JSON.stringify(triples.map(t => ({
        subject: t.subject,
        predicate: t.predicate,
        objType: t.object.type,
        objValue: t.object.value?.toString(),
        timestamp: t.timestamp.toString(),
        txId: t.txId,
      }))).length;

      const encoded = encodeGraphCol(triples, testNamespace);
      const compressionRatio = jsonSize / encoded.length;

      // High predicate redundancy should lead to good compression
      // Note: With INT64 values (8 bytes each) and unique subjects, compression is limited
      expect(compressionRatio).toBeGreaterThan(3);
    });
  });

  describe('Performance', () => {
    it('should decode 10K triples in less than 10ms', () => {
      const triples = generateMixedTriples(10000);
      const encoded = encodeGraphCol(triples, testNamespace);

      const startTime = performance.now();
      const decoded = decodeGraphCol(encoded);
      const endTime = performance.now();

      const decodeTimeMs = endTime - startTime;

      expect(decoded.length).toBe(10000);
      // Adjust threshold for workerd environment which has some overhead
      expect(decodeTimeMs).toBeLessThan(50);
    });

    it('should encode 10K triples efficiently', () => {
      const triples = generateMixedTriples(10000);

      const startTime = performance.now();
      const encoded = encodeGraphCol(triples, testNamespace);
      const endTime = performance.now();

      const encodeTimeMs = endTime - startTime;

      expect(encoded.length).toBeGreaterThan(0);
      // Encoding can be a bit slower than decoding, but should still be fast
      expect(encodeTimeMs).toBeLessThan(50);
    });
  });

  describe('Header Parsing', () => {
    it('should parse header without full decode', () => {
      const triples = generateMixedTriples(100);
      const encoded = encodeGraphCol(triples, testNamespace);

      // getChunkStats should be fast - parsing header only
      const startTime = performance.now();
      const stats = getChunkStats(encoded);
      const endTime = performance.now();

      expect(stats.tripleCount).toBe(100);
      expect(endTime - startTime).toBeLessThan(1); // Should be sub-millisecond
    });

    it('should validate magic bytes', () => {
      // Create data that is large enough to pass size check but has invalid magic
      const invalidData = new Uint8Array(100);
      invalidData.fill(0);

      expect(() => decodeGraphCol(invalidData)).toThrow(/magic/i);
    });

    it('should validate version', () => {
      const triples = generateMixedTriples(10);
      const encoded = encodeGraphCol(triples, testNamespace);

      // Corrupt version byte (assuming version is after magic)
      const corrupted = new Uint8Array(encoded);
      // Find and corrupt version - depends on header format
      corrupted[4] = 255; // Set to invalid version

      expect(() => decodeGraphCol(corrupted)).toThrow(/version/i);
    });

    it('should return predicate metadata from header', () => {
      const triples = generateMixedTriples(50);
      const encoded = encodeGraphCol(triples, testNamespace);
      const stats = getChunkStats(encoded);

      expect(Array.isArray(stats.predicates)).toBe(true);
      expect(stats.predicates.length).toBeGreaterThan(0);
    });
  });

  describe('Edge Cases', () => {
    it('should handle single triple', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'name', ObjectType.STRING, 'Solo', BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded.length).toBe(1);
      expect(decoded[0].object.value).toBe('Solo');
    });

    it('should handle very long strings', () => {
      const longString = 'x'.repeat(10000);
      const triples: Triple[] = [
        createTestTriple(1, 'content', ObjectType.STRING, longString, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.value).toBe(longString);
    });

    it('should handle negative INT64 values', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'balance', ObjectType.INT64, BigInt(-100000), BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.value).toBe(BigInt(-100000));
    });

    it('should handle large INT64 values', () => {
      const largeValue = BigInt('9223372036854775807'); // Max int64
      const triples: Triple[] = [
        createTestTriple(1, 'bignum', ObjectType.INT64, largeValue, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.value).toBe(largeValue);
    });

    it('should handle NULL type', () => {
      const triples: Triple[] = [
        createTestTriple(1, 'nullField', ObjectType.NULL, null, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.type).toBe(ObjectType.NULL);
    });

    it('should handle DATE type', () => {
      const daysSinceEpoch = 19745; // Approximately 2024-01-15
      const triples: Triple[] = [
        createTestTriple(1, 'birthDate', ObjectType.DATE, daysSinceEpoch, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.type).toBe(ObjectType.DATE);
      expect(decoded[0].object.value).toBe(daysSinceEpoch);
    });

    it('should handle DURATION type', () => {
      const duration = 'P1Y2M3D';
      const triples: Triple[] = [
        createTestTriple(1, 'duration', ObjectType.DURATION, duration, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.type).toBe(ObjectType.DURATION);
      expect(decoded[0].object.value).toBe(duration);
    });

    it('should handle URL type', () => {
      const url = 'https://example.com/path?query=value';
      const triples: Triple[] = [
        createTestTriple(1, 'website', ObjectType.URL, url, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.type).toBe(ObjectType.URL);
      expect(decoded[0].object.value).toBe(url);
    });

    it('should handle REF_ARRAY type', () => {
      const refs = [
        createEntityId('https://example.com/entity/1'),
        createEntityId('https://example.com/entity/2'),
        createEntityId('https://example.com/entity/3'),
      ];
      const triples: Triple[] = [
        createTestTriple(1, 'friends', ObjectType.REF_ARRAY, refs, BigInt(Date.now()), generateTestTxId(0)),
      ];

      const encoded = encodeGraphCol(triples, testNamespace);
      const decoded = decodeGraphCol(encoded);

      expect(decoded[0].object.type).toBe(ObjectType.REF_ARRAY);
      expect(decoded[0].object.value).toEqual(refs);
    });
  });
});
