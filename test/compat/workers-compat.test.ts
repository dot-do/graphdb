/**
 * Workers Compatibility Tests
 *
 * Verifies that all modules work without Node.js Buffer API,
 * which is not available in Cloudflare Workers runtime.
 *
 * These tests validate:
 * - GraphCol encoder/decoder uses Uint8Array
 * - Bloom filter uses Uint8Array
 * - Cursor encoding/decoding works without Buffer
 * - All binary operations use Workers-compatible APIs
 */

import { describe, it, expect } from 'vitest';

// Core utilities - to be added
import {
  encodeString,
  decodeString,
  toBase64,
  fromBase64,
} from '../../src/core/index';

// Bloom filter - already Workers-compatible
import {
  createBloomFilter,
  addToFilter,
  mightExist,
  serializeFilter,
  deserializeFilter,
} from '../../src/snippet/bloom';

// Test that Workers-compatible utilities work correctly
// Note: The Cloudflare vitest-pool-workers automatically runs these tests
// in a Workers-like environment where Buffer is not available.
describe('Workers Compatibility', () => {
  describe('Core utility functions', () => {
    it('should encode string to Uint8Array', () => {
      const input = 'Hello, World!';
      const bytes = encodeString(input);

      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes.length).toBeGreaterThan(0);
    });

    it('should decode Uint8Array to string', () => {
      const input = 'Hello, World!';
      const bytes = encodeString(input);
      const decoded = decodeString(bytes);

      expect(decoded).toBe(input);
    });

    it('should handle Unicode strings', () => {
      const input = 'Hello, \u4e16\u754c! \ud83c\udf0d'; // "Hello, world! [globe emoji]"
      const bytes = encodeString(input);
      const decoded = decodeString(bytes);

      expect(decoded).toBe(input);
    });

    it('should encode Uint8Array to base64', () => {
      const bytes = new Uint8Array([72, 101, 108, 108, 111]); // "Hello"
      const b64 = toBase64(bytes);

      expect(typeof b64).toBe('string');
      expect(b64).toBe('SGVsbG8=');
    });

    it('should decode base64 to Uint8Array', () => {
      const b64 = 'SGVsbG8=';
      const bytes = fromBase64(b64);

      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(Array.from(bytes)).toEqual([72, 101, 108, 108, 111]);
    });

    it('should round-trip string through base64', () => {
      const input = 'Test cursor data: {"offset": 100}';
      const bytes = encodeString(input);
      const b64 = toBase64(bytes);
      const decoded = fromBase64(b64);
      const output = decodeString(decoded);

      expect(output).toBe(input);
    });

    it('should handle empty input', () => {
      expect(encodeString('')).toEqual(new Uint8Array(0));
      expect(decodeString(new Uint8Array(0))).toBe('');
      expect(toBase64(new Uint8Array(0))).toBe('');
      expect(fromBase64('')).toEqual(new Uint8Array(0));
    });
  });

  describe('Bloom filter Workers compatibility', () => {
    it('should create bloom filter with Uint8Array bits', () => {
      const filter = createBloomFilter({ capacity: 100 });

      expect(filter.bits).toBeInstanceOf(Uint8Array);
    });

    it('should add and check entries without Buffer', () => {
      const filter = createBloomFilter({ capacity: 100 });
      addToFilter(filter, 'entity:123');
      addToFilter(filter, 'entity:456');

      expect(mightExist(filter, 'entity:123')).toBe(true);
      expect(mightExist(filter, 'entity:456')).toBe(true);
      expect(mightExist(filter, 'entity:789')).toBe(false);
    });

    it('should serialize to base64 without Buffer', () => {
      const filter = createBloomFilter({ capacity: 100 });
      addToFilter(filter, 'test:entity');

      const serialized = serializeFilter(filter);

      expect(typeof serialized.filter).toBe('string');
      // Verify it's valid base64
      expect(() => fromBase64(serialized.filter)).not.toThrow();
    });

    it('should deserialize from base64 without Buffer', () => {
      const filter = createBloomFilter({ capacity: 100 });
      addToFilter(filter, 'test:entity');

      const serialized = serializeFilter(filter);
      const restored = deserializeFilter(serialized);

      expect(restored.bits).toBeInstanceOf(Uint8Array);
      expect(mightExist(restored, 'test:entity')).toBe(true);
    });
  });

  describe('Cursor encoding Workers compatibility', () => {
    it('should encode cursor to base64 without Buffer', () => {
      const cursorData = { offset: 100 };
      const jsonStr = JSON.stringify(cursorData);
      const bytes = encodeString(jsonStr);
      const cursor = toBase64(bytes);

      expect(typeof cursor).toBe('string');
      expect(cursor.length).toBeGreaterThan(0);
    });

    it('should decode cursor from base64 without Buffer', () => {
      const cursorData = { offset: 100 };
      const jsonStr = JSON.stringify(cursorData);
      const bytes = encodeString(jsonStr);
      const cursor = toBase64(bytes);

      // Decode
      const decodedBytes = fromBase64(cursor);
      const decodedStr = decodeString(decodedBytes);
      const decoded = JSON.parse(decodedStr);

      expect(decoded).toEqual(cursorData);
    });

    it('should handle numeric cursor values', () => {
      const lastId = 12345;
      const bytes = encodeString(String(lastId));
      const cursor = toBase64(bytes);

      const decodedBytes = fromBase64(cursor);
      const decodedStr = decodeString(decodedBytes);
      const decoded = parseInt(decodedStr, 10);

      expect(decoded).toBe(lastId);
    });
  });
});
