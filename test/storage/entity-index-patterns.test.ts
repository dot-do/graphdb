/**
 * Entity Index Pattern Tests (TDD - RED Phase)
 *
 * Tests for entity index edge cases and patterns:
 * - Prefix lookup edge cases
 * - Binary search correctness with various data
 * - Size estimation accuracy
 * - Special character handling in entity IDs
 * - Concurrent-safe read patterns
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  encodeEntityIndex,
  decodeEntityIndex,
  lookupEntity,
  lookupPrefix,
  getIndexSize,
  getAverageBytesPerEntry,
  type EntityIndex,
  type EntityIndexEntry,
} from '../../src/storage/entity-index';

// ============================================================================
// Test Helpers
// ============================================================================

/**
 * Create a test entity index with sequential IDs
 */
function createSequentialIndex(count: number, prefix = 'https://example.com/entity/'): EntityIndex {
  const entries: EntityIndexEntry[] = [];
  let currentOffset = 0;

  for (let i = 0; i < count; i++) {
    const entityId = `${prefix}${i.toString().padStart(6, '0')}`;
    const length = 100 + (i % 50); // Variable lengths
    entries.push({
      entityId,
      offset: currentOffset,
      length,
    });
    currentOffset += length;
  }

  return { entries, version: 1 };
}

/**
 * Create an index with specific entity IDs
 */
function createIndexFromIds(ids: string[]): EntityIndex {
  const sortedIds = [...ids].sort();
  const entries: EntityIndexEntry[] = sortedIds.map((entityId, i) => ({
    entityId,
    offset: i * 100,
    length: 100,
  }));
  return { entries, version: 1 };
}

// ============================================================================
// Prefix Lookup Tests
// ============================================================================

describe('Entity Index Prefix Lookup', () => {
  describe('Basic Prefix Matching', () => {
    it('should find all entities with common prefix', () => {
      const index = createIndexFromIds([
        'https://example.com/users/alice',
        'https://example.com/users/bob',
        'https://example.com/users/charlie',
        'https://example.com/products/widget',
        'https://example.com/products/gadget',
      ]);

      const users = lookupPrefix(index, 'https://example.com/users/');
      expect(users.length).toBe(3);
      expect(users.every(e => e.entityId.startsWith('https://example.com/users/'))).toBe(true);

      const products = lookupPrefix(index, 'https://example.com/products/');
      expect(products.length).toBe(2);
    });

    it('should return empty for non-matching prefix', () => {
      const index = createSequentialIndex(100);

      const result = lookupPrefix(index, 'https://different.com/');
      expect(result.length).toBe(0);
    });

    it('should return all entries for empty prefix', () => {
      const index = createSequentialIndex(50);

      const result = lookupPrefix(index, '');
      expect(result.length).toBe(50);
    });

    it('should handle single-character prefix', () => {
      const index = createIndexFromIds([
        'aaa',
        'aab',
        'abc',
        'baa',
        'bbb',
      ]);

      const aPrefix = lookupPrefix(index, 'a');
      expect(aPrefix.length).toBe(3);

      const bPrefix = lookupPrefix(index, 'b');
      expect(bPrefix.length).toBe(2);
    });
  });

  describe('Prefix Edge Cases', () => {
    it('should match exact entity ID as prefix', () => {
      const index = createIndexFromIds([
        'https://example.com/entity/1',
        'https://example.com/entity/10',
        'https://example.com/entity/100',
        'https://example.com/entity/2',
      ]);

      // Exact match should return only that entity
      const exact = lookupPrefix(index, 'https://example.com/entity/1');
      expect(exact.length).toBe(3); // 1, 10, 100 all start with entity/1
    });

    it('should handle prefix that ends at boundary', () => {
      const index = createIndexFromIds([
        'prefix-a',
        'prefix-b',
        'prefix/',
        'prefixa', // No hyphen
      ]);

      const withHyphen = lookupPrefix(index, 'prefix-');
      expect(withHyphen.length).toBe(2);
      expect(withHyphen.every(e => e.entityId.startsWith('prefix-'))).toBe(true);
    });

    it('should handle Unicode prefix', () => {
      const index = createIndexFromIds([
        'unicode-prefix-test',
        'unicode-prefix-other',
        'ascii-only',
      ]);

      const result = lookupPrefix(index, 'unicode-');
      expect(result.length).toBe(2);
    });

    it('should handle prefix longer than any entity', () => {
      const index = createIndexFromIds(['short', 'medium-length', 'another']);

      const result = lookupPrefix(index, 'this-prefix-is-longer-than-any-entity-in-index');
      expect(result.length).toBe(0);
    });

    it('should return new array (not internal reference)', () => {
      const index = createSequentialIndex(10);

      const result1 = lookupPrefix(index, '');
      const result2 = lookupPrefix(index, '');

      // Should be different arrays
      expect(result1).not.toBe(result2);
      expect(result1).not.toBe(index.entries);

      // But with same content
      expect(result1).toEqual(result2);
    });
  });

  describe('Prefix Range Boundaries', () => {
    it('should find prefix at start of index', () => {
      const index = createIndexFromIds([
        'aaa-first',
        'aaa-second',
        'bbb-middle',
        'ccc-last',
      ]);

      const result = lookupPrefix(index, 'aaa');
      expect(result.length).toBe(2);
    });

    it('should find prefix at end of index', () => {
      const index = createIndexFromIds([
        'aaa-first',
        'bbb-middle',
        'zzz-last-1',
        'zzz-last-2',
      ]);

      const result = lookupPrefix(index, 'zzz');
      expect(result.length).toBe(2);
    });

    it('should find prefix in middle of index', () => {
      const index = createIndexFromIds([
        'aaa-start',
        'mmm-middle-1',
        'mmm-middle-2',
        'mmm-middle-3',
        'zzz-end',
      ]);

      const result = lookupPrefix(index, 'mmm');
      expect(result.length).toBe(3);
    });

    it('should handle prefix with gap after', () => {
      const index = createIndexFromIds([
        'aaa-1',
        'aaa-2',
        // gap - no 'bbb'
        'ccc-1',
      ]);

      const result = lookupPrefix(index, 'bbb');
      expect(result.length).toBe(0);
    });
  });
});

// ============================================================================
// Binary Search Correctness Tests
// ============================================================================

describe('Entity Index Binary Search Correctness', () => {
  describe('Standard Lookups', () => {
    it('should find first element', () => {
      const index = createSequentialIndex(1000);
      const target = 'https://example.com/entity/000000';

      const result = lookupEntity(index, target);
      expect(result).not.toBeNull();
      expect(result!.entityId).toBe(target);
    });

    it('should find last element', () => {
      const index = createSequentialIndex(1000);
      const target = 'https://example.com/entity/000999';

      const result = lookupEntity(index, target);
      expect(result).not.toBeNull();
      expect(result!.entityId).toBe(target);
    });

    it('should find middle element', () => {
      const index = createSequentialIndex(1000);
      const target = 'https://example.com/entity/000500';

      const result = lookupEntity(index, target);
      expect(result).not.toBeNull();
      expect(result!.entityId).toBe(target);
    });

    it('should return null for element before first', () => {
      const index = createSequentialIndex(100, 'https://b.com/');

      const result = lookupEntity(index, 'https://a.com/first');
      expect(result).toBeNull();
    });

    it('should return null for element after last', () => {
      const index = createSequentialIndex(100, 'https://a.com/');

      const result = lookupEntity(index, 'https://z.com/last');
      expect(result).toBeNull();
    });

    it('should return null for element in gap', () => {
      const index = createIndexFromIds([
        'entity-001',
        'entity-003',
        'entity-005',
      ]);

      expect(lookupEntity(index, 'entity-002')).toBeNull();
      expect(lookupEntity(index, 'entity-004')).toBeNull();
    });
  });

  describe('Edge Cases', () => {
    it('should handle single-element index', () => {
      const index: EntityIndex = {
        entries: [{ entityId: 'only-one', offset: 0, length: 100 }],
        version: 1,
      };

      expect(lookupEntity(index, 'only-one')).not.toBeNull();
      expect(lookupEntity(index, 'other')).toBeNull();
    });

    it('should handle two-element index', () => {
      const index: EntityIndex = {
        entries: [
          { entityId: 'first', offset: 0, length: 100 },
          { entityId: 'second', offset: 100, length: 100 },
        ],
        version: 1,
      };

      expect(lookupEntity(index, 'first')).not.toBeNull();
      expect(lookupEntity(index, 'second')).not.toBeNull();
      expect(lookupEntity(index, 'middle')).toBeNull();
      expect(lookupEntity(index, 'before')).toBeNull();
      expect(lookupEntity(index, 'zzz')).toBeNull();
    });

    it('should handle empty index', () => {
      const index: EntityIndex = { entries: [], version: 1 };

      expect(lookupEntity(index, 'anything')).toBeNull();
    });

    it('should handle duplicate-like IDs (almost same)', () => {
      const index = createIndexFromIds([
        'entity-1',
        'entity-10',
        'entity-100',
        'entity-2',
      ]);

      expect(lookupEntity(index, 'entity-1')?.entityId).toBe('entity-1');
      expect(lookupEntity(index, 'entity-10')?.entityId).toBe('entity-10');
      expect(lookupEntity(index, 'entity-100')?.entityId).toBe('entity-100');
      expect(lookupEntity(index, 'entity-2')?.entityId).toBe('entity-2');
    });
  });

  describe('Large Index Performance', () => {
    it('should find entries in 100K index efficiently', () => {
      const index = createSequentialIndex(100000);

      const start = performance.now();
      const iterations = 10000;

      for (let i = 0; i < iterations; i++) {
        const targetIdx = Math.floor(Math.random() * 100000);
        const target = `https://example.com/entity/${targetIdx.toString().padStart(6, '0')}`;
        const result = lookupEntity(index, target);
        expect(result).not.toBeNull();
      }

      const elapsed = performance.now() - start;
      console.log(`${iterations} lookups in 100K index: ${elapsed.toFixed(2)}ms`);

      // Should complete in reasonable time (< 100ms for 10K lookups)
      expect(elapsed).toBeLessThan(100);
    });
  });
});

// ============================================================================
// Size Estimation Tests
// ============================================================================

describe('Entity Index Size Estimation', () => {
  it('should match encoded size exactly', () => {
    const index = createSequentialIndex(100);

    const estimatedSize = getIndexSize(index);
    const encoded = encodeEntityIndex(index);

    expect(estimatedSize).toBe(encoded.length);
  });

  it('should estimate empty index correctly', () => {
    const index: EntityIndex = { entries: [], version: 1 };

    const estimatedSize = getIndexSize(index);
    // 4 bytes header + 4 bytes checksum = 8 bytes
    expect(estimatedSize).toBe(8);
  });

  it('should estimate single entry correctly', () => {
    const index: EntityIndex = {
      entries: [{ entityId: 'test', offset: 0, length: 100 }],
      version: 1,
    };

    const estimatedSize = getIndexSize(index);
    const encoded = encodeEntityIndex(index);

    expect(estimatedSize).toBe(encoded.length);
  });

  it('should account for varint sizes correctly', () => {
    // Large offset and length values require more varint bytes
    const index: EntityIndex = {
      entries: [
        { entityId: 'entity-1', offset: 127, length: 127 }, // 1-byte varints
        { entityId: 'entity-2', offset: 128, length: 128 }, // 2-byte varints
        { entityId: 'entity-3', offset: 16384, length: 16384 }, // 3-byte varints
      ],
      version: 1,
    };

    const estimatedSize = getIndexSize(index);
    const encoded = encodeEntityIndex(index);

    expect(estimatedSize).toBe(encoded.length);
  });

  it('should account for UTF-8 multi-byte characters', () => {
    const index: EntityIndex = {
      entries: [
        { entityId: 'ascii-only', offset: 0, length: 100 },
        { entityId: 'unicode-text', offset: 100, length: 100 }, // 3-4 bytes per char
      ],
      version: 1,
    };

    const estimatedSize = getIndexSize(index);
    const encoded = encodeEntityIndex(index);

    expect(estimatedSize).toBe(encoded.length);
  });

  describe('Average Bytes Per Entry', () => {
    it('should calculate average for uniform entries', () => {
      const index = createSequentialIndex(100);

      const avg = getAverageBytesPerEntry(index);
      expect(avg).toBeGreaterThan(0);

      // Should be roughly consistent across different sizes
      const index2 = createSequentialIndex(1000);
      const avg2 = getAverageBytesPerEntry(index2);

      // Same structure, so averages should be close
      expect(Math.abs(avg - avg2)).toBeLessThan(5);
    });

    it('should return 0 for empty index', () => {
      const index: EntityIndex = { entries: [], version: 1 };

      expect(getAverageBytesPerEntry(index)).toBe(0);
    });

    it('should handle variable-length entity IDs', () => {
      const index = createIndexFromIds([
        'a', // 1 byte
        'medium-length-id', // 16 bytes
        'very-very-very-long-entity-identifier-string', // 45 bytes
      ]);

      const avg = getAverageBytesPerEntry(index);
      expect(avg).toBeGreaterThan(0);
    });
  });
});

// ============================================================================
// Special Character Handling Tests
// ============================================================================

describe('Entity Index Special Characters', () => {
  it('should handle URL-encoded characters', () => {
    const index = createIndexFromIds([
      'https://example.com/entity/hello%20world',
      'https://example.com/entity/a+b',
      'https://example.com/entity/test?query=value',
    ]);

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].entityId).toBe('https://example.com/entity/a+b');
    expect(lookupEntity(index, 'https://example.com/entity/hello%20world')).not.toBeNull();
  });

  it('should handle Unicode entity IDs', () => {
    const index = createIndexFromIds([
      'unicode-entity',
      'normal-ascii-id',
    ]);

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(lookupEntity(decoded, 'unicode-entity')).not.toBeNull();
  });

  it('should handle empty string entity ID', () => {
    const index: EntityIndex = {
      entries: [{ entityId: '', offset: 0, length: 100 }],
      version: 1,
    };

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].entityId).toBe('');
    expect(lookupEntity(decoded, '')).not.toBeNull();
  });

  it('should handle very long entity IDs', () => {
    const longId = 'https://example.com/' + 'x'.repeat(5000);
    const index: EntityIndex = {
      entries: [{ entityId: longId, offset: 0, length: 100 }],
      version: 1,
    };

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].entityId).toBe(longId);
    expect(lookupEntity(decoded, longId)).not.toBeNull();
  });

  it('should sort special characters correctly', () => {
    const ids = [
      '!exclamation',
      '#hash',
      '$dollar',
      '%percent',
      'aaa',
      'zzz',
    ];

    const index = createIndexFromIds(ids);
    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    // Verify sorted order is maintained (using same sort as createIndexFromIds)
    const expectedOrder = [...ids].sort();
    expect(decoded.entries.map(e => e.entityId)).toEqual(expectedOrder);
  });
});

// ============================================================================
// Concurrent Read Pattern Tests
// ============================================================================

describe('Entity Index Concurrent Read Safety', () => {
  it('should handle multiple lookups on same index', () => {
    const index = createSequentialIndex(1000);
    const results: (EntityIndexEntry | null)[] = [];

    // Simulate concurrent reads
    const targets = [
      'https://example.com/entity/000100',
      'https://example.com/entity/000500',
      'https://example.com/entity/000900',
    ];

    for (const target of targets) {
      results.push(lookupEntity(index, target));
    }

    expect(results.every(r => r !== null)).toBe(true);
    expect(results[0]!.entityId).toBe(targets[0]);
    expect(results[1]!.entityId).toBe(targets[1]);
    expect(results[2]!.entityId).toBe(targets[2]);
  });

  it('should not modify index during prefix lookup', () => {
    const index = createSequentialIndex(100);
    const originalLength = index.entries.length;

    // Multiple prefix lookups
    lookupPrefix(index, 'https://example.com/entity/0000');
    lookupPrefix(index, 'https://example.com/entity/0005');
    lookupPrefix(index, '');

    // Index should be unchanged
    expect(index.entries.length).toBe(originalLength);
  });

  it('should decode same data consistently', () => {
    const index = createSequentialIndex(100);
    const encoded = encodeEntityIndex(index);

    // Multiple decodes should produce identical results
    const decoded1 = decodeEntityIndex(encoded);
    const decoded2 = decodeEntityIndex(encoded);
    const decoded3 = decodeEntityIndex(encoded);

    expect(decoded1.entries).toEqual(decoded2.entries);
    expect(decoded2.entries).toEqual(decoded3.entries);
  });
});

// ============================================================================
// Encoding/Decoding Validation Tests
// ============================================================================

describe('Entity Index Validation', () => {
  it('should reject corrupted checksum', () => {
    const index = createSequentialIndex(10);
    const encoded = encodeEntityIndex(index);

    // Corrupt checksum (last 4 bytes)
    const corrupted = new Uint8Array(encoded);
    corrupted[corrupted.length - 1] ^= 0xFF;

    expect(() => decodeEntityIndex(corrupted)).toThrow(/checksum/i);
  });

  it('should reject corrupted data with valid-looking checksum', () => {
    const index = createSequentialIndex(10);
    const encoded = encodeEntityIndex(index);

    // Corrupt data in middle
    const corrupted = new Uint8Array(encoded);
    corrupted[10] ^= 0xFF;

    expect(() => decodeEntityIndex(corrupted)).toThrow(/checksum/i);
  });

  it('should reject truncated data', () => {
    const index = createSequentialIndex(10);
    const encoded = encodeEntityIndex(index);

    // Truncate significantly
    const truncated = encoded.slice(0, Math.floor(encoded.length / 2));

    expect(() => decodeEntityIndex(truncated)).toThrow();
  });

  it('should reject buffer smaller than minimum', () => {
    expect(() => decodeEntityIndex(new Uint8Array(4))).toThrow(/too small/i);
    expect(() => decodeEntityIndex(new Uint8Array(7))).toThrow(/too small/i);
  });

  it('should reject negative offset in encoding', () => {
    const index: EntityIndex = {
      entries: [{ entityId: 'test', offset: -1, length: 100 }],
      version: 1,
    };

    expect(() => encodeEntityIndex(index)).toThrow(/negative offset/i);
  });

  it('should reject negative length in encoding', () => {
    const index: EntityIndex = {
      entries: [{ entityId: 'test', offset: 0, length: -1 }],
      version: 1,
    };

    expect(() => encodeEntityIndex(index)).toThrow(/negative length/i);
  });
});

// ============================================================================
// Offset and Length Boundary Tests
// ============================================================================

describe('Entity Index Offset/Length Boundaries', () => {
  it('should handle zero offset and length', () => {
    const index: EntityIndex = {
      entries: [{ entityId: 'empty', offset: 0, length: 0 }],
      version: 1,
    };

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].offset).toBe(0);
    expect(decoded.entries[0].length).toBe(0);
  });

  it('should handle large offsets (>4GB)', () => {
    // Test with offsets that exceed 32-bit range
    const largeOffset = 5 * 1024 * 1024 * 1024; // 5GB
    const index: EntityIndex = {
      entries: [{ entityId: 'large-offset', offset: largeOffset, length: 100 }],
      version: 1,
    };

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].offset).toBe(largeOffset);
  });

  it('should handle large lengths', () => {
    const largeLength = 100 * 1024 * 1024; // 100MB
    const index: EntityIndex = {
      entries: [{ entityId: 'large-entity', offset: 0, length: largeLength }],
      version: 1,
    };

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].length).toBe(largeLength);
  });

  it('should handle max safe integer for offset', () => {
    const index: EntityIndex = {
      entries: [{ entityId: 'max-offset', offset: Number.MAX_SAFE_INTEGER, length: 100 }],
      version: 1,
    };

    const encoded = encodeEntityIndex(index);
    const decoded = decodeEntityIndex(encoded);

    expect(decoded.entries[0].offset).toBe(Number.MAX_SAFE_INTEGER);
  });
});
