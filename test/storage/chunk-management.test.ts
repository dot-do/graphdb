/**
 * Chunk Management Tests (TDD - RED Phase)
 *
 * Tests for CDC chunk management edge cases:
 * - Path generation edge cases
 * - Sequence number ordering
 * - Chunk file listing patterns
 * - Time-based filtering
 * - Namespace path handling
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  parseNamespaceToPath,
  formatDatePath,
  generateSequence,
  getCDCPath,
  parseCDCPath,
} from '../../src/storage/r2-writer';
import { createNamespace, type Namespace } from '../../src/core/types';

// ============================================================================
// Path Generation Tests
// ============================================================================

describe('Chunk Path Generation', () => {
  describe('parseNamespaceToPath', () => {
    it('should reverse domain hierarchy correctly', () => {
      const namespace = createNamespace('https://example.com/crm/acme');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example/crm/acme');
    });

    it('should handle subdomains', () => {
      const namespace = createNamespace('https://api.staging.example.com/v1');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example/.staging/.api/v1');
    });

    it('should handle TLD-only domain', () => {
      const namespace = createNamespace('https://localhost/data');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.localhost/data');
    });

    it('should handle empty path', () => {
      const namespace = createNamespace('https://example.com/');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example');
    });

    it('should handle root path without trailing slash', () => {
      const namespace = createNamespace('https://example.com');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example');
    });

    it('should handle deeply nested paths', () => {
      const namespace = createNamespace('https://example.com/a/b/c/d/e/f');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.com/.example/a/b/c/d/e/f');
    });

    it('should handle numeric TLDs', () => {
      // IP-based namespace (unusual but valid)
      const namespace = createNamespace('https://192.168.1.1/data');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.1/.1/.168/.192/data');
    });

    it('should handle country code TLDs', () => {
      const namespace = createNamespace('https://example.co.uk/api');
      const path = parseNamespaceToPath(namespace);

      expect(path).toBe('.uk/.co/.example/api');
    });

    it('should handle path with URL-encoded characters', () => {
      const namespace = createNamespace('https://example.com/path%20with%20spaces');
      const path = parseNamespaceToPath(namespace);

      // URL.pathname preserves encoding
      expect(path).toContain('%20');
    });
  });

  describe('formatDatePath', () => {
    it('should format date correctly', () => {
      // 2024-01-15 12:00:00 UTC
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 12, 0, 0));
      const datePath = formatDatePath(timestamp);

      expect(datePath).toBe('2024-01-15');
    });

    it('should pad single-digit month', () => {
      // March 5, 2024
      const timestamp = BigInt(Date.UTC(2024, 2, 5, 12, 0, 0));
      const datePath = formatDatePath(timestamp);

      expect(datePath).toBe('2024-03-05');
    });

    it('should handle year boundaries', () => {
      // December 31, 2023, 23:59:59 UTC
      const timestamp = BigInt(Date.UTC(2023, 11, 31, 23, 59, 59));
      const datePath = formatDatePath(timestamp);

      expect(datePath).toBe('2023-12-31');
    });

    it('should handle leap year', () => {
      // February 29, 2024 (leap year)
      const timestamp = BigInt(Date.UTC(2024, 1, 29, 12, 0, 0));
      const datePath = formatDatePath(timestamp);

      expect(datePath).toBe('2024-02-29');
    });

    it('should handle epoch timestamp', () => {
      const timestamp = BigInt(0);
      const datePath = formatDatePath(timestamp);

      expect(datePath).toBe('1970-01-01');
    });

    it('should handle far future date', () => {
      // Year 3000
      const timestamp = BigInt(Date.UTC(3000, 5, 15, 12, 0, 0));
      const datePath = formatDatePath(timestamp);

      expect(datePath).toBe('3000-06-15');
    });
  });

  describe('generateSequence', () => {
    it('should generate HHMMSS-mmm format', () => {
      // 2024-01-15 14:30:45.123 UTC
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 14, 30, 45, 123));
      const sequence = generateSequence(timestamp);

      expect(sequence).toBe('143045-123');
    });

    it('should pad hours, minutes, seconds', () => {
      // 2024-01-15 01:02:03.004 UTC
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 1, 2, 3, 4));
      const sequence = generateSequence(timestamp);

      expect(sequence).toBe('010203-004');
    });

    it('should handle midnight', () => {
      // 2024-01-15 00:00:00.000 UTC
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 0, 0, 0, 0));
      const sequence = generateSequence(timestamp);

      expect(sequence).toBe('000000-000');
    });

    it('should handle end of day', () => {
      // 2024-01-15 23:59:59.999 UTC
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 23, 59, 59, 999));
      const sequence = generateSequence(timestamp);

      expect(sequence).toBe('235959-999');
    });

    it('should be sortable (lexicographic = chronological)', () => {
      const sequences: string[] = [];
      const baseDate = Date.UTC(2024, 0, 15, 0, 0, 0, 0);

      // Generate sequences across the day
      for (let i = 0; i < 1000; i++) {
        const timestamp = BigInt(baseDate + i * 60000); // Every minute
        sequences.push(generateSequence(timestamp));
      }

      // Verify sorted order is maintained
      const sorted = [...sequences].sort();
      expect(sequences).toEqual(sorted);
    });
  });

  describe('getCDCPath', () => {
    it('should generate complete path', () => {
      const namespace = createNamespace('https://example.com/crm/acme');
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 14, 30, 45, 123));

      const path = getCDCPath(namespace, timestamp);

      expect(path).toBe('.com/.example/crm/acme/_wal/2024-01-15/143045-123.gcol');
    });

    it('should generate different paths for different days', () => {
      const namespace = createNamespace('https://example.com/data');
      const day1 = BigInt(Date.UTC(2024, 0, 15, 12, 0, 0, 0));
      const day2 = BigInt(Date.UTC(2024, 0, 16, 12, 0, 0, 0));

      const path1 = getCDCPath(namespace, day1);
      const path2 = getCDCPath(namespace, day2);

      expect(path1).toContain('2024-01-15');
      expect(path2).toContain('2024-01-16');
      expect(path1).not.toBe(path2);
    });

    it('should generate different paths for different milliseconds', () => {
      const namespace = createNamespace('https://example.com/data');
      const ts1 = BigInt(Date.UTC(2024, 0, 15, 12, 0, 0, 100));
      const ts2 = BigInt(Date.UTC(2024, 0, 15, 12, 0, 0, 101));

      const path1 = getCDCPath(namespace, ts1);
      const path2 = getCDCPath(namespace, ts2);

      expect(path1).not.toBe(path2);
      expect(path1).toContain('-100.gcol');
      expect(path2).toContain('-101.gcol');
    });

    it('should generate paths sortable by time', () => {
      const namespace = createNamespace('https://example.com/data');
      const paths: string[] = [];

      // Generate paths over several hours
      const baseTimestamp = Date.UTC(2024, 0, 15, 10, 0, 0, 0);
      for (let i = 0; i < 100; i++) {
        const timestamp = BigInt(baseTimestamp + i * 30000); // Every 30 seconds
        paths.push(getCDCPath(namespace, timestamp));
      }

      // Verify paths are lexicographically sortable
      const sorted = [...paths].sort();
      expect(paths).toEqual(sorted);
    });
  });

  describe('parseCDCPath', () => {
    it('should parse valid new format path', () => {
      const path = '.com/.example/crm/acme/_wal/2024-01-15/143045-123.gcol';
      const parsed = parseCDCPath(path);

      expect(parsed).not.toBeNull();
      expect(parsed!.date).toBe('2024-01-15');
      expect(parsed!.sequence).toBe('143045-123');
    });

    it('should parse valid old format path', () => {
      const path = '.com/.example/crm/acme/_wal/2024-01-15/001.gcol';
      const parsed = parseCDCPath(path);

      expect(parsed).not.toBeNull();
      expect(parsed!.date).toBe('2024-01-15');
      expect(parsed!.sequence).toBe('001');
    });

    it('should return null for invalid paths', () => {
      expect(parseCDCPath('invalid/path')).toBeNull();
      expect(parseCDCPath('.com/.example/data.gcol')).toBeNull();
      expect(parseCDCPath('.com/.example/_wal/data.gcol')).toBeNull();
    });

    it('should return null for missing .gcol extension', () => {
      const path = '.com/.example/crm/acme/_wal/2024-01-15/143045-123';
      expect(parseCDCPath(path)).toBeNull();
    });

    it('should return null for invalid date format', () => {
      const path = '.com/.example/crm/acme/_wal/24-1-15/143045-123.gcol';
      expect(parseCDCPath(path)).toBeNull();
    });

    it('should round-trip through getCDCPath', () => {
      const namespace = createNamespace('https://example.com/test');
      const timestamp = BigInt(Date.UTC(2024, 6, 20, 9, 15, 30, 456));

      const path = getCDCPath(namespace, timestamp);
      const parsed = parseCDCPath(path);

      expect(parsed).not.toBeNull();
      expect(parsed!.date).toBe('2024-07-20');
      expect(parsed!.sequence).toBe('091530-456');
    });
  });
});

// ============================================================================
// Sequence Ordering Tests
// ============================================================================

describe('Chunk Sequence Ordering', () => {
  describe('Chronological Ordering', () => {
    it('should order sequences within same day correctly', () => {
      const sequences: string[] = [];
      const baseTime = Date.UTC(2024, 0, 15, 0, 0, 0, 0);

      // Morning
      sequences.push(generateSequence(BigInt(baseTime)));
      // Noon
      sequences.push(generateSequence(BigInt(baseTime + 12 * 60 * 60 * 1000)));
      // Evening
      sequences.push(generateSequence(BigInt(baseTime + 18 * 60 * 60 * 1000)));
      // Night
      sequences.push(generateSequence(BigInt(baseTime + 23 * 60 * 60 * 1000)));

      // Verify string sorting produces chronological order
      const sorted = [...sequences].sort();
      expect(sequences).toEqual(sorted);
    });

    it('should handle rapid sequences (same second)', () => {
      const baseTime = BigInt(Date.UTC(2024, 0, 15, 12, 30, 45, 0));
      const sequences: string[] = [];

      // Multiple events in same second
      for (let ms = 0; ms < 1000; ms++) {
        sequences.push(generateSequence(baseTime + BigInt(ms)));
      }

      // Verify ordering
      const sorted = [...sequences].sort();
      expect(sequences).toEqual(sorted);
    });

    it('should order across date boundaries', () => {
      const paths: string[] = [];
      const namespace = createNamespace('https://example.com/data');

      // End of day 1
      paths.push(getCDCPath(namespace, BigInt(Date.UTC(2024, 0, 15, 23, 59, 59, 999))));
      // Start of day 2
      paths.push(getCDCPath(namespace, BigInt(Date.UTC(2024, 0, 16, 0, 0, 0, 0))));

      // Day 2's path should sort after day 1's
      const sorted = [...paths].sort();
      expect(sorted[0]).toContain('2024-01-15');
      expect(sorted[1]).toContain('2024-01-16');
    });

    it('should order across year boundaries', () => {
      const paths: string[] = [];
      const namespace = createNamespace('https://example.com/data');

      // End of 2023
      paths.push(getCDCPath(namespace, BigInt(Date.UTC(2023, 11, 31, 23, 59, 59, 999))));
      // Start of 2024
      paths.push(getCDCPath(namespace, BigInt(Date.UTC(2024, 0, 1, 0, 0, 0, 0))));

      const sorted = [...paths].sort();
      expect(sorted[0]).toContain('2023-12-31');
      expect(sorted[1]).toContain('2024-01-01');
    });
  });

  describe('Uniqueness', () => {
    it('should generate unique sequences for different milliseconds', () => {
      const sequences = new Set<string>();
      const baseTime = BigInt(Date.UTC(2024, 0, 15, 12, 0, 0, 0));

      // Generate 1000 sequences across 1 second
      for (let ms = 0; ms < 1000; ms++) {
        sequences.add(generateSequence(baseTime + BigInt(ms)));
      }

      // All should be unique
      expect(sequences.size).toBe(1000);
    });

    it('should handle same millisecond (potential collision)', () => {
      const timestamp = BigInt(Date.UTC(2024, 0, 15, 12, 30, 45, 500));

      // Same timestamp produces same sequence
      const seq1 = generateSequence(timestamp);
      const seq2 = generateSequence(timestamp);

      expect(seq1).toBe(seq2);
      // Note: Collision handling is responsibility of caller
    });
  });
});

// ============================================================================
// Namespace Edge Cases
// ============================================================================

describe('Namespace Edge Cases', () => {
  it('should handle very long namespace paths', () => {
    const longPath = '/a'.repeat(100);
    const namespace = createNamespace(`https://example.com${longPath}`);

    const path = parseNamespaceToPath(namespace);
    expect(path).toContain('.com/.example');
    expect(path.split('/').length).toBeGreaterThan(100);
  });

  it('should handle namespace with query parameters', () => {
    // Note: Query params are typically stripped by URL parsing
    const namespace = createNamespace('https://example.com/api?version=1');
    const path = parseNamespaceToPath(namespace);

    // Query params should not appear in path
    expect(path).not.toContain('?');
    expect(path).not.toContain('version');
  });

  it('should handle namespace with fragment', () => {
    const namespace = createNamespace('https://example.com/api#section');
    const path = parseNamespaceToPath(namespace);

    // Fragment should not appear in path
    expect(path).not.toContain('#');
    expect(path).not.toContain('section');
  });

  it('should handle internationalized domain names', () => {
    // Punycode encoded IDN
    const namespace = createNamespace('https://xn--nxasmq5b.com/data');
    const path = parseNamespaceToPath(namespace);

    expect(path).toContain('.com');
    expect(path).toContain('.xn--nxasmq5b');
  });

  it('should handle port numbers (ignored in path)', () => {
    const namespace = createNamespace('https://example.com:8443/api');
    const path = parseNamespaceToPath(namespace);

    // Port should not appear in path
    expect(path).not.toContain('8443');
    expect(path).toBe('.com/.example/api');
  });

  it('should handle username/password in URL (unusual but valid)', () => {
    const namespace = createNamespace('https://user:pass@example.com/data');
    const path = parseNamespaceToPath(namespace);

    // Credentials should not appear in path
    expect(path).not.toContain('user');
    expect(path).not.toContain('pass');
    expect(path).toBe('.com/.example/data');
  });
});

// ============================================================================
// Time Range Filtering Tests
// ============================================================================

describe('Time Range Path Generation', () => {
  it('should generate all paths within a day range', () => {
    const namespace = createNamespace('https://example.com/data');
    const paths: string[] = [];

    // Generate hourly paths for one day
    const dayStart = Date.UTC(2024, 0, 15, 0, 0, 0, 0);
    for (let hour = 0; hour < 24; hour++) {
      const timestamp = BigInt(dayStart + hour * 60 * 60 * 1000);
      paths.push(getCDCPath(namespace, timestamp));
    }

    // All paths should be in same day
    expect(paths.every(p => p.includes('2024-01-15'))).toBe(true);

    // All paths should be unique
    const uniquePaths = new Set(paths);
    expect(uniquePaths.size).toBe(24);
  });

  it('should generate paths that can be filtered by date prefix', () => {
    const namespace = createNamespace('https://example.com/data');
    const namespacePath = parseNamespaceToPath(namespace);

    // Paths for January 15
    const jan15Path = `${namespacePath}/_wal/2024-01-15/`;
    const timestamp = BigInt(Date.UTC(2024, 0, 15, 12, 0, 0, 0));
    const path = getCDCPath(namespace, timestamp);

    expect(path.startsWith(jan15Path)).toBe(true);
  });

  it('should allow date-only prefix matching for listing', () => {
    const namespace = createNamespace('https://example.com/data');
    const namespacePath = parseNamespaceToPath(namespace);

    // Generate multiple paths on same day
    const paths: string[] = [];
    const dayStart = Date.UTC(2024, 0, 15, 0, 0, 0, 0);
    for (let i = 0; i < 10; i++) {
      const timestamp = BigInt(dayStart + i * 1000);
      paths.push(getCDCPath(namespace, timestamp));
    }

    // All should match date prefix
    const datePrefix = `${namespacePath}/_wal/2024-01-15/`;
    expect(paths.every(p => p.startsWith(datePrefix))).toBe(true);
  });
});

// ============================================================================
// Path Component Tests
// ============================================================================

describe('Path Components', () => {
  it('should always include _wal directory', () => {
    const namespace = createNamespace('https://example.com/any/path');
    const timestamp = BigInt(Date.now());
    const path = getCDCPath(namespace, timestamp);

    expect(path).toContain('/_wal/');
  });

  it('should always use .gcol extension', () => {
    const namespace = createNamespace('https://example.com/data');
    const timestamp = BigInt(Date.now());
    const path = getCDCPath(namespace, timestamp);

    expect(path).toMatch(/\.gcol$/);
  });

  it('should have consistent path structure', () => {
    const namespace = createNamespace('https://test.example.com/api/v1');
    const timestamp = BigInt(Date.UTC(2024, 5, 15, 10, 30, 45, 500));
    const path = getCDCPath(namespace, timestamp);

    // Verify structure: {reversed-domain}/{path}/_wal/{date}/{sequence}.gcol
    const parts = path.split('/');
    const walIndex = parts.indexOf('_wal');

    expect(walIndex).toBeGreaterThan(0);
    expect(parts[walIndex + 1]).toMatch(/^\d{4}-\d{2}-\d{2}$/); // Date
    expect(parts[walIndex + 2]).toMatch(/^\d{6}-\d{3}\.gcol$/); // Sequence
  });
});
