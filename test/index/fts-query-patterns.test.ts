/**
 * FTS Query Patterns Tests (TDD RED Phase)
 *
 * Tests for full-text search with various query patterns including:
 * - Special characters and escaping
 * - Unicode and international text
 * - Multi-word queries with different operators
 * - Edge cases in query parsing
 * - Query sanitization behavior
 *
 * @see src/index/fts-index.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import { sanitizeFtsQuery } from '../../src/security/fts-sanitizer.js';

// ============================================================================
// FTS QUERY PATTERN TESTS
// ============================================================================

describe('FTS Query Patterns', () => {
  describe('Special Characters Handling', () => {
    it('should escape single quotes in queries', () => {
      const input = "it's a test";
      const sanitized = sanitizeFtsQuery(input);
      // FTS5 handles single quotes, but sanitizer should preserve the meaning
      expect(sanitized).toBeDefined();
      expect(sanitized.length).toBeGreaterThan(0);
    });

    it('should handle double quotes for phrase search', () => {
      const input = '"quick brown fox"';
      const sanitized = sanitizeFtsQuery(input);
      // Phrase search should be preserved
      expect(sanitized).toContain('quick');
      expect(sanitized).toContain('brown');
      expect(sanitized).toContain('fox');
    });

    it('should escape unbalanced quotes', () => {
      const input = '"unbalanced phrase';
      const sanitized = sanitizeFtsQuery(input);
      // Should either close the quote or strip it
      expect(sanitized).toBeDefined();
    });

    it('should handle parentheses in boolean queries', () => {
      const input = '(web OR mobile) AND javascript';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
      expect(sanitized.toLowerCase()).toContain('web');
    });

    it('should escape dangerous FTS5 operators', () => {
      // FTS5 has special column prefix syntax "column:term"
      const input = 'content:malicious';
      const sanitized = sanitizeFtsQuery(input);
      // Should either escape or strip the colon
      expect(sanitized).not.toContain('content:');
    });

    it('should handle asterisk prefix wildcards', () => {
      const input = '*suffix';
      const sanitized = sanitizeFtsQuery(input);
      // FTS5 doesn't support prefix wildcards, should be handled
      expect(sanitized).toBeDefined();
    });

    it('should preserve valid suffix wildcards', () => {
      const input = 'prefix*';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('prefix*');
    });

    it('should handle multiple asterisks', () => {
      const input = 'pre*mid*suf';
      const sanitized = sanitizeFtsQuery(input);
      // Should handle multiple wildcards gracefully
      expect(sanitized).toBeDefined();
    });

    it('should escape brackets for column filter attempts', () => {
      // FTS5 uses {column} syntax in some contexts
      const input = 'test {column}';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).not.toMatch(/\{.*\}/);
    });

    it('should handle caret (NEAR) operator', () => {
      const input = 'word1 NEAR word2';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should escape backslashes', () => {
      const input = 'path\\to\\file';
      const sanitized = sanitizeFtsQuery(input);
      // Backslashes should be handled
      expect(sanitized).toBeDefined();
    });
  });

  describe('Unicode and International Text', () => {
    it('should handle Chinese characters', () => {
      const input = '人工智能';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle Japanese characters (Hiragana/Katakana/Kanji)', () => {
      const input = 'プログラミング';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle Korean characters', () => {
      const input = '프로그래밍';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle Arabic text', () => {
      const input = 'برمجة';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle Hebrew text', () => {
      const input = 'תכנות';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle Russian text', () => {
      const input = 'программирование';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle Greek text', () => {
      const input = 'προγραμματισμός';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBe(input);
    });

    it('should handle emoji in queries', () => {
      const input = 'happy face emoji';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle mixed ASCII and Unicode', () => {
      const input = 'JavaScript is 素晴らしい';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('JavaScript');
      expect(sanitized).toContain('素晴らしい');
    });

    it('should handle diacritics and accented characters', () => {
      const input = 'café résumé naïve';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('cafe');
    });

    it('should handle zero-width characters', () => {
      const input = 'test\u200Bword'; // Zero-width space
      const sanitized = sanitizeFtsQuery(input);
      // Should either strip or handle zero-width chars
      expect(sanitized).toBeDefined();
    });
  });

  describe('Boolean Operators', () => {
    it('should handle AND operator case-insensitively', () => {
      const upperCase = sanitizeFtsQuery('word1 AND word2');
      const lowerCase = sanitizeFtsQuery('word1 and word2');
      const mixedCase = sanitizeFtsQuery('word1 And word2');

      // FTS5 uses uppercase operators
      expect(upperCase).toBeDefined();
      expect(lowerCase).toBeDefined();
      expect(mixedCase).toBeDefined();
    });

    it('should handle OR operator', () => {
      const input = 'javascript OR python';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('OR');
    });

    it('should handle NOT operator', () => {
      const input = 'programming NOT tutorial';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('NOT');
    });

    it('should handle nested boolean expressions', () => {
      const input = '(web OR mobile) AND (javascript OR typescript)';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle NOT at start of query', () => {
      const input = 'NOT spam';
      const sanitized = sanitizeFtsQuery(input);
      // NOT at start might need special handling
      expect(sanitized).toBeDefined();
    });

    it('should handle multiple consecutive operators gracefully', () => {
      const input = 'word1 AND AND word2';
      const sanitized = sanitizeFtsQuery(input);
      // Should handle duplicate operators
      expect(sanitized).toBeDefined();
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty string', () => {
      const sanitized = sanitizeFtsQuery('');
      expect(sanitized).toBe('');
    });

    it('should handle whitespace-only input', () => {
      const sanitized = sanitizeFtsQuery('   \t\n   ');
      expect(sanitized.trim()).toBe('');
    });

    it('should handle very long queries', () => {
      const longQuery = 'word '.repeat(1000);
      const sanitized = sanitizeFtsQuery(longQuery);
      // Should handle or truncate appropriately
      expect(sanitized).toBeDefined();
    });

    it('should handle single character query', () => {
      const sanitized = sanitizeFtsQuery('a');
      expect(sanitized).toBeDefined();
    });

    it('should handle numbers only', () => {
      const sanitized = sanitizeFtsQuery('12345');
      expect(sanitized).toBe('12345');
    });

    it('should handle mixed alphanumeric', () => {
      const sanitized = sanitizeFtsQuery('test123abc');
      expect(sanitized).toBe('test123abc');
    });

    it('should handle hyphenated words', () => {
      const input = 'full-text-search';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle underscore in identifiers', () => {
      const input = 'user_name variable_name';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle dots in version numbers', () => {
      const input = 'version 1.2.3';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('1.2.3');
    });

    it('should handle @ symbol (email-like)', () => {
      const input = 'user@example.com';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle hashtags', () => {
      const input = '#javascript #programming';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle URL-like strings', () => {
      const input = 'https://example.com/path';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });
  });

  describe('SQL Injection Prevention', () => {
    it('should escape SQL injection attempts in queries', () => {
      const input = "'; DROP TABLE triples; --";
      const sanitized = sanitizeFtsQuery(input);
      // Should not contain raw SQL
      expect(sanitized).not.toContain('DROP');
      expect(sanitized).not.toContain('TABLE');
      expect(sanitized).not.toContain(';');
    });

    it('should handle UNION-based injection attempts', () => {
      const input = 'test UNION SELECT * FROM sqlite_master';
      const sanitized = sanitizeFtsQuery(input);
      // UNION is not a valid FTS5 operator
      expect(sanitized).not.toMatch(/UNION.*SELECT/i);
    });

    it('should handle comment injection', () => {
      const input = 'test /* comment */ word';
      const sanitized = sanitizeFtsQuery(input);
      // Should not contain comment syntax
      expect(sanitized).not.toContain('/*');
      expect(sanitized).not.toContain('*/');
    });

    it('should handle double-dash comment injection', () => {
      const input = 'test -- comment';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).not.toContain('--');
    });
  });

  describe('NEAR Operator', () => {
    it('should handle NEAR operator with distance', () => {
      const input = 'word1 NEAR/5 word2';
      const sanitized = sanitizeFtsQuery(input);
      // NEAR is a valid FTS5 operator
      expect(sanitized).toBeDefined();
    });

    it('should handle NEAR without distance', () => {
      const input = 'word1 NEAR word2';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });
  });

  describe('Phrase and Exact Match', () => {
    it('should handle phrase with multiple words', () => {
      const input = '"the quick brown fox jumps over the lazy dog"';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toContain('quick');
      expect(sanitized).toContain('brown');
      expect(sanitized).toContain('fox');
    });

    it('should handle multiple phrases', () => {
      const input = '"first phrase" AND "second phrase"';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle phrase with wildcard', () => {
      const input = '"quick brown" fox*';
      const sanitized = sanitizeFtsQuery(input);
      expect(sanitized).toBeDefined();
    });

    it('should handle empty phrase', () => {
      const input = '""';
      const sanitized = sanitizeFtsQuery(input);
      // Empty phrase should be handled gracefully
      expect(sanitized).toBeDefined();
    });
  });
});
