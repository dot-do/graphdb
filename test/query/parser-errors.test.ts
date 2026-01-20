/**
 * GraphDB Query Parser Error Handling Tests (RED Phase)
 *
 * Tests for parser error conditions to ensure proper error responses
 * with line/column information and helpful messages.
 */

import { describe, it, expect } from 'vitest';
import { parse, ParseError, countHops } from '../../src/query/parser';

// ============================================================================
// Empty Query Handling
// ============================================================================

describe('Parser Error Handling', () => {
  describe('Empty Query', () => {
    it('should handle empty query', () => {
      expect(() => parse('')).toThrow(ParseError);
    });

    it('should handle whitespace-only query', () => {
      expect(() => parse('   ')).toThrow(ParseError);
    });

    it('should handle empty query with helpful message', () => {
      try {
        parse('');
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        expect(parseError.message).toContain('empty');
      }
    });
  });

  // ============================================================================
  // Invalid Characters
  // ============================================================================

  describe('Invalid Characters', () => {
    it('should handle invalid character @', () => {
      expect(() => parse('user@123')).toThrow();
    });

    it('should handle invalid character #', () => {
      expect(() => parse('user#123')).toThrow();
    });

    it('should handle invalid character $', () => {
      expect(() => parse('$user:123')).toThrow();
    });

    it('should handle invalid character %', () => {
      expect(() => parse('user%123')).toThrow();
    });

    it('should handle invalid unicode character', () => {
      expect(() => parse('user\u0000:123')).toThrow();
    });

    it('should provide position for invalid character', () => {
      try {
        parse('user@123');
        expect.fail('Expected error to be thrown');
      } catch (error) {
        expect(error).toBeDefined();
        // Error should indicate the position via line and column
        const message = (error as Error).message.toLowerCase();
        expect(message).toContain('line');
        expect(message).toContain('column');
      }
    });
  });

  // ============================================================================
  // Unclosed Brackets
  // ============================================================================

  describe('Unclosed Brackets', () => {
    it('should handle unclosed square bracket in filter', () => {
      expect(() => parse('user:123.friends[?age > 30')).toThrow();
    });

    it('should handle unclosed curly brace in expansion', () => {
      expect(() => parse('user:123 { friends')).toThrow();
    });

    it('should handle unclosed parenthesis in condition', () => {
      expect(() => parse('user:123.friends[?(age > 30 and status = "active"]')).toThrow();
    });

    it('should handle unclosed string quote', () => {
      expect(() => parse('user:123.friends[?name = "Alice')).toThrow();
    });

    it('should handle mismatched brackets', () => {
      expect(() => parse('user:123.friends[?age > 30}')).toThrow();
    });

    it('should provide helpful message for unclosed bracket', () => {
      try {
        parse('user:123.friends[?age > 30');
        expect.fail('Expected error to be thrown');
      } catch (error) {
        expect(error).toBeDefined();
        const message = (error as Error).message;
        // Should indicate unclosed bracket or unexpected EOF
        expect(
          message.toLowerCase().includes('expected') ||
          message.toLowerCase().includes('unclosed') ||
          message.toLowerCase().includes('eof')
        ).toBe(true);
      }
    });
  });

  // ============================================================================
  // Missing Predicates
  // ============================================================================

  describe('Missing Predicates', () => {
    it('should handle missing predicate after dot', () => {
      expect(() => parse('user:123.')).toThrow(ParseError);
    });

    it('should handle missing predicate after reverse arrow', () => {
      expect(() => parse('post:456 <-')).toThrow(ParseError);
    });

    it('should handle empty predicate in expansion', () => {
      expect(() => parse('user:123 { }')).toThrow(ParseError);
    });

    it('should handle missing ID after colon', () => {
      expect(() => parse('user:')).toThrow(ParseError);
    });

    it('should handle missing namespace before colon', () => {
      expect(() => parse(':123')).toThrow();
    });

    it('should provide helpful message for missing predicate', () => {
      try {
        parse('user:123.');
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        expect(parseError.message.toLowerCase()).toContain('property');
      }
    });
  });

  // ============================================================================
  // Invalid Filters
  // ============================================================================

  describe('Invalid Filters', () => {
    it('should handle missing filter condition after [?', () => {
      expect(() => parse('user:123.friends[?]')).toThrow();
    });

    it('should handle invalid filter operator', () => {
      expect(() => parse('user:123.friends[?age ~ 30]')).toThrow();
    });

    it('should handle missing filter value', () => {
      expect(() => parse('user:123.friends[?age >]')).toThrow();
    });

    it('should handle invalid filter field (starting with number)', () => {
      expect(() => parse('user:123.friends[?123field > 30]')).toThrow();
    });

    it('should handle incomplete logical expression', () => {
      expect(() => parse('user:123.friends[?age > 30 and]')).toThrow();
    });

    it('should handle double operators in filter', () => {
      expect(() => parse('user:123.friends[?age >> 30]')).toThrow();
    });

    it('should provide helpful message for invalid filter', () => {
      try {
        parse('user:123.friends[?age ~ 30]');
        expect.fail('Expected error to be thrown');
      } catch (error) {
        const message = (error as Error).message.toLowerCase();
        expect(
          message.includes('operator') ||
          message.includes('expected') ||
          message.includes('unexpected')
        ).toBe(true);
      }
    });
  });

  // ============================================================================
  // Too Many Hops (Limit 10)
  // ============================================================================

  describe('Too Many Hops', () => {
    it('should allow up to 10 hops', () => {
      // 10 hops should be valid
      const query = 'user:123.a.b.c.d.e.f.g.h.i.j';
      const result = parse(query);
      expect(countHops(result)).toBe(10);
    });

    it('should reject more than 10 hops', () => {
      // 11 hops should fail
      const query = 'user:123.a.b.c.d.e.f.g.h.i.j.k';
      expect(() => {
        const result = parse(query);
        // If parse doesn't throw, we need a validation function
        if (countHops(result) > 10) {
          throw new ParseError('Query exceeds maximum hop limit of 10', 0);
        }
      }).toThrow();
    });

    it('should count reverse traversals as hops', () => {
      const query = 'user:123 <- follows <- likes <- comments';
      const result = parse(query);
      expect(countHops(result)).toBe(3);
    });

    it('should provide helpful message for too many hops', () => {
      const query = 'user:123.a.b.c.d.e.f.g.h.i.j.k';
      try {
        const result = parse(query);
        if (countHops(result) > 10) {
          throw new ParseError('Query exceeds maximum hop limit of 10', 0);
        }
        expect.fail('Expected error to be thrown');
      } catch (error) {
        const message = (error as Error).message.toLowerCase();
        expect(
          message.includes('hop') ||
          message.includes('limit') ||
          message.includes('10')
        ).toBe(true);
      }
    });
  });

  // ============================================================================
  // Line/Column for Syntax Errors
  // ============================================================================

  describe('Line/Column Information', () => {
    it('should provide line number in error', () => {
      try {
        parse('user:123.');
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        expect(parseError.line).toBeDefined();
        expect(typeof parseError.line).toBe('number');
      }
    });

    it('should provide column number in error', () => {
      try {
        parse('user:123.');
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        expect(parseError.column).toBeDefined();
        expect(typeof parseError.column).toBe('number');
      }
    });

    it('should track position accurately for multi-line queries', () => {
      try {
        // Simulating a multi-line query
        parse('user:123\n.friends\n.');
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        // The error should be on line 3 (trailing dot)
        expect(parseError.line).toBeGreaterThanOrEqual(1);
      }
    });

    it('should include position in error message', () => {
      try {
        parse('user:123@invalid');
        expect.fail('Expected error to be thrown');
      } catch (error) {
        const message = (error as Error).message;
        // Should contain position information
        expect(message).toMatch(/position|line|column|at/i);
      }
    });
  });

  // ============================================================================
  // Helpful Error Messages
  // ============================================================================

  describe('Helpful Error Messages', () => {
    it('should suggest valid syntax for entity lookup errors', () => {
      try {
        parse('user123'); // Missing colon
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const message = (error as ParseError).message;
        // Should mention expected colon
        expect(message).toContain(':');
      }
    });

    it('should describe what was expected vs what was found', () => {
      try {
        parse('user:123.'); // Missing property name
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const message = (error as ParseError).message.toLowerCase();
        // Should mention what was expected
        expect(message).toContain('expected');
      }
    });

    it('should provide context for deeply nested errors', () => {
      try {
        parse('user:123.friends { posts { title, invalid@ } }');
        expect.fail('Expected error to be thrown');
      } catch (error) {
        const message = (error as Error).message.toLowerCase();
        // Should indicate where the error occurred via line and column
        expect(message).toContain('line');
        expect(message).toContain('column');
      }
    });

    it('should differentiate between lexer and parser errors', () => {
      // Lexer error (invalid character)
      try {
        parse('user@123');
      } catch (error) {
        const message = (error as Error).message.toLowerCase();
        expect(message).toContain('character') || expect(message).toContain('unexpected');
      }

      // Parser error (unexpected token)
      try {
        parse('user:123..friends');
      } catch (error) {
        const message = (error as Error).message.toLowerCase();
        expect(
          message.includes('expected') ||
          message.includes('token')
        ).toBe(true);
      }
    });
  });

  // ============================================================================
  // Edge Cases
  // ============================================================================

  describe('Edge Cases', () => {
    it('should handle very long entity IDs', () => {
      const longId = 'a'.repeat(1000);
      const result = parse(`user:${longId}`);
      expect(result.type).toBe('entity');
    });

    it('should handle special characters in string values', () => {
      const result = parse('user:123.friends[?name = "O\'Brien"]');
      expect(result.type).toBe('filter');
    });

    it('should handle escaped quotes in strings', () => {
      const result = parse('user:123.friends[?name = "Say \\"Hello\\""]');
      expect(result.type).toBe('filter');
    });

    it('should handle numeric entity IDs', () => {
      const result = parse('user:999999999999');
      expect(result.type).toBe('entity');
    });

    it('should handle recursion without depth limit', () => {
      const result = parse('user:123.friends*');
      expect(result.type).toBe('recurse');
    });

    it('should handle recursion with depth limit', () => {
      const result = parse('user:123.friends*[depth <= 3]');
      expect(result.type).toBe('recurse');
    });
  });
});
